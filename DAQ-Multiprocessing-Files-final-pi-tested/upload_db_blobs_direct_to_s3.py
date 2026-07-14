#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import platform
import sqlite3
import sys
import tarfile
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    import boto3
    from boto3.s3.transfer import TransferConfig
    from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
except ImportError:  # pragma: no cover
    boto3 = None
    TransferConfig = None
    BotoCoreError = ClientError = NoCredentialsError = Exception


DEFAULT_BUCKET = os.environ.get("S3_BUCKET", "sonibel-testing")
DEFAULT_PREFIX = os.environ.get("S3_PREFIX", "direct-pi-blob-uploads")
DEFAULT_MULTIPART_THRESHOLD_MB = 8
DEFAULT_MULTIPART_CHUNK_MB = 64
DEFAULT_MAX_CONCURRENCY = 8
DEFAULT_WORKERS = 8
LARGE_BLOB_FALLBACK_LIMIT_BYTES = 128 * 1024 * 1024
VALID_KINDS = {"audio", "current", "blob"}


@dataclass(frozen=True)
class BlobSource:
    table: str
    blob_column: str
    rowid: int
    kind: str
    size: int
    metadata: dict[str, Any]


class ProgressPrinter:
    def __init__(self, total_bytes: int) -> None:
        self.total_bytes = total_bytes
        self.transferred = 0
        self.start = time.monotonic()
        self.last_print = self.start
        self.lock = threading.Lock()

    def __call__(self, bytes_amount: int) -> None:
        with self.lock:
            self.transferred += bytes_amount
            now = time.monotonic()
            if now - self.last_print < 0.75 and self.transferred < self.total_bytes:
                return
            elapsed = max(now - self.start, 1e-6)
            pct = (self.transferred / self.total_bytes) * 100 if self.total_bytes else 100
            rate = self.transferred / elapsed
            print(
                f"\rUploaded {human_bytes(self.transferred)} / {human_bytes(self.total_bytes)} "
                f"({pct:5.1f}%) at {human_bytes(rate)}/s",
                end="",
                flush=True,
            )
            self.last_print = now
            if self.transferred >= self.total_bytes:
                print()


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def human_bytes(value: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    n = float(value)
    for unit in units:
        if abs(n) < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(n)} {unit}"
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} TB"


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sqlite_uri(path: Path) -> str:
    return "file:" + path.resolve().as_posix().replace(":", "%3A") + "?mode=ro"


def open_sqlite_readonly(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(sqlite_uri(path), uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA schema_version").fetchone()
    return conn


def sanitize_key_part(value: str) -> str:
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.,="
    return "".join(ch if ch in allowed else "_" for ch in value).strip("_") or "unnamed"


def file_signature(paths: list[Path]) -> tuple[tuple[str, int, int], ...]:
    sig = []
    for path in paths:
        if path.exists():
            st = path.stat()
            sig.append((str(path), st.st_size, st.st_mtime_ns))
        else:
            sig.append((str(path), -1, -1))
    return tuple(sig)


def wait_until_stable(db_path: Path, stable_seconds: float, timeout_seconds: float) -> None:
    if stable_seconds <= 0:
        return

    watched = [db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")]
    deadline = time.monotonic() + timeout_seconds
    last_sig = file_signature(watched)
    stable_since = time.monotonic()

    print(f"Waiting for DB/WAL files to stay unchanged for {stable_seconds:.1f}s...")
    while time.monotonic() < deadline:
        time.sleep(1.0)
        sig = file_signature(watched)
        if sig != last_sig:
            last_sig = sig
            stable_since = time.monotonic()
            continue
        if time.monotonic() - stable_since >= stable_seconds:
            print("DB files look stable.")
            return

    raise TimeoutError(f"DB files did not stay stable within {timeout_seconds:.1f}s")


def create_sqlite_snapshot(source_db: Path, snapshot_dir: Path) -> Path:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = snapshot_dir / f"{source_db.stem}-blob-snapshot-{utc_stamp()}{source_db.suffix or '.db'}"

    print(f"Creating SQLite snapshot: {snapshot_path}")
    src = open_sqlite_readonly(source_db)
    dst = sqlite3.connect(snapshot_path)
    try:
        with dst:
            src.backup(dst, pages=4096)
    finally:
        dst.close()
        src.close()

    return snapshot_path


def json_safe(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"blob_bytes": len(value)}
    return value


def parse_kind_maps(values: list[str] | None) -> dict[str, str]:
    mappings: dict[str, str] = {}
    for value in values or []:
        if "=" not in value:
            raise ValueError(f"Kind map must look like table.column=audio, got {value!r}")
        lhs, rhs = value.split("=", 1)
        key = lhs.strip().lower()
        kind = rhs.strip().lower()
        if not key:
            raise ValueError(f"Kind map has an empty table/column name: {value!r}")
        if kind not in VALID_KINDS:
            raise ValueError(f"Kind map {value!r} uses invalid kind {kind!r}; use audio, current, or blob")
        mappings[key] = kind
    return mappings


def infer_kind(
    table: str,
    blob_column: str,
    size: int,
    row: sqlite3.Row | None = None,
    kind_maps: dict[str, str] | None = None,
    force_kind: str | None = None,
    use_size_hints: bool = True,
) -> str:
    if force_kind:
        return force_kind

    table_key = table.lower()
    column_key = blob_column.lower()
    exact_key = f"{table_key}.{column_key}"
    if kind_maps:
        if exact_key in kind_maps:
            return kind_maps[exact_key]
        if table_key in kind_maps:
            return kind_maps[table_key]
        if column_key in kind_maps:
            return kind_maps[column_key]

    names = f"{table} {blob_column}".lower()
    if row is not None:
        names += " " + " ".join(row.keys()).lower()

    if "current" in names or "curr" in names:
        return "current"
    if "audio" in names or "mic" in names or "iepe" in names or "sound" in names:
        return "audio"

    if not use_size_hints:
        return "blob"
    if size == 102400:
        return "current"
    if size == 204800:
        return "audio"
    return "blob"


def blob_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    cols = list(conn.execute(f"PRAGMA table_info({quote_ident(table)})"))
    declared = [row["name"] for row in cols if "BLOB" in (row["type"] or "").upper()]

    found = list(declared)
    for row in cols:
        name = row["name"]
        if name in found:
            continue
        try:
            probe = conn.execute(
                f"SELECT typeof({quote_ident(name)}) AS ty "
                f"FROM {quote_ident(table)} WHERE {quote_ident(name)} IS NOT NULL LIMIT 1"
            ).fetchone()
        except sqlite3.DatabaseError:
            continue
        if probe and probe["ty"] == "blob":
            found.append(name)
    return found


def table_names(conn: sqlite3.Connection) -> list[str]:
    return [
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]


def iter_blob_sources(
    conn: sqlite3.Connection,
    table_filter: str | None = None,
    blob_column_filter: str | None = None,
    kind_maps: dict[str, str] | None = None,
    force_kind: str | None = None,
    use_size_hints: bool = True,
) -> Iterable[BlobSource]:
    for table in table_names(conn):
        if table_filter and table != table_filter:
            continue

        blob_cols = blob_columns(conn, table)
        if blob_column_filter:
            blob_cols = [col for col in blob_cols if col == blob_column_filter]

        q_table = quote_ident(table)
        for blob_col in blob_cols:
            q_blob = quote_ident(blob_col)
            query = (
                f"SELECT rowid AS __rowid__, length({q_blob}) AS __blob_len__, * "
                f"FROM {q_table} WHERE {q_blob} IS NOT NULL ORDER BY rowid"
            )
            for row in conn.execute(query):
                size = int(row["__blob_len__"])
                metadata = {
                    key: json_safe(row[key])
                    for key in row.keys()
                    if key not in {"__rowid__", "__blob_len__", blob_col}
                }
                kind = infer_kind(
                    table,
                    blob_col,
                    size,
                    row,
                    kind_maps=kind_maps,
                    force_kind=force_kind,
                    use_size_hints=use_size_hints,
                )
                yield BlobSource(table, blob_col, int(row["__rowid__"]), kind, size, metadata)


def open_blob_reader(conn: sqlite3.Connection, source: BlobSource) -> Any:
    if hasattr(conn, "blobopen"):
        return conn.blobopen(source.table, source.blob_column, source.rowid, readonly=True)

    if source.size > LARGE_BLOB_FALLBACK_LIMIT_BYTES:
        raise RuntimeError(
            "This Python sqlite3 does not support streaming blobopen(), and the BLOB is "
            f"{human_bytes(source.size)}. Refusing to load it all into RAM. Use /usr/bin/python3 "
            "from Python 3.11+ or install a Python version with sqlite3.Connection.blobopen()."
        )

    row = conn.execute(
        f"SELECT {quote_ident(source.blob_column)} AS payload "
        f"FROM {quote_ident(source.table)} WHERE rowid = ?",
        (source.rowid,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"Row {source.rowid} disappeared from {source.table}")
    return io.BytesIO(bytes(row["payload"]))


def sha256_blob(conn: sqlite3.Connection, source: BlobSource) -> str:
    h = hashlib.sha256()
    with open_blob_reader(conn, source) as blob:
        while True:
            block = blob.read(8 * 1024 * 1024)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def build_blob_key(prefix: str, db_path: Path, source: BlobSource, run_id: str) -> str:
    now = datetime.now(timezone.utc)
    clean_prefix = prefix.strip("/")
    host = sanitize_key_part(platform.node() or "raspberrypi")
    db_name = sanitize_key_part(db_path.stem)
    table = sanitize_key_part(source.table)
    column = sanitize_key_part(source.blob_column)
    filename = f"{table}-row{source.rowid}-{column}.bin"
    parts = [
        clean_prefix,
        f"{now:%Y}",
        f"{now:%m}",
        f"{now:%d}",
        host,
        db_name,
        run_id,
        source.kind,
        filename,
    ]
    return "/".join(part for part in parts if part)


def build_run_base_key(prefix: str, db_path: Path, run_id: str) -> str:
    clean_prefix = prefix.strip("/")
    host = sanitize_key_part(platform.node() or "raspberrypi")
    db_name = sanitize_key_part(db_path.stem)
    return "/".join(
        part
        for part in [
            clean_prefix,
            datetime.now(timezone.utc).strftime("%Y/%m/%d"),
            host,
            db_name,
            run_id,
        ]
        if part
    )


def build_archive_key(prefix: str, db_path: Path, run_id: str) -> str:
    return f"{build_run_base_key(prefix, db_path, run_id)}/audio-current-blobs.tar"


def build_archive_member_name(source: BlobSource) -> str:
    table = sanitize_key_part(source.table)
    column = sanitize_key_part(source.blob_column)
    filename = f"{table}-row{source.rowid}-{column}.bin"
    return f"{sanitize_key_part(source.kind)}/{filename}"


def session_from_args(args: argparse.Namespace) -> Any:
    kwargs: dict[str, Any] = {}
    if args.profile:
        kwargs["profile_name"] = args.profile
    if args.region:
        kwargs["region_name"] = args.region
    return boto3.Session(**kwargs)


def upload_blob(
    s3: Any,
    conn: sqlite3.Connection,
    source: BlobSource,
    bucket: str,
    key: str,
    config: TransferConfig,
    sha256_hex: str | None,
    show_progress: bool = True,
) -> None:
    extra_args = {
        "ContentType": "application/octet-stream",
        "Metadata": {
            "kind": source.kind,
            "table": source.table,
            "column": source.blob_column,
            "rowid": str(source.rowid),
            "size-bytes": str(source.size),
        },
    }
    if sha256_hex:
        extra_args["Metadata"]["sha256"] = sha256_hex

    single_thread_config = TransferConfig(
        multipart_threshold=config.multipart_threshold,
        multipart_chunksize=config.multipart_chunksize,
        max_concurrency=1,
        use_threads=False,
    )

    blob = open_blob_reader(conn, source)
    try:
        s3.upload_fileobj(
            blob,
            bucket,
            key,
            ExtraArgs=extra_args,
            Callback=ProgressPrinter(source.size) if show_progress else None,
            Config=single_thread_config,
        )
    finally:
        try:
            blob.close()
        except sqlite3.Error:
            pass

    head = s3.head_object(Bucket=bucket, Key=key)
    remote_size = int(head["ContentLength"])
    if remote_size != source.size:
        raise RuntimeError(f"S3 size mismatch for rowid={source.rowid}: local={source.size} remote={remote_size}")


def upload_manifest(s3: Any, bucket: str, key: str, manifest: dict[str, Any]) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(manifest, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def upload_one_source(
    args: argparse.Namespace,
    db_path: Path,
    original_db_path: Path,
    source: BlobSource,
    run_id: str,
    make_sha256: bool,
    upload_per_blob_manifest: bool,
    snapshot_used: bool,
) -> dict[str, Any]:
    session = session_from_args(args)
    s3 = session.client("s3")
    config = TransferConfig(
        multipart_threshold=args.multipart_threshold_mb * 1024 * 1024,
        multipart_chunksize=args.multipart_chunk_mb * 1024 * 1024,
        max_concurrency=1,
        use_threads=False,
    )

    with open_sqlite_readonly(db_path) as conn:
        sha256_hex = sha256_blob(conn, source) if make_sha256 else None
        blob_key = build_blob_key(args.prefix, original_db_path, source, run_id)
        manifest_key = f"{blob_key}.manifest.json"
        upload_blob(
            s3,
            conn,
            source,
            args.bucket,
            blob_key,
            config,
            sha256_hex,
            show_progress=False,
        )

    manifest = {
        "bucket": args.bucket,
        "key": blob_key,
        "s3_uri": f"s3://{args.bucket}/{blob_key}",
        "manifest_key": manifest_key if upload_per_blob_manifest else None,
        "source_db": str(original_db_path),
        "snapshot_used": snapshot_used,
        "kind": source.kind,
        "table": source.table,
        "blob_column": source.blob_column,
        "rowid": source.rowid,
        "size_bytes": source.size,
        "sha256": sha256_hex,
        "uploaded_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_host": platform.node(),
        "columns": source.metadata,
    }

    if upload_per_blob_manifest:
        upload_manifest(s3, args.bucket, manifest_key, manifest)

    return manifest


def add_json_to_tar(tar: tarfile.TarFile, arcname: str, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, indent=2).encode("utf-8")
    info = tarfile.TarInfo(arcname)
    info.size = len(body)
    info.mtime = int(time.time())
    info.mode = 0o644
    tar.addfile(info, io.BytesIO(body))


def add_blob_to_tar(tar: tarfile.TarFile, conn: sqlite3.Connection, source: BlobSource, arcname: str) -> None:
    info = tarfile.TarInfo(arcname)
    info.size = source.size
    info.mtime = int(time.time())
    info.mode = 0o644

    blob = open_blob_reader(conn, source)
    try:
        tar.addfile(info, blob)
    finally:
        try:
            blob.close()
        except sqlite3.Error:
            pass


def create_blob_archive(
    conn: sqlite3.Connection,
    args: argparse.Namespace,
    sources: list[BlobSource],
    original_db_path: Path,
    run_id: str,
    make_sha256: bool,
    snapshot_used: bool,
) -> tuple[Path, dict[str, Any]]:
    archive_dir = (args.archive_dir or Path(tempfile.gettempdir())).expanduser()
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path = archive_dir / f"{sanitize_key_part(original_db_path.stem)}-blobs-{run_id}.tar"

    objects: list[dict[str, Any]] = []
    total_bytes = sum(source.size for source in sources)
    print(f"Creating archive: {archive_path}")
    print(f"Packing {len(sources)} BLOB(s), raw payload total {human_bytes(total_bytes)}")

    with tarfile.open(archive_path, "w") as tar:
        for index, source in enumerate(sources, start=1):
            arcname = build_archive_member_name(source)
            sha256_hex = sha256_blob(conn, source) if make_sha256 else None
            add_blob_to_tar(tar, conn, source, arcname)
            print(f"[{index}/{len(sources)}] Packed {source.kind} rowid={source.rowid} {human_bytes(source.size)}")
            objects.append(
                {
                    "archive_member": arcname,
                    "kind": source.kind,
                    "table": source.table,
                    "blob_column": source.blob_column,
                    "rowid": source.rowid,
                    "size_bytes": source.size,
                    "sha256": sha256_hex,
                    "columns": source.metadata,
                }
            )

        index_manifest = {
            "format": "tar archive of raw SQLite BLOB payloads",
            "source_db": str(original_db_path),
            "snapshot_used": snapshot_used,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_host": platform.node(),
            "count": len(objects),
            "payload_bytes": total_bytes,
            "objects": objects,
        }
        add_json_to_tar(tar, "upload-index.json", index_manifest)

    archive_size = archive_path.stat().st_size
    index_manifest["archive_file"] = str(archive_path)
    index_manifest["archive_size_bytes"] = archive_size
    return archive_path, index_manifest


def upload_archive(
    args: argparse.Namespace,
    archive_path: Path,
    archive_key: str,
    index_manifest: dict[str, Any],
) -> None:
    session = session_from_args(args)
    s3 = session.client("s3")
    config = TransferConfig(
        multipart_threshold=args.multipart_threshold_mb * 1024 * 1024,
        multipart_chunksize=args.multipart_chunk_mb * 1024 * 1024,
        max_concurrency=args.max_concurrency,
        use_threads=args.max_concurrency > 1,
    )

    archive_size = archive_path.stat().st_size
    extra_args = {
        "ContentType": "application/x-tar",
        "Metadata": {
            "source-host": platform.node() or "unknown",
            "blob-count": str(index_manifest["count"]),
            "payload-bytes": str(index_manifest["payload_bytes"]),
        },
    }

    print(f"Uploading archive {human_bytes(archive_size)} to s3://{args.bucket}/{archive_key}")
    s3.upload_file(
        str(archive_path),
        args.bucket,
        archive_key,
        ExtraArgs=extra_args,
        Callback=ProgressPrinter(archive_size),
        Config=config,
    )

    head = s3.head_object(Bucket=args.bucket, Key=archive_key)
    remote_size = int(head["ContentLength"])
    if remote_size != archive_size:
        raise RuntimeError(f"S3 archive size mismatch: local={archive_size} remote={remote_size}")

    manifest_key = f"{archive_key}.manifest.json"
    index_manifest = dict(index_manifest)
    index_manifest["bucket"] = args.bucket
    index_manifest["key"] = archive_key
    index_manifest["s3_uri"] = f"s3://{args.bucket}/{archive_key}"
    index_manifest["manifest_key"] = manifest_key
    index_manifest["uploaded_at_utc"] = datetime.now(timezone.utc).isoformat()
    upload_manifest(s3, args.bucket, manifest_key, index_manifest)
    print(f"Archive: s3://{args.bucket}/{archive_key}")
    print(f"Index:   s3://{args.bucket}/{manifest_key}")


def print_sources(sources: list[BlobSource]) -> None:
    if not sources:
        print("No matching BLOB rows found.")
        return
    for src in sources:
        print(
            f"{src.kind:7} table={src.table} rowid={src.rowid} "
            f"column={src.blob_column} size={src.size}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True, help="SQLite DB containing BLOB rows")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--region", default=os.environ.get("AWS_DEFAULT_REGION"))
    parser.add_argument("--profile", help="AWS profile name from ~/.aws/credentials")
    parser.add_argument("--kind", choices=["all", "audio", "current", "blob"], default="all")
    parser.add_argument(
        "--audio-current-only",
        action="store_true",
        help="Upload only rows detected as audio or current; skip unknown generic BLOB rows",
    )
    parser.add_argument(
        "--force-kind",
        choices=["audio", "current", "blob"],
        help="Label every matching BLOB as this kind before filtering/uploading",
    )
    parser.add_argument(
        "--kind-map",
        action="append",
        help=(
            "Map table/column names to a kind, e.g. audio_frames.payload=audio, "
            "current_frames.payload=current, current_payload=current. Can be repeated."
        ),
    )
    parser.add_argument(
        "--no-size-hints",
        action="store_true",
        help="Do not use legacy 102400/current and 204800/audio size guesses",
    )
    parser.add_argument("--table", help="Only scan this SQLite table")
    parser.add_argument("--blob-column", help="Only upload this BLOB column")
    parser.add_argument("--row-id", type=int, help="Only upload this SQLite rowid")
    parser.add_argument("--limit", type=int, help="Maximum matching BLOB rows to upload")
    parser.add_argument("--snapshot", dest="snapshot", action="store_true", default=True)
    parser.add_argument("--no-snapshot", dest="snapshot", action="store_false")
    parser.add_argument("--snapshot-dir", type=Path, help="Directory for temporary SQLite snapshot")
    parser.add_argument("--keep-snapshot", action="store_true", help="Do not delete the temporary snapshot")
    parser.add_argument("--sha256", action="store_true", help="Calculate SHA-256 for each BLOB before upload")
    parser.add_argument("--skip-sha256", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--archive",
        action="store_true",
        help="Pack selected raw BLOB .bin files into one .tar and upload that single S3 object",
    )
    parser.add_argument("--archive-dir", type=Path, help="Directory for the temporary .tar archive")
    parser.add_argument("--keep-archive", action="store_true", help="Do not delete the temporary .tar archive")
    parser.add_argument(
        "--per-blob-manifest",
        action="store_true",
        help="Write one .manifest.json object next to each BLOB. Default is one run-level upload-index.json only.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Number of BLOB rows to upload in parallel",
    )
    parser.add_argument("--wait-until-stable", type=float, default=0.0)
    parser.add_argument("--wait-timeout", type=float, default=300.0)
    parser.add_argument("--multipart-threshold-mb", type=int, default=DEFAULT_MULTIPART_THRESHOLD_MB)
    parser.add_argument("--multipart-chunk-mb", type=int, default=DEFAULT_MULTIPART_CHUNK_MB)
    parser.add_argument("--max-concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if boto3 is None:
        print(
            "Missing dependency: boto3. On Raspberry Pi OS run:\n"
            "  sudo apt update\n"
            "  sudo apt install -y python3-boto3",
            file=sys.stderr,
        )
        return 2

    source_db = args.db.expanduser().resolve()
    if not source_db.exists():
        print(f"Database not found: {source_db}", file=sys.stderr)
        return 2

    snapshot_path: Path | None = None
    archive_path: Path | None = None
    upload_db = source_db

    try:
        kind_maps = parse_kind_maps(args.kind_map)
        wait_until_stable(source_db, args.wait_until_stable, args.wait_timeout)
        if args.snapshot and not args.dry_run:
            snapshot_dir = args.snapshot_dir or Path(tempfile.gettempdir())
            snapshot_path = create_sqlite_snapshot(source_db, snapshot_dir.expanduser())
            upload_db = snapshot_path

        with open_sqlite_readonly(upload_db) as conn:
            sources = list(
                iter_blob_sources(
                    conn,
                    args.table,
                    args.blob_column,
                    kind_maps=kind_maps,
                    force_kind=args.force_kind,
                    use_size_hints=not args.no_size_hints,
                )
            )
            if args.kind != "all":
                sources = [s for s in sources if s.kind == args.kind]
            if args.audio_current_only:
                sources = [s for s in sources if s.kind in {"audio", "current"}]
            if args.row_id is not None:
                sources = [s for s in sources if s.rowid == args.row_id]
            if args.limit is not None:
                sources = sources[: max(args.limit, 0)]

            print_sources(sources)
            if args.dry_run:
                print("Dry run only; no S3 upload performed.")
                return 0
            if not sources:
                return 1

            run_id = utc_stamp()
            uploaded: list[dict[str, Any]] = []
            start = time.monotonic()
            make_sha256 = args.sha256 and not args.skip_sha256
            if args.archive:
                archive_key = build_archive_key(args.prefix, source_db, run_id)
                archive_path, index_manifest = create_blob_archive(
                    conn,
                    args,
                    sources,
                    source_db,
                    run_id,
                    make_sha256,
                    snapshot_path is not None,
                )
                upload_archive(args, archive_path, archive_key, index_manifest)
                elapsed = max(time.monotonic() - start, 1e-6)
                total_bytes = sum(source.size for source in sources)
                print(f"Uploaded {len(sources)} BLOB(s) in one archive.")
                print(f"Done in {elapsed:.1f}s, average {human_bytes(total_bytes / elapsed)}/s")
                return 0

            worker_count = max(1, args.workers)
            print(
                f"Uploading {len(sources)} BLOB object(s) with {worker_count} worker(s); "
                f"sha256={'on' if make_sha256 else 'off'}, "
                f"per_blob_manifest={'on' if args.per_blob_manifest else 'off'}"
            )

            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = {
                    executor.submit(
                        upload_one_source,
                        args,
                        upload_db,
                        source_db,
                        source,
                        run_id,
                        make_sha256,
                        args.per_blob_manifest,
                        snapshot_path is not None,
                    ): source
                    for source in sources
                }

                completed = 0
                for future in as_completed(futures):
                    source = futures[future]
                    manifest = future.result()
                    uploaded.append(manifest)
                    completed += 1
                    print(
                        f"[{completed}/{len(sources)}] Uploaded {source.kind} "
                        f"rowid={source.rowid} {human_bytes(source.size)}"
                    )

            uploaded.sort(key=lambda item: (item["kind"], item["table"], item["blob_column"], item["rowid"]))

            session = session_from_args(args)
            s3 = session.client("s3")

            index_key = "/".join(
                [
                    build_run_base_key(args.prefix, source_db, run_id),
                    "upload-index.json",
                ]
            )
            upload_manifest(
                s3,
                args.bucket,
                index_key,
                {
                    "source_db": str(source_db),
                    "uploaded_at_utc": datetime.now(timezone.utc).isoformat(),
                    "source_host": platform.node(),
                    "count": len(uploaded),
                    "objects": uploaded,
                },
            )

            elapsed = max(time.monotonic() - start, 1e-6)
            total_bytes = sum(item["size_bytes"] for item in uploaded)
            print(f"Uploaded {len(uploaded)} BLOB object(s).")
            print(f"Index: s3://{args.bucket}/{index_key}")
            print(f"Done in {elapsed:.1f}s, average {human_bytes(total_bytes / elapsed)}/s")
            return 0

    except NoCredentialsError:
        print("AWS credentials were not found. Configure ~/.aws/credentials on the Pi.", file=sys.stderr)
        return 2
    except (BotoCoreError, ClientError, OSError, sqlite3.Error, TimeoutError, RuntimeError) as exc:
        print(f"Upload failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if snapshot_path and not args.keep_snapshot:
            try:
                snapshot_path.unlink(missing_ok=True)
            except OSError:
                pass
        if archive_path and not args.keep_archive:
            try:
                archive_path.unlink(missing_ok=True)
            except OSError:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
