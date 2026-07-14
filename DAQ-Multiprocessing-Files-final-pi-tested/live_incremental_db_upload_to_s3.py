#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import platform
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

try:
    import upload_db_blobs_direct_to_s3 as blob_uploader
except ImportError as exc:  # pragma: no cover
    print(
        "Could not import sibling uploader helpers from upload_db_blobs_direct_to_s3.py: "
        f"{exc}",
        file=sys.stderr,
    )
    raise


DEFAULT_BUCKET = blob_uploader.DEFAULT_BUCKET
DEFAULT_PREFIX = os.environ.get("S3_PREFIX", "daq-live-blob-uploads")
DEFAULT_CHUNK_MB = 256
DEFAULT_TIME_BUDGET_MIN = 60.0
DEFAULT_STOP_MARGIN_SECONDS = 180.0
VALID_KINDS = blob_uploader.VALID_KINDS


@dataclass(frozen=True)
class Chunk:
    sources: list[blob_uploader.BlobSource]
    payload_bytes: int


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_state_db(source_db: Path) -> Path:
    return source_db.with_suffix(source_db.suffix + ".upload_state.db")


def open_state_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS upload_chunks (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source_db       TEXT    NOT NULL,
            bucket          TEXT    NOT NULL,
            prefix          TEXT    NOT NULL,
            run_id          TEXT,
            archive_key     TEXT,
            manifest_key    TEXT,
            status          TEXT    NOT NULL,
            row_count       INTEGER NOT NULL DEFAULT 0,
            payload_bytes   INTEGER NOT NULL DEFAULT 0,
            archive_bytes   INTEGER,
            started_at_utc  TEXT    NOT NULL,
            updated_at_utc  TEXT    NOT NULL,
            completed_at_utc TEXT,
            error           TEXT
        );

        CREATE TABLE IF NOT EXISTS uploaded_rows (
            source_db       TEXT    NOT NULL,
            bucket          TEXT    NOT NULL,
            prefix          TEXT    NOT NULL,
            table_name      TEXT    NOT NULL,
            blob_column     TEXT    NOT NULL,
            rowid           INTEGER NOT NULL,
            kind            TEXT    NOT NULL,
            size_bytes      INTEGER NOT NULL,
            chunk_id        INTEGER NOT NULL,
            uploaded_at_utc TEXT    NOT NULL,
            PRIMARY KEY (source_db, bucket, prefix, table_name, blob_column, rowid)
        );

        CREATE INDEX IF NOT EXISTS idx_uploaded_rows_lookup
            ON uploaded_rows(source_db, bucket, prefix, table_name, blob_column, rowid);

        CREATE INDEX IF NOT EXISTS idx_upload_chunks_status
            ON upload_chunks(status, started_at_utc);
        """
    )
    conn.commit()
    return conn


def uploaded_watermark(
    state: sqlite3.Connection,
    source_db: Path,
    bucket: str,
    prefix: str,
    table: str,
    blob_column: str,
) -> int:
    row = state.execute(
        """
        SELECT COALESCE(MAX(rowid), 0) AS max_rowid
        FROM uploaded_rows
        WHERE source_db = ?
          AND bucket = ?
          AND prefix = ?
          AND table_name = ?
          AND blob_column = ?
        """,
        (str(source_db), bucket, prefix, table, blob_column),
    ).fetchone()
    return int(row["max_rowid"]) if row else 0


def source_matches_filters(
    source: blob_uploader.BlobSource,
    args: argparse.Namespace,
) -> bool:
    if args.kind != "all" and source.kind != args.kind:
        return False
    if args.audio_current_only and source.kind not in {"audio", "current"}:
        return False
    if args.row_id is not None and source.rowid != args.row_id:
        return False
    return True


def iter_blob_columns(
    conn: sqlite3.Connection,
    table_filter: str | None,
    blob_column_filter: str | None,
) -> Iterable[tuple[str, str]]:
    for table in blob_uploader.table_names(conn):
        if table_filter and table != table_filter:
            continue
        blob_cols = blob_uploader.blob_columns(conn, table)
        if blob_column_filter:
            blob_cols = [col for col in blob_cols if col == blob_column_filter]
        for blob_col in blob_cols:
            yield table, blob_col


def build_source_from_row(
    table: str,
    blob_col: str,
    row: sqlite3.Row,
    args: argparse.Namespace,
    kind_maps: dict[str, str],
) -> blob_uploader.BlobSource:
    size = int(row["__blob_len__"])
    metadata = {
        key: blob_uploader.json_safe(row[key])
        for key in row.keys()
        if key not in {"__rowid__", "__blob_len__", blob_col}
    }
    kind = blob_uploader.infer_kind(
        table,
        blob_col,
        size,
        row,
        kind_maps=kind_maps,
        force_kind=args.force_kind,
        use_size_hints=not args.no_size_hints,
    )
    return blob_uploader.BlobSource(
        table=table,
        blob_column=blob_col,
        rowid=int(row["__rowid__"]),
        kind=kind,
        size=size,
        metadata=metadata,
    )


def select_next_chunk(
    source_db: Path,
    state: sqlite3.Connection,
    args: argparse.Namespace,
    kind_maps: dict[str, str],
) -> Chunk:
    max_chunk_bytes = max(1, int(args.max_chunk_mb * 1024 * 1024))
    max_rows = max(0, int(args.max_rows_per_chunk or 0))
    sources: list[blob_uploader.BlobSource] = []
    payload_bytes = 0

    with blob_uploader.open_sqlite_readonly(source_db) as conn:
        for table, blob_col in iter_blob_columns(conn, args.table, args.blob_column):
            last_uploaded = uploaded_watermark(
                state, source_db, args.bucket, args.prefix, table, blob_col
            )
            q_table = blob_uploader.quote_ident(table)
            q_blob = blob_uploader.quote_ident(blob_col)
            query = (
                f"SELECT rowid AS __rowid__, length({q_blob}) AS __blob_len__, * "
                f"FROM {q_table} "
                f"WHERE {q_blob} IS NOT NULL AND rowid > ? "
                f"ORDER BY rowid"
            )
            for row in conn.execute(query, (last_uploaded,)):
                source = build_source_from_row(table, blob_col, row, args, kind_maps)
                if not source_matches_filters(source, args):
                    continue

                if sources and payload_bytes + source.size > max_chunk_bytes:
                    return Chunk(sources=sources, payload_bytes=payload_bytes)

                sources.append(source)
                payload_bytes += source.size

                if max_rows and len(sources) >= max_rows:
                    return Chunk(sources=sources, payload_bytes=payload_bytes)

                if payload_bytes >= max_chunk_bytes:
                    return Chunk(sources=sources, payload_bytes=payload_bytes)

    return Chunk(sources=sources, payload_bytes=payload_bytes)


def pending_summary(
    source_db: Path,
    state: sqlite3.Connection,
    args: argparse.Namespace,
    kind_maps: dict[str, str],
) -> tuple[int, int]:
    count = 0
    total_bytes = 0
    with blob_uploader.open_sqlite_readonly(source_db) as conn:
        for table, blob_col in iter_blob_columns(conn, args.table, args.blob_column):
            last_uploaded = uploaded_watermark(
                state, source_db, args.bucket, args.prefix, table, blob_col
            )
            q_table = blob_uploader.quote_ident(table)
            q_blob = blob_uploader.quote_ident(blob_col)
            query = (
                f"SELECT rowid AS __rowid__, length({q_blob}) AS __blob_len__, * "
                f"FROM {q_table} "
                f"WHERE {q_blob} IS NOT NULL AND rowid > ? "
                f"ORDER BY rowid"
            )
            for row in conn.execute(query, (last_uploaded,)):
                source = build_source_from_row(table, blob_col, row, args, kind_maps)
                if source_matches_filters(source, args):
                    count += 1
                    total_bytes += source.size
    return count, total_bytes


def create_chunk_record(
    state: sqlite3.Connection,
    source_db: Path,
    args: argparse.Namespace,
    chunk: Chunk,
) -> int:
    now = utc_now_iso()
    with state:
        cur = state.execute(
            """
            INSERT INTO upload_chunks (
                source_db, bucket, prefix, status, row_count, payload_bytes,
                started_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(source_db),
                args.bucket,
                args.prefix,
                "packing",
                len(chunk.sources),
                chunk.payload_bytes,
                now,
                now,
            ),
        )
    return int(cur.lastrowid)


def update_chunk(
    state: sqlite3.Connection,
    chunk_id: int,
    status: str,
    **fields: object,
) -> None:
    allowed = {
        "run_id",
        "archive_key",
        "manifest_key",
        "archive_bytes",
        "error",
        "completed_at_utc",
    }
    assignments = ["status = ?", "updated_at_utc = ?"]
    values: list[object] = [status, utc_now_iso()]

    for key, value in fields.items():
        if key not in allowed:
            raise ValueError(f"Invalid upload_chunks field: {key}")
        assignments.append(f"{key} = ?")
        values.append(value)

    values.append(chunk_id)
    with state:
        state.execute(
            f"UPDATE upload_chunks SET {', '.join(assignments)} WHERE id = ?",
            values,
        )


def mark_chunk_uploaded(
    state: sqlite3.Connection,
    source_db: Path,
    args: argparse.Namespace,
    chunk_id: int,
    chunk: Chunk,
    archive_key: str,
) -> None:
    uploaded_at = utc_now_iso()
    manifest_key = f"{archive_key}.manifest.json"
    with state:
        for source in chunk.sources:
            state.execute(
                """
                INSERT OR REPLACE INTO uploaded_rows (
                    source_db, bucket, prefix, table_name, blob_column, rowid,
                    kind, size_bytes, chunk_id, uploaded_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(source_db),
                    args.bucket,
                    args.prefix,
                    source.table,
                    source.blob_column,
                    source.rowid,
                    source.kind,
                    source.size,
                    chunk_id,
                    uploaded_at,
                ),
            )
        state.execute(
            """
            UPDATE upload_chunks
            SET status = 'complete',
                archive_key = ?,
                manifest_key = ?,
                completed_at_utc = ?,
                updated_at_utc = ?
            WHERE id = ?
            """,
            (archive_key, manifest_key, uploaded_at, uploaded_at, chunk_id),
        )


def print_chunk_preview(chunk: Chunk) -> None:
    print(
        f"Next chunk: {len(chunk.sources)} row(s), "
        f"{blob_uploader.human_bytes(chunk.payload_bytes)} payload"
    )
    for source in chunk.sources[:12]:
        print(
            f"  {source.kind:7} table={source.table} rowid={source.rowid} "
            f"column={source.blob_column} size={blob_uploader.human_bytes(source.size)}"
        )
    if len(chunk.sources) > 12:
        print(f"  ... {len(chunk.sources) - 12} more row(s)")


def ensure_upload_dependencies(args: argparse.Namespace) -> int:
    if args.dry_run or args.status:
        return 0
    if blob_uploader.boto3 is None:
        print(
            "Missing dependency: boto3. On Raspberry Pi OS run:\n"
            "  sudo apt update\n"
            "  sudo apt install -y python3-boto3",
            file=sys.stderr,
        )
        return 2
    return 0


def budget_remaining(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    return deadline - time.monotonic()


def should_start_another_chunk(args: argparse.Namespace, deadline: float | None) -> bool:
    remaining = budget_remaining(deadline)
    if remaining is None:
        return True
    if remaining <= 0:
        return False
    if remaining <= args.stop_margin_seconds:
        print(
            "Time budget nearly exhausted "
            f"({remaining:.0f}s left, margin {args.stop_margin_seconds:.0f}s); "
            "leaving remaining rows as backlog."
        )
        return False
    return True


def upload_chunk(
    source_db: Path,
    state: sqlite3.Connection,
    args: argparse.Namespace,
    chunk: Chunk,
) -> tuple[int, int]:
    chunk_id = create_chunk_record(state, source_db, args, chunk)
    run_id = f"live-{blob_uploader.utc_stamp()}-chunk{chunk_id:06d}"
    archive_key = blob_uploader.build_archive_key(args.prefix, source_db, run_id)
    archive_path: Path | None = None

    print(
        f"Packing chunk {chunk_id}: {len(chunk.sources)} row(s), "
        f"{blob_uploader.human_bytes(chunk.payload_bytes)} payload"
    )

    try:
        with blob_uploader.open_sqlite_readonly(source_db) as conn:
            archive_path, manifest = blob_uploader.create_blob_archive(
                conn,
                args,
                chunk.sources,
                source_db,
                run_id,
                args.sha256,
                False,
            )

        archive_size = archive_path.stat().st_size
        manifest["live_incremental"] = {
            "state_db": str(args.state_db),
            "chunk_id": chunk_id,
            "run_id": run_id,
            "source_host": platform.node(),
            "time_budget_minutes": args.time_budget_min,
            "max_chunk_mb": args.max_chunk_mb,
        }
        update_chunk(
            state,
            chunk_id,
            "uploading",
            run_id=run_id,
            archive_key=archive_key,
            manifest_key=f"{archive_key}.manifest.json",
            archive_bytes=archive_size,
        )

        blob_uploader.upload_archive(args, archive_path, archive_key, manifest)
        mark_chunk_uploaded(state, source_db, args, chunk_id, chunk, archive_key)
        print(f"Chunk {chunk_id} complete.")
        return len(chunk.sources), chunk.payload_bytes

    except Exception as exc:
        update_chunk(state, chunk_id, "failed", error=f"{type(exc).__name__}: {exc}")
        raise

    finally:
        if archive_path and not args.keep_archive:
            try:
                archive_path.unlink(missing_ok=True)
            except OSError:
                pass


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be >= 0")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True, help="SQLite DB containing BLOB rows")
    parser.add_argument("--state-db", type=Path, help="Separate SQLite DB used to remember uploaded rows")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--region", default=os.environ.get("AWS_DEFAULT_REGION"))
    parser.add_argument("--profile", help="AWS profile name from ~/.aws/credentials")

    parser.add_argument("--kind", choices=["all", *sorted(VALID_KINDS)], default="all")
    parser.add_argument(
        "--audio-current-only",
        action="store_true",
        help="Upload only rows detected as audio or current; skip unknown generic BLOB rows",
    )
    parser.add_argument("--force-kind", choices=sorted(VALID_KINDS))
    parser.add_argument(
        "--kind-map",
        action="append",
        help=(
            "Map table/column names to a kind, e.g. audio_frames.payload=audio. "
            "Can be repeated."
        ),
    )
    parser.add_argument("--no-size-hints", action="store_true")
    parser.add_argument("--table", help="Only scan this SQLite table")
    parser.add_argument("--blob-column", help="Only upload this BLOB column")
    parser.add_argument("--row-id", type=int, help="Only upload this SQLite rowid")

    parser.add_argument(
        "--max-chunk-mb",
        type=positive_float,
        default=DEFAULT_CHUNK_MB,
        help="Maximum raw BLOB payload to pack into one upload archive",
    )
    parser.add_argument("--max-rows-per-chunk", type=int, help="Optional row cap per chunk")
    parser.add_argument(
        "--time-budget-min",
        type=positive_float,
        default=DEFAULT_TIME_BUDGET_MIN,
        help="Clean runtime budget. 0 means unlimited.",
    )
    parser.add_argument(
        "--stop-margin-seconds",
        type=positive_float,
        default=DEFAULT_STOP_MARGIN_SECONDS,
        help="Do not start a new chunk if less than this much budget remains",
    )
    parser.add_argument(
        "--follow",
        action="store_true",
        help="Poll for newly committed rows instead of exiting when caught up",
    )
    parser.add_argument("--poll-seconds", type=positive_float, default=10.0)
    parser.add_argument(
        "--idle-timeout-min",
        type=positive_float,
        default=0.0,
        help="In --follow mode, exit after this many idle minutes. 0 means no idle timeout.",
    )
    parser.add_argument("--max-chunks", type=int, help="Stop after uploading this many chunks")

    parser.add_argument("--archive-dir", type=Path, help="Directory for temporary chunk .tar archives")
    parser.add_argument("--keep-archive", action="store_true", help="Do not delete temporary chunk archives")
    parser.add_argument("--sha256", action="store_true", help="Calculate SHA-256 for each BLOB before upload")
    parser.add_argument("--multipart-threshold-mb", type=int, default=blob_uploader.DEFAULT_MULTIPART_THRESHOLD_MB)
    parser.add_argument("--multipart-chunk-mb", type=int, default=blob_uploader.DEFAULT_MULTIPART_CHUNK_MB)
    parser.add_argument("--max-concurrency", type=int, default=blob_uploader.DEFAULT_MAX_CONCURRENCY)

    parser.add_argument("--status", action="store_true", help="Print pending backlog and exit")
    parser.add_argument("--dry-run", action="store_true", help="Show the next chunk without uploading")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    dep_rc = ensure_upload_dependencies(args)
    if dep_rc:
        return dep_rc

    source_db = args.db.expanduser().resolve()
    if not source_db.exists():
        print(f"Database not found: {source_db}", file=sys.stderr)
        return 2

    args.state_db = (args.state_db or default_state_db(source_db)).expanduser().resolve()
    args.archive_dir = (args.archive_dir or (source_db.parent / "upload_chunks")).expanduser().resolve()

    try:
        kind_maps = blob_uploader.parse_kind_maps(args.kind_map)
    except ValueError as exc:
        print(f"Invalid --kind-map: {exc}", file=sys.stderr)
        return 2

    deadline = None
    if args.time_budget_min > 0:
        deadline = time.monotonic() + args.time_budget_min * 60.0

    state = open_state_db(args.state_db)
    uploaded_chunks = 0
    uploaded_rows = 0
    uploaded_bytes = 0
    idle_since: float | None = None

    print(f"Source DB: {source_db}")
    print(f"State DB:  {args.state_db}")

    try:
        if args.status:
            count, total_bytes = pending_summary(source_db, state, args, kind_maps)
            print(f"Pending: {count} row(s), {blob_uploader.human_bytes(total_bytes)} payload")
            return 0

        while True:
            if not should_start_another_chunk(args, deadline):
                return 0

            chunk = select_next_chunk(source_db, state, args, kind_maps)
            if not chunk.sources:
                if not args.follow:
                    print("Caught up: no pending committed rows matched the filters.")
                    return 0

                now = time.monotonic()
                if idle_since is None:
                    idle_since = now
                idle_minutes = (now - idle_since) / 60.0
                if args.idle_timeout_min and idle_minutes >= args.idle_timeout_min:
                    print(f"Idle timeout reached after {idle_minutes:.1f} min; exiting.")
                    return 0

                remaining = budget_remaining(deadline)
                if remaining is not None and remaining <= 0:
                    print("Time budget exhausted while waiting for new rows.")
                    return 0

                print(f"No new committed rows yet; sleeping {args.poll_seconds:.1f}s.")
                time.sleep(args.poll_seconds)
                continue

            idle_since = None

            if args.dry_run:
                print_chunk_preview(chunk)
                return 0

            rows, bytes_uploaded = upload_chunk(source_db, state, args, chunk)
            uploaded_chunks += 1
            uploaded_rows += rows
            uploaded_bytes += bytes_uploaded

            if args.max_chunks and uploaded_chunks >= args.max_chunks:
                print(f"Reached --max-chunks={args.max_chunks}; exiting cleanly.")
                return 0

    except blob_uploader.NoCredentialsError:
        print("AWS credentials were not found. Configure ~/.aws/credentials on the Pi.", file=sys.stderr)
        return 2
    except (
        blob_uploader.BotoCoreError,
        blob_uploader.ClientError,
        OSError,
        sqlite3.Error,
        RuntimeError,
    ) as exc:
        print(f"Live upload failed: {exc}", file=sys.stderr)
        return 1
    finally:
        state.close()
        print(
            "Session summary: "
            f"{uploaded_chunks} chunk(s), {uploaded_rows} row(s), "
            f"{blob_uploader.human_bytes(uploaded_bytes)} payload uploaded"
        )


if __name__ == "__main__":
    raise SystemExit(main())
