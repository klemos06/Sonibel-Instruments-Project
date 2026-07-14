#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shlex
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path


DEFAULT_PENDING_DIR = Path("./daq_data/backlog/pending")
DEFAULT_UPLOADED_DIR = Path("./daq_data/backlog/uploaded")
DEFAULT_FAILED_DIR = Path("./daq_data/backlog/failed")
DEFAULT_STATE_DIR = Path("./daq_data/backlog/upload_state")
DEFAULT_LOCK_FILE = Path("./daq_data/backlog/upload.lock")
DEFAULT_BUCKET = os.environ.get("S3_BUCKET", "sonibel-testing")
DEFAULT_PREFIX = os.environ.get("S3_PREFIX", "daq-unmodified-backlog-uploads")


def human_bytes(value: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    n = float(value)
    for unit in units:
        if abs(n) < 1024 or unit == units[-1]:
            return f"{int(n)} {unit}" if unit == "B" else f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} TB"


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sqlite_uri(path: Path) -> str:
    return "file:" + path.resolve().as_posix().replace(":", "%3A") + "?mode=ro"


def source_db_key(path: Path) -> str:
    return str(path.resolve())


def count_source_rows(db_path: Path, table: str, blob_column: str) -> tuple[int, int]:
    conn = sqlite3.connect(sqlite_uri(db_path), uri=True, timeout=30)
    try:
        q_table = quote_ident(table)
        q_blob = quote_ident(blob_column)
        row = conn.execute(
            f"SELECT COUNT(*) AS cnt, COALESCE(SUM(length({q_blob})), 0) AS bytes "
            f"FROM {q_table} WHERE {q_blob} IS NOT NULL"
        ).fetchone()
        return int(row[0]), int(row[1])
    finally:
        conn.close()


def count_uploaded_rows(
    state_db: Path,
    db_path: Path,
    bucket: str,
    prefix: str,
    table: str,
    blob_column: str,
) -> int:
    if not state_db.exists():
        return 0

    conn = sqlite3.connect(state_db, timeout=30)
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) FROM uploaded_rows
            WHERE source_db = ?
              AND bucket = ?
              AND prefix = ?
              AND table_name = ?
              AND blob_column = ?
            """,
            (source_db_key(db_path), bucket, prefix, table, blob_column),
        ).fetchone()
        return int(row[0]) if row else 0
    except sqlite3.Error:
        return 0
    finally:
        conn.close()


def move_with_sidecars(db_path: Path, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / db_path.name
    if dest.exists():
        stem = db_path.stem
        suffix = db_path.suffix
        for i in range(1, 1000):
            candidate = dest_dir / f"{stem}.dup{i}{suffix}"
            if not candidate.exists():
                dest = candidate
                break
        else:
            raise RuntimeError(f"Could not find unique destination for {db_path}")

    shutil.move(str(db_path), str(dest))
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(db_path) + suffix)
        if sidecar.exists():
            shutil.move(str(sidecar), str(Path(str(dest) + suffix)))
    for suffix in (".manifest.json", ".json"):
        sidecar = Path(str(db_path) + suffix)
        if sidecar.exists():
            shutil.move(str(sidecar), str(Path(str(dest) + suffix)))
    return dest


def acquire_lock(lock_file: Path) -> object:
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_file.open("w")
    try:
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        raise RuntimeError(f"Another backlog uploader is already running: {lock_file}") from exc
    return handle


def uploader_script_path() -> Path:
    return Path(__file__).resolve().with_name("live_incremental_db_upload_to_s3.py")


def run_live_uploader_for_db(
    args: argparse.Namespace,
    db_path: Path,
    state_db: Path,
    remaining_minutes: float,
) -> int:
    script = uploader_script_path()
    cmd = [
        sys.executable,
        str(script),
        "--db", str(db_path),
        "--state-db", str(state_db),
        "--bucket", args.bucket,
        "--prefix", args.prefix,
        "--table", args.table,
        "--blob-column", args.blob_column,
        "--force-kind", args.force_kind,
        "--max-chunk-mb", str(args.max_chunk_mb),
        "--time-budget-min", f"{remaining_minutes:.3f}",
        "--stop-margin-seconds", str(args.stop_margin_seconds),
        "--multipart-threshold-mb", str(args.multipart_threshold_mb),
        "--multipart-chunk-mb", str(args.multipart_chunk_mb),
        "--max-concurrency", str(args.max_concurrency),
    ]

    if args.sha256:
        cmd.append("--sha256")
    if args.archive_dir:
        cmd.extend(["--archive-dir", str(args.archive_dir)])
    if args.keep_archive:
        cmd.append("--keep-archive")
    if args.region:
        cmd.extend(["--region", args.region])
    if args.profile:
        cmd.extend(["--profile", args.profile])
    if args.max_chunks_per_db:
        cmd.extend(["--max-chunks", str(args.max_chunks_per_db)])

    print("Running:", " ".join(shlex.quote(part) for part in cmd), flush=True)
    return subprocess.run(cmd).returncode


def pending_dbs(pending_dir: Path, pattern: str) -> list[Path]:
    if not pending_dir.exists():
        return []
    return sorted(path.resolve() for path in pending_dir.glob(pattern) if path.is_file())


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be >= 0")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pending-dir", type=Path, default=DEFAULT_PENDING_DIR)
    parser.add_argument("--uploaded-dir", type=Path, default=DEFAULT_UPLOADED_DIR)
    parser.add_argument("--failed-dir", type=Path, default=DEFAULT_FAILED_DIR)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR)
    parser.add_argument("--lock-file", type=Path, default=DEFAULT_LOCK_FILE)
    parser.add_argument("--pattern", default="*.db")

    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--region", default=os.environ.get("AWS_DEFAULT_REGION"))
    parser.add_argument("--profile")
    parser.add_argument("--table", default="audio_frames")
    parser.add_argument("--blob-column", default="payload")
    parser.add_argument("--force-kind", choices=["audio", "current", "blob"], default="audio")

    parser.add_argument("--max-chunk-mb", type=positive_float, default=256.0)
    parser.add_argument("--time-budget-min", type=positive_float, default=30.0)
    parser.add_argument("--stop-margin-seconds", type=positive_float, default=120.0)
    parser.add_argument("--follow", action="store_true", help="Keep polling pending-dir until time budget expires")
    parser.add_argument("--poll-seconds", type=positive_float, default=10.0)
    parser.add_argument("--max-dbs", type=int, help="Maximum pending DBs to process in this run")
    parser.add_argument("--max-chunks-per-db", type=int, help="Forwarded to the row-level uploader")
    parser.add_argument("--move-failed", action="store_true", help="Move a DB to failed-dir on upload command failure")

    parser.add_argument("--archive-dir", type=Path)
    parser.add_argument("--keep-archive", action="store_true")
    parser.add_argument("--sha256", action="store_true")
    parser.add_argument("--multipart-threshold-mb", type=int, default=8)
    parser.add_argument("--multipart-chunk-mb", type=int, default=64)
    parser.add_argument("--max-concurrency", type=int, default=8)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    args.pending_dir.mkdir(parents=True, exist_ok=True)
    args.uploaded_dir.mkdir(parents=True, exist_ok=True)
    args.failed_dir.mkdir(parents=True, exist_ok=True)
    args.state_dir.mkdir(parents=True, exist_ok=True)

    try:
        lock_handle = acquire_lock(args.lock_file)
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 1

    deadline = None
    if args.time_budget_min > 0:
        deadline = time.monotonic() + args.time_budget_min * 60.0

    processed = 0
    uploaded = 0

    try:
        while True:
            dbs = pending_dbs(args.pending_dir, args.pattern)
            if not dbs:
                if not args.follow:
                    print("No pending DBs.")
                    return 0
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= args.stop_margin_seconds:
                    print("Time budget exhausted while waiting for pending DBs.")
                    return 0
                print(f"No pending DBs; sleeping {args.poll_seconds:.1f}s.", flush=True)
                time.sleep(args.poll_seconds)
                continue

            for db_path in dbs:
                if args.max_dbs and processed >= args.max_dbs:
                    print(f"Reached --max-dbs={args.max_dbs}; exiting.")
                    return 0

                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= args.stop_margin_seconds:
                    print(
                        f"Time budget nearly exhausted ({remaining:.0f}s left); "
                        "leaving remaining DBs in pending."
                    )
                    return 0
                remaining_minutes = 0.0 if remaining is None else max(0.0, remaining / 60.0)

                state_db = args.state_dir / f"{db_path.name}.upload_state.db"
                total_rows, total_bytes = count_source_rows(db_path, args.table, args.blob_column)
                done_rows = count_uploaded_rows(
                    state_db, db_path, args.bucket, args.prefix,
                    args.table, args.blob_column,
                )

                print(
                    f"[DB] {db_path.name}: {done_rows}/{total_rows} rows uploaded, "
                    f"{human_bytes(total_bytes)} total payload",
                    flush=True,
                )

                if total_rows == 0 or done_rows >= total_rows:
                    dest = move_with_sidecars(db_path, args.uploaded_dir)
                    print(f"[DB] Complete; moved to {dest}", flush=True)
                    uploaded += 1
                    processed += 1
                    continue

                if args.dry_run:
                    print("[DRY-RUN] Would invoke row-level uploader.")
                    processed += 1
                    continue

                rc = run_live_uploader_for_db(args, db_path, state_db, remaining_minutes)
                processed += 1
                if rc != 0:
                    print(f"[DB] Upload command failed for {db_path} with rc={rc}", file=sys.stderr)
                    if args.move_failed:
                        dest = move_with_sidecars(db_path, args.failed_dir)
                        print(f"[DB] Moved failed DB to {dest}", flush=True)
                    continue

                done_rows = count_uploaded_rows(
                    state_db, db_path, args.bucket, args.prefix,
                    args.table, args.blob_column,
                )
                if total_rows == 0 or done_rows >= total_rows:
                    dest = move_with_sidecars(db_path, args.uploaded_dir)
                    print(f"[DB] Upload complete; moved to {dest}", flush=True)
                    uploaded += 1
                else:
                    print(
                        f"[DB] Partial progress: {done_rows}/{total_rows} rows; "
                        "leaving DB in pending.",
                        flush=True,
                    )

            if not args.follow:
                return 0

    finally:
        try:
            lock_handle.close()
        except Exception:
            pass
        print(f"Session summary: processed={processed}, completed={uploaded}", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
