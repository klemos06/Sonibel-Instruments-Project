from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_ACTIVE_DB = Path("./daq_data/audio_log.db")
DEFAULT_PENDING_DIR = Path("./daq_data/backlog/pending")
DEFAULT_EMPTY_DIR = Path("./daq_data/backlog/empty")
DEFAULT_SEQUENCE_FILE = Path("./daq_data/backlog/session_sequence.txt")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def human_bytes(value: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    n = float(value)
    for unit in units:
        if abs(n) < 1024 or unit == units[-1]:
            return f"{int(n)} {unit}" if unit == "B" else f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} TB"


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

    print(f"Waiting for active DB files to stay unchanged for {stable_seconds:.1f}s...")
    while time.monotonic() < deadline:
        time.sleep(1.0)
        sig = file_signature(watched)
        if sig != last_sig:
            last_sig = sig
            stable_since = time.monotonic()
            continue
        if time.monotonic() - stable_since >= stable_seconds:
            print("Active DB files look stable.")
            return

    raise TimeoutError(f"Active DB files did not stay stable within {timeout_seconds:.1f}s")


def read_next_session_id(sequence_file: Path) -> int:
    try:
        text = sequence_file.read_text(encoding="utf-8").strip()
        value = int(text)
        return max(1, value)
    except (OSError, ValueError):
        return 1


def write_next_session_id(sequence_file: Path, next_id: int) -> None:
    sequence_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = sequence_file.with_suffix(sequence_file.suffix + ".tmp")
    tmp.write_text(f"{next_id}\n", encoding="utf-8")
    with tmp.open("r+b") as handle:
        os.fsync(handle.fileno())
    tmp.replace(sequence_file)


def reserve_session_id(sequence_file: Path) -> int:
    session_id = read_next_session_id(sequence_file)
    write_next_session_id(sequence_file, session_id + 1)
    return session_id


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def inspect_audio_frames(conn: sqlite3.Connection, table: str, blob_column: str) -> dict[str, Any]:
    if not table_exists(conn, table):
        return {
            "row_count": 0,
            "payload_bytes": 0,
            "min_weld_id": None,
            "max_weld_id": None,
            "min_timestamp": None,
            "max_timestamp": None,
        }

    q_table = quote_ident(table)
    q_blob = quote_ident(blob_column)
    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS row_count,
            COALESCE(SUM(length({q_blob})), 0) AS payload_bytes,
            MIN(weld_id) AS min_weld_id,
            MAX(weld_id) AS max_weld_id,
            MIN(timestamp) AS min_timestamp,
            MAX(timestamp) AS max_timestamp
        FROM {q_table}
        WHERE {q_blob} IS NOT NULL
        """
    ).fetchone()
    return {
        "row_count": int(row["row_count"]),
        "payload_bytes": int(row["payload_bytes"]),
        "min_weld_id": row["min_weld_id"],
        "max_weld_id": row["max_weld_id"],
        "min_timestamp": row["min_timestamp"],
        "max_timestamp": row["max_timestamp"],
    }


def checkpoint_and_inspect(db_path: Path, table: str, blob_column: str) -> dict[str, Any]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        quick_check = conn.execute("PRAGMA quick_check").fetchone()[0]
        if quick_check != "ok":
            raise RuntimeError(f"SQLite quick_check failed: {quick_check}")
        return inspect_audio_frames(conn, table, blob_column)
    finally:
        conn.close()


def remove_if_exists(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def move_sidecar(src: Path, dest: Path) -> None:
    if src.exists():
        if dest.exists():
            raise FileExistsError(f"Destination already exists: {dest}")
        shutil.move(str(src), str(dest))


def fsync_dir(path: Path) -> None:
    if os.name == "nt":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def build_session_name(session_id: int, stamp: str, summary: dict[str, Any]) -> str:
    min_weld = summary.get("min_weld_id")
    max_weld = summary.get("max_weld_id")
    if min_weld is not None and max_weld is not None:
        weld_part = f"welds_{int(min_weld):06d}-{int(max_weld):06d}"
    else:
        weld_part = "welds_none"
    return f"session_{session_id:06d}_{stamp}_{weld_part}.db"


def seal_db(args: argparse.Namespace) -> Path | None:
    active_db = args.active_db.expanduser().resolve()
    if not active_db.exists():
        print(f"No active DB found at {active_db}; nothing to seal.")
        return None

    wait_until_stable(active_db, args.wait_until_stable, args.wait_timeout)

    summary = checkpoint_and_inspect(active_db, args.table, args.blob_column)
    row_count = int(summary["row_count"])
    payload_bytes = int(summary["payload_bytes"])

    if row_count == 0 and not args.keep_empty:
        print(f"Active DB has no rows; removing empty DB at {active_db}")
        remove_if_exists(active_db)
        remove_if_exists(Path(str(active_db) + "-wal"))
        remove_if_exists(Path(str(active_db) + "-shm"))
        return None

    session_id = reserve_session_id(args.sequence_file.expanduser().resolve())
    stamp = utc_stamp()
    filename = build_session_name(session_id, stamp, summary)

    dest_dir = args.pending_dir if row_count > 0 else args.empty_dir
    dest_dir = dest_dir.expanduser().resolve()
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_db = dest_dir / filename
    if dest_db.exists():
        raise FileExistsError(f"Destination already exists: {dest_db}")

    print(
        f"Sealing active DB: rows={row_count}, payload={human_bytes(payload_bytes)}, "
        f"session_id={session_id}"
    )
    shutil.move(str(active_db), str(dest_db))
    move_sidecar(Path(str(active_db) + "-wal"), Path(str(dest_db) + "-wal"))
    move_sidecar(Path(str(active_db) + "-shm"), Path(str(dest_db) + "-shm"))

    manifest = {
        "sealed_at_utc": utc_iso(),
        "session_id": session_id,
        "source_active_db": str(active_db),
        "sealed_db": str(dest_db),
        "table": args.table,
        "blob_column": args.blob_column,
        "summary": summary,
        "note": "Database sealed after DAQ service shutdown.",
    }
    manifest_path = Path(str(dest_db) + ".manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    fsync_dir(dest_dir)
    fsync_dir(active_db.parent)

    print(f"Sealed DB: {dest_db}")
    print(f"Manifest:  {manifest_path}")
    return dest_db


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be >= 0")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--active-db", type=Path, default=DEFAULT_ACTIVE_DB)
    parser.add_argument("--pending-dir", type=Path, default=DEFAULT_PENDING_DIR)
    parser.add_argument("--empty-dir", type=Path, default=DEFAULT_EMPTY_DIR)
    parser.add_argument("--sequence-file", type=Path, default=DEFAULT_SEQUENCE_FILE)
    parser.add_argument("--table", default="audio_frames")
    parser.add_argument("--blob-column", default="payload")
    parser.add_argument("--wait-until-stable", type=positive_float, default=3.0)
    parser.add_argument("--wait-timeout", type=positive_float, default=60.0)
    parser.add_argument("--keep-empty", action="store_true")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        seal_db(args)
        return 0
    except (OSError, sqlite3.Error, TimeoutError, RuntimeError) as exc:
        print(f"Seal failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
