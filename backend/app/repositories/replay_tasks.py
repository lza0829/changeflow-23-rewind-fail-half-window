from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

_COLUMNS = (
    "id, stream_id, status, target_cursor, window_end_seq, cursor_before, cursor_after, "
    "candidate_count, replayed_count, skipped_count, quarantined_count, last_processed_seq, "
    "fail_at_seq, report_json, created_at, updated_at"
)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    item["report"] = json.loads(item.pop("report_json") or "{}")
    return item


def create_task(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    target_cursor: int,
    window_end_seq: int,
    cursor_before: int,
    candidate_count: int,
    fail_at_seq: int | None,
) -> int:
    now = _now()
    cur = conn.execute(
        """
        INSERT INTO replay_tasks(
            stream_id, status, target_cursor, window_end_seq, cursor_before, cursor_after,
            candidate_count, replayed_count, skipped_count, quarantined_count,
            last_processed_seq, fail_at_seq, report_json, created_at, updated_at
        ) VALUES (?, 'running', ?, ?, ?, ?, ?, 0, 0, 0, NULL, ?, '{}', ?, ?)
        """,
        (
            stream_id,
            target_cursor,
            window_end_seq,
            cursor_before,
            cursor_before,
            candidate_count,
            fail_at_seq,
            now,
            now,
        ),
    )
    return int(cur.lastrowid)


def update_progress(
    conn: sqlite3.Connection,
    task_id: int,
    *,
    replayed: int,
    skipped: int,
    quarantined: int,
    last_processed_seq: int | None,
    cursor_after: int,
) -> None:
    conn.execute(
        """
        UPDATE replay_tasks
        SET replayed_count=?, skipped_count=?, quarantined_count=?,
            last_processed_seq=?, cursor_after=?, updated_at=?
        WHERE id=?
        """,
        (replayed, skipped, quarantined, last_processed_seq, cursor_after, _now(), task_id),
    )


def finish_task(
    conn: sqlite3.Connection, task_id: int, *, status: str, cursor_after: int, report: dict[str, Any]
) -> None:
    conn.execute(
        """
        UPDATE replay_tasks
        SET status=?, cursor_after=?, report_json=?, updated_at=?
        WHERE id=?
        """,
        (status, cursor_after, json.dumps(report, ensure_ascii=False), _now(), task_id),
    )


def get_task(conn: sqlite3.Connection, task_id: int) -> dict[str, Any] | None:
    row = conn.execute(f"SELECT {_COLUMNS} FROM replay_tasks WHERE id=?", (task_id,)).fetchone()
    return _row_to_dict(row) if row else None


def list_tasks(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"SELECT {_COLUMNS} FROM replay_tasks ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def count_running_for_stream(conn: sqlite3.Connection, stream_id: int) -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) c FROM replay_tasks WHERE stream_id=? AND status='running'",
            (stream_id,),
        ).fetchone()["c"]
    )
