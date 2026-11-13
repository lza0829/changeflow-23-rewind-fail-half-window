from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def is_applied(conn: sqlite3.Connection, stream_id: int, seq: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM applied_seqs WHERE stream_id=? AND seq=?",
        (stream_id, seq),
    ).fetchone()
    return row is not None


def mark_applied(conn: sqlite3.Connection, stream_id: int, seq: int, run_id: int) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO applied_seqs(stream_id, seq, run_id, applied_at)
        VALUES (?,?,?,?)
        """,
        (stream_id, seq, run_id, _now()),
    )


def high_water_mark(conn: sqlite3.Connection, stream_id: int) -> int:
    """已成功 Apply 的 seq 高水位（applied_seqs 的最大 seq）。

    重放期间流游标 ``streams.apply_cursor`` 会被临时回拨，不能当作高水位；
    applied_seqs 在重放中保持不变，才是判断“哪些事件可以重放”的耐久依据。
    """
    row = conn.execute(
        "SELECT COALESCE(MAX(seq), 0) AS hwm FROM applied_seqs WHERE stream_id=?",
        (stream_id,),
    ).fetchone()
    return int(row["hwm"] or 0)


def list_cursor_view(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT s.id AS stream_id, s.name, s.apply_cursor,
               (SELECT COUNT(*) FROM applied_seqs a WHERE a.stream_id=s.id) AS applied_seq_count,
               (SELECT MAX(seq) FROM applied_seqs a WHERE a.stream_id=s.id) AS max_applied_seq
        FROM streams s
        ORDER BY s.id
        """
    ).fetchall()
    return [dict(r) for r in rows]
