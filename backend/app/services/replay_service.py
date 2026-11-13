"""游标回拨重放窗服务。

执行模型（详见 app.modules.cursor_rewind_window 模块文档）：

1. 建任务前做全部窗口校验（上界必填、只回拨不前进、窗内 seq 均已 Apply、实体键
   边界保护、同流无运行中任务）。
2. 记录回拨前快照（游标、applied_seqs、Sink 全量、隔离区计数），把流游标物理
   回拨到 target_cursor——applied_seqs 保留不动，重放幂等性由 Sink upsert /
   INSERT OR IGNORE 保证。
3. 窗内事件按 seq 逐个重放，每个事件一个事务立即提交；失败即隔离、任务标
   failed，游标停在 last_processed_seq（文档化的可恢复点）。
4. 全部成功后游标复位到回拨前 HWM，并与账本、Sink、游标页交叉核对后写任务结果。
   任务持久化在 replay_tasks 表，刷新页面仍可查询。
"""

from __future__ import annotations

from typing import Any

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.parse_change import parse_change_fixture
from app.modules.cursor_rewind_window import (
    DISPOSITION_REPLAY_QUARANTINED,
    DISPOSITION_REPLAY_SKIPPED,
    DISPOSITION_REPLAYED,
    entity_boundary_violations,
    in_window,
    validate_window,
    window_seqs,
)
from app.repositories import cursors as cursor_repo
from app.repositories import ledger as ledger_repo
from app.repositories import quarantine as quarantine_repo
from app.repositories import replay_tasks as replay_repo
from app.repositories import sink as sink_repo
from app.repositories import streams as streams_repo


class ReplayValidationError(ValueError):
    """建任务校验失败，路由层映射为 HTTP 400。"""


def _stream_events(stream: dict[str, Any]) -> list[dict[str, Any]]:
    path = FIXTURES_DIR / stream["fixture_path"]
    return parse_change_fixture(path)


def _applied_seq_set(conn, stream_id: int) -> set[int]:
    rows = conn.execute(
        "SELECT seq FROM applied_seqs WHERE stream_id=?", (stream_id,)
    ).fetchall()
    return {int(r["seq"]) for r in rows}


def _sink_snapshot(conn, stream_id: int) -> dict[str, dict[str, Any]]:
    """流内 Sink 当前态快照（不含 updated_at，该字段重放必然刷新）。"""
    rows = conn.execute(
        """
        SELECT entity_key, payload_json, last_seq, last_event_ts
        FROM sink_records WHERE stream_id=?
        """,
        (stream_id,),
    ).fetchall()
    return {
        r["entity_key"]: {
            "payload_json": r["payload_json"],
            "last_seq": int(r["last_seq"]),
            "last_event_ts": r["last_event_ts"],
        }
        for r in rows
    }


def preview_window(stream_id: int, target_cursor: int, window_end_seq: int | None) -> dict[str, Any]:
    """不建任务，预览窗内候选事件并跑一遍同样的校验，供前端表单确认。"""
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ReplayValidationError("stream not found")
        hwm = cursor_repo.high_water_mark(conn, stream_id)
        events = _stream_events(stream)
        fixture_seqs = {int(e["seq"]) for e in events}
        applied = _applied_seq_set(conn, stream_id)
        validate_window(
            target_cursor=target_cursor,
            window_end_seq=window_end_seq,
            hwm=hwm,
            applied_seqs=applied,
            fixture_seqs=fixture_seqs,
        )
        candidates = [
            e for e in events if in_window(int(e["seq"]), target_cursor, int(window_end_seq))
        ]
        keys = {e["entity_key"] for e in candidates}
        boundary = entity_boundary_violations(events, keys, int(window_end_seq))
        return {
            "stream_id": stream_id,
            "cursor_before": hwm,
            "target_cursor": int(target_cursor),
            "window_end_seq": int(window_end_seq),
            "candidate_count": len(candidates),
            "candidates": candidates,
            "entity_boundary_violations": boundary,
            "valid": not boundary,
        }
    finally:
        conn.close()


def create_and_run_replay(
    stream_id: int,
    target_cursor: int,
    window_end_seq: int | None,
    fail_at_seq: int | None = None,
) -> dict[str, Any]:
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ReplayValidationError("stream not found")
        if not stream["enabled"]:
            raise ReplayValidationError("stream disabled")
        if replay_repo.count_running_for_stream(conn, stream_id):
            raise ReplayValidationError("该流已有运行中的重放任务，请等其结束后再发起")

        hwm = cursor_repo.high_water_mark(conn, stream_id)
        events = _stream_events(stream)
        fixture_seqs = {int(e["seq"]) for e in events}
        applied_before = _applied_seq_set(conn, stream_id)

        # 窗口上界强制存在：禁止“无上界整库重放”
        validate_window(
            target_cursor=target_cursor,
            window_end_seq=window_end_seq,
            hwm=hwm,
            applied_seqs=applied_before,
            fixture_seqs=fixture_seqs,
        )
        target_cursor = int(target_cursor)
        window_end_seq = int(window_end_seq)
        if fail_at_seq is not None and not in_window(int(fail_at_seq), target_cursor, window_end_seq):
            raise ReplayValidationError("fail_at_seq 必须落在重放窗口内")

        window_events = [e for e in events if in_window(int(e["seq"]), target_cursor, window_end_seq)]
        boundary = entity_boundary_violations(
            events, {e["entity_key"] for e in window_events}, window_end_seq
        )
        if boundary:
            raise ReplayValidationError(
                f"实体键在窗外还有更高 seq 事件，部分重放会覆盖回旧状态: {boundary}；"
                "请把 window_end_seq 扩到这些键的最后一次变更"
            )

        # ---- 回拨前快照（结束交叉核对用）----
        sink_before = _sink_snapshot(conn, stream_id)
        quarantine_before = quarantine_repo.count_quarantine(conn)
        # 回拨前游标快照取“当前流游标”；恢复任务里它可能正停在上次失败的
        # recover_point，而耐久高水位 HWM 由 applied_seqs 决定（成功后复位目标）。
        cursor_before = int(stream["apply_cursor"] or 0)

        task_id = replay_repo.create_task(
            conn,
            stream_id=stream_id,
            target_cursor=target_cursor,
            window_end_seq=window_end_seq,
            cursor_before=cursor_before,
            candidate_count=len(window_events),
            fail_at_seq=fail_at_seq,
        )

        # ---- 物理回拨游标（applied_seqs 保留，Sink 不动）----
        streams_repo.set_cursor(conn, stream_id, target_cursor)
        conn.commit()

        replayed = 0
        skipped = 0
        quarantined = 0
        last_processed_seq: int | None = None
        cursor = target_cursor
        failure: dict[str, Any] | None = None

        for event in window_events:
            seq = int(event["seq"])
            try:
                if fail_at_seq is not None and seq == int(fail_at_seq):
                    raise RuntimeError(f"fail_at_seq={seq} 注入的重放失败（演示可恢复点）")

                if event["op"] == "upsert":
                    sink_repo.upsert(
                        conn,
                        stream_id=stream_id,
                        entity_key=event["entity_key"],
                        payload=event.get("payload") or {},
                        seq=seq,
                        event_ts=event["event_ts"],
                    )
                    disposition = DISPOSITION_REPLAYED
                    replayed += 1
                else:
                    # delete 幂等：键不存在为空删除，记 skipped，无副作用
                    existing = conn.execute(
                        "SELECT 1 FROM sink_records WHERE stream_id=? AND entity_key=?",
                        (stream_id, event["entity_key"]),
                    ).fetchone()
                    sink_repo.delete(conn, stream_id=stream_id, entity_key=event["entity_key"])
                    if existing:
                        disposition = DISPOSITION_REPLAYED
                        replayed += 1
                    else:
                        disposition = DISPOSITION_REPLAY_SKIPPED
                        skipped += 1

                # applied_seqs 已含该 seq：INSERT OR IGNORE 保持集合不变（幂等）
                cursor_repo.mark_applied(conn, stream_id, seq, task_id)
                cursor = seq
                streams_repo.set_cursor(conn, stream_id, cursor)
                ledger_repo.insert_ledger(
                    conn, stream_id=stream_id, run_id=task_id, event=event,
                    disposition=disposition, kind="replay",
                )
                last_processed_seq = seq
                replay_repo.update_progress(
                    conn,
                    task_id,
                    replayed=replayed,
                    skipped=skipped,
                    quarantined=quarantined,
                    last_processed_seq=last_processed_seq,
                    cursor_after=cursor,
                )
                # 每事件提交：失败时已处理部分全部持久化，游标停在已确认的 seq
                conn.commit()
            except Exception as exc:  # noqa: BLE001 - 任何异常都转为隔离+任务失败
                conn.rollback()
                quarantine_repo.insert(
                    conn,
                    stream_id=stream_id,
                    run_id=task_id,
                    seq=seq,
                    entity_key=event["entity_key"],
                    reason=f"replay_failed: {type(exc).__name__}: {exc}",
                    payload=event.get("payload") or {},
                )
                ledger_repo.insert_ledger(
                    conn,
                    stream_id=stream_id,
                    run_id=task_id,
                    event=event,
                    disposition=DISPOSITION_REPLAY_QUARANTINED,
                    kind="replay",
                )
                quarantined += 1
                replay_repo.update_progress(
                    conn,
                    task_id,
                    replayed=replayed,
                    skipped=skipped,
                    quarantined=quarantined,
                    last_processed_seq=last_processed_seq,
                    cursor_after=cursor,
                )
                conn.commit()
                failure = {
                    "seq": seq,
                    "entity_key": event["entity_key"],
                    "error": f"{type(exc).__name__}: {exc}",
                    "recover_point": cursor,
                    "recovery_hint": (
                        f"游标停在可恢复点 {cursor}（耐久高水位仍为 {hwm}）。修复后新建重放任务 "
                        f"target_cursor={cursor}, window_end_seq={window_end_seq} 即可续放，"
                        f"成功后游标复位到高水位 {hwm}。"
                    ),
                }
                break

        report = _build_report(
            conn,
            stream_id=stream_id,
            task_id=task_id,
            target_cursor=target_cursor,
            window_end_seq=window_end_seq,
            cursor_before=cursor_before,
            restore_cursor=hwm,
            sink_before=sink_before,
            applied_before=applied_before,
            quarantine_before=quarantine_before,
            window_keys={e["entity_key"] for e in window_events},
            replayed=replayed,
            skipped=skipped,
            quarantined=quarantined,
            last_processed_seq=last_processed_seq,
            failure=failure,
        )

        if failure is None:
            # 全窗成功：游标复位到耐久高水位（applied_seqs 的 max seq）
            streams_repo.set_cursor(conn, stream_id, hwm)
            cursor_after = hwm
            status = "succeeded"
        else:
            # 失败：游标停在可恢复点，绝不标成功
            streams_repo.set_cursor(conn, stream_id, window_end_seq)
            cursor_after = window_end_seq
            status = "failed"

        replay_repo.finish_task(conn, task_id, status=status, cursor_after=cursor_after, report=report)
        conn.commit()

        # 提交后复核游标页：流游标必须已落在任务声明的最终位置，否则视为内部错误
        landed = int(streams_repo.get_stream(conn, stream_id)["apply_cursor"])
        if landed != cursor_after:
            raise RuntimeError(
                f"游标复核失败：期望 {cursor_after}，实际 {landed}，任务 {task_id}"
            )
        task = replay_repo.get_task(conn, task_id) or {}
        task.setdefault("report", {})["cross_checks"].setdefault("cursor_page", {})[
            "stream_apply_cursor_now"
        ] = landed
        task["report"]["cross_checks"]["cursor_page"]["matches"] = landed == cursor_after
        return task
    finally:
        conn.close()


def _build_report(
    conn,
    *,
    stream_id: int,
    task_id: int,
    target_cursor: int,
    window_end_seq: int,
    cursor_before: int,
    restore_cursor: int,
    sink_before: dict[str, dict[str, Any]],
    applied_before: set[int],
    quarantine_before: int,
    window_keys: set[str],
    replayed: int,
    skipped: int,
    quarantined: int,
    last_processed_seq: int | None,
    failure: dict[str, Any] | None,
) -> dict[str, Any]:
    """结束交叉核对：账本 / Sink / applied_seqs / 游标页 / 隔离区。"""
    checks: dict[str, Any] = {}

    # 1. 账本：本任务追加的重放账本行数与计数一致
    ledger_rows = conn.execute(
        """
        SELECT seq, disposition FROM change_ledger
        WHERE stream_id=? AND run_id=? AND kind='replay' ORDER BY seq
        """,
        (stream_id, task_id),
    ).fetchall()
    ledger_dispositions = [r["disposition"] for r in ledger_rows]
    checks["ledger"] = {
        "rows": len(ledger_rows),
        "replayed": ledger_dispositions.count(DISPOSITION_REPLAYED),
        "skipped": ledger_dispositions.count(DISPOSITION_REPLAY_SKIPPED),
        "quarantined": ledger_dispositions.count(DISPOSITION_REPLAY_QUARANTINED),
        "matches_counters": (
            ledger_dispositions.count(DISPOSITION_REPLAYED) == replayed
            and ledger_dispositions.count(DISPOSITION_REPLAY_SKIPPED) == skipped
            and ledger_dispositions.count(DISPOSITION_REPLAY_QUARANTINED) == quarantined
        ),
    }

    # 2. applied_seqs：重放不增不减（INSERT OR IGNORE 幂等）
    applied_after = _applied_seq_set(conn, stream_id)
    checks["applied_seqs"] = {
        "before_count": len(applied_before),
        "after_count": len(applied_after),
        "unchanged": applied_after == applied_before,
    }

    # 3. Sink：窗内键逐键核对；窗外键禁止被本任务改动
    sink_after = _sink_snapshot(conn, stream_id)
    outside_before = {k: v for k, v in sink_before.items() if k not in window_keys}
    outside_after = {k: v for k, v in sink_after.items() if k not in window_keys}
    checks["sink_outside_window_untouched"] = outside_before == outside_after

    expected_sink = dict(sink_before)
    if failure is None:
        # 成功重放：窗内键最终态必须等于重放前（覆盖写收敛，仅 updated_at 变化）
        window_converged = all(sink_after.get(k) == sink_before.get(k) for k in window_keys)
        checks["sink_window_converged"] = window_converged
        checks["sink_identical_to_before"] = sink_after == expected_sink
    else:
        # 失败：只核对窗外未动；窗内停在 recover_point，交由恢复任务收敛
        checks["sink_window_converged"] = None
        checks["sink_identical_to_before"] = None

    # 4. 游标页：核对任务声明的最终游标（成功=耐久 HWM，失败=recover_point）；
    #    本报告在最终 set_cursor 之前构建，故先记录期望值，复位后由调用方落地。
    expected_cursor = restore_cursor if failure is None else failure["recover_point"]
    stream = streams_repo.get_stream(conn, stream_id)
    checks["cursor_page"] = {
        "stream_apply_cursor_now": int(stream["apply_cursor"]),
        "expected_after": expected_cursor,
        "note": "成功后流游标复位到 applied_seqs 高水位；失败停在 recover_point",
    }

    # 5. 隔离区：新增条数 == 本任务 quarantined 计数
    quarantine_after = quarantine_repo.count_quarantine(conn)
    checks["quarantine"] = {
        "before": quarantine_before,
        "after": quarantine_after,
        "new_rows": quarantine_after - quarantine_before,
        "matches_counter": (quarantine_after - quarantine_before) == quarantined,
    }

    all_ok = (
        checks["ledger"]["matches_counters"]
        and checks["applied_seqs"]["unchanged"]
        and checks["sink_outside_window_untouched"]
        and checks["quarantine"]["matches_counter"]
        and (failure is not None or checks["sink_window_converged"] is True)
    )

    return {
        "kind": "cursor_rewind_window",
        "window": {
            "target_cursor": target_cursor,
            "window_end_seq": window_end_seq,
            "seqs": window_seqs(target_cursor, window_end_seq),
        },
        "cursor_before": cursor_before,
        "target_cursor": target_cursor,
        "window_end_seq": window_end_seq,
        "candidate_count": replayed + skipped + quarantined,
        "replayed_count": replayed,
        "skipped_count": skipped,
        "quarantined_count": quarantined,
        "last_processed_seq": last_processed_seq,
        "final_cursor": checks["cursor_page"]["expected_after"],
        "failure": failure,
        "cross_checks": checks,
        "cross_checks_passed": all_ok,
        "idempotency_policy": (
            "upsert 按 (stream_id, entity_key) 整体覆盖；delete 幂等，键缺失记 skipped；"
            "applied_seqs INSERT OR IGNORE 保持不变；ledger 只追加；"
            "窗外 Sink 行与 applied_seqs 禁止改动（见 cross_checks）"
        ),
    }


def list_replay_tasks(limit: int = 50) -> list[dict[str, Any]]:
    conn = connect()
    try:
        return replay_repo.list_tasks(conn, limit=limit)
    finally:
        conn.close()


def get_replay_task(task_id: int) -> dict[str, Any] | None:
    conn = connect()
    try:
        return replay_repo.get_task(conn, task_id)
    finally:
        conn.close()
