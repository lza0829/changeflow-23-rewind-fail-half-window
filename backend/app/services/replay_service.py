"""游标回拨重放窗服务。

执行模型（详见 app.modules.cursor_rewind_window 模块文档）：

1. 建任务前做全部窗口校验（上界必填、只回拨不前进、窗内 seq 均已 Apply、实体键
   边界保护、同流无运行中任务）。
2. 记录回拨前快照（游标、applied_seqs、Sink 全量、隔离区计数），把流游标物理
   回拨到 target_cursor——applied_seqs 保留不动，重放幂等性由 Sink upsert /
   INSERT OR IGNORE 保证。
3. 窗内事件按 seq 逐个重放，每个事件一个事务立即提交；失败即隔离、任务标
   failed，游标停在任务开始前的可恢复点（recoverable_cursor），不抬到窗末。
4. 全部成功后游标复位到回拨前 HWM，并与账本、Sink、游标页交叉核对后写任务结果；
   同时落一份窗完整性证明（window_proof），供 /replay/tasks/{id}/window-proof 只读核验。
   任务持久化在 replay_tasks 表，刷新页面仍可查询。
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
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


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _sink_fingerprint(entry: dict[str, Any] | None) -> dict[str, Any] | None:
    """Sink 行的内容指纹（payload / last_seq / last_event_ts），缺行返回 None。

    ``updated_at`` 不参与：重放覆盖写与“窗口外行未被改动”的判断都不应受它影响。
    ``fingerprint`` 覆盖整行内容，``payload_sha256`` 单独给出 payload 指纹，
    便于“任务前/后载荷指纹”直观对照。
    """
    if entry is None:
        return None
    payload = json.loads(entry.get("payload_json") or "{}")
    return {
        "payload_sha256": _sha256(payload),
        "fingerprint": _sha256(
            {
                "payload": payload,
                "last_seq": int(entry["last_seq"]),
                "last_event_ts": entry["last_event_ts"],
            }
        ),
        "last_seq": int(entry["last_seq"]),
        "last_event_ts": entry["last_event_ts"],
    }


def _out_of_window_sample(
    sink_before: dict[str, dict[str, Any]],
    sink_after: dict[str, dict[str, Any]],
    window_keys: set[str],
    *,
    limit: int = 3,
) -> tuple[list[dict[str, Any]], list[str], int]:
    """窗外键的任务前/后指纹抽样，至少 1 个（有窗外键时）。

    返回（抽样列表, 实际被改动的窗外键, 窗外键总数）。抽样按 entity_key 排序取
    前 ``limit`` 个，保证同一任务重复读取时结果稳定。
    """
    outside_keys = sorted(k for k in (set(sink_before) | set(sink_after)) if k not in window_keys)
    changed = [
        k
        for k in outside_keys
        if _sink_fingerprint(sink_before.get(k)) != _sink_fingerprint(sink_after.get(k))
    ]
    sample = [
        {
            "entity_key": key,
            "before": _sink_fingerprint(sink_before.get(key)),
            "after": _sink_fingerprint(sink_after.get(key)),
            "unchanged": key not in changed,
        }
        for key in outside_keys[: max(1, limit)]
    ]
    return sample, changed, len(outside_keys)


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
        # 窗内已触达实体键 → 本任务对它的逐事件处置（证明用，成功提交才记）
        touched: dict[str, list[dict[str, Any]]] = {}

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
                touched.setdefault(event["entity_key"], []).append(
                    {"seq": seq, "op": event["op"], "disposition": disposition}
                )
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
                touched.setdefault(event["entity_key"], []).append(
                    {"seq": seq, "op": event["op"], "disposition": DISPOSITION_REPLAY_QUARANTINED}
                )
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
                    "recoverable_cursor": cursor,
                    "recovery_hint": (
                        f"游标停在可恢复点 {cursor}（窗末为 {window_end_seq}，耐久高水位仍为 "
                        f"{hwm}）。修复后新建重放任务 target_cursor={cursor}, "
                        f"window_end_seq={window_end_seq} 即可续放，成功后游标复位到高水位 {hwm}。"
                    ),
                }
                break

        report = _build_report(
            conn,
            stream_id=stream_id,
            task_id=task_id,
            window_start=target_cursor + 1,
            target_cursor=target_cursor,
            window_end_seq=window_end_seq,
            cursor_before=cursor_before,
            restore_cursor=hwm,
            sink_before=sink_before,
            applied_before=applied_before,
            quarantine_before=quarantine_before,
            window_keys={e["entity_key"] for e in window_events},
            touched=touched,
            replayed=replayed,
            skipped=skipped,
            quarantined=quarantined,
            last_processed_seq=last_processed_seq,
            failure=failure,
            fail_at_seq=fail_at_seq,
        )

        if failure is None:
            # 全窗成功：游标复位到耐久高水位（applied_seqs 的 max seq）
            streams_repo.set_cursor(conn, stream_id, hwm)
            cursor_after = hwm
            status = "succeeded"
            recoverable_cursor = hwm
        else:
            # 失败：游标停在最后一个已确认的可恢复点，绝不标成功、绝不抬到窗末。
            # recoverable_cursor 必为“任务开始前可恢复点”语义：它要么是 target_cursor
            # （一个事件都没成功），要么是最后一个成功提交的 seq。
            recoverable_cursor = failure["recover_point"]
            streams_repo.set_cursor(conn, stream_id, recoverable_cursor)
            cursor_after = recoverable_cursor
            status = "failed"

        replay_repo.finish_task(
            conn,
            task_id,
            status=status,
            cursor_after=cursor_after,
            report=report,
            window_proof=report.get("window_proof"),
        )
        conn.commit()

        # 提交后复核游标页：流游标必须已落在任务声明的最终位置，否则视为内部错误
        landed = int(streams_repo.get_stream(conn, stream_id)["apply_cursor"])
        if landed != cursor_after:
            raise RuntimeError(
                f"游标复核失败：期望 {cursor_after}，实际 {landed}，任务 {task_id}"
            )
        # 落地后复核：任务状态与窗完成标志必须一致（failed 不得声称窗完成）
        task = replay_repo.get_task(conn, task_id) or {}
        proof = task.get("window_proof") or {}
        if (task.get("status") == "failed") and proof.get("window_completed"):
            raise RuntimeError(f"窗完整性证明自相矛盾：failed 任务却标记 window_completed，任务 {task_id}")
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
    window_start: int,
    target_cursor: int,
    window_end_seq: int,
    cursor_before: int,
    restore_cursor: int,
    sink_before: dict[str, dict[str, Any]],
    applied_before: set[int],
    quarantine_before: int,
    window_keys: set[str],
    touched: dict[str, list[dict[str, Any]]],
    replayed: int,
    skipped: int,
    quarantined: int,
    last_processed_seq: int | None,
    failure: dict[str, Any] | None,
    fail_at_seq: int | None,
) -> dict[str, Any]:
    """结束交叉核对：账本 / Sink / applied_seqs / 游标页 / 隔离区 + 窗完整性证明。"""
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

    # 3. Sink：窗内键逐键核对；窗外键禁止被本任务改动（附抽样指纹）
    sink_after = _sink_snapshot(conn, stream_id)
    outside_sample, outside_changed, outside_count = _out_of_window_sample(
        sink_before, sink_after, window_keys
    )
    checks["sink_outside_window"] = {
        "outside_key_count": outside_count,
        "changed_keys": outside_changed,
        "sample": outside_sample,
    }
    checks["sink_outside_window_untouched"] = not outside_changed

    expected_sink = dict(sink_before)
    if failure is None:
        # 成功重放：窗内键最终态必须等于重放前（覆盖写收敛，仅 updated_at 变化）
        window_converged = all(sink_after.get(k) == sink_before.get(k) for k in window_keys)
        checks["sink_window_converged"] = window_converged
        checks["sink_identical_to_before"] = sink_after == expected_sink
    else:
        # 失败：只核对窗外未动；窗内停在 recover_point，交由恢复任务收敛。
        # 注意这并不等于“窗完成”——final_cursor 仍停在可恢复点，window_completed=False。
        checks["sink_window_converged"] = None
        checks["sink_identical_to_before"] = None

    # 4. 游标页：核对任务声明的最终游标（成功=耐久 HWM，失败=recover_point）；
    #    本报告在最终 set_cursor 之前构建，故先记录期望值，复位后由调用方落地。
    recoverable_cursor = restore_cursor if failure is None else int(failure["recover_point"])
    stream = streams_repo.get_stream(conn, stream_id)
    checks["cursor_page"] = {
        "stream_apply_cursor_now": int(stream["apply_cursor"]),
        "expected_after": recoverable_cursor,
        "note": "成功后流游标复位到 applied_seqs 高水位；失败停在 recover_point（不得抬到窗末）",
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

    window_completed = failure is None and last_processed_seq == window_end_seq
    proof = _build_window_proof(
        task_id=task_id,
        window_start=window_start,
        window_end_seq=window_end_seq,
        failure=failure,
        fail_at_seq=fail_at_seq,
        recoverable_cursor=recoverable_cursor,
        cursor_before=cursor_before,
        target_cursor=target_cursor,
        window_completed=window_completed,
        touched=touched,
        checks=checks,
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
        "window_completed": window_completed,
        "failure": failure,
        "window_proof": proof,
        "cross_checks": checks,
        "cross_checks_passed": all_ok,
        "idempotency_policy": (
            "upsert 按 (stream_id, entity_key) 整体覆盖；delete 幂等，键缺失记 skipped；"
            "applied_seqs INSERT OR IGNORE 保持不变；ledger 只追加；"
            "窗外 Sink 行与 applied_seqs 禁止改动（见 cross_checks）"
        ),
    }


def _build_window_proof(
    *,
    task_id: int,
    window_start: int,
    window_end_seq: int,
    failure: dict[str, Any] | None,
    fail_at_seq: int | None,
    recoverable_cursor: int,
    cursor_before: int,
    target_cursor: int,
    window_completed: bool,
    touched: dict[str, list[dict[str, Any]]],
    checks: dict[str, Any],
) -> dict[str, Any]:
    """本次重放的窗完整性证明：窗边界 / 失败点 / 可恢复游标 / 触达键 / 窗外抽样。

    ``out_of_window_untouched_sample`` 为窗外键的任务前/后载荷指纹（至少 1 个，
    有窗外键时），``verified_untouched`` 是逐键指纹比对结论；``window_inside_clean``
    用于说明“失败任务绝不把半窗脏写说成窗完成”——失败时该值为 False。
    """
    outside_sample = checks["sink_outside_window"]["sample"]
    return {
        "task_id": int(task_id),  # 证明与任务一一对应，便于按 task_id 只读核验
        "window_start": int(window_start),
        "window_end_seq": int(window_end_seq),
        "target_cursor": int(target_cursor),
        "cursor_before": int(cursor_before),
        "failure_seq": None if failure is None else int(failure["seq"]),
        "fail_at_seq": None if fail_at_seq is None else int(fail_at_seq),
        "recoverable_cursor": int(recoverable_cursor),
        "window_completed": bool(window_completed),
        "window_inside_clean": bool(window_completed),
        "in_window_touched_keys": sorted(touched),
        "in_window_touched_events": {k: v for k, v in sorted(touched.items())},
        "out_of_window_untouched_sample": outside_sample,
        "out_of_window_untouched_verified": checks["sink_outside_window_untouched"],
        "out_of_window_key_count": checks["sink_outside_window"]["outside_key_count"],
        "out_of_window_changed_keys": checks["sink_outside_window"]["changed_keys"],
        "window_keys_in_sink_converged": checks["sink_window_converged"],
        "cross_checks_passed": (
            checks["ledger"]["matches_counters"]
            and checks["applied_seqs"]["unchanged"]
            and checks["sink_outside_window_untouched"]
            and checks["quarantine"]["matches_counter"]
            and (failure is not None or checks["sink_window_converged"] is True)
        ),
        "generated_at": _now(),
        "proof_policy": (
            "in_window_touched_keys 只含本任务实际处理过的事件键（成功提交/隔离）；"
            "out_of_window_untouched_sample 至少含 1 个窗外键的任务前/后载荷指纹；"
            "失败任务 recoverable_cursor 停在任务开始前可恢复点且 window_completed=False"
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


def get_window_proof(task_id: int) -> dict[str, Any] | None:
    """只读核验接口（按 task_id）：返回该任务的窗完整性证明。

    证明在建任务结束时随任务一起落库（replay_tasks.window_proof_json），这里只读
    不回写。历史任务（迁库前）没有证明，返回 ``available=False`` 而不伪造数据。

    返回结构与页面三块一一对应：

    * ``in_window_touched_keys`` —— 窗内已触达
    * ``out_of_window_untouched_sample`` —— 窗外未改（含至少 1 个键的任务前/后载荷指纹）
    * ``recoverable_cursor`` / ``failure_seq`` / ``window_completed`` —— 可恢复游标与失败点
    """
    conn = connect()
    try:
        task = replay_repo.get_task(conn, task_id)
        if not task:
            return None
        proof = task.get("window_proof")
        if not proof:
            return {
                "available": False,
                "task_id": int(task_id),
                "status": task["status"],
                "cursor_after": int(task["cursor_after"]),
                "reason": "该任务没有落库的窗完整性证明（历史任务或非回拨重放任务）",
            }
        result = dict(proof)
        result["available"] = True
        # 证明与任务行同源核对：页面三块数字都从这里取，不另算一遍
        result["task_status"] = task["status"]
        result["cursor_after_task_row"] = int(task["cursor_after"])
        result["matches_task_row"] = (
            proof.get("recoverable_cursor") == int(task["cursor_after"])
            and proof.get("window_end_seq") == int(task["window_end_seq"])
            and proof.get("window_start") == int(task["target_cursor"]) + 1
            and (
                proof.get("window_completed") is False
                if task["status"] == "failed"
                else proof.get("window_completed") is True
            )
        )
        return result
    finally:
        conn.close()
