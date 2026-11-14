"""游标回拨重放窗（cursor_rewind_window）。

场景
----
流游标 apply_cursor 被正常 Apply 推高后，偶发需要在一个**有界 seq 窗口**内把游标
回拨、让窗内事件重新走一遍 Apply（修正错误 payload、补回被误删的实体等）。

本模块**不是**乱序到达缓冲（out_of_order_buffer），**不是**快照追平
（snapshot_catchup_race）：窗口里的事件都是历史上已经成功 Apply 过的事件，按 seq
顺序原地重放，不接收新事件，不追平外部快照。

硬约束
------
1. 重放任务必须显式给出 ``target_cursor``（回拨到的游标，等价于“从 target+1 开始
   重放”）与 ``window_end_seq``（窗口上界，含）。**禁止无上界整库重放**——服务层
   在入参处拒绝缺失上界的请求。
2. 仅 ``target_cursor < seq <= window_end_seq`` 的窗内事件会再次 Apply；窗外
   （seq <= target 或 seq > window_end）的事件、Sink 行、applied_seqs 一律不允许被
   本任务改动（服务层在结束时用窗外快照交叉核对）。
3. 游标是“先回拨、按序推进、成功后复位”的临时状态：

   .. code-block:: text

       cursor_before(=HWM) ──回拨──> target_cursor
                                   ──窗内逐 seq 推进──> last_processed_seq
                                   ──全窗成功后复位──> cursor_before(=HWM)

   中途失败则游标停在**任务开始前的可恢复点**（``recoverable_cursor``：窗内一个事件
   都没成功时就是 ``target_cursor``，否则是最后一个已确认 seq），任务标 ``failed``，
   绝不允许半窗脏数据却显示成功，也绝不把游标抬到 ``window_end_seq``。

幂等 / 覆盖策略
---------------
* ``upsert``：按 ``(stream_id, entity_key)`` 覆盖写（ON CONFLICT DO UPDATE），
  payload / last_seq / last_event_ts 整体替换。同一实体键重放后 Sink 仍按
  entity_key 唯一可解释，不会产生重复行。
* ``delete``：按实体键幂等删除；键已不存在则记 ``replay_skipped``（空删除，无副作用）。
* ``applied_seqs`` 以 ``(stream_id, seq)`` 为主键 ``INSERT OR IGNORE``：重放不新增、
  不删除 seq 记录，applied_seq_count 与 max_applied_seq 重放前后保持不变。
* ``change_ledger`` 只追加：每次重放追加一条 disposition 为
  ``replayed`` / ``replay_skipped`` / ``replay_quarantined`` 的账本行，历史账本不
  改写。
* 实体键边界：若某个 entity_key 在窗外（seq > window_end_seq）还有更新的事件，
  拒绝建任务——部分重放该键会把它从高 seq 的新状态覆盖回旧状态。合法重放的最终
  Sink 与重放前逐行一致（仅 updated_at 时间戳变化），即“重放收敛、不越放越重复”。

可恢复点（失败语义）
--------------------
每个窗内事件处理完立即提交（Sink / 游标 / 账本 / applied_seqs 同事务），因此
``last_processed_seq`` 与流游标始终一致。失败时：

* 出错事件进隔离区（disposition=replay_quarantined，reason 含异常信息）；
* 流游标停在可恢复点（``last_processed_seq``；一个事件都没成功则为 ``target_cursor``），
  是一个已确认的 seq 而非悬空位置，且不得抬到窗末；
* 任务 status=failed，窗完整性证明 ``window_completed=false``，报告给出
  recover_point 与“如何恢复”：修复后新建一个 ``[recover_point, window_end_seq]``
  的重放任务，成功后游标复位到原 HWM。
"""

from __future__ import annotations

from typing import Any

# 账本 disposition 取值，集中定义避免服务层散落魔法字符串
DISPOSITION_REPLAYED = "replayed"
DISPOSITION_REPLAY_SKIPPED = "replay_skipped"
DISPOSITION_REPLAY_QUARANTINED = "replay_quarantined"


def window_seqs(target_cursor: int, window_end_seq: int) -> list[int]:
    """回拨窗覆盖的 seq 集合（含上界，不含 target_cursor 本身）。"""
    return list(range(int(target_cursor) + 1, int(window_end_seq) + 1))


def in_window(seq: int, target_cursor: int, window_end_seq: int) -> bool:
    """``seq`` 是否落在回拨窗 ``(target_cursor, window_end_seq]`` 内。

    上界必须参与判定：只判下界会让 ``seq > window_end_seq`` 的窗外事件也被重放，
    把窗外实体行从高 seq 状态覆盖回旧状态（“成功越窗外”回归）。
    """
    seq = int(seq)
    return int(target_cursor) < seq <= int(window_end_seq)


def validate_window(
    *,
    target_cursor: int,
    window_end_seq: Any,
    hwm: int,
    applied_seqs: set[int],
    fixture_seqs: set[int],
) -> None:
    """建任务前的全部窗口校验，不通过抛 ``ValueError``（路由转 400）。

    ``window_end_seq`` 显式判 None：API 层虽强制必填，这里再挡一层，确保
    “无上界整库重放”在任何调用路径下都无法落地。
    """
    if window_end_seq is None:
        raise ValueError("window_end_seq 必填：禁止无上界整库重放")
    target_cursor = int(target_cursor)
    window_end_seq = int(window_end_seq)
    if target_cursor < 0:
        raise ValueError("target_cursor 不能为负")
    if target_cursor >= window_end_seq:
        raise ValueError("target_cursor 必须小于 window_end_seq（窗内至少一个 seq）")
    if target_cursor > hwm:
        raise ValueError(
            f"target_cursor={target_cursor} 高于当前游标 {hwm}：回拨任务只能后退，不能前推"
        )
    if window_end_seq > hwm:
        raise ValueError(
            f"window_end_seq={window_end_seq} 高于已提交高水位 {hwm}：只允许重放已 Apply 的事件"
        )
    seqs = window_seqs(target_cursor, window_end_seq)
    missing_fixture = [s for s in seqs if s not in fixture_seqs]
    if missing_fixture:
        raise ValueError(f"窗口内 seq 在夹具中不存在: {missing_fixture}")
    missing_applied = [s for s in seqs if s not in applied_seqs]
    if missing_applied:
        raise ValueError(f"窗口内 seq 尚未成功 Apply，不能回拨重放: {missing_applied}")


def entity_boundary_violations(
    all_events: list[dict[str, Any]],
    window_keys: set[str],
    window_end_seq: int,
) -> list[str]:
    """返回在窗外（seq > window_end_seq）仍有事件的窗内实体键。

    判定与事件落在窗内/窗外无关：只要某个被窗口触及的实体键在窗口上界之后还有
    事件，部分重放就会把该键从高 seq 的新状态覆盖回旧状态，必须拒绝。
    """
    return sorted(
        {
            e["entity_key"]
            for e in all_events
            if e["entity_key"] in window_keys and int(e["seq"]) > int(window_end_seq)
        }
    )
