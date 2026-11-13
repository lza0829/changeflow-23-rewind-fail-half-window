import pytest

from app.db import connect
from app.repositories import cursors as cursor_repo
from app.repositories import replay_tasks as replay_repo
from app.repositories import sink as sink_repo
from app.repositories import streams as streams_repo
from app.services import apply_service, replay_service


def _sink_map(conn, stream_id):
    return {r["entity_key"]: r for r in sink_repo.list_sink(conn) if r["stream_id"] == stream_id}


def _apply_to_end(stream_id=1):
    """正常 Apply 把游标推到夹具末尾（演示前置：先抬高游标）。"""
    run = apply_service.run_apply(stream_id)
    assert run["status"] == "succeeded"
    return run


def test_happy_path_rewind_bounded_window(isolated_db):
    # 前置：正常 Apply 抬高游标到 5
    run = _apply_to_end(1)
    assert run["cursor_after"] == 5
    conn = connect()
    sink_before = {k: dict(v) for k, v in _sink_map(conn, 1).items()}
    conn.close()

    # 回拨窗 [2, 4]：target_cursor=1，window_end_seq=4（含上界）
    task = replay_service.create_and_run_replay(
        stream_id=1, target_cursor=1, window_end_seq=4
    )
    assert task["status"] == "succeeded"
    assert task["cursor_before"] == 5
    assert task["target_cursor"] == 1
    assert task["window_end_seq"] == 4
    assert task["candidate_count"] == 3  # seq 2,3,4

    report = task["report"]
    # seq2 upsert、seq3 upsert、seq4 delete：3 个事件全部重放（delete 时行存在）
    assert report["replayed_count"] == 3
    assert report["skipped_count"] == 0
    assert report["quarantined_count"] == 0
    # 全窗成功后游标复位到回拨前高水位
    assert task["cursor_after"] == 5
    assert report["final_cursor"] == 5

    # 交叉核对全部通过
    cc = report["cross_checks"]
    assert cc["ledger"]["matches_counters"]
    assert cc["applied_seqs"]["unchanged"] is True
    assert cc["sink_outside_window_untouched"] is True
    assert cc["sink_window_converged"] is True
    assert cc["sink_identical_to_before"] is True
    assert cc["cursor_page"]["matches"] is True
    assert cc["cursor_page"]["expected_after"] == 5
    assert cc["quarantine"]["matches_counter"] is True
    assert report["cross_checks_passed"] is True

    # Sink 逐行收敛（payload/last_seq 一致；ORD-1002 仍被 seq4 删除）
    conn = connect()
    sink_after = _sink_map(conn, 1)
    assert set(sink_after) == set(sink_before)
    for key, before in sink_before.items():
        assert sink_after[key]["payload"] == before["payload"]
        assert sink_after[key]["last_seq"] == before["last_seq"]
    # ORD-1002 已被 seq4 删除，不在 sink
    assert "ORD-1002" not in sink_after
    # 游标页恢复 HWM=5，applied seq 集合不变（1..5）
    view = cursor_repo.list_cursor_view(conn)
    orders = next(v for v in view if v["stream_id"] == 1)
    assert orders["apply_cursor"] == 5
    assert orders["applied_seq_count"] == 5
    assert orders["max_applied_seq"] == 5
    conn.close()


def test_rewind_is_idempotent_across_repeats(isolated_db):
    _apply_to_end(1)
    replay_service.create_and_run_replay(1, 1, 4)
    conn = connect()
    sink_once = {k: dict(v) for k, v in _sink_map(conn, 1).items()}
    conn.close()

    # 再来一次同样的有界重放：不越放越重复
    second = replay_service.create_and_run_replay(1, 1, 4)
    assert second["status"] == "succeeded"
    conn = connect()
    sink_twice = _sink_map(conn, 1)
    assert set(sink_twice) == set(sink_once)
    for key, before in sink_once.items():
        assert sink_twice[key]["payload"] == before["payload"]
        assert sink_twice[key]["last_seq"] == before["last_seq"]
    # applied_seqs 始终只有 1..5，没有因重放膨胀
    assert cursor_repo.list_cursor_view(conn)[0]["applied_seq_count"] == 5
    # 账本只追加：两次任务各 3 行重放账本
    rows = conn.execute(
        "SELECT COUNT(*) c FROM change_ledger WHERE run_id=?", (second["id"],)
    ).fetchone()["c"]
    assert rows == 3
    conn.close()


def test_outside_window_rows_untouched(isolated_db):
    # 两路流：只重放 Orders(1)，Inventory(2) 完全不动；Orders 窗外键 ORD-1003(seq5) 不动
    _apply_to_end(1)
    _apply_to_end(2)
    conn = connect()
    inv_before = {k: dict(v) for k, v in _sink_map(conn, 2).items()}
    ord1003_before = dict(_sink_map(conn, 1)["ORD-1003"])
    conn.close()

    task = replay_service.create_and_run_replay(1, 1, 4)
    assert task["status"] == "succeeded"

    conn = connect()
    inv_after = _sink_map(conn, 2)
    assert set(inv_after) == set(inv_before)
    for key, before in inv_before.items():
        assert inv_after[key]["payload"] == before["payload"]
    ord1003_after = _sink_map(conn, 1)["ORD-1003"]
    assert ord1003_after["payload"] == ord1003_before["payload"]
    assert ord1003_after["last_seq"] == ord1003_before["last_seq"] == 5
    # Inventory 游标仍为 3，未被回拨
    assert streams_repo.get_stream(conn, 2)["apply_cursor"] == 3
    conn.close()


def test_reject_unbounded_and_invalid_windows(isolated_db):
    _apply_to_end(1)

    # 禁止无上界整库重放
    with pytest.raises(ValueError, match="window_end_seq"):
        replay_service.create_and_run_replay(1, 0, None)

    # target >= window_end：空窗
    with pytest.raises(ValueError, match="target_cursor 必须小于"):
        replay_service.create_and_run_replay(1, 3, 3)

    # 回拨目标高于当前游标：这是前推不是回拨
    with pytest.raises(ValueError, match="只能后退"):
        replay_service.create_and_run_replay(1, 6, 7)

    # 窗口上界超过已提交高水位
    with pytest.raises(ValueError, match="高水位"):
        replay_service.create_and_run_replay(1, 0, 6)

    # 实体键边界：Orders 的 ORD-1002 在 seq3 upsert、seq4 delete。窗口 [0,3]
    # 触及 ORD-1002，而 seq4（同键）在窗外 -> 拒绝（部分重放会把该键覆盖回旧状态）
    with pytest.raises(ValueError, match="实体键"):
        replay_service.create_and_run_replay(1, 0, 3)

    # 把上界扩到该键最后一次变更 seq4 即合法（ORD-1003@seq5 不在窗内键集合中）
    ok = replay_service.create_and_run_replay(1, 0, 4)
    assert ok["status"] == "succeeded"


def test_mid_window_failure_marks_failed_and_keeps_recover_point(isolated_db):
    _apply_to_end(1)
    conn = connect()
    sink_before = {k: dict(v) for k, v in _sink_map(conn, 1).items()}
    conn.close()

    # 在 seq3 注入失败：seq2 已重放提交，seq3 进隔离，seq4/5 不再处理
    task = replay_service.create_and_run_replay(1, 1, 4, fail_at_seq=3)
    assert task["status"] == "failed"
    report = task["report"]
    assert report["failure"]["seq"] == 3
    assert report["failure"]["recover_point"] == 2
    assert report["replayed_count"] == 1      # seq2
    assert report["quarantined_count"] == 1   # seq3
    assert report["last_processed_seq"] == 2
    # 游标停在可恢复点 2，没有伪装成功
    assert task["cursor_after"] == 2
    conn = connect()
    assert streams_repo.get_stream(conn, 1)["apply_cursor"] == 2
    # 失败任务也持久化，刷新后仍可按 id 取回
    assert replay_repo.get_task(conn, task["id"])["status"] == "failed"
    # 隔离区有本次失败事件
    q = conn.execute(
        "SELECT seq, reason FROM quarantine WHERE run_id=? AND seq=3", (task["id"],)
    ).fetchone()
    assert q is not None and "replay_failed" in q["reason"]
    conn.close()

    # 恢复：按提示新建 [2, 4] 续放任务，成功后游标复位 5，Sink 收敛
    recovery = replay_service.create_and_run_replay(1, 2, 4)
    assert recovery["status"] == "succeeded"
    assert recovery["cursor_after"] == 5
    conn = connect()
    sink_after = _sink_map(conn, 1)
    for key, before in sink_before.items():
        assert sink_after[key]["payload"] == before["payload"]
        assert sink_after[key]["last_seq"] == before["last_seq"]
    assert set(sink_after) == set(sink_before)
    conn.close()


def test_preview_does_not_create_task(isolated_db):
    _apply_to_end(1)
    preview = replay_service.preview_window(1, 1, 4)
    assert preview["valid"] is True
    assert preview["candidate_count"] == 3
    assert [e["seq"] for e in preview["candidates"]] == [2, 3, 4]
    conn = connect()
    assert replay_repo.list_tasks(conn) == []
    # 游标未被预览改动
    assert streams_repo.get_stream(conn, 1)["apply_cursor"] == 5
    conn.close()


def test_skip_disposition_for_idempotent_delete_of_missing_key(isolated_db):
    # Orders 正常 Apply 后 ORD-1002 已被 seq4 删除。
    # 为精确命中 skipped：重放窗 [3,4]（seq3 upsert + seq4 delete），成功后
    # ORD-1002 再次不存在；再重放 [4,4] -> 空删除记 skipped，无副作用，游标复位 5
    _apply_to_end(1)
    task = replay_service.create_and_run_replay(1, 2, 4)
    assert task["status"] == "succeeded"
    report = task["report"]
    assert report["candidate_count"] == 2
    assert report["replayed_count"] == 2

    # 再跑 [4,4]：键已缺失，空删除记 skipped，无副作用，游标复位 5
    again = replay_service.create_and_run_replay(1, 3, 4)
    assert again["status"] == "succeeded"
    assert again["report"]["skipped_count"] == 1
    assert again["report"]["replayed_count"] == 0
    assert again["cursor_after"] == 5
    conn = connect()
    assert "ORD-1002" not in _sink_map(conn, 1)
    conn.close()
