# 03-changeflow（变流）

Changeflow — CDC / 变更流落库：事件入账 → 序/缺口裁决 → 幂等应用到 sink → 游标提交与对账。

**不是** Feed 多源归并（Feedstitch），**不是** 作业依赖编排（Batchgate）。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5300 |
| API | http://localhost:10300 |

本地开发（可选）：

```bash
# backend
cd backend && pip install -r requirements.txt
set DATA_DIR=..\data
uvicorn app.main:app --reload --port 10300

# frontend
cd frontend && npm install && npm run dev
```

## 主链

配置变更流与夹具 → 触发 Apply → 按序应用 / 缺口隔离 → Sink + 游标 + 运行报告对账

## 种子说明

- **Orders CDC（clean）**：JSONL，seq 1→5 连续 upsert/delete，首跑应全部应用、游标推进到 5。
- **Inventory CDC（clean）**：JSONL，seq 1→3 连续 upsert，独立实体键，用于第二路流演示。

打开「应用台」选流跑一次，再看账本 / 隔离区 / Sink / 游标页计数是否与本次报告一致。

## 游标回拨重放窗（cursor_rewind_window）

流游标只升不降是主链默认行为；修正历史事件时需要在**有界 seq 窗口**内把游标回拨、
让窗内事件再走一遍 Apply。入口在「回拨重放窗」页与 `POST /api/replay/tasks`。

**硬规则**

- 建任务必须显式给 `target_cursor`（回拨到的游标，重放从 target+1 开始）与
  `window_end_seq`（窗口上界，含）。**无上界整库重放一律拒绝**（Pydantic +
  服务层 + 前端三重拦截）。
- 只允许回拨已成功 Apply 的事件：`target_cursor < 高水位`、`window_end_seq <= 高水位`，
  高水位取 `applied_seqs` 的 max seq（重放期间流游标会临时回拨，不能当高水位）。
- 实体键边界保护：窗内触及的 entity_key 若在窗外还有更高 seq 事件，拒绝建任务，
  避免把该键从新状态覆盖回旧状态（需把上界扩到该键最后一次变更）。

**执行与幂等**

1. 记录回拨前快照（游标、applied_seqs、Sink 全量、隔离区计数），流游标物理回拨到
   target；`applied_seqs` 保留不动。
2. 窗内事件按 seq 逐个重放、每事件一个事务立即提交：
   - `upsert` 按 `(stream_id, entity_key)` 整体覆盖，同键不产生重复行，Sink 始终可解释；
   - `delete` 幂等，键已不存在记 `replay_skipped`（无副作用）；
   - `applied_seqs` 用 `INSERT OR IGNORE`，重放不增不减；账本只追加
     （`kind='replay'`，disposition 为 replayed / replay_skipped / replay_quarantined）。
3. 中途失败：出错事件进隔离区，任务标 `failed`，游标停在**任务开始前的可恢复点**
   （`recoverable_cursor`：一个窗内事件都没成功时即 `target_cursor`，否则是最后一个
   已确认 seq），绝不抬到 `window_end_seq`，也绝无半窗脏数据却显示成功。修复后按报告
   里的 recovery_hint 新建 `[recoverable_cursor, window_end_seq]` 任务即可续放，
   成功后游标复位高水位。
4. 任务结果写明：回拨前游标、回拨目标、窗内候选数、重放/跳过/隔离计数、最终游标，
   并附带与账本、Sink、applied_seqs、游标页、隔离区的交叉核对
   （`cross_checks_passed`）。任务持久化在 `replay_tasks` 表，刷新页面仍在。
- 窗外 Sink 行与其他流的数据禁止被任务改动，结束时用窗外快照逐键核对。窗内判定严格
  使用 `(target_cursor, window_end_seq]`，`seq > window_end_seq` 的事件永不重放。

**窗完整性证明（每次任务落一份）**

任务结束时随任务一起落库到 `replay_tasks.window_proof_json`，并可从任务详情与只读接口
`GET /api/replay/tasks/{task_id}/window-proof` 读取（`available=false` 表示历史任务没有证明）：

| 字段 | 含义 |
| --- | --- |
| `window_start` / `window_end_seq` | 本次重放窗边界（`window_start = target_cursor + 1`，含上界） |
| `failure_seq` | 失败时出错事件的 seq；成功为 `null` |
| `recoverable_cursor` | 任务结束后的可恢复游标：成功=复位到高水位，失败=停在可恢复点 |
| `window_completed` | 本次是否真的跑完整窗；失败恒为 `false`（半窗绝不称完成） |
| `in_window_touched_keys` | 窗内实际处理过的实体键（含被隔离的失败事件）及逐事件处置 |
| `out_of_window_untouched_sample` | 至少 1 个窗外键的**任务前/后载荷指纹**（`payload_sha256` + 整行 `fingerprint`） |

接口还回填 `matches_task_row`，把证明与任务行（`status` / `cursor_after` / 窗边界）逐项比对。
Replay 页在任务详情展示「窗内已触达 / 窗外未改 / 可恢复游标」三块，数字全部来自该只读接口，
与接口同源；点击任务列表任一行即可按 `task_id` 重新拉取，失败与成功各跑一轮时两处数字一致。

**演示路径（本地夹具即可）**

1. 应用台对 Orders CDC 正常 Apply，游标 0 → 5（seq5 写入窗外行 `ORD-1003`）；
2. 回拨重放窗发起 `target_cursor=1, window_end_seq=4`（重放 seq 2/3/4），预览确认候选后执行；
3. 任务成功、游标复位 5，Sink 与重放前逐行一致（`ORD-1003` 的 `last_seq` 仍为 5）、
   applied_seqs 仍为 1..5、交叉核对全通过，证明里 `ORD-1003` 前/后指纹一致；
4. 再填 `fail_at_seq=3` 发起一次：观察 failed、`recoverable_cursor=2`、`window_completed=false`，
   游标页停在 2（不是窗末 4），证明的「窗内已触达」含 `ORD-1001`/`ORD-1002`；
5. 按 recovery_hint 以 `target_cursor=2, window_end_seq=4` 续放，成功后游标复位 5。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口（未实现，见题单）

当前仓库已实现「连续序幂等应用」主链与「游标回拨重放窗」。缺口等待/跳过、乱序缓冲、事务整批、双 sink 部分失败、毒丸选择性跳过等仍在 `backend/app/modules/*` 留空包，按题单逐题换主链路实现。
