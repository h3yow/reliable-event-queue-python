# 本地可靠事件队列（Python）

仅在**本进程与本地存储**内运行的事件投递与消费可靠队列：外部事件经 HTTP 接收后
写入本地 SQLite（启用 WAL），由进程内多线程消费者处理；不依赖任何外部中间件、
外部服务或真实凭据。

## 1. 事件模型

| 字段 | 说明 |
| --- | --- |
| `event_id` | 业务提供的幂等键（字符串，非空） |
| `offset` | 存储分配的**单调递增位点**（SQLite 自增整数，从 1 开始；`-1` 表示尚无提交） |
| `payload` | 事件内容（字符串，原样保存） |
| `status` | `pending` → `processing` →（成功）`succeeded` /（失败到顶）`dead_letter` |
| `attempts` | 已尝试处理次数（首次处理后记为 1） |
| `result` | 成功后的处理结果，**成功后不可再被改变** |
| `last_error` | 最近一次失败的错误类型与信息 |
| `next_visible_at` | 退避结束、可再次被领取的时间 |
| `created_at` | 首次接收时间（单调时钟），同时是去重窗口起点 |

状态机：

```
接收成功   pending
消费者领取 processing
   ├─ 成功                     → succeeded（终态）
   ├─ 可重试错误且未达次数上限  → pending（按指数退避重新可见）
   └─ 不可重试错误 / 重试到顶   → dead_letter（终态，可查询/可重放）
```

进程崩溃时停在 `processing` 的事件，在下次打开存储时**回退为 `pending`**
（消费者池优雅停机时对最后一轮已认领未处理事件执行同样回收），保证不丢事件。

## 2. 幂等与去重规则

- 以 `event_id` 为幂等键，`dedup_keys` 表记录 `{event_id → 首次时间, 位点}`。
  不同 `event_id` 互不可见，不会互相覆盖或误判为重复。
- **首次投递**：写入事件 + 登记去重键，判定 `accepted`，分配新位点。
- **窗口内重复**（距首次接收 < `dedup_window_seconds`，默认 60s）：判定
  `duplicate_in_window`，**不写入、不覆盖**，返回首次位点，原因中给出首次位点、
  时间差与剩余窗口时间。
- **窗口过期后**（≥ 窗口）：判定 `not_duplicate_window_expired`，旧键失效，
  该标识按**全新事件**入队并分配新位点；旧事件的状态与结果保持不变。
- 接收路径的“检查-登记”在同一把进程锁内完成，并发首次投递同一 `event_id`
  只有一个生效，其余全部判为重复。

判定结果三态可通过 `ReceiveResult.decision` 区分，`reason` 为可直接展示的中文说明。

## 3. 重试与隔离规则

- 业务处理器抛出 `RetryableError` 视为可重试；抛出 `FatalError`（或其它任意
  异常）视为不可重试。
- 指数退避：第 `attempts` 次失败后等待
  `min(base_backoff * 2**(attempts-1), max_backoff)`（默认 0.05s 起，封顶 2s）。
- 达到 `max_attempts`（默认 3，含首次）仍失败：进入隔离区 `dead_letters`，
  记录事件标识、位点、尝试次数、最后错误和**进入原因**（“达到上限”或
  “不可重试错误”）。
- 不可重试错误**不重试**，当次即隔离。
- `POST /dead-letters/{offset}/replay`（或 `service.replay_dead`）可把隔离区
  事件重置为 `pending` 重新处理；对**已成功**事件的重放/重处理一律跳过，
  成功结果永不改变。重放后隔离区记录标记 `replayed=true`，默认查询隐藏，
  `?include_replayed=true` 可查全部历史。

## 4. 位点语义

- 位点是接收顺序的单调整数（从 1 开始），不是执行完成顺序。
- 已提交位点 `committed_offset` 持久化在 `meta` 表，进程重启后原样恢复。
- 提交规则（`POST /offset` / `service.commit_offset`）：
  1. **回退明确拒绝**：请求位点 < 当前提交位点时抛 `OffsetRollbackError`
     （HTTP 409），原因中给出请求值、当前值与“只能单调递增”的说明；
  2. **不允许跨空洞提交**：目标位点之前若仍有事件未到终态（成功/隔离），
     同样 409 拒绝，原因中给出首个未完成位点；
  3. 相等提交幂等成功。
- `service.auto_advance()` 把提交位点推进到“最长连续终态前缀”的末尾。
- 注意：隔离区事件也是终态；位点连续到隔离事件时可以提交（表示该位置已有
  确定结论），但业务上仍可查询并重放它。

## 5. 并发一致性

- 单个 SQLite 连接 + 进程内 `RLock` 串行化所有写入；开启 WAL 与 busy_timeout。
- 领取事件使用 `BEGIN IMMEDIATE` 事务：把“按位点顺序选出的到期 `pending` 事件”
  原子地批量置为 `processing`。多个消费者线程并行时，每个事件一次只会被一个
  线程领取——不重复生效；失败回退/退避、崩溃回退 `pending`——不丢失。
- 业务处理器在锁外执行，互不阻塞；其异常被捕获并归类为重试/致命。

## 6. HTTP 接口

| 方法 路径 | 说明 |
| --- | --- |
| `POST /events` | 接收事件 `{event_id, payload}`，返回 `decision/reason/offset` |
| `GET  /events/{offset}` | 查询事件状态、尝试次数、结果与错误 |
| `GET  /offset` | 查看当前已提交位点 |
| `POST /offset` | 提交位点 `{offset}`，回退/跨空洞返回 409 并说明原因 |
| `GET  /dead-letters?include_replayed=false` | 查询隔离区 |
| `POST /dead-letters/{offset}/replay` | 重放隔离区事件 |

服务默认监听 `127.0.0.1:8000`，数据库文件为当前目录 `reliable_queue.db`，
可用环境变量 `RELIABLE_QUEUE_DB` 覆盖。

## 7. 本地验证方法

```bash
# 安装（uv）
uv sync

# 运行全部单测（终端实时日志含 event_id / 位点 / 当前提交位点 / 重试次数）
uv run pytest -q

# 按场景运行
uv run pytest tests/test_dedup.py       # 重复投递、窗口过期、不同事件互不覆盖
uv run pytest tests/test_retry.py      # 退避重试、隔离原因、成功结果不可变
uv run pytest tests/test_concurrency.py # 8 线程并发消费、收发并行、不丢不重
uv run pytest tests/test_recovery.py   # 崩溃恢复、位点/隔离区持久化、回退拒绝
uv run pytest tests/test_api.py        # HTTP 端到端

# 启动服务手动验证（仅本地）
uv run python main.py
curl -s -X POST localhost:8000/events -d '{"event_id":"e1","payload":"hi"}' -H 'Content-Type: application/json'
curl -s -X POST localhost:8000/events -d '{"event_id":"e1","payload":"hi"}' -H 'Content-Type: application/json'  # duplicate_in_window
curl -s localhost:8000/dead-letters
```

代码内使用示例：

```python
from app.models import QueueConfig, RetryableError
from app.service import EventQueueService

svc = EventQueueService("queue.db", QueueConfig())
svc.receive("evt-1", '{"order": 42}')
svc.process_batch(lambda event: "processed")  # 单线程批处理
# 多线程：
# from app.consumer import ConsumerPool
# ConsumerPool(svc, handler, workers=8).run_until_drained()
svc.auto_advance()
```

## 8. 目录结构

```
app/models.py    事件/状态/判定/配置类型
app/storage.py   SQLite 持久化、认领事务、崩溃恢复、隔离区、位点
app/dedup.py     幂等接收与去重窗口
app/retry.py     退避与重试/隔离判定
app/service.py   门面：接收/消费/位点/隔离区/重放
app/consumer.py  多线程消费者池（优雅停机回收在途事件）
app/api.py       FastAPI 路由
main.py          服务入口
tests/           重复投递/重试/并发/重启恢复/API 测试
```
