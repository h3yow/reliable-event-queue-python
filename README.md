# 本地可靠事件队列（Python）

在现有 FastAPI 服务基础上实现的一套**仅运行于本进程 + 本地存储**的可靠事件
投递与消费能力。无外部中间件、无外部服务、无真实凭据：所有状态保存在单个
本地 SQLite 文件中（默认 `queue.db`，WAL 模式）。

## 目录结构

```
app/
  models.py    事件 / 状态 / 会话 / 运行概览等领域模型
  errors.py    错误分类：可重试、不可重试、位点回退 / 越界 / 被阻挡
  storage.py   SQLite 持久化：事件、去重注册、隔离区、位点、会话
  service.py   ReliableQueue：幂等接收、退避重试、隔离区、会话租约、概览
  api.py       FastAPI 路由
main.py        服务入口（uvicorn）
tests/         重复投递 / 并发 / 重试隔离 / 重启恢复 / 会话租约 / 概览位点测试
```

## 事件模型与状态机

每条事件包含：`id`（UUID）、`key`（业务幂等键，去重标识）、`type`、
`payload`、单调递增的 `offset`（接收位点）、`attempts`（尝试次数）、
`available_at`（退避后可再次投递时间）、`result`（成功结果，终态不可变）。

状态机：

```
                 接收
PENDING ───────────────────────► PROCESSING ──成功──► SUCCEEDED（终态）
   ▲                              │  ▲
   │ 退避到期重投                  │  │ 重启后回收认领超时事件
   └──────────────────────────────┘  │
                                     │
   可重试错误且未达上限               │
PROCESSING ──失败──► FAILED ─退避到期─┘

PROCESSING ──不可重试错误 / 重试耗尽──► DEAD（隔离区，可重放）
DEAD ──replay──► PENDING …… 重放成功则隔离记录置 RESOLVED
```

## 幂等与去重规则

- 去重单位是业务键 `key`，去重检查、位点分配与事件插入在**单个 SQLite
  事务**内原子完成（`BEGIN IMMEDIATE`），并发重复投递恰好只有一条生效。
- 窗口内重复投递返回 `accepted=false`，并给出可解释的 `reason`：
  - `INFLIGHT_OR_RETRYING`：同键事件仍在 PENDING / PROCESSING / FAILED /
    DEAD（去重窗口默认 300 秒）。
  - `ALREADY_SUCCEEDED`：事件此前已成功——**永久幂等**，重复投递不会再次
    执行处理器，成功结果永不改变。
- 去重窗口过期后，相同 `key` 按**新事件**处理，获得新的 `id` 与 `offset`，
  与历史事件互不覆盖。
- 不同 `key` 的事件彼此独立，不会互相误判为重复。

## 重试与隔离规则

处理器可抛出三类结果（未预期异常按可重试处理，保证不丢事件）：

- `RetryableError`：按指数退避重试。第 n 次失败后的等待时间为
  `min(base_delay * backoff**(n-1), max_delay)`，默认
  `0.1s, 2x, 上限 5s`，默认最多 3 次尝试。
- `NonRetryableError`：不重试，**立即进入隔离区**。
- 可重试错误达到 `max_attempts` 后进入隔离区。

隔离区（dead letters）持久化、可查询（`GET /dead-letters`），记录尝试次数
与最后错误；可通过重放接口重新处理：成功后事件回到 SUCCEEDED、隔离记录置
RESOLVED；再次失败则按同样规则回到隔离区。

并发消费：认领使用单条 `UPDATE ... WHERE id IN (SELECT ... LIMIT) RETURNING`
原子完成，多个消费者并行时每条事件只会被一个消费者拿到；成功落库带
`WHERE status='PROCESSING'` 终态守护，杜绝重复生效。

## 消费者会话与租约

- 消费方每启动一次（`ReliableQueue.start()`）即登记一个**独立会话**
  （`sessions` 表，含启动时间与心跳），并持有本次运行的所有认领。
- 认领事件时绑定 `session_id` 与 `lease_expires_at = now + lease_duration`
  （默认 10s，构造参数 `lease_duration` 可调，应大于单次处理耗时）。
  处理期间可以通过事件或运行概览看出**哪个会话占着、租约还剩多久**。
- **强杀 / 重启快速接管**：会话被强杀或整个进程重启后来不及收尾，其占着
  的 PROCESSING 事件在**租约到期的瞬间**即可被任何新会话认领接管，
  不必等待 `processing_timeout`；接管与正常认领在同一条
  `UPDATE ... RETURNING` 内原子完成。
- **不会同时处理 / 处理两遍**：租约有效期内其他会话拿不到该事件；即便
  租约到期被接管，旧会话迟到的成功 / 重试 / 隔离写入都会被
  `WHERE session_id=?` 会话守卫拒绝，成功结果永不重复生效。
- **正常退出干净收尾**：`close()` 会把本会话仍占着的事件立即释放回
  FAILED 且立即可投递（不等租约到期），并将会话标记为 CLOSED。

## 运行概览（只读一致快照）

`GET /overview`（或 `queue.get_overview()`）在**单个 SQLite 读事务**内
采集，并发消费与人工重放同时进行时读到的也是同一快照，数字与明细必然
对得上，不会读到改到一半的状态。内容包括：

- `counts`：各状态（PENDING / PROCESSING / SUCCEEDED / FAILED / DEAD）
  的事件数量；
- `backlog`：积压量（PENDING + FAILED）；
- `oldest_pending_wait`：最老一条待处理事件已等待的秒数；
- `cursor` / `max_offset` / `cursor_gap`：已确认位点、最新接收位置及差距；
- `sessions`：每个会话的状态、启动时间、心跳，以及它手里正占着的事件
  明细（`key` / `offset` / `attempts` / 租约到期时间与剩余秒数）。

## 位点（cursor）语义

- 每个事件按接收顺序分配严格单调递增的 `offset`（从 1 开始）。
- 事件成功后位点按偏移顺序自动连续推进，语义为「此前事件均已成功处理」；
  前序事件尚未成功时不会跳过。位点也支持显式提交。
- **位点回退被明确拒绝**：提交值小于当前值时返回
  `409 拒绝位点回退：当前位点 X，请求提交 Y；位点只能单调递增`
  （`CursorRegressionError`）。
- 提交值超过已知最大位点同样被拒绝（`CursorOutOfRangeError`），防止位点
  凭空跳跃。
- **前推被未终结事件阻挡时拒绝**：人工把位点往前推时，目标位置之前
  （含）只要还有没到最终结果的事件（PENDING / PROCESSING / FAILED /
  DEAD 都算），提交被拒绝并抛出 `CursorBlockedError`，错误信息里逐条
  列出阻挡事件的 `key` / `offset` / `status`；只有前面事件全部确认
  处理成功的位置才允许提交。
- **重复提交当前位点幂等**：提交值等于当前位点时直接成功、不改变状态。
- 位点与隔离区、事件状态全部持久化在 SQLite，进程重启后自动恢复；重启时还会
  将认领超时（默认 30s）的 PROCESSING 事件回收为 FAILED 重新投递。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/events` | 投递事件 `{key,type,payload}`，返回是否接收与重复原因 |
| GET | `/events` / `/events/{id}` | 查询事件 |
| POST | `/process?limit=` | 认领并处理到期事件（演示用默认处理器恒成功） |
| GET | `/dead-letters?active=true` | 查询隔离区 |
| POST | `/dead-letters/{id}/replay` | 重放隔离事件 |
| GET | `/cursor` | 查看当前位点与最大偏移 |
| POST | `/cursor` | 显式提交位点（回退 / 越界 / 被未终结事件阻挡返回 409） |
| GET | `/overview` | 只读运行概览一致快照（状态计数 / 积压 / 位点差距 / 会话租约明细） |

## 兼容范围

- **数据兼容**：旧版 `queue.db` 可直接打开——启动时自动为 `events` 表
  补充 `session_id` / `lease_expires_at` 列（默认空 / 0），并新建
  `sessions` 表；旧库中处于 PROCESSING 的事件没有租约，仍按原有
  `processing_timeout` 回收路径处理。
- **接口兼容**：已有 HTTP 接口的路径、请求与响应字段全部保留；
  `GET /events*` 响应新增 `session_id` / `lease_expires_at` 字段
  （纯增量）；`POST /cursor` 新增一种 409 情形（被未终结事件阻挡，
  响应体含 `blockers` 明细），原有回退 / 越界拒绝行为不变。
- **行为兼容**：位点自动推进、幂等去重、退避重试、隔离区重放语义不变；
  位点显式提交在原有约束之上新增「前方事件必须全部成功」的约束。

## 本地验证

无需任何外部依赖，使用 uv 即可：

```bash
# 安装依赖并运行全部单测（重复投递 / 并发 / 重试隔离 / 重启恢复）
uv run pytest -s

# 启动服务（默认监听 127.0.0.1:8000，数据落盘 ./queue.db）
uv run python main.py

# 投递事件
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"key":"evt-1","type":"order","payload":"{\"amount\":100}"}'
# 重复投递 -> reason=INFLIGHT_OR_RETRYING / 成功后 -> ALREADY_SUCCEEDED
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"key":"evt-1","type":"order","payload":"x"}'
# 处理
curl -s -X POST "localhost:8000/process?limit=10"
# 查询隔离区 / 位点；位点回退会被拒绝并说明原因
curl -s localhost:8000/dead-letters
curl -s -X POST localhost:8000/cursor -H 'Content-Type: application/json' \
  -d '{"value":0}'
# 运行概览：状态计数 / 积压 / 最老待处理等待 / 位点差距 / 会话租约明细
curl -s localhost:8000/overview
# 位点前推被未终结事件阻挡时返回 409 及 blockers 明细
curl -s -X POST localhost:8000/cursor -H 'Content-Type: application/json' \
  -d '{"value":1}'
```

单测以 `-s` 运行时会打印处理日志，包含事件标识（`key` / `event_id`）、
当前位点（`offset` / `cursor`）与重试次数（`attempts`），例如：

```
INFO [reliable_queue] 可靠队列已启动 session=ef0a... cursor=0
INFO [reliable_queue] 接收新事件 key=flaky offset=1 event_id=3720...
INFO [reliable_queue] 开始处理 key=flaky offset=1 attempts=2
INFO [reliable_queue] 处理成功 key=flaky offset=1 attempts=3 result=eventually-ok
INFO [reliable_queue] 位点自动推进 cursor=1（由 key=flaky offset=1 触发）
```
