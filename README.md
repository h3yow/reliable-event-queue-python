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
  service.py   ReliableQueue：幂等接收、退避重试、隔离区、会话租约、并发协调
  api.py       FastAPI 路由
main.py        服务入口（uvicorn）
tests/         重复投递 / 并发 / 重试隔离 / 重启恢复 / 会话租约 / 运行概览 / 位点安全测试
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

- **每次 `ReliableQueue.start()` 即创建一个独立会话**（`sess-xxxx`，持久化在
  `sessions` 表）。认领事件时在事件上登记 `session_id` 与
  `lease_expires_at`（租约到期时间 = 认领时间 + `processing_timeout`，
  默认 30 秒），处理期间可随时看出「哪个会话占着、租约还剩多久」
  （`GET /events/{id}` 与 `GET /overview` 均可见）。
- **快速接管**：新会话启动时回收三类孤儿事件并立即重新投递——
  ① 持有会话已关闭 / 不存在（进程被强杀、整体重启）；② 租约已过期；
  ③ 旧格式数据认领超时兜底。前两类不等待任何额外超时，被杀会话未处理完的
  事件马上能被新会话接手。
- **不重复生效**：终态写入（成功 / 重试 / 进隔离区）除 `PROCESSING` 状态
  守护外还带 `session_id` 守卫——租约过期被新会话接管后，旧会话迟到的
  写入会被拒绝，同一事件不会生效两次。
- **干净退出**：`close()` 会把本会话仍持有的事件立即释放回可投递状态
  （不等租约到期），并将会话标记为 CLOSED。

## 运行概览（只读）

`GET /overview`（或 `queue.overview()`）返回单事务一致快照，并发消费与
人工重放同时进行时数字与明细也对得上：

- `counts`：各状态（PENDING / PROCESSING / SUCCEEDED / FAILED / DEAD）数量
- `backlog`：积压 = PENDING + FAILED
- `oldest_pending_age`：最老一条待处理事件已等待秒数（无待处理为 null）
- `cursor` / `max_offset` / `cursor_gap`：已确认位点、最新接收位置及差距
- `sessions`：每个会话的状态及其正占着的事件列表（事件标识、位点、
  重试次数、租约到期时间与剩余秒数）

## 位点（cursor）语义

- 每个事件按接收顺序分配严格单调递增的 `offset`（从 1 开始）。
- 事件成功后位点按偏移顺序自动连续推进，语义为「此前事件均已成功处理」；
  前序事件尚未成功时不会跳过。位点也支持显式提交。
- **位点回退被明确拒绝**：提交值小于当前值时返回
  `409 拒绝位点回退：当前位点 X，请求提交 Y；位点只能单调递增`
  （`CursorRegressionError`）。
- 提交值超过已知最大位点同样被拒绝（`CursorOutOfRangeError`），防止位点
  凭空跳跃。
- **位点提交安全约束**：人工把位点往前推时，目标位置之前（含）必须全部
  已确认成功；只要还有未到终态的事件（PENDING / PROCESSING / FAILED /
  DEAD 都算），提交被拒绝并返回 409，`detail.blockers` 列出每条阻挡事件
  的 `offset / key / status`（`CursorBlockedError`）。被挡住的事件处理
  成功后，同一位置即可提交。
- **重复提交当前位点是幂等成功**（无操作），不因后续新到的未处理事件而
  报错。
- 位点与隔离区、事件状态全部持久化在 SQLite，进程重启后自动恢复；重启时还会
  回收孤儿 PROCESSING 事件（见「消费者会话与租约」）重新投递。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/events` | 投递事件 `{key,type,payload}`，返回是否接收与重复原因 |
| GET | `/events` / `/events/{id}` | 查询事件（含持有会话与租约到期时间） |
| POST | `/process?limit=` | 认领并处理到期事件（演示用默认处理器恒成功） |
| GET | `/dead-letters?active=true` | 查询隔离区 |
| POST | `/dead-letters/{id}/replay` | 重放隔离事件 |
| GET | `/cursor` | 查看当前位点与最大偏移 |
| POST | `/cursor` | 显式提交位点（回退 / 越界 / 被未完成事件阻挡返回 409） |
| GET | `/overview` | 只读运行概览（单事务一致快照） |

## 兼容性说明

- **数据兼容**：旧版 `queue.db` 直接可用。启动时自动为 `events` 表补充
  `session_id` / `lease_expires_at` 列（默认空 / 0）并新建 `sessions`
  表；旧库中处于 PROCESSING 的事件按「认领超时」兜底规则回收。
- **接口兼容**：已有接口的路径、请求与响应字段全部保留，仅响应中新增
  `session_id` / `lease_expires_at` 等字段；`POST /cursor` 在原有回退 /
  越界拒绝之上新增「未完成事件阻挡」拒绝（同样 409），重复提交当前位点
  仍为幂等成功。
- **行为兼容**：位点自动推进、去重、退避重试、隔离区与重放语义不变。

## 本地验证

无需任何外部依赖，使用 uv 即可：

```bash
# 安装依赖并运行全部单测（重复投递 / 并发 / 重试隔离 / 重启恢复 /
# 会话租约 / 运行概览 / 位点安全）
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
# 运行概览：各状态数量 / 积压 / 最老待处理 / 位点差距 / 会话持有明细
curl -s localhost:8000/overview
# 查询隔离区 / 位点；位点回退会被拒绝并说明原因
curl -s localhost:8000/dead-letters
curl -s -X POST localhost:8000/cursor -H 'Content-Type: application/json' \
  -d '{"value":0}'
# 位点前有未处理完的事件时提交会被 409 拒绝并列出阻挡事件
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"key":"evt-2","type":"order","payload":"y"}'
curl -s -X POST localhost:8000/cursor -H 'Content-Type: application/json' \
  -d '{"value":2}'
```

单测以 `-s` 运行时会打印处理日志，包含事件标识（`key` / `event_id`）、
当前位点（`offset` / `cursor`）、重试次数（`attempts`）与持有会话
（`session`），例如：

```
INFO [reliable_queue] 可靠队列已启动 session=sess-e084940e4399 cursor=0
INFO [reliable_queue] 接收新事件 key=flaky offset=1 event_id=3720...
INFO [reliable_queue] 开始处理 key=flaky offset=1 attempts=2 session=sess-e084940e4399
INFO [reliable_queue] 处理成功 key=flaky offset=1 attempts=3 result=eventually-ok
INFO [reliable_queue] 位点自动推进 cursor=1（由 key=flaky offset=1 触发）
```
