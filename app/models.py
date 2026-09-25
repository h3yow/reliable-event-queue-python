"""事件模型：状态、接收判定、错误、死信等公共类型。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class EventStatus(str, enum.Enum):
    """事件生命周期状态。

    PENDING   已接收、待消费
    PROCESSING 已被某个消费者认领（崩溃重启后会回退为 PENDING）
    SUCCEEDED 处理成功（终态，结果不可再被改变）
    DEAD_LETTER 超过重试上限 / 不可重试错误，进入隔离区（终态，可查询/可重放）
    """

    PENDING = "pending"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    DEAD_LETTER = "dead_letter"


class ReceiveDecision(str, enum.Enum):
    """接收（投递）一个事件时的幂等判定结果，reason 给出可解释原因。"""

    ACCEPTED = "accepted"                    # 全新事件，已受理
    DUPLICATE_IN_WINDOW = "duplicate_in_window"  # 去重窗口内的重复投递
    NOT_DUPLICATE_WINDOW_EXPIRED = "not_duplicate_window_expired"  # 窗口过期，按新事件处理


@dataclass(frozen=True)
class Event:
    event_id: str
    payload: str
    offset: int          # 单调递增位点，由存储层分配
    created_at: float    # 首次接收时间（窗口起点）
    status: EventStatus = EventStatus.PENDING
    attempts: int = 0    # 已尝试处理次数
    result: str | None = None        # 成功后记录的处理结果
    last_error: str | None = None    # 最近一次失败原因
    next_visible_at: float = 0.0     # 退避结束、可再次被领取的时间


@dataclass(frozen=True)
class ReceiveResult:
    """接收事件的返回：判定结论 + 可解释原因 + 位点。"""

    decision: ReceiveDecision
    reason: str
    offset: int | None
    event_id: str


class RetryableError(Exception):
    """可重试错误：消费者抛出后按退避策略重试。"""


class FatalError(Exception):
    """不可重试错误：立即进入隔离区。"""


@dataclass
class DeadLetterEntry:
    """隔离区记录，可查询。"""

    event_id: str
    offset: int
    payload: str
    attempts: int
    last_error: str
    reason: str          # 进入隔离区的原因（超过上限 / 不可重试）
    dead_at: float
    replayed: bool = False


@dataclass
class EnqueueOutcome:
    """内部入队结果（区别于对外的 ReceiveResult）。"""

    is_new: bool
    reason: str
    offset: int


@dataclass
class QueueConfig:
    dedup_window_seconds: float = 60.0   # 去重窗口 TTL
    max_attempts: int = 3                # 最大尝试次数（含首次）
    base_backoff_seconds: float = 0.05  # 指数退避基数
    max_backoff_seconds: float = 2.0    # 退避上限
    poll_limit: int = 16                # 单次最多领取事件数
