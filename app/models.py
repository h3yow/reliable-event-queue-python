"""领域模型：事件、状态、处理结果等类型定义。"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum


class EventStatus(str, Enum):
    """事件在队列中的生命周期状态。"""

    PENDING = "PENDING"        # 已接收，待处理
    PROCESSING = "PROCESSING"  # 已被某个消费者认领
    SUCCEEDED = "SUCCEEDED"    # 处理成功（终态，结果不可变）
    FAILED = "FAILED"          # 仍在重试中，等待退避后再次投递
    DEAD = "DEAD"              # 超过重试上限或不可重试，进入隔离区


class DLQStatus(str, Enum):
    """隔离区记录状态。"""

    ACTIVE = "ACTIVE"          # 在隔离区中，可查询 / 可重放
    RESOLVED = "RESOLVED"      # 重放后处理成功


class DuplicateReason(str, Enum):
    """重复判定的可解释原因。"""

    NONE = "NONE"                      # 非重复
    ALREADY_SUCCEEDED = "ALREADY_SUCCEEDED"  # 事件此前已成功
    INFLIGHT_OR_RETRYING = "INFLIGHT_OR_RETRYING"  # 事件仍在处理/重试窗口内
    DEDUP_WINDOW = "DEDUP_WINDOW"      # 去重窗口内再次投递


@dataclass
class Event:
    """一条待处理的外部事件。"""

    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    key: str = ""                # 业务幂等键（去重标识）
    type: str = ""               # 事件类型
    payload: str = ""            # 事件负载（JSON 字符串）
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    status: EventStatus = EventStatus.PENDING
    attempts: int = 0            # 已尝试处理次数
    available_at: float = 0.0    # 退避结束、可再次投递的时间
    last_error: str = ""
    result: str = ""             # 成功处理结果（成功后不可变）
    offset: int = 0              # 单调位点，接收顺序分配


@dataclass
class ReceiveResult:
    """接收（投递）事件的结果。"""

    accepted: bool
    event: Event | None = None
    duplicate: bool = False
    reason: DuplicateReason = DuplicateReason.NONE
    message: str = ""


@dataclass
class DeadLetter:
    """隔离区中的一条记录。"""

    event_id: str
    key: str
    attempts: int
    last_error: str
    status: DLQStatus
    dead_at: float
    event: Event | None = None


@dataclass
class CursorState:
    """已提交消费位点。"""

    value: int = 0
    updated_at: float = 0.0


@dataclass
class ProcessOutcome:
    """单次消费处理的结果。"""

    event_id: str
    key: str
    status: EventStatus
    attempts: int
    result: str = ""
    error: str = ""
    requeued: bool = False
    dead: bool = False
