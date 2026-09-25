"""失败重试与指数退避策略。

- 可重试错误（RetryableError）：按指数退避重新调度，直到达到最大尝试次数。
- 不可重试错误（FatalError）：立即进入隔离区。
- attempts 表示“已经发生的处理次数”；首次处理后 attempts=1。
"""
from __future__ import annotations

from .models import QueueConfig, RetryableError, FatalError  # noqa: F401  (再导出便于使用)


def backoff_delay(attempts: int, config: QueueConfig) -> float:
    """第 attempts 次失败后需要等待的秒数：base * 2**(attempts-1)，封顶 max。"""
    raw = config.base_backoff_seconds * (2 ** max(0, attempts - 1))
    return min(raw, config.max_backoff_seconds)


def should_retry(error: BaseException, attempts: int, config: QueueConfig) -> bool:
    """是否还应重试。

    - FatalError / 其它非 RetryableError：不可重试，立即隔离。
    - RetryableError 且 attempts 已达上限：不再重试，隔离。
    """
    if not isinstance(error, RetryableError):
        return False
    return attempts < config.max_attempts


def dead_reason(error: BaseException, attempts: int, config: QueueConfig) -> str:
    """进入隔离区的可解释原因。"""
    if not isinstance(error, RetryableError):
        return (
            f"不可重试错误 {type(error).__name__}: {error}，不进行重试，"
            f"直接进入隔离区（已尝试 {attempts} 次）"
        )
    return (
        f"可重试错误 {type(error).__name__}: {error}，已尝试 {attempts} 次"
        f"达到上限 {config.max_attempts} 次，进入隔离区"
    )
