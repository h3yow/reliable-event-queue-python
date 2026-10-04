"""处理事件时可能抛出的错误分类。"""

from __future__ import annotations


class HandlerError(Exception):
    """事件处理器错误基类。"""

    def __init__(self, message: str = ""):
        super().__init__(message)
        self.message = message


class RetryableError(HandlerError):
    """可重试错误：按退避策略重试，超出上限进入隔离区。"""


class NonRetryableError(HandlerError):
    """不可重试错误：立即进入隔离区。"""


class CursorRegressionError(Exception):
    """位点回退被拒绝时抛出。"""


class CursorOutOfRangeError(Exception):
    """位点超出已知范围时抛出。"""


class CursorBlockedError(Exception):
    """位点提交被未终结事件阻挡时抛出。

    blockers 为阻挡事件的 (key, offset, status) 列表，用于说明
    「目标位置之前还有哪些事件没到最终结果」。
    """

    def __init__(self, message: str = "", blockers: list | None = None):
        super().__init__(message)
        self.message = message
        self.blockers = blockers or []
