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
