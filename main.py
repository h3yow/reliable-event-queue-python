"""本地可靠事件队列服务入口。

仅使用本进程 + 本地 SQLite，无任何外部中间件依赖。
"""

from __future__ import annotations

import uvicorn

from app.api import create_app
from app.service import ReliableQueue

queue = ReliableQueue("queue.db")
queue.start()
app = create_app(queue)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
