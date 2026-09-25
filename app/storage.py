"""本地 SQLite 持久化层。

仅使用标准库 sqlite3，数据全部落在本地文件；单连接 + 进程内互斥锁保证
多线程消费下的写一致性，事务使用 BEGIN IMMEDIATE 防止认领竞争。
"""
from __future__ import annotations

import sqlite3
import threading

from .models import DeadLetterEntry, Event, EventStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    offset_n        INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT NOT NULL,
    payload         TEXT NOT NULL,
    created_at      REAL NOT NULL,
    status          TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    result          TEXT,
    last_error      TEXT,
    next_visible_at REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_events_status ON events(status, next_visible_at, offset_n);
CREATE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id);

CREATE TABLE IF NOT EXISTS dedup_keys (
    event_id   TEXT PRIMARY KEY,
    first_seen REAL NOT NULL,
    offset_n   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dead_letters (
    offset_n   INTEGER PRIMARY KEY,
    event_id   TEXT NOT NULL,
    payload    TEXT NOT NULL,
    attempts   INTEGER NOT NULL,
    last_error TEXT NOT NULL,
    reason     TEXT NOT NULL,
    dead_at    REAL NOT NULL,
    replayed   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

COMMITTED_OFFSET_KEY = "committed_offset"


def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        event_id=row["event_id"],
        payload=row["payload"],
        offset=row["offset_n"],
        created_at=row["created_at"],
        status=EventStatus(row["status"]),
        attempts=row["attempts"],
        result=row["result"],
        last_error=row["last_error"],
        next_visible_at=row["next_visible_at"],
    )


class Storage:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()
            self.recover_inflight()

    # ---------- 恢复 ----------
    def recover_inflight(self) -> int:
        """PROCESSING（已认领未终态）事件回退为 PENDING 重投。

        两个时机调用：Storage 初始化（进程崩溃重启恢复）、消费者池优雅停止
        （最后一轮已认领但来不及处理的事件回收）。
        """
        with self._lock:
            cur = self._conn.execute(
                "UPDATE events SET status=?, next_visible_at=0 WHERE status=?",
                (EventStatus.PENDING.value, EventStatus.PROCESSING.value),
            )
            self._conn.commit()
            return cur.rowcount

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def close(self) -> None:
        with self._lock:
            if getattr(self, "_closed", False):
                return
            self._conn.commit()
            self._conn.close()
            self._closed = True

    # ---------- 去重键 ----------
    def get_dedup(self, event_id: str) -> tuple[float, int] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT first_seen, offset_n FROM dedup_keys WHERE event_id=?",
                (event_id,),
            ).fetchone()
            return None if row is None else (row["first_seen"], row["offset_n"])

    def drop_dedup(self, event_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM dedup_keys WHERE event_id=?", (event_id,))

    # ---------- 事件写入 / 查询 ----------
    def insert_event(self, event_id: str, payload: str, now: float) -> int:
        """登记去重键并写入新事件，返回分配的位点（连续自增）。"""
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO events(event_id, payload, created_at, status) "
                "VALUES(?,?,?,?)",
                (event_id, payload, now, EventStatus.PENDING.value),
            )
            offset = cur.lastrowid
            self._conn.execute(
                "INSERT INTO dedup_keys(event_id, first_seen, offset_n) VALUES(?,?,?)",
                (event_id, now, offset),
            )
            self._conn.commit()
            return offset

    def get_event(self, offset: int) -> Event | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM events WHERE offset_n=?", (offset,)
            ).fetchone()
            return None if row is None else _row_to_event(row)

    def claim_due(self, now: float, limit: int) -> list[Event]:
        """事务性认领到期可处理事件：PENDING 且退避已结束，按位点顺序。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute(
                    "SELECT * FROM events WHERE status=? AND next_visible_at<=? "
                    "ORDER BY offset_n LIMIT ?",
                    (EventStatus.PENDING.value, now, limit),
                ).fetchall()
                offsets = [r["offset_n"] for r in rows]
                if offsets:
                    placeholders = ",".join("?" * len(offsets))
                    self._conn.execute(
                        f"UPDATE events SET status=? WHERE offset_n IN ({placeholders})",
                        [EventStatus.PROCESSING.value, *offsets],
                    )
                self._conn.commit()
                return [
                    Event(
                        event_id=r["event_id"], payload=r["payload"], offset=r["offset_n"],
                        created_at=r["created_at"], status=EventStatus.PROCESSING,
                        attempts=r["attempts"], result=r["result"],
                        last_error=r["last_error"], next_visible_at=r["next_visible_at"],
                    )
                    for r in rows
                ]
            except Exception:
                self._conn.rollback()
                raise

    def mark_success(self, offset: int, result: str, attempts: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE events SET status=?, result=?, attempts=?, last_error=NULL "
                "WHERE offset_n=?",
                (EventStatus.SUCCEEDED.value, result, attempts, offset),
            )
            self._conn.commit()

    def mark_retry(self, offset: int, error: str, attempts: int, next_visible_at: float) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE events SET status=?, attempts=?, last_error=?, next_visible_at=? "
                "WHERE offset_n=?",
                (EventStatus.PENDING.value, attempts, error, next_visible_at, offset),
            )
            self._conn.commit()

    def mark_dead(self, offset: int, error: str, attempts: int, next_visible_at: float) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE events SET status=?, attempts=?, last_error=?, next_visible_at=? "
                "WHERE offset_n=?",
                (EventStatus.DEAD_LETTER.value, attempts, error, next_visible_at, offset),
            )
            self._conn.commit()

    # ---------- 隔离区 ----------
    def upsert_dead_letter(self, entry: DeadLetterEntry) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO dead_letters(offset_n, event_id, payload, attempts, "
                "last_error, reason, dead_at, replayed) VALUES(?,?,?,?,?,?,?,0) "
                "ON CONFLICT(offset_n) DO UPDATE SET event_id=excluded.event_id, "
                "payload=excluded.payload, attempts=excluded.attempts, "
                "last_error=excluded.last_error, reason=excluded.reason, "
                "dead_at=excluded.dead_at, replayed=0",
                (entry.offset, entry.event_id, entry.payload, entry.attempts,
                 entry.last_error, entry.reason, entry.dead_at),
            )
            self._conn.commit()

    def list_dead_letters(self, include_replayed: bool = False) -> list[DeadLetterEntry]:
        with self._lock:
            sql = "SELECT * FROM dead_letters"
            if not include_replayed:
                sql += " WHERE replayed=0"
            sql += " ORDER BY offset_n"
            rows = self._conn.execute(sql).fetchall()
            return [
                DeadLetterEntry(
                    event_id=r["event_id"], offset=r["offset_n"], payload=r["payload"],
                    attempts=r["attempts"], last_error=r["last_error"], reason=r["reason"],
                    dead_at=r["dead_at"], replayed=bool(r["replayed"]),
                )
                for r in rows
            ]

    def mark_dead_replayed(self, offset: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE dead_letters SET replayed=1 WHERE offset_n=?", (offset,)
            )
            self._conn.commit()

    def reopen_dead_event(self, offset: int, now: float) -> bool:
        """重放：隔离区事件重新置为 PENDING，重置尝试计数获得完整重试预算。

        已成功事件不得改动，返回是否生效。隔离区表中的历史记录保留。
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM events WHERE offset_n=?", (offset,)
            ).fetchone()
            if row is None or row["status"] != EventStatus.DEAD_LETTER.value:
                return False
            self._conn.execute(
                "UPDATE events SET status=?, attempts=0, last_error=NULL, "
                "next_visible_at=? WHERE offset_n=?",
                (EventStatus.PENDING.value, now, offset),
            )
            self._conn.commit()
            return True

    # ---------- 位点 ----------
    def get_committed_offset(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key=?", (COMMITTED_OFFSET_KEY,)
            ).fetchone()
            return -1 if row is None else int(row["value"])

    def set_committed_offset(self, offset: int) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta(key, value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (COMMITTED_OFFSET_KEY, str(offset)),
            )
            self._conn.commit()

    def all_terminal_through(self, offset: int) -> bool:
        """位点 <= offset 的事件是否全部到达终态（成功/隔离）。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(1) AS c FROM events WHERE offset_n<=? AND status NOT IN (?,?)",
                (offset, EventStatus.SUCCEEDED.value, EventStatus.DEAD_LETTER.value),
            ).fetchone()
            return row["c"] == 0

    def first_non_terminal_offset(self) -> int | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT MIN(offset_n) AS m FROM events WHERE status NOT IN (?,?)",
                (EventStatus.SUCCEEDED.value, EventStatus.DEAD_LETTER.value),
            ).fetchone()
            return None if row is None or row["m"] is None else row["m"]
