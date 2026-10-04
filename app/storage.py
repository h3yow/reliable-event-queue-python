"""基于 SQLite 的本地持久化存储。

所有队列状态（事件、去重注册、隔离区、消费位点）都保存在单个本地
SQLite 文件中，进程重启后可完整恢复。写入均在事务中完成，配合 WAL
与文件互斥锁保证多消费者并发下的一致性。
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from app.models import (
    CursorState,
    DeadLetter,
    DLQStatus,
    Event,
    EventStatus,
    HeldEvent,
    QueueOverview,
    Session,
    SessionOverview,
    SessionStatus,
)

# 从 PROCESSING 回收的认领事件（claim_at）超过该时间视为消费者崩溃
EVENT_COLUMNS = (
    "id, `key`, type, payload, status, attempts, available_at, last_error, "
    "result, offset, created_at, updated_at, claimed_at, dedup_expires_at, "
    "session_id, lease_expires_at"
)


def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        id=row["id"],
        key=row["key"],
        type=row["type"],
        payload=row["payload"],
        status=EventStatus(row["status"]),
        attempts=row["attempts"],
        available_at=row["available_at"],
        last_error=row["last_error"] or "",
        result=row["result"] or "",
        offset=row["offset"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        session_id=row["session_id"] or "",
        lease_expires_at=row["lease_expires_at"],
    )


class Storage:
    def __init__(self, db_path: str | Path = "queue.db") -> None:
        self.db_path = str(db_path)
        self._conn: sqlite3.Connection | None = None
        # 所有连接访问都通过该锁串行化：多个线程共享一个连接时，
        # 自动提交语句不得与其他线程的显式事务交错
        self._tx_lock = threading.RLock()

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    def init_schema(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
                id                 TEXT PRIMARY KEY,
                `key`              TEXT NOT NULL,
                type               TEXT NOT NULL,
                payload            TEXT NOT NULL DEFAULT '',
                status             TEXT NOT NULL,
                attempts           INTEGER NOT NULL DEFAULT 0,
                available_at       REAL NOT NULL DEFAULT 0,
                last_error         TEXT NOT NULL DEFAULT '',
                result             TEXT NOT NULL DEFAULT '',
                offset             INTEGER NOT NULL UNIQUE,
                created_at         REAL NOT NULL,
                updated_at         REAL NOT NULL,
                claimed_at         REAL NOT NULL DEFAULT 0,
                dedup_expires_at   REAL NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_events_status_avail
                ON events(status, available_at);
            CREATE INDEX IF NOT EXISTS idx_events_key ON events(`key`);

            CREATE TABLE IF NOT EXISTS dedup_registry (
                `key`              TEXT PRIMARY KEY,
                event_id           TEXT NOT NULL,
                expires_at         REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_dedup_expires
                ON dedup_registry(expires_at);

            CREATE TABLE IF NOT EXISTS dead_letters (
                event_id     TEXT PRIMARY KEY REFERENCES events(id),
                `key`        TEXT NOT NULL,
                attempts     INTEGER NOT NULL,
                last_error   TEXT NOT NULL,
                status       TEXT NOT NULL,
                dead_at      REAL NOT NULL,
                resolved_at  REAL NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS meta (
                name   TEXT PRIMARY KEY,
                value  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id          TEXT PRIMARY KEY,
                started_at  REAL NOT NULL,
                closed_at   REAL NOT NULL DEFAULT 0,
                status      TEXT NOT NULL
            );
            """
        )
        # 兼容旧库：为已存在的 events 表补充会话 / 租约列
        cols = {
            row["name"] for row in conn.execute("PRAGMA table_info(events)")
        }
        if "session_id" not in cols:
            conn.execute(
                "ALTER TABLE events ADD COLUMN session_id TEXT NOT NULL DEFAULT ''"
            )
        if "lease_expires_at" not in cols:
            conn.execute(
                "ALTER TABLE events ADD COLUMN lease_expires_at REAL NOT NULL DEFAULT 0"
            )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id)"
        )
        conn.commit()

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            conn = sqlite3.connect(
                self.db_path,
                timeout=30.0,
                check_same_thread=False,
                isolation_level=None,  # 显式事务
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._conn = conn
        return self._conn

    @property
    def conn(self) -> sqlite3.Connection:
        return self._connect()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------ #
    # 接收 / 去重
    # ------------------------------------------------------------------ #
    def receive_if_new(
        self, event: Event, dedup_expires_at: float, now: float
    ) -> tuple[bool, Event | None]:
        """在单个事务内原子完成「去重检查 + 位点分配 + 插入」。

        返回 (accepted, existing)：
        - accepted=True  ：新事件已插入（event.offset 已被填充）
        - accepted=False ：去重窗口内已有同键事件，返回已有事件
        """
        with self._tx_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self.conn.execute(
                    "SELECT event_id, expires_at FROM dedup_registry "
                    "WHERE `key`=?",
                    (event.key,),
                ).fetchone()
                if row is not None:
                    if row["expires_at"] > now:
                        existing_row = self.conn.execute(
                            f"SELECT {EVENT_COLUMNS} FROM events WHERE id=?",
                            (row["event_id"],),
                        ).fetchone()
                        self.conn.execute("COMMIT")
                        return False, (
                            _row_to_event(existing_row) if existing_row else None
                        )
                    # 已过期：删除登记，继续作为新事件
                    self.conn.execute(
                        "DELETE FROM dedup_registry WHERE `key`=?",
                        (event.key,),
                    )

                max_row = self.conn.execute(
                    "SELECT COALESCE(MAX(offset), 0) AS m FROM events"
                ).fetchone()
                event.offset = int(max_row["m"]) + 1

                self.conn.execute(
                    """
                    INSERT INTO events (id, `key`, type, payload, status, attempts,
                                        available_at, last_error, result, offset,
                                        created_at, updated_at, claimed_at,
                                        dedup_expires_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        event.id, event.key, event.type, event.payload,
                        event.status.value, event.attempts, event.available_at,
                        event.last_error, event.result, event.offset,
                        event.created_at, event.updated_at, 0.0,
                        dedup_expires_at,
                    ),
                )
                self.conn.execute(
                    """
                    INSERT INTO dedup_registry(`key`, event_id, expires_at)
                    VALUES (?,?,?)
                    ON CONFLICT(`key`) DO UPDATE SET
                        event_id=excluded.event_id,
                        expires_at=excluded.expires_at
                    """,
                    (event.key, event.id, dedup_expires_at),
                )
                self.conn.execute("COMMIT")
                return True, None
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def insert_event(self, event: Event, dedup_expires_at: float) -> None:
        """插入新事件（要求调用方已完成去重与位点分配）。"""
        with self._tx_lock:
            self.conn.execute(
                """
                INSERT INTO events (id, `key`, type, payload, status, attempts,
                                    available_at, last_error, result, offset,
                                    created_at, updated_at, claimed_at,
                                    dedup_expires_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    event.id, event.key, event.type, event.payload,
                    event.status.value, event.attempts, event.available_at,
                    event.last_error, event.result, event.offset,
                    event.created_at, event.updated_at, 0.0,
                    dedup_expires_at,
                ),
            )
            self.conn.execute(
                """
                INSERT INTO dedup_registry(`key`, event_id, expires_at)
                VALUES (?,?,?)
                ON CONFLICT(`key`) DO UPDATE SET
                    event_id=excluded.event_id,
                    expires_at=excluded.expires_at
                """,
                (event.key, event.id, dedup_expires_at),
            )
            self.conn.commit()

    def lookup_active_dedup(self, key: str, now: float) -> Event | None:
        """返回去重注册表中未过期登记所指向的事件；过期则清除登记。"""
        with self._tx_lock:
            row = self.conn.execute(
                "SELECT event_id, expires_at FROM dedup_registry WHERE `key`=?",
                (key,),
            ).fetchone()
            if row is None:
                return None
            if row["expires_at"] <= now:
                self.conn.execute(
                    "DELETE FROM dedup_registry WHERE `key`=?", (key,)
                )
                self.conn.commit()
                return None
            return self.get_event(row["event_id"])

    def get_event_by_key(self, key: str) -> Event | None:
        with self._tx_lock:
            row = self.conn.execute(
                f"SELECT {EVENT_COLUMNS} FROM events WHERE `key`=? "
                "ORDER BY offset DESC LIMIT 1",
                (key,),
            ).fetchone()
            return _row_to_event(row) if row else None

    def get_event(self, event_id: str) -> Event | None:
        with self._tx_lock:
            row = self.conn.execute(
                f"SELECT {EVENT_COLUMNS} FROM events WHERE id=?", (event_id,)
            ).fetchone()
            return _row_to_event(row) if row else None

    def next_offset(self) -> int:
        """在事务内分配下一个单调位点。"""
        with self._tx_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self.conn.execute(
                    "SELECT COALESCE(MAX(offset), 0) AS m FROM events"
                ).fetchone()
                self.conn.execute("COMMIT")
                return int(row["m"]) + 1
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def max_offset(self) -> int:
        with self._tx_lock:
            row = self.conn.execute(
                "SELECT COALESCE(MAX(offset), 0) AS m FROM events"
            ).fetchone()
            return int(row["m"])

    # ------------------------------------------------------------------ #
    # 会话
    # ------------------------------------------------------------------ #
    def create_session(self, session_id: str, now: float) -> Session:
        with self._tx_lock:
            self.conn.execute(
                "INSERT INTO sessions(id, started_at, closed_at, status) "
                "VALUES (?,?,0,?)",
                (session_id, now, SessionStatus.ACTIVE.value),
            )
            self.conn.commit()
        return Session(
            id=session_id, started_at=now, status=SessionStatus.ACTIVE
        )

    def close_session(self, session_id: str, now: float) -> None:
        with self._tx_lock:
            self.conn.execute(
                "UPDATE sessions SET status=?, closed_at=? WHERE id=?",
                (SessionStatus.CLOSED.value, now, session_id),
            )
            self.conn.commit()

    def get_session(self, session_id: str) -> Session | None:
        with self._tx_lock:
            row = self.conn.execute(
                "SELECT id, started_at, closed_at, status FROM sessions "
                "WHERE id=?",
                (session_id,),
            ).fetchone()
            if row is None:
                return None
            return Session(
                id=row["id"], started_at=row["started_at"],
                status=SessionStatus(row["status"]), closed_at=row["closed_at"],
            )

    def release_session_events(self, session_id: str, now: float) -> int:
        """会话正常退出：把它仍持有的 PROCESSING 事件退回 FAILED
        （立即可被其他会话认领），返回释放条数。"""
        with self._tx_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self.conn.execute(
                    """
                    UPDATE events
                    SET status=?, claimed_at=0, updated_at=?, available_at=?,
                        session_id='', lease_expires_at=0
                    WHERE status=? AND session_id=?
                    """,
                    (
                        EventStatus.FAILED.value, now, now,
                        EventStatus.PROCESSING.value, session_id,
                    ),
                )
                self.conn.execute("COMMIT")
                return cur.rowcount
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------------ #
    # 消费
    # ------------------------------------------------------------------ #
    def claim_due(
        self,
        now: float,
        limit: int,
        session_id: str = "",
        lease_duration: float = 0.0,
    ) -> list[Event]:
        """原子地认领至多 limit 条到期可投递事件（PENDING 或退避到期的
        FAILED）。使用单条 UPDATE ... RETURNING 保证多消费者下每条事件
        只会被一个消费者拿到。认领时记录持有会话与租约到期时间。"""
        if limit <= 0:
            return []
        lease_expires_at = now + lease_duration if lease_duration > 0 else 0.0
        with self._tx_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self.conn.execute(
                    f"""
                    UPDATE events SET
                        status=?,
                        claimed_at=?,
                        updated_at=?,
                        attempts=attempts+1,
                        session_id=?,
                        lease_expires_at=?
                    WHERE id IN (
                        SELECT id FROM events
                        WHERE status IN (?, ?) AND available_at<=?
                        ORDER BY offset
                        LIMIT ?
                    )
                    RETURNING {EVENT_COLUMNS}
                    """,
                    (
                        EventStatus.PROCESSING.value, now, now,
                        session_id, lease_expires_at,
                        EventStatus.PENDING.value, EventStatus.FAILED.value,
                        now, limit,
                    ),
                ).fetchall()
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        return [_row_to_event(r) for r in rows]

    def heartbeat(
        self,
        event_id: str,
        now: float,
        session_id: str = "",
        lease_duration: float = 0.0,
    ) -> None:
        """续约：刷新认领时间，并在指定会话与租约时长时延长租约。"""
        lease_expires_at = now + lease_duration if lease_duration > 0 else 0.0
        with self._tx_lock:
            if session_id:
                self.conn.execute(
                    "UPDATE events SET claimed_at=?, lease_expires_at=? "
                    "WHERE id=? AND status=? AND session_id=?",
                    (
                        now, lease_expires_at, event_id,
                        EventStatus.PROCESSING.value, session_id,
                    ),
                )
            else:
                self.conn.execute(
                    "UPDATE events SET claimed_at=? WHERE id=? AND status=?",
                    (now, event_id, EventStatus.PROCESSING.value),
                )
            self.conn.commit()

    def mark_succeeded(
        self, event_id: str, result: str, now: float, session_id: str = ""
    ) -> bool:
        """仅当事件仍处于 PROCESSING 时才能落成功结果，防止重复生效。

        传入 session_id 时还要求事件仍由该会话持有：租约过期被其他会话
        接管后，旧会话的成功写入会被拒绝，避免同一事件生效两次。
        返回 True 表示本次调用真正把事件置为成功；False 表示事件已处于
        终态（成功结果不可变）或已被其他会话接管。
        """
        guard = "AND session_id=?" if session_id else ""
        params: tuple = (
            (EventStatus.SUCCEEDED.value, result, now, event_id,
             EventStatus.PROCESSING.value, session_id)
            if session_id else
            (EventStatus.SUCCEEDED.value, result, now, event_id,
             EventStatus.PROCESSING.value)
        )
        with self._tx_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self.conn.execute(
                    f"""
                    UPDATE events
                    SET status=?, result=?, last_error='', updated_at=?,
                        claimed_at=0, session_id='', lease_expires_at=0
                    WHERE id=? AND status=? {guard}
                    """,
                    params,
                )
                self.conn.execute("COMMIT")
                return cur.rowcount == 1
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def mark_retry(
        self,
        event_id: str,
        error: str,
        available_at: float,
        now: float,
        session_id: str = "",
    ) -> None:
        guard = "AND session_id=?" if session_id else ""
        params: tuple = (
            (EventStatus.FAILED.value, error, available_at, now, event_id,
             EventStatus.PROCESSING.value, session_id)
            if session_id else
            (EventStatus.FAILED.value, error, available_at, now, event_id,
             EventStatus.PROCESSING.value)
        )
        with self._tx_lock:
            self.conn.execute(
                f"""
                UPDATE events
                SET status=?, last_error=?, available_at=?, updated_at=?,
                    claimed_at=0, session_id='', lease_expires_at=0
                WHERE id=? AND status=? {guard}
                """,
                params,
            )
            self.conn.commit()

    def mark_dead(
        self, event_id: str, error: str, now: float, session_id: str = ""
    ) -> None:
        """进入隔离区：事件置 DEAD，并写入/更新 dead_letters。"""
        guard = "AND session_id=?" if session_id else ""
        params: tuple = (
            (EventStatus.DEAD.value, error, now, event_id, session_id)
            if session_id else
            (EventStatus.DEAD.value, error, now, event_id)
        )
        with self._tx_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self.conn.execute(
                    f"SELECT {EVENT_COLUMNS} FROM events WHERE id=?",
                    (event_id,),
                ).fetchone()
                if row is None:
                    raise KeyError(event_id)
                self.conn.execute(
                    f"""
                    UPDATE events
                    SET status=?, last_error=?, updated_at=?, claimed_at=0,
                        session_id='', lease_expires_at=0
                    WHERE id=? {guard}
                    """,
                    params,
                )
                self.conn.execute(
                    """
                    INSERT INTO dead_letters
                        (event_id, `key`, attempts, last_error, status, dead_at)
                    VALUES (?,?,?,?,?,?)
                    ON CONFLICT(event_id) DO UPDATE SET
                        attempts=excluded.attempts,
                        last_error=excluded.last_error,
                        status=excluded.status,
                        dead_at=excluded.dead_at,
                        resolved_at=0
                    """,
                    (
                        event_id, row["key"], row["attempts"], error,
                        DLQStatus.ACTIVE.value, now,
                    ),
                )
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def reset_for_replay(self, event_id: str, now: float) -> None:
        """把隔离区事件重新置为可认领（PENDING）。"""
        with self._tx_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.execute(
                    """
                    UPDATE events
                    SET status=?, available_at=?, claimed_at=0, updated_at=?,
                        session_id='', lease_expires_at=0
                    WHERE id=? AND status=?
                    """,
                    (
                        EventStatus.PENDING.value, now, now, event_id,
                        EventStatus.DEAD.value,
                    ),
                )
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def resolve_dead_letter(self, event_id: str, now: float) -> None:
        with self._tx_lock:
            self.conn.execute(
                "UPDATE dead_letters SET status=?, resolved_at=? WHERE event_id=?",
                (DLQStatus.RESOLVED.value, now, event_id),
            )
            self.conn.commit()

    def recover_orphans(self, now: float, timeout: float) -> int:
        """重启/周期性恢复：回收 PROCESSING 中的孤儿事件，退回 FAILED
        （attempts 已在认领时增加），立即可被新会话认领。返回回收条数。

        孤儿判定（满足任一）：
        - 持有会话已不存在或已关闭（进程被强杀 / 重启）：立即接管，
          不需要等待租约到期；
        - 租约已到期（lease_expires_at < now）：会话失联；
        - 旧格式数据兜底：无会话信息且认领时间超过 timeout。
        """
        with self._tx_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self.conn.execute(
                    """
                    UPDATE events
                    SET status=?, claimed_at=0, updated_at=?, available_at=?,
                        session_id='', lease_expires_at=0
                    WHERE status=?
                      AND (
                          (session_id != '' AND session_id NOT IN (
                              SELECT id FROM sessions WHERE status=?
                          ))
                          OR (lease_expires_at > 0 AND lease_expires_at < ?)
                          OR (session_id = '' AND claimed_at > 0
                              AND claimed_at < ?)
                      )
                    """,
                    (
                        EventStatus.FAILED.value, now, now,
                        EventStatus.PROCESSING.value,
                        SessionStatus.ACTIVE.value,
                        now, now - timeout,
                    ),
                )
                self.conn.execute("COMMIT")
                return cur.rowcount
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def list_events(self, limit: int = 100) -> list[Event]:
        with self._tx_lock:
            rows = self.conn.execute(
                f"SELECT {EVENT_COLUMNS} FROM events ORDER BY offset LIMIT ?",
                (limit,),
            ).fetchall()
            return [_row_to_event(r) for r in rows]

    # ------------------------------------------------------------------ #
    # 隔离区
    # ------------------------------------------------------------------ #
    def list_dead_letters(
        self, status_filter: str | None = None
    ) -> list[DeadLetter]:
        with self._tx_lock:
            sql = "SELECT * FROM dead_letters"
            params: tuple = ()
            if status_filter:
                sql += " WHERE status=?"
                params = (status_filter,)
            sql += " ORDER BY dead_at"
            rows = self.conn.execute(sql, params).fetchall()
            result = []
            for row in rows:
                event = self.get_event(row["event_id"])
                result.append(
                    DeadLetter(
                        event_id=row["event_id"],
                        key=row["key"],
                        attempts=row["attempts"],
                        last_error=row["last_error"],
                        status=DLQStatus(row["status"]),
                        dead_at=row["dead_at"],
                        event=event,
                    )
                )
            return result

    # ------------------------------------------------------------------ #
    # 运行概览（只读一致快照）
    # ------------------------------------------------------------------ #
    def snapshot_overview(self, now: float) -> QueueOverview:
        """在单个读事务内采集运行概览，并发消费 / 重放同时进行时
        数字与明细仍来自同一快照，不会读到改到一半的状态。"""
        with self._tx_lock:
            self.conn.execute("BEGIN DEFERRED")
            try:
                counts = {s.value: 0 for s in EventStatus}
                for row in self.conn.execute(
                    "SELECT status, COUNT(*) AS n FROM events GROUP BY status"
                ):
                    counts[row["status"]] = int(row["n"])

                backlog = (
                    counts[EventStatus.PENDING.value]
                    + counts[EventStatus.FAILED.value]
                )

                row = self.conn.execute(
                    "SELECT MIN(created_at) AS oldest FROM events "
                    "WHERE status=?",
                    (EventStatus.PENDING.value,),
                ).fetchone()
                oldest_pending_age = (
                    max(0.0, now - float(row["oldest"]))
                    if row["oldest"] is not None else None
                )

                cur_row = self.conn.execute(
                    "SELECT value FROM meta WHERE name='cursor'"
                ).fetchone()
                cursor = int(cur_row["value"]) if cur_row else 0
                max_row = self.conn.execute(
                    "SELECT COALESCE(MAX(offset), 0) AS m FROM events"
                ).fetchone()
                max_offset = int(max_row["m"])

                sessions: list[SessionOverview] = []
                sess_rows = self.conn.execute(
                    "SELECT id, started_at, status FROM sessions "
                    "ORDER BY started_at"
                ).fetchall()
                held_rows = self.conn.execute(
                    f"SELECT {EVENT_COLUMNS} FROM events "
                    "WHERE status=? AND session_id != '' ORDER BY offset",
                    (EventStatus.PROCESSING.value,),
                ).fetchall()
                held_by_session: dict[str, list[HeldEvent]] = {}
                for hr in held_rows:
                    ev = _row_to_event(hr)
                    held_by_session.setdefault(ev.session_id, []).append(
                        HeldEvent(
                            event_id=ev.id,
                            key=ev.key,
                            offset=ev.offset,
                            attempts=ev.attempts,
                            lease_expires_at=ev.lease_expires_at,
                            lease_remaining=ev.lease_expires_at - now,
                        )
                    )
                for sr in sess_rows:
                    sessions.append(
                        SessionOverview(
                            session_id=sr["id"],
                            status=SessionStatus(sr["status"]),
                            started_at=sr["started_at"],
                            held_events=held_by_session.get(sr["id"], []),
                        )
                    )
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        return QueueOverview(
            now=now,
            counts=counts,
            backlog=backlog,
            oldest_pending_age=oldest_pending_age,
            cursor=cursor,
            max_offset=max_offset,
            cursor_gap=max_offset - cursor,
            sessions=sessions,
        )

    def cursor_blockers(self, value: int) -> list[tuple[int, str, str]]:
        """返回位点 value 之前（含）所有未到终态（非 SUCCEEDED）的事件，
        按位点升序，用于位点提交前的安全检查与拒绝说明。"""
        with self._tx_lock:
            rows = self.conn.execute(
                "SELECT offset, `key`, status FROM events "
                "WHERE offset<=? AND status!=? ORDER BY offset",
                (value, EventStatus.SUCCEEDED.value),
            ).fetchall()
            return [
                (int(r["offset"]), r["key"], r["status"]) for r in rows
            ]

    # ------------------------------------------------------------------ #
    # 位点
    # ------------------------------------------------------------------ #
    def get_cursor(self) -> CursorState:
        with self._tx_lock:
            row = self.conn.execute(
                "SELECT value FROM meta WHERE name='cursor'"
            ).fetchone()
            if row is None:
                return CursorState(value=0, updated_at=0.0)
            return CursorState(value=int(row["value"]), updated_at=0.0)

    def commit_cursor(self, value: int, now: float) -> None:
        with self._tx_lock:
            self.conn.execute(
                """
                INSERT INTO meta(name, value) VALUES ('cursor', ?)
                ON CONFLICT(name) DO UPDATE SET value=excluded.value
                """,
                (str(value),),
            )
            self.conn.commit()
