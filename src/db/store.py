# -*- coding: utf-8 -*-
"""
智能体运行时 SQLite 持久化层

在本地 SQLite（agent_runtime.db）中持久化：
- 运行记录（agent_runs）    ：run_id / thread_id / 用户输入 / 意图 / 计划 / 状态
- 计划步骤（agent_steps）   ：每一步的工具、参数、状态、结果，供前端计划列表实时展示
- 事件日志（agent_events）  ：SSE 事件流水，支撑 回溯 / 定点回放 的审计

配合 LangGraph 的 SqliteSaver 检查点，共同支撑：
回溯（rewind）、即时中断（interrupt）、定点回放（replay）。
"""
import json
import logging
import sqlite3
import threading
import uuid
from datetime import datetime
from typing import Any, Optional

from src.config.db_config import get_agent_db_path

logger = logging.getLogger(__name__)

# ---------- 步骤状态 ----------
STEP_PENDING = "pending"                # 等待执行
STEP_RUNNING = "running"                # 执行中
STEP_COMPLETED = "completed"            # 已完成
STEP_FAILED = "failed"                  # 执行失败
STEP_INTERRUPTED = "interrupted"        # 中断/取消
STEP_AWAITING_CONFIRM = "awaiting_confirm"  # 等待人工确认

# ---------- 运行状态 ----------
RUN_CREATED = "created"
RUN_RUNNING = "running"
RUN_INTERRUPTED = "interrupted"
RUN_COMPLETED = "completed"
RUN_FAILED = "failed"
RUN_STOPPED = "stopped"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS agent_runs (
    run_id TEXT PRIMARY KEY,
    thread_id TEXT,
    user_input TEXT,
    intent_name TEXT,
    intent_confidence REAL,
    plan_json TEXT,
    status TEXT DEFAULT 'created',
    error TEXT,
    stop_requested INTEGER DEFAULT 0,
    created_at TEXT,
    updated_at TEXT
);
CREATE TABLE IF NOT EXISTS agent_steps (
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    tool_name TEXT,
    title TEXT,
    description TEXT,
    params_json TEXT,
    status TEXT DEFAULT 'pending',
    result TEXT,
    error TEXT,
    started_at TEXT,
    finished_at TEXT,
    PRIMARY KEY (run_id, seq)
);
CREATE TABLE IF NOT EXISTS agent_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    event_type TEXT,
    data_json TEXT,
    created_at TEXT
);
CREATE TABLE IF NOT EXISTS agent_pending (
    run_id TEXT NOT NULL,
    step_seq INTEGER NOT NULL,
    payload_json TEXT,
    created_at TEXT,
    PRIMARY KEY (run_id, step_seq)
);
CREATE INDEX IF NOT EXISTS idx_steps_run ON agent_steps(run_id);
CREATE INDEX IF NOT EXISTS idx_events_run ON agent_events(run_id);
"""


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class RunStore:
    """运行/步骤/事件 的 SQLite 存取

    提供线程安全读写；LangGraph 检查点由独立的 SqliteSaver 管理，
    本类只负责业务数据（计划、步骤、事件、运行状态）。
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or get_agent_db_path()
        self._lock = threading.Lock()
        self._init_db()

    # ============================================
    # 基础连接
    # ============================================
    def _connect(self) -> sqlite3.Connection:
        # timeout=30：SQLite 写锁冲突时等待 30s 而不是立即抛 database is locked
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_db(self):
        with self._lock:
            conn = self._connect()
            try:
                conn.executescript(_SCHEMA)
                conn.commit()
            finally:
                conn.close()
        logger.info("SQLite 持久化层就绪: %s", self.db_path)

    # ============================================
    # 运行记录
    # ============================================
    def create_run(self, user_input: str, intent: Optional[dict] = None) -> dict:
        """创建一次运行记录，返回 {run_id, thread_id}"""
        run_id = "run_" + uuid.uuid4().hex[:12]
        thread_id = "thread_" + uuid.uuid4().hex[:12]
        intent = intent or {}
        now = _now()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT INTO agent_runs (run_id, thread_id, user_input, intent_name, "
                    "intent_confidence, status, created_at, updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (run_id, thread_id, user_input,
                     intent.get("intent_name"), intent.get("confidence"),
                     RUN_CREATED, now, now),
                )
                conn.commit()
            finally:
                conn.close()
        logger.info("创建运行记录: %s (thread=%s)", run_id, thread_id)
        return {"run_id": run_id, "thread_id": thread_id}

    def update_run_intent(self, run_id: str, intent: dict):
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE agent_runs SET intent_name=?, intent_confidence=?, updated_at=? WHERE run_id=?",
                    (intent.get("intent_name"), intent.get("confidence"), _now(), run_id),
                )
                conn.commit()
            finally:
                conn.close()

    def save_plan(self, run_id: str, plan: dict):
        """保存计划（幂等：直接覆盖）"""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE agent_runs SET plan_json=?, status=?, updated_at=? WHERE run_id=?",
                    (json.dumps(plan, ensure_ascii=False), RUN_RUNNING, _now(), run_id),
                )
                conn.commit()
            finally:
                conn.close()
        self._save_steps(run_id, plan.get("steps", []))

    def _save_steps(self, run_id: str, steps: list[dict]):
        now = _now()
        with self._lock:
            conn = self._connect()
            try:
                for s in steps:
                    conn.execute(
                        "INSERT OR REPLACE INTO agent_steps "
                        "(run_id, seq, tool_name, title, description, params_json, status, started_at, finished_at) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (run_id, s.get("seq"), s.get("tool_name", ""), s.get("title", ""),
                         s.get("description", ""), json.dumps(s.get("params", {}), ensure_ascii=False),
                         STEP_PENDING, now, None),
                    )
                conn.commit()
            finally:
                conn.close()

    def get_run(self, run_id: str) -> Optional[dict]:
        conn = self._connect()
        try:
            row = conn.execute("SELECT * FROM agent_runs WHERE run_id=?", (run_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def list_runs(self, limit: int = 20) -> list[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM agent_runs ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def update_run_status(self, run_id: str, status: str, error: Optional[str] = None):
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE agent_runs SET status=?, error=?, updated_at=? WHERE run_id=?",
                    (status, error, _now(), run_id),
                )
                conn.commit()
            finally:
                conn.close()

    def set_thread_id(self, run_id: str, thread_id: str):
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE agent_runs SET thread_id=?, updated_at=? WHERE run_id=?",
                    (thread_id, _now(), run_id),
                )
                conn.commit()
            finally:
                conn.close()

    # ============================================
    # 停止（即时中断）
    # ============================================
    def request_stop(self, run_id: str):
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE agent_runs SET stop_requested=1, updated_at=? WHERE run_id=?",
                    (_now(), run_id),
                )
                conn.commit()
            finally:
                conn.close()

    def clear_stop(self, run_id: str):
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE agent_runs SET stop_requested=0, updated_at=? WHERE run_id=?",
                    (_now(), run_id),
                )
                conn.commit()
            finally:
                conn.close()

    def is_stop_requested(self, run_id: str) -> bool:
        run = self.get_run(run_id)
        return bool(run and run.get("stop_requested"))

    # ============================================
    # 待确认（pending_confirm 持久化）
    # ============================================
    def save_pending(self, run_id: str, seq: int, payload: dict):
        """保存某步骤等待人工确认/参数补充的 payload（中断恢复时按此重建分支A）"""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO agent_pending (run_id, step_seq, payload_json, created_at) "
                    "VALUES (?,?,?,?)",
                    (run_id, seq, json.dumps(payload or {}, ensure_ascii=False), _now()),
                )
                conn.commit()
            finally:
                conn.close()

    def get_pending(self, run_id: str, seq: int) -> Optional[dict]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT payload_json FROM agent_pending WHERE run_id=? AND step_seq=?",
                (run_id, seq),
            ).fetchone()
            if not row:
                return None
            return json.loads(row["payload_json"] or "{}")
        finally:
            conn.close()

    def clear_pending(self, run_id: str, seq: Optional[int] = None):
        """清除待确认记录（seq=None 时清除该运行全部）"""
        with self._lock:
            conn = self._connect()
            try:
                if seq is None:
                    conn.execute("DELETE FROM agent_pending WHERE run_id=?", (run_id,))
                else:
                    conn.execute(
                        "DELETE FROM agent_pending WHERE run_id=? AND step_seq=?",
                        (run_id, seq),
                    )
                conn.commit()
            finally:
                conn.close()

    # ============================================
    # 计划步骤
    # ============================================
    def mark_step(self, run_id: str, seq: int, status: str,
                  result: Optional[str] = None, error: Optional[str] = None,
                  started: bool = False):
        """更新某一步的状态与结果"""
        with self._lock:
            conn = self._connect()
            try:
                if started:
                    conn.execute(
                        "UPDATE agent_steps SET status=?, started_at=? WHERE run_id=? AND seq=?",
                        (status, _now(), run_id, seq),
                    )
                elif status == STEP_PENDING:
                    conn.execute(
                        "UPDATE agent_steps SET status=?, result=NULL, error=NULL, finished_at=NULL "
                        "WHERE run_id=? AND seq=?",
                        (status, run_id, seq),
                    )
                else:
                    conn.execute(
                        "UPDATE agent_steps SET status=?, result=?, error=?, finished_at=? "
                        "WHERE run_id=? AND seq=?",
                        (status, result, error, _now(), run_id, seq),
                    )
                conn.commit()
            finally:
                conn.close()

    def get_steps(self, run_id: str) -> list[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM agent_steps WHERE run_id=? ORDER BY seq", (run_id,)
            ).fetchall()
            steps = []
            for r in rows:
                d = dict(r)
                try:
                    d["params"] = json.loads(d.get("params_json") or "{}")
                except Exception:
                    d["params"] = {}
                steps.append(d)
            return steps
        finally:
            conn.close()

    def reset_steps_from(self, run_id: str, seq: int):
        """将 seq 及之后的所有步骤重置为 pending（供回溯/回放使用）"""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE agent_steps SET status=?, result=NULL, error=NULL, "
                    "started_at=NULL, finished_at=NULL WHERE run_id=? AND seq>=?",
                    (STEP_PENDING, run_id, seq),
                )
                conn.commit()
            finally:
                conn.close()

    def get_run_snapshot(self, run_id: str) -> dict:
        """重建一次运行的完整快照（计划 + 步骤），用于回溯/回放重新执行"""
        run = self.get_run(run_id)
        if not run:
            raise ValueError(f"运行不存在: {run_id}")
        plan = json.loads(run.get("plan_json") or "{}")
        steps = self.get_steps(run_id)
        step_results = {}
        for s in steps:
            if s.get("status") == STEP_COMPLETED and s.get("result"):
                step_results[str(s["seq"])] = s["result"]
        return {
            "run_id": run_id,
            "thread_id": run.get("thread_id"),
            "user_input": run.get("user_input"),
            "intent": {
                "intent_name": run.get("intent_name"),
                "confidence": run.get("intent_confidence"),
            },
            "plan": plan,
            "steps": steps,
            "step_results": step_results,
            "run_status": run.get("status"),
        }

    # ============================================
    # 事件日志
    # ============================================
    def append_event(self, run_id: str, event_type: str, data: dict):
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT INTO agent_events (run_id, event_type, data_json, created_at) VALUES (?,?,?,?)",
                    (run_id, event_type, json.dumps(data or {}, ensure_ascii=False), _now()),
                )
                conn.commit()
            finally:
                conn.close()

    def get_events(self, run_id: str, limit: int = 500) -> list[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM agent_events WHERE run_id=? ORDER BY id LIMIT ?", (run_id, limit)
            ).fetchall()
            events = []
            for r in rows:
                d = dict(r)
                try:
                    d["data"] = json.loads(d.get("data_json") or "{}")
                except Exception:
                    d["data"] = {}
                events.append(d)
            return events
        finally:
            conn.close()


# 全局单例
run_store = RunStore()
