# -*- coding: utf-8 -*-
"""
LangGraph 编排核心

图结构（多 Agent 协同）：

    START → planner ──→ execute ──→ execute ──→ ... ──→ finalize → END
                │                      │
                └──(无步骤)→ finalize ──┘

节点职责：
- planner  ：规划 Agent —— 意图理解 + 基于现有工具生成执行计划，落库并推送 plan_created
- execute  ：执行 Agent —— 逐个执行计划步骤（支持 HITL interrupt 即时中断，等待人工确认）
- finalize ：协调 Agent —— 汇总所有步骤结果，生成最终回复

能力支撑：
- 即时中断   ：interrupt() + AsyncSqliteSaver 检查点，图在确认节点暂停，等待前端选择
- 中断恢复   ：Command(resume=choice) 恢复执行
- 回溯/回放  ：从指定步骤重建初始状态重新执行（见 ChatService.rewind/replay）
"""
import json
import logging
import uuid
from typing import Any, Optional

import aiosqlite
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.errors import GraphInterrupt
from langgraph.graph import StateGraph, START, END
from langgraph.types import Command, interrupt

from src.core.agents import planner_agent, coordinator_agent
from src.core.graph.state import GraphState
from src.core.registry import tool_registry
from src.db.store import (
    RunStore, run_store,
    STEP_PENDING, STEP_RUNNING, STEP_COMPLETED, STEP_FAILED, STEP_INTERRUPTED, STEP_AWAITING_CONFIRM,
    RUN_RUNNING, RUN_INTERRUPTED, RUN_COMPLETED, RUN_FAILED, RUN_STOPPED,
)

logger = logging.getLogger(__name__)


class GraphRuntime:
    """LangGraph 运行器：编译后的图 + SQLite 检查点"""

    def __init__(self, store: Optional[RunStore] = None, db_path: Optional[str] = None):
        self.store = store or run_store
        self.db_path = db_path or self.store.db_path
        self.conn: Optional[aiosqlite.Connection] = None
        self.checkpointer: Optional[AsyncSqliteSaver] = None
        self.graph = None
        # 图执行器池：每个执行器拥有独立的 aiosqlite 连接 + AsyncSqliteSaver。
        # 避免所有 run 共享单个连接——并发执行或任务被取消时会互相干扰（checkpoint 写挂起）
        self._pool: list[dict] = []

    async def initialize(self):
        """构建图（幂等）"""
        if self._pool:
            return
        await self._new_entry()
        logger.info("LangGraph 图构建完成（checkpointer: %s）", self.db_path)

    async def _new_entry(self) -> dict:
        """新建一个图执行器（独立连接 + 检查点）"""
        # timeout=30：检查点与业务库（RunStore）并发写同一 db 时等待 30s，避免 database is locked
        conn = await aiosqlite.connect(self.db_path, timeout=30)
        checkpointer = AsyncSqliteSaver(conn)
        await checkpointer.setup()
        graph = self._build_graph(checkpointer)
        entry = {"graph": graph, "conn": conn, "in_use": False}
        self._pool.append(entry)
        if self.graph is None:
            self.graph = graph
        logger.info("新建图执行器，当前池大小=%d", len(self._pool))
        return entry

    async def _acquire(self) -> dict:
        """取一个空闲执行器；无空闲则新建"""
        for e in self._pool:
            if not e["in_use"]:
                e["in_use"] = True
                return e
        e = await self._new_entry()
        e["in_use"] = True
        return e

    def _release(self, entry: dict):
        entry["in_use"] = False

    async def close(self):
        for e in self._pool:
            await e["conn"].close()
        self._pool = []
        self.graph = None

    # ============================================
    # 图构建
    # ============================================
    def _build_graph(self, checkpointer: AsyncSqliteSaver):
        g = StateGraph(GraphState)
        g.add_node("planner", self._planner_node)
        g.add_node("execute", self._execute_node)
        g.add_node("finalize", self._finalize_node)
        g.add_edge(START, "planner")
        g.add_conditional_edges(
            "planner",
            self._route_after_plan,
            {"execute": "execute", "finalize": "finalize"},
        )
        g.add_conditional_edges(
            "execute",
            self._route_after_execute,
            {"execute": "execute", "finalize": "finalize"},
        )
        g.add_edge("finalize", END)
        return g.compile(checkpointer=checkpointer)

    # ============================================
    # 节点
    # ============================================
    async def _planner_node(self, state: GraphState, config) -> dict:
        run_id = state["run_id"]
        store = config["configurable"]["store"]
        emit = config["configurable"]["on_event"]

        # 回溯/回放场景：计划已存在，跳过规划直接执行
        if state.get("plan") and state["plan"].get("steps"):
            return {
                "plan": state["plan"],
                "current_index": state.get("current_index", 0),
            }

        # 正常流程：规划 Agent 生成计划
        plan = await planner_agent.create_plan(state.get("user_input", ""), state.get("intent") or {})
        store.save_plan(run_id, plan)
        await emit("plan_created", {
            "run_id": run_id,
            "intent": plan.get("intent", ""),
            "confidence": plan.get("confidence"),
            "summary": plan.get("summary", ""),
            "steps": plan.get("steps", []),
        })
        return {"plan": plan, "current_index": 0}

    async def _execute_node(self, state: GraphState, config) -> dict:
        run_id = state["run_id"]
        store = config["configurable"]["store"]
        emit = config["configurable"]["on_event"]
        plan = state.get("plan") or {}
        steps = plan.get("steps", [])
        idx = state.get("current_index", 0)

        # 停止请求：不再执行
        if state.get("stop_requested") or store.is_stop_requested(run_id):
            store.update_run_status(run_id, RUN_STOPPED)
            return {"stop_requested": True, "current_index": len(steps), "run_status": RUN_STOPPED}

        if idx >= len(steps):
            return {"current_index": idx}

        step = steps[idx]
        seq = step.get("seq")

        # ---- 分支A：该步骤正在等待人工确认 / 参数补充（resume 流程） ----
        pending = state.get("pending_confirm") or store.get_pending(run_id, seq)
        if pending and pending.get("step_seq") == seq:
            choice = interrupt(pending)  # 恢复执行时返回用户的选择
            updates: dict = {"pending_confirm": None, "current_index": idx + 1}

            # 参数补齐场景：用户提供了缺失参数
            if pending.get("type") == "ask_params":
                filled = dict(pending.get("params", {}))
                if isinstance(choice, dict):
                    filled.update(choice)
                try:
                    result = await tool_registry.execute(pending.get("tool", ""), **filled)
                    store.clear_pending(run_id, seq)
                    store.mark_step(run_id, seq, STEP_COMPLETED, result=str(result))
                    await emit("plan_step_result", {
                        "run_id": run_id, "seq": seq, "status": STEP_COMPLETED, "result": str(result),
                    })
                    await emit("step_result", {"run_id": run_id, "seq": seq, "content": str(result)})
                except Exception as e:
                    logger.exception("参数补齐后执行失败: seq=%s", seq)
                    store.mark_step(run_id, seq, STEP_FAILED, error=str(e))
                    await emit("plan_step_result", {
                        "run_id": run_id, "seq": seq, "status": STEP_FAILED, "error": str(e),
                    })
                    updates["error"] = str(e)
                return updates

            # 人工确认场景
            if choice == "confirm":
                try:
                    result = await tool_registry.execute(
                        pending.get("tool", ""), **pending.get("params", {})
                    )
                    store.clear_pending(run_id, seq)
                    store.mark_step(run_id, seq, STEP_COMPLETED, result=str(result))
                    await emit("plan_step_result", {
                        "run_id": run_id, "seq": seq, "status": STEP_COMPLETED, "result": str(result),
                    })
                    await emit("step_result", {"run_id": run_id, "seq": seq, "content": str(result)})
                except Exception as e:
                    logger.exception("确认后执行失败: seq=%s", seq)
                    store.mark_step(run_id, seq, STEP_FAILED, error=str(e))
                    await emit("plan_step_result", {
                        "run_id": run_id, "seq": seq, "status": STEP_FAILED, "error": str(e),
                    })
                    updates["error"] = str(e)
            else:
                # 用户取消 → 标记中断，后续步骤不再执行
                store.clear_pending(run_id, seq)
                store.mark_step(run_id, seq, STEP_INTERRUPTED, result="用户取消操作")
                await emit("plan_step_result", {
                    "run_id": run_id, "seq": seq, "status": STEP_INTERRUPTED, "result": "用户取消操作",
                })
                updates["stop_requested"] = True
            return updates

        # ---- 分支B：正常执行当前步骤 ----
        if store.is_stop_requested(run_id):
            store.update_run_status(run_id, RUN_STOPPED)
            return {"stop_requested": True, "current_index": len(steps), "run_status": RUN_STOPPED}

        store.mark_step(run_id, seq, STEP_RUNNING, started=True)
        await emit("plan_step_start", {
            "run_id": run_id, "seq": seq, "tool": step.get("tool_name", ""),
            "title": step.get("title", ""),
        })

        try:
            if step.get("tool_name"):
                params = step.get("params", {}) or {}
                missing = tool_registry.check_missing_params(step["tool_name"], params)
                # 必填参数缺失 → 即时中断，向前端追问
                if missing:
                    payload = {
                        "step_seq": seq,
                        "type": "ask_params",
                        "tool": step["tool_name"],
                        "params": params,
                        "missing": missing,
                        "question": f"执行「{step.get('title', step['tool_name'])}」需要补充以下信息：{', '.join(missing)}",
                    }
                    store.mark_step(run_id, seq, STEP_AWAITING_CONFIRM, result=payload["question"])
                    payload["run_id"] = run_id
                    await emit("ask_params", payload)
                    await emit("plan_step_result", {
                        "run_id": run_id, "seq": seq, "status": STEP_AWAITING_CONFIRM, "result": payload["question"],
                    })
                    # 即时中断：pending 持久化，恢复时从分支A继续
                    store.save_pending(run_id, seq, payload)
                    interrupt(payload)
                    return {"current_index": idx + 1}
                result = await tool_registry.execute(step["tool_name"], **params)
            else:
                # 多 Agent 协同：无工具步骤 → 协调 Agent 基于已有步骤结果作答
                result = await coordinator_agent.answer(step, state.get("step_results", {}))

            # HITL：工具返回确认信号 → 即时中断
            if isinstance(result, dict) and result.get("_requires_confirm"):
                payload = {
                    "step_seq": seq,
                    "type": "human_confirm",
                    "confirm_id": result.get("confirm_id", ""),
                    "question": result.get("question") or result.get("result") or "",
                    "options": result.get("options", []),
                    "tool": (result.get("on_confirm") or {}).get("tool", ""),
                    "params": (result.get("on_confirm") or {}).get("params", {}),
                }
                store.mark_step(run_id, seq, STEP_AWAITING_CONFIRM, result=payload["question"])
                payload["run_id"] = run_id
                await emit("human_confirm", payload)
                await emit("plan_step_result", {
                    "run_id": run_id, "seq": seq, "status": STEP_AWAITING_CONFIRM, "result": payload["question"],
                })
                # 即时中断：pending 持久化，恢复时从分支A继续
                store.save_pending(run_id, seq, payload)
                interrupt(payload)
                return {"current_index": idx + 1}

            store.mark_step(run_id, seq, STEP_COMPLETED, result=str(result))
            step_results = dict(state.get("step_results") or {})
            step_results[str(seq)] = str(result)
            await emit("plan_step_result", {
                "run_id": run_id, "seq": seq, "status": STEP_COMPLETED, "result": str(result),
            })
            await emit("step_result", {"run_id": run_id, "seq": seq, "content": str(result)})
            return {"step_results": step_results, "current_index": idx + 1}
        except GraphInterrupt:
            # 即时中断：LangGraph 运行时负责暂停图并保存检查点，此处原样抛出
            raise
        except Exception as e:
            logger.exception("步骤执行失败: seq=%s tool=%s", seq, step.get("tool_name"))
            store.mark_step(run_id, seq, STEP_FAILED, error=str(e))
            await emit("plan_step_result", {
                "run_id": run_id, "seq": seq, "status": STEP_FAILED, "error": str(e),
            })
            return {"error": str(e), "current_index": idx + 1}

    async def _finalize_node(self, state: GraphState, config) -> dict:
        run_id = state["run_id"]
        store = config["configurable"]["store"]
        emit = config["configurable"]["on_event"]
        steps = store.get_steps(run_id)
        plan = state.get("plan") or {}
        user_input = state.get("user_input", "")

        # 用户主动停止
        if state.get("stop_requested"):
            interrupted = [s for s in steps if s.get("status") == STEP_INTERRUPTED]
            if interrupted:
                answer = "❌ 操作已取消，未继续执行后续步骤。如需重新开始，可在计划中点击「从该步回溯」。"
            else:
                answer = "⏹️ 已停止执行。剩余步骤保持待执行状态，可随时「从该步回溯」继续。"
            store.update_run_status(run_id, RUN_STOPPED)
            await emit("tool_result", {"content": answer})
            await emit("run_done", {"run_id": run_id, "status": RUN_STOPPED})
            return {"final_answer": answer, "run_status": RUN_STOPPED}

        # 协调 Agent 汇总最终回复
        error = state.get("error")
        answer = await coordinator_agent.synthesize(user_input, plan, steps)
        status = RUN_FAILED if error else RUN_COMPLETED
        store.update_run_status(run_id, status, error=error)
        await emit("tool_result", {"content": answer})
        await emit("run_done", {"run_id": run_id, "status": status})
        return {"final_answer": answer, "run_status": status}

    # ============================================
    # 条件路由
    # ============================================
    def _route_after_plan(self, state) -> str:
        steps = (state.get("plan") or {}).get("steps", [])
        return "execute" if steps else "finalize"

    def _route_after_execute(self, state) -> str:
        if state.get("stop_requested"):
            return "finalize"
        idx = state.get("current_index", 0)
        steps = (state.get("plan") or {}).get("steps", [])
        if idx < len(steps):
            return "execute"
        return "finalize"

    # ============================================
    # 对外执行接口
    # ============================================
    def build_config(self, run_id: str, thread_id: str, emit) -> dict:
        """构建图执行 config（节点通过 config.configurable 访问 store / on_event）"""
        return {
            "configurable": {
                "thread_id": thread_id,
                "run_id": run_id,
                "store": self.store,
                "on_event": emit,
            }
        }

    async def invoke_graph(self, initial_state: dict, config: dict) -> dict:
        """执行图直到结束或中断

        Returns:
            {"state": 最终状态, "interrupted": 是否因人工确认而暂停}
        """
        entry = await self._acquire()
        try:
            graph = entry["graph"]
            final = await graph.ainvoke(initial_state, config)
            snap = await graph.aget_state(config)
            interrupted = bool(snap and snap.next)
            return {"state": final, "interrupted": interrupted}
        finally:
            self._release(entry)

    async def resume_graph(self, config: dict, choice: str) -> dict:
        """恢复被中断的图（用户对确认框的选择）

        注：检查点持久化在 db 中，恢复时可用池中任意空闲执行器读取。
        """
        entry = await self._acquire()
        try:
            graph = entry["graph"]
            final = await graph.ainvoke(Command(resume=choice), config)
            snap = await graph.aget_state(config)
            interrupted = bool(snap and snap.next)
            return {"state": final, "interrupted": interrupted}
        finally:
            self._release(entry)
