# -*- coding: utf-8 -*-
"""
对话服务（LangGraph 版）

执行链路（每次执行 Agent 前必先理解意图、再列计划、逐步骤执行）：
    用户输入 → 意图识别 → 计划生成（plan_created）→ 逐步骤执行（plan_step_start /
    plan_step_result 实时推送）→ 协调汇总（tool_result）→ done

能力：
- 计划列表：plan_created 推送全部步骤，每步状态通过 SSE 实时更新（前端常驻展示、可折叠）
- 即时中断：HITL 确认 / 参数追问通过 LangGraph interrupt 挂起，前端选择后 resume 恢复
- 回溯    ：rewind_run 从指定步骤重新执行（原运行内回退）
- 定点回放：replay_run 从指定步骤克隆出新的运行重新执行（原运行保留）
- 多 Agent：规划 Agent + 执行 Agent + 协调 Agent 接力（见 src/core/agents.py）
"""
import asyncio
import json
import logging
import uuid
from typing import Any, AsyncGenerator, Optional

from src.config.db_config import get_agent_checkpoint_db_path
from src.core.graph import GraphRuntime
from src.core.intent import IntentRecognizer
from src.core.registry import tool_registry
from src.db.store import (
    run_store, RunStore,
    RUN_RUNNING, RUN_INTERRUPTED, RUN_COMPLETED, RUN_FAILED, RUN_STOPPED,
    STEP_COMPLETED,
)

# 导入独立工具模块（导入即触发 @tool 装饰器注册）
import src.tools.simple_tools    # noqa: F401
import src.tools.inventory_tools   # noqa: F401
import src.tools.purchase_tools    # noqa: F401
import src.tools.approval_tools    # noqa: F401

logger = logging.getLogger(__name__)


def _sse(event: str, data: Any = None) -> str:
    return f"event: {event}\ndata: {json.dumps(data or {}, ensure_ascii=False)}\n\n"


class ChatService:
    """对话服务：意图识别 + 计划编排 + LangGraph 执行 + 回溯/回放/恢复"""

    def __init__(self):
        self.runtime: Optional[GraphRuntime] = None
        self.recognizer: Optional[IntentRecognizer] = None
        self.store: RunStore = run_store

    # ============================================
    # 初始化
    # ============================================
    async def initialize(self):
        """初始化（工具注册 + 意图识别器 + LangGraph 运行时）"""
        tools_desc = self._build_tools_description()
        self.recognizer = IntentRecognizer(tools_desc)
        # 检查点独立 db（与业务库分离），避免并发写锁竞争
        self.runtime = GraphRuntime(
            store=self.store,
            db_path=get_agent_checkpoint_db_path(),
        )
        await self.runtime.initialize()
        logger.info("ChatService(LangGraph) 初始化完成，共 %d 个工具", len(tool_registry.list_tools_simple()))

    def _build_tools_description(self) -> str:
        tools = tool_registry.list_tools_simple()
        if not tools:
            return "当前没有可用的工具。"
        return "\n".join(f"- {t['name']}: {t['description']}" for t in tools)

    def get_available_tools(self) -> list[dict]:
        return tool_registry.list_tools_simple()

    # ============================================
    # 事件发射器（SSE 队列 + 事件日志落库）
    # ============================================
    def _make_emitter(self, run_id: str, queue: Optional[asyncio.Queue]):
        async def emit(event_type: str, data: dict):
            try:
                self.store.append_event(run_id, event_type, data)
            except Exception:
                logger.debug("事件落库失败: %s", event_type)
            if queue is not None:
                await queue.put({"event": event_type, "data": data})
        return emit

    async def _emit_to_queue(self, queue: asyncio.Queue, event: str, data: dict):
        await queue.put({"event": event, "data": data})

    # ============================================
    # 核心事件流
    # ============================================
    async def _stream_events(
        self, user_input: str, history: Optional[list] = None
    ) -> AsyncGenerator[tuple[str, dict], None]:
        """意图理解 → 计划 → 执行 的完整事件流（(event, data) 元组）"""
        if not self.runtime or not self.recognizer:
            yield ("error", {"content": "服务未初始化"})
            yield ("done", {"status": "error"})
            return

        # ---- 1. 意图理解 ----
        yield ("thinking_start", {"content": "正在理解意图..."})
        intent = await self.recognizer.recognize_async(user_input, history=history)
        yield ("thinking_result", {
            "intent": intent.intent_name,
            "confidence": intent.confidence,
            "reasoning": intent.reasoning,
            "parameters": intent.parameters,
        })

        if intent.intent_name == "unknown" or intent.confidence < 0.3:
            yield ("tool_result", {"content": self._build_uncertain_msg(user_input, intent.intent_name)})
            yield ("done", {"status": "completed"})
            return

        # ---- 2. 创建运行记录 ----
        try:
            run = self.store.create_run(user_input, {
                "intent_name": intent.intent_name,
                "confidence": intent.confidence,
            })
        except Exception as e:
            logger.exception("创建运行记录失败")
            yield ("error", {"content": f"创建运行记录失败: {str(e)}"})
            yield ("done", {"status": "error"})
            return
        run_id, thread_id = run["run_id"], run["thread_id"]
        self.store.update_run_intent(run_id, {
            "intent_name": intent.intent_name,
            "confidence": intent.confidence,
        })

        # ---- 3. 图执行（后台任务，事件经队列推送） ----
        queue: asyncio.Queue = asyncio.Queue()
        emit = self._make_emitter(run_id, queue)
        initial_state = {
            "run_id": run_id,
            "thread_id": thread_id,
            "user_input": user_input,
            "history": history or [],
            "intent": {
                "intent_name": intent.intent_name,
                "confidence": intent.confidence,
                "parameters": intent.parameters,
                "reasoning": intent.reasoning,
                "method": intent.method,
            },
        }
        config = self.runtime.build_config(run_id, thread_id, emit)
        task = asyncio.create_task(self._run_graph_task(initial_state, config, queue))

        try:
            while True:
                item = await queue.get()
                if item.get("event") == "__task_done__":
                    break
                yield (item["event"], item["data"])
        finally:
            # 客户端断开时【不取消】图任务：
            # 让 run 完整执行完（事件已落库，状态照常更新），
            # 避免中途取消打断 AsyncSqliteSaver 的 checkpoint 写导致连接挂起。
            # 用户之后可从历史面板查看该 run。
            if task.done() and not task.cancelled():
                try:
                    await task
                except Exception:
                    pass

    async def _run_graph_task(self, initial_state: dict, config: dict, queue: asyncio.Queue):
        """后台执行 LangGraph，推送 run_paused / done / error"""
        run_id = initial_state["run_id"]
        try:
            result = await self.runtime.invoke_graph(initial_state, config)
            if result["interrupted"]:
                self.store.update_run_status(run_id, RUN_INTERRUPTED)
                await self._emit_to_queue(queue, "run_paused", {"run_id": run_id, "status": RUN_INTERRUPTED})
                await self._emit_to_queue(queue, "done", {"run_id": run_id, "status": RUN_INTERRUPTED})
            else:
                run = self.store.get_run(run_id)
                status = (run or {}).get("status", RUN_COMPLETED)
                await self._emit_to_queue(queue, "done", {"run_id": run_id, "status": status})
        except asyncio.CancelledError:
            self.store.update_run_status(run_id, RUN_INTERRUPTED, error="已中断")
            raise
        except Exception as e:
            logger.exception("图执行异常: run=%s", run_id)
            self.store.update_run_status(run_id, RUN_FAILED, error=str(e))
            await self._emit_to_queue(queue, "error", {"content": f"执行异常: {str(e)}"})
            await self._emit_to_queue(queue, "done", {"run_id": run_id, "status": RUN_FAILED})
        finally:
            await queue.put({"event": "__task_done__", "data": {}})

    # ============================================
    # 流式 / 非流式对外接口
    # ============================================
    async def process_stream(
        self, user_input: str, history: Optional[list] = None
    ) -> AsyncGenerator[str, None]:
        async for event, data in self._stream_events(user_input, history):
            yield _sse(event, data)

    async def process(self, user_input: str, history: Optional[list] = None) -> dict:
        """非流式处理（收集事件后返回响应）"""
        intent_name, confidence = "unknown", 0.0
        result_text = ""
        resp: dict = {"intent": "unknown", "confidence": 0, "result": "", "error": None}

        async for event, data in self._stream_events(user_input, history):
            if event == "thinking_result":
                intent_name = data.get("intent", "unknown")
                confidence = data.get("confidence", 0)
            elif event == "plan_created":
                resp.update({
                    "run_id": data.get("run_id"),
                    "plan": {
                        "summary": data.get("summary", ""),
                        "steps": data.get("steps", []),
                    },
                })
            elif event == "tool_result":
                result_text = data.get("content", "")
            elif event == "human_confirm":
                resp.update({
                    "_requires_confirm": True,
                    "confirm_id": data.get("confirm_id"),
                    "question": data.get("question"),
                    "options": data.get("options"),
                    "run_id": data.get("run_id"),
                })
            elif event == "ask_params":
                resp.update({
                    "ask_params": True,
                    "question": data.get("question"),
                    "missing_params": data.get("missing", []),
                })
            elif event == "error":
                resp["error"] = data.get("content", "执行异常")

        resp.update({
            "intent": intent_name,
            "confidence": confidence,
            "result": result_text,
        })
        return resp

    # ============================================
    # 中断恢复（HITL 确认 / 参数补齐）
    # ============================================
    async def resume_run(self, run_id: str, value: Any) -> dict:
        """恢复被中断的运行

        Args:
            run_id: 运行 ID
            value: 用户的选择（"confirm"/"cancel"）或补齐的参数 dict
        """
        run = self.store.get_run(run_id)
        if not run:
            return {"result": "运行不存在", "error": "run_not_found"}
        if not self.runtime:
            return {"result": "服务未初始化", "error": "not_initialized"}

        thread_id = run.get("thread_id") or ("thread_" + uuid.uuid4().hex[:12])
        if not run.get("thread_id"):
            self.store.set_thread_id(run_id, thread_id)

        self.store.clear_stop(run_id)
        self.store.update_run_status(run_id, RUN_RUNNING)
        emit = self._make_emitter(run_id, None)
        config = self.runtime.build_config(run_id, thread_id, emit)

        try:
            result = await self.runtime.resume_graph(config, value)
            interrupted = result["interrupted"]
            if interrupted:
                self.store.update_run_status(run_id, RUN_INTERRUPTED)
                return {
                    "result": self._last_tool_result(run_id) or "已恢复执行，但仍有待确认项",
                    "status": RUN_INTERRUPTED,
                    "interrupted": True,
                    "steps": self.store.get_steps(run_id),
                }
            run_now = self.store.get_run(run_id)
            return {
                "result": self._last_tool_result(run_id) or "执行完成",
                "status": (run_now or {}).get("status", RUN_COMPLETED),
                "steps": self.store.get_steps(run_id),
            }
        except Exception as e:
            logger.exception("恢复执行失败: run=%s", run_id)
            self.store.update_run_status(run_id, RUN_FAILED, error=str(e))
            return {"result": f"恢复执行失败: {str(e)}", "error": str(e)}

    # ============================================
    # 回溯（rewind） / 定点回放（replay）
    # ============================================
    def _build_restart_state(self, run_id: str, target_seq: int,
                             new_run_id: Optional[str] = None) -> tuple[dict, dict, str]:
        """基于已持久化的运行快照，重建从 target_seq 重新执行的初始状态

        Returns:
            (initial_state, config, run_id_for_exec)
        """
        snapshot = self.store.get_run_snapshot(run_id)
        plan = snapshot.get("plan") or {}
        steps = plan.get("steps", [])
        idx = next((i for i, s in enumerate(steps) if s.get("seq") == target_seq), 0)
        if idx >= len(steps):
            raise ValueError(f"目标步骤不存在: seq={target_seq}")

        exec_run_id = new_run_id or run_id
        if new_run_id:
            # 定点回放：克隆计划到新运行，并保留目标步骤之前的已完成结果
            self.store.save_plan(new_run_id, plan)
            for s in snapshot.get("steps", []):
                if s.get("seq") < target_seq and s.get("status") == STEP_COMPLETED and s.get("result"):
                    self.store.mark_step(new_run_id, s["seq"], STEP_COMPLETED, result=s["result"])
            self.store.reset_steps_from(new_run_id, target_seq)
        else:
            # 回溯：原运行内回退
            self.store.reset_steps_from(run_id, target_seq)

        # 目标步骤之前的结果（供协调 Agent / 后续步骤引用）
        step_results = {}
        for s in snapshot.get("steps", []):
            if s.get("seq") < target_seq and s.get("status") == STEP_COMPLETED and s.get("result"):
                step_results[str(s["seq"])] = s["result"]

        thread_id = "thread_" + uuid.uuid4().hex[:12]
        self.store.set_thread_id(exec_run_id, thread_id)
        self.store.clear_stop(exec_run_id)
        self.store.update_run_status(exec_run_id, RUN_RUNNING)

        initial_state = {
            "run_id": exec_run_id,
            "thread_id": thread_id,
            "user_input": snapshot.get("user_input", ""),
            "history": [],
            "intent": snapshot.get("intent", {}),
            "plan": plan,
            "step_results": step_results,
            "current_index": idx,
            "run_status": RUN_RUNNING,
        }
        return initial_state, thread_id, exec_run_id

    async def rewind_run(self, run_id: str, target_seq: int) -> dict:
        """回溯：从指定步骤在原运行内重新执行（该步骤之后的结果将被覆盖）"""
        initial_state, thread_id, exec_run_id = self._build_restart_state(run_id, target_seq)
        emit = self._make_emitter(exec_run_id, None)
        config = self.runtime.build_config(exec_run_id, thread_id, emit)
        result = await self.runtime.invoke_graph(initial_state, config)
        if result["interrupted"]:
            self.store.update_run_status(exec_run_id, RUN_INTERRUPTED)
            status = RUN_INTERRUPTED
        else:
            status = (self.store.get_run(exec_run_id) or {}).get("status")
        return {
            "run_id": exec_run_id,
            "status": status,
            "steps": self.store.get_steps(exec_run_id),
            "result": self._last_tool_result(exec_run_id) or "已从目标步骤重新执行",
        }

    async def replay_run(self, run_id: str, target_seq: int) -> dict:
        """定点回放：从指定步骤克隆出新运行重新执行（原运行历史保留）"""
        snapshot = self.store.get_run_snapshot(run_id)
        new_run = self.store.create_run(snapshot.get("user_input", ""), snapshot.get("intent", {}))
        new_run_id = new_run["run_id"]
        initial_state, thread_id, _ = self._build_restart_state(run_id, target_seq, new_run_id=new_run_id)
        emit = self._make_emitter(new_run_id, None)
        config = self.runtime.build_config(new_run_id, thread_id, emit)
        result = await self.runtime.invoke_graph(initial_state, config)
        if result["interrupted"]:
            self.store.update_run_status(new_run_id, RUN_INTERRUPTED)
            status = RUN_INTERRUPTED
        else:
            status = (self.store.get_run(new_run_id) or {}).get("status")
        return {
            "run_id": new_run_id,
            "status": status,
            "steps": self.store.get_steps(new_run_id),
            "result": self._last_tool_result(new_run_id) or "定点回放完成",
        }

    # ============================================
    # 停止 / 查询
    # ============================================
    def _last_tool_result(self, run_id: str) -> str:
        """从事件日志中提取最后一次 tool_result 内容（最终答复）"""
        try:
            events = self.store.get_events(run_id, limit=100)
            for e in reversed(events):
                if e.get("event_type") == "tool_result":
                    return e.get("data", {}).get("content", "")
        except Exception:
            logger.debug("读取最终答复失败: %s", run_id)
        return ""

    def stop_run(self, run_id: str) -> dict:
        """即时中断：请求停止（执行 Agent 在步骤边界检查该标记）"""
        run = self.store.get_run(run_id)
        if not run:
            return {"result": "运行不存在", "error": "run_not_found"}
        self.store.request_stop(run_id)
        return {"result": "已请求停止，将在当前步骤结束后生效"}

    def get_run_detail(self, run_id: str) -> Optional[dict]:
        run = self.store.get_run(run_id)
        if not run:
            return None
        return {
            "run_id": run.get("run_id"),
            "user_input": run.get("user_input"),
            "intent": run.get("intent_name"),
            "confidence": run.get("intent_confidence"),
            "status": run.get("status"),
            "error": run.get("error"),
            "created_at": run.get("created_at"),
            "updated_at": run.get("updated_at"),
            "plan": json.loads(run.get("plan_json") or "{}"),
            "steps": self.store.get_steps(run_id),
        }

    def list_runs(self, limit: int = 20) -> list[dict]:
        return self.store.list_runs(limit)

    # ============================================
    # 工具
    # ============================================
    def _build_uncertain_msg(self, user_input: str, intent_name: str) -> str:
        suggestions = []
        if any(kw in user_input for kw in ["库存", "物资", "材料"]):
            suggestions.append("查询库存")
        if any(kw in user_input for kw in ["采购", "购买", "买", "进货"]):
            suggestions.append("发起采购")
        if any(kw in user_input for kw in ["订单", "订单编号", "PO"]):
            suggestions.append("查询采购订单")
        if not suggestions:
            return (
                "我没完全理解您的意思，请更具体地描述一下您想做什么？\n\n"
                "例如：\n"
                "- 查询某个物资的库存\n"
                "- 发起采购申请\n"
                "- 查询采购订单"
            )
        return f"您是想要 **{'、'.join(suggestions)}** 吗？请更具体地描述一下。"
