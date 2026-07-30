# -*- coding: utf-8 -*-
"""
对话服务
封装 Agent，提供工具注册、对话处理和**多轮参数补齐**能力
"""
import json
import logging
from typing import Any, AsyncGenerator, Optional

from src.core.agent import Agent
from src.tools.base import tool

# 导入独立工具模块（导入即触发 @tool 装饰器注册）
import src.tools.inventory_tools
import src.tools.purchase_tools

logger = logging.getLogger(__name__)


class ChatService:
    """对话服务

    负责：
    1. 初始化 Agent
    2. 注册业务工具
    3. 处理用户对话（流式/非流式）
    4. 多轮参数补齐 — 当工具必填参数缺失时，追问用户并自动补齐
    """

    def __init__(self):
        self.agent: Agent | None = None
        # 多轮参数补齐状态
        self._pending_intent: Optional[str] = None       # 待执行工具名
        self._pending_missing: list[str] = []             # 仍缺失的参数
        self._pending_params: dict = {}                   # 已有部分参数

    def initialize(self):
        """初始化 Agent 并注册默认工具"""
        logger.info("=" * 40)
        logger.info("开始初始化 ChatService...")
        self.agent = Agent()
        logger.info("Agent 实例创建完成，开始注册工具...")
        self._register_builtin_tools()
        self.agent.refresh_recognizer()
        logger.info("ChatService 初始化完成")

    def _register_builtin_tools(self):
        """注册内置工具"""
        logger.info("注册内置工具...")

        @self.agent.register_tool(description="向用户打招呼")
        def greet(name: str = "朋友"):
            """向用户打招呼"""
            return f"你好，{name}！我是采购智能助手，有什么可以帮你的？"

        @self.agent.register_tool(description="获取当前时间")
        def get_current_time():
            """获取当前日期和时间"""
            from datetime import datetime
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logger.info("内置工具注册完成")

    def get_available_tools(self) -> list[dict]:
        """获取可用工具列表"""
        if not self.agent:
            return []
        return self.agent.get_tool_list()

    def clear_pending(self):
        """清除待办参数补齐状态"""
        self._pending_intent = None
        self._pending_missing = []
        self._pending_params = {}
        logger.debug("待办参数补齐状态已清除")

    async def process(self, user_input: str, history: list | None = None) -> dict:
        """非流式处理用户输入（支持 pending 补齐）"""
        if not self.agent:
            return {"intent": "unknown", "confidence": 0, "result": "", "error": "服务未初始化"}

        # 如果有待办补齐，先处理补齐
        if self._pending_intent:
            logger.info("检测到待办补齐: [%s] 缺失=%s", self._pending_intent, self._pending_missing)
            fill = await self.agent.fill_pending_params(
                user_input=user_input,
                intent_name=self._pending_intent,
                missing_params=self._pending_missing,
                partial_params=self._pending_params,
            )
            if fill["filled"]:
                self.clear_pending()
                return {
                    "intent": self._pending_intent,
                    "confidence": 1.0,
                    "result": fill["result"],
                    "error": None,
                    "reasoning": "多轮补齐后执行工具",
                }
            else:
                # 还没补齐，更新状态继续问
                self._pending_params = fill["params"]
                self._pending_missing = fill["still_missing"]
                return {
                    "intent": self._pending_intent,
                    "confidence": 1.0,
                    "result": fill["msg"],
                    "error": None,
                    "reasoning": "参数仍未补齐",
                    "missing_params": fill["still_missing"],
                    "pending_intent": self._pending_intent,
                    "pending_params": fill["params"],
                }

        # 正常处理
        result = await self.agent.process(user_input, history=history)

        # 如果返回了 pending_intent，保存状态
        if result.get("pending_intent"):
            self._pending_intent = result["pending_intent"]
            self._pending_missing = result.get("missing_params", [])
            self._pending_params = result.get("pending_params", {})

        return result

    async def process_stream(
        self, user_input: str, history: list | None = None
    ) -> AsyncGenerator[str, None]:
        """流式处理用户输入（支持 pending 补齐）"""
        if not self.agent:
            yield f"event: error\ndata: {json.dumps({'content': '服务未初始化'}, ensure_ascii=False)}\n\n"
            yield "event: done\ndata: {}\n\n"
            return

        def _sse(event: str, data: Any = None) -> str:
            return f"event: {event}\ndata: {json.dumps(data or {}, ensure_ascii=False)}\n\n"

        # 如果有待办补齐，先处理补齐
        if self._pending_intent:
            logger.info("检测到待办补齐: [%s] 缺失=%s", self._pending_intent, self._pending_missing)
            yield _sse("thinking_start", {"content": "正在提取补充信息..."})

            fill = await self.agent.fill_pending_params(
                user_input=user_input,
                intent_name=self._pending_intent,
                missing_params=self._pending_missing,
                partial_params=self._pending_params,
            )

            if fill["filled"]:
                self.clear_pending()
                yield _sse("tool_result", {"content": fill["result"]})
            else:
                self._pending_params = fill["params"]
                self._pending_missing = fill["still_missing"]
                yield _sse("ask_params", {
                    "content": fill["msg"],
                    "missing_params": fill["still_missing"],
                    "pending_intent": self._pending_intent,
                    "pending_params": fill["params"],
                })

            yield _sse("done", {})
            return

        # 正常流式处理
        result_text = ""
        pending_intent = None
        pending_missing = []
        pending_params = {}

        async for event_str in self.agent.process_stream(user_input, history=history):
            yield event_str
            # 从 SSE 事件中捕捉 ask_params
            if event_str.startswith("event: ask_params"):
                import re
                m = re.search(r'data: (\{.*\})', event_str)
                if m:
                    data = json.loads(m.group(1))
                    pending_intent = data.get("pending_intent")
                    pending_missing = data.get("missing_params", [])
                    pending_params = data.get("pending_params", {})

        # 保存 pending 状态
        if pending_intent:
            self._pending_intent = pending_intent
            self._pending_missing = pending_missing
            self._pending_params = pending_params
