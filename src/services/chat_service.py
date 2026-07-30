# -*- coding: utf-8 -*-
"""
对话服务
封装 Agent，提供工具注册和对话处理能力
"""
import logging
from typing import AsyncGenerator

from src.core.agent import Agent
from src.tools.base import tool

# 导入独立工具模块（导入即触发 @tool 装饰器注册）
import src.tools.inventory_tools

logger = logging.getLogger(__name__)


class ChatService:
    """对话服务

    负责：
    1. 初始化 Agent
    2. 注册业务工具
    3. 处理用户对话（流式/非流式）
    """

    def __init__(self):
        self.agent: Agent | None = None

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

    async def process(self, user_input: str, history: list | None = None) -> dict:
        """非流式处理用户输入"""
        if not self.agent:
            return {"intent": "unknown", "confidence": 0, "result": "", "error": "服务未初始化"}
        return await self.agent.process(user_input, history=history)

    async def process_stream(
        self, user_input: str, history: list | None = None
    ) -> AsyncGenerator[str, None]:
        """流式处理用户输入，产生 SSE 事件字符串"""
        if not self.agent:
            import json
            yield f"event: error\ndata: {json.dumps({'content': '服务未初始化'}, ensure_ascii=False)}\n\n"
            yield "event: done\ndata: {}\n\n"
            return

        async for event in self.agent.process_stream(user_input, history=history):
            yield event
