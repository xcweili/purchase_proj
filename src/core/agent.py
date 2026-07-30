# -*- coding: utf-8 -*-
"""
Agent 编排器
整合意图识别 + Tool 注册 + Tool 调用
"""
import json
import logging
from typing import Any, AsyncGenerator, Optional

from src.core.registry import tool_registry
from src.core.intent import IntentRecognizer

logger = logging.getLogger(__name__)


class Agent:
    """LangChain Agent 编排器

    职责：
    1. 注册/管理 Tool
    2. 识别用户意图
    3. 执行对应的 Tool
    """

    def __init__(self):
        self.registry = tool_registry
        self._recognizer: Optional[IntentRecognizer] = None

    @property
    def recognizer(self) -> IntentRecognizer:
        if self._recognizer is None:
            tools_desc = self._build_tools_description()
            self._recognizer = IntentRecognizer(tools_desc)
        return self._recognizer

    def _build_tools_description(self) -> str:
        """构建工具描述文本"""
        tools = self.registry.list_tools_simple()
        if not tools:
            return "当前没有可用的工具。"
        lines = []
        for t in tools:
            lines.append(f"- {t['name']}: {t['description']}")
        return "\n".join(lines)

    def refresh_recognizer(self):
        """刷新意图识别器（注册新工具后调用）"""
        self._recognizer = None

    def register_tool(
        self,
        name: Optional[str] = None,
        description: str = "",
        args_schema: Optional[Any] = None,
    ):
        """注册工具（装饰器）"""
        def decorator(func):
            wrapped = self.registry.register(
                name=name,
                description=description,
                args_schema=args_schema,
            )(func)
            self._recognizer = None
            return wrapped
        return decorator

    def get_tool_list(self) -> list[dict]:
        """获取工具列表"""
        return self.registry.list_tools_simple()

    async def process(self, user_input: str, history: list | None = None, **context) -> dict:
        """处理用户输入（非流式）"""
        logger.info("=" * 40)
        logger.info("收到用户输入: %s", user_input)
        logger.info("历史消息数: %d", len(history or []))

        intent = await self.recognizer.recognize_async(user_input, history=history)
        logger.info(
            "意图识别结果: [%s] 置信度=%.2f 参数=%s 推理=%s",
            intent.intent_name, intent.confidence, intent.parameters, intent.reasoning,
        )

        if intent.intent_name == "unknown" or intent.confidence < 0.3:
            return {
                "intent": intent.intent_name,
                "confidence": intent.confidence,
                "result": None,
                "error": "无法识别意图",
                "reasoning": intent.reasoning,
            }

        try:
            params = {**intent.parameters, **context}
            logger.info("开始执行工具: [%s] 参数=%s", intent.intent_name, params)
            result = await self.registry.execute(intent.intent_name, **params)
            logger.info("工具执行完成: [%s] 结果=%s", intent.intent_name, str(result)[:200])
            return {
                "intent": intent.intent_name,
                "confidence": intent.confidence,
                "result": result,
                "error": None,
                "reasoning": intent.reasoning,
            }
        except Exception as e:
            logger.exception("工具执行失败: %s", intent.intent_name)
            return {
                "intent": intent.intent_name,
                "confidence": intent.confidence,
                "result": None,
                "error": f"工具执行失败: {str(e)}",
                "reasoning": intent.reasoning,
            }

    async def process_stream(
        self, user_input: str, history: list | None = None, **context
    ) -> AsyncGenerator[str, None]:
        """处理用户输入（流式，SSE 事件）

        产生的事件：
            thinking_start  — 开始意图识别
            thinking_result — 意图识别结果（含意图名、置信度、推理过程）
            tool_start      — 开始执行工具
            tool_result     — 工具执行结果
            error           — 错误信息
            done            — 完成信号
        """
        def _sse(event: str, data: Any) -> str:
            return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

        # 1. 意图识别阶段
        yield _sse("thinking_start", {"content": "正在分析意图..."})

        intent = await self.recognizer.recognize_async(user_input, history=history)
        logger.info(
            "意图识别: intent=%s, confidence=%.2f, params=%s",
            intent.intent_name, intent.confidence, intent.parameters,
        )

        yield _sse("thinking_result", {
            "intent": intent.intent_name,
            "confidence": intent.confidence,
            "reasoning": intent.reasoning,
            "parameters": intent.parameters,
        })

        # 2. 检查能否识别
        if intent.intent_name == "unknown" or intent.confidence < 0.3:
            yield _sse("error", {"content": "无法识别意图"})
            yield _sse("done", {})
            return

        # 3. 工具执行阶段
        yield _sse("tool_start", {"content": f"正在调用工具: {intent.intent_name}"})

        try:
            params = {**intent.parameters, **context}
            logger.info("[流式] 开始执行工具: [%s] 参数=%s", intent.intent_name, params)
            result = await self.registry.execute(intent.intent_name, **params)
            logger.info("[流式] 工具执行完成: [%s]", intent.intent_name)
            yield _sse("tool_result", {"content": str(result)})
        except Exception as e:
            logger.exception("工具执行失败: %s", intent.intent_name)
            yield _sse("error", {"content": f"工具执行失败: {str(e)}"})

        yield _sse("done", {})

    async def chat(self, user_input: str, **context) -> str:
        """简洁的对话接口（直接返回文本结果）"""
        result = await self.process(user_input, **context)
        if result["error"]:
            return f"抱歉，{result['error']}"
        return str(result["result"])
