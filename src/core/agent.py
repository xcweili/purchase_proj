# -*- coding: utf-8 -*-
"""
Agent 编排器
整合意图识别 + Tool 注册 + Tool 调用 + 多轮参数补齐
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
    4. 检测缺失参数并触发多轮补齐
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

    # ============================================
    # 缺失参数检测
    # ============================================
    def _check_missing(self, intent_name: str, params: dict) -> list[str]:
        """检查工具必填参数是否齐全"""
        return self.registry.check_missing_params(intent_name, params)

    def _build_missing_msg(self, intent_name: str, missing: list[str]) -> str:
        """构建提示用户补全参数的回复"""
        tool = self.registry.get_tool(intent_name)
        tool_desc = tool.description if tool else intent_name
        param_names_cn = {
            "order_id": "订单编号",
            "supplier": "供应商名称",
            "name": "物资名称",
            "threshold": "库存阈值",
        }
        need = "、".join(param_names_cn.get(p, p) for p in missing)
        return f"您想查询{tool_desc}，请提供以下信息：**{need}**"

    async def process(self, user_input: str, history: list | None = None, **context) -> dict:
        """处理用户输入（非流式）

        返回包含 missing_params 和 pending_intent 字段，
        上层调用方可通过这些字段判断是否需要追问。
        """
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

        # 检查必填参数
        params = {**intent.parameters, **context}
        missing = self._check_missing(intent.intent_name, params)
        if missing:
            logger.info("参数不完整: [%s] 缺失=%s", intent.intent_name, missing)
            return {
                "intent": intent.intent_name,
                "confidence": intent.confidence,
                "result": self._build_missing_msg(intent.intent_name, missing),
                "error": None,
                "reasoning": intent.reasoning,
                "missing_params": missing,
                "pending_intent": intent.intent_name,
                "pending_params": params,
            }

        # 参数齐全，执行工具
        try:
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

        事件：
            thinking_start   — 开始意图识别
            thinking_result  — 意图识别结果
            tool_start       — 开始执行工具
            tool_result      — 工具执行结果
            ask_params       — 需要用户补全参数（含 missing_params/pending_intent）
            error            — 错误
            done             — 完成
        """
        def _sse(event: str, data: Any) -> str:
            return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

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

        if intent.intent_name == "unknown" or intent.confidence < 0.3:
            yield _sse("error", {"content": "无法识别意图"})
            yield _sse("done", {})
            return

        # 检查必填参数
        params = {**intent.parameters, **context}
        missing = self._check_missing(intent.intent_name, params)
        if missing:
            msg = self._build_missing_msg(intent.intent_name, missing)
            yield _sse("ask_params", {
                "content": msg,
                "missing_params": missing,
                "pending_intent": intent.intent_name,
                "pending_params": params,
            })
            yield _sse("done", {})
            return

        # 参数齐全，执行工具
        yield _sse("tool_start", {"content": f"正在调用工具: {intent.intent_name}"})

        try:
            logger.info("[流式] 开始执行工具: [%s] 参数=%s", intent.intent_name, params)
            result = await self.registry.execute(intent.intent_name, **params)
            logger.info("[流式] 工具执行完成: [%s]", intent.intent_name)
            yield _sse("tool_result", {"content": str(result)})
        except Exception as e:
            logger.exception("工具执行失败: %s", intent.intent_name)
            yield _sse("error", {"content": f"工具执行失败: {str(e)}"})

        yield _sse("done", {})

    async def fill_pending_params(
        self, user_input: str, intent_name: str, missing_params: list[str], partial_params: dict
    ) -> dict:
        """尝试从用户输入中提取缺失的参数

        Args:
            user_input: 用户输入文本
            intent_name: 待执行的工具名
            missing_params: 还缺失的参数列表
            partial_params: 已有的部分参数

        Returns:
            {
                "filled": 是否全部补齐,
                "params": 补齐后的完整参数,
                "still_missing": 仍然缺失的参数,
                "result": 补齐后的执行结果（如果全部补齐）,
                "msg": 提示信息
            }
        """
        # 用 LLM 从用户输入中提取缺失参数值
        from src.config.llm_config import get_current_provider
        from langchain_openai import ChatOpenAI
        from langchain_core.prompts import ChatPromptTemplate

        provider = get_current_provider()
        if not provider:
            return {"filled": False, "params": partial_params, "still_missing": missing_params,
                    "result": None, "msg": "服务配置异常"}

        llm = ChatOpenAI(
            model=provider.model,
            openai_api_key=provider.api_key,
            openai_api_base=provider.base_url,
            temperature=0,
            timeout=provider.timeout,
        )

        prompt = ChatPromptTemplate.from_messages([
            ("system", "你是一个参数提取助手。从用户输入中提取指定参数的值，返回纯 JSON，不要包含其他文字。"),
            ("human", (
                "用户输入: {user_input}\n"
                "需要提取的参数: {missing_params}\n"
                "已确定的参数: {partial_params}\n"
                "请只返回JSON: {{{{提取的参数名: 提取的值}}}}"
            )),
        ])

        try:
            messages = prompt.format_messages(
                user_input=user_input,
                missing_params=json.dumps(missing_params, ensure_ascii=False),
                partial_params=json.dumps(partial_params, ensure_ascii=False),
            )
            response = await llm.ainvoke(messages)

            import re
            json_match = re.search(r'\{.*\}', response.content, re.DOTALL)
            if json_match:
                extracted = json.loads(json_match.group(0))
                params = {**partial_params}
                for k, v in extracted.items():
                    if v is not None and str(v).strip():
                        params[k] = v

                still_missing = self._check_missing(intent_name, params)
                if still_missing:
                    return {
                        "filled": False, "params": params, "still_missing": still_missing,
                        "result": None,
                        "msg": self._build_missing_msg(intent_name, still_missing),
                    }

                # 全部齐全，执行
                result = await self.registry.execute(intent_name, **params)
                return {
                    "filled": True, "params": params, "still_missing": [],
                    "result": str(result), "msg": None,
                }
        except Exception as e:
            logger.warning("参数提取失败: %s", e)

        return {
            "filled": False, "params": partial_params, "still_missing": missing_params,
            "result": None, "msg": self._build_missing_msg(intent_name, missing_params),
        }
