# -*- coding: utf-8 -*-
"""
意图识别模块
基于视频《智能体意图识别如何做？》的四种方案，采用混合方案：
  1. 关键词/规则快速匹配（第一层，低延迟）
  2. LLM 结构化输出（第二层，高准确率）
  3. 多轮意图切换检测
  4. 兜底意图 + 分层识别
"""
import re
import logging
from typing import Optional
from pydantic import BaseModel, Field

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser

from src.config.llm_config import get_current_provider

logger = logging.getLogger(__name__)


class IntentResult(BaseModel):
    """意图识别结果"""
    intent_name: str = Field(description="识别到的意图名称，对应已注册的工具名称，不匹配返回 unknown")
    confidence: float = Field(description="置信度 (0-1)")
    parameters: dict = Field(default_factory=dict, description="从用户输入中提取的参数键值对")
    reasoning: str = Field(default="", description="推理过程说明")
    method: str = Field(default="llm", description="识别方式: keyword / llm")


# ============================================
# 增强的系统提示词（含分层设计、示例、意图切换检测）
# ============================================
INTENT_SYSTEM_PROMPT = """你是一个智能助手，负责识别用户意图并从输入中提取参数。

## 可用工具列表
{tools_description}

## 识别规则
1. 根据用户输入选择最匹配的一个工具
2. 从输入中提取该工具所需的参数
3. 如果用户输入明显不匹配任何工具，intent_name 返回 "unknown"
4. 如果匹配到工具但缺少必要参数，尽量从上下文中推断，推断不出则在 parameters 中留空

## 多轮对话意图切换检测
- 如果用户正在追问上一轮的细节（如"具体有哪些"、"详细信息"、"为什么"），保持原意图
- 如果用户提出了全新的需求（话题改变），切换到新意图
- 如果用户表达感谢、告别等社交用语，匹配到对应的社交工具

## 示例
用户: "帮我查一下碳钢钢板的库存"
意图: query_inventory_by_name, 参数: {{"name": "碳钢钢板"}}

用户: "查一下库存不足的东西"
意图: query_low_stock, 参数: {{"threshold": 100}}

用户: "你好"
意图: greet, 参数: {{}}
"""


class KeywordMatcher:
    """第一层：关键词/规则快速匹配

    适合意图少、用户表达固定的场景。
    匹配成功 → 直接返回，不调 LLM（节省成本 + 低延迟）
    匹配失败 → 交给 LLM 识别
    """

    def __init__(self):
        self._rules: list[dict] = []

    def add_rule(self, intent_name: str, keywords: list[str], confidence: float = 0.85):
        """添加关键词规则

        Args:
            intent_name: 匹配到的意图名
            keywords: 触发关键词列表（任一匹配即命中）
            confidence: 匹配成功时的置信度
        """
        self._rules.append({
            "intent": intent_name,
            "keywords": keywords,
            "confidence": confidence,
        })

    def match(self, text: str) -> Optional[IntentResult]:
        """尝试关键词匹配"""
        for rule in self._rules:
            for kw in rule["keywords"]:
                if kw in text:
                    logger.info("关键词命中: [%s] ← 匹配到 '%s'", rule["intent"], kw)
                    return IntentResult(
                        intent_name=rule["intent"],
                        confidence=rule["confidence"],
                        parameters={},
                        reasoning=f"关键词「{kw}」触发匹配",
                        method="keyword",
                    )
        return None

    def match_with_params(
        self, text: str, intent_name: str, patterns: list[tuple[str, str, type]]
    ) -> Optional[IntentResult]:
        """带参数提取的关键词匹配

        Args:
            text: 用户输入
            intent_name: 意图名
            patterns: [(关键词, 参数名, 参数类型), ...]
        """
        for keyword, param_name, _ in patterns:
            if keyword not in text:
                continue
            # 尝试从文本中提取参数值（冒号/空格后的内容）
            match = re.search(rf'{re.escape(keyword)}\s*[:：]?\s*(\S+)', text)
            param_value = match.group(1) if match else ""
            logger.info("关键词+参数命中: [%s] %s=%s", intent_name, param_name, param_value)
            return IntentResult(
                intent_name=intent_name,
                confidence=0.8,
                parameters={param_name: param_value},
                reasoning=f"关键词「{keyword}」触发匹配，提取参数 {param_name}={param_value}",
                method="keyword",
            )
        return None


# 全局关键词匹配器
keyword_matcher = KeywordMatcher()


class IntentRecognizer:
    """意图识别器（混合方案）

    第一层：关键词规则快速匹配
    第二层：LLM 结构化输出精确识别
    """

    def __init__(self, tools_description: str):
        """
        Args:
            tools_description: 工具列表描述文本
        """
        provider = get_current_provider()
        if not provider:
            raise ValueError("未配置LLM提供商")

        self.llm = ChatOpenAI(
            model=provider.model,
            openai_api_key=provider.api_key,
            openai_api_base=provider.base_url,
            temperature=0.05,  # 意图识别使用极低温度，减少随机性
            timeout=provider.timeout,
        )

        self.parser = PydanticOutputParser(pydantic_object=IntentResult)

        self.base_prompt = ChatPromptTemplate.from_messages([
            ("system", INTENT_SYSTEM_PROMPT),
            ("human", "用户输入: {user_input}\n\n{format_instructions}"),
        ])

        self.history_prompt = ChatPromptTemplate.from_messages([
            ("system", INTENT_SYSTEM_PROMPT),
            ("placeholder", "{history}"),
            ("human", "用户输入: {user_input}\n\n{format_instructions}"),
        ])

        self.tools_description = tools_description

        # ===== 注册关键词规则 =====
        self._register_keyword_rules()

    def _register_keyword_rules(self):
        """注册关键词快速匹配规则"""
        # 社交类
        keyword_matcher.add_rule("greet", ["你好", "您好", "嗨", "hello", "hi", "hey"])
        keyword_matcher.add_rule("get_current_time", ["时间", "几点了", "日期", "今天几号"])

        # 库存查询类（带参数提取）
        # 这些会在 recognize() 中特殊处理
        logger.debug("关键词规则已注册")

    def _build_messages(self, user_input: str, history: list | None = None):
        """构建 LLM 消息列表（含历史上下文）"""
        if not history:
            return self.base_prompt.format_messages(
                tools_description=self.tools_description,
                user_input=user_input,
                format_instructions=self.parser.get_format_instructions(),
            )

        from langchain_core.messages import HumanMessage, AIMessage
        lc_history = []
        for msg in history[-6:]:  # 只保留最近6轮，防止超长
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if role == "user":
                lc_history.append(HumanMessage(content=content))
            else:
                lc_history.append(AIMessage(content=content))

        return self.history_prompt.format_messages(
            tools_description=self.tools_description,
            history=lc_history,
            user_input=user_input,
            format_instructions=self.parser.get_format_instructions(),
        )

    def recognize(self, user_input: str, history: list | None = None) -> IntentResult:
        """识别用户意图（混合方案）

        Args:
            user_input: 用户输入的文本
            history: 对话历史

        Returns:
            IntentResult: 意图识别结果
        """
        text = user_input.strip().lower()

        # === 第一层：关键词快速匹配 ===
        # 1a. 简单关键词匹配
        result = keyword_matcher.match(text)
        if result:
            return result

        # 1b. 库存查询关键词（带参数提取）
        inventory_patterns = [
            ("库存", "name", str),
            ("物资", "name", str),
            ("材料", "name", str),
        ]
        result = keyword_matcher.match_with_params(text, "query_inventory_by_name", inventory_patterns)
        if result:
            return result

        # 1c. 低库存关键词
        if any(kw in text for kw in ["库存不足", "低库存", "缺货", "库存告急", "预警"]):
            # 尝试提取阈值
            threshold_match = re.search(r'低于\s*(\d+)|小于\s*(\d+)|(\d+)\s*以下', text)
            threshold = int(threshold_match.group(1) or threshold_match.group(2) or threshold_match.group(3) or 100)
            logger.info("关键词命中: [query_low_stock] 阈值=%d", threshold)
            return IntentResult(
                intent_name="query_low_stock",
                confidence=0.85,
                parameters={"threshold": threshold},
                reasoning="关键词「库存不足」触发匹配",
                method="keyword",
            )

        # === 第二层：LLM 意图识别 ===
        return self._recognize_with_llm(user_input, history)

    def _recognize_with_llm(self, user_input: str, history: list | None = None) -> IntentResult:
        """LLM 意图识别"""
        provider = get_current_provider()
        logger.info(
            "调用 LLM 意图识别: provider=%s model=%s",
            provider.name if provider else "N/A",
            provider.model if provider else "N/A",
        )

        messages = self._build_messages(user_input, history)
        response = self.llm.invoke(messages)
        result = self.parser.parse(response.content)
        result.method = "llm"

        logger.info(
            "LLM 返回: intent=%s confidence=%.2f params=%s",
            result.intent_name, result.confidence, result.parameters,
        )
        return result

    async def recognize_async(self, user_input: str, history: list | None = None) -> IntentResult:
        """异步识别用户意图（混合方案）"""
        text = user_input.strip().lower()

        # === 第一层：关键词快速匹配 ===
        result = keyword_matcher.match(text)
        if result:
            return result

        # 库存查询关键词
        inventory_patterns = [
            ("库存", "name", str),
            ("物资", "name", str),
            ("材料", "name", str),
        ]
        result = keyword_matcher.match_with_params(text, "query_inventory_by_name", inventory_patterns)
        if result:
            return result

        # 低库存关键词
        if any(kw in text for kw in ["库存不足", "低库存", "缺货", "库存告急", "预警"]):
            threshold_match = re.search(r'低于\s*(\d+)|小于\s*(\d+)|(\d+)\s*以下', text)
            threshold = int(threshold_match.group(1) or threshold_match.group(2) or threshold_match.group(3) or 100)
            logger.info("关键词命中: [query_low_stock] 阈值=%d", threshold)
            return IntentResult(
                intent_name="query_low_stock",
                confidence=0.85,
                parameters={"threshold": threshold},
                reasoning="关键词「库存不足」触发匹配",
                method="keyword",
            )

        # === 第二层：LLM 意图识别 ===
        provider = get_current_provider()
        logger.info(
            "调用 LLM 意图识别: provider=%s model=%s",
            provider.name if provider else "N/A",
            provider.model if provider else "N/A",
        )

        messages = self._build_messages(user_input, history)
        response = await self.llm.ainvoke(messages)
        result = self.parser.parse(response.content)
        result.method = "llm"

        logger.info(
            "LLM 返回: intent=%s confidence=%.2f params=%s reasoning=%s",
            result.intent_name, result.confidence, result.parameters, result.reasoning,
        )
        return result
