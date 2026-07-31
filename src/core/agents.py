# -*- coding: utf-8 -*-
"""
多 Agent 定义

LangGraph 编排下的三个角色：
- PlannerAgent    规划 Agent：理解意图后，基于现有工具生成执行计划
- ExecutorAgent   执行 Agent：在 StateGraph 的 execute 节点中逐个调用工具
                  （见 src/core/graph/graph.py，与图内逻辑融为一体）
- CoordinatorAgent 协调 Agent：处理无需工具的回答步骤、汇总多步骤结果生成最终回复

三者协作实现"多 Agent 协同"：一个用户需求可能拆成多个工具步骤
（例如 采购 = 查库存 → 批量采购(需人工确认)），由不同 Agent 接力完成。
"""
import json
import logging
from typing import Any, Optional

from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate

from src.config.llm_config import get_current_provider
from src.core.planner import Plan, PlanStep
from src.core.registry import tool_registry

logger = logging.getLogger(__name__)

# ============================================
# 规划 Agent 提示词
# ============================================
PLANNER_SYSTEM_PROMPT = """你是一个采购智能助手的「计划编排 Agent」。
用户提出了需求，你必须：先理解意图 → 基于现有工具列出可执行的计划 → 由执行 Agent 逐步骤执行。

## 可用工具
{tools_description}

## 计划规则
1. 先理解用户意图，把目标拆解为 1 到 N 个有序步骤（每个步骤对应一次工具调用）
2. 步骤的 tool_name 必须是可用工具之一；若某步骤需要综合分析已有结果（不调用工具），tool_name 留空字符串
3. 简单任务（打招呼、查时间、单一查询且参数完整）只给 1 个步骤
4. 复杂任务必须拆成多个步骤实现多工具协同，例如：
   - "采购100吨碳钢钢板" → 步骤1: query_inventory_by_name（先查当前库存） → 步骤2: batch_purchase（发起采购，需人工确认）
   - "查一下碳钢钢板库存是否充足，不足就采购" → 步骤1: query_inventory_by_name → 步骤2: query_low_stock → 步骤3: batch_purchase
5. 从用户输入中提取每个工具所需的参数，提取不到的参数留空，由执行时提示补充
6. 涉及金额、下单、审批、采购等敏感操作，need_confirm 必须设为 true
7. 每个步骤的 title 用一句话概括该步骤做什么（前端计划列表直接展示）
"""

# ============================================
# 协调 Agent 提示词
# ============================================
COORDINATOR_SYSTEM_PROMPT = """你是一个采购智能助手的「协调 Agent」。
多个执行 Agent 已经完成了一系列工具调用，请你综合所有步骤的结果，用简洁、友好的中文回复用户。

## 用户需求
{user_input}

## 执行计划
{plan_summary}

## 各步骤结果
{step_results}

## 要求
- 直接给出最终答复，不要再描述"计划/步骤"本身
- 结论先行，用 Markdown 组织（表格/列表）
- 如果某些步骤失败或缺失信息，如实说明
- 不要编造数据
"""


def _build_tools_desc() -> str:
    """构建规划 Agent 可读的工具描述（含参数名与必填项）"""
    tools = tool_registry.list_tools()
    if not tools:
        return "当前没有可用的工具。"
    lines = []
    for t in tools:
        fn = t["function"]
        name = fn["name"]
        desc = fn.get("description", "")
        params = fn.get("parameters", {})
        props = params.get("properties", {})
        required = params.get("required", [])
        if props:
            arg_parts = []
            for pname, pinfo in props.items():
                req_mark = "必填" if pname in required else "可选"
                arg_parts.append(f"{pname}({pinfo.get('type','str')},{req_mark})")
            arg_str = " 参数: " + ", ".join(arg_parts)
        else:
            arg_str = ""
        lines.append(f"- {name}: {desc}{arg_str}")
    return "\n".join(lines)


class PlannerAgent:
    """规划 Agent：意图理解 → 生成执行计划"""

    def __init__(self):
        provider = get_current_provider()
        if not provider:
            raise ValueError("未配置LLM提供商")
        self.llm = ChatOpenAI(
            model=provider.model,
            openai_api_key=provider.api_key,
            openai_api_base=provider.base_url,
            temperature=0.05,
            timeout=provider.timeout,
        )
        self.parser = PydanticOutputParser(pydantic_object=Plan)
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", PLANNER_SYSTEM_PROMPT),
            ("human", "用户输入: {user_input}\n意图识别结果: {intent_info}\n\n{format_instructions}"),
        ])

    async def create_plan(self, user_input: str, intent: dict) -> dict:
        """生成计划

        Args:
            user_input: 用户输入
            intent: 意图识别结果 {intent_name, confidence, parameters, method, reasoning}

        Returns:
            Plan 的 dict 形式
        """
        intent_name = (intent or {}).get("intent_name", "")
        method = (intent or {}).get("method", "")

        # ---- 快速通道：极简意图（打招呼/时间）无需 LLM，直接生成单步计划 ----
        if method == "keyword" and intent_name in ("greet", "get_current_time"):
            tool = tool_registry.get_tool(intent_name)
            if tool:
                params = (intent or {}).get("parameters", {}) or {}
                missing = tool_registry.check_missing_params(intent_name, params)
                if not missing:
                    plan = Plan(
                        intent=intent_name,
                        confidence=(intent or {}).get("confidence", 0.85),
                        summary=tool.description,
                        steps=[PlanStep(
                            seq=1,
                            title=tool.description,
                            description=tool.description,
                            tool_name=intent_name,
                            params=params,
                        )],
                    )
                    return plan.to_dict()

        # 其余场景（含复杂采购需求）一律走 LLM 规划，支持多工具协同拆分
        tools_desc = _build_tools_desc()
        intent_info = json.dumps(intent or {}, ensure_ascii=False)
        try:
            messages = self.prompt.format_messages(
                tools_description=tools_desc,
                user_input=user_input,
                intent_info=intent_info,
                format_instructions=self.parser.get_format_instructions(),
            )
            response = await self.llm.ainvoke(messages)
            plan = self.parser.parse(response.content)
            # 兜底：LLM 未生成任何步骤时给一个回复步骤
            if not plan.steps:
                plan.steps = [PlanStep(seq=1, title="回复用户", description="直接回复用户", tool_name="")]
            return plan.to_dict()
        except Exception as e:
            logger.warning("计划生成失败，使用兜底单步计划: %s", e)
            return Plan(
                intent=intent_name or "reply",
                confidence=0.5,
                summary="回复用户",
                steps=[PlanStep(seq=1, title="回复用户", description="直接回复用户", tool_name="")],
            ).to_dict()


class CoordinatorAgent:
    """协调 Agent：处理无工具步骤 + 汇总最终回复"""

    def __init__(self):
        provider = get_current_provider()
        self.llm = None
        if provider:
            self.llm = ChatOpenAI(
                model=provider.model,
                openai_api_key=provider.api_key,
                openai_api_base=provider.base_url,
                temperature=0.2,
                timeout=provider.timeout,
            )

    async def answer(self, step: dict, step_results: dict) -> str:
        """处理无需工具的回答步骤（多 Agent 协同：基于前面步骤结果作答）"""
        context = "\n".join(
            f"[步骤 {k}] {v[:2000]}" for k, v in (step_results or {}).items()
        )
        if not self.llm:
            return "（无法调用协调 Agent）" + (context or step.get("description", ""))
        try:
            prompt = ChatPromptTemplate.from_messages([
                ("system", "你是采购智能助手的「协调 Agent」。请基于上下文回答用户，不要编造。"),
                ("human", "任务: {task}\n\n已有上下文:\n{context}"),
            ])
            messages = prompt.format_messages(
                task=step.get("description", step.get("title", "")),
                context=context or "无",
            )
            resp = await self.llm.ainvoke(messages)
            return str(resp.content)
        except Exception as e:
            logger.warning("协调 Agent 回答失败: %s", e)
            return context or step.get("description", "")

    async def synthesize(self, user_input: str, plan: dict, steps: list[dict]) -> str:
        """汇总所有步骤结果，生成最终回复

        - 单步骤且成功 → 直接返回该步骤结果（省一次 LLM 调用）
        - 多步骤 / 有失败 → LLM 综合生成
        """
        completed = [s for s in steps if s.get("status") == "completed" and s.get("result")]
        failed = [s for s in steps if s.get("status") == "failed"]
        cancelled = [s for s in steps if s.get("status") == "interrupted"]

        # 用户取消了关键操作 → 直接告知
        if cancelled:
            return "❌ 操作已取消，未继续执行后续步骤。如需重新开始，请在计划中点击「从该步回溯」。"

        # 单步骤直接返回
        if len(completed) == 1 and not failed and len(steps) == 1:
            return completed[0]["result"]

        if not self.llm:
            return "\n\n".join(s.get("result", "") for s in completed) or "（无可用结果）"

        step_lines = []
        for s in steps:
            status_cn = {
                "pending": "待执行", "running": "执行中", "completed": "已完成",
                "failed": "失败", "interrupted": "已中断", "awaiting_confirm": "待确认",
            }.get(s.get("status"), s.get("status"))
            head = f"{s.get('seq')}. {s.get('title', '')} [{status_cn}]"
            if s.get("result"):
                step_lines.append(f"{head}\n{s['result'][:1500]}")
            elif s.get("error"):
                step_lines.append(f"{head}\n错误: {s['error']}")
            else:
                step_lines.append(head)

        summary = plan.get("summary", "")
        try:
            messages = ChatPromptTemplate.from_messages([
                ("system", COORDINATOR_SYSTEM_PROMPT),
            ]).format_messages(
                user_input=user_input,
                plan_summary=summary or "（无）",
                step_results="\n\n".join(step_lines),
            )
            resp = await self.llm.ainvoke(messages)
            return str(resp.content)
        except Exception as e:
            logger.warning("最终汇总失败，退化为拼接: %s", e)
            return "\n\n".join(s.get("result", "") for s in completed) or "（执行完成）"


# 全局实例
planner_agent = PlannerAgent()
coordinator_agent = CoordinatorAgent()
