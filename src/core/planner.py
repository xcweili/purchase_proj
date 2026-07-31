# -*- coding: utf-8 -*-
"""
计划模型

LangGraph 思路下的核心数据结构：每次执行 Agent 之前，先理解意图，
再基于现有工具列出执行计划（Plan），然后逐步骤执行。
"""
from typing import Optional
from pydantic import BaseModel, Field


class PlanStep(BaseModel):
    """计划中的单个步骤

    - tool_name 为空时，表示该步骤不需要调用工具，由"协调 Agent"基于上下文直接回答
      （多 Agent 协同场景）
    """
    seq: int = Field(description="步骤序号，从 1 开始")
    title: str = Field(description="步骤标题（简短，展示在计划列表中）")
    description: str = Field(default="", description="步骤详细说明")
    tool_name: str = Field(default="", description="要调用的工具名，来自可用工具列表；无合适工具则为空字符串")
    params: dict = Field(default_factory=dict, description="执行工具所需的参数字典")
    depends_on: list[int] = Field(default_factory=list, description="依赖的步骤序号列表")
    need_confirm: bool = Field(default=False, description="该步骤是否需要人工确认（HITL）")

    def to_dict(self) -> dict:
        return self.model_dump()


class Plan(BaseModel):
    """执行计划：意图理解后的产物"""
    intent: str = Field(default="", description="理解到的意图名称")
    confidence: float = Field(default=0.8, description="意图置信度 0-1")
    summary: str = Field(default="", description="一句话说明本次要完成的目标")
    steps: list[PlanStep] = Field(default_factory=list, description="按顺序执行的计划步骤")

    def to_dict(self) -> dict:
        return self.model_dump()
