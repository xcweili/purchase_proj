# -*- coding: utf-8 -*-
"""
LangGraph 图状态定义
"""
from typing import Any, TypedDict


class GraphState(TypedDict, total=False):
    # 运行标识
    run_id: str
    thread_id: str
    # 输入
    user_input: str
    history: list[dict]
    # 意图理解结果
    intent: dict
    # 执行计划（Plan.to_dict()）
    plan: dict
    # 当前执行到的步骤索引
    current_index: int
    # 已完成的步骤结果 {seq: result}
    step_results: dict
    # 待人工确认的载荷（触发 interrupt 时写入）
    pending_confirm: dict
    # 停止/回溯请求
    stop_requested: bool
    rewind: bool
    # 运行信息
    error: str
    final_answer: str
    run_status: str
