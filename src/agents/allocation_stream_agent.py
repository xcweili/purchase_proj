# -*- coding: utf-8 -*-
"""仓库调配智能体 - 流式版本"""
import json
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, Any, List


def _convert_for_json(obj):
    """将无法序列化的类型转换为可序列化格式"""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(obj, date):
        return obj.strftime("%Y-%m-%d")
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _extract_minimal_stock_for_stream(stock: Dict[str, Any]) -> Dict[str, Any]:
    """提取库存关键字段用于流式接口"""
    return {
        "仓库编码": stock.get('loc_code') or stock.get('warehouseCode', ''),
        "仓库名称": stock.get('loc_name') or stock.get('warehouseName', ''),
        "物料编码": stock.get('material_code') or stock.get('materialCode', ''),
        "技术规范ID": stock.get('tech_id') or '',
        "库存数量": float(stock.get('stock_qty', 0) or 0),
        "库存类型": stock.get('source_type', '') or stock.get('factory_name', ''),
        "距离(km)": float(stock.get('distance', 0) or 0),
    }


def _extract_plan_for_stream(plan: Dict[str, Any]) -> Dict[str, Any]:
    """提取计划关键字段用于流式接口"""
    return {
        "计划ID": plan.get('planId', ''),
        "计划编码": plan.get('planCode', ''),
        "物料编码": plan.get('materialCode', ''),
        "物料描述": plan.get('materialDesc', ''),
        "需求数量": float(plan.get('demandQty', 0) or 0),
        "单位": plan.get('unit', ''),
        "目标仓库": plan.get('warehouseCode', ''),
        "技术规范ID": plan.get('techSpecId', ''),
        "项目名称": plan.get('projectName', ''),
    }


STREAM_ALLOCATION_PROMPT = """你是一个专业的电力物料仓库调配专家。我将提供物料需求计划和可用库存数据，请你分析并给出调配建议。

## 任务说明
请分析以下物料需求计划，从可用库存中选择最合适的仓库进行调配，并详细说明你的分析过程和理由。

## 输入数据

### 调配策略
{strategy_description}

### 需求计划列表
{plans_json}

### 当前仓库与其他仓库的距离（单位：km）
{distances_json}

### 可用库存数据
{stocks_json}

## 分析要求

请按照以下结构输出详细的分析报告：

1. **需求概览**：简要描述本次需要调配的物料和数量

2. **库存分析**：分析各仓库的库存情况，包括库存数量、距离等关键因素

3. **调配方案**：
   - 针对每个需求计划，列出所有可选的仓库
   - 分析每个仓库的优缺点（距离、库存充足度等）
   - 给出最终推荐的调配仓库及理由

4. **调配结果汇总**：总结本次调配的总体情况

请用自然、清晰的语言进行分析，让用户能够理解你的决策过程。
"""

STREAM_ALLOCATION_BATCH_PROMPT = """你是一个专业的电力物料仓库调配专家。我将提供多个物料需求计划和可用库存数据，请你一次性分析所有计划并给出最优的调配方案。

## 任务说明
请一次性分析以下所有需求计划，从可用库存中为每个计划选择最合适的仓库进行调配，并详细说明你的分析过程和理由。

**重要**：你必须为**每一个需求计划**单独输出一份完整的分析结果，按照下面规定的格式，一个计划一个计划地列出结果。

## 输入数据

### 调配策略
{strategy_description}

### 需求计划列表（共 {plan_count} 个计划）
{plans_json}

### 当前仓库与其他仓库的距离（单位：km）
{distances_json}

### 可用库存数据
{stocks_json}

## 输出格式要求

**重要**：你必须按照以下格式，为每一个需求计划单独输出一份完整的分析结果。

请使用Markdown格式输出，使用##、###标题，表格使用|分隔。

**输出结构必须包含以下内容，并严格按照顺序输出**：

---

## 【计划 1/{plan_count}】调配分析

### 一、需求概况
- 计划ID: {plan_id_placeholder}
- 物料编码: {material_code_placeholder}
- 物料描述: {material_desc_placeholder}
- 需求数量: {demand_qty_placeholder}
- 目标仓库: {target_warehouse_placeholder}

### 二、可用库存分析
- 可用库存总量: {total_available_placeholder}
- 各仓库库存分布

### 三、最优调配方案
| 来源仓库 | 仓库名称 | 调拨数量 | 库存类型 | 距离(km) |
|---------|---------|---------|---------|---------|
| 仓库A | 示例仓库 | 100 | 自有库存 | 50 |

### 四、分析结论与建议
- 当前库存是否满足需求
- 匹配状态（完全匹配/部分匹配/无匹配）
- 后续处理建议

---

**然后继续输出计划2，计划3...直到所有{plan_count}个计划都分析完毕**

## 最终汇总

请在分析完所有计划后，给出本次调配的总体汇总：
- 完全匹配数量
- 部分匹配数量
- 无匹配数量
- 整体建议

请用简洁、清晰的语言进行分析。
"""


class AllocationStreamAgent:
    """仓库调配智能体 - 流式版本"""

    def __init__(self, llm_stream_func):
        self.llm_stream_func = llm_stream_func

    def _build_stream_prompt(self, plans: List[Dict[str, Any]], stocks: List[Dict[str, Any]],
                             distances_map: Dict[str, float], strategy: str = 'time',
                             analyze_mode: str = "iterative") -> str:
        """构建流式接口的prompt

        Args:
            plans: 需求计划列表
            stocks: 库存数据列表
            distances_map: 仓库距离映射
            strategy: 调配策略
            analyze_mode: 分析模式，"batch"一次性分析所有组合，"iterative"逐个分析
        """
        enriched_stocks = []
        for stock in stocks:
            enriched_stock = _extract_minimal_stock_for_stream(stock)
            loc_code = stock.get('loc_code') or stock.get('warehouseCode', '')
            enriched_stock["距离(km)"] = distances_map.get(loc_code, 0)
            enriched_stocks.append(enriched_stock)

        strategy_descriptions = {
            'time': '时效优先策略：优先选择距离最近的仓库进行调配，以最快速度满足需求。',
            'cost': '成本优先策略：优先选择距离最近的仓库，以降低运输成本。',
            'stock': '库存优先策略：优先选择库存充足的仓库，确保能够满足需求。',
            'emerg': '紧急调配策略：综合考虑距离和库存，以最快速度响应紧急需求。'
        }
        strategy_desc = strategy_descriptions.get(strategy, strategy_descriptions['time'])

        if analyze_mode == "batch":
            prompt = STREAM_ALLOCATION_BATCH_PROMPT.format(
                strategy_description=strategy_desc,
                plan_count=len(plans),
                plans_json=json.dumps([_extract_plan_for_stream(p) for p in plans], ensure_ascii=False, indent=2),
                distances_json=json.dumps(distances_map, ensure_ascii=False, indent=2),
                stocks_json=json.dumps(enriched_stocks, ensure_ascii=False, indent=2),
                plan_id_placeholder="{plan_id}",
                material_code_placeholder="{material_code}",
                material_desc_placeholder="{material_desc}",
                demand_qty_placeholder="{demand_qty}",
                target_warehouse_placeholder="{target_warehouse}",
                total_available_placeholder="{total_available}"
            )
        else:
            prompt = STREAM_ALLOCATION_PROMPT.format(
                strategy_description=strategy_desc,
                plans_json=json.dumps([_extract_plan_for_stream(p) for p in plans], ensure_ascii=False, indent=2),
                distances_json=json.dumps(distances_map, ensure_ascii=False, indent=2),
                stocks_json=json.dumps(enriched_stocks, ensure_ascii=False, indent=2)
            )
        return prompt

    async def stream_analyze(self, plans: List[Dict[str, Any]], stocks: List[Dict[str, Any]],
                             distances_map: Dict[str, float], strategy: str = 'time',
                             analyze_mode: str = "iterative"):
        """流式分析调配方案

        Args:
            plans: 需求计划列表
            stocks: 库存数据列表
            distances_map: 仓库距离映射
            strategy: 调配策略
            analyze_mode: 分析模式，"batch"一次性分析所有组合，"iterative"逐个分析(默认)
        """
        prompt = self._build_stream_prompt(plans, stocks, distances_map, strategy, analyze_mode)
        system_prompt = "你是一个专业的电力物料仓库调配专家，擅长分析库存数据并给出最优调配方案。"
        async for chunk in self.llm_stream_func(prompt, system_prompt):
            yield chunk
