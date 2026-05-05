# -*- coding: utf-8 -*-
"""库存分析智能体 - 流式版本"""
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


def _extract_stock_for_stream(stock: Dict[str, Any]) -> Dict[str, Any]:
    """提取库存关键字段用于流式接口"""
    return {
        "仓库编码": stock.get('warehouse_code', ''),
        "仓库名称": stock.get('warehouse_name', ''),
        "物料编码": stock.get('material_code', ''),
        "物料描述": stock.get('material_desc', ''),
        "技术规范ID": stock.get('tech_id', ''),
        "当前库存": float(stock.get('current_stock', 0) or 0),
        "在途库存": float(stock.get('in_transit_stock', 0) or 0),
        "实际可用库存": float(stock.get('current_stock', 0) or 0) + float(stock.get('in_transit_stock', 0) or 0),
    }


def _extract_outbound_for_stream(outbound: Dict[str, Any]) -> Dict[str, Any]:
    """提取出库数据关键字段用于流式接口"""
    return {
        "仓库编码": outbound.get('warehouse_code', ''),
        "物料编码": outbound.get('material_code', ''),
        "技术规范ID": outbound.get('tech_id', ''),
        "月份": outbound.get('month', ''),
        "出库数量": float(outbound.get('outbound_qty', 0) or 0),
        "历史平均出库": float(outbound.get('historical_avg_qty', 0) or 0),
        "最近1个月出库": float(outbound.get('last_1_month_qty', 0) or 0),
        "最近2个月出库": float(outbound.get('last_2_month_qty', 0) or 0),
        "最近3个月出库": float(outbound.get('last_3_month_qty', 0) or 0),
    }


STREAM_INVENTORY_ANALYSIS_PROMPT = """你是一个专业的电力物料库存分析专家。我将提供库存数据和历史出库数据，请你进行深度分析并给出专业建议。

## 任务说明
请分析以下库存数据和历史出库数据，评估库存健康状况，计算合理库存水位，并给出补货建议。

## 输入数据

### 分析参数
- 分析周期：{period_desc}
- 库存层级：{inventory_levels}
- 季节因子权重：{season_factor}
- 安全冗余比例：{safety_ratio}

### 当前库存数据
{stocks_json}

### 历史出库数据（近3个月）
{outbound_json}

## 分析要求

请按照以下结构输出详细的分析报告：

1. **库存概览**：总体库存状况概述

2. **库存健康分析**：
   - 各物料的当前库存水平
   - 在途库存情况
   - 库存周转率分析

3. **需求预测**：
   - 基于历史数据的需求趋势分析
   - 季节性因素影响评估

4. **库存水位计算**：
   - 高位线、补库线、应急线的计算
   - 当前库存与水位线的对比分析

5. **补货建议**：
   - 需要补货的物料清单
   - 建议补货数量
   - 优先级排序

6. **风险提示**：
   - 库存不足风险预警
   - 库存积压风险预警

请用自然、清晰的语言进行分析，让用户能够理解你的分析过程和建议。
"""

STREAM_INVENTORY_ANALYSIS_BATCH_PROMPT = """你是一个专业的电力物料库存分析专家。我将提供多个仓库-物料-技术规范组合的库存数据和历史出库数据，请你一次性分析所有组合并给出专业的库存分析建议。

## 任务说明
请一次性分析以下所有组合（仓库×物料×技术规范）的库存数据，评估每个组合的库存健康状况，计算合理库存水位，并给出补货或利库建议。

**重要**：你必须为**每一个组合**单独输出一份完整的分析结果，按照下面规定的格式，一个组合一个组合地列出结果。

## 输入数据

### 分析参数
- 分析周期：{period_desc}
- 库存层级：{inventory_levels}
- 季节因子权重：{season_factor}
- 安全冗余比例：{safety_ratio}

### 组合数量
共有 {combo_count} 个组合需要分析

### 当前库存数据
{stocks_json}

### 历史出库数据（近3个月）
{outbound_json}

## 输出格式要求

**重要**：你必须按照以下格式，为每一个组合单独输出一份完整的分析结果。

请使用Markdown格式输出，使用##、###标题，表格使用|分隔。

**输出结构必须包含以下内容，并严格按照顺序输出**：

---

## 【组合 1/{combo_count}】库存分析

### 一、组合信息
- 仓库编码: {warehouse_code_placeholder}
- 仓库名称: {warehouse_name_placeholder}
- 库存层级: {inventory_level_placeholder}
- 物料编码: {material_code_placeholder}
- 技术规范ID: {tech_id_placeholder}
- 物料描述: {material_desc_placeholder}

### 二、当前库存状况
- 当前库存: {current_stock_placeholder}
- 在途库存: {in_transit_stock_placeholder}
- 实际可用库存: {available_stock_placeholder}

### 三、历史消耗分析
- 最高月出库: {max_outbound_placeholder}
- 最低月出库: {min_outbound_placeholder}
- 平均月出库: {avg_outbound_placeholder}
- 中位数出库: {median_outbound_placeholder}
- 同比变化: {yoy_change_placeholder}
- 环比变化: {mom_change_placeholder}
- 季节性特征: {seasonality_placeholder}

### 四、水位线分析
| 指标 | 计算值 | 说明 |
|------|--------|------|
| 应急线 | {emergency_line_placeholder} | 最低库存标准 |
| 补库线 | {replenish_line_placeholder} | 可以开始补库 |
| 高位线 | {high_line_placeholder} | 库存已处于高点 |

### 五、分析结论与建议
- 当前库存状态评估
- 是否需要补库
- 建议补货数量和时间

---

**然后继续输出组合2，组合3...直到所有{combo_count}个组合都分析完毕**

请用简洁、清晰的语言进行分析，重点关注中位数、正态分布、同比环比等指标。
"""


class InventoryAnalysisStreamAgent:
    """库存分析智能体 - 流式版本"""

    def __init__(self, llm_stream_func):
        self.llm_stream_func = llm_stream_func

    def _build_stream_prompt(self, stocks: List[Dict[str, Any]], outbound: List[Dict[str, Any]],
                            start_date: str = None, end_date: str = None,
                            inventory_levels: List[str] = None, season_factor: float = 0.3,
                            safety_ratio: float = 0.2, analyze_mode: str = "iterative") -> str:
        """构建流式接口的prompt

        Args:
            stocks: 库存数据列表
            outbound: 历史出库数据列表
            start_date: 开始日期
            end_date: 结束日期
            inventory_levels: 库存层级列表
            season_factor: 季节因子权重
            safety_ratio: 安全冗余比例
            analyze_mode: 分析模式，"batch"一次性分析所有组合，"iterative"逐个分析
        """
        if start_date and end_date:
            period_desc = f"{start_date} 至 {end_date}"
        elif start_date:
            period_desc = f"从 {start_date} 开始"
        elif end_date:
            period_desc = f"截至 {end_date}"
        else:
            period_desc = "全部历史数据"

        if inventory_levels:
            levels_desc = ", ".join(inventory_levels)
        else:
            levels_desc = "所有层级"

        if analyze_mode == "batch":
            prompt = STREAM_INVENTORY_ANALYSIS_BATCH_PROMPT.format(
                period_desc=period_desc,
                inventory_levels=levels_desc,
                season_factor=season_factor if season_factor else "默认(0.3)",
                safety_ratio=safety_ratio if safety_ratio else "默认(0.2)",
                combo_count=len(stocks),
                stocks_json=json.dumps([_extract_stock_for_stream(s) for s in stocks], ensure_ascii=False, indent=2),
                outbound_json=json.dumps([_extract_outbound_for_stream(o) for o in outbound], ensure_ascii=False, indent=2),
                warehouse_code_placeholder="{warehouse_code}",
                warehouse_name_placeholder="{warehouse_name}",
                inventory_level_placeholder="{inventory_level}",
                material_code_placeholder="{material_code}",
                tech_id_placeholder="{tech_id}",
                material_desc_placeholder="{material_desc}",
                current_stock_placeholder="{current_stock}",
                in_transit_stock_placeholder="{in_transit_stock}",
                available_stock_placeholder="{available_stock}",
                max_outbound_placeholder="{max_outbound}",
                min_outbound_placeholder="{min_outbound}",
                avg_outbound_placeholder="{avg_outbound}",
                median_outbound_placeholder="{median_outbound}",
                yoy_change_placeholder="{yoy_change}",
                mom_change_placeholder="{mom_change}",
                seasonality_placeholder="{seasonality}",
                emergency_line_placeholder="{emergency_line}",
                replenish_line_placeholder="{replenish_line}",
                high_line_placeholder="{high_line}"
            )
        else:
            prompt = STREAM_INVENTORY_ANALYSIS_PROMPT.format(
                period_desc=period_desc,
                inventory_levels=levels_desc,
                season_factor=season_factor if season_factor else "默认(0.3)",
                safety_ratio=safety_ratio if safety_ratio else "默认(0.2)",
                stocks_json=json.dumps([_extract_stock_for_stream(s) for s in stocks], ensure_ascii=False, indent=2),
                outbound_json=json.dumps([_extract_outbound_for_stream(o) for o in outbound], ensure_ascii=False, indent=2)
            )
        return prompt

    async def stream_analyze(self, stocks: List[Dict[str, Any]], outbound: List[Dict[str, Any]],
                            start_date: str = None, end_date: str = None,
                            inventory_levels: List[str] = None, season_factor: float = 0.3,
                            safety_ratio: float = 0.2, analyze_mode: str = "iterative"):
        """流式分析库存

        Args:
            stocks: 库存数据列表
            outbound: 历史出库数据列表
            start_date: 开始日期
            end_date: 结束日期
            inventory_levels: 库存层级列表
            season_factor: 季节因子权重
            safety_ratio: 安全冗余比例
            analyze_mode: 分析模式，"batch"一次性分析所有组合，"iterative"逐个分析(默认)
        """
        prompt = self._build_stream_prompt(stocks, outbound, start_date, end_date,
                                          inventory_levels, season_factor, safety_ratio, analyze_mode)
        system_prompt = "你是一个专业的电力物料库存分析专家，擅长分析库存数据、预测需求并给出合理的补货建议。"
        async for chunk in self.llm_stream_func(prompt, system_prompt):
            yield chunk
