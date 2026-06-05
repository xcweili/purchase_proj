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

STREAM_INVENTORY_ANALYSIS_BATCH_PROMPT = """你是一位资深的电力物料智能库存分析专家，具备卓越的数据分析能力和丰富的库存管理实战经验。我将提供多个【仓库-物料-技术规范】组合的库存数据和历史出库数据，请你运用高级数据分析算法和智能预测模型，对每个组合进行精准分析。

## 🎯 任务说明
运用先进的智能库存分析算法，分析以下所有组合（共 {combo_count} 个组合）的库存数据。**关键要求**：

**⚠️ 核心规则：分组处理原则**
- 每个组合由【仓库编码 + 物料编码 + 技术规范ID】唯一确定
- 分析时必须严格按照组合进行分组，**只使用属于该组合的历史出库数据**
- 禁止跨组合使用数据，避免数据污染导致分析错误

你的分析将直接影响企业的库存管理策略和资金运作效率，请务必严谨对待。

---

## 📊 输入数据

### 分析参数
- 预测周期：{period_desc}（未来时间段，用于计算预测需求量）
- 库存层级：{inventory_levels}
- 季节因子权重：{season_factor}
- 安全冗余比例：{safety_ratio}

### 组合数量
共有 {combo_count} 个组合需要分析

### 当前库存数据全景（按组合组织）
{stocks_json}

### 历史出库数据（用于计算消耗规律）
{outbound_json}

---

## 🧠 智能分析算法说明

请运用以下算法进行分析：

1. **数据过滤**：对每个组合，只提取该组合（相同仓库编码+物料编码+技术规范ID）的历史出库数据
2. **需求预测**：基于历史数据计算月均消耗量，结合预测周期计算总需求量
3. **水位计算**：
   - 应急线 = 请根据历史数据中的最低月出库、消耗趋势和供货周期综合判断，设定安全底线
   - 补库线 = 请结合月均消耗量、安全冗余比例、消耗波动性和供货周期进行计算，作为补货启动信号
   - 高位线 = 请根据库存持有成本、消耗稳定性和合理库存上限综合设定
4. **状态判定**：
   - 紧急：当前库存 ≤ 应急线
   - 低：应急线 < 当前库存 ≤ 补库线
   - 中：补库线 < 当前库存 ≤ 高位线
   - 高：当前库存 > 高位线
5. **补货建议**：当当前库存 ≤ 补库线时，建议补货量 = 补库线 - 当前库存

---

## 📝 输出格式要求

**重要**：输出分为两个阶段，请严格按照以下顺序输出：

### 阶段一：详细分析报告（逐条分析）

---

## 【组合 1/{combo_count}】智能库存分析报告

### 一、组合唯一标识
- 📦 仓库编码: {warehouse_code_placeholder}
- 🏢 仓库名称: {warehouse_name_placeholder}
- 📊 库存层级: {inventory_level_placeholder}
- 🔧 物料编码: {material_code_placeholder}
- 📄 技术规范ID: {tech_id_placeholder}
- 📝 物料描述: {material_desc_placeholder}

### 二、当前库存态势分析
- 📈 当前库存: {current_stock_placeholder} 件
- 🚚 在途库存: {in_transit_stock_placeholder} 件
- ✅ 实际可用库存: {available_stock_placeholder} 件
- 📉 库存充足率: [基于预测周期计算]

### 三、深度历史消耗分析
运用智能算法对**该组合专属**的历史出库数据进行多维度分析：
- 🔝 最高月出库: {max_outbound_placeholder} 件
- 🔻 最低月出库: {min_outbound_placeholder} 件
- 📊 平均月出库: {avg_outbound_placeholder} 件
- 📈 中位数出库: {median_outbound_placeholder} 件
- 📉 需求趋势: [上升/下降/平稳]
- 🌡️ 季节性特征: [强/中/弱]

### 四、智能水位线计算（基于月均消耗）
通过分析与机器学习算法计算得出科学库存水位（具体计算方式你可以按自己的理解进行调整）：
| 水位线 | 计算值（件） | 设定依据 |
|--------|-------------|----------|
| 🔴 应急线 | {emergency_line_placeholder} | 基于历史最低消耗和供货周期推算的安全底线 |
| 🟡 补库线 | {replenish_line_placeholder} | 结合月均消耗量与波动性确定的补货启动点 |
| 🟢 高位线 | {high_line_placeholder} | 根据库存持有成本和消耗上限设定的库存上限 |
| 📊 当前水位状态 | [智能判定] | 紧急/低/中/高 |

### 五、智能分析结论与建议
- 📋 当前库存状态评估：[详细评估，不少于50字]
- 🔄 是否需要补库：[是/否]
- 📦 建议补货数量：[精确计算值] 件
- ⏰ 建议补货时间：[智能建议]
- ⚠️ 风险提示：[潜在风险分析]

---

**继续输出组合2，组合3...直到所有{combo_count}个组合都分析完毕**

## 📈 最终智能汇总报告

请在分析完所有组合后，给出本次智能库存分析的总体汇总：
- 🎯 正常库存组合数量：[数量]
- ⚠️ 需关注库存组合数量：[数量]
- ❌ 紧急补货组合数量：[数量]
- 💡 智能优化建议：[综合建议，不少于100字]

---

### 阶段二：数据解析入库（非常重要）

**重要**：在完成所有分析报告输出后，请输出一个JSON格式的结果数据，用于系统后续处理和数据库存储。

**JSON格式要求**：
```json
{{
  "total": {combo_count},
  "normalCount": [正常库存数量],
  "warningCount": [需关注数量],
  "emergencyCount": [紧急补货数量],
  "suggestion": "[综合建议]",
  "results": [
    {{
      "warehouseCode": "仓库编码",
      "warehouseName": "仓库名称",
      "inventoryLevel": "库存层级",
      "materialCode": "物料编码",
      "techId": "技术规范ID",
      "materialDesc": "物料描述",
      "currentStock": [当前库存],
      "inTransitStock": [在途库存],
      "availableStock": [可用库存],
      "emergencyLine": [应急线],
      "replenishLine": [补库线],
      "highLine": [高位线],
      "waterLevelStatus": "emergency|low|medium|high",
      "waterLevelStatusName": "紧急|低|中|高",
      "suggestedAction": "立即补库|建议补库|正常",
      "suggestedQty": [建议补货数量],
      "riskLevel": "low|medium|high"
    }}
  ]
}}
```

**⚠️ 数据一致性要求**：
- JSON中的数据必须与前面分析报告中的数据**完全一致**
- 所有数值必须是精确计算的结果，禁止估算或编造
- 组合顺序必须与输入数据顺序保持一致

---

请运用你的专业知识，提供专业、详细、智能化的分析报告。
"""


class InventoryAnalysisStreamAgent:
    """库存分析智能体 - 流式版本"""

    def __init__(self, llm_stream_func):
        self.llm_stream_func = llm_stream_func

    def _build_stream_prompt(self, stocks: List[Dict[str, Any]], outbound: List[Dict[str, Any]],
                            start_date: str = None, end_date: str = None,
                            inventory_levels: List[str] = None, season_factor: float = 0.3,
                            safety_ratio: float = 0.2) -> str:
        """构建流式接口的prompt

        Args:
            stocks: 库存数据列表
            outbound: 历史出库数据列表
            start_date: 开始日期
            end_date: 结束日期
            inventory_levels: 库存层级列表
            season_factor: 季节因子权重
            safety_ratio: 安全冗余比例
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
        return prompt

    async def stream_analyze(self, stocks: List[Dict[str, Any]], outbound: List[Dict[str, Any]],
                            start_date: str = None, end_date: str = None,
                            inventory_levels: List[str] = None, season_factor: float = 0.3,
                            safety_ratio: float = 0.2):
        """流式分析库存

        Args:
            stocks: 库存数据列表
            outbound: 历史出库数据列表
            start_date: 开始日期
            end_date: 结束日期
            inventory_levels: 库存层级列表
            season_factor: 季节因子权重
            safety_ratio: 安全冗余比例
        """
        prompt = self._build_stream_prompt(stocks, outbound, start_date, end_date,
                                          inventory_levels, season_factor, safety_ratio)
        system_prompt = "你是一个专业的电力物料库存分析专家，擅长分析库存数据、预测需求并给出合理的补货建议。"
        async for chunk in self.llm_stream_func(prompt, system_prompt):
            yield chunk
