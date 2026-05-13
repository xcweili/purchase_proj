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

STREAM_ALLOCATION_BATCH_PROMPT = """你是一位资深的电力物料仓库智能调配专家，具备卓越的数据分析能力和丰富的实战经验。我将提供多个物料需求计划和可用库存数据，请你运用高级算法和智能分析技术，一次性对所有计划进行深度分析并给出最优调配方案。

## 🎯 任务说明
请运用先进的智能调配算法，一次性分析以下所有需求计划（共 {plan_count} 个计划），从全局库存网络中为每个计划精准匹配最合适的仓库进行智能调配。

**⚠️ 核心规则：数据分组原则**
- **需求数据**：每个计划包含【目标仓库 + 物料编码 + 技术规范ID + 需求数量】
- **库存数据**：每个库存记录包含【来源仓库 + 物料编码 + 技术规范ID + 库存数量 + 库存类型 + 距离】
- **匹配规则**：只匹配相同【物料编码 + 技术规范ID】的库存和需求计划
- **库存约束**：从仓库调拨的数量 **绝对不能超过** 该仓库的可用库存数量

你的分析将直接影响企业的物资流转效率和成本控制，请务必严谨对待。

---

## 📊 输入数据

### 调配策略
{strategy_description}

### 需求计划列表（共 {plan_count} 个计划）
每个计划代表一个目标仓库对特定物料的需求：
{plans_json}

### 当前仓库与其他仓库的距离矩阵（单位：km）
{distances_json}

### 可用库存数据全景
每个库存记录代表一个来源仓库可提供的物料：
{stocks_json}

### 🧠 智能调配算法说明（非常重要）

**四种调配策略详解**：

**1. 时效优先策略（time）**：
- 🎯 目标：最快速度满足需求
- 📊 算法：优先选择距离最近的仓库
- 📋 步骤：
  - 按距离升序排序可用仓库
  - 从最近的仓库开始分配
  - 每次分配不超过仓库可用库存
  - 重复直到需求满足或无可用库存

**2. 成本优先策略（cost）**：
- 🎯 目标：运输成本最小化
- 📊 算法：优先选择距离最近的仓库（距离越近成本越低）
- 📋 步骤：同时效优先策略

**3. 库存优先策略（stock）**：
- 🎯 目标：确保需求能够被满足
- 📊 算法：优先选择库存充足的仓库
- 📋 步骤：
  - 按库存数量降序排序可用仓库
  - 从库存最多的仓库开始分配
  - 每次分配不超过仓库可用库存
  - 重复直到需求满足或无可用库存

**4. 紧急调配策略（emerg）**：
- 🎯 目标：综合考虑时效和库存保障
- 📊 算法：综合评分 = 库存充足度 × 0.6 + (1 - 距离/最大距离) × 0.4
- 📋 步骤：
  - 计算每个仓库的综合评分
  - 按评分降序排序
  - 从评分最高的仓库开始分配

**⚠️ 严格约束条件**：
1. 从仓库调拨的数量 **不能超过** 该仓库的可用库存数量
2. 调拨数量之和 **必须等于** 需求数量（完全匹配时）或小于需求数量（部分匹配时）
3. 同一仓库不能被重复分配超过其库存数量

---

## 📝 输出格式要求

**重要**：输出分为两个阶段，请严格按照以下顺序输出：

### 阶段一：详细分析报告（逐条分析）

---

## 【计划 1/{plan_count}】智能调配分析报告

### 一、需求概况
- 📋 计划ID: {plan_id_placeholder}
- 🔧 物料编码: {material_code_placeholder}
- 📝 物料描述: {material_desc_placeholder}
- 📦 需求数量: {demand_qty_placeholder} 件
- 📍 目标仓库: {target_warehouse_placeholder}
- 📄 技术规范ID: [从数据中提取]

### 二、库存态势分析
运用智能算法对各仓库库存进行深度扫描和评估：

**可用库存仓库列表（按策略排序）**：
| 序号 | 来源仓库 | 仓库名称 | 库存数量 | 库存类型 | 距离(km) | 库存充足率 |
|------|---------|---------|---------|---------|---------|-----------|
| 1 | [仓库1] | [名称1] | [数量1] | [类型1] | [距离1] | [充足率1] |
| 2 | [仓库2] | [名称2] | [数量2] | [类型2] | [距离2] | [充足率2] |
| ... | ... | ... | ... | ... | ... | ... |

**库存综合评估**：
- 可用库存总量：{total_available_placeholder} 件
- 满足需求比例：[计算值]%
- 涉及仓库数量：[数量] 个
- 平均距离：[计算值] km

### 三、智能调配方案
通过多维度优化算法计算，最优调配方案如下：

**调配详情**：
| 来源仓库 | 仓库名称 | 调拨数量 | 库存类型 | 距离(km) | 匹配得分 | 理由 |
|---------|---------|---------|---------|---------|---------|------|
| [仓库] | [名称] | [数量] | [类型] | [距离] | [得分] | [理由] |

**调配汇总**：
- ✅ 调配数量：[数量] 件
- 📊 满足率：[计算值]%
- 🏭 涉及仓库：[数量] 个
- 📉 平均距离：[计算值] km

### 四、智能分析结论与建议
- ✅ 当前库存是否满足需求：[是/否/部分]
- 📊 匹配状态：[完全匹配/部分匹配/无匹配]
- 🎯 匹配得分：[计算值]
- 📋 智能推荐：[详细建议，不少于50字]
- ⚠️ 风险提示：[潜在风险分析]

---

**继续输出计划2，计划3...直到所有{plan_count}个计划都分析完毕**

## 📈 最终智能汇总报告

请在分析完所有计划后，给出本次智能调配的总体汇总：
- 🎯 完全匹配计划数量：[数量]
- ⚠️ 部分匹配计划数量：[数量]
- ❌ 无匹配计划数量：[数量]
- 📊 平均匹配得分：[计算值]
- 📦 总调配数量：[数量] 件
- 💡 智能优化建议：[综合建议，不少于100字]

---

### 阶段二：JSON格式输出（非常重要）

**重要**：在完成所有分析报告输出后，请输出一个JSON格式的结果数据，用于系统后续处理和数据库存储。

**JSON格式要求**：
```json
{{
  "total": {plan_count},
  "fullMatchCount": [完全匹配数量],
  "partialMatchCount": [部分匹配数量],
  "noneMatchCount": [无匹配数量],
  "avgScore": [平均匹配得分],
  "suggestion": "[综合建议]",
  "results": [
    {{
      "planId": "计划ID",
      "materialCode": "物料编码",
      "materialDesc": "物料描述",
      "demandQty": [需求数量],
      "warehouseCode": "目标仓库",
      "techSpecId": "技术规范ID",
      "matchedQty": [匹配数量],
      "availableStock": [可用库存],
      "sourceWarehouseCode": "来源仓库",
      "sourceWarehouseName": "来源仓库名称",
      "sourceType": "库存类型",
      "status": "full|partial|none",
      "statusName": "完全匹配|部分匹配|无匹配",
      "score": [匹配得分],
      "reason": "匹配理由",
      "allocationType": "本仓库|跨仓调拨"
    }}
  ]
}}
```

**⚠️ 数据一致性要求**：
- JSON中的数据必须与前面分析报告中的数据**完全一致**
- 所有数值必须是精确计算的结果，禁止估算或编造
- 计划顺序必须与输入数据顺序保持一致
- 调拨数量必须满足：0 ≤ 调拨数量 ≤ 仓库可用库存数量

---

请运用你的专业知识，提供专业、详细、智能化的分析报告。
"""


class AllocationStreamAgent:
    """仓库调配智能体 - 流式版本"""

    def __init__(self, llm_stream_func):
        self.llm_stream_func = llm_stream_func

    def _build_stream_prompt(self, plans: List[Dict[str, Any]], stocks: List[Dict[str, Any]],
                             distances_map: Dict[str, float], strategy: str = 'time') -> str:
        """构建流式接口的prompt

        Args:
            plans: 需求计划列表
            stocks: 库存数据列表
            distances_map: 仓库距离映射
            strategy: 调配策略
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
        return prompt

    async def stream_analyze(self, plans: List[Dict[str, Any]], stocks: List[Dict[str, Any]],
                             distances_map: Dict[str, float], strategy: str = 'time'):
        """流式分析调配方案

        Args:
            plans: 需求计划列表
            stocks: 库存数据列表
            distances_map: 仓库距离映射
            strategy: 调配策略
        """
        prompt = self._build_stream_prompt(plans, stocks, distances_map, strategy)
        system_prompt = "你是一个专业的电力物料仓库调配专家，擅长分析库存数据并给出最优调配方案。"
        async for chunk in self.llm_stream_func(prompt, system_prompt):
            yield chunk
