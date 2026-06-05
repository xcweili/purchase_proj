# -*- coding: utf-8 -*-
"""供应商匹配智能体 - 流式版本"""
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


def _extract_plan_for_stream(plan: Dict[str, Any]) -> Dict[str, Any]:
    """提取计划关键字段用于流式接口"""
    return {
        "计划ID": plan.get('planId', ''),
        "物料编码": plan.get('materialCode', ''),
        "物料描述": plan.get('materialDesc', ''),
        "需求数量": float(plan.get('demandQty', 0) or 0),
        "单价": float(plan.get('unitPrice', 0) or 0),
        "仓库编码": plan.get('warehouseCode', ''),
        "技术规范ID": plan.get('techSpecId', ''),
        "项目名称": plan.get('projectName', ''),
        "需求金额": float(plan.get('demandQty', 0) or 0) * float(plan.get('unitPrice', 0) or 0),
    }


def _extract_supplier_for_stream(supplier: Dict[str, Any]) -> Dict[str, Any]:
    """提取供应商关键字段用于流式接口"""
    return {
        "供应商编码": supplier.get('supplier_code', ''),
        "供应商名称": supplier.get('supplier_name', ''),
        "物料编码": supplier.get('material_code', ''),
        "执行比例(%)": float(supplier.get('execution_rate', 0) or 0),
        "剩余可用数量": float(supplier.get('remain_qty', 0) or 0),
        "剩余可用金额": float(supplier.get('remain_amount', 0) or 0),
        "协议单价": float(supplier.get('unit_price', 0) or 0),
        "供货周期(天)": supplier.get('delivery_cycle', ''),
    }


STREAM_SUPPLIER_MATCH_PROMPT = """你是一个专业的电力物料采购供应商匹配专家。我将提供补货需求计划和供应商协议数据，请你分析并给出最优的供应商选择方案。

## 任务说明
请分析以下补货需求计划，根据供应商的协议执行情况和可用库存，为每个需求选择最合适的供应商，并详细说明你的分析过程和理由。

## 输入数据

### 补货计划列表
{plans_json}

### 供应商协议数据
{suppliers_json}

### 匹配规则说明
1. **执行比例阶梯**：20%、50%、80%
2. **执行比例计算**：已执行金额 / 执行金额总额度
3. **阶梯规则**：优先选择执行比例未达到下一阶梯的供应商
4. **补货频率**：周
5. **供货周期**：15-45天

### 三种匹配策略
1. **均衡策略**：按执行比例阶梯分配，优先选择执行比例较低的供应商
2. **成本策略**：选择单价最低的供应商，追求总成本最小化
3. **配送策略**：优先选择能满足全部需求的单个供应商，简化配送流程

## 分析要求

请按照以下结构输出详细的分析报告：

1. **需求概览**：简要描述本次需要补货的物料和数量

2. **供应商分析**：
   - 各供应商的执行比例情况
   - 各供应商的可用库存和金额
   - 各供应商的单价对比

3. **匹配方案**：
   - 针对每个需求计划，分析各策略下的最佳供应商选择
   - 详细说明选择理由
   - 计算分配数量和金额

4. **方案对比**：
   - 三种策略的优缺点对比
   - 推荐策略建议

5. **结果汇总**：总结本次供应商匹配的总体情况

请用自然、清晰的语言进行分析，让用户能够理解你的决策过程。
"""

STREAM_SUPPLIER_MATCH_BATCH_PROMPT = """你是一位资深的电力物料智能采购供应商匹配专家，具备卓越的供应链分析能力和丰富的供应商管理实战经验。我将提供多个补货需求计划和供应商协议数据，请你运用高级智能匹配算法，一次性对所有计划进行深度分析并给出最优的供应商选择方案。

## 🎯 任务说明
运用先进的智能供应商匹配算法，一次性分析以下所有补货需求计划（共 {plan_count} 个计划）。

**⚠️ 核心规则：数据分组原则**
- **需求数据**：每个计划包含【仓库 + 物料编码 + 技术规范ID + 需求数量】
- **供应商数据**：每个供应商协议包含【供应商 + 物料编码 + 技术规范ID + 剩余可用数量 + 剩余可用金额 + 执行比例】
- **匹配规则**：只匹配相同【物料编码 + 技术规范ID】的供应商和需求计划

你的分析将直接影响企业的采购成本和供应链效率，请务必严谨对待。

---

## 📊 输入数据

### 补货计划列表（共 {plan_count} 个计划）
每个计划代表一个仓库对特定物料的补货需求：
{plans_json}

### 供应商协议数据全景
每个供应商协议代表供应商可提供的物料及约束条件：
{suppliers_json}

### 🧠 智能匹配规则说明（非常重要）

**执行比例阶梯规则**：
| 阶梯 | 执行比例范围 | 状态 |
|------|-------------|------|
| 第一阶梯 | 0% - 20% | 优先选择 |
| 第二阶梯 | 20% - 50% | 次优先选择 |
| 第三阶梯 | 50% - 80% | 谨慎选择 |
| 第四阶梯 | 80%以上 | 避免选择 |

**约束条件**：
1. ⚠️ 供应商分配数量 **不能超过** 其剩余可用数量
2. ⚠️ 供应商分配金额 **不能超过** 其剩余可用金额
3. ⚠️ 供应商执行比例 **不能超过** 100%
4. ⚠️ 分配数量之和 **必须等于** 需求数量（允许部分匹配时除外）

### 三种智能匹配策略详解

**1. 均衡策略**：
- 🎯 目标：实现供应商均衡发展，避免个别供应商过度依赖
- 📊 算法：优先选择执行比例较低的供应商
- 📋 步骤：
  - 按执行比例升序排序供应商
  - 从执行比例最低的供应商开始分配
  - 每次分配不超过供应商剩余可用数量和金额限制
  - 重复直到需求满足或无可用供应商

**2. 成本策略**：
- 🎯 目标：采购总成本最小化
- 📊 算法：选择单价最低的供应商
- 📋 步骤：
  - 按协议单价升序排序供应商
  - 从单价最低的供应商开始分配
  - 每次分配不超过供应商剩余可用数量和金额限制
  - 重复直到需求满足或无可用供应商

**3. 配送策略**：
- 🎯 目标：简化配送流程，提高效率
- 📊 算法：优先选择能满足全部需求的单个供应商
- 📋 步骤：
  - 筛选出剩余可用数量 ≥ 需求数量的供应商
  - 从中选择最优（可按执行比例或单价）
  - 若无单个供应商能满足，采用多供应商组合

---

## 📝 输出格式要求

**重要**：输出分为两个阶段，请严格按照以下顺序输出：

### 阶段一：详细分析报告（逐条分析）

---

## 【计划 1/{plan_count}】智能供应商匹配分析报告

### 一、需求概况
- 📋 计划ID: {plan_id_placeholder}
- 🔧 物料编码: {material_code_placeholder}
- 📝 物料描述: {material_desc_placeholder}
- 📦 需求数量: {demand_qty_placeholder} 件
- 📍 目标仓库: {warehouse_code_placeholder}
- 📄 技术规范ID: [从数据中提取]

### 二、智能供应商筛选与评估
运用智能算法筛选匹配的供应商并进行多维度评估：

**符合条件的供应商列表（按执行比例排序）**：
| 序号 | 供应商名称 | 协议单价 | 剩余可用数量 | 剩余可用金额 | 执行比例 | 供货周期 |
|------|-----------|---------|-------------|-------------|---------|---------|
| 1 | [供应商1] | [单价] | [数量] | [金额] | [比例] | [周期] |
| 2 | [供应商2] | [单价] | [数量] | [金额] | [比例] | [周期] |
| ... | ... | ... | ... | ... | ... | ... |

**供应商综合评估**：
- 符合条件的供应商数量：[数量]
- 总可用供应量：[数量] 件
- 单价范围：[最低] - [最高]
- 执行比例分布：[分析]

### 三、三种策略智能匹配方案

**⚠️ 约束检查**：所有方案均需满足供应商剩余数量和金额限制

| 策略 | 推荐供应商 | 分配数量 | 单价 | 金额 | 执行比例变化 | 理由 |
|------|-----------|---------|------|------|-------------|------|
| ⚖️ 均衡策略 | [供应商名称] | [数量] | [单价] | [金额] | [原比例]→[新比例] | [详细理由，不少于30字] |
| 💰 成本策略 | [供应商名称] | [数量] | [单价] | [金额] | [原比例]→[新比例] | [详细理由，不少于30字] |
| 🚚 配送策略 | [供应商名称] | [数量] | [单价] | [金额] | [原比例]→[新比例] | [详细理由，不少于30字] |

### 四、智能分析结论与建议
- ✅ 是否有合适的供应商：[是/否/部分]
- 🎯 推荐的供应商选择策略：[策略名称]
- 📋 推荐理由：[详细说明，不少于50字]
- ⚠️ 风险提示：[潜在风险分析]
- 💡 后续处理建议：[专业建议]

---

**继续输出计划2，计划3...直到所有{plan_count}个计划都分析完毕**

## 📈 最终智能汇总报告

请在分析完所有计划后，给出本次智能供应商匹配的总体汇总：
- 🎯 完全匹配计划数量：[数量]
- ⚠️ 部分匹配计划数量：[数量]
- ❌ 无匹配计划数量：[数量]
- 💰 预计采购总金额：[金额]
- 💡 智能优化建议：[综合建议，不少于100字]

---

### 阶段二：数据解析入库（非常重要）

**重要**：在完成所有分析报告输出后，请输出一个JSON格式的结果数据，用于系统后续处理和数据库存储。

**JSON格式要求**：
```json
{{
  "total": {plan_count},
  "matchedCount": [完全匹配数量],
  "partialMatchedCount": [部分匹配数量],
  "unmatchedCount": [无匹配数量],
  "suggestion": "[综合建议]",
  "results": [
    {{
      "planId": "计划ID",
      "materialCode": "物料编码",
      "materialDesc": "物料描述",
      "demandQty": [需求数量],
      "warehouseCode": "目标仓库",
      "techSpecId": "技术规范ID",
      "matched": true|false,
      "matchedSupplierCode": "供应商编码",
      "matchedSupplierName": "供应商名称",
      "allocatedQty": [分配数量],
      "unitPrice": [单价],
      "totalAmount": [总金额],
      "strategy": "均衡|成本|配送",
      "reason": "匹配理由",
      "executionRate": [执行比例]
    }}
  ]
}}
```

**⚠️ 数据一致性要求**：
- JSON中的数据必须与前面分析报告中的数据**完全一致**
- 所有数值必须是精确计算的结果，禁止估算或编造
- 计划顺序必须与输入数据顺序保持一致
- 分配数量必须满足：0 ≤ 分配数量 ≤ 供应商剩余可用数量

---

请运用你的专业知识，提供专业、详细、智能化的分析报告。
"""


class SupplierMatchStreamAgent:
    """供应商匹配智能体 - 流式版本"""

    def __init__(self, llm_stream_func):
        self.llm_stream_func = llm_stream_func

    def _build_stream_prompt(self, plans: List[Dict[str, Any]], suppliers: List[Dict[str, Any]]) -> str:
        """构建流式接口的prompt

        Args:
            plans: 补货计划列表
            suppliers: 供应商列表
        """
        prompt = STREAM_SUPPLIER_MATCH_BATCH_PROMPT.format(
            plan_count=len(plans),
            plans_json=json.dumps([_extract_plan_for_stream(p) for p in plans], ensure_ascii=False, indent=2),
            suppliers_json=json.dumps([_extract_supplier_for_stream(s) for s in suppliers], ensure_ascii=False, indent=2),
            plan_id_placeholder="{plan_id}",
            material_code_placeholder="{material_code}",
            material_desc_placeholder="{material_desc}",
            demand_qty_placeholder="{demand_qty}",
            warehouse_code_placeholder="{warehouse_code}"
        )
        return prompt

    async def stream_analyze(self, plans: List[Dict[str, Any]], suppliers: List[Dict[str, Any]]) -> None:
        """流式分析供应商匹配

        Args:
            plans: 补货计划列表
            suppliers: 供应商列表
        """
        prompt = self._build_stream_prompt(plans, suppliers)
        system_prompt = "你是一个专业的电力物料采购供应商匹配专家，擅长分析供应商协议数据并给出最优的供应商选择方案。"
        async for chunk in self.llm_stream_func(prompt, system_prompt):
            yield chunk
