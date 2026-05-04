# -*- coding: utf-8 -*-
"""供应商匹配服务 - 流式版本（复用原服务逻辑）"""
import json
from typing import List, Dict, Any, Optional

from ..services.supplier_match_service import SupplierMatchService


class SupplierMatchStreamService:
    """供应商匹配服务 - 流式版本（复用SupplierMatchService的数据查询逻辑）"""

    def __init__(self, db, llm_stream_func):
        self.db = db
        self.llm_stream_func = llm_stream_func
        self._service = SupplierMatchService.__new__(SupplierMatchService)
        self._service.db = db

    async def stream_analyze(self, input_plans: List[Dict[str, Any]] = None):
        """流式分析供应商匹配"""
        try:
            # 步骤1：获取计划数据
            yield "🔍 [步骤 1/4] 正在查询补货计划...\n"
            if input_plans and len(input_plans) > 0:
                plans = input_plans
                yield f"✅ [步骤 1/4] 已获取 {len(plans)} 条输入计划\n\n"
            else:
                plans = await self._service._query_plans()
                yield f"✅ [步骤 1/4] 已从数据库获取 {len(plans)} 条补货计划\n\n"

            if not plans:
                yield "📋 未查询到补货计划。\n"
                return

            yield f"📋 计划预览（前5条）：\n"
            for i, plan in enumerate(plans[:5], 1):
                yield f"  {i}. 物料: {plan.get('materialCode', '')}, "
                yield f"需求: {plan.get('demandQty', 0)}, "
                yield f"仓库: {plan.get('warehouseCode', '')}\n"
            if len(plans) > 5:
                yield f"  ... 还有 {len(plans) - 5} 条计划\n"
            yield "\n"

            # 步骤2：提取物料编码
            yield "🔍 [步骤 2/4] 正在提取物料编码...\n"
            material_codes = list(set(
                p.get('materialCode', '')
                for p in plans
                if p.get('materialCode')
            ))
            yield f"✅ [步骤 2/4] 提取到 {len(material_codes)} 个物料编码\n"
            yield f"   物料: {', '.join(material_codes[:5])}"
            if len(material_codes) > 5:
                yield f" ... 还有{len(material_codes) - 5}个"
            yield "\n\n"

            # 步骤3：查询供应商数据
            yield "🔍 [步骤 3/4] 正在查询供应商数据...\n"
            all_suppliers = []
            for plan in plans:
                suppliers = await self._service._get_protocol_suppliers(plan)
                all_suppliers.extend(suppliers)

            yield f"✅ [步骤 3/4] 已获取 {len(all_suppliers)} 条供应商记录\n\n"

            if not all_suppliers:
                yield "🤝 未查询到供应商数据。\n"
                return

            yield "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            yield "🤖 开始AI分析...\n"
            yield "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

            # 调用LLM分析
            prompt = self._build_stream_prompt(plans, all_suppliers)
            system_prompt = "你是一个专业的电力物料采购供应商匹配专家，擅长分析供应商协议数据并给出最优的供应商选择方案。请用清晰的中文进行分析。"

            async for chunk in self.llm_stream_func(prompt, system_prompt):
                yield chunk

        except Exception as e:
            yield f"❌ 分析失败: {str(e)}\n"
            import traceback
            yield f"详细信息: {traceback.format_exc()}\n"

    def _build_stream_prompt(self, plans: List[Dict[str, Any]], suppliers: List[Dict[str, Any]]) -> str:
        """构建流式接口的prompt"""

        # 转换计划数据
        plans_formatted = []
        for p in plans:
            plans_formatted.append({
                "计划ID": p.get('planId', ''),
                "物料编码": p.get('materialCode', ''),
                "物料描述": p.get('materialDesc', ''),
                "需求数量": float(p.get('demandQty', 0) or 0),
                "仓库编码": p.get('warehouseCode', ''),
                "技术规范ID": p.get('techSpecId', ''),
            })

        # 转换供应商数据
        suppliers_formatted = []
        for s in suppliers:
            suppliers_formatted.append({
                "供应商编码": s.get('supplierCode', ''),
                "供应商名称": s.get('supplierName', ''),
                "物料编码": s.get('materialCode', ''),
                "技术规范ID": s.get('techSpecId', ''),
                "执行比例(%)": float(s.get('executionRate', 0) or 0),
                "剩余可用数量": float(s.get('remainQty', 0) or 0),
                "剩余可用金额": float(s.get('remainAmount', 0) or 0),
                "协议单价": float(s.get('unitPrice', 0) or 0),
            })

        prompt = f"""你是一个专业的电力物料采购供应商匹配专家。我将提供补货需求计划和供应商协议数据，请你分析并给出最优的供应商选择方案。

## 任务说明
请分析以下补货需求计划，根据供应商的协议执行情况和可用库存，为每个需求选择最合适的供应商，并详细说明你的分析过程和理由。

## 输入数据

### 补货计划列表
{json.dumps(plans_formatted[:20], ensure_ascii=False, indent=2)}

### 供应商协议数据
{json.dumps(suppliers_formatted[:50], ensure_ascii=False, indent=2)}

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
        return prompt
