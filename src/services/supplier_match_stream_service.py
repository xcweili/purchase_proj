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
        """流式分析供应商匹配 - 完全复用原服务逻辑"""
        try:
            yield "🔍 [步骤 1/6] 正在获取补货计划...\n"
            if input_plans and len(input_plans) > 0:
                plans = input_plans
                yield f"✅ [步骤 1/6] 已获取 {len(plans)} 条输入计划\n\n"
            else:
                plans = await self._service._query_plans()
                yield f"✅ [步骤 1/6] 已从数据库获取 {len(plans)} 条补货计划\n\n"

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

            yield "🔍 [步骤 2/6] 正在提取物料编码和技术规范ID...\n"
            material_tech_pairs = []
            for plan in plans:
                mc = plan.get('materialCode', '')
                tc = plan.get('techSpecId', '')
                if mc and tc:
                    material_tech_pairs.append((mc, tc))

            unique_pairs = list(set(material_tech_pairs))
            yield f"✅ [步骤 2/6] 共有 {len(unique_pairs)} 个唯一的物料×技术规范ID组合\n\n"

            yield "🔍 [步骤 3/6] 正在查询协议商库存数据...\n"
            all_supplier_data = []
            for i, plan in enumerate(plans):
                material_code = plan.get('materialCode', '')
                tech_id = plan.get('techSpecId', '')
                warehouse_code = plan.get('warehouseCode', '')

                material_desc = plan.get('materialDesc', '') or plan.get('fd_desc', '')
                if not material_desc:
                    material_desc = await self._service._get_material_desc_from_stock(material_code, tech_id)

                company = await self._service._get_company_from_warehouse(warehouse_code)

                suppliers = await self._service._get_protocol_suppliers(plan)

                plan_data = {
                    'plan': plan,
                    'material_desc': material_desc,
                    'company': company,
                    'suppliers': suppliers,
                    'supplier_count': len(suppliers)
                }
                all_supplier_data.append(plan_data)

                if i < 3:
                    yield f"   处理进度: {i+1}/{len(plans)}, 供应商: {len(suppliers)}\n"

            total_suppliers = sum(d['supplier_count'] for d in all_supplier_data)
            plans_with_suppliers = sum(1 for d in all_supplier_data if d['supplier_count'] > 0)
            yield f"✅ [步骤 3/6] 查询完成，共获取 {total_suppliers} 条供应商记录\n"
            yield f"   有供应商匹配的计划: {plans_with_suppliers}/{len(plans)}\n\n"

            yield "🔍 [步骤 4/6] 正在分析供应商执行比例...\n"
            supplier_stats = {
                'total_suppliers': total_suppliers,
                'plans_with_matches': plans_with_suppliers,
                'plans_without_matches': len(plans) - plans_with_suppliers,
                'avg_execution_rate': 0,
                'execution_rate_distribution': {'0-20%': 0, '20-50%': 0, '50-80%': 0, '80%+': 0}
            }

            all_rates = []
            for data in all_supplier_data:
                for supplier in data['suppliers']:
                    rate = float(supplier.get('executionRate', 0) or 0)
                    all_rates.append(rate)
                    if rate < 20:
                        supplier_stats['execution_rate_distribution']['0-20%'] += 1
                    elif rate < 50:
                        supplier_stats['execution_rate_distribution']['20-50%'] += 1
                    elif rate < 80:
                        supplier_stats['execution_rate_distribution']['50-80%'] += 1
                    else:
                        supplier_stats['execution_rate_distribution']['80%+'] += 1

            if all_rates:
                supplier_stats['avg_execution_rate'] = sum(all_rates) / len(all_rates)

            yield f"✅ [步骤 4/6] 执行比例分布: 0-20%: {supplier_stats['execution_rate_distribution']['0-20%']}, "
            yield f"20-50%: {supplier_stats['execution_rate_distribution']['20-50%']}, "
            yield f"50-80%: {supplier_stats['execution_rate_distribution']['50-80%']}, "
            yield f"80%+: {supplier_stats['execution_rate_distribution']['80%+']}\n\n"

            yield "🔍 [步骤 5/6] 正在准备AI分析数据...\n"
            yield f"✅ [步骤 5/6] 汇总统计: 总供应商={total_suppliers}, "
            yield f"有匹配计划={plans_with_suppliers}, "
            yield f"无匹配计划={len(plans) - plans_with_suppliers}\n\n"

            yield "🔍 [步骤 6/6] 开始AI智能分析...\n"
            yield f"✅ [步骤 6/6] 数据准备完成\n\n"

            yield "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            yield "🤖 开始AI分析...\n"
            yield "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

            prompt = self._build_stream_prompt(all_supplier_data, supplier_stats)
            system_prompt = "你是一个专业的电力物料采购供应商匹配专家，擅长分析供应商协议数据并给出最优的供应商选择方案。请用清晰的中文进行分析。"

            async for chunk in self.llm_stream_func(prompt, system_prompt):
                yield chunk

        except Exception as e:
            yield f"❌ 分析失败: {str(e)}\n"
            import traceback
            yield f"详细信息: {traceback.format_exc()}\n"

    def _build_stream_prompt(self, all_supplier_data: List[Dict[str, Any]], supplier_stats: Dict[str, Any]) -> str:
        """构建流式接口的prompt"""

        plans_formatted = []
        for data in all_supplier_data:
            plan = data.get('plan', {})
            suppliers = data.get('suppliers', [])

            suppliers_summary = []
            for s in suppliers[:5]:
                suppliers_summary.append({
                    "供应商编码": s.get('supplierCode', ''),
                    "供应商名称": s.get('supplierName', ''),
                    "执行比例(%)": float(s.get('executionRate', 0) or 0),
                    "剩余可用数量": float(s.get('remainQty', 0) or 0),
                    "剩余可用金额": float(s.get('remainAmount', 0) or 0),
                    "协议单价": float(s.get('unitPrice', 0) or 0),
                })

            plans_formatted.append({
                "计划ID": plan.get('planId', ''),
                "物料编码": plan.get('materialCode', ''),
                "物料描述": data.get('material_desc', ''),
                "需求数量": float(plan.get('demandQty', 0) or 0),
                "仓库编码": plan.get('warehouseCode', ''),
                "技术规范ID": plan.get('techSpecId', ''),
                "所属单位": data.get('company', ''),
                "供应商数量": data.get('supplier_count', 0),
                "供应商详情": suppliers_summary
            })

        prompt = f"""你是一个专业的电力物料采购供应商匹配专家。我将提供补货需求计划和供应商协议数据，请你分析并给出最优的供应商选择方案。

## 任务说明
请分析以下补货需求计划，根据供应商的协议执行情况和可用库存，为每个需求选择最合适的供应商，并详细说明你的分析过程和理由。

## 汇总统计
- 总计划数: {len(all_supplier_data)}
- 有供应商匹配的计划: {supplier_stats['plans_with_matches']}
- 无供应商匹配的计划: {supplier_stats['plans_without_matches']}
- 总供应商数: {supplier_stats['total_suppliers']}
- 平均执行比例: {supplier_stats['avg_execution_rate']:.1f}%
- 执行比例分布:
  - 0-20%: {supplier_stats['execution_rate_distribution']['0-20%']}家
  - 20-50%: {supplier_stats['execution_rate_distribution']['20-50%']}家
  - 50-80%: {supplier_stats['execution_rate_distribution']['50-80%']}家
  - 80%+: {supplier_stats['execution_rate_distribution']['80%+']}家

## 输入数据

### 补货计划与供应商数据
{json.dumps(plans_formatted[:20], ensure_ascii=False, indent=2)}

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
