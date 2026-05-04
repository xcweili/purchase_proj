# -*- coding: utf-8 -*-
"""调配服务 - 流式版本（复用原服务逻辑）"""
import json
from typing import List, Dict, Any, Optional

from ..services.allocation_service import AllocationService


class AllocationStreamService:
    """调配服务 - 流式版本（复用AllocationService的数据查询逻辑）"""

    def __init__(self, db, llm_stream_func):
        self.db = db
        self.llm_stream_func = llm_stream_func
        self._service = AllocationService.__new__(AllocationService)
        self._service.db = db

    async def stream_analyze(self, strategy: str = "time", warehouse_code: str = "",
                            source_type: str = "", project_unit: str = "",
                            demand_start_date: str = "", demand_end_date: str = "",
                            plan_type: str = "", material_codes: List[str] = None):
        """流式分析调配方案 - 完全复用原服务逻辑"""
        try:
            yield "🔍 [步骤 1/6] 正在查询需求计划数据...\n"
            plans = await self._service._query_plans(
                project_unit=project_unit,
                start_date=demand_start_date,
                end_date=demand_end_date,
                plan_type=plan_type,
                warehouse_code=warehouse_code,
                material_codes=material_codes
            )

            if not plans:
                yield "📋 未查询到符合条件的需求计划。\n"
                return

            yield f"✅ [步骤 1/6] 已获取 {len(plans)} 条需求计划\n\n"
            yield f"📋 计划预览（前5条）：\n"
            for i, plan in enumerate(plans[:5], 1):
                yield f"  {i}. 物料: {plan.get('materialCode', '')}, "
                yield f"需求: {plan.get('demandQty', 0)}, "
                yield f"仓库: {plan.get('warehouseCode', '')}\n"
            if len(plans) > 5:
                yield f"  ... 还有 {len(plans) - 5} 条计划\n"
            yield "\n"

            yield "🔍 [步骤 2/6] 正在提取物料编码和技术规范ID...\n"
            material_codes_list = self._service._extract_material_codes(plans)
            tech_ids_list = self._service._extract_tech_ids(plans)
            yield f"✅ [步骤 2/6] 提取到 {len(material_codes_list)} 个物料编码\n"
            yield f"   物料: {', '.join(material_codes_list[:5])}"
            if len(material_codes_list) > 5:
                yield f" ... 还有{len(material_codes_list) - 5}个"
            yield "\n"
            yield f"   技术规范ID: {len(tech_ids_list)} 个\n\n"

            yield "🔍 [步骤 3/6] 正在查询库存数据...\n"
            stocks = self._service._query_stocks(
                material_codes=material_codes_list,
                source_type=source_type,
                target_warehouse=warehouse_code,
                tech_ids=tech_ids_list if tech_ids_list else None
            )

            if not stocks:
                yield "📦 未查询到符合条件的库存数据。\n"
                return

            yield f"✅ [步骤 3/6] 已获取 {len(stocks)} 条库存记录\n\n"
            yield f"📦 库存预览（前5条）：\n"
            for i, stock in enumerate(stocks[:5], 1):
                yield f"  {i}. 仓库: {stock.get('loc_code', '')}, "
                yield f"物料: {stock.get('material_code', '')}, "
                yield f"库存: {stock.get('stock_qty', 0)}\n"
            if len(stocks) > 5:
                yield f"  ... 还有 {len(stocks) - 5} 条库存记录\n"
            yield "\n"

            yield "🔍 [步骤 4/6] 正在构建物料来源类型映射...\n"
            material_source_types = self._service._build_material_source_type_map(stocks)
            yield f"✅ [步骤 4/6] 已构建 {len(material_source_types)} 个物料的来源映射\n\n"

            yield "🔍 [步骤 5/6] 正在处理调配逻辑...\n"
            processed_plans = []
            for i, plan in enumerate(plans, 1):
                material_code = plan.get('materialCode', '')
                tech_id = plan.get('techSpecId', '')
                demand_qty = float(plan.get('demandQty', 0) or 0)
                target_warehouse = plan.get('warehouseCode', '')

                matching_stocks = [s for s in stocks if s.get('material_code') == material_code]

                if not matching_stocks:
                    processed_plans.append({
                        'plan': plan,
                        'status': 'none',
                        'matches': [],
                        'total_available': 0,
                        'demand': demand_qty
                    })
                    continue

                total_available = sum(float(s.get('stock_qty', 0) or 0) for s in matching_stocks)
                sorted_stocks = sorted(matching_stocks, key=lambda x: float(x.get('stock_qty', 0) or 0), reverse=True)

                matches = []
                remaining_qty = demand_qty
                for stock in sorted_stocks:
                    if remaining_qty <= 0:
                        break
                    allocate_qty = min(remaining_qty, float(stock.get('stock_qty', 0) or 0))
                    matches.append({
                        'source_warehouse': stock.get('loc_code', ''),
                        'source_warehouse_name': stock.get('loc_name', ''),
                        'allocate_qty': allocate_qty,
                        'stock_qty': float(stock.get('stock_qty', 0) or 0),
                        'distance': stock.get('distance', ''),
                        'source_type': stock.get('source_type', '')
                    })
                    remaining_qty -= allocate_qty

                status = 'full' if remaining_qty <= 0 else 'partial' if matches else 'none'
                processed_plans.append({
                    'plan': plan,
                    'status': status,
                    'matches': matches,
                    'total_available': total_available,
                    'demand': demand_qty
                })

                if i <= 3:
                    yield f"   处理进度: {i}/{len(plans)}\n"

            full_match = sum(1 for p in processed_plans if p['status'] == 'full')
            partial_match = sum(1 for p in processed_plans if p['status'] == 'partial')
            none_match = sum(1 for p in processed_plans if p['status'] == 'none')

            yield f"✅ [步骤 5/6] 处理完成\n"
            yield f"   完全匹配: {full_match}, 部分匹配: {partial_match}, 无匹配: {none_match}\n\n"

            yield "🔍 [步骤 6/6] 正在构建AI分析数据...\n"
            yield f"✅ [步骤 6/6] 数据准备完成，开始AI分析\n\n"

            yield "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            yield "🤖 开始AI智能分析...\n"
            yield "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

            prompt = self._build_stream_prompt(processed_plans, strategy, warehouse_code)
            system_prompt = "你是一个专业的电力物资调配专家，擅长分析库存分布并给出最优的调配方案。请用清晰的中文进行分析。"

            async for chunk in self.llm_stream_func(prompt, system_prompt):
                content = self._parse_llm_chunk(chunk)
                if content:
                    yield content

        except Exception as e:
            yield f"❌ 分析失败: {str(e)}\n"
            import traceback
            yield f"详细信息: {traceback.format_exc()}\n"

    def _parse_llm_chunk(self, chunk: str) -> Optional[str]:
        """解析LLM返回的JSON格式chunk，提取内容和思考过程"""
        try:
            data = json.loads(chunk)
            choices = data.get('choices', [])
            if choices:
                delta = choices[0].get('delta', {})
                reasoning = delta.get('reasoning_content', '')
                content = delta.get('content', '')
                
                if reasoning:
                    return f"{reasoning}"
                elif content:
                    return content
        except json.JSONDecodeError:
            pass
        return None

    def _build_stream_prompt(self, processed_plans: List[Dict[str, Any]], strategy: str, target_warehouse: str) -> str:
        """构建流式接口的prompt"""

        plans_formatted = []
        for p in processed_plans:
            plan = p.get('plan', {})
            plans_formatted.append({
                "计划ID": plan.get('planId', ''),
                "物料编码": plan.get('materialCode', ''),
                "物料描述": plan.get('materialDesc', ''),
                "需求数量": float(p.get('demand', 0) or 0),
                "目标仓库": plan.get('warehouseCode', ''),
                "技术规范ID": plan.get('techSpecId', ''),
                "匹配状态": p.get('status', ''),
                "总可用库存": float(p.get('total_available', 0) or 0),
                "调配方案": [{
                    'source_warehouse': m.get('source_warehouse', ''),
                    'source_warehouse_name': m.get('source_warehouse_name', ''),
                    'allocate_qty': float(m.get('allocate_qty', 0) or 0),
                    'stock_qty': float(m.get('stock_qty', 0) or 0),
                    'distance': str(m.get('distance', '')),
                    'source_type': m.get('source_type', '')
                } for m in p.get('matches', [])]
            })

        prompt = f"""你是一个专业的电力物资调配专家。我将提供需求计划和库存数据，请你分析并给出最优的调配方案。

## 任务说明
请分析以下需求计划，根据库存分布情况，为每个需求计划制定最优的物资调配方案，并详细说明你的分析过程和理由。

## 输入数据

### 需求计划列表
{json.dumps(plans_formatted[:30], ensure_ascii=False, indent=2)}

### 调配策略
当前策略: {strategy}

策略说明:
- time: 时效优先策略，优先选择距离近的仓库
- cost: 成本优先策略，优先选择库存充足的仓库
- stock: 库存均衡策略，尽量平衡各仓库库存
- emerg: 紧急优先策略，优先满足紧急需求

### 目标仓库
{target_warehouse if target_warehouse else '未指定'}

## 分析要求

请按照以下结构输出详细的分析报告：

1. **需求概览**：简要描述本次需要调配的物料种类和总体需求

2. **库存分析**：
   - 各仓库的库存分布情况
   - 库存充足程度分析
   - 是否存在缺货风险

3. **调配方案**：
   - 针对每个需求计划，详细说明调配方案
   - 调配来源仓库和数量
   - 选择该方案的理由

4. **优化建议**：
   - 如何提高调配效率
   - 如何减少调配成本
   - 如何平衡库存

5. **结果汇总**：总结本次调配的总体情况

请用自然、清晰的语言进行分析，让用户能够理解你的决策过程。
"""
        return prompt
