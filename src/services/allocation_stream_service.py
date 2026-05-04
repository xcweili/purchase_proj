# -*- coding: utf-8 -*-
"""调配服务 - 流式版本（复用原服务逻辑）"""
import json
from typing import List, Dict, Any, Optional

from ..services.allocation_service import AllocationService


class AllocationStreamService:
    """调配服务 - 流式版本（复用AllocationService的数据查询逻辑）"""

    def __init__(self, db, llm_stream_func):
        # 复用原服务的数据查询能力
        self.db = db
        self.llm_stream_func = llm_stream_func
        # 创建一个内部实例来使用其数据查询方法
        self._allocation_service = AllocationService.__new__(AllocationService)
        self._allocation_service.db = db

    async def stream_analyze(self, strategy: str = "time", warehouse_code: str = "",
                            source_type: str = "", project_unit: str = "",
                            demand_start_date: str = "", demand_end_date: str = "",
                            plan_type: str = "", material_codes: List[str] = None):
        """流式分析调配方案"""
        try:
            # 步骤1：查询计划数据
            yield "🔍 [步骤 1/4] 正在查询需求计划数据...\n"
            plans = self._allocation_service._query_plans(
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

            yield f"✅ [步骤 1/4] 已获取 {len(plans)} 条需求计划\n\n"
            yield f"📋 计划列表预览：\n"
            for i, plan in enumerate(plans[:5], 1):
                yield f"  {i}. 物料: {plan.get('materialCode', plan.get('fd_material_code', ''))}, "
                yield f"需求: {plan.get('demandQty', plan.get('fd_demand_qty', ''))}, "
                yield f"仓库: {plan.get('warehouseCode', plan.get('fd_warehouse_code', ''))}\n"
            if len(plans) > 5:
                yield f"  ... 还有 {len(plans) - 5} 条计划\n"
            yield "\n"

            # 步骤2：提取参数并查询库存
            yield "🔍 [步骤 2/4] 正在提取物料编码和技术规范ID...\n"
            material_codes_list = list(set(
                p.get('materialCode', p.get('fd_material_code', ''))
                for p in plans
                if p.get('materialCode') or p.get('fd_material_code')
            ))
            tech_ids_list = list(set(
                p.get('techSpecId', p.get('fd_tech_spec_id', ''))
                for p in plans
                if p.get('techSpecId') or p.get('fd_tech_spec_id')
            ))

            yield f"✅ [步骤 2/4] 提取到 {len(material_codes_list)} 个物料编码\n"
            yield f"   物料: {', '.join(material_codes_list[:5])}"
            if len(material_codes_list) > 5:
                yield f" ... 还有{len(material_codes_list) - 5}个"
            yield "\n"
            yield f"   技术规范ID: {len(tech_ids_list)} 个\n\n"

            # 步骤3：查询库存数据
            yield "🔍 [步骤 3/4] 正在查询库存数据...\n"
            stocks = self._allocation_service._query_stocks(
                material_codes=material_codes_list,
                source_type=source_type,
                target_warehouse=warehouse_code,
                tech_ids=tech_ids_list if tech_ids_list else None
            )

            if not stocks:
                yield "📦 未查询到符合条件的库存数据。\n"
                return

            yield f"✅ [步骤 3/4] 已获取 {len(stocks)} 条库存记录\n\n"
            yield f"📦 库存预览（前5条）：\n"
            for i, stock in enumerate(stocks[:5], 1):
                yield f"  {i}. 仓库: {stock.get('loc_code', '')}, "
                yield f"物料: {stock.get('material_code', '')}, "
                yield f"库存: {stock.get('stock_qty', 0)}\n"
            if len(stocks) > 5:
                yield f"  ... 还有 {len(stocks) - 5} 条库存记录\n"
            yield "\n"

            # 步骤4：查询距离数据并开始LLM分析
            yield "🔍 [步骤 4/4] 正在查询仓库距离数据...\n"
            distances_map = self._allocation_service._get_warehouse_distances()
            yield f"✅ [步骤 4/4] 已获取 {len(distances_map)} 个仓库的距离信息\n\n"

            yield "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            yield "🤖 开始AI分析...\n"
            yield "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

            # 调用LLM分析
            prompt = self._build_stream_prompt(plans, stocks, distances_map, strategy)
            system_prompt = "你是一个专业的电力物料仓库调配专家，擅长分析库存数据并给出最优调配方案。请用清晰的中文进行分析。"

            async for chunk in self.llm_stream_func(prompt, system_prompt):
                yield chunk

        except Exception as e:
            yield f"❌ 分析失败: {str(e)}\n"
            import traceback
            yield f"详细信息: {traceback.format_exc()}\n"

    def _build_stream_prompt(self, plans: List[Dict[str, Any]], stocks: List[Dict[str, Any]],
                            distances_map: Dict[str, float], strategy: str = 'time') -> str:
        """构建流式接口的prompt"""

        # 转换计划数据格式
        plans_formatted = []
        for p in plans:
            plans_formatted.append({
                "计划ID": p.get('planId', p.get('fd_plan_id', '')),
                "物料编码": p.get('materialCode', p.get('fd_material_code', '')),
                "物料描述": p.get('materialDesc', p.get('fd_material_desc', '')),
                "需求数量": float(p.get('demandQty', p.get('fd_demand_qty', 0) or 0)),
                "单位": p.get('unit', p.get('fd_unit', '')),
                "目标仓库": p.get('warehouseCode', p.get('fd_warehouse_code', '')),
                "技术规范ID": p.get('techSpecId', p.get('fd_tech_spec_id', '')),
            })

        # 添加距离信息到库存数据
        stocks_formatted = []
        for s in stocks:
            loc_code = s.get('loc_code', '')
            stocks_formatted.append({
                "仓库编码": loc_code,
                "仓库名称": s.get('loc_name', ''),
                "物料编码": s.get('material_code', ''),
                "技术规范ID": s.get('tech_id', ''),
                "库存数量": float(s.get('stock_qty', 0) or 0),
                "库存类型": s.get('source_type', ''),
                "距离(km)": distances_map.get(loc_code, 0),
            })

        strategy_descriptions = {
            'time': '时效优先策略：优先选择距离最近的仓库进行调配，以最快速度满足需求。',
            'cost': '成本优先策略：优先选择距离最近的仓库，以降低运输成本。',
            'stock': '库存优先策略：优先选择库存充足的仓库，确保能够满足需求。',
            'emerg': '紧急调配策略：综合考虑距离和库存，以最快速度响应紧急需求。'
        }

        prompt = f"""你是一个专业的电力物料仓库调配专家。我将提供物料需求计划和可用库存数据，请你分析并给出调配建议。

## 任务说明
请分析以下物料需求计划，从可用库存中选择最合适的仓库进行调配，并详细说明你的分析过程和理由。

## 调配策略
{strategy_descriptions.get(strategy, strategy_descriptions['time'])}

## 需求计划列表
{json.dumps(plans_formatted, ensure_ascii=False, indent=2)}

## 当前仓库与其他仓库的距离（单位：km）
{json.dumps(distances_map, ensure_ascii=False, indent=2)}

## 可用库存数据
{json.dumps(stocks_formatted, ensure_ascii=False, indent=2)}

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
        return prompt
