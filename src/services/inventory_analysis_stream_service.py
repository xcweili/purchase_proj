# -*- coding: utf-8 -*-
"""库存分析服务 - 流式版本（复用原服务逻辑）"""
import json
from typing import List, Dict, Any, Optional

from ..services.inventory_analysis_service import InventoryAnalysisService


class InventoryAnalysisStreamService:
    """库存分析服务 - 流式版本（复用InventoryAnalysisService的数据查询逻辑）"""

    def __init__(self, db, llm_stream_func):
        self.db = db
        self.llm_stream_func = llm_stream_func
        self._service = InventoryAnalysisService.__new__(InventoryAnalysisService)
        self._service.db = db

    async def stream_analyze(self, start_date: str = None, end_date: str = None,
                            inventory_levels: List[str] = None, material_codes: List[str] = None,
                            season_factor_weight: float = None, safety_redundancy_ratio: float = None):
        """流式分析库存"""
        try:
            # 步骤1：获取仓库信息
            yield "🔍 [步骤 1/4] 正在查询仓库信息...\n"
            warehouse_info = await self._service._get_warehouse_info_by_levels(inventory_levels)
            warehouse_codes = list(warehouse_info.keys())

            if not warehouse_codes:
                yield "📦 未查询到符合条件的仓库。\n"
                return

            yield f"✅ [步骤 1/4] 已获取 {len(warehouse_codes)} 个仓库\n"
            yield f"   仓库列表: {', '.join(warehouse_codes[:5])}"
            if len(warehouse_codes) > 5:
                yield f" ... 还有{len(warehouse_codes) - 5}个"
            yield "\n\n"

            # 步骤2：获取物料编码
            yield "🔍 [步骤 2/4] 正在获取物料编码列表...\n"
            if not material_codes or len(material_codes) == 0:
                material_codes = await self._service._get_all_material_codes()

            if not material_codes:
                yield "📋 未获取到物料编码。\n"
                return

            yield f"✅ [步骤 2/4] 已获取 {len(material_codes)} 个物料编码\n"
            yield f"   物料: {', '.join(material_codes[:5])}"
            if len(material_codes) > 5:
                yield f" ... 还有{len(material_codes) - 5}个"
            yield "\n\n"

            # 步骤3：查询库存数据
            yield "🔍 [步骤 3/4] 正在查询库存数据...\n"
            all_stocks = []
            all_outbound = []

            for warehouse_code in warehouse_codes[:10]:
                for material_code in material_codes[:20]:
                    tech_ids = await self._service._tech_ids_by_warehouse_material(
                        warehouse_code, material_code, start_date, end_date
                    )
                    for tech_id in tech_ids:
                        stock = await self._service._get_current_stock(warehouse_code, material_code, tech_id)
                        if stock:
                            all_stocks.append(stock)
                        outbound = await self._service._get_historical_outbound(
                            warehouse_code, material_code, tech_id, start_date, end_date
                        )
                        if outbound:
                            all_outbound.append(outbound)

            yield f"✅ [步骤 3/4] 已获取 {len(all_stocks)} 条库存数据, {len(all_outbound)} 条出库数据\n\n"

            if not all_stocks:
                yield "📦 暂无库存数据。\n"
                return

            yield "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            yield "🤖 开始AI分析...\n"
            yield "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

            # 调用LLM分析
            prompt = self._build_stream_prompt(all_stocks, all_outbound, start_date, end_date,
                                              inventory_levels, season_factor_weight, safety_redundancy_ratio)
            system_prompt = "你是一个专业的电力物料库存分析专家，擅长分析库存数据、预测需求并给出合理的补货建议。请用清晰的中文进行分析。"

            async for chunk in self.llm_stream_func(prompt, system_prompt):
                yield chunk

        except Exception as e:
            yield f"❌ 分析失败: {str(e)}\n"
            import traceback
            yield f"详细信息: {traceback.format_exc()}\n"

    def _build_stream_prompt(self, stocks: List[Dict], outbound: List[Dict],
                            start_date: str = None, end_date: str = None,
                            inventory_levels: List[str] = None, season_factor: float = 0.3,
                            safety_ratio: float = 0.2) -> str:
        """构建流式接口的prompt"""

        # 周期描述
        if start_date and end_date:
            period_desc = f"{start_date} 至 {end_date}"
        elif start_date:
            period_desc = f"从 {start_date} 开始"
        elif end_date:
            period_desc = f"截至 {end_date}"
        else:
            period_desc = "全部历史数据"

        # 库存层级
        if inventory_levels:
            levels_desc = ", ".join(inventory_levels)
        else:
            levels_desc = "所有层级"

        prompt = f"""你是一个专业的电力物料库存分析专家。我将提供库存数据和历史出库数据，请你进行深度分析并给出专业建议。

## 任务说明
请分析以下库存数据和历史出库数据，评估库存健康状况，计算合理库存水位，并给出补货建议。

## 分析参数
- 分析周期：{period_desc}
- 库存层级：{levels_desc}
- 季节因子权重：{season_factor if season_factor else "默认(0.3)"}
- 安全冗余比例：{safety_ratio if safety_ratio else "默认(0.2)"}

## 当前库存数据
{json.dumps(stocks[:20], ensure_ascii=False, indent=2)}

## 历史出库数据
{json.dumps(outbound[:50], ensure_ascii=False, indent=2)}

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
        return prompt
