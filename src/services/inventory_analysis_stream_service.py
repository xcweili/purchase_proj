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
        """流式分析库存 - 完全复用原服务逻辑"""
        try:
            yield "🔍 [步骤 1/6] 正在查询仓库信息...\n"
            warehouse_info = await self._service._get_warehouse_info_by_levels(inventory_levels)
            warehouse_codes = list(warehouse_info.keys())

            if not warehouse_codes:
                yield "📦 未查询到符合条件的仓库。\n"
                return

            yield f"✅ [步骤 1/6] 已获取 {len(warehouse_codes)} 个仓库\n"
            yield f"   仓库列表: {', '.join(warehouse_codes[:5])}"
            if len(warehouse_codes) > 5:
                yield f" ... 还有{len(warehouse_codes) - 5}个"
            yield "\n\n"

            yield "🔍 [步骤 2/6] 正在获取物料编码列表...\n"
            if not material_codes or len(material_codes) == 0:
                material_codes = await self._service._get_all_material_codes()
            yield f"✅ [步骤 2/6] 共有 {len(material_codes)} 个物料编码\n"
            yield f"   物料: {', '.join(material_codes[:5])}"
            if len(material_codes) > 5:
                yield f" ... 还有{len(material_codes) - 5}个"
            yield "\n\n"

            yield "🔍 [步骤 3/6] 正在构建仓库×物料×tech_id组合...\n"
            all_combinations = []
            combo_count = 0
            for warehouse_code in warehouse_codes:
                for material_code in material_codes:
                    tech_ids = await self._service._get_tech_ids_by_warehouse_material(
                        warehouse_code, material_code, start_date, end_date
                    )
                    if not tech_ids:
                        continue
                    for tech_id in tech_ids:
                        all_combinations.append({
                            'warehouse_code': warehouse_code,
                            'material_code': material_code,
                            'tech_id': tech_id
                        })
                        combo_count += 1
                        if combo_count <= 3:
                            yield f"   组合预览: {warehouse_code} + {material_code} + {tech_id}\n"

            yield f"✅ [步骤 3/6] 共有 {len(all_combinations)} 个有效组合\n\n"

            if not all_combinations:
                yield "📊 没有找到有效的仓库×物料×tech_id组合。\n"
                return

            yield "🔍 [步骤 4/6] 正在查询每个组合的当前库存和历史出库数据...\n"
            analyzed_data = []
            data_count = 0

            for combo in all_combinations:
                warehouse_code = combo['warehouse_code']
                material_code = combo['material_code']
                tech_id = combo['tech_id']

                if not tech_id:
                    continue

                warehouse_name = warehouse_info.get(warehouse_code, {}).get('name', '')
                inventory_level = warehouse_info.get(warehouse_code, {}).get('level', '')

                current_stock_data = await self._service._get_current_stock(warehouse_code, material_code, tech_id)
                outbound_data = await self._service._get_outbound_data(warehouse_code, material_code, tech_id, start_date, end_date)

                current_stock = 0
                in_transit_stock = 0
                material_desc = ''
                if current_stock_data and len(current_stock_data) > 0:
                    current_stock = float(current_stock_data[0].get('current_stock', 0) or 0)
                    in_transit_stock = float(current_stock_data[0].get('in_transit_stock', 0) or 0)
                    material_desc = current_stock_data[0].get('material_desc', '') or ''

                analyzed_data.append({
                    'warehouse_code': warehouse_code,
                    'warehouse_name': warehouse_name,
                    'inventory_level': inventory_level,
                    'material_code': material_code,
                    'tech_id': tech_id,
                    'current_stock': current_stock,
                    'in_transit_stock': in_transit_stock,
                    'material_desc': material_desc,
                    'outbound_data': outbound_data,
                    'is_cached': False
                })

                data_count += 1
                if data_count <= 3:
                    yield f"   处理进度: {data_count}/{len(all_combinations)}\n"

            yield f"✅ [步骤 4/6] 处理完成，共 {len(analyzed_data)} 个组合\n\n"

            yield "🔍 [步骤 5/6] 正在准备AI分析数据...\n"
            summary_stats = {
                'total_combinations': len(analyzed_data),
                'cached_count': 0,
                'new_analysis_count': len(analyzed_data),
                'total_current_stock': sum(float(d.get('current_stock', 0) or 0) for d in analyzed_data),
                'total_in_transit': sum(float(d.get('in_transit_stock', 0) or 0) for d in analyzed_data)
            }
            yield f"✅ [步骤 5/6] 汇总统计: 组合={summary_stats['total_combinations']}\n\n"

            yield "🔍 [步骤 6/6] 开始AI智能分析...\n"
            yield f"✅ [步骤 6/6] 数据准备完成\n\n"

            yield "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            yield "🤖 开始AI分析...\n"
            yield "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

            prompt = self._build_stream_prompt(analyzed_data, summary_stats, start_date, end_date)
            system_prompt = "你是一个专业的电力物资库存分析专家，擅长分析库存数据和历史消耗模式。请用清晰的中文进行分析。"

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
            # 如果不是JSON格式，直接返回原始内容
            return chunk.strip()
        return None

    def _build_stream_prompt(self, analyzed_data: List[Dict[str, Any]], summary_stats: Dict[str, Any],
                           start_date: str, end_date: str) -> str:
        """构建流式接口的prompt"""

        data_formatted = []
        for d in analyzed_data:
            item = {
                "仓库编码": d.get('warehouse_code', ''),
                "仓库名称": d.get('warehouse_name', ''),
                "库存层级": d.get('inventory_level', ''),
                "物料编码": d.get('material_code', ''),
                "技术规范ID": d.get('tech_id', ''),
                "当前库存": float(d.get('current_stock', 0) or 0),
                "在途库存": float(d.get('in_transit_stock', 0) or 0),
                "物料描述": d.get('material_desc', ''),
            }
            if not d.get('is_cached') and d.get('outbound_data'):
                outbound_list = []
                for ob in d.get('outbound_data', [])[:10]:
                    outbound_list.append({
                        "月份": str(ob.get('month', '')),
                        "出库数量": float(ob.get('outbound_qty', 0) or 0)
                    })
                item["历史出库"] = outbound_list
            data_formatted.append(item)

        prompt = f"""你是一个专业的电力物资库存分析专家。我将提供仓库×物料×技术规范ID组合的库存数据和历史出库数据，请你分析库存状况并给出建议。

## 任务说明
请分析以下库存数据，结合历史出库模式，判断当前库存是否充足，并给出补货或利库建议。

## 水位线分析方法（非常重要）

**分析步骤：**
1. **数据挖掘**: 仔细分析历史出库数据，提取关键指标：
   - 历史最高月出库量
   - 历史最低月出库量  
   - 平均月出库量
   - 季节性波动规律（如有）
   
2. **水位线制定**: 根据历史数据分析结果，智能确定三级水位线：
   - **应急线**: 库存警戒线，低于此线需立即紧急补货
   - **补库线**: 安全库存线，低于此线建议启动补货流程
   - **高位线**: 库存上限线，高于此线建议考虑利库

**水位判断标准：**
   - **紧急状态**: 当前库存 <= 应急线 → **立即紧急补货**
   - **低水位**: 当前库存 > 应急线 且 <= 补库线 → **立即补库**
   - **中水位**: 当前库存 > 补库线 且 <= 高位线 → **建议补库**
   - **高水位**: 当前库存 > 高位线 → **正常**，无需补库，可考虑利库

**考虑因素：**
- 物料消耗的季节性变化
- 历史出库的波动幅度
- 供应商交货周期
- 物料的重要程度和替代性

## 汇总统计
- 分析组合数量: {summary_stats['total_combinations']}
- 总当前库存: {summary_stats['total_current_stock']}
- 总在途库存: {summary_stats['total_in_transit']}

## 时间范围
- 开始日期: {start_date if start_date else '未指定'}
- 结束日期: {end_date if end_date else '未指定'}

## 输入数据

### 库存分析数据
{json.dumps(data_formatted[:30], ensure_ascii=False, indent=2)}

## 分析要求

**输出格式要求：**
- 请使用标准Markdown格式输出
- 标题使用 #、##、### 格式
- 列表使用 - 或数字开头
- 表格使用 | 分隔符
- 重要内容使用 **粗体**

**输出结构：**

# 电力物资库存分析报告

## 一、库存概览
简要描述整体库存状况，包括库存总量、在途库存等

## 二、重点物料分析

### 库存明细表（必须包含水位线）
| 物料编码 | 仓库编码 | 当前库存 | 应急线 | 补库线 | 高位线 | 水位状态 | 库存层级 |
|---------|---------|---------|--------|--------|--------|---------|---------|
| 示例数据 | WH001 | 100 | 30 | 80 | 120 | 高水位 | 周转库 |

### 水位状态说明
针对每个物料详细说明：
- 当前库存数量和在途库存
- 分析历史出库数据得出的关键指标（最高、最低、平均出库量）
- 根据历史数据分析制定的三级水位线（应急线、补库线、高位线）
- 判断当前所处水位（紧急/低/中/高）
- 是否需要补库及具体补库建议
- 季节性因素对库存的影响评估

## 三、出库模式分析
- 分析历史出库数据的规律
- 识别季节性变化（如有）
- 预测未来需求趋势

## 四、补货建议
- 哪些物料需要立即紧急补货（紧急状态）
- 哪些物料需要立即补货（低水位）
- 哪些物料需要建议补货（中水位）
- 哪些物料库存正常，可考虑利库（高水位）
- 针对每个物料的具体补货数量建议和时间安排

## 五、库存优化建议
- 如何提高库存周转率
- 如何减少库存积压
- 如何优化库存结构
- 季节性库存管理策略

请用自然、清晰的语言进行分析，重点关注三级水位线（应急线、补库线、高位线）的智能分析和补库建议。
"""
        return prompt
