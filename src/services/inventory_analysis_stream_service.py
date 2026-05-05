# -*- coding: utf-8 -*-
"""库存分析服务 - 流式版本（复用原服务逻辑）"""
import json
import statistics
from datetime import datetime
from typing import List, Dict, Any, Optional

from ..services.inventory_analysis_service import InventoryAnalysisService
from ..services.context_manager import ContextManager


class InventoryAnalysisStreamService:
    """库存分析服务 - 流式版本（复用InventoryAnalysisService的数据查询逻辑）"""

    def __init__(self, db, llm_stream_func, llm_func=None):
        self.db = db
        self.llm_stream_func = llm_stream_func
        self.llm_func = llm_func
        self._service = InventoryAnalysisService.__new__(InventoryAnalysisService)
        self._service.db = db
        if llm_func:
            self.context_manager = ContextManager(llm_func, llm_stream_func)
        else:
            self.context_manager = None

    async def stream_analyze(self, start_date: str = None, end_date: str = None,
                            inventory_levels: List[str] = None, material_codes: List[str] = None,
                            season_factor_weight: float = None, safety_redundancy_ratio: float = None,
                            analyze_mode: str = "iterative"):
        """流式分析库存

        Args:
            start_date: 开始日期
            end_date: 结束日期
            inventory_levels: 库存层级列表
            material_codes: 物料编码列表
            season_factor_weight: 季节因子权重
            safety_redundancy_ratio: 安全冗余比例
            analyze_mode: 分析模式，"batch"一次性分析所有组合，"iterative"逐个分析(默认)
        """
        # 立即输出第一个消息，让用户知道服务正在处理
        yield "🚀 库存分析服务启动...\n"
        
        try:
            yield "🔍 [步骤 1/3] 正在查询仓库信息...\n"
            warehouse_info = await self._service._get_warehouse_info_by_levels(inventory_levels)
            warehouse_codes = list(warehouse_info.keys())

            if not warehouse_codes:
                yield "📦 未查询到符合条件的仓库。\n"
                return

            yield f"✅ [步骤 1/3] 已获取 {len(warehouse_codes)} 个仓库\n"
            yield f"   仓库列表: {', '.join(warehouse_codes[:5])}"
            if len(warehouse_codes) > 5:
                yield f" ... 还有{len(warehouse_codes) - 5}个"
            yield "\n\n"

            yield "🔍 [步骤 2/3] 正在获取物料编码列表...\n"
            if not material_codes or len(material_codes) == 0:
                material_codes = await self._service._get_all_material_codes()
            yield f"✅ [步骤 2/3] 共有 {len(material_codes)} 个物料编码\n"
            yield f"   物料: {', '.join(material_codes[:5])}"
            if len(material_codes) > 5:
                yield f" ... 还有{len(material_codes) - 5}个"
            yield "\n\n"

            yield "🔍 [步骤 3/3] 正在构建仓库×物料×tech_id组合...\n"
            all_combinations = []
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

            yield f"✅ [步骤 3/3] 共有 {len(all_combinations)} 个有效组合\n\n"

            if not all_combinations:
                yield "📊 没有找到有效的仓库×物料×tech_id组合。\n"
                return

            if analyze_mode == "batch":
                yield "📦 [批量分析模式] 将一次性分析所有组合...\n\n"
                async for chunk in self._batch_analyze(all_combinations, warehouse_info, start_date, end_date):
                    yield chunk
            else:
                yield "🔄 [迭代分析模式] 将逐个分析每个组合...\n\n"
                async for chunk in self._iterative_analyze(all_combinations, warehouse_info, start_date, end_date):
                    yield chunk

        except Exception as e:
            yield f"❌ 整体分析失败: {str(e)}\n"
            import traceback
            yield f"详细信息: {traceback.format_exc()}\n"

    async def _batch_analyze(self, all_combinations: List[Dict[str, Any]], warehouse_info: Dict,
                             start_date: str, end_date: str):
        """批量分析模式 - 一次性分析所有组合"""
        try:
            all_combo_data = []

            yield "🔍 正在收集所有组合的库存和历史数据...\n"

            for idx, combo in enumerate(all_combinations, 1):
                warehouse_code = combo['warehouse_code']
                material_code = combo['material_code']
                tech_id = combo['tech_id']

                if not tech_id:
                    continue

                warehouse_name = warehouse_info.get(warehouse_code, {}).get('name', '')
                inventory_level = warehouse_info.get(warehouse_code, {}).get('level', '')

                current_stock_data = await self._service._get_current_stock(warehouse_code, material_code, tech_id)
                current_stock = 0
                in_transit_stock = 0
                material_desc = ''
                if current_stock_data and len(current_stock_data) > 0:
                    current_stock = float(current_stock_data[0].get('current_stock', 0) or 0)
                    in_transit_stock = float(current_stock_data[0].get('in_transit_stock', 0) or 0)
                    material_desc = current_stock_data[0].get('material_desc', '') or ''

                outbound_data = await self._service._get_outbound_data(warehouse_code, material_code, tech_id, start_date, end_date)
                stats = self._calculate_outbound_stats(outbound_data, start_date, end_date)

                combo_data = {
                    'warehouse_code': warehouse_code,
                    'warehouse_name': warehouse_name,
                    'inventory_level': inventory_level,
                    'material_code': material_code,
                    'tech_id': tech_id,
                    'current_stock': current_stock,
                    'in_transit_stock': in_transit_stock,
                    'material_desc': material_desc,
                    'available_stock': current_stock + in_transit_stock,
                    '历史出库': [{
                        '月份': str(ob.get('posting_month', ob.get('month', ''))),
                        '出库数量': float(ob.get('outbound_qty', 0) or 0)
                    } for ob in outbound_data[:24]],
                    '统计数据': stats
                }
                all_combo_data.append(combo_data)

                yield f"✅ [组合 {idx}/{len(all_combinations)}] {warehouse_code} × {material_code} × {tech_id}\n"

            yield f"\n📊 共收集到 {len(all_combo_data)} 个组合的数据\n\n"

            yield "🤖 开始AI批量分析...\n"
            yield "────────────────────────────────────────\n"

            prompt = self._build_batch_prompt(all_combo_data, start_date, end_date)
            system_prompt = "你是一个专业的电力物资库存分析专家，擅长分析库存数据和历史消耗模式。请用清晰的中文进行分析。"

            if self.context_manager and self.context_manager.is_too_long(prompt):
                yield "⚠️ 检测到数据量较大，将采用分层推理模式...\n"
                async for chunk in self.context_manager._streaming_hierarchical_reasoning(prompt, system_prompt):
                    content = self._parse_llm_chunk(chunk)
                    if content:
                        yield content
            else:
                async for chunk in self.llm_stream_func(prompt, system_prompt):
                    content = self._parse_llm_chunk(chunk)
                    if content:
                        yield content

            yield "\n📊 分析汇总报告\n"
            yield "────────────────────────────────────────\n"
            yield f"   总组合数: {len(all_combo_data)}\n"
            yield f"   成功分析: {len(all_combo_data)}\n"
            yield f"   分析失败: 0\n"
            yield "────────────────────────────────────────\n"

        except Exception as e:
            yield f"❌ 批量分析失败: {str(e)}\n"
            import traceback
            yield f"详细信息: {traceback.format_exc()}\n"

    async def _iterative_analyze(self, all_combinations: List[Dict[str, Any]], warehouse_info: Dict,
                                 start_date: str, end_date: str):
        """迭代分析模式 - 逐个分析每个组合"""
        total_count = len(all_combinations)
        success_count = 0
        fail_count = 0

        for idx, combo in enumerate(all_combinations, 1):
            warehouse_code = combo['warehouse_code']
            material_code = combo['material_code']
            tech_id = combo['tech_id']

            if not tech_id:
                continue

            warehouse_name = warehouse_info.get(warehouse_code, {}).get('name', '')
            inventory_level = warehouse_info.get(warehouse_code, {}).get('level', '')

            yield "\n📋 [分析 {idx}/{total_count}] 开始分析组合\n".format(idx=idx, total_count=total_count)
            yield "────────────────────────────────────────\n"
            yield f"   仓库编码: {warehouse_code}\n"
            yield f"   仓库名称: {warehouse_name}\n"
            yield f"   库存层级: {inventory_level}\n"
            yield f"   物料编码: {material_code}\n"
            yield f"   技术规范ID: {tech_id}\n"
            yield "────────────────────────────────────────\n"

            try:
                yield "🔍 [子步骤 1/2] 查询当前库存数据...\n"
                current_stock_data = await self._service._get_current_stock(warehouse_code, material_code, tech_id)

                current_stock = 0
                in_transit_stock = 0
                material_desc = ''
                if current_stock_data and len(current_stock_data) > 0:
                    current_stock = float(current_stock_data[0].get('current_stock', 0) or 0)
                    in_transit_stock = float(current_stock_data[0].get('in_transit_stock', 0) or 0)
                    material_desc = current_stock_data[0].get('material_desc', '') or ''

                yield f"✅ [子步骤 1/2] 当前库存: {current_stock}, 在途库存: {in_transit_stock}\n"
                if material_desc:
                    yield f"   物料描述: {material_desc}\n"

                yield "🔍 [子步骤 2/2] 查询历史出库数据...\n"
                outbound_data = await self._service._get_outbound_data(warehouse_code, material_code, tech_id, start_date, end_date)

                if outbound_data:
                    yield f"✅ [子步骤 2/2] 获取到 {len(outbound_data)} 条历史出库记录\n"
                else:
                    yield "⚠️ [子步骤 2/2] 未查询到历史出库数据\n"

                stats = self._calculate_outbound_stats(outbound_data, start_date, end_date)

                yield f"📈 历史数据统计:\n"
                yield f"   最高月出库: {stats['max_outbound']:.0f}\n"
                yield f"   最低月出库: {stats['min_outbound']:.0f}\n"
                yield f"   平均月出库: {stats['avg_outbound']:.2f}\n"
                yield f"   中位数出库: {stats['median_outbound']:.2f}\n"
                if stats['std_dev']:
                    yield f"   标准差: {stats['std_dev']:.2f}\n"
                if stats['yoy_change'] is not None:
                    yield f"   同比变化: {stats['yoy_change']:+.1f}%\n"
                if stats['mom_change'] is not None:
                    yield f"   环比变化: {stats['mom_change']:+.1f}%\n"
                if stats['seasonality']:
                    yield f"   季节性特征: {stats['seasonality']}\n"

                combo_data = [{
                    'warehouse_code': warehouse_code,
                    'warehouse_name': warehouse_name,
                    'inventory_level': inventory_level,
                    'material_code': material_code,
                    'tech_id': tech_id,
                    'current_stock': current_stock,
                    'in_transit_stock': in_transit_stock,
                    'material_desc': material_desc,
                    'available_stock': current_stock + in_transit_stock,
                    '历史出库': [{
                        '月份': str(ob.get('posting_month', ob.get('month', ''))),
                        '出库数量': float(ob.get('outbound_qty', 0) or 0)
                    } for ob in outbound_data[:24]],
                    '统计数据': stats
                }]

                yield "\n🤖 开始AI智能分析...\n"
                yield "────────────────────────────────────────\n"

                prompt = self._build_single_combo_prompt(combo_data, start_date, end_date)
                system_prompt = "你是一个专业的电力物资库存分析专家，擅长分析库存数据和历史消耗模式。请用清晰的中文进行分析。"

                if self.context_manager and self.context_manager.is_too_long(prompt):
                    yield "⚠️ 检测到数据量较大，将采用分层推理模式...\n"
                    async for chunk in self.context_manager._streaming_hierarchical_reasoning(prompt, system_prompt):
                        content = self._parse_llm_chunk(chunk)
                        if content:
                            yield content
                else:
                    async for chunk in self.llm_stream_func(prompt, system_prompt):
                        content = self._parse_llm_chunk(chunk)
                        if content:
                            yield content

                yield "\n✅ [分析完成] 组合分析成功\n"
                success_count += 1

            except Exception as e:
                yield f"\n❌ [分析失败] {str(e)}\n"
                fail_count += 1

            yield "\n────────────────────────────────────────\n\n"

        yield "\n📊 分析汇总报告\n"
        yield "────────────────────────────────────────\n"
        yield f"   总组合数: {total_count}\n"
        yield f"   成功分析: {success_count}\n"
        yield f"   分析失败: {fail_count}\n"
        yield "────────────────────────────────────────\n"

    def _calculate_outbound_stats(self, outbound_data: List[Dict[str, Any]], start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """计算历史出库数据的统计指标"""
        stats = {
            'max_outbound': 0,
            'min_outbound': 0,
            'avg_outbound': 0,
            'median_outbound': 0,
            'std_dev': None,
            'yoy_change': None,
            'mom_change': None,
            'seasonality': None,
            'total_records': len(outbound_data)
        }

        if not outbound_data:
            return stats

        outbound_list = []
        month_map = {}
        for ob in outbound_data:
            qty = float(ob.get('outbound_qty', 0) or 0)
            month = str(ob.get('posting_month', ob.get('month', '')))
            outbound_list.append(qty)
            month_map[month] = qty

        if not outbound_list:
            return stats

        stats['max_outbound'] = max(outbound_list)
        stats['min_outbound'] = min(outbound_list)
        stats['avg_outbound'] = sum(outbound_list) / len(outbound_list)

        sorted_list = sorted(outbound_list)
        n = len(sorted_list)
        if n % 2 == 0:
            stats['median_outbound'] = (sorted_list[n//2-1] + sorted_list[n//2]) / 2
        else:
            stats['median_outbound'] = sorted_list[n//2]

        if len(outbound_list) > 1:
            mean = stats['avg_outbound']
            variance = sum((x - mean) ** 2 for x in outbound_list) / len(outbound_list)
            stats['std_dev'] = variance ** 0.5

        start_year = None
        end_year = None
        if start_date and len(start_date) == 6:
            start_year = int(start_date[:4])
        if end_date and len(end_date) == 6:
            end_year = int(end_date[:4])

        if start_year and end_year and end_year > start_year:
            sorted_months = sorted(month_map.keys(), reverse=True)
            if len(sorted_months) >= 12:
                recent_6m = [month_map[m] for m in sorted_months[:6] if m in month_map]
                recent_avg = sum(recent_6m) / len(recent_6m) if recent_6m else 0

                last_year_months = [m for m in sorted_months if m.startswith(str(end_year - 1))]
                last_year_6m = [month_map[m] for m in last_year_months[:6] if m in month_map]
                last_year_avg = sum(last_year_6m) / len(last_year_6m) if last_year_6m else 0

                if last_year_avg > 0:
                    stats['yoy_change'] = ((recent_avg - last_year_avg) / last_year_avg) * 100

        sorted_months = sorted(month_map.keys(), reverse=True)
        if len(sorted_months) >= 2:
            current_month = sorted_months[0]
            prev_month = sorted_months[1]
            current_qty = month_map.get(current_month, 0)
            prev_qty = month_map.get(prev_month, 0)
            if prev_qty > 0:
                stats['mom_change'] = ((current_qty - prev_qty) / prev_qty) * 100

        if len(outbound_list) >= 6:
            mean = stats['avg_outbound']
            std = stats.get('std_dev')
            if std and mean > 0:
                cv = std / mean
                if cv > 0.5:
                    stats['seasonality'] = "波动较大，存在季节性特征"
                elif cv > 0.25:
                    stats['seasonality'] = "波动适中"
                else:
                    stats['seasonality'] = "波动较小，消耗稳定"

        return stats

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
            return chunk.strip()
        return None

    def _build_batch_prompt(self, all_combo_data: List[Dict[str, Any]], start_date: str, end_date: str) -> str:
        """为批量分析构建prompt"""
        period_desc = f"{start_date} 至 {end_date}" if start_date and end_date else "全部历史数据"

        combo_list = []
        for i, data in enumerate(all_combo_data, 1):
            stats = data.get('统计数据', {})
            combo_list.append({
                '序号': i,
                '仓库编码': data.get('warehouse_code', ''),
                '仓库名称': data.get('warehouse_name', ''),
                '库存层级': data.get('inventory_level', ''),
                '物料编码': data.get('material_code', ''),
                '技术规范ID': data.get('tech_id', ''),
                '物料描述': data.get('material_desc', ''),
                '当前库存': data.get('current_stock', 0),
                '在途库存': data.get('in_transit_stock', 0),
                '实际可用库存': data.get('available_stock', 0),
                '历史最高月出库': stats.get('max_outbound', 0),
                '历史最低月出库': stats.get('min_outbound', 0),
                '平均月出库': stats.get('avg_outbound', 0),
                '中位数出库': stats.get('median_outbound', 0),
                '同比变化': stats.get('yoy_change'),
                '环比变化': stats.get('mom_change'),
                '季节性特征': stats.get('seasonality', '数据不足'),
                '历史出库': data.get('历史出库', [])
            })

        prompt = f"""你是一个专业的电力物资库存分析专家。我将提供多个仓库-物料-技术规范组合的库存数据和历史出库数据，请你一次性分析所有组合并给出专业的库存分析建议。

## 任务说明
请一次性分析以下所有组合（共 {len(combo_list)} 个组合）的库存数据，评估每个组合的库存健康状况，计算合理库存水位，并给出补货或利库建议。

**重要**：你必须为**每一个组合**单独输出一份完整的分析结果，按照下面规定的格式，一个组合一个组合地列出结果。

## 时间范围
- 开始日期: {start_date if start_date else '未指定'}
- 结束日期: {end_date if end_date else '未指定'}

## 输入数据

### 组合数量
共有 {len(combo_list)} 个组合需要分析

### 组合数据列表
{json.dumps(combo_list, ensure_ascii=False, indent=2)}

## 水位线分析方法

### 水位线制定原则：
请根据历史出库数据统计，综合考虑以下因素，自主分析判断合适的水位线值：
- 历史消耗量趋势（最高、最低、平均、中位数）
- 数据离散程度（标准差）和消耗稳定性
- 同比环比变化趋势
- 季节性特征
- 供货周期（15-45天）
- 物料重要程度和使用场景

### 水位线定义：
- **应急线**：仓库存储的最低标准，低于此线必须走应急补库流程
- **补库线**：可以开始补库了，库存量可能有一定风险
- **高位线**：仓库库存已处于高点，完全不用再补库，可以考虑利库

### 水位判断标准：
- **紧急状态**: 实际可用库存 <= 应急线 → **立即紧急补货**
- **低水位**: 实际可用库存 > 应急线 且 <= 补库线 → **立即补库**
- **中水位**: 实际可用库存 > 补库线 且 <= 高位线 → **建议补库**
- **高水位**: 实际可用库存 > 高位线 → **正常**，无需补库，可考虑利库

## 输出格式要求

**重要**：你必须按照以下格式，为每一个组合单独输出一份完整的分析结果。

请使用Markdown格式输出，使用##、###标题，表格使用|分隔。

**输出结构必须包含以下内容，并严格按照顺序输出**：

---

## 【组合 1/{len(combo_list)}】库存分析

### 一、组合信息
- 仓库编码: [从输入数据中获取]
- 仓库名称: [从输入数据中获取]
- 库存层级: [从输入数据中获取]
- 物料编码: [从输入数据中获取]
- 技术规范ID: [从输入数据中获取]
- 物料描述: [从输入数据中获取]

### 二、当前库存状况
- 当前库存: [从输入数据中获取]
- 在途库存: [从输入数据中获取]
- 实际可用库存: [从输入数据中获取]

### 三、历史消耗分析
- 历史最高月出库: [从输入数据中获取]
- 历史最低月出库: [从输入数据中获取]
- 平均月出库: [从输入数据中获取]
- 中位数出库: [从输入数据中获取]
- 同比变化: [从输入数据中获取]
- 环比变化: [从输入数据中获取]
- 季节性特征: [从输入数据中获取]

### 四、水位线分析
| 指标 | 计算值 | 说明 |
|------|--------|------|
| 应急线 | [计算值] | 最低库存标准 |
| 补库线 | [计算值] | 可以开始补库 |
| 高位线 | [计算值] | 库存已处于高点 |

### 五、分析结论与建议
- 当前库存状态评估
- 是否需要补库
- 建议补货数量和时间

---

**然后继续输出组合2，组合3...直到所有{len(combo_list)}个组合都分析完毕**

请用简洁、清晰的语言进行分析，重点关注中位数、正态分布、同比环比等指标。
"""
        return prompt

    def _build_single_combo_prompt(self, combo_data, start_date, end_date):
        """为单个组合构建分析prompt"""
        data = combo_data[0]
        stats = data.get('统计数据', {})
        outbound_list = data.get('历史出库', [])

        prompt = f"""你是一个专业的电力物资库存分析专家。请分析以下库存数据，结合历史出库模式，判断当前库存是否充足，并给出补货或利库建议。

## 时间范围
- 开始日期: {start_date if start_date else '未指定'}
- 结束日期: {end_date if end_date else '未指定'}

## 基础信息
1. 仓库编码：{data.get('warehouse_code', '')}
2. 仓库名称：{data.get('warehouse_name', '')}
3. 库存层级：{data.get('inventory_level', '')}
4. 物料编码：{data.get('material_code', '')}
5. 技术规范ID：{data.get('tech_id', '')}
6. 当前库存：{data.get('current_stock', 0)}
7. 在途库存：{data.get('in_transit_stock', 0)}
8. 实际可用库存：{data.get('available_stock', 0)}（当前库存 + 在途库存）
9. 物料描述：{data.get('material_desc', '')}

## 历史出库数据统计（重要！）
请根据以下统计数据和原始数据，进行综合分析：

### 统计指标
| 指标 | 数值 | 说明 |
|------|------|------|
| 历史最高月出库 | {stats.get('max_outbound', 0):.0f} | 历史单月最大出库量 |
| 历史最低月出库 | {stats.get('min_outbound', 0):.0f} | 历史单月最小出库量 |
| 平均月出库 | {stats.get('avg_outbound', 0):.2f} | 所有月份的平均值 |
| 中位数出库 | {stats.get('median_outbound', 0):.2f} | 50%的月份出库量低于此值 |
| 标准差 | {"N/A" if not stats.get('std_dev') else f"{stats['std_dev']:.2f}"} | 出库量的离散程度 |
| 同比变化 | {"N/A" if stats.get('yoy_change') is None else f"{stats['yoy_change']:+.1f}%"} | 去年同期对比 |
| 环比变化 | {"N/A" if stats.get('mom_change') is None else f"{stats['mom_change']:+.1f}%"} | 上月对比 |
| 季节性特征 | {stats.get('seasonality', '数据不足')} | 出库波动特征 |
| 数据记录数 | {stats.get('total_records', 0)} | 历史出库记录条数 |

### 原始历史出库数据（按月份排序）
{json.dumps(outbound_list, ensure_ascii=False, indent=2)}

## 分析参数说明
请根据以下参数进行水位线分析：

### 1. 正态分布分析
- **中位数（median_outbound）**：是分布的中心点，50%的数据在此值以下
- 如果 **实际可用库存 > 中位数**，说明库存相对充足
- 如果 **实际可用库存 < 中位数**，说明库存相对紧张
- **标准差（std_dev）**：反映数据离散程度，标准差大说明消耗不稳定

### 2. 同比分析（yoy_change）
- **同比 > 0**：最近消耗相比去年同期增长，可能需要增加库存
- **同比 < 0**：最近消耗相比去年同期下降，可以适当减少库存
- **同比 = 0或N/A**：消耗相对稳定

### 3. 环比分析（mom_change）
- **环比 > 0**：本月消耗相比上月增长
- **环比 < 0**：本月消耗相比上月下降
- 帮助判断短期消耗趋势

### 4. 季节性判断
- **波动较大**：存在明显的季节性，需要考虑季节因素设置水位线
- **波动适中**：有一定的变化，但不是季节性的
- **波动较小**：消耗相对稳定，可以参考平均值设置水位线

## 水位线分析方法（非常重要）

### 水位线制定原则：
请根据历史出库数据统计，综合考虑以下因素，自主分析判断合适的水位线值：
- 历史消耗量趋势（最高、最低、平均、中位数）
- 数据离散程度（标准差）和消耗稳定性
- 同比环比变化趋势
- 季节性特征
- 供货周期（15-45天）
- 物料重要程度和使用场景

### 水位线定义：
- **应急线**：仓库存储的最低标准，低于此线必须走应急补库流程，不能再低了
- **补库线**：可以开始补库了，库存量可能有一定风险
- **高位线**：仓库库存已处于高点，完全不用再补库，可以考虑利库

### 水位判断标准：
- **紧急状态**: 实际可用库存 <= 应急线 → **立即紧急补货**
- **低水位**: 实际可用库存 > 应急线 且 <= 补库线 → **立即补库**
- **中水位**: 实际可用库存 > 补库线 且 <= 高位线 → **建议补库**
- **高水位**: 实际可用库存 > 高位线 → **正常**，无需补库，可考虑利库

## 分析要求

**输出格式要求：**
- 使用Markdown格式输出
- 使用##、### 标题
- 表格使用 | 分隔符
- 重要内容使用 **粗体**

**输出结构：**

## 库存分析结果

### 一、当前库存状况
- 当前库存数量、在途库存、实际可用库存
- 与中位数的对比（充足/紧张）

### 二、历史消耗分析
- 消耗趋势（同比、环比）
- 消耗稳定性（季节性特征）
- 离散程度（标准差分析）

### 三、水位线分析
| 指标 | 计算值 | 说明 |
|------|--------|------|
| 应急线 | 计算值 | 基于最低值 |
| 补库线 | 计算值 | 基于中位数 |
| 高位线 | 计算值 | 基于最高值 |
| 当前水位 | 判断结果 | 紧急/低/中/高 |

### 四、分析结论与补库建议
- 当前库存状态评估
- 是否需要补库
- 建议补货数量和时间

请用简洁、清晰的语言进行分析，重点关注中位数、正态分布、同比环比等指标，给出智能分析跟补库建议。
"""
        return prompt
