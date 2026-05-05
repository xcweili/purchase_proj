# -*- coding: utf-8 -*-
"""调配服务 - 流式版本（复用原服务逻辑）"""
import json
from typing import List, Dict, Any, Optional

from ..services.allocation_service import AllocationService
from ..services.context_manager import ContextManager


class AllocationStreamService:
    """调配服务 - 流式版本（复用AllocationService的数据查询逻辑）"""

    def __init__(self, db, llm_stream_func, llm_func=None):
        self.db = db
        self.llm_stream_func = llm_stream_func
        self.llm_func = llm_func
        self._service = AllocationService.__new__(AllocationService)
        self._service.db = db
        # 初始化上下文管理器
        if llm_func:
            self.context_manager = ContextManager(llm_func, llm_stream_func)
        else:
            self.context_manager = None

    async def stream_analyze(self, strategy: str = "time", warehouse_code: str = "",
                            source_type: str = "", project_unit: str = "",
                            demand_start_date: str = "", demand_end_date: str = "",
                            plan_type: str = "", material_codes: List[str] = None):
        """流式分析调配方案 - 按计划维度遍历，每个计划单独调用LLM分析"""
        try:
            # ==================== 步骤1: 查询需求计划数据 ====================
            yield "🔍 [步骤 1/4] 正在查询需求计划数据...\n"
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

            yield f"✅ [步骤 1/4] 已获取 {len(plans)} 条需求计划\n\n"

            # ==================== 步骤2: 提取物料编码和技术规范ID ====================
            yield "🔍 [步骤 2/4] 正在提取物料编码和技术规范ID...\n"
            material_codes_list = self._service._extract_material_codes(plans)
            tech_ids_list = self._service._extract_tech_ids(plans)
            yield f"✅ [步骤 2/4] 提取到 {len(material_codes_list)} 个物料编码\n"
            yield f"   物料: {', '.join(material_codes_list[:5])}"
            if len(material_codes_list) > 5:
                yield f" ... 还有{len(material_codes_list) - 5}个"
            yield "\n"
            yield f"   技术规范ID: {len(tech_ids_list)} 个\n\n"

            # ==================== 步骤3: 查询库存数据 ====================
            yield "🔍 [步骤 3/4] 正在查询库存数据...\n"
            stocks = self._service._query_stocks(
                material_codes=material_codes_list,
                source_type=source_type,
                target_warehouse=warehouse_code,
                tech_ids=tech_ids_list if tech_ids_list else None
            )

            if not stocks:
                yield "📦 未查询到符合条件的库存数据。\n"
                return

            yield f"✅ [步骤 3/4] 已获取 {len(stocks)} 条库存记录\n\n"

            # ==================== 步骤4: 构建物料来源类型映射 ====================
            yield "🔍 [步骤 4/4] 正在构建物料来源类型映射...\n"
            material_source_types = self._service._build_material_source_type_map(stocks)
            yield f"✅ [步骤 4/4] 已构建 {len(material_source_types)} 个物料的来源映射\n\n"

            # ==================== 步骤5: 逐一处理每个计划 ====================
            total_count = len(plans)
            full_match_count = 0
            partial_match_count = 0
            none_match_count = 0

            for idx, plan in enumerate(plans, 1):
                plan_id = plan.get('planId') or plan.get('plan_id', f'plan_{idx}')
                material_code = plan.get('materialCode') or plan.get('material_code', '')
                tech_spec_id = plan.get('techSpecId') or ''
                demand_qty = float(plan.get('demandQty') or plan.get('demand_qty', 0))
                target_warehouse = plan.get('warehouseCode', '')
                material_desc = plan.get('materialDesc', '')

                # 输出当前处理进度和参数
                yield "\n📋 [处理 {idx}/{total_count}] 开始处理计划\n".format(idx=idx, total_count=total_count)
                yield "────────────────────────────────────────\n"
                yield f"   计划ID: {plan_id}\n"
                yield f"   物料编码: {material_code}\n"
                yield f"   物料描述: {material_desc}\n"
                yield f"   技术规范ID: {tech_spec_id}\n"
                yield f"   需求数量: {demand_qty}\n"
                yield f"   目标仓库: {target_warehouse}\n"
                yield f"   策略: {strategy}\n"
                yield "────────────────────────────────────────\n"

                try:
                    # 查询当前物料的可用库存
                    yield "🔍 [子步骤 1/2] 查询物料库存...\n"
                    matching_stocks = [s for s in stocks if s.get('material_code') == material_code]
                    
                    if matching_stocks:
                        yield f"✅ [子步骤 1/2] 找到 {len(matching_stocks)} 个库存记录\n"
                        for i, stock in enumerate(matching_stocks[:3], 1):
                            yield f"   [{i}] 仓库: {stock.get('loc_code', '')}, 库存: {stock.get('stock_qty', 0)}, 类型: {stock.get('source_type', '')}\n"
                        if len(matching_stocks) > 3:
                            yield f"   ... 还有 {len(matching_stocks) - 3} 个库存\n"
                    else:
                        yield "⚠️ [子步骤 1/2] 未找到匹配的库存\n"

                    total_available = sum(float(s.get('stock_qty', 0) or 0) for s in matching_stocks)
                    yield f"   总可用库存: {total_available}\n"

                    # 调用LLM进行调配分析
                    yield "\n🤖 开始AI智能分析...\n"
                    yield "────────────────────────────────────────\n"
                    
                    prompt = self._build_single_plan_prompt(plan, matching_stocks, strategy, warehouse_code)
                    system_prompt = "你是一个专业的电力物资调配专家，擅长分析库存分布并给出最优的调配方案。请用清晰的中文进行分析。"

                    # 使用上下文管理器处理超长prompt
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

                    # 判断匹配状态
                    if total_available >= demand_qty:
                        status = 'full'
                        full_match_count += 1
                    elif total_available > 0:
                        status = 'partial'
                        partial_match_count += 1
                    else:
                        status = 'none'
                        none_match_count += 1

                    yield f"\n✅ [处理完成] 匹配状态: {'完全匹配' if status == 'full' else '部分匹配' if status == 'partial' else '无匹配'}\n"

                except Exception as e:
                    yield f"\n❌ [处理失败] {str(e)}\n"
                    none_match_count += 1

                yield "\n────────────────────────────────────────\n\n"

            # 输出最终汇总
            yield "\n📊 调配分析汇总报告\n"
            yield "────────────────────────────────────────\n"
            yield f"   总计划数: {total_count}\n"
            yield f"   完全匹配: {full_match_count}\n"
            yield f"   部分匹配: {partial_match_count}\n"
            yield f"   无匹配: {none_match_count}\n"
            
            # 生成建议
            suggestion = ""
            if full_match_count == total_count:
                suggestion = f"{total_count}项完全匹配可直接审核"
            elif full_match_count + partial_match_count > 0:
                suggestion = f"{full_match_count}项完全匹配可直接审核，{partial_match_count}项部分匹配建议跨仓调拨或协议补库"
            else:
                suggestion = "所有物料无库存，建议触发协议补库流程"
            
            if none_match_count > 0 and full_match_count + partial_match_count > 0:
                suggestion += f"，{none_match_count}项建议走应急采购"
            
            yield f"   建议: {suggestion}\n"
            yield "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

        except Exception as e:
            yield f"❌ 整体分析失败: {str(e)}\n"
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
            return chunk.strip()
        return None

    def _build_single_plan_prompt(self, plan: Dict[str, Any], stocks: List[Dict[str, Any]], strategy: str, target_warehouse: str) -> str:
        """为单个计划构建调配分析prompt"""
        plan_id = plan.get('planId') or plan.get('plan_id', '')
        material_code = plan.get('materialCode') or plan.get('material_code', '')
        material_desc = plan.get('materialDesc', '')
        demand_qty = float(plan.get('demandQty') or plan.get('demand_qty', 0))
        tech_spec_id = plan.get('techSpecId') or ''
        plan_warehouse_code = plan.get('warehouseCode', '')
        
        stocks_formatted = []
        for s in stocks:
            stocks_formatted.append({
                '仓库编码': s.get('loc_code', ''),
                '仓库名称': s.get('loc_name', ''),
                '库存数量': float(s.get('stock_qty', 0) or 0),
                '库存类型': s.get('source_type', ''),
                '距离': s.get('distance', '')
            })

        prompt = f"""你是一个专业的电力物资调配专家。请根据以下计划信息和库存数据，进行智能调配分析。

## 任务说明
分析单个需求计划的库存匹配情况，给出最优调配方案和建议。

## 当前计划信息
- 计划ID: {plan_id}
- 物料编码: {material_code}
- 物料描述: {material_desc}
- 技术规范ID: {tech_spec_id}
- 需求数量: {demand_qty}
- 目标仓库: {plan_warehouse_code}
- 调配策略: {strategy}

## 可用库存数据
{json.dumps(stocks_formatted, ensure_ascii=False, indent=2)}

## 分析要求

**输出格式要求：**
- 使用Markdown格式输出
- 使用##、### 标题
- 列表使用 - 开头
- 表格使用 | 分隔

**输出结构：**

## 调配分析结果

### 一、需求概况
- 计划ID、物料编码、需求数量等基本信息

### 二、库存匹配分析
- 可用库存总量
- 各仓库库存分布
- 是否满足需求

### 三、最优调配方案
| 来源仓库 | 仓库名称 | 调拨数量 | 库存类型 |
|---------|---------|---------|---------|
| 示例 | 示例仓库 | 100 | 自有库存 |

### 四、分析结论与建议
- 当前库存是否满足需求
- 匹配状态（完全匹配/部分匹配/无匹配）
- 后续处理建议

请用简洁、清晰的语言进行分析。
"""
        return prompt
