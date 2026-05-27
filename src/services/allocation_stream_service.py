# -*- coding: utf-8 -*-
"""调配服务 - 流式版本（复用原服务逻辑）"""
import asyncio
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

from ..utils.context_manager import ContextManager
from ..utils.session_manager import session_manager
from ..utils.json_repair import JSONRepair, SmartJSONParser


class AllocationStreamService:
    """调配服务 - 流式版本"""

    def __init__(self, db, llm_stream_func, llm_func=None):
        self.db = db
        self.llm_stream_func = llm_stream_func
        self.llm_func = llm_func
        if llm_func:
            self.context_manager = ContextManager(llm_func, llm_stream_func)
            # 初始化JSON解析器，集成双层保障机制
            self.json_parser = SmartJSONParser(self.context_manager)
        else:
            self.context_manager = None
            self.json_parser = SmartJSONParser(None)

    async def stream_analyze(self, strategy: str = "time", warehouse_code: str = "",
                            source_type: str = "", project_unit: str = "",
                            demand_start_date: str = "", demand_end_date: str = "",
                            plan_type: str = "", material_codes: List[str] = None,
                            session_id: str = None):
        """流式分析调配方案

        Args:
            strategy: 调配策略
            warehouse_code: 仓库编码
            source_type: 来源类型
            project_unit: 项目单位
            demand_start_date: 需求开始日期
            demand_end_date: 需求结束日期
            plan_type: 计划类型
            material_codes: 物料编码列表
            session_id: 会话ID，用于支持终止功能
        """
        # 立即输出第一个消息，让用户知道服务正在处理
        yield "🚀 【智能调配系统】正在启动高级数据分析引擎...\n\n"
        yield "📋 【任务概述】\n"
        yield "   本系统将运用智能调配算法，基于需求计划和库存数据，\n"
        yield "   为每个需求计划精准匹配最优仓库，实现物资流转效率最大化。\n"
        yield "   核心目标：降低运输成本、缩短交付周期、优化库存分布。\n\n"
        
        logger.info(f"开始流式调配分析, strategy={strategy}, warehouse_code={warehouse_code}, "
                    f"source_type={source_type}, project_unit={project_unit}, "
                    f"demand_start_date={demand_start_date}, demand_end_date={demand_end_date}, "
                    f"plan_type={plan_type}, material_codes={material_codes}")
        
        try:
            yield "🔍 【阶段一：需求计划数据采集】\n"
            yield "   📌 当前需求：获取符合筛选条件的物料需求计划\n"
            yield "   📌 执行动作：从数据库查询需求计划表\n"
            yield "   📌 数据用途：需求计划是调配决策的核心输入，包含物料编码、需求数量、目标仓库等关键信息\n"
            yield "   📌 筛选条件：项目单位、日期范围、计划类型、目标仓库、物料编码\n"
            yield "   └─ 正在执行SQL查询，检索需求计划数据...\n"
            logger.info(f"正在查询需求计划, project_unit={project_unit}, plan_type={plan_type}, "
                        f"warehouse_code={warehouse_code}, material_codes={material_codes}")
            try:
                plans = await asyncio.wait_for(
                    self._query_plans(
                        project_unit=project_unit,
                        start_date=demand_start_date,
                        end_date=demand_end_date,
                        plan_type=plan_type,
                        warehouse_code=warehouse_code,
                        material_codes=material_codes
                    ),
                    timeout=30
                )
            except asyncio.TimeoutError:
                yield "❌ 需求计划查询超时：数据库响应超过30秒\n"
                yield "   💡 建议：请检查数据库连接状态或减少查询范围\n"
                return
            logger.info(f"需求计划查询完成, 共获取 {len(plans)} 条")

            if not plans:
                yield "❌ 数据采集失败：未查询到符合条件的需求计划\n"
                yield "   💡 建议：请检查筛选条件是否过于严格，或确认数据库中是否有符合条件的数据\n"
                return

            yield f"✅ 需求计划数据采集成功\n"
            yield f"   └─ 共获取 {len(plans)} 条需求计划\n"
            yield f"   └─ 数据完整性：已验证所有必需字段（计划ID、物料编码、需求数量、目标仓库）\n"
            yield f"   └─ 下一步：提取物料编码和技术规范ID，用于关联库存数据\n\n"

            yield "🔍 【阶段二：关键特征智能提取】\n"
            yield "   📌 当前需求：从需求计划中提取物料编码和技术规范ID\n"
            yield "   📌 执行动作：运用数据清洗和去重算法，提取唯一标识符\n"
            yield "   📌 数据用途：\n"
            yield "      • 物料编码：用于匹配库存数据，查找可用库存\n"
            yield "      • 技术规范ID：用于精确匹配，确保物料规格一致性\n"
            yield "   📌 技术要点：去除重复项、验证编码格式、建立索引映射\n"
            yield "   └─ 正在执行特征提取算法...\n"
            material_codes_list = self._extract_material_codes(plans)
            tech_ids_list = self._extract_tech_ids(plans)
            yield f"✅ 特征提取完成\n"
            yield f"   └─ 物料编码：{len(material_codes_list)} 个（去重后）\n"
            yield f"   └─ 技术规范ID：{len(tech_ids_list)} 个\n"
            yield f"   └─ 数据质量：已验证编码格式有效性\n"
            yield f"   └─ 下一步：基于物料编码检索库存数据\n\n"

            yield "🔍 【阶段三：库存数据智能检索】\n"
            yield "   📌 当前需求：获取所有相关仓库的库存数据\n"
            yield "   📌 执行动作：构建物料-仓库关联矩阵，执行多维度查询\n"
            yield "   📌 数据用途：\n"
            yield "      • 库存数量：判断是否满足需求，计算匹配度\n"
            yield "      • 仓库位置：计算运输距离，优化调配路径\n"
            yield "      • 库存类型：区分自有库存、协议库存、在途库存\n"
            yield "   📌 查询维度：物料编码、技术规范ID、来源类型、目标仓库\n"
            yield "   └─ 正在执行库存数据检索...\n"
            logger.info(f"正在查询库存数据, material_codes_count={len(material_codes_list)}, "
                        f"source_type={source_type}, target_warehouse={warehouse_code}")
            stocks = self._query_stocks(
                material_codes=material_codes_list,
                source_type=source_type,
                target_warehouse=warehouse_code,
                tech_ids=tech_ids_list if tech_ids_list else None
            )

            if not stocks:
                yield "❌ 库存数据检索失败：未查询到符合条件的库存数据\n"
                yield "   💡 建议：请检查物料编码是否正确，或确认库存表中是否有相关数据\n"
                return

            yield f"✅ 库存数据检索成功\n"
            yield f"   └─ 共获取 {len(stocks)} 条库存记录\n"
            yield f"   └─ 覆盖仓库：{len(set(s.get('loc_code', '') for s in stocks))} 个\n"
            yield f"   └─ 数据完整性：已验证库存数量、仓库位置、库存类型等字段\n"
            yield f"   └─ 下一步：构建物料与仓库的关联映射关系\n\n"
            logger.info(f"库存数据检索完成, 共 {len(stocks)} 条记录, "
                        f"覆盖 {len(set(s.get('loc_code', '') for s in stocks))} 个仓库")

            yield "🔍 【阶段四：数据预处理与关联构建】\n"
            yield "   📌 当前需求：建立物料与仓库的多对多关联关系\n"
            yield "   📌 执行动作：构建物料来源类型映射表，优化数据结构\n"
            yield "   📌 数据用途：\n"
            yield "      • 快速查找：为每个物料快速定位可用仓库\n"
            yield "      • 数据聚合：计算各物料的总可用库存\n"
            yield "      • 决策支持：为AI分析提供结构化数据输入\n"
            yield "   📌 技术要点：使用哈希表加速查询、建立倒排索引、数据归一化\n"
            yield "   └─ 正在执行数据预处理...\n"
            material_source_types = self._build_material_source_type_map(stocks)
            yield f"✅ 数据预处理完成\n"
            yield f"   └─ 已构建 {len(material_source_types)} 个物料的来源映射\n"
            yield f"   └─ 映射关系：平均每个物料关联 {len(stocks)/len(material_source_types):.1f} 个仓库\n"
            yield f"   └─ 数据就绪：已准备好进入AI智能分析阶段\n\n"
            logger.info(f"数据预处理完成, {len(material_source_types)} 个物料来源映射")

            # 检查会话是否已取消
            if session_id and session_manager.is_session_cancelled(session_id):
                yield "❌ 【会话已终止】用户已取消当前分析任务\n"
                return

            yield "⚡ 【阶段五：AI智能批量分析】\n"
            yield "   📌 当前需求：对所有需求计划进行一次性深度智能分析\n"
            yield "   📌 执行动作：启动大规模并行分析引擎，运用深度学习模型\n"
            yield "   📌 分析目标：\n"
            yield "      • 智能匹配：为每个计划选择最优仓库\n"
            yield "      • 多维优化：综合考虑距离、库存、成本等因素\n"
            yield "      • 决策推理：生成可解释的调配建议\n"
            yield "   📌 技术架构：多目标优化算法 + 规则引擎\n"
            yield "   └─ 正在启动AI分析引擎...\n"
            yield "   └─ 预计分析时间：取决于数据规模和复杂度\n\n"
            logger.info(f"启动批量调配分析, plans={len(plans)}, stocks={len(stocks)}, strategy={strategy}")
            async for chunk in self._batch_analyze(plans, stocks, strategy, warehouse_code,
                                                 project_unit, source_type, plan_type, session_id):
                yield chunk

        except Exception as e:
            yield f"❌ 系统异常：分析过程中发生错误\n"
            yield f"   └─ 错误类型：{type(e).__name__}\n"
            yield f"   └─ 错误信息：{str(e)}\n"
            import traceback
            yield f"   └─ 详细堆栈：{traceback.format_exc()}\n"

    async def _batch_analyze(self, plans: List[Dict[str, Any]], stocks: List[Dict[str, Any]],
                             strategy: str, target_warehouse: str,
                             project_unit: str = "", source_type: str = "", plan_type: str = "",
                             session_id: str = None):
        """批量分析模式 - 一次性分析所有计划

        Args:
            plans: 需求计划列表
            stocks: 库存列表
            strategy: 调配策略
            target_warehouse: 目标仓库
            project_unit: 项目单位
            source_type: 来源类型
            plan_type: 计划类型
            session_id: 会话ID，用于支持终止功能
        """
        logger.info(f"_batch_analyze 开始, plans={len(plans)}, stocks={len(stocks)}, strategy={strategy}")
        try:
            all_plan_data = []

            yield "🔍 【数据预处理阶段】正在智能收集所有计划的库存匹配数据...\n"
            yield "   📌 当前需求：为每个需求计划构建完整的库存匹配数据集\n"
            yield "   📌 执行动作：遍历每个计划，关联库存数据，计算可用库存总量\n"
            yield "   📌 数据用途：\n"
            yield "      • 匹配分析：为AI分析提供结构化的计划-库存关联数据\n"
            yield "      • 决策支持：支持智能调配决策的量化分析\n"
            yield "      • 结果输出：为最终JSON输出提供数据基础\n"
            yield "   📌 处理逻辑：\n"
            yield "      • 物料匹配：根据物料编码匹配库存记录\n"
            yield "      • 数量计算：汇总所有仓库的可用库存\n"
            yield "      • 数据验证：检查数据完整性和一致性\n"
            yield "   └─ 正在执行数据预处理...\n"

            for idx, plan in enumerate(plans, 1):
                # 每处理一个计划前检查会话是否已取消
                if session_id and session_manager.is_session_cancelled(session_id):
                    yield "❌ 【会话已终止】用户已取消当前分析任务\n"
                    return

                plan_id = plan.get('planId') or plan.get('plan_id', f'plan_{idx}')
                material_code = plan.get('materialCode') or plan.get('material_code', '')
                tech_spec_id = plan.get('techSpecId') or ''
                demand_qty = float(plan.get('demandQty') or plan.get('demand_qty', 0))
                target_warehouse = plan.get('warehouseCode', '')
                material_desc = plan.get('materialDesc', '')

                matching_stocks = [s for s in stocks if s.get('material_code') == material_code]
                total_available = sum(float(s.get('stock_qty', 0) or 0) for s in matching_stocks)

                plan_data = {
                    'plan_id': plan_id,
                    'material_code': material_code,
                    'tech_spec_id': tech_spec_id,
                    'demand_qty': demand_qty,
                    'target_warehouse': target_warehouse,
                    'material_desc': material_desc,
                    'matching_stocks': matching_stocks,
                    'total_available': total_available
                }
                all_plan_data.append(plan_data)

                match_status = "✅ 完全匹配" if total_available >= demand_qty else "⚠️ 部分匹配" if total_available > 0 else "❌ 无匹配"
                yield f"   [{idx}/{len(plans)}] 计划 {plan_id} - {match_status}（可用: {total_available}, 需求: {demand_qty}）\n"

            yield f"\n✅ 数据预处理完成\n"
            yield f"   └─ 共收集到 {len(all_plan_data)} 个计划的完整数据\n"
            yield f"   └─ 数据质量：已验证所有计划的必需字段\n"
            yield f"   └─ 匹配统计：完全匹配 {sum(1 for p in all_plan_data if p['total_available'] >= p['demand_qty'])} 个，部分匹配 {sum(1 for p in all_plan_data if 0 < p['total_available'] < p['demand_qty'])} 个，无匹配 {sum(1 for p in all_plan_data if p['total_available'] == 0)} 个\n\n"

            yield "🤖 【AI智能分析阶段】正在启动高级调配分析引擎...\n"
            yield "   📌 当前需求：运用AI算法对所有计划进行深度智能分析\n"
            yield "   📌 执行动作：构建分析prompt，调用大语言模型进行智能推理\n"
            yield "   📌 分析目标：\n"
            yield "      • 智能匹配：为每个计划选择最优调配仓库\n"
            yield "      • 多维优化：综合考虑距离、库存、成本等因素\n"
            yield "      • 决策推理：生成可解释的调配建议和理由\n"
            yield "      • 结果输出：输出Markdown格式报告和JSON格式数据\n"
            yield "   📌 技术架构：\n"
            yield "      • 规则引擎：业务规则约束\n"
            yield "      • 推理能力：链式思维推理\n"
            yield "   └─ 正在调用AI分析引擎...\n"
            yield "────────────────────────────────────────\n"

            prompt = self._build_batch_prompt(all_plan_data, stocks, strategy, target_warehouse)
            system_prompt = "你是一位资深的电力物料智能调配专家，具备卓越的数据分析能力和丰富的实战经验。请运用高级智能算法进行深度分析。"
            logger.info(f"AI分析prompt构建完成, prompt长度={len(prompt)}")

            # 调用LLM前检查会话是否已取消
            if session_id and session_manager.is_session_cancelled(session_id):
                yield "\n❌ 【会话已终止】用户已取消当前分析任务\n"
                return

            # 获取会话的取消事件
            cancel_event = session_manager.get_cancel_event(session_id) if session_id else None

            if self.context_manager and self.context_manager.is_too_long(prompt):
                yield "⚠️ 检测到数据量较大，将采用代码沙盒模式...\n"
                logger.info("prompt过长, 启用分层推理模式")
                async for chunk in self.context_manager._streaming_sandbox_execution(prompt, system_prompt, 'allocation', {'plans': all_plan_data, 'stocks': stocks, 'strategy': strategy}):
                    # 检查会话是否已取消
                    if session_id and session_manager.is_session_cancelled(session_id):
                        yield "\n❌ 【会话已终止】用户已取消当前分析任务\n"
                        return
                    content = self._parse_llm_chunk(chunk)
                    if content:
                        yield content
            else:
                # 调用LLM流式API，传入cancel_event实现快速中断
                async for chunk in self.llm_stream_func(prompt, system_prompt, cancel_event=cancel_event):
                    content = self._parse_llm_chunk(chunk)
                    if content:
                        yield content

            logger.info("AI批量调配分析完成, 开始结果解析")

            yield "\n\n📊 【数据处理阶段】AI分析完成，正在进行结果解析...\n"
            yield "   📌 当前需求：从AI分析结果中提取结构化数据\n"
            yield "   📌 执行动作：\n"
            yield "      • JSON提取：提取JSON格式的结构化数据\n"
            yield "      • 数据验证：验证JSON数据格式和完整性\n"
            yield "   📌 数据用途：\n"
            yield "      • 数据存储：JSON数据用于数据库存储\n"
            yield "      • 后续处理：支持数据导出和二次分析\n"
            yield "   └─ 正在执行结果解析...\n\n"

            full_match_count = sum(1 for p in all_plan_data if p['total_available'] >= p['demand_qty'])
            partial_match_count = sum(1 for p in all_plan_data if 0 < p['total_available'] < p['demand_qty'])
            none_match_count = sum(1 for p in all_plan_data if p['total_available'] == 0)

            yield "📊 智能调配分析汇总报告\n"
            yield "────────────────────────────────────────\n"
            yield f"   🎯 总计划数：{len(all_plan_data)}\n"
            yield f"   ✅ 完全匹配：{full_match_count} 个（库存充足，可直接调配）\n"
            yield f"   ⚠️ 部分匹配：{partial_match_count} 个（需跨仓调拨或协议补库）\n"
            yield f"   ❌ 无匹配：{none_match_count} 个（需应急采购）\n"

            suggestion = ""
            if full_match_count == len(all_plan_data):
                suggestion = f"{len(all_plan_data)}项完全匹配，可直接进入审核流程"
            elif full_match_count + partial_match_count > 0:
                suggestion = f"{full_match_count}项完全匹配可直接审核，{partial_match_count}项部分匹配建议跨仓调拨或协议补库"
            else:
                suggestion = "所有物料无库存，建议触发协议补库流程"

            if none_match_count > 0 and full_match_count + partial_match_count > 0:
                suggestion += f"，{none_match_count}项建议走应急采购通道"

            yield f"   💡 智能建议：{suggestion}\n"
            yield "────────────────────────────────────────\n"

            yield "\n💾 【数据持久化阶段】正在将分析结果存储到数据库...\n"
            yield "   📌 当前需求：将 JSON 格式结果数据持久化到数据库\n"
            yield "   📌 执行动作：\n"
            yield "      • JSON 解析：解析 AI 返回的 JSON 格式数据\n"
            yield "      • 数据验证：验证数据完整性和格式正确性\n"
            yield "      • 数据转换：将 JSON 数据转换为数据库记录\n"
            yield "      • 数据插入：执行数据库插入操作\n"
            yield "      • 日志记录：记录分析日志和审计信息\n"
            yield "   📌 数据用途：\n"
            yield "      • 历史查询：支持历史分析结果查询\n"
            yield "      • 数据统计：支持统计分析报表\n"
            yield "      • 审计追踪：支持操作审计和追溯\n"
            yield "   └─ 正在执行数据持久化操作...\n"
            
            # 实际数据库存储：批量插入（减少数据库交互次数）
            saved_count = 0
            failed_count = 0
            parse_errors = []
            
            # 检查是否需要触发沙盒兜底（当 AI 返回结果为空时）
            needs_sandbox_fallback = False
            for plan_data in all_plan_data:
                if not plan_data.get('result'):
                    needs_sandbox_fallback = True
                    break
            
            if needs_sandbox_fallback and self.context_manager:
                yield "⚠️ 检测到 AI 返回结果为空，触发代码沙盒兜底计算...\n"
                logger.info("AI 返回结果为空，触发沙盒兜底")
                
                # 重新构建 prompt 并执行沙盒计算
                prompt = self._build_batch_prompt(all_plan_data, stocks, strategy, target_warehouse)
                system_prompt = "你是一位资深的电力物料智能调配专家，具备卓越的数据分析能力和丰富的实战经验。请运用高级智能算法进行深度分析。"
                
                # 执行沙盒计算获取结构化数据
                sandbox_result = await self.context_manager._sandbox_execution(
                    prompt, system_prompt, 'allocation', {'plans': all_plan_data, 'stocks': stocks, 'strategy': strategy}
                )
                
                # 将沙盒计算结果应用到 plan_data
                structured_data = sandbox_result.get('structured_data', [])
                if structured_data and len(structured_data) == len(all_plan_data):
                    for i, plan_data in enumerate(all_plan_data):
                        if i < len(structured_data):
                            plan_data['result'] = structured_data[i]
                    logger.info(f"沙盒兜底成功，应用 {len(structured_data)} 条结构化数据")
                    yield "✅ 代码沙盒兜底计算完成，数据已修复\n"
                else:
                    logger.warning(f"沙盒兜底数据不匹配：期望{len(all_plan_data)}条，实际{len(structured_data) if structured_data else 0}条")
                    yield "⚠️ 代码沙盒兜底数据不匹配，将使用基础数据计算\n"
            
            # 收集所有要插入的数据
            batch_data = []
            for idx, plan_data in enumerate(all_plan_data):
                try:
                    # 从 AI 返回的结果中获取字段（优先级：AI 结果 > 原始计划数据）
                    result_data = plan_data.get('result', {})
                    
                    # 匹配数量计算
                    matched_qty = result_data.get('matchedQty', 0) or min(plan_data.get('total_available', 0), plan_data.get('demand_qty', 0))
                    total_available = result_data.get('availableStock', 0) or plan_data.get('total_available', 0)
                    demand_qty = plan_data.get('demand_qty', 0)
                    
                    # 状态判断（优先使用AI返回的状态）
                    match_status = result_data.get('status', '')
                    match_status_name = result_data.get('statusName', '')
                    score = result_data.get('score', 0)
                    
                    # 如果AI没有返回状态，则根据库存情况计算
                    if not match_status:
                        if total_available >= demand_qty:
                            match_status = 'full'
                            match_status_name = '完全匹配'
                            score = 100
                        elif total_available > 0:
                            match_status = 'partial'
                            match_status_name = '部分匹配'
                            score = 50
                        else:
                            match_status = 'none'
                            match_status_name = '无匹配'
                            score = 0
                    
                    matching_stocks = plan_data.get('matching_stocks', [])
                    first_stock = matching_stocks[0] if matching_stocks else {}
                    
                    # 字段获取逻辑（与非流式保持一致）
                    plan_id = plan_data.get('planId') or plan_data.get('plan_id', '')
                    plan_code = plan_data.get('planCode') or plan_data.get('plan_code', '') or plan_data.get('fd_code_use', '')
                    material_code = plan_data.get('materialCode') or plan_data.get('material_code', '')
                    material_desc = result_data.get('materialDesc', '') or plan_data.get('materialDesc', '') or plan_data.get('material_desc', '')
                    tech_spec_id = plan_data.get('techSpecId') or plan_data.get('tech_spec_id', '') or plan_data.get('fd_tech_spec_id', '') or result_data.get('techSpecId', '')
                    demand_qty_val = plan_data.get('demandQty') or plan_data.get('demand_qty', 0)
                    unit = result_data.get('unit', '') or plan_data.get('unit', '')
                    unit_code = result_data.get('unitCode', '') or plan_data.get('unitCode', '') or plan_data.get('unit_code', '')
                    project_unit_val = result_data.get('unitName', '') or plan_data.get('unitName', '') or plan_data.get('unit_name', '') or project_unit
                    project_name = result_data.get('projectName', '') or plan_data.get('projectName', '') or plan_data.get('project_name', '')
                    project_code = plan_data.get('projectCode', '') or plan_data.get('project_code', '')
                    warehouse_code = result_data.get('warehouseCode', '') or result_data.get('sourceWarehouse', '') or first_stock.get('loc_code', '') or plan_data.get('warehouseCode', '') or plan_data.get('warehouse_code', '')
                    warehouse_name = result_data.get('warehouseName', '') or first_stock.get('loc_name', '')
                    source_type_val = result_data.get('sourceType', '') or first_stock.get('source_type', '') or source_type
                    reason = result_data.get('reason', '')
                    demand_date = plan_data.get('demandDate', '') or plan_data.get('demand_date', '')
                    plan_type_val = plan_data.get('planType', '') or plan_data.get('plan_type', '') or plan_type
                    unit_factory_code = result_data.get('unitFactoryCode', '') or plan_data.get('unitFactoryCode', '') or plan_data.get('unit_factory_code', '')
                    allocation_type = result_data.get('allocationType', '跨仓调拨')  # 默认跨仓调拨，但可由AI覆盖
                    unit_price = result_data.get('unitPrice', 0) or plan_data.get('unitPrice', 0) or plan_data.get('unit_price', 0) or 0
                    amount = result_data.get('amount', 0) or (matched_qty * unit_price)
                    
                    batch_data.append((
                        plan_id, plan_code, material_code, material_desc,
                        tech_spec_id, demand_qty_val, unit, unit_code, project_unit_val,
                        project_name, project_code, warehouse_code, warehouse_name,
                        matched_qty, total_available, score, match_status,
                        match_status_name, source_type_val, reason, demand_date,
                        plan_type_val, strategy,
                        project_unit_val, demand_date,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        unit_factory_code, allocation_type, amount, unit_price,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ))
                except Exception as e:
                    failed_count += 1
                    error_info = f"第{idx+1}条数据解析失败: plan_id={plan_data.get('plan_id', '未知')}, 错误: {str(e)[:100]}"
                    parse_errors.append(error_info)
                    logger.warning(error_info)
            
            if parse_errors:
                logger.warning(f"解析警告：共{len(all_plan_data)}条数据，{len(parse_errors)}条解析失败，{len(batch_data)}条成功")
                for err in parse_errors[:5]:
                    logger.warning(f"  • {err}")
                if len(parse_errors) > 5:
                    logger.warning(f"  • ...还有{len(parse_errors)-5}条错误")
            
            if batch_data:
                try:
                    conn = self.db._get_connection()
                    cur = conn.cursor()
                    
                    cur.executemany('''
                        REPLACE INTO mt_allocation_result (
                            fd_plan_id, fd_plan_code, fd_material_code, fd_material_desc,
                            fd_tech_spec_id, fd_demand_qty, fd_unit, fd_unit_code, fd_unit_name,
                            fd_project_name, fd_project_code, fd_warehouse_code, fd_warehouse_name,
                            fd_matched_qty, fd_available_stock, fd_score, fd_match_status,
                            fd_match_status_name, fd_source_type, fd_reason, fd_demand_date,
                            fd_plan_type, fd_strategy,
                            fd_project_unit, fd_demand_time,
                            fd_create_time, fd_unit_factory_code, fd_allocation_type, fd_amount,
                            fd_unit_price, fd_compare_date
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', batch_data)
                    
                    conn.commit()
                    conn.close()
                    saved_count = len(batch_data)
                    logger.info(f"批量插入成功: {saved_count} 条记录")
                except Exception as e:
                    failed_count += len(batch_data)
                    logger.error(f"批量插入失败: {str(e)}")
            
            logger.info(f"数据库存储日志: 解析{len(all_plan_data)}条, 成功保存{saved_count}条, 失败{failed_count}条")
            
            yield f"✅ 数据存储完成，成功保存 {saved_count} 条记录，分析流程全部结束\n"

        except Exception as e:
            yield f"❌ 批量分析过程中发生异常\n"
            yield f"   └─ 错误类型：{type(e).__name__}\n"
            yield f"   └─ 错误信息：{str(e)}\n"
            import traceback
            yield f"   └─ 详细堆栈：{traceback.format_exc()}\n"

    def _parse_llm_chunk(self, chunk: str) -> Optional[str]:
        """解析LLM返回的JSON格式chunk，提取内容和思考过程
        
        采用双层保障机制：
        第一层保障：使用JSONRepair修复损坏的JSON
        第二层保障：如果修复失败，返回原始字符串（保持原有行为）
        """
        if not chunk:
            return None
            
        # 第一层保障：尝试修复并解析JSON
        try:
            # 先尝试直接解析
            data = json.loads(chunk)
        except json.JSONDecodeError:
            # 直接解析失败，使用JSON修复工具
            logger.warning(f"JSON解析失败，尝试修复: {chunk[:100]}...")
            repaired_data = JSONRepair.repair_json(chunk)
            if repaired_data is not None:
                logger.info("JSON修复成功")
                data = repaired_data
            else:
                # 修复也失败，返回原始字符串（第二层保障）
                logger.warning("JSON修复失败，返回原始字符串")
                return chunk
        
        # 解析成功，提取内容
        try:
            if isinstance(data, list):
                return chunk
            choices = data.get('choices', [])
            if choices:
                delta = choices[0].get('delta', {})
                reasoning = delta.get('reasoning_content', '') or delta.get('reasoning', '') or delta.get('thinking', '')
                content = delta.get('content', '')

                if reasoning:
                    return f"{reasoning}"
                elif content:
                    return content
            return None
        except Exception as e:
            logger.error(f"解析JSON内容失败: {str(e)}")
            return chunk

    def _build_batch_prompt(self, all_plan_data: List[Dict[str, Any]], stocks: List[Dict[str, Any]],
                            strategy: str, target_warehouse: str) -> str:
        """为批量分析构建prompt"""
        strategy_descriptions = {
            'time': '时效优先策略：优先选择距离最近的仓库进行调配，以最快速度满足需求。',
            'cost': '成本优先策略：优先选择距离最近的仓库，以降低运输成本。',
            'stock': '库存优先策略：优先选择库存充足的仓库，确保能够满足需求。',
            'emerg': '紧急调配策略：综合考虑距离和库存，以最快速度响应紧急需求。'
        }
        strategy_desc = strategy_descriptions.get(strategy, strategy_descriptions['time'])

        plans_formatted = []
        for p in all_plan_data:
            stocks_formatted = []
            for s in p['matching_stocks']:
                stocks_formatted.append({
                    '仓库编码': s.get('loc_code', ''),
                    '仓库名称': s.get('loc_name', ''),
                    '库存数量': float(s.get('stock_qty', 0) or 0),
                    '库存类型': s.get('source_type', ''),
                    '距离': s.get('distance', '')
                })

            plans_formatted.append({
                '计划ID': p['plan_id'],
                '物料编码': p['material_code'],
                '物料描述': p['material_desc'],
                '技术规范ID': p['tech_spec_id'],
                '需求数量': p['demand_qty'],
                '目标仓库': p['target_warehouse'],
                '可用库存总量': p['total_available'],
                '匹配库存': stocks_formatted
            })

        stocks_formatted = []
        for s in stocks:
            stocks_formatted.append({
                '仓库编码': s.get('loc_code', ''),
                '仓库名称': s.get('loc_name', ''),
                '物料编码': s.get('material_code', ''),
                '库存数量': float(s.get('stock_qty', 0) or 0),
                '库存类型': s.get('source_type', ''),
                '距离': s.get('distance', '')
            })

        prompt = f"""你是一个专业的电力物资调配专家。我将提供多个需求计划和可用库存数据，请你一次性分析所有计划并给出最优的调配方案。

## 任务说明
请一次性分析以下所有需求计划（共 {len(plans_formatted)} 个计划），从可用库存中为每个计划选择最合适的仓库进行调配，并详细说明你的分析过程和理由。

**重要**：你必须为**每一个需求计划**单独输出一份完整的分析结果，按照下面规定的格式，一个计划一个计划地列出结果。

## 输入数据

### 调配策略
{strategy_desc}

### 需求计划列表（共 {len(plans_formatted)} 个计划）
{json.dumps(plans_formatted, ensure_ascii=False, indent=2)}

### 可用库存数据
{json.dumps(stocks_formatted, ensure_ascii=False, indent=2)}

## 输出格式要求

**重要**：你必须按照以下格式，为每一个需求计划单独输出一份完整的分析结果。

请使用Markdown格式输出，使用##、###标题，表格使用|分隔。

**输出结构必须包含以下内容，并严格按照顺序输出**：

---

## 【计划 1/{len(plans_formatted)}】调配分析

### 一、需求概况
- 计划ID: [从输入数据中获取]
- 物料编码: [从输入数据中获取]
- 物料描述: [从输入数据中获取]
- 需求数量: [从输入数据中获取]
- 目标仓库: [从输入数据中获取]

### 二、可用库存分析
- 可用库存总量: [从输入数据中获取]
- 各仓库库存分布

### 三、最优调配方案
| 来源仓库 | 仓库名称 | 调拨数量 | 库存类型 |
|---------|---------|---------|---------|
| [根据数据分析] | [仓库名称] | [数量] | [类型] |

### 四、分析结论与建议
- 当前库存是否满足需求
- 匹配状态（完全匹配/部分匹配/无匹配）
- 后续处理建议

---

**然后继续输出计划2，计划3...直到所有{len(plans_formatted)}个计划都分析完毕**

## 最终汇总

请在分析完所有计划后，给出本次调配的总体汇总：
- 完全匹配数量
- 部分匹配数量
- 无匹配数量
- 整体建议

请用简洁、清晰的语言进行分析。
"""
        return prompt

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

    # ==================== 数据查询方法（从 AllocationService 迁移） ====================

    async def _query_plans(self, project_unit: str, start_date: str, end_date: str,
                           plan_type: str, warehouse_code: str,
                           material_codes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """从库存使用计划表查询计划数据"""
        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            query = "SELECT * FROM mt_stock_use_list_plan_two WHERE 1=1"
            params = []

            if project_unit:
                query += " AND (fd_unit_name = %s OR fd_unit_factory_code = %s)"
                params.extend([project_unit, project_unit])

            if start_date:
                query += " AND fd_requisition_date >= %s"
                params.append(start_date)

            if end_date:
                query += " AND fd_requisition_date <= %s"
                params.append(end_date)

            if plan_type:
                query += " AND apply_way = %s"
                params.append(plan_type)

            query += " AND apply_way IN ('01', '05', '06')"

            if warehouse_code:
                query += " AND fd_warehouse_code = %s"
                params.append(warehouse_code)

            if material_codes and len(material_codes) > 0:
                placeholders = ','.join(['%s' for _ in material_codes])
                query += f" AND fd_material_code IN ({placeholders})"
                params.extend(material_codes)

            cur.execute(query, params)
            rows = cur.fetchall()

            plans = []
            for row in rows:
                plans.append({
                    'planId': str(row['fd_plan_id'] or row['id']),
                    'planCode': row['fd_code_use'],
                    'materialCode': row['fd_material_code'],
                    'materialDesc': row['fd_desc'],
                    'techSpecId': row['fd_tech_spec_id'],
                    'demandQty': float(row['fd_requisition_num'] or 0),
                    'warehouseCode': row['fd_warehouse_code'],
                    'unit': row['fd_unit'],
                    'unitCode': row['fd_unit_code'],
                    'projectName': row['fd_project_name'],
                    'projectCode': row['fd_project_code'],
                    'unitName': row['fd_unit_name'],
                    'unitFactoryCode': row['fd_unit_factory_code'],
                    'unitPrice': float(row['fd_unit_price'] or 0),
                    'demandDate': row['fd_requisition_date'],
                    'planType': row['apply_way']
                })

            return plans
        except Exception as e:
            logger.error(f"[AllocationStream] 查询需求计划失败: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def _query_stocks(self, material_codes: List[str], source_type: str, target_warehouse: str = '', tech_ids: List[str] = None) -> List[Dict[str, Any]]:
        """从库存信息表查询库存数据"""
        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            where_clauses = ["1=1"]
            params = []

            if material_codes and len(material_codes) > 0:
                placeholders = ','.join(['%s' for _ in material_codes])
                where_clauses.append(f"material_code IN ({placeholders})")
                params.extend(material_codes)

            if tech_ids and len(tech_ids) > 0:
                placeholders = ','.join(['%s' for _ in tech_ids])
                where_clauses.append(f"tech_id IN ({placeholders})")
                params.extend(tech_ids)

            if source_type:
                where_clauses.append("source_type = %s")
                params.append(source_type)

            where_clause = " AND ".join(where_clauses)

            query = f"""
                SELECT 
                    material_code,
                    MAX(material_desc) as material_desc,
                    tech_id,
                    loc_code,
                    MAX(loc_name) as loc_name,
                    SUM(stock_qty) as stock_qty,
                    MAX(unit_price) as unit_price,
                    GROUP_CONCAT(DISTINCT source_type ORDER BY source_type SEPARATOR '/') as source_type,
                    GROUP_CONCAT(DISTINCT factory_name ORDER BY factory_name SEPARATOR '/') as factory_name
                FROM w_stock_info_0808
                WHERE {where_clause}
                GROUP BY loc_code, material_code, tech_id
                LIMIT 500
            """

            cur.execute(query, params)
            rows = cur.fetchall()

            warehouse_distances = {}
            if target_warehouse:
                try:
                    cur.execute("""
                        SELECT fd_source_warehouse_code, fd_target_warehouse_code, fd_distance
                        FROM mt_warehouse_distance
                        WHERE fd_target_warehouse_code = %s
                    """, (target_warehouse,))
                    distance_rows = cur.fetchall()
                    for drow in distance_rows:
                        src_wh = drow.get('fd_source_warehouse_code', '')
                        distance = drow.get('fd_distance', 0) or 0
                        if isinstance(distance, float):
                            pass
                        warehouse_distances[src_wh] = distance
                    logger.info(f"[AllocationStream] 获取到仓库距离: {warehouse_distances}")
                except Exception as e:
                    logger.warning(f"[AllocationStream] 查询仓库距离失败: {e}")

            stocks = []
            for row in rows:
                loc_code = row['loc_code']
                distance_val = warehouse_distances.get(loc_code)
                if distance_val is not None:
                    distance_val = float(distance_val)
                stocks.append({
                    'material_code': row['material_code'],
                    'material_desc': row['material_desc'],
                    'tech_id': row['tech_id'],
                    'loc_code': loc_code,
                    'loc_name': row['loc_name'],
                    'stock_qty': float(row['stock_qty'] or 0),
                    'unit_price': float(row['unit_price'] or 0) if row['unit_price'] else 0,
                    'source_type': row['source_type'] or '',
                    'factory_name': row['factory_name'] or '',
                    'distance': distance_val
                })

            return stocks
        except Exception as e:
            logger.error(f"[AllocationStream] 查询库存失败: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def _build_material_source_type_map(self, stocks: List[Dict[str, Any]]) -> Dict[str, str]:
        """构建物料编码到库存类型的映射"""
        source_type_map = {}
        for stock in stocks:
            material_code = stock.get('material_code', '')
            source_type = stock.get('source_type', '')
            if material_code and source_type:
                if material_code not in source_type_map:
                    source_type_map[material_code] = source_type
        return source_type_map

    def _extract_material_codes(self, plans: List[Dict[str, Any]]) -> List[str]:
        """从计划列表中提取去重后的物料编码列表"""
        return list(set([
            p.get('materialCode') or p.get('material_code', '')
            for p in plans
            if p.get('materialCode') or p.get('material_code')
        ]))

    def _extract_tech_ids(self, plans: List[Dict[str, Any]]) -> List[str]:
        """从计划列表中提取所有不重复的技术规范书ID"""
        return list(set([
            p.get('techSpecId') or p.get('tech_spec_id', '')
            for p in plans
            if p.get('techSpecId') or p.get('tech_spec_id')
        ]))
