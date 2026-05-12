# -*- coding: utf-8 -*-
"""供应商匹配服务 - 流式版本（复用原服务逻辑）"""
import asyncio
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

from ..utils.context_manager import ContextManager
from ..utils.session_manager import session_manager


class SupplierMatchStreamService:
    """供应商匹配服务 - 流式版本"""

    def __init__(self, db, llm_stream_func, llm_func=None):
        self.db = db
        self.llm_stream_func = llm_stream_func
        self.llm_func = llm_func
        if llm_func:
            self.context_manager = ContextManager(llm_func, llm_stream_func)
        else:
            self.context_manager = None

    async def stream_analyze(self, input_plans: List[Dict[str, Any]] = None, analyze_mode: str = "batch", session_id: str = None):
        """流式分析供应商匹配

        Args:
            input_plans: 输入的补货计划列表，如果为None则从数据库查询
            analyze_mode: 分析模式，"batch"一次性分析所有组合(默认)，"iterative"逐个分析
            session_id: 会话ID，用于支持终止功能
        """
        # 立即输出第一个消息，让用户知道服务正在处理
        yield "🚀 【智能供应商匹配系统】正在启动高级供应商匹配引擎...\n\n"
        yield "📋 【任务概述】\n"
        yield "   本系统将运用智能供应商匹配算法，基于补货计划和供应商协议数据，\n"
        yield "   为每个补货计划精准匹配最优供应商，实现采购成本最小化和供应链效率最大化。\n"
        yield "   核心目标：优化供应商结构、降低采购成本、保障供应稳定性。\n\n"
        
        logger.info(f"开始流式供应商匹配分析, input_plans_count={len(input_plans) if input_plans else 0}, "
                    f"analyze_mode={analyze_mode}")
        
        try:
            yield "🔍 【阶段一：补货计划数据采集】\n"
            yield "   📌 当前需求：获取需要进行供应商匹配的补货计划\n"
            yield "   📌 执行动作：从外部输入或数据库查询补货计划数据\n"
            yield "   📌 数据用途：补货计划是供应商匹配的核心输入，包含物料编码、需求数量、目标仓库等关键信息\n"
            yield "   📌 数据来源：\n"
            yield "      • 外部输入：用户提供的补货计划列表\n"
            yield "      • 数据库查询：从补货计划表获取待处理计划\n"
            yield "   └─ 正在执行补货计划数据检索...\n"
            if input_plans and len(input_plans) > 0:
                yield "   └─ 数据来源：使用外部输入的补货计划数据\n"
                plans = input_plans
                yield f"✅ 补货计划数据采集成功\n"
                yield f"   └─ 共获取 {len(plans)} 条输入计划\n"
                yield f"   └─ 数据完整性：已验证所有必需字段\n"
                yield f"   └─ 下一步：查询供应商协议数据\n\n"
                logger.info(f"使用外部输入计划, count={len(plans)}")
            else:
                yield "   └─ 数据来源：从数据库智能检索补货计划\n"
                logger.info("正在从数据库查询补货计划...")
                try:
                    plans = await asyncio.wait_for(
                        self._query_plans(),
                        timeout=30
                    )
                except asyncio.TimeoutError:
                    yield "❌ 补货计划查询超时：数据库响应超过30秒\n"
                    yield "   💡 建议：请检查数据库连接状态\n"
                    return
                logger.info(f"从数据库获取补货计划, count={len(plans)}")

            if not plans:
                yield "❌ 补货计划数据采集失败：未查询到补货计划\n"
                yield "   💡 建议：请检查数据源配置，或确认是否有待处理的补货计划\n"
                return

            # 检查会话是否已取消
            if session_id and session_manager.is_session_cancelled(session_id):
                yield "❌ 【会话已终止】用户已取消当前分析任务\n"
                return

            if analyze_mode == "batch":
                yield "⚡ 【阶段二：AI智能批量匹配】\n"
                yield "   📌 当前需求：对所有补货计划进行一次性深度智能匹配分析\n"
                yield "   📌 执行动作：启动大规模并行匹配引擎，运用智能匹配算法\n"
                yield "   📌 匹配目标：\n"
                yield "      • 供应商优选：为每个计划选择最优供应商\n"
                yield "      • 多策略分析：提供均衡、成本、配送三种策略方案\n"
                yield "      • 执行比例优化：优化供应商执行比例，实现均衡发展\n"
                yield "   📌 技术架构：多目标优化 + 规则引擎 + 决策树\n"
                yield "   └─ 正在启动AI匹配引擎...\n"
                yield "   └─ 预计分析时间：取决于计划数量和供应商数据规模\n\n"
                logger.info(f"启动批量供应商匹配, plans={len(plans)}")
                async for chunk in self._batch_analyze(plans, session_id):
                    yield chunk
            else:
                yield "🔄 【阶段二：AI迭代匹配】\n"
                yield "   📌 当前需求：对每个补货计划依次进行独立匹配分析\n"
                yield "   📌 执行动作：启动迭代匹配模式，逐个处理计划\n"
                yield "   📌 匹配特点：适合计划数量较大或需要实时反馈的场景\n"
                yield "   └─ 正在启动迭代匹配...\n\n"
                logger.info(f"启动迭代供应商匹配, plans={len(plans)}")
                async for chunk in self._iterative_analyze(plans, session_id):
                    yield chunk

        except Exception as e:
            yield f"❌ 系统异常：分析过程中发生错误\n"
            yield f"   └─ 错误类型：{type(e).__name__}\n"
            yield f"   └─ 错误信息：{str(e)}\n"
            import traceback
            yield f"   └─ 详细堆栈：{traceback.format_exc()}\n"

    async def _batch_analyze(self, plans: List[Dict[str, Any]], session_id: str = None):
        """批量分析模式 - 一次性分析所有计划

        Args:
            plans: 补货计划列表
            session_id: 会话ID，用于支持终止功能
        """
        logger.info(f"_batch_analyze 开始, plans={len(plans)}")
        try:
            all_plan_data = []
            all_suppliers = []

            yield "🔍 【数据预处理阶段】正在智能收集所有计划和供应商数据...\n"
            yield "   📌 当前需求：为每个补货计划构建完整的供应商数据集\n"
            yield "   📌 执行动作：遍历每个计划，查询供应商协议数据，构建关联关系\n"
            yield "   📌 数据用途：\n"
            yield "      • 供应商匹配：为AI分析提供计划-供应商关联数据\n"
            yield "      • 策略分析：支持均衡、成本、配送三种策略分析\n"
            yield "      • 结果输出：为最终JSON输出提供数据基础\n"
            yield "   📌 处理逻辑：\n"
            yield "      • 计划查询：查询补货计划的详细信息\n"
            yield "      • 供应商查询：查询协议供应商数据\n"
            yield "      • 数据关联：建立计划与供应商的关联关系\n"
            yield "   └─ 正在执行数据预处理...\n"

            for idx, plan in enumerate(plans, 1):
                # 每处理一个计划前检查会话是否已取消
                if session_id and session_manager.is_session_cancelled(session_id):
                    yield "❌ 【会话已终止】用户已取消当前分析任务\n"
                    return

                plan_id = plan.get('planId') or plan.get('plan_id', f'plan_{idx}')
                material_code = plan.get('materialCode', '')
                tech_id = plan.get('techSpecId', '')
                demand_qty = float(plan.get('demandQty', 0) or 0)

                material_desc = plan.get('materialDesc', '') or plan.get('fd_desc', '')
                if not material_desc:
                    try:
                        material_desc = await asyncio.wait_for(
                            self._get_material_desc_from_stock(material_code, tech_id),
                            timeout=20
                        )
                    except asyncio.TimeoutError:
                        logger.warning(f"获取物料描述超时, material_code={material_code}")
                        material_desc = ''

                company = plan.get('company', '')

                try:
                    suppliers = await asyncio.wait_for(
                        self._get_protocol_suppliers(plan),
                        timeout=20
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"查询协议供应商超时, plan_id={plan_id}")
                    suppliers = []

                plan_data = {
                    'plan_id': plan_id,
                    'material_code': material_code,
                    'tech_id': tech_id,
                    'tech_spec_id': plan.get('techSpecId', '') or tech_id,
                    'demand_qty': demand_qty,
                    'material_desc': material_desc or '',
                    'company': company or '',
                    'project_def': plan.get('projectDef', ''),
                    'project_desc': plan.get('projectDesc', ''),
                    'suppliers': suppliers
                }
                all_plan_data.append(plan_data)

                for s in suppliers:
                    s['plan_id'] = plan_id
                    s['material_code'] = material_code
                    all_suppliers.append(s)

                match_status = "✅ 有供应商" if suppliers else "❌ 无供应商"
                yield f"   [{idx}/{len(plans)}] 计划 {plan_id} - {match_status}（找到 {len(suppliers)} 个供应商）\n"

            yield f"\n✅ 数据预处理完成\n"
            yield f"   └─ 共收集到 {len(all_plan_data)} 个计划和 {len(all_suppliers)} 个供应商数据\n"
            yield f"   └─ 数据质量：已验证所有计划的必需字段\n"
            yield f"   └─ 匹配统计：有供应商 {sum(1 for p in all_plan_data if p['suppliers'])} 个，无供应商 {sum(1 for p in all_plan_data if not p['suppliers'])} 个\n\n"

            yield "🤖 【AI智能分析阶段】正在启动高级供应商匹配引擎...\n"
            yield "   📌 当前需求：运用AI算法对所有计划进行深度智能匹配分析\n"
            yield "   📌 执行动作：构建分析prompt，调用大语言模型进行智能推理\n"
            yield "   📌 匹配目标：\n"
            yield "      • 供应商优选：为每个计划选择最优供应商\n"
            yield "      • 多策略分析：提供均衡、成本、配送三种策略方案\n"
            yield "      • 执行比例优化：优化供应商执行比例，实现均衡发展\n"
            yield "      • 结果输出：输出Markdown格式报告和JSON格式数据\n"
            yield "   📌 技术架构：\n"
            yield "      • 规则引擎：业务规则约束\n"
            yield "      • 推理能力：链式思维推理\n"
            yield "   └─ 正在调用AI匹配引擎...\n"
            yield "────────────────────────────────────────\n"

            prompt = self._build_batch_prompt(all_plan_data, all_suppliers)
            system_prompt = "你是一位资深的电力物料智能采购供应商匹配专家，具备卓越的供应链分析能力和丰富的供应商管理实战经验。请运用高级智能算法进行深度分析。"
            logger.info(f"AI匹配prompt构建完成, prompt长度={len(prompt)}, "
                        f"有供应商计划数={sum(1 for p in all_plan_data if p['suppliers'])}")

            # 调用LLM前检查会话是否已取消
            if session_id and session_manager.is_session_cancelled(session_id):
                yield "\n❌ 【会话已终止】用户已取消当前分析任务\n"
                return

            # 获取会话的取消事件
            cancel_event = session_manager.get_cancel_event(session_id) if session_id else None

            if self.context_manager and self.context_manager.is_too_long(prompt):
                yield "⚠️ 检测到数据量较大，将采用代码沙盒模式...\n"
                logger.info("prompt过长, 启用分层推理模式")
                async for chunk in self.context_manager._streaming_sandbox_execution(prompt, system_prompt, 'supplier', {'plans': all_plan_data, 'suppliers': all_suppliers}):
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

            yield "\n\n📊 【数据处理阶段】AI分析完成，正在进行结果解析...\n"
            yield "   📌 当前需求：从AI分析结果中提取结构化数据\n"
            yield "   📌 执行动作：\n"
            yield "      • JSON提取：提取JSON格式的结构化数据\n"
            yield "      • 数据验证：验证JSON数据格式和完整性\n"
            yield "   📌 数据用途：\n"
            yield "      • 数据存储：JSON数据用于数据库存储\n"
            yield "      • 后续处理：支持数据导出和二次分析\n"
            yield "   └─ 正在执行结果解析...\n\n"
            logger.info("AI批量供应商匹配分析完成, 开始结果解析")

            matched_count = sum(1 for p in all_plan_data if p['suppliers'])
            yield "📊 智能供应商匹配汇总报告\n"
            yield "────────────────────────────────────────\n"
            yield f"   🎯 总计划数：{len(all_plan_data)}\n"
            yield f"   ✅ 有供应商匹配：{matched_count} 个（可正常采购）\n"
            yield f"   ❌ 无供应商匹配：{len(all_plan_data) - matched_count} 个（需补充供应商）\n"
            yield "────────────────────────────────────────\n"

            yield "\n💾 【数据持久化阶段】正在将分析结果存储到数据库...\n"
            yield "   📌 当前需求：将JSON格式结果数据持久化到数据库\n"
            yield "   📌 执行动作：\n"
            yield "      • JSON解析：解析AI返回的JSON格式数据\n"
            yield "      • 数据验证：验证数据完整性和格式正确性\n"
            yield "      • 数据转换：将JSON数据转换为数据库记录\n"
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
            
            # 辅助函数：获取策略名称
            def get_strategy_name(strategy_code):
                strategy_map = {'balanced': '均衡策略', 'cost': '成本优先', 'delivery': '配送优先'}
                return strategy_map.get(strategy_code, strategy_code)
            
            # 收集所有要插入的数据
            batch_data = []
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            for idx, plan_data in enumerate(all_plan_data):
                try:
                    # 从AI返回的结果中获取字段（优先级：AI结果 > 原始数据）
                    result_data = plan_data.get('result', {})
                    
                    suppliers = result_data.get('suppliers', []) or plan_data.get('suppliers', [])
                    material_desc = result_data.get('materialDesc', '') or plan_data.get('material_desc', '')
                    company = result_data.get('company', '') or plan_data.get('company', '')
                    project_def = result_data.get('projectDef', '') or plan_data.get('projectDef', '') or plan_data.get('project_def', '')
                    project_desc = result_data.get('projectDesc', '') or plan_data.get('projectDesc', '') or plan_data.get('project_desc', '')
                    
                    # 字段获取逻辑（与非流式保持一致）
                    plan_id = result_data.get('planId') or plan_data.get('planId') or plan_data.get('plan_id', '')
                    plan_code = result_data.get('planCode') or plan_data.get('plan_code', '') or plan_data.get('fd_code_use', '')
                    material_code = result_data.get('materialCode') or plan_data.get('materialCode') or plan_data.get('material_code', '')
                    demand_qty = result_data.get('demandQty', 0) or plan_data.get('demandQty', 0) or plan_data.get('demand_qty', 0)
                    unit_price = result_data.get('unitPrice', 0) or plan_data.get('unitPrice', 0) or plan_data.get('unit_price', 0) or 0
                    tech_spec_id = result_data.get('techSpecId', '') or plan_data.get('techSpecId', '') or plan_data.get('tech_spec_id', '') or plan_data.get('fd_spec_doc_id', '')
                    amount = result_data.get('amount', 0) or (demand_qty * unit_price)
                    
                    if suppliers:
                        # 构建三种策略的匹配结果
                        # 1. 均衡策略：优先选择执行比例较低的供应商
                        balanced_suppliers = sorted(suppliers, key=lambda x: float(x.get('executionRate', 0) or 0))[:3]
                        balanced_total_cost = sum(float(s.get('unitPrice', 0) or 0) * float(s.get('remainQty', 0) or 0) for s in balanced_suppliers)
                        balanced_allocated = sum(float(s.get('remainQty', 0) or 0) for s in balanced_suppliers)
                        balanced_unmet = max(0, demand_qty - balanced_allocated)
                        balanced_supplier_names = ','.join([s.get('supplierName', '') for s in balanced_suppliers])
                        # 计算第一条供应商的成本
                        balanced_first_unit_price = float(balanced_suppliers[0].get('unitPrice', 0) or 0) if balanced_suppliers else 0
                        balanced_first_allocated = float(balanced_suppliers[0].get('remainQty', 0) or 0) if balanced_suppliers else 0
                        balanced_first_cost = balanced_first_unit_price * balanced_first_allocated
                        # 判断匹配状态
                        balanced_status = '成功' if balanced_unmet == 0 else '部分匹配'
                        
                        # 2. 成本策略：优先选择单价最低的供应商
                        cost_suppliers = sorted(suppliers, key=lambda x: float(x.get('unitPrice', 0) or float('inf')))[:3]
                        cost_total_cost = sum(float(s.get('unitPrice', 0) or 0) * float(s.get('remainQty', 0) or 0) for s in cost_suppliers)
                        cost_allocated = sum(float(s.get('remainQty', 0) or 0) for s in cost_suppliers)
                        cost_unmet = max(0, demand_qty - cost_allocated)
                        cost_supplier_names = ','.join([s.get('supplierName', '') for s in cost_suppliers])
                        # 计算第一条供应商的成本
                        cost_first_unit_price = float(cost_suppliers[0].get('unitPrice', 0) or 0) if cost_suppliers else 0
                        cost_first_allocated = float(cost_suppliers[0].get('remainQty', 0) or 0) if cost_suppliers else 0
                        cost_first_cost = cost_first_unit_price * cost_first_allocated
                        # 判断匹配状态
                        cost_status = '成功' if cost_unmet == 0 else '部分匹配'
                        
                        # 3. 配送策略：优先选择能满足全部需求的单个供应商
                        delivery_suppliers = [s for s in suppliers if float(s.get('remainQty', 0) or 0) >= demand_qty]
                        if not delivery_suppliers:
                            delivery_suppliers = sorted(suppliers, key=lambda x: float(x.get('remainQty', 0) or 0), reverse=True)[:3]
                        delivery_total_cost = sum(float(s.get('unitPrice', 0) or 0) * float(s.get('remainQty', 0) or 0) for s in delivery_suppliers)
                        delivery_allocated = sum(float(s.get('remainQty', 0) or 0) for s in delivery_suppliers)
                        delivery_unmet = max(0, demand_qty - delivery_allocated)
                        delivery_supplier_names = ','.join([s.get('supplierName', '') for s in delivery_suppliers])
                        # 计算第一条供应商的成本
                        delivery_first_unit_price = float(delivery_suppliers[0].get('unitPrice', 0) or 0) if delivery_suppliers else 0
                        delivery_first_allocated = float(delivery_suppliers[0].get('remainQty', 0) or 0) if delivery_suppliers else 0
                        delivery_first_cost = delivery_first_unit_price * delivery_first_allocated
                        # 判断匹配状态
                        delivery_status = '成功' if delivery_unmet == 0 else '部分匹配'
                        
                        # 添加三种策略的数据
                        batch_data.extend([
                            (
                                plan_id, material_code, material_desc, balanced_status, 'balanced',
                                company, project_def, project_desc,
                                demand_qty, tech_spec_id,
                                json.dumps(balanced_suppliers, ensure_ascii=False), balanced_total_cost, balanced_unmet,
                                current_time, current_time,
                                balanced_suppliers[0].get('supplierCode', '') if balanced_suppliers else '',
                                balanced_suppliers[0].get('supplierName', '') if balanced_suppliers else '',
                                balanced_allocated,
                                balanced_first_unit_price,
                                balanced_first_cost,
                                float(balanced_suppliers[0].get('executionRate', 0) or 0) if balanced_suppliers else 0,
                                float(balanced_suppliers[0].get('remainQty', 0) or 0) if balanced_suppliers else 0
                            ),
                            (
                                plan_id, material_code, material_desc, cost_status, 'cost',
                                company, project_def, project_desc,
                                demand_qty, tech_spec_id,
                                json.dumps(cost_suppliers, ensure_ascii=False), cost_total_cost, cost_unmet,
                                current_time, current_time,
                                cost_suppliers[0].get('supplierCode', '') if cost_suppliers else '',
                                cost_suppliers[0].get('supplierName', '') if cost_suppliers else '',
                                cost_allocated,
                                cost_first_unit_price,
                                cost_first_cost,
                                float(cost_suppliers[0].get('executionRate', 0) or 0) if cost_suppliers else 0,
                                float(cost_suppliers[0].get('remainQty', 0) or 0) if cost_suppliers else 0
                            ),
                            (
                                plan_id, material_code, material_desc, delivery_status, 'delivery',
                                company, project_def, project_desc,
                                demand_qty, tech_spec_id,
                                json.dumps(delivery_suppliers, ensure_ascii=False), delivery_total_cost, delivery_unmet,
                                current_time, current_time,
                                delivery_suppliers[0].get('supplierCode', '') if delivery_suppliers else '',
                                delivery_suppliers[0].get('supplierName', '') if delivery_suppliers else '',
                                delivery_allocated,
                                delivery_first_unit_price,
                                delivery_first_cost,
                                float(delivery_suppliers[0].get('executionRate', 0) or 0) if delivery_suppliers else 0,
                                float(delivery_suppliers[0].get('remainQty', 0) or 0) if delivery_suppliers else 0
                            )
                        ])
                    else:
                        # 无供应商时也保存记录
                        for strategy_code in ['balanced', 'cost', 'delivery']:
                            batch_data.append((
                                plan_id, material_code, material_desc, '失败', strategy_code,
                                company, project_def, project_desc,
                                demand_qty, tech_spec_id,
                                json.dumps([]), 0, demand_qty,
                                current_time, current_time,
                                '', '', 0, 0, 0, 0, 0
                            ))
                except Exception as e:
                    failed_count += 3
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
                        REPLACE INTO mt_supplier_match_result (
                            fd_plan_id, fd_material_code, fd_material_desc, fd_match_status, fd_strategy,
                            fd_company, fd_project_def, fd_project_desc,
                            fd_demand_qty, fd_tech_spec_id,
                            fd_supplier_results, fd_total_cost, fd_unmet_demand,
                            fd_create_time, fd_update_time,
                            fd_supplier_code, fd_supplier_name, fd_allocated_qty,
                            fd_unit_price, fd_cost, fd_execution_rate, fd_remain_quantity
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', batch_data)
                    
                    conn.commit()
                    conn.close()
                    saved_count = len(batch_data)
                    logger.info(f"批量插入成功: {saved_count} 条记录")
                except Exception as e:
                    failed_count += len(batch_data)
                    logger.error(f"批量插入失败: {str(e)}")
            
            logger.info(f"数据库存储日志: 解析{len(all_plan_data)}条, 成功保存{saved_count}条, 失败{failed_count}条")
            
            yield f"✅ 数据存储完成，成功保存 {saved_count} 条记录（每个计划3种策略），分析流程全部结束\n"

        except Exception as e:
            yield f"❌ 批量分析过程中发生异常\n"
            yield f"   └─ 错误类型：{type(e).__name__}\n"
            yield f"   └─ 错误信息：{str(e)}\n"
            import traceback
            yield f"   └─ 详细堆栈：{traceback.format_exc()}\n"

    async def _iterative_analyze(self, plans: List[Dict[str, Any]], session_id: str = None):
        """迭代分析模式 - 逐个分析每个计划

        Args:
            plans: 补货计划列表
            session_id: 会话ID，用于支持终止功能
        """
        logger.info(f"_iterative_analyze 开始, plans={len(plans)}")
        total_count = len(plans)
        matched_count = 0
        unmatched_count = 0

        for idx, plan in enumerate(plans, 1):
            plan_id = plan.get('planId') or plan.get('plan_id', f'plan_{idx}')
            material_code = plan.get('materialCode', '')
            tech_id = plan.get('techSpecId', '')
            demand_qty = float(plan.get('demandQty', 0) or 0)
            company = plan.get('company', '')

            yield "\n📋 [处理 {idx}/{total_count}] 开始处理计划\n".format(idx=idx, total_count=total_count)
            yield "────────────────────────────────────────\n"
            yield f"   计划ID: {plan_id}\n"
            yield f"   物料编码: {material_code}\n"
            yield f"   技术规范ID: {tech_id}\n"
            yield f"   需求数量: {demand_qty}\n"
            yield "────────────────────────────────────────\n"

            try:
                yield "🔍 [子步骤 1/2] 获取物料描述...\n"
                material_desc = plan.get('materialDesc', '') or plan.get('fd_desc', '')
                if not material_desc:
                    try:
                        material_desc = await asyncio.wait_for(
                            self._get_material_desc_from_stock(material_code, tech_id),
                            timeout=20
                        )
                    except asyncio.TimeoutError:
                        logger.warning(f"获取物料描述超时, material_code={material_code}, plan_id={plan_id}")
                        material_desc = ''
                yield f"✅ [子步骤 1/2] 物料描述: {material_desc or '未获取到'}\n"

                yield "🔍 [子步骤 2/2] 查询协议商库存...\n"
                try:
                    suppliers = await asyncio.wait_for(
                        self._get_protocol_suppliers(plan),
                        timeout=20
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"查询协议供应商超时, plan_id={plan_id}")
                    suppliers = []

                if suppliers:
                    yield f"✅ [子步骤 3/3] 找到 {len(suppliers)} 个协议供应商\n"
                    for i, s in enumerate(suppliers[:3], 1):
                        yield f"   [{i}] 供应商: {s.get('supplierName', '')}, 执行率: {s.get('executionRate', 0)}%, 可用量: {s.get('remainQty', 0)}\n"
                    if len(suppliers) > 3:
                        yield f"   ... 还有 {len(suppliers) - 3} 个供应商\n"
                    matched_count += 1
                else:
                    yield "⚠️ [子步骤 3/3] 未找到匹配的协议供应商\n"
                    unmatched_count += 1

                yield "\n🤖 开始AI智能分析...\n"
                yield "────────────────────────────────────────\n"

                prompt = self._build_single_plan_prompt(plan, suppliers, material_desc, company)
                system_prompt = "你是一个专业的电力物料采购供应商匹配专家，擅长分析供应商协议数据并给出最优的供应商选择方案。请用清晰的中文进行分析。"

                # 调用LLM前检查会话是否已取消
                if session_id and session_manager.is_session_cancelled(session_id):
                    yield "\n❌ 【会话已终止】用户已取消当前分析任务\n"
                    return

                # 获取会话的取消事件
                cancel_event = session_manager.get_cancel_event(session_id) if session_id else None

                if self.context_manager and self.context_manager.is_too_long(prompt):
                    yield "⚠️ 检测到数据量较大，将采用代码沙盒模式...\n"
                    async for chunk in self.context_manager._streaming_sandbox_execution(prompt, system_prompt, 'supplier', {'plans': all_plan_data, 'suppliers': all_suppliers}):
                        content = self._parse_llm_chunk(chunk)
                        if content:
                            yield content
                else:
                    async for chunk in self.llm_stream_func(prompt, system_prompt, cancel_event=cancel_event):
                        content = self._parse_llm_chunk(chunk)
                        if content:
                            yield content

                status = '有匹配' if suppliers else '无匹配'
                yield f"\n✅ [处理完成] 供应商匹配状态: {status}\n"
                logger.info(f"计划 {plan_id} 迭代分析完成, 状态={status}")

            except Exception as e:
                yield f"\n❌ [处理失败] {str(e)}\n"
                logger.warning(f"计划 {plan_id} 迭代分析失败: {str(e)}")
                unmatched_count += 1

            yield "\n────────────────────────────────────────\n\n"

        yield "\n📊 供应商匹配汇总报告\n"
        yield "────────────────────────────────────────\n"
        yield f"   总计划数: {total_count}\n"
        yield f"   有供应商匹配: {matched_count}\n"
        yield f"   无供应商匹配: {unmatched_count}\n"
        yield "────────────────────────────────────────\n"
        logger.info(f"_iterative_analyze 完成, 总计划数={total_count}, 有匹配={matched_count}, 无匹配={unmatched_count}")

    def _parse_llm_chunk(self, chunk) -> Optional[str]:
        """解析LLM返回的chunk，提取内容和思考过程"""
        try:
            # 如果chunk已经是列表或字典，直接使用
            if isinstance(chunk, (list, dict)):
                data = chunk
            else:
                # 尝试解析为JSON
                data = json.loads(chunk)
            
            # 如果是列表，尝试提取第一个元素
            if isinstance(data, list):
                if len(data) > 0 and isinstance(data[0], dict):
                    delta = data[0].get('delta', {})
                    reasoning = delta.get('reasoning_content', '')
                    content = delta.get('content', '')
                    
                    if reasoning:
                        return f"{reasoning}"
                    elif content:
                        return content
                return None
            
            # 如果是字典，按原有逻辑处理
            choices = data.get('choices', [])
            if choices:
                delta = choices[0].get('delta', {})
                reasoning = delta.get('reasoning_content', '')
                content = delta.get('content', '')

                if reasoning:
                    return f"{reasoning}"
                elif content:
                    return content
            return None
        except json.JSONDecodeError:
            # 如果不是有效JSON，直接返回原始字符串
            if isinstance(chunk, str):
                return chunk
        return None

    def _build_batch_prompt(self, all_plan_data: List[Dict[str, Any]], all_suppliers: List[Dict[str, Any]]) -> str:
        """为批量分析构建prompt"""
        plans_formatted = []
        for p in all_plan_data:
            plans_formatted.append({
                '计划ID': p['plan_id'],
                '物料编码': p['material_code'],
                '物料描述': p['material_desc'],
                '技术规范ID': p['tech_id'],
                '需求数量': p['demand_qty'],
                '所属单位': p['company']
            })

        suppliers_formatted = []
        for s in all_suppliers:
            suppliers_formatted.append({
                '计划ID': s.get('plan_id', ''),
                '供应商编码': s.get('supplierCode', ''),
                '供应商名称': s.get('supplierName', ''),
                '执行比例(%)': float(s.get('executionRate', 0) or 0),
                '剩余可用数量': float(s.get('remainQty', 0) or 0),
                '剩余可用金额': float(s.get('remainAmount', 0) or 0),
                '协议单价': float(s.get('unitPrice', 0) or 0),
            })

        prompt = f"""你是一个专业的电力物料采购供应商匹配专家。我将提供多个补货需求计划和供应商协议数据，请你一次性分析所有计划并给出最优的供应商选择方案。

## 任务说明
请一次性分析以下所有补货需求计划（共 {len(plans_formatted)} 个计划），根据每个计划的供应商协议执行情况和可用库存，为每个需求计划单独选择最合适的供应商。

**重要**：你必须为**每一个补货计划**单独输出一份完整的分析结果，按照下面规定的格式，一个计划一个计划地列出结果。

## 输入数据

### 补货计划列表（共 {len(plans_formatted)} 个计划）
{json.dumps(plans_formatted, ensure_ascii=False, indent=2)}

### 供应商协议数据
{json.dumps(suppliers_formatted, ensure_ascii=False, indent=2)}

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

## 输出格式要求

**重要**：你必须按照以下格式，为每一个补货计划单独输出一份完整的分析结果。

请使用Markdown格式输出，使用##、###标题，表格使用|分隔。

**输出结构必须包含以下内容，并严格按照顺序输出**：

---

## 【计划 1/{len(plans_formatted)}】供应商匹配分析

### 一、需求概况
- 计划ID: [从输入数据中获取]
- 物料编码: [从输入数据中获取]
- 物料描述: [从输入数据中获取]
- 需求数量: [从输入数据中获取]
- 目标仓库: [从输入数据中获取]

### 二、供应商评估
- 可用供应商数量
- 各供应商执行比例分析
- 可用库存和金额对比

### 三、推荐方案
| 策略 | 推荐供应商 | 分配数量 | 单价 | 理由 |
|------|-----------|---------|------|------|

### 四、分析结论与建议
- 是否有合适的供应商
- 推荐的供应商选择策略
- 后续处理建议

---

**然后继续输出计划2，计划3...直到所有{len(plans_formatted)}个计划都分析完毕**

请用简洁、清晰的语言进行分析。
"""
        return prompt

    def _build_single_plan_prompt(self, plan: Dict[str, Any], suppliers: List[Dict[str, Any]], material_desc: str, company: str) -> str:
        """为单个计划构建供应商匹配分析prompt"""
        plan_id = plan.get('planId') or plan.get('plan_id', '')
        material_code = plan.get('materialCode', '')
        demand_qty = float(plan.get('demandQty', 0) or 0)
        tech_id = plan.get('techSpecId', '')

        suppliers_formatted = []
        for s in suppliers[:10]:
            suppliers_formatted.append({
                '供应商编码': s.get('supplierCode', ''),
                '供应商名称': s.get('supplierName', ''),
                '执行比例(%)': float(s.get('executionRate', 0) or 0),
                '剩余可用数量': float(s.get('remainQty', 0) or 0),
                '剩余可用金额': float(s.get('remainAmount', 0) or 0),
                '协议单价': float(s.get('unitPrice', 0) or 0),
            })

        prompt = f"""你是一个专业的电力物料采购供应商匹配专家。请根据以下补货计划和供应商协议数据，进行智能匹配分析。

## 任务说明
分析单个补货计划的供应商匹配情况，为其选择最合适的供应商，并给出多种策略方案。

## 当前计划信息
- 计划ID: {plan_id}
- 物料编码: {material_code}
- 物料描述: {material_desc}
- 技术规范ID: {tech_id}
- 需求数量: {demand_qty}
- 所属单位: {company}

## 协议供应商数据
{json.dumps(suppliers_formatted, ensure_ascii=False, indent=2)}

## 分析要求

**输出格式要求：**
- 使用Markdown格式输出
- 使用##、### 标题
- 列表使用 - 开头
- 表格使用 | 分隔

**输出结构：**

## 供应商匹配分析结果

### 一、需求概况
- 计划ID、物料编码、需求数量等基本信息

### 二、供应商评估
- 供应商数量和整体情况
- 各供应商执行率分析

### 三、推荐方案
| 策略 | 推荐供应商 | 数量 | 单价 | 理由 |
|------|-----------|------|------|------|
| 均衡 | 示例供应商 | 100 | 100 | 综合最优 |

### 四、分析结论与建议
- 是否有合适的供应商
- 推荐的供应商选择策略
- 后续处理建议

请用简洁、清晰的语言进行分析。
"""
        return prompt

    # ==================== 数据查询方法（从 SupplierMatchService 迁移） ====================

    async def _query_plans(self) -> List[Dict[str, Any]]:
        """查询补货计划"""
        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            cur.execute('''
                SELECT id, fd_plan_id, fd_material_no, fd_material_desc, fd_purchase_qty,
                       fd_purchase_unit, fd_spec_doc_id,
                       fd_purchase_req_NO, fd_project_def, fd_project_desc, fd_unit
                FROM mt_replenishment_plan
                WHERE fd_deleted = 0
                ORDER BY id
            ''')
            rows = cur.fetchall()

            plans = []
            for row in rows:
                plans.append({
                    'planId': row['fd_plan_id'] or str(row['id']),
                    'materialCode': row['fd_material_no'] or '',
                    'materialDesc': row['fd_material_desc'] or '',
                    'demandQty': row['fd_purchase_qty'] or 0,
                    'unit': row['fd_purchase_unit'] or '',
                    'techSpecId': row['fd_spec_doc_id'] or '',
                    'purchaseReqNo': row['fd_purchase_req_NO'] or '',
                    'projectDef': row['fd_project_def'] or '',
                    'projectDesc': row['fd_project_desc'] or '',
                    'company': row['fd_unit'] or '',
                })

            return plans
        except Exception as e:
            logger.error(f"[SupplierMatchStream] 查询补货计划失败: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    async def _get_protocol_suppliers(self, plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        """获取协议供应商"""
        material_code = plan.get('materialCode', '')
        tech_spec_id = plan.get('techSpecId', '')

        if not material_code or not tech_spec_id:
            logger.warning(f"[SupplierMatchStream] 物料编码或技术规范书ID为空，跳过查询: material_code={material_code}, tech_spec_id={tech_spec_id}")
            return []

        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            query = '''
                SELECT fd_protocol_no, fd_protocol_line, fd_mdm_supplier, fd_network_supplier,
                       fd_supplier_desc, fd_material_code, fd_material_desc,
                       fd_price_net, fd_net_price, fd_amount_net, fd_quantity, fd_remain_quantity,
                       fd_execution_rate, fd_alloc_rate, fd_tech_spec_id
                FROM mt_protocol_stock
                WHERE fd_status = '有效'
                  AND fd_material_code = %s
                  AND fd_tech_spec_id = %s
            '''
            params = [material_code, tech_spec_id]

            cur.execute(query, params)
            rows = cur.fetchall()

            suppliers = []
            for row in rows:
                suppliers.append({
                    'supplierCode': row['fd_mdm_supplier'] or row['fd_network_supplier'] or '',
                    'supplierName': row['fd_supplier_desc'] or '',
                    'materialCode': row['fd_material_code'] or '',
                    'materialDesc': row['fd_material_desc'] or '',
                    'protocolNo': row['fd_protocol_no'] or '',
                    'protocolLine': row['fd_protocol_line'] or '',
                    'unitPrice': float(row['fd_price_net'] or row['fd_net_price'] or 0),
                    'totalAmount': float(row['fd_amount_net'] or 0),
                    'totalQty': float(row['fd_quantity'] or 0),
                    'remainQty': float(row['fd_remain_quantity'] or 0),
                    'executionRate': float(row['fd_execution_rate'] or 0),
                    'allocRate': float(row['fd_alloc_rate'] or 0),
                    'techSpecId': row['fd_tech_spec_id'] or '',
                })

            return suppliers
        except Exception as e:
            logger.error(f"[SupplierMatchStream] 查询协议供应商失败: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    async def _get_material_desc_from_stock(self, material_code: str, tech_id: str = None) -> str:
        """从库存表获取物料描述"""
        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            query = '''
                SELECT material_desc
                FROM w_stock_info_0808
                WHERE material_code = %s
            '''
            params = [material_code]

            if tech_id:
                query += ' AND tech_id = %s'
                params.append(tech_id)

            query += ' LIMIT 1'

            cur.execute(query, params)
            row = cur.fetchone()

            if row and row['material_desc']:
                return row['material_desc']

        except Exception as e:
            logger.error(f"[SupplierMatchStream] 获取物料描述失败: {str(e)}")
        finally:
            if conn:
                conn.close()

        return ''
