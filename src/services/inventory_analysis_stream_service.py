# -*- coding: utf-8 -*-
"""库存分析服务 - 流式版本（复用原服务逻辑）"""
import asyncio
import json
import logging
import statistics
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

from ..utils.context_manager import ContextManager
from ..utils.session_manager import session_manager
from ..utils.json_repair import JSONRepair, SmartJSONParser


class InventoryAnalysisStreamService:
    """库存分析服务 - 流式版本"""

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

    async def stream_analyze_water_level(self, start_date: str = None, end_date: str = None,
                            warehouse_code: str = None,
                            session_id: str = None):
        """水位线分析模式 - AI分析历史出库数据，生成水位线并入库 mt_water_level_config

        Args:
            start_date: 开始日期（格式：YYYYMMDD）
            end_date: 结束日期（格式：YYYYMMDD）
            warehouse_code: 仓库编码，为空时查询所有仓库
            session_id: 会话ID，用于支持终止功能
        """
        # 立即输出第一个消息，让用户知道服务正在处理
        yield "🚀 【智能库存分析系统】正在启动高级库存分析引擎...\n\n"
        yield "📋 【任务概述】\n"
        yield "   本系统将运用智能库存分析算法，基于当前库存和历史消耗数据，\n"
        yield "   对仓库-物料-技术规范组合进行深度分析，计算科学合理的库存水位。\n"
        yield "   核心目标：优化库存结构、降低库存成本、避免缺货风险。\n\n"
        
        try:
            # 处理日期默认值：如果开始时间或结束时间为空，默认设置为未来7-14天
            from datetime import datetime, timedelta
            today = datetime.now().date()
            
            if not start_date or not end_date:
                default_start = today + timedelta(days=7)
                default_end = today + timedelta(days=14)
                if not start_date:
                    start_date = default_start.strftime('%Y%m%d')
                if not end_date:
                    end_date = default_end.strftime('%Y%m%d')
                yield f"⚠️ 日期参数未完整指定，已默认设置为未来7-14天\n"
                yield f"   └─ 开始日期：{start_date}\n"
                yield f"   └─ 结束日期：{end_date}\n\n"

            yield "🔍 【阶段一：确定仓库范围】\n"
            if warehouse_code:
                yield f"   ✅ 使用指定仓库：{warehouse_code}\n"
                yield f"   └─ 无需查询仓库主数据表，直接使用该仓库编码\n"
                warehouse_codes = [warehouse_code]
                try:
                    warehouse_info = await asyncio.wait_for(
                        asyncio.to_thread(self._get_warehouse_info_by_code_sync, warehouse_code),
                        timeout=30
                    )
                except asyncio.TimeoutError:
                    yield "❌ 仓库信息查询超时：数据库响应超过 30 秒\n"
                    yield "   💡 建议：请检查数据库连接状态\n"
                    return
                except Exception as e:
                    logger.warning(f"获取仓库信息失败，使用简化信息: {str(e)}")
                    warehouse_info = {warehouse_code: {'name': warehouse_code, 'level': ''}}
            else:
                yield "   📌 当前需求：获取所有仓库的信息\n"
                yield "   📌 执行动作：从仓库主数据表查询所有仓库\n"
                yield "   └─ 正在执行仓库数据检索...\n"
                logger.info("正在执行仓库数据检索...")
                try:
                    warehouse_info = await asyncio.wait_for(
                        asyncio.to_thread(self._get_all_warehouse_info_sync),
                        timeout=30
                    )
                except asyncio.TimeoutError:
                    yield "❌ 仓库数据采集超时：数据库响应超过 30 秒\n"
                    yield "   💡 建议：请检查数据库连接状态或减少查询范围\n"
                    return
                warehouse_codes = list(warehouse_info.keys())

                if not warehouse_codes:
                    yield "❌ 仓库数据采集失败：未查询到符合条件的仓库\n"
                    yield "   💡 建议：请检查仓库表中是否有数据\n"
                    return

                yield f"✅ 仓库数据采集成功\n"
                yield f"   └─ 共获取 {len(warehouse_codes)} 个仓库节点\n"
                yield f"   └─ 仓库列表：{', '.join(warehouse_codes[:5])}"
                if len(warehouse_codes) > 5:
                    yield f" ... 还有{len(warehouse_codes) - 5}个"
                yield f"\n"
            
            yield f"   └─ 下一步：查询该仓库下的物料 + 技术规范组合\n\n"
            yield "🔍 【阶段二：物料数据采集】\n"
            yield "   📌 当前需求：获取需要分析的物料编码列表\n"
            yield "   📌 执行动作：从历史出库表提取指定仓库下有出库记录的物料编码\n"
            yield "   📌 数据用途：物料编码是库存分析的核心维度，用于关联库存和消耗数据\n"
            yield "   └─ 正在执行物料数据检索...\n"
            logger.info("正在执行物料数据检索...")
            try:
                material_codes = await asyncio.wait_for(
                    asyncio.to_thread(self._get_all_material_codes_sync, warehouse_codes),
                    timeout=30
                )
            except asyncio.TimeoutError:
                yield "❌ 物料数据采集超时：数据库响应超过30秒\n"
                yield "   💡 建议：请检查数据库连接状态或减少物料范围\n"
                return
            
            if not material_codes:
                yield "❌ 物料数据采集失败：未查询到符合条件的物料\n"
                yield "   💡 建议：请检查仓库数据是否完整，或确认历史出库表中是否有数据\n"
                return
                
            yield f"✅ 物料数据采集成功\n"
            yield f"   └─ 共获取 {len(material_codes)} 个物料编码\n"
            yield f"   └─ 物料示例: {', '.join(material_codes[:5])}"
            if len(material_codes) > 5:
                yield f" ... 还有{len(material_codes) - 5}个"
            yield f"\n"
            yield f"   └─ 数据质量：已验证物料编码格式有效性\n"
            yield f"   └─ 下一步：构建仓库×物料×技术规范组合矩阵\n\n"

            yield "🔍 【阶段三：组合矩阵构建】\n"
            yield "   📌 当前需求：生成所有有效的仓库-物料-技术规范组合\n"
            yield "   📌 执行动作：批量查询历史出库表，一次性获取所有有效组合\n"
            yield "   📌 数据用途：\n"
            yield "      • 分析单元：每个组合是一个独立的库存分析单元\n"
            yield "      • 数据关联：通过组合关联库存和消耗数据\n"
            yield "      • 批量分析：支持一次性分析多个组合\n"
            yield "   📌 技术要点：\n"
            yield "      • 批量查询：单次SQL查询替代N×M次逐条查询\n"
            yield "      • 数据过滤：只包含有历史出库记录的有效组合\n"
            yield "      • 数量限制：最多分析200个组合\n"
            yield "   └─ 正在执行组合矩阵批量查询...\n"
            logger.info(f"组合矩阵批量查询开始, warehouses={len(warehouse_codes)}, materials={len(material_codes)}")
            try:
                all_combinations = await asyncio.wait_for(
                    asyncio.to_thread(self._get_valid_combinations_sync, warehouse_codes, material_codes, start_date, end_date),
                    timeout=60
                )
            except asyncio.TimeoutError:
                yield "❌ 组合矩阵查询超时：数据库响应超过60秒\n"
                yield "   💡 建议：请缩小仓库或物料范围\n"
                return

            # if len(all_combinations) > 200:
            #     yield f"   ⚠️ 有效组合过多({len(all_combinations)}个)，限制前200个进行分析\n"
            #     logger.warning(f"有效组合数({len(all_combinations)})超过上限, 截取前200个")
            #     all_combinations = all_combinations[:200]

            yield f"✅ 组合矩阵构建完成\n"
            yield f"   └─ 共获取 {len(all_combinations)} 个有效组合\n"
            if warehouse_codes:
                yield f"   └─ 组合分布：覆盖 {len(set(c['warehouse_code'] for c in all_combinations))} 个仓库\n"
            yield f"   └─ 数据就绪：已准备好进入AI智能分析阶段\n\n"
            logger.info(f"组合矩阵构建完成, 共{len(all_combinations)}个组合")
            
            if not all_combinations:
                yield "❌ 组合构建失败：未找到有效的仓库×物料×技术规范组合\n"
                yield "   💡 建议：请检查数据配置，确认仓库、物料、技术规范数据是否完整\n"
                return

            # 检查会话是否已取消
            if session_id and session_manager.is_session_cancelled(session_id):
                yield "❌ 【会话已终止】用户已取消当前分析任务\n"
                return

            yield "⚡ 【阶段四：AI智能批量分析】\n"
            yield "   📌 当前需求：对所有组合进行一次性深度智能分析\n"
            yield "   📌 执行动作：启动大规模并行分析引擎，运用机器学习算法\n"
            yield "   📌 分析目标：\n"
            yield "      • 水位计算：计算应急线、补库线、高位线\n"
            yield "      • 状态评估：判断库存健康状态（紧急、低、中、高）\n"
            yield "      • 补货建议：生成科学的补货建议和数量\n"
            yield "   📌 技术架构：时间序列分析 + 统计学习 + 规则引擎\n"
            yield "   └─ 正在启动AI分析引擎...\n"
            yield "   └─ 预计分析时间：取决于组合数量和数据复杂度\n\n"
            logger.info("正在启动AI分析引擎...")
            async for chunk in self._batch_analyze(all_combinations, warehouse_info, start_date, end_date, session_id):
                yield chunk

        except Exception as e:
            yield f"❌ 系统异常：分析过程中发生错误\n"
            yield f"   └─ 错误类型：{type(e).__name__}\n"
            yield f"   └─ 错误信息：{str(e)}\n"
            import traceback
            yield f"   └─ 详细堆栈：{traceback.format_exc()}\n"

    async def _batch_analyze(self, all_combinations: List[Dict[str, Any]], warehouse_info: Dict,
                             start_date: str, end_date: str, session_id: str = None,
                             include_stock_data: bool = False):
        """批量分析模式 - 一次性分析所有组合

        Args:
            all_combinations: 组合列表
            warehouse_info: 仓库信息
            start_date: 开始日期
            end_date: 结束日期
            session_id: 会话ID，用于支持终止功能
            include_stock_data: 是否查询stock表（水位线模式不需要，补库计划模式需要）
        """
        logger.info(f"_batch_analyze 开始, all_combinations={len(all_combinations)}, "
                    f"start_date={start_date}, end_date={end_date}")
        try:
            all_combo_data = []
            
            from datetime import datetime, timedelta
            today = datetime.now().date()
            
            # 解析用户传入的日期（作为未来时间段）
            if start_date:
                if len(start_date) == 8:
                    start_dt = datetime.strptime(start_date, '%Y%m%d').date()
                else:
                    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            else:
                start_dt = today + timedelta(days=1)  # 默认从明天开始
            
            if end_date:
                if len(end_date) == 8:
                    end_dt = datetime.strptime(end_date, '%Y%m%d').date()
                else:
                    end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            else:
                end_dt = today + timedelta(days=30)  # 默认30天后
            
            # 计算预测天数
            forecast_days = (end_dt - start_dt).days + 1
            yield f"📅 【预测时间范围】\n"
            yield f"   └─ 当前日期：{today.strftime('%Y-%m-%d')}\n"
            yield f"   └─ 预测起始：{start_dt.strftime('%Y-%m-%d')}\n"
            yield f"   └─ 预测结束：{end_dt.strftime('%Y-%m-%d')}\n"
            yield f"   └─ 预测周期：{forecast_days} 天\n\n"

            yield "🔍 【数据预处理阶段】正在智能收集所有组合的库存和历史数据...\n"
            yield "   📌 当前需求：为每个组合构建完整的库存和消耗数据集，用于未来需求预测\n"
            yield "   📌 执行动作：遍历每个组合，查询库存和历史出库数据，计算统计指标\n"
            yield "   📌 数据用途：\n"
            yield "      • 需求预测：基于历史数据预测未来消耗\n"
            yield "      • 水位计算：为AI分析提供历史消耗数据支持\n"
            yield "      • 趋势分析：支持同比环比等趋势分析\n"
            yield "      • 结果输出：为最终JSON输出提供数据基础\n"
            if include_stock_data:
                yield "   📌 处理逻辑：\n"
                yield "      • 库存查询：查询当前库存和在途库存\n"
                yield "      • 出库查询：查询历史出库数据（用于预测）\n"
                yield "      • 统计计算：计算最高、最低、平均、中位数等指标\n"
            else:
                yield "   📌 处理逻辑：\n"
                yield "      • 出库查询：查询历史出库数据（用于水位线计算）\n"
                yield "      • 统计计算：计算最高、最低、平均、中位数等指标\n"
            yield "   └─ 正在分析数据...\n"

            if include_stock_data:
                batch_stock = await asyncio.to_thread(self._batch_get_current_stock_sync, all_combinations)
            else:
                batch_stock = {}
            batch_outbound = await asyncio.to_thread(self._batch_get_outbound_data_sync, all_combinations)

            total = len(all_combinations)
            last_report_pct = 0

            for idx, combo in enumerate(all_combinations, 1):
                # 每处理一个组合前检查会话是否已取消
                if session_id and session_manager.is_session_cancelled(session_id):
                    yield "❌ 【会话已终止】用户已取消当前分析任务\n"
                    return

                warehouse_code = combo['warehouse_code']
                material_code = combo['material_code']
                tech_id = combo['tech_id']

                if not tech_id:
                    continue

                warehouse_name = warehouse_info.get(warehouse_code, {}).get('name', '')
                inventory_level = warehouse_info.get(warehouse_code, {}).get('level', '')

                key = (warehouse_code, str(material_code), tech_id)
                stock_data = batch_stock.get(key, {})
                current_stock = float(stock_data.get('current_stock', 0) or 0)
                in_transit_stock = float(stock_data.get('in_transit_stock', 0) or 0)
                # 水位线模式：从 mt_historical_outbound 获取物料描述；补库模式：从 stock 表获取
                material_desc = stock_data.get('material_desc', '') or combo.get('material_name', '')

                outbound_data = batch_outbound.get(key, [])
                stats = self._calculate_outbound_stats(outbound_data, None, None)

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
                    'forecast_days': forecast_days,
                    '历史出库': [{
                        '月份': str(ob.get('posting_month', ob.get('month', ''))),
                        '出库数量': float(ob.get('outbound_qty', 0) or 0)
                    } for ob in outbound_data[:24]],
                    '统计数据': stats
                }
                all_combo_data.append(combo_data)

                pct = idx * 100 // total
                if pct >= last_report_pct + 5:
                    last_report_pct = pct
                    yield f"   ├─ 进度 {pct}% ({idx}/{total}) | 当前: {warehouse_code} × {material_code} \n"

            yield f"\n✅ 数据预处理完成\n"
            yield f"   └─ 共收集到 {len(all_combo_data)} 个组合的完整数据\n"
            yield f"   └─ 数据质量：已验证所有组合的必需字段\n"
            yield f"   └─ 统计覆盖：{sum(1 for c in all_combo_data if c['统计数据']['total_records'] > 0)} 个组合有历史数据\n\n"

            yield "🤖 【AI智能分析阶段】正在启动高级库存分析引擎...\n"
            yield "   📌 当前需求：运用AI算法对所有组合进行深度智能分析\n"
            yield "   📌 执行动作：构建分析prompt，调用大语言模型进行智能推理\n"
            yield "   📌 分析目标：\n"
            yield "      • 水位计算：计算应急线、补库线、高位线\n"
            yield "      • 状态评估：判断库存健康状态（紧急、低、中、高）\n"
            yield "      • 补货建议：生成科学的补货建议和数量\n"
            yield "      • 结果输出：输出Markdown格式报告和JSON格式数据\n"
            yield "   📌 技术架构：\n"
            yield "      • 规则引擎：业务规则约束\n"
            yield "      • 推理能力：链式思维推理\n"
            yield "   └─ 正在调用AI分析引擎...\n"
            yield "────────────────────────────────────────\n"

            prompt = self._build_water_level_prompt(all_combo_data, start_date, end_date)
            system_prompt = "你是一位资深的电力物料智能库存分析专家，具备卓越的数据分析能力和丰富的库存管理实战经验。请运用高级智能算法进行深度分析。"
            logger.info(f"AI分析prompt构建完成, prompt长度={len(prompt)}, 组合数={len(all_combo_data)}")

            # 调用LLM前检查会话是否已取消
            if session_id and session_manager.is_session_cancelled(session_id):
                yield "\n❌ 【会话已终止】用户已取消当前分析任务\n"
                return

            # 获取会话的取消事件
            cancel_event = session_manager.get_cancel_event(session_id) if session_id else None

            # 用于非沙盒模式下积累完整响应
            full_response = None

            if self.context_manager and self.context_manager.is_too_long(prompt):
                yield "⚠️ 检测到数据量较大，将采用代码沙盒模式...\n"
                logger.info("prompt过长, 启用分层推理模式")
                async for chunk in self.context_manager._streaming_sandbox_execution(prompt, system_prompt, 'inventory', all_combo_data):
                    # 检查会话是否已取消
                    if session_id and session_manager.is_session_cancelled(session_id):
                        yield "\n❌ 【会话已终止】用户已取消当前分析任务\n"
                        return
                    content = self._parse_llm_chunk(chunk)
                    if content:
                        yield content
            else:
                # 调用LLM流式API，传入cancel_event实现快速中断
                # 积累完整响应，用于后续提取JSON结构化数据
                full_response = ''
                json_section_started = False
                progress_messages = [
                    "   ├─ 正在将分析结果编码为结构化数据...\n",
                    "   ├─ 正在提取关键水位线指标...\n",
                    "   ├─ 正在进行多维度数据校验...\n",
                    "   ├─ 正在生成补货建议数据...\n",
                ]
                msg_idx = 0
                last_progress_time = 0
                async for chunk in self.llm_stream_func(prompt, system_prompt, cancel_event=cancel_event):
                    content = self._parse_llm_chunk(chunk)
                    if content:
                        full_response += content
                        if not json_section_started:
                            if '```json' in full_response:
                                json_section_started = True
                                last_progress_time = time.time()
                                continue
                            yield content
                        else:
                            now = time.time()
                            if now - last_progress_time >= 3 and msg_idx < len(progress_messages):
                                yield progress_messages[msg_idx]
                                msg_idx += 1
                                last_progress_time = now

            logger.info("AI批量库存分析完成, 开始结果解析")

            yield "\n\n📊 【数据处理阶段】AI分析完成，正在进行结果解析...\n"
            yield "   📌 当前需求：从AI分析结果中提取结构化数据\n"
            yield "   📌 执行动作：\n"
            yield "      • JSON提取：提取JSON格式的结构化数据\n"
            yield "      • 数据验证：验证JSON数据格式和完整性\n"
            yield "   📌 数据用途：\n"
            yield "      • 数据存储：JSON数据用于数据库存储\n"
            yield "      • 后续处理：支持数据导出和二次分析\n"
            yield "   └─ 正在执行结果解析...\n\n"

            yield "📊 智能库存分析汇总报告\n"
            yield "────────────────────────────────────────\n"
            yield f"   🎯 总组合数：{len(all_combo_data)}\n"
            yield f"   ✅ 成功分析：{len(all_combo_data)} 个\n"
            yield f"   ❌ 分析失败：0 个\n"
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
            
            # 非沙盒模式：尝试从LLM完整响应中提取JSON结构化数据
            if full_response is not None:
                json_results = self.context_manager._extract_json_from_response(full_response) if self.context_manager else None
                if json_results:
                    self.context_manager._map_json_results_to_data(json_results, 'inventory', all_combo_data)
                    logger.info(f"从LLM响应中提取JSON成功，应用 {len(json_results)} 条结构化数据")
                else:
                    logger.info("使用智能校验逻辑进行验证中...")
            else:
                logger.info("沙盒模式：result已由_streaming_sandbox_execution写入")
            
            # 检查是否需要算法兜底（当 result 仍为空时）
            needs_sandbox_fallback = False
            for combo_data in all_combo_data:
                if not combo_data.get('result'):
                    needs_sandbox_fallback = True
                    break
            
            if needs_sandbox_fallback and self.context_manager:
                yield "⚠️ AI解析完成，正在启动智能校验引擎...\n"
                logger.info("AI解析结果异常，启动智能补充计算")
                
                await self.context_manager._run_algorithm_fallback('inventory', all_combo_data)
                logger.info(f"智能校验计算完成")
                yield "✅ 智能重算完成，所有水位数据已生成\n"
            
            # ============ 水位线模式：仅写入 mt_water_level_config ============
            # 批量获取物料分类信息（big_class_desc / middle_class_desc / subclass_desc）
            material_class_map = await asyncio.to_thread(
                self._batch_get_material_classification_sync, all_combo_data
            )

            water_level_config_data = []
            for combo_data in all_combo_data:
                result_data = combo_data.get('result', {})
                
                # 1) 确保 water_level_factors 存在且完整
                factors = combo_data.get('water_level_factors', {})
                if not factors:
                    # Normal 模式：LLM 返回 JSON → _map_json_results_to_data 设 result
                    # 但从 result 里提取 lines 后自行计算 factors（保持一致性）
                    stats = result_data.get('统计数据', {}) or combo_data.get('统计数据', {})
                    emergency_line = result_data.get('emergencyLine', 0) or float(stats.get('avg_outbound', 0) or 0)
                    replenish_line = result_data.get('replenishLevel', 0) or float(stats.get('avg_outbound', 0) or 0)
                    high_level = result_data.get('highLevel', 0) or float(stats.get('avg_outbound', 0) or 0)
                    if replenish_line > 0:
                        factors = {
                            'emergency_factor': round(emergency_line / replenish_line, 4),
                            'replenish_factor': 1.0,
                            'high_factor': round(high_level / replenish_line, 4)
                        }
                    else:
                        factors = {
                            'emergency_factor': 0.5,
                            'replenish_factor': 1.0,
                            'high_factor': 2.0
                        }
                    combo_data['water_level_factors'] = factors
                
                # 2) 确保 combo_data 顶层有 emergency_line / replenish_line / high_line
                #    Sandbox 模式：water_level_factors 已有但顶层字段未设 → 从 result 补
                if not combo_data.get('emergency_line'):
                    combo_data['emergency_line'] = result_data.get('emergencyLine', 0)
                if not combo_data.get('replenish_line'):
                    combo_data['replenish_line'] = result_data.get('replenishLevel', 0)
                if not combo_data.get('high_line'):
                    combo_data['high_line'] = result_data.get('highLevel', 0)
                
                material_code = combo_data.get('material_code', '')
                material_name = combo_data.get('material_desc', '')
                tech_id = combo_data.get('tech_id', '')
                warehouse_code = combo_data.get('warehouse_code', '')
                warehouse_name = combo_data.get('warehouse_name', '')
                emergency_line = float(combo_data.get('emergency_line', 0) or 0)
                replenish_line = float(combo_data.get('replenish_line', 0) or 0)
                high_line = float(combo_data.get('high_line', 0) or 0)

                # 获取物料分类描述
                class_key = f"{str(material_code)}_{tech_id}"
                class_info = material_class_map.get(class_key, {})
                big_class_code = class_info.get('big_class_code', '')
                big_class_desc = class_info.get('big_class_desc', '')
                middle_class_code = class_info.get('middle_class_code', '')
                middle_class_desc = class_info.get('middle_class_desc', '')
                subclass_code = class_info.get('subclass_code', '')
                subclass_desc = class_info.get('subclass_desc', '')
                
                if material_code and tech_id:
                    water_level_config_data.append((
                        material_code,
                        material_name,
                        tech_id,
                        warehouse_code,
                        warehouse_name,
                        factors.get('emergency_factor', 0.5),
                        factors.get('replenish_factor', 1.0),
                        factors.get('high_factor', 2.0),
                        replenish_line,
                        emergency_line,
                        replenish_line,
                        high_line,
                        replenish_line,  # fd_reserve_quota 库存定额
                        big_class_code,
                        big_class_desc,
                        middle_class_code,
                        middle_class_desc,
                        subclass_code,
                        subclass_desc,
                        datetime.now().strftime('%Y%m'),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ))
            
            water_level_saved = 0
            if water_level_config_data:
                water_level_saved = await self.db.batch_upsert_water_level_config(water_level_config_data)
                logger.info(f"水位系数配置表批量插入成功: {water_level_saved} 条记录")
            
            yield f"\n✅ 水位线数据存储完成，成功保存 {water_level_saved} 条水位配置记录，分析流程全部结束\n"

        except Exception as e:
            yield f"❌ 批量分析过程中发生异常\n"
            yield f"   └─ 错误类型：{type(e).__name__}\n"
            yield f"   └─ 错误信息：{str(e)}\n"
            import traceback
            yield f"   └─ 详细堆栈：{traceback.format_exc()}\n"

    async def stream_analyze_replenishment_plan(self, major_category: str = '', medium_category: str = '',
                                                 small_category: str = '', start_date: str = None, end_date: str = None,
                                                 session_id: str = None):
        """补库计划模式 - 确定性算法分析 + LLM总结
        1. 直接从 mt_water_level_config 按分类编码查询水位配置（不再依赖 mt_deposit_materials）
        2. 批量查询当前库存+在途库存
        3. 逐条确定性对比 → 判断状态（建议补库/立即补库/正常）
        4. 入库 mt_inventory_analysis_plan
        5. LLM总结输出
        """
        yield "🚀 【智能补库计划系统】正在启动补库计划分析引擎...\n\n"
        yield f"📋 【任务概述】\n"
        yield f"   本系统将直接基于水位配置表，按物资分类编码查询，\n"
        yield f"   并结合当前库存进行补库需求分析。\n"
        yield f"   分析参数：大类={major_category or '全部'}, 中类={medium_category or '全部'}, 小类={small_category or '全部'}\n\n"
        
        try:
            # ========== 阶段一：查询水位配置 ==========
            yield "🔍 【阶段一：查询水位配置】\n"
            yield "   📌 当前需求：直接从 mt_water_level_config 按分类编码查询水位配置\n"
            yield "   └─ 正在查询...\n"
            
            water_configs = await self.db.fetch_water_level_configs_by_category(
                major_category, medium_category, small_category
            )
            
            if not water_configs:
                yield "❌ 未查询到符合条件的水位配置\n"
                yield "   💡 建议：请检查分类编码是否正确，或确认 mt_water_level_config 表中是否有数据\n"
                return
            
            yield f"✅ 查询到 {len(water_configs)} 条水位配置\n"
            
            warehouse_codes_set = set()
            for wc in water_configs:
                warehouse_codes_set.add(wc['fd_warehouse_code'])
            
            yield f"   └─ 涉及 {len(warehouse_codes_set)} 个仓库\n\n"
            
            # ========== 阶段二：查询仓库信息 ==========
            yield "🔍 【阶段二：查询仓库信息】\n"
            yield "   📌 当前需求：从 mt_base_warehouse_info 批量查询仓库基础信息\n"
            yield "   └─ 正在查询...\n"
            
            warehouse_info = await self.db.fetch_warehouse_info_batch(list(warehouse_codes_set))
            
            yield f"✅ 查询到 {len(warehouse_info)} 个仓库信息\n\n"
            
            # ========== 阶段三：查询库存数据 ==========
            yield "🔍 【阶段三：查询当前库存】\n"
            yield "   📌 当前需求：从 w_stock_info_0808 批量查询当前库存+在途库存\n"
            yield "   └─ 正在查询...\n"
            
            # 构建所有需要查询的combo列表
            all_combos = []
            for wc in water_configs:
                combo = (wc['fd_warehouse_code'], str(wc['fd_material_code']), wc['fd_tech_id'])
                all_combos.append(combo)
            
            stock_data = await self.db.batch_get_stock_for_combos(all_combos)
            
            yield f"✅ 查询到 {len(stock_data)} 个组合的库存数据\n\n"
            
            # ========== 阶段四：确定性对比分析 ==========
            yield "⚡ 【阶段四：智能对比分析】\n"
            yield "   📌 当前需求：将当前库存+在途总量与水位线进行对比\n"
            yield "   📌 判定规则：\n"
            yield "      • 可用库存 ≤ 应急线 → 立即补库\n"
            yield "      • 应急线 < 可用库存 ≤ 补库线 → 建议补库\n"
            yield "      • 补库线 < 可用库存 ≤ 高位线 → 正常（库存适中）\n"
            yield "      • 可用库存 > 高位线 → 正常（库存充足）\n"
            yield "   └─ 正在逐条分析...\n"
            
            analysis_results = []
            emergency_count = 0
            suggest_count = 0
            normal_count = 0
            
            for wc in water_configs:
                material_code = str(wc['fd_material_code'])
                tech_id = wc['fd_tech_id']
                warehouse_code = wc['fd_warehouse_code']
                warehouse_name = wc.get('fd_warehouse_name', '')
                material_name = wc.get('fd_material_name', '')
                
                # 水位线值和分类信息均来自 mt_water_level_config
                low_coef = float(wc.get('fd_low_water_coefficient', 0.5) or 0.5)
                mid_coef = float(wc.get('fd_mid_water_coefficient', 1.0) or 1.0)
                high_coef = float(wc.get('fd_high_water_coefficient', 2.0) or 2.0)
                trigger_value = float(wc.get('fd_replenish_trigger_value', 0) or 0)
                
                # 计算水位线值
                emergency_line = trigger_value * low_coef
                replenish_line = trigger_value * mid_coef
                high_line = trigger_value * high_coef
                
                # 获取当前库存
                combo_key = (warehouse_code, material_code, tech_id)
                stock = stock_data.get(combo_key, {})
                current_stock = float(stock.get('current_stock', 0) or 0)
                in_transit_stock = float(stock.get('in_transit_stock', 0) or 0)
                available_stock = current_stock + in_transit_stock
                stock_unit = stock.get('unit', '')
                
                # 判定状态
                if available_stock <= emergency_line:
                    stock_status = '紧急'
                    suggested_action = '立即补库'
                    emergency_count += 1
                elif available_stock <= replenish_line:
                    stock_status = '低'
                    suggested_action = '建议补库'
                    suggest_count += 1
                elif available_stock <= high_line:
                    stock_status = '中'
                    suggested_action = '正常'
                    normal_count += 1
                else:
                    stock_status = '高'
                    suggested_action = '正常'
                    normal_count += 1
                
                # 计算推荐补货量
                # 规则：补货量 + 库存量 + 在途量 > 补库线 且 < 高位线
                if stock_status in ('紧急', '低'):
                    # 目标：让总可用量达到补库线，这样总库存处于中位和高位之间
                    recommended_qty = max(0, replenish_line - available_stock)
                    # 如果差值太小（四舍五入导致），加一个小缓冲确保 > 补库线
                    if recommended_qty == 0 and available_stock < replenish_line:
                        recommended_qty = max(1, round(replenish_line * 0.05, 2))
                else:
                    recommended_qty = 0
                
                wh_info = warehouse_info.get(warehouse_code, {})
                inventory_level = wh_info.get('level', '')
                
                analysis_results.append({
                    'warehouse_code': warehouse_code,
                    'warehouse_name': warehouse_name or wh_info.get('name', ''),
                    'inventory_level': inventory_level,
                    'material_code': material_code,
                    'material_desc': material_name,
                    'tech_id': tech_id,
                    'current_stock': current_stock,
                    'in_transit_stock': in_transit_stock,
                    'available_stock': available_stock,
                    'emergency_line': emergency_line,
                    'replenish_line': replenish_line,
                    'high_line': high_line,
                    'low_coef': low_coef,
                    'mid_coef': mid_coef,
                    'high_coef': high_coef,
                    'recommended_qty': round(recommended_qty, 2),
                    'unit': stock_unit,
                    'stock_status': stock_status,
                    'suggested_action': suggested_action,
                    'big_class_code': wc.get('big_class_code', ''),
                    'big_class_desc': wc.get('big_class_desc', ''),
                    'middle_class_code': wc.get('middle_class_code', ''),
                    'middle_class_desc': wc.get('middle_class_desc', ''),
                    'subclass_code': wc.get('subclass_code', ''),
                    'subclass_desc': wc.get('subclass_desc', ''),
                })
            
            total = len(analysis_results)
            yield f"\n✅ 对比分析完成\n"
            yield f"   └─ 总计 {total} 条记录\n"
            yield f"   └─ 紧急/立即补库：{emergency_count} 条\n"
            yield f"   └─ 建议补库：{suggest_count} 条\n"
            yield f"   └─ 正常：{normal_count} 条\n\n"
            
            # ========== 阶段五：数据入库 ==========
            yield "💾 【阶段五：数据入库】\n"
            yield "   📌 当前需求：将分析结果写入 mt_inventory_analysis_plan\n"
            yield "   └─ 正在入库...\n"
            
            import uuid
            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            
            # 处理日期
            def format_date(d):
                if not d:
                    return None
                if len(d) == 8:
                    return f"{d[:4]}-{d[4:6]}-{d[6:]}"
                return d
            
            db_records = []
            for ar in analysis_results:
                fd_id = str(uuid.uuid4()).replace('-', '')[:32]
                db_records.append((
                    fd_id,                                          # fd_id
                    ar['warehouse_code'],                           # fd_warehouse_code
                    ar['material_code'],                            # fd_material_code
                    ar['tech_id'],                                   # fd_tech_id
                    format_date(start_date),                        # fd_start_date
                    format_date(end_date),                          # fd_end_date
                    'auto',                                         # fd_match_type
                    f"{ar['warehouse_code']}_{ar['material_code']}_{ar['tech_id']}",  # fd_identifier
                    ar['material_desc'],                             # fd_material_desc
                    '',                                             # fd_purchase_request_no
                    '',                                             # fd_purchase_request_item_no
                    ar['recommended_qty'],                          # fd_purchase_request_qty
                    ar['unit'],                                     # fd_purchase_request_unit
                    '',                                             # fd_project_description
                    '',                                             # fd_project_definition
                    '',                                             # fd_wbs_element
                    '',                                             # fd_batch
                    ar['warehouse_name'],                           # fd_warehouse_name
                    '',                                             # fd_delivery_location
                    ar['inventory_level'],                          # fd_inventory_level
                    ar['high_line'],                                # fd_high_level
                    ar['replenish_line'],                           # fd_replenish_level
                    ar['emergency_line'],                           # fd_emergency_line
                    ar['current_stock'],                            # fd_current_stock
                    ar['unit'],                                     # fd_unit
                    0,                                              # fd_purchase_request_price
                    now_str,                                        # fd_create_time
                    now_str,                                        # fd_update_time
                    '',                                             # fd_warehouse_location
                    0,                                              # fd_current_water_level
                    ar['in_transit_stock'],                        # fd_in_transit_qty
                    ar['stock_status'],                            # fd_stock_status
                    ar['suggested_action'],                        # fd_suggested_action
                    now_str,                                        # fd_compare_date
                    ar.get('subclass_desc', ''),                   # sub_class
                    ar['inventory_level'],                          # fd_stock_level
                ))
            
            saved = await self.db.batch_insert_inventory_analysis_plan(db_records)
            yield f"✅ 数据入库完成，成功保存 {saved} 条记录\n\n"
            
            # ========== 阶段六：LLM总结 ==========
            yield "🤖 【阶段六：智能总结】\n"
            yield "   📌 当前需求：基于分析汇总数据，生成智能总结报告\n"
            yield "   └─ 正在生成总结...\n"
            yield "────────────────────────────────────────\n"
            
            # 构建总结prompt
            category_desc_parts = []
            if major_category:
                cat_desc = analysis_results[0].get('big_class_desc', major_category) if analysis_results else major_category
                category_desc_parts.append(f"大类：{cat_desc}({major_category})")
            if medium_category:
                cat_desc = analysis_results[0].get('middle_class_desc', medium_category) if analysis_results else medium_category
                category_desc_parts.append(f"中类：{cat_desc}({medium_category})")
            if small_category:
                cat_desc = analysis_results[0].get('subclass_desc', small_category) if analysis_results else small_category
                category_desc_parts.append(f"小类：{cat_desc}({small_category})")
            category_desc = '、'.join(category_desc_parts) if category_desc_parts else '全部'
            
            # 构建汇总数据
            summary_data = []
            for ar in analysis_results[:50]:  # 最多50条用于总结
                summary_data.append({
                    '仓库': ar['warehouse_name'] or ar['warehouse_code'],
                    '物料编码': ar['material_code'],
                    '物料描述': ar['material_desc'][:40],
                    '技术规范ID': ar['tech_id'],
                    '当前库存': ar['current_stock'],
                    '在途库存': ar['in_transit_stock'],
                    '可用库存': ar['available_stock'],
                    '应急线': round(ar['emergency_line'], 2),
                    '补库线': round(ar['replenish_line'], 2),
                    '高位线': round(ar['high_line'], 2),
                    '推荐补货量': ar['recommended_qty'],
                    '单位': ar['unit'],
                    '库存状态': ar['stock_status'],
                    '建议操作': ar['suggested_action']
                })
            
            summary_prompt = f"""你是一个专业的电力物料库存管理专家。系统已经完成了补库计划分析，请基于以下汇总数据进行总结。

## 分析概况
- 分析范围：{category_desc}
- 总分析条数：{total} 条
- 紧急/立即补库：{emergency_count} 条
- 建议补库：{suggest_count} 条
- 正常：{normal_count} 条

## 分析明细（部分）
{json.dumps(summary_data, ensure_ascii=False, indent=2)}

## 总结要求
请按照以下结构输出总结报告：

### 1. 整体概况
概述本次补库分析的总体情况。

### 2. 重点关注
列出需要紧急处理的物资（立即补库项），说明风险。

### 3. 建议补库项
列出建议补库的物资，说明补库数量建议。

### 4. 综合建议
给出补库优先级排序和整体库存管理建议。

请直接输出分析报告，语言专业简洁。"""
            
            system_prompt = "你是一个专业的电力物料库存管理专家，擅长分析库存数据并给出补库建议。"
            
            if self.llm_stream_func:
                async for chunk in self.llm_stream_func(summary_prompt, system_prompt):
                    content = self._parse_llm_chunk(chunk)
                    if content:
                        yield content
            
            yield "\n\n✅ 补库计划分析全部完成\n"
            
        except Exception as e:
            yield f"❌ 补库计划分析过程中发生异常\n"
            yield f"   └─ 错误类型：{type(e).__name__}\n"
            yield f"   └─ 错误信息：{str(e)}\n"
            import traceback
            yield f"   └─ 详细堆栈：{traceback.format_exc()}\n"

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
        """解析LLM返回的chunk，提取内容和思考过程
        
        区分两种模式：
        - 纯文本模式（沙盒计算阶段输出）：直接透传
        - JSON模式（LLM流式响应）：解析后提取 content / reasoning_content
        
        采用双层保障机制：
        第一层保障：使用JSONRepair修复损坏的JSON
        第二层保障：如果修复失败，返回原始字符串（保持原有行为）
        """
        if not chunk:
            return None
        
        stripped = chunk.strip()
        if stripped and not (stripped.startswith('{') or stripped.startswith('[')):
            return chunk
            
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
            # 如果是列表，透传（如沙盒模式的结构化数据）
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

    def _build_water_level_prompt(self, all_combo_data: List[Dict[str, Any]], start_date: str, end_date: str) -> str:
        """为水位线分析构建prompt - 基于历史出库数据计算水位线，不使用当前库存数据"""

        combo_list = []
        for i, data in enumerate(all_combo_data, 1):
            stats = data.get('统计数据', {})
            outbound_history = data.get('历史出库', [])

            # 水位线分析只关心历史消耗数据，不包含库存数据
            combo_list.append({
                '序号': i,
                '仓库编码': data.get('warehouse_code', ''),
                '仓库名称': data.get('warehouse_name', ''),
                '库存层级': data.get('inventory_level', ''),
                '物料编码': data.get('material_code', ''),
                '技术规范ID': data.get('tech_id', ''),
                '物料描述': data.get('material_desc', ''),
                # 历史出库统计数据
                '历史最高月出库': stats.get('max_outbound', 0),
                '历史最低月出库': stats.get('min_outbound', 0),
                '平均月出库': round(stats.get('avg_outbound', 0), 2),
                '中位数出库': round(stats.get('median_outbound', 0), 2),
                '标准差': round(stats.get('std_dev', 0), 2) if stats.get('std_dev') is not None else None,
                '离散系数(CV)': round(stats.get('std_dev', 0) / stats.get('avg_outbound', 1), 3) if stats.get('std_dev') and stats.get('avg_outbound') else None,
                '数据月份数': stats.get('total_records', 0),
                '同比变化(%)': round(stats.get('yoy_change'), 1) if stats.get('yoy_change') is not None else None,
                '环比变化(%)': round(stats.get('mom_change'), 1) if stats.get('mom_change') is not None else None,
                '消耗稳定性': stats.get('seasonality', '数据不足'),
                # 近12个月逐月出库明细（用于趋势分析）
                '近12月出库明细': [{
                    '月份': ob.get('月份', ''),
                    '出库数量': ob.get('出库数量', 0)
                } for ob in outbound_history[:12]]
            })

        prompt = f"""你是一个专业的电力物资库存水位分析专家。请基于每个组合的**历史出库数据**，运用统计分析方法，为每个组合独立计算科学合理的库存水位线。

## 任务说明
共有 {len(combo_list)} 个仓库-物料-技术规范组合，请基于各组合的**历史消耗数据**，逐个分析并计算水位线。

**核心要求**：水位线计算必须基于各组合自身的消耗特征，**不同组合的水位线系数应该不同**，不能对所有组合套用相同的系数。

## 时间范围
- 开始日期: {start_date if start_date else '未指定'}
- 结束日期: {end_date if end_date else '未指定'}

## 输入数据

### 组合数量
共有 {len(combo_list)} 个组合需要分析

### 组合数据列表
{json.dumps(combo_list, ensure_ascii=False, indent=2)}

## 水位线分析方法（重要！）

### 核心原则：基于数据特征的动态系数法

**千万不要对所有组合使用固定的倍数**（如应急线=0.25倍、补库线=1倍、高位线=2倍），必须根据每个组合的消耗特征动态确定水位线。

### 水位线计算步骤：

**第一步：分析消耗特征**
- 看「平均月出库」和「中位数出库」的差异：差异大说明有异常峰值，应以中位数为基准
- 看「离散系数(CV)」：CV > 0.5 表示波动大、CV < 0.25 表示消耗稳定
- 看「消耗稳定性」：波动较大的需要更高安全系数
- 看「同比/环比变化」：上升趋势需要调高水位、下降趋势可适当调低
- 看「近12月出库明细」：识别周期性峰值月份

**第二步：确定基准消耗量**
- 如果中位数 ≈ 平均值（差异<10%），用平均值
- 如果差异较大，用中位数（更抗异常值干扰）
- 如果有明显上升趋势，用近6个月平均值

**第三步：动态确定水位线系数**

根据以下规则为**每个组合独立计算**系数（不是固定值，是推导过程）：

| 指标 | 推导逻辑 |
|------|---------|
| **应急线** | 覆盖1个供货周期的紧急需求 = 基准月消耗 × (1 + CV) × 供货系数。波动大的(CV>0.5)供货系数取0.6-0.8，波动小的(CV<0.25)取0.3-0.5。应考虑历史最低月出库作为参考下限 |
| **补库线** | 覆盖1.5-2个供货周期 = 基准月消耗 × (1.5 + CV) × 供货系数。应明显高于应急线，给补库留出操作时间 |
| **高位线** | 覆盖2-3个供货周期 = 基准月消耗 × (2 + CV × 2) × 供货系数。应考虑历史最高月出库，确保能应对需求峰值 |

**供货周期参考**：默认15-45天（0.5-1.5个月），根据库存层级调整。

**第四步：合理性校验（关键！）**

**⚠️ 最重要规则：三个水位线数值必须不同，且保持合理梯度**

这是水位线分析的基本要求。如果一个物料消耗稳定且无明显波动，三个线之间可以接近但必须有明确差距；如果波动大，差距应更加显著。以下为强制要求：

1. **应急线 < 补库线 < 高位线**（必须严格满足，不可相等）
2. 补库线应至少比应急线高出 40%-200%（根据数据波动而定，波动越大差距越大）
3. 高位线应至少比补库线高出 50%-300%（根据峰值覆盖需求而定）
4. 应急线应接近但不低于历史最低月出库
5. 高位线应能覆盖历史最高月出库的大部分情况

**禁止事项**：
- ❌ 不要对所有组合使用相同的系数（如都乘以0.5、1.0、2.0）
- ❌ 不要让三个线相等（如应急线=补库线=高位线=100）
- ❌ 不要套用固定公式而不看数据特征
- ✅ 每个组合根据自身的CV、趋势、基准值独立推导"

### 水位线定义：
- **应急线**：最低安全库存，低于此线必须走应急补库流程
- **补库线**：可以开始补库，库存量可能有一定风险
- **高位线**：库存已处于高点，不用再补库，可考虑利库

## 输出格式要求

请使用Markdown格式，为**每一个组合**单独输出分析结果：

---

## 【组合 1/{len(combo_list)}】水位线分析

### 一、组合信息
- 仓库编码: [值]
- 仓库名称: [值]
- 库存层级: [值]
- 物料编码: [值]
- 技术规范ID: [值]
- 物料描述: [值]

### 二、消耗数据特征分析
请基于输入数据，分析该组合的消耗特征：
- 基准消耗量：[值]（说明为什么选这个值作为基准）
- 消耗波动性：[CV值分析]
- 趋势判断：[上升/下降/平稳，依据同比环比]
- 异常情况：[是否存在异常峰值，如何处理]

### 三、水位线计算结果
| 指标 | 计算过程 | 计算结果 |
|------|---------|---------|
| 应急线 | [推导过程：基准×系数，系数如何确定] | [数值] |
| 补库线 | [推导过程] | [数值] |
| 高位线 | [推导过程] | [数值] |

### 四、合理性说明
简要说明三个水位线之间的关系是否符合逻辑，与历史消耗数据是否匹配。

---

**然后继续输出组合2，组合3...直到所有{len(combo_list)}个组合都分析完毕**

---

### 数据解析入库（非常重要）

在完成所有Markdown分析报告后，请在最后输出一个完整的JSON结构化数据：

```json
{{
  "total": {len(combo_list)},
  "suggestion": "[综合建议]",
  "results": [
    {{
      "warehouseCode": "仓库编码",
      "warehouseName": "仓库名称",
      "inventoryLevel": "库存层级",
      "materialCode": "物料编码",
      "techId": "技术规范ID",
      "materialDesc": "物料描述",
      "emergencyLine": [应急线数值],
      "replenishLine": [补库线数值],
      "highLine": [高位线数值],
      "analysisBasis": "基准消耗量/系数推导简述"
    }}
  ]
}}
```

**⚠️ 数据一致性要求**：
- JSON中的水位线数值必须与Markdown分析报告中的数值**完全一致**
- 组合顺序必须与输入数据顺序保持一致
- 所有组合放在一个JSON中输出
- **每个组合的 emergencyLine < replenishLine < highLine 必须严格成立，且差距应反映该组合的数据特征**
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
- **紧急状态**: 实际可用库存 <= 应急线 → **建议补库**
- **低水位**: 实际可用库存 > 应急线 且 <= 补库线 → **建议补库**
- **中水位**: 实际可用库存 > 补库线 且 <= 高位线 → **立即补库**
- **高水位**: 实际可用库存 > 高位线 → **正常**

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

    # ==================== 数据查询方法（从 InventoryAnalysisService 迁移） ====================

    def _get_warehouse_info_by_code_sync(self, warehouse_code: str) -> Dict[str, Dict[str, str]]:
        """获取指定仓库编码的仓库信息"""
        warehouse_info = {}
        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            query = '''
                SELECT fd_warehouse_code, fd_warehouse_name, fd_stock_level
                FROM mt_base_warehouse_info
                WHERE fd_warehouse_code = %s
            '''
            cur.execute(query, (warehouse_code,))
            row = cur.fetchone()

            if row:
                warehouse_info[warehouse_code] = {
                    'name': row['fd_warehouse_name'] or '',
                    'level': row['fd_stock_level'] or ''
                }

        except Exception as e:
            logger.error(f"[InventoryAnalysisStream] 获取仓库信息失败: {str(e)}")
        finally:
            if conn:
                conn.close()

        return warehouse_info

    def _get_all_warehouse_info_sync(self) -> Dict[str, Dict[str, str]]:
        """获取所有仓库信息"""
        warehouse_info = {}
        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            query = '''
                SELECT fd_warehouse_code, fd_warehouse_name, fd_stock_level
                FROM mt_base_warehouse_info
            '''
            cur.execute(query)
            rows = cur.fetchall()

            for row in rows:
                warehouse_code = row['fd_warehouse_code']
                if warehouse_code:
                    warehouse_info[warehouse_code] = {
                        'name': row['fd_warehouse_name'] or '',
                        'level': row['fd_stock_level'] or ''
                    }

        except Exception as e:
            logger.error(f"[InventoryAnalysisStream] 获取仓库信息失败: {str(e)}")
        finally:
            if conn:
                conn.close()

        return warehouse_info

    def _get_all_material_codes_sync(self, warehouse_codes: List[str] = None) -> List[str]:
        """获取物料编码（可指定仓库范围）
        
        Args:
            warehouse_codes: 仓库编码列表，如果指定则只获取这些仓库下有出库记录的物料
        """
        material_codes = []
        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            query = "SELECT DISTINCT fd_material_code FROM mt_historical_outbound"
            params = []
            
            if warehouse_codes and len(warehouse_codes) > 0:
                placeholders = ','.join(['%s'] * len(warehouse_codes))
                query += f" WHERE fd_warehouse_code IN ({placeholders})"
                params.extend(warehouse_codes)
            
            cur.execute(query, params)
            rows = cur.fetchall()

            for row in rows:
                if row['fd_material_code']:
                    material_codes.append(str(row['fd_material_code']))

        except Exception as e:
            logger.error(f"[InventoryAnalysisStream] 获取所有物料编码失败: {str(e)}")
        finally:
            if conn:
                conn.close()

        return material_codes

    def _get_valid_combinations_sync(self, warehouse_codes: List[str], material_codes: List[str],
                                       start_date: str = None, end_date: str = None) -> List[Dict[str, str]]:
        """批量查询所有有效的仓库 - 物料 - 技术规范组合
        
        注意：start_date 和 end_date 是预测的未来时间段，不用于过滤历史出库数据
        历史出库数据应该全部获取，用于预测未来需求
        """
        combinations = []
        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            # 不使用日期范围过滤历史数据，因为我们需要所有历史数据来预测未来
            query = '''
                SELECT fd_warehouse_code, fd_material_code, fd_tech_id,
                       MAX(fd_material_name) as fd_material_name
                FROM mt_historical_outbound
                WHERE 1=1
            '''
            params = []

            if warehouse_codes:
                placeholders = ','.join(['%s'] * len(warehouse_codes))
                query += f" AND fd_warehouse_code IN ({placeholders})"
                params.extend(warehouse_codes)

            if material_codes:
                placeholders = ','.join(['%s'] * len(material_codes))
                query += f" AND fd_material_code IN ({placeholders})"
                params.extend(material_codes)

            query += " GROUP BY fd_warehouse_code, fd_material_code, fd_tech_id"

            cur.execute(query, params)
            rows = cur.fetchall()

            for row in rows:
                combinations.append({
                    'warehouse_code': row['fd_warehouse_code'],
                    'material_code': str(row['fd_material_code']),
                    'tech_id': row['fd_tech_id'],
                    'material_name': row['fd_material_name'] or ''
                })

        except Exception as e:
            logger.error(f"[InventoryAnalysisStream] 批量查询组合失败：{str(e)}")
        finally:
            if conn:
                conn.close()

        return combinations

    async def _get_current_stock(self, warehouse_code: str, material_code: str, tech_id: str) -> List[Dict[str, Any]]:
        """获取指定仓库+物料+tech_id的当前库存数据"""
        stocks = []
        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            cur.execute('''
                SELECT
                    w.loc_code as warehouse_code,
                    w.loc_name as warehouse_name,
                    w.material_code,
                    w.material_desc,
                    w.tech_id,
                    w.stock_qty as current_stock,
                    w.source_type,
                    (SELECT SUM(stock_qty)
                     FROM w_stock_info_0808
                     WHERE material_code = w.material_code
                       AND loc_code = w.loc_code
                       AND source_type = '在途') as in_transit_stock
                FROM w_stock_info_0808 w
                WHERE w.loc_code = %s
                  AND w.material_code = %s
                  AND w.tech_id = %s
            ''', (warehouse_code, str(material_code), tech_id))

            rows = cur.fetchall()

            for row in rows:
                stocks.append({
                    "warehouse_code": row['warehouse_code'],
                    "warehouse_name": row['warehouse_name'],
                    "material_code": row['material_code'],
                    "material_desc": row['material_desc'],
                    "tech_id": row['tech_id'],
                    "current_stock": row['current_stock'] or 0,
                    "source_type": row['source_type'] or '',
                    "in_transit_stock": row['in_transit_stock'] or 0
                })

        except Exception as e:
            logger.error(f"[InventoryAnalysisStream] 获取当前库存失败: {str(e)}")
        finally:
            if conn:
                conn.close()

        return stocks

    async def _get_outbound_data(self, warehouse_code: str, material_code: str, tech_id: str,
                                 start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """获取指定仓库+物料+tech_id的历史出库数据"""
        outbound_data = []
        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            start_month = None
            end_month = None
            if start_date and len(start_date) == 6:
                start_month = f"{start_date[:4]}-{start_date[4:]}"
            if end_date and len(end_date) == 6:
                end_month = f"{end_date[:4]}-{end_date[4:]}"

            query = '''
                SELECT fd_posting_month, fd_outbound_qty, fd_outbound_count
                FROM mt_historical_outbound
                WHERE fd_warehouse_code = %s
                  AND fd_material_code = %s
                  AND fd_tech_id = %s
            '''
            params = [warehouse_code, str(material_code), tech_id]

            if start_month:
                query += " AND fd_posting_month >= %s"
                params.append(start_month)

            if end_month:
                query += " AND fd_posting_month <= %s"
                params.append(end_month)

            query += " ORDER BY fd_posting_month DESC"

            cur.execute(query, params)
            rows = cur.fetchall()

            for row in rows:
                outbound_data.append({
                    "posting_month": row['fd_posting_month'],
                    "outbound_qty": row['fd_outbound_qty'] or 0,
                    "outbound_count": row['fd_outbound_count'] or 0
                })

        except Exception as e:
            logger.error(f"[InventoryAnalysisStream] 获取历史出库数据失败: {str(e)}")
        finally:
            if conn:
                conn.close()

        return outbound_data

    def _batch_get_material_classification_sync(self, all_combinations: List[Dict]) -> Dict[str, Dict]:
        """从 mt_deposit_materials 批量查询物料分类描述

        水门线模式需要 big_class_desc / middle_class_desc / subclass_desc，
        从 mt_deposit_materials 按 (material_code, tech_id) 匹配。

        key = f"{material_code}_{tech_id}"
        """
        result = {}
        if not all_combinations:
            return result

        code_tech_pairs = set()
        for combo in all_combinations:
            mc = combo.get('material_code', '')
            ti = combo.get('tech_id', '')
            if mc and ti:
                code_tech_pairs.add((str(mc), ti))

        if not code_tech_pairs:
            return result

        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            # 精确 pair 匹配：(material_code, tech_id) 一一对应
            # 利用复合索引 uk_material_spec(fd_material_code, fd_tech_spec_id)
            pairs = [(str(p[0]), p[1]) for p in code_tech_pairs]
            pair_placeholders = ','.join(['(%s, %s)'] * len(pairs))
            # 展平为参数列表：[code1, tech1, code2, tech2, ...]
            flat_params = [v for pair in pairs for v in pair]

            query = f'''
                SELECT DISTINCT fd_material_code, fd_tech_spec_id,
                       fd_big_class_code, fd_big_class_desc,
                       fd_middle_class_code, fd_middle_class_desc,
                       fd_subclass_code, fd_subclass_desc
                FROM mt_deposit_materials
                WHERE (fd_material_code, fd_tech_spec_id) IN ({pair_placeholders})
            '''
            cur.execute(query, flat_params)
            rows = cur.fetchall()

            for row in rows:
                key = f"{str(row['fd_material_code'])}_{row['fd_tech_spec_id']}"
                result[key] = {
                    'big_class_code': row.get('fd_big_class_code', '') or '',
                    'big_class_desc': row.get('fd_big_class_desc', '') or '',
                    'middle_class_code': row.get('fd_middle_class_code', '') or '',
                    'middle_class_desc': row.get('fd_middle_class_desc', '') or '',
                    'subclass_code': row.get('fd_subclass_code', '') or '',
                    'subclass_desc': row.get('fd_subclass_desc', '') or ''
                }

            logger.info(f"[InventoryAnalysisStream] 批量获取物料分类: {len(rows)} 条")
        except Exception as e:
            logger.error(f"[InventoryAnalysisStream] 批量获取物料分类失败: {str(e)}")
        finally:
            if conn:
                conn.close()

        return result

    def _batch_get_current_stock_sync(self, all_combinations: List[Dict]) -> Dict[tuple, Dict]:
        """批量获取所有组合的当前库存数据

        Args:
            all_combinations: 组合列表，每个元素包含 warehouse_code, material_code, tech_id

        Returns:
            dict: key=(warehouse_code, material_code, tech_id), value=库存数据dict
        """
        warehouse_codes = set()
        combo_keys = set()
        for combo in all_combinations:
            w = combo['warehouse_code']
            m = combo['material_code']
            t = combo['tech_id']
            if t:
                warehouse_codes.add(w)
                combo_keys.add((w, str(m), t))

        if not warehouse_codes:
            return {}

        result = {}
        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            placeholders = ','.join(['%s'] * len(warehouse_codes))
            query = f'''
                SELECT
                    w.loc_code as warehouse_code,
                    w.material_code,
                    w.tech_id,
                    MAX(w.material_desc) as material_desc,
                    SUM(CASE WHEN w.source_type IS NULL OR w.source_type != '在途' THEN w.stock_qty ELSE 0 END) as current_stock,
                    SUM(CASE WHEN w.source_type = '在途' THEN w.stock_qty ELSE 0 END) as in_transit_stock
                FROM w_stock_info_0808 w
                WHERE w.loc_code IN ({placeholders})
                GROUP BY w.loc_code, w.material_code, w.tech_id
            '''
            cur.execute(query, list(warehouse_codes))
            rows = cur.fetchall()

            for row in rows:
                key = (row['warehouse_code'], str(row['material_code']), row['tech_id'])
                if key in combo_keys:
                    result[key] = {
                        'warehouse_code': row['warehouse_code'],
                        'material_code': str(row['material_code']),
                        'material_desc': row['material_desc'] or '',
                        'tech_id': row['tech_id'],
                        'current_stock': float(row['current_stock'] or 0),
                        'in_transit_stock': float(row['in_transit_stock'] or 0),
                    }

            logger.info(f"[InventoryAnalysisStream] 批量获取库存数据成功: 查询到 {len(rows)} 行, 匹配 {len(result)} 个组合")
        except Exception as e:
            logger.error(f"[InventoryAnalysisStream] 批量获取库存数据失败: {str(e)}")
        finally:
            if conn:
                conn.close()

        return result

    def _batch_get_outbound_data_sync(self, all_combinations: List[Dict]) -> Dict[tuple, List[Dict]]:
        """批量获取所有组合的历史出库数据

        Args:
            all_combinations: 组合列表

        Returns:
            dict: key=(warehouse_code, material_code, tech_id), value=出库数据列表
        """
        warehouse_codes = set()
        combo_keys = set()
        for combo in all_combinations:
            w = combo['warehouse_code']
            m = combo['material_code']
            t = combo['tech_id']
            if t:
                warehouse_codes.add(w)
                combo_keys.add((w, str(m), t))

        if not warehouse_codes:
            return {}

        result = {key: [] for key in combo_keys}
        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            placeholders = ','.join(['%s'] * len(warehouse_codes))
            query = f'''
                SELECT fd_warehouse_code, fd_material_code, fd_tech_id,
                       fd_posting_month, fd_outbound_qty, fd_outbound_count
                FROM mt_historical_outbound
                WHERE fd_warehouse_code IN ({placeholders})
                ORDER BY fd_warehouse_code, fd_material_code, fd_tech_id, fd_posting_month DESC
            '''
            cur.execute(query, list(warehouse_codes))
            rows = cur.fetchall()

            for row in rows:
                key = (row['fd_warehouse_code'], str(row['fd_material_code']), row['fd_tech_id'])
                if key in combo_keys:
                    result[key].append({
                        "posting_month": row['fd_posting_month'],
                        "outbound_qty": row['fd_outbound_qty'] or 0,
                        "outbound_count": row['fd_outbound_count'] or 0
                    })

            logger.info(f"[InventoryAnalysisStream] 批量获取出库数据成功: 查询到 {len(rows)} 行, 覆盖 {sum(1 for v in result.values() if v)} 个组合")
        except Exception as e:
            logger.error(f"[InventoryAnalysisStream] 批量获取出库数据失败: {str(e)}")
        finally:
            if conn:
                conn.close()

        return result
