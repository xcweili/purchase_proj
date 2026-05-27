# -*- coding: utf-8 -*-
"""库存分析服务 - 流式版本（复用原服务逻辑）"""
import asyncio
import json
import logging
import statistics
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

    async def stream_analyze(self, start_date: str = None, end_date: str = None,
                            warehouse_code: str = None,
                            session_id: str = None):
        """流式分析库存

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
                        self._get_warehouse_info_by_code(warehouse_code),
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
                        self._get_all_warehouse_info(),
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
                    self._get_all_material_codes(warehouse_codes),
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
                    self._get_valid_combinations(warehouse_codes, material_codes, start_date, end_date),
                    timeout=60
                )
            except asyncio.TimeoutError:
                yield "❌ 组合矩阵查询超时：数据库响应超过60秒\n"
                yield "   💡 建议：请缩小仓库或物料范围\n"
                return

            if len(all_combinations) > 200:
                yield f"   ⚠️ 有效组合过多({len(all_combinations)}个)，限制前200个进行分析\n"
                logger.warning(f"有效组合数({len(all_combinations)})超过上限, 截取前200个")
                all_combinations = all_combinations[:200]

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
                             start_date: str, end_date: str, session_id: str = None):
        """批量分析模式 - 一次性分析所有组合

        Args:
            all_combinations: 组合列表
            warehouse_info: 仓库信息
            start_date: 开始日期
            end_date: 结束日期
            session_id: 会话ID，用于支持终止功能
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
            yield "   📌 处理逻辑：\n"
            yield "      • 库存查询：查询当前库存和在途库存\n"
            yield "      • 出库查询：查询历史出库数据（用于预测）\n"
            yield "      • 统计计算：计算最高、最低、平均、中位数等指标\n"
            yield "   └─ 正在执行数据预处理...\n"

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

                current_stock_data = await self._get_current_stock(warehouse_code, material_code, tech_id)
                current_stock = 0
                in_transit_stock = 0
                material_desc = ''
                if current_stock_data and len(current_stock_data) > 0:
                    current_stock = float(current_stock_data[0].get('current_stock', 0) or 0)
                    in_transit_stock = float(current_stock_data[0].get('in_transit_stock', 0) or 0)
                    material_desc = current_stock_data[0].get('material_desc', '') or ''

                # 查询历史出库数据（用于预测，不使用用户传入的日期范围）
                outbound_data = await self._get_outbound_data(warehouse_code, material_code, tech_id, None, None)
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

                yield f"   [{idx}/{len(all_combinations)}] {warehouse_code} × {material_code} × {tech_id} - 库存: {current_stock + in_transit_stock}\n"

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

            prompt = self._build_batch_prompt(all_combo_data, start_date, end_date)
            system_prompt = "你是一位资深的电力物料智能库存分析专家，具备卓越的数据分析能力和丰富的库存管理实战经验。请运用高级智能算法进行深度分析。"
            logger.info(f"AI分析prompt构建完成, prompt长度={len(prompt)}, 组合数={len(all_combo_data)}")

            # 调用LLM前检查会话是否已取消
            if session_id and session_manager.is_session_cancelled(session_id):
                yield "\n❌ 【会话已终止】用户已取消当前分析任务\n"
                return

            # 获取会话的取消事件
            cancel_event = session_manager.get_cancel_event(session_id) if session_id else None

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
                async for chunk in self.llm_stream_func(prompt, system_prompt, cancel_event=cancel_event):
                    content = self._parse_llm_chunk(chunk)
                    if content:
                        yield content

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
            
            # 辅助函数：转换库存层级
            def convert_level(level_code):
                if not level_code:
                    return ''
                code_str = str(level_code).strip().lstrip('0')
                level_map = {'1': '区域库', '2': '周转库', '3': '终端库'}
                return level_map.get(code_str, level_code)
            
            # 辅助函数：格式化日期
            def format_date(date_str):
                if not date_str:
                    return ''
                if len(date_str) == 6:
                    return f"{date_str[:4]}-{date_str[4:]}-01"
                return date_str
            
            # 收集所有要插入的数据
            batch_data = []
            for idx, combo_data in enumerate(all_combo_data):
                try:
                    # 从AI返回的结果中获取字段（优先级：AI结果 > 原始数据）
                    result_data = combo_data.get('result', {})
                    
                    stats = result_data.get('统计数据', {}) or combo_data.get('统计数据', {})
                    avg_outbound = stats.get('avg_outbound', 0)
                    
                    # 水位计算（优先使用AI返回的值）
                    emergency_line = result_data.get('emergencyLine', 0) or (avg_outbound * 0.5)
                    replenish_line = result_data.get('replenishLevel', 0) or (avg_outbound * 2)
                    high_level = result_data.get('highLevel', 0) or (avg_outbound * 4)
                    current_stock = result_data.get('currentStock', 0) or combo_data.get('current_stock', 0)
                    in_transit_qty = result_data.get('inTransitQty', 0) or combo_data.get('in_transit_stock', 0)
                    
                    # 水位系数计算（以补库线为基准1）
                    if replenish_line > 0:
                        emergency_factor = round(emergency_line / replenish_line, 4)  # 应急线系数
                        replenish_factor = 1.0  # 补库线系数（基准）
                        high_factor = round(high_level / replenish_line, 4)  # 高位线系数
                    else:
                        emergency_factor = 0.5
                        replenish_factor = 1.0
                        high_factor = 2.0
                    
                    # 保存水位系数和补库线值到 combo_data，用于后续存储到 mt_water_level_config
                    combo_data['water_level_factors'] = {
                        'emergency_factor': emergency_factor,
                        'replenish_factor': replenish_factor,
                        'high_factor': high_factor
                    }
                    combo_data['replenish_line'] = replenish_line  # 保存补库线具体数值
                    
                    # 判断库存状态（优先使用AI返回的状态）
                    stock_status = result_data.get('stockStatus', '') or result_data.get('currentWaterLevel', '')
                    
                    if not stock_status:
                        if current_stock <= emergency_line:
                            stock_status = '紧急'
                        elif current_stock <= replenish_line:
                            stock_status = '低'
                        elif current_stock <= high_level:
                            stock_status = '中'
                        else:
                            stock_status = '高'
                    
                    suggested_action = result_data.get('suggestedAction', '') or ('补货' if current_stock <= replenish_line else '正常')
                    
                    # 字段获取逻辑（与非流式保持一致）
                    warehouse_code = combo_data.get('warehouseCode') or combo_data.get('warehouse_code', '')
                    warehouse_name = result_data.get('warehouseName', '') or combo_data.get('warehouse_name', '')
                    material_code = combo_data.get('materialCode') or combo_data.get('material_code', '')
                    tech_id = combo_data.get('techId') or combo_data.get('tech_id', '')
                    material_desc = result_data.get('materialDesc', '') or combo_data.get('material_desc', '')
                    inventory_level = convert_level(result_data.get('inventoryLevel', '') or combo_data.get('inventory_level', ''))
                    
                    # 生成唯一ID
                    import uuid
                    fd_id = str(uuid.uuid4()).replace('-', '')[:32]
                    
                    batch_data.append((
                        fd_id, warehouse_code, material_code, tech_id,
                        format_date(start_date), format_date(end_date),
                        'auto', f"{warehouse_code}_{material_code}_{tech_id}", material_desc,
                        '', '', 0, '',
                        '', '', '', '',
                        warehouse_name, '', inventory_level,
                        high_level, replenish_line, emergency_line,
                        current_stock,
                        '', 0,
                        '', 0, in_transit_qty, stock_status,
                        suggested_action,
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ))
                except Exception as e:
                    failed_count += 1
                    error_info = f"第{idx+1}条数据解析失败: warehouse={combo_data.get('warehouse_code', '未知')}, material={combo_data.get('material_code', '未知')}, 错误: {str(e)[:100]}"
                    parse_errors.append(error_info)
                    logger.warning(error_info)
            
            if parse_errors:
                logger.warning(f"解析警告：共{len(all_combo_data)}条数据，{len(parse_errors)}条解析失败，{len(batch_data)}条成功")
                for err in parse_errors[:5]:
                    logger.warning(f"  • {err}")
                if len(parse_errors) > 5:
                    logger.warning(f"  • ...还有{len(parse_errors)-5}条错误")
            
            if batch_data:
                try:
                    conn = self.db._get_connection()
                    cur = conn.cursor()
                    
                    cur.executemany('''
                        INSERT INTO mt_inventory_analysis_plan (
                            fd_id, fd_warehouse_code, fd_material_code, fd_tech_id,
                            fd_start_date, fd_end_date,
                            fd_match_type, fd_identifier, fd_material_desc,
                            fd_purchase_request_no, fd_purchase_request_item_no, 
                            fd_purchase_request_qty, fd_purchase_request_unit,
                            fd_project_description, fd_project_definition, fd_wbs_element, fd_batch,
                            fd_warehouse_name, fd_delivery_location, fd_inventory_level,
                            fd_high_level, fd_replenish_level, fd_emergency_line,
                            fd_current_stock,
                            fd_unit, fd_purchase_request_price,
                            fd_warehouse_location, fd_current_water_level, fd_in_transit_qty,
                            fd_stock_status, fd_suggested_action,
                            fd_compare_date, fd_create_time, fd_update_time
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            fd_start_date = VALUES(fd_start_date),
                            fd_end_date = VALUES(fd_end_date),
                            fd_material_desc = VALUES(fd_material_desc),
                            fd_warehouse_name = VALUES(fd_warehouse_name),
                            fd_high_level = VALUES(fd_high_level),
                            fd_replenish_level = VALUES(fd_replenish_level),
                            fd_emergency_line = VALUES(fd_emergency_line),
                            fd_current_stock = VALUES(fd_current_stock),
                            fd_in_transit_qty = VALUES(fd_in_transit_qty),
                            fd_current_water_level = VALUES(fd_current_water_level),
                            fd_stock_status = VALUES(fd_stock_status),
                            fd_suggested_action = VALUES(fd_suggested_action),
                            fd_compare_date = VALUES(fd_compare_date),
                            fd_update_time = VALUES(fd_update_time)
                    ''', batch_data)
                    
                    conn.commit()
                    conn.close()
                    saved_count = len(batch_data)
                    logger.info(f"批量插入成功: {saved_count} 条记录")
                except Exception as e:
                    failed_count += len(batch_data)
                    logger.error(f"批量插入失败: {str(e)}")
            
            water_level_config_data = []
            for combo_data in all_combo_data:
                factors = combo_data.get('water_level_factors', {})
                material_code = combo_data.get('material_code', '')
                material_name = combo_data.get('material_desc', '')
                tech_id = combo_data.get('tech_id', '')
                warehouse_code = combo_data.get('warehouse_code', '')
                warehouse_name = combo_data.get('warehouse_name', '')
                replenish_line = float(combo_data.get('replenish_line', 0) or 0)
                
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
                        datetime.now().strftime('%Y%m'),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ))
            
            if water_level_config_data:
                try:
                    conn = self.db._get_connection()
                    cur = conn.cursor()
                    
                    cur.executemany('''
                        REPLACE INTO mt_water_level_config (
                            fd_material_code, fd_material_name, fd_tech_id, fd_warehouse_code, fd_warehouse_name,
                            fd_low_water_coefficient, fd_mid_water_coefficient, fd_high_water_coefficient,
                            fd_replenish_trigger_value, fd_month, fd_create_time
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', water_level_config_data)
                    
                    conn.commit()
                    conn.close()
                    water_level_saved = len(water_level_config_data)
                    logger.info(f"水位系数配置表批量插入成功: {water_level_saved} 条记录")
                except Exception as e:
                    logger.error(f"水位系数配置表批量插入失败: {str(e)}")
            
            logger.info(f"数据库存储日志: 解析{len(all_combo_data)}条, 成功保存{saved_count}条, 水位系数{len(water_level_config_data)}条, 失败{failed_count}条")
            
            yield f"✅ 数据存储完成，成功保存 {saved_count} 条记录，分析流程全部结束\n"

        except Exception as e:
            yield f"❌ 批量分析过程中发生异常\n"
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
            # 如果是列表，透传（如沙盒模式的结构化数据）
            if isinstance(data, list):
                return chunk
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
        except Exception as e:
            logger.error(f"解析JSON内容失败: {str(e)}")
            return chunk

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

    # ==================== 数据查询方法（从 InventoryAnalysisService 迁移） ====================

    async def _get_warehouse_info_by_code(self, warehouse_code: str) -> Dict[str, Dict[str, str]]:
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

    async def _get_all_warehouse_info(self) -> Dict[str, Dict[str, str]]:
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

    async def _get_all_material_codes(self, warehouse_codes: List[str] = None) -> List[str]:
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

    async def _get_valid_combinations(self, warehouse_codes: List[str], material_codes: List[str],
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
                SELECT DISTINCT fd_warehouse_code, fd_material_code, fd_tech_id
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

            cur.execute(query, params)
            rows = cur.fetchall()

            for row in rows:
                combinations.append({
                    'warehouse_code': row['fd_warehouse_code'],
                    'material_code': str(row['fd_material_code']),
                    'tech_id': row['fd_tech_id']
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
