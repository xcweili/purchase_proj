
# -*- coding: utf-8 -*-
"""调配服务 - 业务逻辑处理层"""
import copy
import logging
from decimal import Decimal
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='[SVC] %(message)s')


class AllocationService:
    """调配服务类 - 处理调配业务逻辑
    
    核心职责：
    1. 根据筛选条件查询库存使用计划
    2. 调用LLM智能体进行智能调配匹配
    3. 支持四种调配策略：时间优先(time)、成本优先(cost)、库存优先(stock)、紧急(emerg)
    4. 保存调配结果到数据库
    
    数据来源：
    - mt_stock_use_list_plan_two: 库存使用计划
    - w_stock_info_0808: 当前库存信息
    - mt_allocation_result: 调配结果存储
    """

    def __init__(self, db, allocation_agent):
        self.db = db
        self.allocation_agent = allocation_agent

    # 支持的四种调配策略
    STRATEGIES = ['time', 'cost', 'stock', 'emerg']

    async def process_allocation(self, strategy: str, warehouse_code: str,
                                  source_type: str, project_unit: str,
                                  demand_start_date: str, demand_end_date: str,
                                  plan_type: str, material_codes: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        处理智能调配业务 - 从数据库查询数据，支持多策略并行处理

        Args:
            strategy: 匹配策略，4种：time/cost/stock/emerg。如果为空则自动运行所有策略
            warehouse_code: 仓库编码筛选（仅用于查询计划，库存查询只用material_codes）
            source_type: 库存类型筛选
            project_unit: 项目单位
            demand_start_date: 需求开始时间
            demand_end_date: 需求结束时间
            plan_type: 计划类型
            material_codes: 物料编码列表

        Returns:
            处理结果字典，包含各策略的分析结果
        """
        logger.info(f"[AllocationService] strategy: {strategy}")
        logger.info(f"[AllocationService] warehouseCode: {warehouse_code}")
        logger.info(f"[AllocationService] sourceType: {source_type}")
        logger.info(f"[AllocationService] projectUnit: {project_unit}")
        logger.info(f"[AllocationService] demandStartDate: {demand_start_date}")
        logger.info(f"[AllocationService] demandEndDate: {demand_end_date}")
        logger.info(f"[AllocationService] planType: {plan_type}")
        logger.info(f"[AllocationService] materialCodes: {material_codes}")

        # ==================== 步骤1: 确定要执行的策略 ====================
        # 如果strategy为空，默认执行所有四种策略
        strategies_to_run = []
        if strategy and strategy.strip():
            strategies_to_run.append(strategy.strip())
        else:
            strategies_to_run = self.STRATEGIES

        logger.info(f"[AllocationService] 将执行策略: {strategies_to_run}")

        # 存储所有策略的执行结果
        all_strategy_results = {}

        # ==================== 步骤2: 遍历执行每种策略 ====================
        for strat in strategies_to_run:
            logger.info(f"[AllocationService] 开始执行策略: {strat}")

            # 步骤2.1: 查询库存使用计划
            # 数据来源: mt_stock_use_list_plan_two
            # 强制筛选 apply_way IN ('01', '05', '06')
            plans = await self._query_plans(project_unit, demand_start_date, demand_end_date, plan_type, warehouse_code, material_codes)
            logger.info(f"[AllocationService] [策略{strat}] 从DB查询到plans数量: {len(plans)}")

            # 如果没有计划，生成空结果
            if not plans:
                all_strategy_results[strat] = self._empty_result("没有可处理的计划")
                continue

            # 步骤2.2: 从计划中提取物料编码和技术规范书ID，用于查询库存
            material_codes_list = self._extract_material_codes(plans)
            tech_ids_list = self._extract_tech_ids(plans)
            
            # 步骤2.3: 查询库存数据（同时查询到目标仓库的距离）
            # 数据来源: w_stock_info_0808, mt_warehouse_distance
            # 按物料编码+技术规范书ID过滤，确保库存与需求匹配
            stocks = self._query_stocks(material_codes_list, source_type, warehouse_code, tech_ids_list)
            logger.info(f"[AllocationService] [策略{strat}] 从DB查询到stocks数量: {len(stocks)}")

            # 步骤2.4: 构建物料到库存类型的映射
            material_source_types = self._build_material_source_type_map(stocks)

            # 步骤2.5: 处理所有计划，调用LLM智能体进行调配匹配
            # 返回结果和统计信息
            results, stats = await self._process_plans(plans, stocks, material_source_types, strat, warehouse_code,
                                                      source_type, project_unit, demand_start_date, demand_end_date, plan_type)

            # 步骤2.6: 生成建议文本
            suggestion = self._generate_suggestion(stats, len(results))

            logger.info(f"[AllocationService] [策略{strat}] 完成: total={stats['total']}, full={stats['fullMatchCount']}, "
                        f"partial={stats['partialMatchCount']}, none={stats['noneMatchCount']}")

            # 步骤2.7: 组装策略结果
            all_strategy_results[strat] = {
                "code": 200,
                "message": "success",
                "data": {
                    "total": stats['total'],
                    "fullMatchCount": stats['fullMatchCount'],
                    "partialMatchCount": stats['partialMatchCount'],
                    "noneMatchCount": stats['noneMatchCount'],
                    "avgScore": stats['avgScore'],
                    "suggestion": suggestion,
                    "results": results
                }
            }

        # ==================== 步骤3: 返回所有策略的结果 ====================
        return {
            "code": 200,
            "message": "success",
            "data": {
                "strategies": all_strategy_results
            }
        }

    async def _query_plans(self, project_unit: str, start_date: str, end_date: str,
                           plan_type: str, warehouse_code: str,
                           material_codes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """从库存使用计划表查询计划数据
        
        数据来源: mt_stock_use_list_plan_two
        强制筛选: apply_way IN ('01', '05', '06')
        
        Args:
            project_unit: 项目单位（支持单位名称或工厂编码）
            start_date: 需求开始日期
            end_date: 需求结束日期
            plan_type: 计划类型（对应apply_way字段）
            warehouse_code: 仓库编码
            material_codes: 物料编码列表（可选）
        
        返回:
            计划数据列表
        """
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
                'planId': row['fd_plan_id'] or row['id'],
                'planCode': row['fd_code_use'],
                'materialCode': row['fd_material_code'],
                'materialDesc': row['fd_desc'],
                'techSpecId': row['fd_tech_spec_id'],
                'demandQty': row['fd_requisition_num'] or 0,
                'warehouseCode': row['fd_warehouse_code'],
                'unit': row['fd_unit'],
                'unitCode': row['fd_unit_code'],
                'projectName': row['fd_project_name'],
                'projectCode': row['fd_project_code'],
                'unitName': row['fd_unit_name'],
                'unitFactoryCode': row['fd_unit_factory_code'],
                'unitPrice': row['fd_unit_price'] or 0,
                'demandDate': row['fd_requisition_date'],
                'planType': row['apply_way']
            })

        conn.close()
        return plans

    def _query_stocks(self, material_codes: List[str], source_type: str, target_warehouse: str = '', tech_ids: List[str] = None) -> List[Dict[str, Any]]:
        """从库存信息表查询库存数据（按仓库+物料+技术规范书分组汇总）
        
        数据来源: w_stock_info_0808
        同时查询仓库距离信息：mt_warehouse_distance
        
        注意：同一仓库同一物料同一技术规范书可能有多条记录（不同工厂、不同库存类型），
        此处按 loc_code + material_code + tech_id 分组汇总库存数量。
        
        Args:
            material_codes: 物料编码列表
            source_type: 库存类型（如'寄售'、'自有'等）
            target_warehouse: 目标仓库编码（用于查询距离）
            tech_ids: 技术规范书ID列表（可选，用于精确匹配）
        
        返回:
            库存数据列表，每个仓库+物料+技术规范书只有一条记录
        """
        conn = self.db._get_connection()
        cur = conn.cursor()

        # 构建查询条件
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

        # 按 loc_code + material_code + tech_id 分组汇总
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
        """

        cur.execute(query, params)
        rows = cur.fetchall()

        # 查询仓库距离信息（仅当有目标仓库时）
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
                    if isinstance(distance, Decimal):
                        distance = float(distance)
                    warehouse_distances[src_wh] = distance
                logger.info(f"[AllocationService] 获取到仓库距离: {warehouse_distances}")
            except Exception as e:
                logger.warning(f"[AllocationService] 查询仓库距离失败: {e}")

        stocks = []
        for row in rows:
            loc_code = row['loc_code']
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
                'distance': warehouse_distances.get(loc_code)
            })

        conn.close()
        return stocks

    def _build_material_source_type_map(self, stocks: List[Dict[str, Any]]) -> Dict[str, str]:
        """构建物料编码到库存类型的映射
        
        用途: 快速查找物料对应的库存类型，用于后续结果填充
        
        Args:
            stocks: 库存数据列表
        
        返回:
            物料编码到库存类型的字典映射
        """
        source_type_map = {}
        for stock in stocks:
            material_code = stock.get('material_code', '')
            source_type = stock.get('source_type', '')
            if material_code and source_type:
                if material_code not in source_type_map:
                    source_type_map[material_code] = source_type
        return source_type_map

    def _extract_material_codes(self, plans: List[Dict[str, Any]]) -> List[str]:
        """从计划列表中提取去重后的物料编码列表
        
        用途: 获取所有需要查询库存的物料编码
        
        Args:
            plans: 计划数据列表
        
        返回:
            去重后的物料编码列表
        """
        return list(set([
            p.get('materialCode') or p.get('material_code', '')
            for p in plans
            if p.get('materialCode') or p.get('material_code')
        ]))

    def _extract_tech_ids(self, plans: List[Dict[str, Any]]) -> List[str]:
        """从计划列表中提取所有不重复的技术规范书ID
        
        Args:
            plans: 计划列表
        
        Returns:
            不重复的技术规范书ID列表
        """
        return list(set([
            p.get('techSpecId') or p.get('tech_spec_id', '')
            for p in plans
            if p.get('techSpecId') or p.get('tech_spec_id')
        ]))

    async def _process_plans(self, plans: List[Dict[str, Any]], filtered_stocks: List[Dict[str, Any]],
                             material_source_types: Dict[str, str], strategy: str, warehouse_code: str,
                             source_type: str = "", project_unit: str = "", 
                             demand_start_date: str = "", demand_end_date: str = "", 
                             plan_type: str = "") -> tuple:
        """批量处理所有计划并统计匹配结果
        
        处理流程:
        1. 遍历每个计划
        2. 调用_process_single_plan处理单个计划
        3. 更新统计信息（完全匹配、部分匹配、无匹配数量）
        4. 计算平均匹配分数
        
        Args:
            plans: 计划数据列表
            filtered_stocks: 库存数据列表（会被更新）
            material_source_types: 物料到库存类型的映射
            strategy: 当前执行的策略（time/cost/stock/emerg）
            warehouse_code: 仓库编码
            source_type: 库存类型
            project_unit: 项目单位
            demand_start_date: 需求开始日期
            demand_end_date: 需求结束日期
            plan_type: 计划类型
        
        返回:
            (结果列表, 统计信息字典)
        """
        all_results = []
        stats = {
            'fullMatchCount': 0,
            'partialMatchCount': 0,
            'noneMatchCount': 0,
            'totalScore': 0,
            'total': 0
        }

        for idx, plan in enumerate(plans):
            result = await self._process_single_plan(idx, plan, filtered_stocks, material_source_types, strategy, 
                                                     warehouse_code, source_type, project_unit, 
                                                     demand_start_date, demand_end_date, plan_type)

            if result:
                all_results.append(result)
                self._update_stats(stats, result)

        stats['total'] = len(all_results)
        stats['avgScore'] = round(stats['totalScore'] / stats['total'], 1) if stats['total'] > 0 else 0

        return all_results, stats

    async def _process_single_plan(self, idx: int, plan: Dict[str, Any],
                                    filtered_stocks: List[Dict[str, Any]],
                                    material_source_types: Dict[str, str], strategy: str,
                                    warehouse_code: str, source_type: str = "", 
                                    project_unit: str = "", demand_start_date: str = "", 
                                    demand_end_date: str = "", plan_type: str = "") -> Dict[str, Any]:
        """处理单个计划的调配匹配
        
        处理流程:
        1. 过滤出当前计划对应的库存（按 material_code + tech_id）
        2. 调用LLM智能体进行调配匹配
        3. 使用计划原始数据覆盖LLM返回结果，避免幻觉
        4. 填充库存类型和可用库存信息
        5. 设置状态名称
        6. 更新库存（扣除已匹配数量）
        7. 保存调配结果到数据库
        
        Args:
            idx: 计划索引（用于日志）
            plan: 单个计划数据
            filtered_stocks: 库存数据列表（会被更新）
            material_source_types: 物料到库存类型的映射
            strategy: 当前执行的策略
            warehouse_code: 仓库编码
            source_type: 库存类型
            project_unit: 项目单位
            demand_start_date: 需求开始日期
            demand_end_date: 需求结束日期
            plan_type: 计划类型
        
        返回:
            调配结果字典
        """
        plan_id = plan.get('planId') or plan.get('plan_id', f'plan_{idx}')
        material_code = plan.get('materialCode') or plan.get('material_code', '')
        tech_spec_id = plan.get('techSpecId') or ''
        demand_qty = plan.get('demandQty') or plan.get('demand_qty', 0)

        logger.info(f"[AllocationService] 处理计划[{idx}]: planId={plan_id}, "
                    f"materialCode={material_code}, techSpecId={tech_spec_id}, demandQty={demand_qty}")

        result = await self.allocation_agent.process_single_plan(
            plan=plan,
            all_stocks=filtered_stocks,
            warehouse_code=warehouse_code,
            strategy=strategy
        )

        if result:
            # 直接使用 plan 的值覆盖 LLM 返回结果，避免幻觉
            result['techSpecId'] = plan.get('techSpecId', '')
            result['materialDesc'] = plan.get('materialDesc', '')
            result['unit'] = plan.get('unit', '')
            result['unitCode'] = plan.get('unitCode', '')
            result['unitName'] = plan.get('unitName', '')
            result['projectName'] = plan.get('projectName', '')
            result['planCode'] = plan.get('planCode', '')
            result['demandDate'] = plan.get('demandDate', '')
            result['planType'] = plan.get('planType', '')
            result['warehouseCode'] = result.get('warehouseCode', '') or plan.get('warehouseCode', '')
            result['sourceType'] = result.get('sourceType', '') or plan.get('sourceType', '')
            result['unitFactoryCode'] = plan.get('unitFactoryCode', '')

            # 获取库存类型（直接用库存数据里的值，避免歧义）
            selected_warehouse_code = result.get('warehouseCode', '')
            source_type_val = self._get_source_type_from_stocks(filtered_stocks, material_code, selected_warehouse_code)
            result['sourceType'] = source_type_val

            # 判断调拨方式（本仓库/跨仓调拨）
            plan_warehouse = plan.get('warehouseCode', '')
            if plan_warehouse and selected_warehouse_code == plan_warehouse:
                result['allocationType'] = '本仓库'
            else:
                result['allocationType'] = '跨仓调拨'

            # 获取实际可用库存（必须用实际库存值覆盖 LLM 可能编造的值）
            available_stock = self._get_available_stock(filtered_stocks, material_code,
                                                        result.get('warehouseCode', ''), tech_spec_id)
            result['availableStock'] = available_stock

            # 修正 matchedQty（不能超过实际可用库存）
            original_matched_qty = result.get('matchedQty', 0) or 0
            if original_matched_qty > available_stock:
                logger.warning(f"[AllocationService] matchedQty({original_matched_qty}) > availableStock({available_stock})，LLM判断可能有误，重新走fallback")
                # 如果实际可用库存不足需求，应该重新调用fallback逻辑重新计算最佳仓库
                if available_stock < demand_qty:
                    logger.info(f"[AllocationService] 仓库{result.get('warehouseCode')}库存({available_stock})不足需求({demand_qty})，重新计算最佳仓库")
                    result = self.allocation_agent.fallback_process_single_plan(
                        plan=plan,
                        all_stocks=filtered_stocks,
                        warehouse_code=warehouse_code,
                        strategy=strategy
                    )
                    # 重新获取新仓库的库存信息
                    selected_warehouse_code = result.get('warehouseCode', '')
                    available_stock = self._get_available_stock(filtered_stocks, material_code, selected_warehouse_code, tech_spec_id)
                    result['availableStock'] = available_stock
                    original_matched_qty = result.get('matchedQty', 0) or 0
                    if original_matched_qty > available_stock:
                        result['matchedQty'] = min(original_matched_qty, available_stock)
                    # 重新计算状态（因为 available_stock 可能和 fallback 返回时的假设不同）
                    if result.get('matchedQty', 0) >= demand_qty:
                        result['status'] = 'full'
                    elif result.get('matchedQty', 0) > 0:
                        result['status'] = 'partial'
                    else:
                        result['status'] = 'none'
                    # 重新获取sourceType
                    source_type_val = self._get_source_type_from_stocks(filtered_stocks, material_code, selected_warehouse_code)
                    result['sourceType'] = source_type_val

                    # 重新判断调拨方式（本仓库/跨仓调拨）
                    plan_warehouse = plan.get('warehouseCode', '')
                    if plan_warehouse and selected_warehouse_code == plan_warehouse:
                        result['allocationType'] = '本仓库'
                    else:
                        result['allocationType'] = '跨仓调拨'

                    # 重新获取unitFactoryCode
                    result['unitFactoryCode'] = plan.get('unitFactoryCode', '')

                    # 重新获取unitPrice（从计划中获取）
                    result['unitPrice'] = plan.get('unitPrice', 0) or 0
            
            # 设置状态名称
            status = result.get('status', 'none')
            result['statusName'] = self._get_status_name(status)

            # 计算调配金额：matched_qty * unit_price（从计划中获取单价）
            matched_qty = result.get('matchedQty', 0) or 0
            unit_price = plan.get('unitPrice', 0) or 0
            result['unitPrice'] = unit_price
            result['amount'] = matched_qty * unit_price if matched_qty > 0 else 0

            self._update_stock_after_match(filtered_stocks, result, material_code)
            logger.info(f"[AllocationService] 计划[{idx}]结果: status={result.get('status')}, "
                        f"matchedQty={result.get('matchedQty')}, "
                        f"sourceWarehouse={result.get('warehouseCode')}, "
                        f"sourceType={result.get('sourceType')}, "
                        f"allocationType={result.get('allocationType')}, "
                        f"amount={result.get('amount')}")

            # 每处理完一个计划就保存一次到数据库
            self._save_allocation_result(plan, result, strategy, source_type, project_unit,
                                        demand_start_date, demand_end_date, plan_type)

        return result

    def _get_available_stock(self, stocks: List[Dict[str, Any]], material_code: str, warehouse_code: str, tech_id: str = '') -> float:
        """获取指定物料在指定仓库的可用库存

        注意：库存数据已在查询时按 loc_code + material_code + tech_id 分组汇总，
        每个仓库+物料+技术规范书只有一条记录。

        Args:
            stocks: 库存数据列表
            material_code: 物料编码
            warehouse_code: 仓库编码
            tech_id: 技术规范书ID（已在查询时过滤，此处仅做校验）

        Returns:
            可用库存数量，未找到则返回0
        """
        for stock in stocks:
            stock_material = stock.get('material_code') or stock.get('materialCode', '')
            stock_warehouse = stock.get('loc_code') or stock.get('warehouseCode', '')
            stock_tech_id = stock.get('tech_id') or ''
            if stock_material == material_code and stock_warehouse == warehouse_code:
                if not tech_id or stock_tech_id == tech_id:
                    return float(stock.get('stock_qty', 0) or 0)
        return 0.0

    def _get_source_type_from_stocks(self, stocks: List[Dict[str, Any]], material_code: str, warehouse_code: str) -> str:
        """获取指定物料在指定仓库的source_type（直接用库存数据里的值，避免歧义）
        
        注意：由于库存数据可能有多个source_type（用/分隔），这里按优先级选择一个
        
        Args:
            stocks: 库存数据列表
            material_code: 物料编码
            warehouse_code: 仓库编码
        
        返回:
            source_type值（库存/专业仓/在途/供应商等），未找到则返回空
        """
        source_type_priority = ['库存', '专业仓', '周转库', '区域库', '终端库', '在途', '供应商', '成品']
        
        for stock in stocks:
            stock_material = stock.get('material_code') or stock.get('materialCode', '')
            stock_warehouse = stock.get('loc_code') or stock.get('warehouseCode', '')
            if stock_material == material_code and stock_warehouse == warehouse_code:
                source_type = stock.get('source_type', '')
                if source_type:
                    # 如果是多个source_type拼接的，按优先级选择第一个
                    if '/' in source_type:
                        types = source_type.split('/')
                        for priority_type in source_type_priority:
                            if priority_type in types:
                                return priority_type
                        # 如果都不在优先级列表中，返回第一个
                        return types[0]
                    return source_type
                return stock.get('factory_name', '')
        return ''

    def _get_unit_price_from_stocks(self, stocks: List[Dict[str, Any]], material_code: str, warehouse_code: str) -> float:
        """获取指定物料在指定仓库的单价

        Args:
            stocks: 库存数据列表
            material_code: 物料编码
            warehouse_code: 仓库编码

        Returns:
            单价，未找到则返回0
        """
        for stock in stocks:
            stock_material = stock.get('material_code') or stock.get('materialCode', '')
            stock_warehouse = stock.get('loc_code') or stock.get('warehouseCode', '')
            if stock_material == material_code and stock_warehouse == warehouse_code:
                return float(stock.get('unit_price', 0) or 0)
        return 0.0

    def _get_status_name(self, status: str) -> str:
        """将状态编码转换为中文名称
        
        状态映射:
        - 'full': '完全匹配'
        - 'partial': '部分匹配'
        - 'none': '无匹配'
        
        Args:
            status: 状态编码
        
        返回:
            中文状态名称
        """
        status_map = {
            'full': '完全匹配',
            'partial': '部分匹配',
            'none': '无匹配'
        }
        return status_map.get(status, status)

    def _save_allocation_result(self, plan: Dict[str, Any], result: Dict[str, Any], strategy: str,
                                source_type: str = "", project_unit: str = "",
                                demand_start_date: str = "", demand_end_date: str = "",
                                plan_type: str = "") -> None:
        """保存单个计划的调配结果到数据库
        
        存储表: mt_allocation_result
        逻辑主键: fd_plan_id + fd_strategy（相同计划+策略组合会被覆盖）
        
        Args:
            plan: 原始计划数据
            result: 调配结果数据
            strategy: 当前策略
            source_type: 库存类型
            project_unit: 项目单位
            demand_start_date: 需求开始日期
            demand_end_date: 需求结束日期
            plan_type: 计划类型
        """
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            plan_id = plan.get('planId') or plan.get('plan_id', '')
            plan_code = plan.get('planCode') or plan.get('fd_code_use', '')
            material_code = plan.get('materialCode') or plan.get('material_code', '')
            material_desc = result.get('materialDesc', '') or plan.get('materialDesc', '')
            tech_spec_id = plan.get('techSpecId') or plan.get('fd_tech_spec_id', '') or result.get('techSpecId', '')
            demand_qty = plan.get('demandQty') or plan.get('demand_qty', 0)
            unit = result.get('unit', '') or plan.get('unit', '')
            unit_name = result.get('unitName', '') or plan.get('unitName', '')
            project_name = result.get('projectName', '') or plan.get('projectName', '')
            project_code = plan.get('projectCode', '')
            warehouse_code = result.get('warehouseCode', '') or result.get('sourceWarehouse', '') or plan.get('warehouseCode', '')
            warehouse_name = result.get('warehouseName', '')
            matched_qty = result.get('matchedQty', 0) or 0
            available_stock = result.get('availableStock', 0) or 0
            score = result.get('score', 0) or 0
            match_status = result.get('status', 'none')
            match_status_name = result.get('statusName', '')
            source_type_val = result.get('sourceType', '') or source_type
            reason = result.get('reason', '')
            demand_date = plan.get('demandDate', '')
            plan_type_val = plan.get('planType', '') or plan_type
            project_unit_val = plan.get('unitName', '') or project_unit
            unit_code = result.get('unitCode', '') or plan.get('unitCode', '')
            unit_factory_code = result.get('unitFactoryCode', '') or plan.get('unitFactoryCode', '')
            allocation_type = result.get('allocationType', '跨仓调拨')
            amount = result.get('amount', 0) or 0
            unit_price = result.get('unitPrice', 0) or plan.get('unitPrice', 0) or 0

            cur.execute('''
                REPLACE INTO mt_allocation_result (
                    fd_plan_id, fd_plan_code, fd_material_code, fd_material_desc,
                    fd_tech_spec_id, fd_demand_qty, fd_unit, fd_unit_code, fd_unit_name,
                    fd_project_name, fd_project_code, fd_warehouse_code, fd_warehouse_name,
                    fd_matched_qty, fd_available_stock, fd_score, fd_match_status,
                    fd_match_status_name, fd_source_type, fd_reason, fd_demand_date,
                    fd_plan_type, fd_strategy,
                    fd_project_unit, fd_demand_time,
                    fd_create_time, fd_unit_factory_code, fd_allocation_type, fd_amount,
                    fd_unit_price
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                plan_id, plan_code, material_code, material_desc,
                tech_spec_id, demand_qty, unit, unit_code, unit_name,
                project_name, project_code, warehouse_code, warehouse_name,
                matched_qty, available_stock, score, match_status,
                match_status_name, source_type_val, reason, demand_date,
                plan_type_val, strategy,
                project_unit_val, demand_date,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                unit_factory_code, allocation_type, amount, unit_price
            ))

            conn.commit()
            conn.close()

            logger.info(f"[AllocationService] 保存调配结果: planId={plan_id}, materialCode={material_code}, "
                        f"status={match_status}, allocationType={allocation_type}, "
                        f"unitFactoryCode={unit_factory_code}, amount={amount}, unitPrice={unit_price}")

        except Exception as e:
            logger.error(f"[AllocationService] 保存调配结果失败: {str(e)}")

    def _update_stock_after_match(self, filtered_stocks: List[Dict[str, Any]],
                                   result: Dict[str, Any], material_code: str):
        """根据匹配结果更新内存中的库存数据
        
        用途: 在批量处理多个计划时，扣除已匹配的库存数量，避免重复分配
        
        Args:
            filtered_stocks: 库存数据列表（会被修改）
            result: 调配结果
            material_code: 物料编码
        """
        matched_qty = result.get('matchedQty', 0) or 0
        source_warehouse = result.get('warehouseCode', '')

        if matched_qty > 0 and source_warehouse:
            for stock in filtered_stocks:
                stock_warehouse = stock.get('loc_code') or stock.get('warehouseCode') or stock.get('locCode') or ''
                stock_material = stock.get('material_code') or stock.get('materialCode') or ''

                if stock_warehouse == source_warehouse and stock_material == material_code:
                    current_qty = float(stock.get('stock_qty', 0) or 0)
                    matched_qty_float = float(matched_qty)
                    new_qty = max(0, current_qty - matched_qty_float)
                    stock['stock_qty'] = new_qty
                    logger.info(f"[AllocationService] 更新库存: warehouse={source_warehouse}, "
                                f"material={material_code}, 当前库存={current_qty} -> 新库存={new_qty}")
                    break

    def _update_stats(self, stats: Dict[str, Any], result: Dict[str, Any]):
        """根据单个计划的匹配结果更新统计信息
        
        更新字段:
        - fullMatchCount: 完全匹配数量
        - partialMatchCount: 部分匹配数量
        - noneMatchCount: 无匹配数量
        - totalScore: 累计匹配分数
        
        Args:
            stats: 统计信息字典（会被修改）
            result: 单个计划的调配结果
        """
        status = result.get('status', 'none')
        if status == 'full':
            stats['fullMatchCount'] += 1
        elif status == 'partial':
            stats['partialMatchCount'] += 1
        else:
            stats['noneMatchCount'] += 1

        score = result.get('score', 0) or 0
        stats['totalScore'] += score

    def _generate_suggestion(self, stats: Dict[str, Any], total: int) -> str:
        """根据统计信息生成业务建议文本
        
        建议规则:
        - 全部完全匹配: "{total}项完全匹配可直接审核"
        - 有部分匹配: "{full}项完全匹配可直接审核，{partial}项部分匹配建议跨仓调拨或协议补库"
        - 无匹配: "所有物料无库存，建议触发协议补库流程"
        - 有未匹配项: 追加"{none}项建议走应急采购"
        
        Args:
            stats: 统计信息字典
            total: 总计划数
        
        返回:
            建议文本
        """
        full = stats['fullMatchCount']
        partial = stats['partialMatchCount']
        none = stats['noneMatchCount']

        if full == total:
            return f"{total}项完全匹配可直接审核"
        elif full + partial > 0:
            return f"{full}项完全匹配可直接审核，{partial}项部分匹配建议跨仓调拨或协议补库"
        else:
            suggestion = "所有物料无库存，建议触发协议补库流程"

        if none > 0:
            suggestion += f"，{none}项建议走应急采购"

        return suggestion

    def _empty_result(self, suggestion: str) -> Dict[str, Any]:
        """返回空结果"""
        return {
            "code": 200,
            "message": "success",
            "data": {
                "total": 0,
                "fullMatchCount": 0,
                "partialMatchCount": 0,
                "noneMatchCount": 0,
                "avgScore": 0,
                "suggestion": suggestion,
                "results": []
            }
        }
