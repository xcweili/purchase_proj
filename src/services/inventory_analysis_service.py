
# -*- coding: utf-8 -*-
"""库存分析服务 - 业务逻辑处理层"""
import logging
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='[SVC] %(message)s')


class InventoryAnalysisService:
    """库存分析服务类 - 处理库存分析业务逻辑
    
    核心职责：
    1. 根据库存层级和物料编码筛选仓库和物料组合
    2. 查询历史出库数据进行库存水位分析
    3. 调用LLM智能体进行智能分析
    4. 保存分析结果到数据库
    
    数据来源：
    - mt_base_warehouse_info: 仓库基础信息
    - mt_historical_outbound: 历史出库记录
    - w_stock_info_0808: 当前库存信息
    - mt_inventory_analysis_plan: 分析结果存储
    """

    def __init__(self, db, inventory_analysis_agent):
        self.db = db
        self.agent = inventory_analysis_agent

    async def analyze(self,
                     start_date: Optional[str] = None,
                     end_date: Optional[str] = None,
                     inventory_levels: Optional[List[str]] = None,
                     material_codes: Optional[List[str]] = None,
                     season_factor_weight: Optional[float] = None,
                     safety_redundancy_ratio: Optional[float] = None,
                     stream: bool = False) -> Dict[str, Any]:
        """
        处理库存分析业务 - 仓库×物料交叉后查询实际tech_id组合，只分析有数据的组合

        Args:
            start_date: 开始日期（格式：YYYYMM），用于筛选历史出库数据
            end_date: 结束日期（格式：YYYYMM），用于筛选历史出库数据
            inventory_levels: 库存层级列表，如 ['区域库', '周转库', '终端库']
            material_codes: 物料编码列表
            season_factor_weight: 季节因子权重（暂不处理）
            safety_redundancy_ratio: 安全冗余比例（暂不处理）
            stream: 是否流式输出

        Returns:
            处理结果字典
        """
        logger.info(f"[InventoryAnalysisService] 分析参数: start_date={start_date}, end_date={end_date}, "
                   f"inventory_levels={inventory_levels}, material_codes={material_codes}")
        logger.info(f"[InventoryAnalysisService] 季节因子权重: {season_factor_weight}, 安全冗余比例: {safety_redundancy_ratio}")

        # ==================== 步骤1: 根据库存层级筛选仓库 ====================
        # 数据来源: mt_base_warehouse_info
        # 如果inventory_levels为空，则查询所有仓库
        warehouse_info = await self._get_warehouse_info_by_levels(inventory_levels)
        warehouse_codes = list(warehouse_info.keys())
        logger.info(f"[InventoryAnalysisService] 筛选出 {len(warehouse_codes)} 个仓库")

        if not warehouse_codes:
            return {
                "code": 200,
                "message": "success",
                "data": {
                    "total": 0,
                    "results": []
                }
            }

        # ==================== 步骤2: 获取物料编码列表 ====================
        # 数据来源: mt_historical_outbound（如果material_codes为空）
        # 或使用用户传入的物料编码列表
        if not material_codes or len(material_codes) == 0:
            material_codes = await self._get_all_material_codes()
        logger.info(f"[InventoryAnalysisService] 物料编码数量: {len(material_codes)}")

        if not material_codes:
            return {
                "code": 200,
                "message": "success",
                "data": {
                    "total": 0,
                    "results": []
                }
            }

        # ==================== 步骤3: 构建仓库×物料×tech_id组合 ====================
        # 目的: 只分析有历史出库数据的组合，避免无效分析
        # 数据来源: mt_historical_outbound（通过_tech_ids_by_warehouse_material查询）
        all_combinations = []
        for warehouse_code in warehouse_codes:
            for material_code in material_codes:
                # 查询该仓库+物料组合有哪些tech_id
                tech_ids = await self._get_tech_ids_by_warehouse_material(
                    warehouse_code, material_code, start_date, end_date
                )

                # 如果没有tech_id，直接跳过这个组合（不添加到分析列表）
                if not tech_ids:
                    continue

                for tech_id in tech_ids:
                    all_combinations.append({
                        'warehouse_code': warehouse_code,
                        'material_code': material_code,
                        'tech_id': tech_id
                    })

        logger.info(f"[InventoryAnalysisService] 实际需要分析的组合数量: {len(all_combinations)}")

        if not all_combinations:
            return {
                "code": 200,
                "message": "success",
                "data": {
                    "total": 0,
                    "results": []
                }
            }

        # ==================== 步骤4: 逐一分析每个组合 ====================
        all_results = []
        cached_count = 0
        analyzed_count = 0

        for combo in all_combinations:
            warehouse_code = combo['warehouse_code']
            material_code = combo['material_code']
            tech_id = combo['tech_id']

            # 如果tech_id为空，跳过不分析
            if not tech_id:
                continue

            warehouse_name = warehouse_info.get(warehouse_code, {}).get('name', '')
            inventory_level = warehouse_info.get(warehouse_code, {}).get('level', '')

            logger.info(f"[InventoryAnalysisService] 分析组合: {warehouse_code} + {material_code} + {tech_id}")

            # 步骤4.1: 先检查缓存（数据库中是否已有该组合的分析结果）
            # 数据来源: mt_inventory_analysis_plan
            existing_result = await self._get_existing_analysis_result(warehouse_code, material_code, tech_id, start_date, end_date)

            if existing_result:
                logger.info(f"[InventoryAnalysisService] 使用缓存结果: {warehouse_code} + {material_code} + {tech_id}")
                all_results.append(existing_result)
                cached_count += 1
                continue

            # 步骤4.2: 查询当前库存和历史出库数据
            # 数据来源: w_stock_info_0808（当前库存）
            # 数据来源: mt_historical_outbound（历史出库）
            current_stock_data = await self._get_current_stock(warehouse_code, material_code, tech_id)
            outbound_data = await self._get_outbound_data(warehouse_code, material_code, tech_id, start_date, end_date)

            # 如果没有出库数据，生成空结果
            if not outbound_data:
                all_results.append(self._build_empty_result(warehouse_code, warehouse_name, material_code, tech_id, inventory_level, start_date, end_date))
                continue

            # 步骤4.3: 调用LLM智能体进行分析
            # 输入数据: 当前库存(current_stock_data) + 历史出库(outbound_data)
            result = await self.agent.analyze(
                warehouse_code=warehouse_code,
                warehouse_name=warehouse_name,
                material_code=material_code,
                tech_id=tech_id,
                data1=current_stock_data if current_stock_data else [{
                    "warehouse_code": warehouse_code,
                    "warehouse_name": warehouse_name,
                    "material_code": material_code,
                    "material_desc": "",
                    "tech_id": tech_id,
                    "current_stock": 0,
                    "in_transit_stock": 0
                }],
                data2=outbound_data,
                stream=stream
            )

            if stream:
                return result
            else:
                # 步骤4.4: 解析LLM响应
                parsed = self.agent.parse_analysis_response(result.get('response', ''))
                logger.info(f"[InventoryAnalysisService] LLM响应预览: {str(parsed) if parsed else '解析失败'}")

                if parsed:
                    # 从 current_stock_data 中提取在途数量和物料描述
                    in_transit_qty = 0
                    material_desc = parsed[0].get('materialDesc', '') or parsed[0].get('material_desc', '') or ''
                    if current_stock_data and len(current_stock_data) > 0:
                        in_transit_qty = current_stock_data[0].get('in_transit_stock', 0) or 0
                        # 如果 LLM 返回的物料描述为空，尝试从库存数据中获取
                        if not material_desc:
                            material_desc = current_stock_data[0].get('material_desc', '') or ''

                    # 获取仓库位置信息
                    warehouse_location = await self._get_warehouse_location(warehouse_code)

                    # 将物料描述设置到 parsed_item 中，以便后续使用
                    parsed[0]['materialDesc'] = material_desc

                    # 构建统一格式的结果
                    item = self._build_result_item(
                        parsed[0], warehouse_code, warehouse_name, material_code, tech_id,
                        inventory_level, in_transit_qty, warehouse_location, start_date, end_date
                    )
                else:
                    # LLM解析失败，使用agent的fallback方法进行本地计算
                    logger.info(f"[InventoryAnalysisService] LLM解析失败，使用本地兜底计算")
                    fallback_result = self.agent.fallback_analyze(
                        warehouse_code=warehouse_code,
                        warehouse_name=warehouse_name,
                        material_code=material_code,
                        tech_id=tech_id,
                        current_stock=current_stock_data if current_stock_data else [],
                        historical_outbound=outbound_data if outbound_data else []
                    )
                    if fallback_result and len(fallback_result) > 0:
                        fallback_item = fallback_result[0]
                        item = self._build_result_item(
                            fallback_item, warehouse_code, warehouse_name, material_code, tech_id,
                            inventory_level, 0, await self._get_warehouse_location(warehouse_code), start_date, end_date
                        )
                    else:
                        item = self._build_empty_result(warehouse_code, warehouse_name, material_code, tech_id, inventory_level, start_date, end_date)

                # 步骤4.5: 保存到数据库
                self._save_analysis_result(item, start_date, end_date)

                all_results.append(item)
                analyzed_count += 1

        logger.info(f"[InventoryAnalysisService] 分析完成: 缓存命中 {cached_count} 个，新增分析 {analyzed_count} 个")

        return {
            "code": 200,
            "message": "success",
            "data": {
                "total": len(all_results),
                "results": all_results
            }
        }

    async def _get_warehouse_location(self, warehouse_code: str) -> str:
        """获取仓库位置信息（如"长沙区域库"）
        
        数据来源: mt_base_warehouse_info
        查询字段: fd_warehouse_name, fd_address, fd_city_code
        
        返回:
            仓库位置字符串，优先使用城市代码+区域库格式
        """
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            cur.execute('''
                SELECT fd_warehouse_name, fd_address, fd_city_code
                FROM mt_base_warehouse_info
                WHERE fd_warehouse_code = %s
            ''', (warehouse_code,))

            row = cur.fetchone()
            conn.close()

            if row:
                # 构建仓库位置信息：优先使用城市+仓库名称
                city_code = row['fd_city_code'] or ''
                warehouse_name = row['fd_warehouse_name'] or ''

                # 如果有城市代码，拼接成"XX区域库"格式
                if city_code and '区域' not in warehouse_name:
                    return f"{city_code}区域库"
                elif warehouse_name:
                    return warehouse_name

            return ''

        except Exception as e:
            logger.error(f"[InventoryAnalysisService] 获取仓库位置失败: {str(e)}")
            return ''

    async def _get_in_transit_quantity(self, warehouse_code: str, material_code: str) -> float:
        """获取物料的在途数量
        
        数据来源: w_stock_info_0808
        筛选条件: source_type='在途'
        
        Args:
            warehouse_code: 仓库编码
            material_code: 物料编码
        
        返回:
            在途数量总和
        """
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            cur.execute('''
                SELECT SUM(stock_qty) as in_transit_qty
                FROM w_stock_info_0808
                WHERE loc_code = %s
                  AND material_code = %s
                  AND source_type = '在途'
            ''', (warehouse_code, material_code))

            row = cur.fetchone()
            conn.close()

            if row and row['in_transit_qty']:
                return float(row['in_transit_qty'])

            return 0.0

        except Exception as e:
            logger.error(f"[InventoryAnalysisService] 获取在途数量失败: {str(e)}")
            return 0.0

    def _calculate_water_level(self, current_stock: float, replenish_level: float) -> float:
        """计算当前库存水位
        
        水位公式: 当前库存 / 补库线
        
        Args:
            current_stock: 当前库存数量
            replenish_level: 补库线数量
        
        返回:
            水位值（保留2位小数），如果补库线为0则返回0
        """
        if not replenish_level or replenish_level == 0:
            return 0.0
        return round(current_stock / replenish_level, 2)

    def _determine_stock_status(self, water_level: float, current_stock: float,
                                 replenish_level: float, emergency_line: float) -> str:
        """判断库存状态（高水位/中水位/低水位）
        
        判断逻辑:
        - 低水位: 当前库存 <= 补库线
        - 中水位: 当前库存 > 补库线 且 <= 高位线（高位线=补库线*1.5）
        - 高水位: 当前库存 > 高位线
        
        Args:
            water_level: 当前水位值
            current_stock: 当前库存数量
            replenish_level: 补库线数量
            emergency_line: 紧急线数量（暂未使用）
        
        返回:
            库存状态字符串: "高水位" / "中水位" / "低水位"
        """
        # 如果库存 <= 补库线，为低水位
        if not replenish_level or current_stock <= replenish_level:
            return "低水位"

        # 如果库存 > 补库线 且 库存 <= 高位线，为中水位
        high_level = replenish_level * 1.5  # 高位线默认为补库线的1.5倍
        if current_stock > replenish_level and current_stock <= high_level:
            return "中水位"

        # 如果库存 > 高位线，为高水位
        if current_stock > high_level:
            return "高水位"

        return "低水位"

    def _generate_suggested_action(self, stock_status: str, water_level: float,
                                   current_stock: float, replenish_level: float,
                                   emergency_line: float) -> str:
        """根据库存状态生成建议操作
        
        建议规则:
        - 高水位 → 正常
        - 中水位 → 建议补库
        - 低水位 → 立即补库
        
        Args:
            stock_status: 库存状态（高水位/中水位/低水位）
            water_level: 当前水位值（暂未使用）
            current_stock: 当前库存数量（暂未使用）
            replenish_level: 补库线数量（暂未使用）
            emergency_line: 紧急线数量（暂未使用）
        
        返回:
            建议操作字符串: "正常" / "建议补库" / "立即补库"
        """
        # 高水位 → 正常
        if stock_status == "高水位":
            return "正常"

        # 中水位 → 建议补库
        if stock_status == "中水位":
            return "建议补库"

        # 低水位 → 立即补库
        if stock_status == "低水位":
            return "立即补库"

        # 默认返回正常
        return "正常"

    async def _get_existing_analysis_result(self, warehouse_code: str, material_code: str, tech_id: str,
                                            start_date: str = None, end_date: str = None) -> Optional[Dict[str, Any]]:
        """查询数据库中已存在的分析结果（用于缓存优化）
        
        数据来源: mt_inventory_analysis_plan
        查询条件: fd_warehouse_code + fd_material_code + fd_tech_id + 时间范围
        
        Args:
            warehouse_code: 仓库编码
            material_code: 物料编码
            tech_id: 技术规范书ID
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
        
        返回:
            已存在的分析结果字典，不存在则返回None
        """
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            query = '''SELECT * FROM mt_inventory_analysis_plan
                       WHERE fd_warehouse_code = %s
                         AND fd_material_code = %s
                         AND fd_tech_id = %s'''

            params = [warehouse_code, material_code, tech_id]

            if start_date and len(start_date) == 6:
                start_date_for_query = f"{start_date[:4]}-{start_date[4:]}-01"
                query += " AND fd_start_date = %s"
                params.append(start_date_for_query)

            if end_date and len(end_date) == 6:
                end_date_for_query = f"{end_date[:4]}-{end_date[4:]}-01"
                query += " AND fd_end_date = %s"
                params.append(end_date_for_query)

            cur.execute(query, params)
            row = cur.fetchone()
            conn.close()

            if row:
                return {
                    "startDate": row['fd_start_date'],
                    "endDate": row['fd_end_date'],
                    "matchType": row['fd_match_type'] or '',
                    "unit": row['fd_unit'] or '',
                    "projectDescription": row['fd_project_description'] or '',
                    "purchaseRequestNo": row['fd_purchase_request_no'] or '',
                    "purchaseRequestItemNo": row['fd_purchase_request_item_no'] or '',
                    "identifier": row['fd_identifier'] or '',
                    "deliveryLocation": row['fd_delivery_location'] or '',
                    "materialCode": row['fd_material_code'],
                    "techSpecId": row['fd_tech_id'],
                    "materialDesc": row['fd_material_desc'] or '',
                    "purchaseRequestQty": row['fd_purchase_request_qty'] if row['fd_purchase_request_qty'] is not None else '',
                    "purchaseRequestUnit": row['fd_purchase_request_unit'] or '',
                    "wbsElement": row['fd_wbs_element'] or '',
                    "purchaseRequestPrice": row['fd_purchase_request_price'] if row['fd_purchase_request_price'] is not None else '',
                    "projectDefinition": row['fd_project_definition'] or '',
                    "batch": row['fd_batch'] or '',
                    "warehouseName": row['fd_warehouse_name'] or '',
                    "warehouseLocation": row['fd_warehouse_location'] or '',
                    "highLevel": row['fd_high_level'] if row['fd_high_level'] is not None else '',
                    "replenishLevel": row['fd_replenish_level'] if row['fd_replenish_level'] is not None else '',
                    "inventoryLevel": row['fd_inventory_level'] or '',
                    "currentStock": row['fd_current_stock'] if row['fd_current_stock'] is not None else 0,
                    "emergencyLine": row['fd_emergency_line'] if row['fd_emergency_line'] is not None else '',
                    "currentWaterLevel": row['fd_current_water_level'] if row['fd_current_water_level'] is not None else '',
                    "inTransitQty": row['fd_in_transit_qty'] if row['fd_in_transit_qty'] is not None else 0,
                    "stockStatus": row['fd_stock_status'] or '',
                    "suggestedAction": row['fd_suggested_action'] or ''
                }

        except Exception as e:
            logger.error(f"[InventoryAnalysisService] 查询已存在分析结果失败: {str(e)}")

        return None

    def _save_analysis_result(self, item: Dict[str, Any], start_date: str = None, end_date: str = None) -> bool:
        """保存分析结果到数据库
        
        存储表: mt_inventory_analysis_plan
        逻辑主键: fd_warehouse_code + fd_material_code + fd_tech_id
        策略: 使用REPLACE INTO，相同主键组合直接覆盖（支持更新）
        
        Args:
            item: 分析结果字典
            start_date: 开始日期（可选，用于记录分析时间范围）
            end_date: 结束日期（可选，用于记录分析时间范围）
        
        返回:
            保存成功返回True，失败返回False
        """
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 转换 inventory_level 为中文
            def convert_level(level_code):
                if not level_code:
                    return ''
                code_str = str(level_code).strip().lstrip('0')
                level_map = {
                    '1': '区域库',
                    '2': '周转库',
                    '3': '终端库'
                }
                return level_map.get(code_str, level_code)

            def format_date_for_db(date_str):
                if not date_str:
                    return ''
                if len(date_str) == 6:
                    return f"{date_str[:4]}-{date_str[4:]}-01"
                return date_str

            def to_decimal(val):
                if val is None or val == '':
                    return 0
                try:
                    return float(val)
                except:
                    return 0

            inventory_level = convert_level(item.get('inventoryLevel', ''))

            cur.execute('''
                INSERT INTO mt_inventory_analysis_plan (
                    fd_id, fd_warehouse_code, fd_material_code, fd_tech_id,
                    fd_start_date, fd_end_date,
                    fd_match_type, fd_identifier, fd_material_desc,
                    fd_purchase_request_no, fd_purchase_request_item_no, fd_purchase_request_qty, fd_purchase_request_unit,
                    fd_project_description, fd_project_definition, fd_wbs_element, fd_batch,
                    fd_warehouse_name, fd_delivery_location, fd_inventory_level,
                    fd_high_level, fd_replenish_level, fd_emergency_line,
                    fd_current_stock,
                    fd_unit, fd_purchase_request_price,
                    fd_warehouse_location, fd_current_water_level, fd_in_transit_qty,
                    fd_stock_status, fd_suggested_action,
                    fd_create_time, fd_update_time
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    fd_start_date = VALUES(fd_start_date),
                    fd_end_date = VALUES(fd_end_date),
                    fd_match_type = VALUES(fd_match_type),
                    fd_identifier = VALUES(fd_identifier),
                    fd_material_desc = VALUES(fd_material_desc),
                    fd_purchase_request_no = VALUES(fd_purchase_request_no),
                    fd_purchase_request_item_no = VALUES(fd_purchase_request_item_no),
                    fd_purchase_request_qty = VALUES(fd_purchase_request_qty),
                    fd_purchase_request_unit = VALUES(fd_purchase_request_unit),
                    fd_project_description = VALUES(fd_project_description),
                    fd_project_definition = VALUES(fd_project_definition),
                    fd_wbs_element = VALUES(fd_wbs_element),
                    fd_batch = VALUES(fd_batch),
                    fd_warehouse_name = VALUES(fd_warehouse_name),
                    fd_delivery_location = VALUES(fd_delivery_location),
                    fd_inventory_level = VALUES(fd_inventory_level),
                    fd_high_level = VALUES(fd_high_level),
                    fd_replenish_level = VALUES(fd_replenish_level),
                    fd_emergency_line = VALUES(fd_emergency_line),
                    fd_current_stock = VALUES(fd_current_stock),
                    fd_unit = VALUES(fd_unit),
                    fd_purchase_request_price = VALUES(fd_purchase_request_price),
                    fd_warehouse_location = VALUES(fd_warehouse_location),
                    fd_current_water_level = VALUES(fd_current_water_level),
                    fd_in_transit_qty = VALUES(fd_in_transit_qty),
                    fd_stock_status = VALUES(fd_stock_status),
                    fd_suggested_action = VALUES(fd_suggested_action),
                    fd_update_time = VALUES(fd_update_time)
            ''', (
                str(uuid.uuid4()),
                item.get('warehouseCode', ''),
                item.get('materialCode', ''),
                item.get('techSpecId', ''),
                format_date_for_db(start_date) if start_date else '',
                format_date_for_db(end_date) if end_date else '',
                item.get('matchType', ''),
                item.get('identifier', ''),
                item.get('materialDesc', ''),
                item.get('purchaseRequestNo', ''),
                item.get('purchaseRequestItemNo', ''),
                item.get('purchaseRequestQty', 0) or 0,
                item.get('purchaseRequestUnit', ''),
                item.get('projectDescription', ''),
                item.get('projectDefinition', ''),
                item.get('wbsElement', ''),
                item.get('batch', ''),
                item.get('warehouseName', ''),
                item.get('deliveryLocation', ''),
                inventory_level,
                to_decimal(item.get('highLevel')),
                to_decimal(item.get('replenishLevel')),
                to_decimal(item.get('emergencyLine')),
                to_decimal(item.get('currentStock')),
                item.get('unit', ''),
                to_decimal(item.get('purchaseRequestPrice')),
                item.get('warehouseLocation', ''),
                to_decimal(item.get('currentWaterLevel')),
                to_decimal(item.get('inTransitQty')),
                item.get('stockStatus', ''),
                item.get('suggestedAction', ''),
                now,
                now
            ))

            conn.commit()
            conn.close()

            logger.info(f"[InventoryAnalysisService] 保存分析结果成功: {item.get('materialCode')}+{item.get('techSpecId')}")
            return True

        except Exception as e:
            logger.error(f"[InventoryAnalysisService] 保存分析结果失败: {str(e)}")
            return False

    def _build_result_item(self, parsed_item: Dict[str, Any], warehouse_code: str, warehouse_name: str,
                          material_code: str, tech_id: str, inventory_level: str,
                          in_transit_qty: float = 0, warehouse_location: str = '',
                          start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """构建统一格式的库存分析输出结果
        
        处理流程:
        1. 从LLM解析结果中提取关键数值（当前库存、补库线、紧急线）
        2. 计算库存水位
        3. 判断库存状态
        4. 生成建议操作
        5. 组装统一格式的输出字典
        
        Args:
            parsed_item: LLM解析后的分析结果
            warehouse_code: 仓库编码
            warehouse_name: 仓库名称
            material_code: 物料编码
            tech_id: 技术规范书ID
            inventory_level: 库存层级（区域库/周转库/终端库）
            in_transit_qty: 在途数量
            warehouse_location: 仓库位置
            start_date: 分析开始日期
            end_date: 分析结束日期
        
        返回:
            格式化的分析结果字典
        """

        # 提取关键数值
        current_stock = float(parsed_item.get('currentStock', 0) or 0)
        in_transit_stock = float(parsed_item.get('inTransitStock', 0) or parsed_item.get('in_transit_stock', 0) or 0)
        available_stock = float(parsed_item.get('availableStock', 0) or 0)
        replenish_level = float(parsed_item.get('replenishLevel', '') or parsed_item.get('reorderLevel', '') or 0)
        emergency_line = float(parsed_item.get('emergencyLine', '') or 0)

        # 如果 LLM 没有返回 available_stock，则计算得出
        if available_stock == 0:
            available_stock = current_stock + in_transit_stock

        # 如果 LLM 没有返回 in_transit_stock，但参数传入了，使用参数的值
        if in_transit_stock == 0 and in_transit_qty > 0:
            in_transit_stock = in_transit_qty
            available_stock = current_stock + in_transit_qty

        # 计算水位
        current_water_level = self._calculate_water_level(available_stock, replenish_level)

        # 判断库存状态
        stock_status = self._determine_stock_status(current_water_level, available_stock,
                                                     replenish_level, emergency_line)

        # 生成建议操作
        suggested_action = self._generate_suggested_action(stock_status, current_water_level,
                                                           available_stock, replenish_level, emergency_line)

        return {
            "startDate": start_date,
            "endDate": end_date,
            "matchType": "",
            "unit": parsed_item.get('unit', '') or '',
            "projectDescription": "",
            "purchaseRequestNo": "",
            "purchaseRequestItemNo": "",
            "identifier": "",
            "deliveryLocation": warehouse_name,
            "materialCode": material_code,
            "techSpecId": tech_id,
            "warehouseCode": warehouse_code,
            "materialDesc": parsed_item.get('materialDesc', '') or parsed_item.get('material_desc', '') or '',
            "purchaseRequestQty": parsed_item.get('purchaseQty', '') or parsed_item.get('requiredQty', '') or '',
            "purchaseRequestUnit": parsed_item.get('unit', '') or '',
            "wbsElement": "",
            "purchaseRequestPrice": "",
            "projectDefinition": "",
            "batch": "",
            "warehouseName": warehouse_name,
            "warehouseLocation": warehouse_location,
            "highLevel": parsed_item.get('highLevel', '') or '',
            "replenishLevel": parsed_item.get('replenishLevel', '') or parsed_item.get('reorderLevel', '') or '',
            "inventoryLevel": inventory_level,
            "currentStock": current_stock,
            "inTransitStock": in_transit_stock,
            "availableStock": available_stock,
            "emergencyLine": parsed_item.get('emergencyLine', '') or '',
            "currentWaterLevel": current_water_level,
            "inTransitQty": in_transit_stock,
            "stockStatus": stock_status,
            "suggestedAction": suggested_action
        }

    def _build_empty_result(self, warehouse_code: str, warehouse_name: str, material_code: str,
                           tech_id: str, inventory_level: str,
                           start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """构建空分析结果（无历史出库数据时使用）
        
        当某仓库+物料+tech_id组合没有历史出库数据时，生成空结果
        库存状态设为"无数据"，建议操作为"待分析"
        
        Args:
            warehouse_code: 仓库编码
            warehouse_name: 仓库名称
            material_code: 物料编码
            tech_id: 技术规范书ID
            inventory_level: 库存层级
            start_date: 分析开始日期
            end_date: 分析结束日期
        
        返回:
            空的分析结果字典
        """
        return {
            "startDate": start_date,
            "endDate": end_date,
            "matchType": "",
            "unit": "",
            "projectDescription": "",
            "purchaseRequestNo": "",
            "purchaseRequestItemNo": "",
            "identifier": "",
            "deliveryLocation": warehouse_name,
            "materialCode": material_code,
            "techSpecId": tech_id,
            "warehouseCode": warehouse_code,
            "materialDesc": "",
            "purchaseRequestQty": "",
            "purchaseRequestUnit": "",
            "wbsElement": "",
            "purchaseRequestPrice": "",
            "projectDefinition": "",
            "batch": "",
            "warehouseName": warehouse_name,
            "warehouseLocation": "",
            "highLevel": "",
            "replenishLevel": "",
            "inventoryLevel": inventory_level,
            "currentStock": 0,
            "emergencyLine": "",
            "currentWaterLevel": 0,
            "inTransitQty": 0,
            "stockStatus": "无数据",
            "suggestedAction": "待分析"
        }

    async def _get_tech_ids_by_warehouse_material(self, warehouse_code: str, material_code: str,
                                                   start_date: str = None, end_date: str = None) -> List[str]:
        """查询仓库+物料组合在历史出库表中有哪些技术规范书ID
        
        数据来源: mt_historical_outbound
        目的: 只分析有历史出库数据的仓库+物料+tech_id组合，避免无效分析
        
        Args:
            warehouse_code: 仓库编码
            material_code: 物料编码
            start_date: 开始月份（可选，格式YYYYMM）
            end_date: 结束月份（可选，格式YYYYMM）
        
        返回:
            tech_id列表
        """
        tech_ids = []
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            # 转换日期格式：从 YYYYMM 转换为 YYYY-MM
            start_month = None
            end_month = None
            if start_date and len(start_date) == 6:
                start_month = f"{start_date[:4]}-{start_date[4:]}"
            if end_date and len(end_date) == 6:
                end_month = f"{end_date[:4]}-{end_date[4:]}"

            query = '''
                SELECT DISTINCT fd_tech_id
                FROM mt_historical_outbound
                WHERE fd_warehouse_code = %s
                  AND fd_material_code = %s
            '''
            params = [warehouse_code, str(material_code)]

            if start_month:
                query += " AND fd_posting_month >= %s"
                params.append(start_month)

            if end_month:
                query += " AND fd_posting_month <= %s"
                params.append(end_month)

            cur.execute(query, params)
            rows = cur.fetchall()

            for row in rows:
                tech_id = row['fd_tech_id']
                if tech_id:
                    tech_ids.append(tech_id)

            conn.close()

        except Exception as e:
            logger.error(f"[InventoryAnalysisService] 查询tech_id失败: {str(e)}")

        return tech_ids

    async def _get_all_material_codes(self) -> List[str]:
        """从历史出库表中获取所有物料编码
        
        数据来源: mt_historical_outbound
        用途: 当用户未指定物料编码时，获取全量物料列表
        
        返回:
            物料编码列表
        """
        material_codes = []
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            cur.execute("SELECT DISTINCT fd_material_code FROM mt_historical_outbound")
            rows = cur.fetchall()

            for row in rows:
                if row['fd_material_code']:
                    material_codes.append(str(row['fd_material_code']))

            conn.close()

        except Exception as e:
            logger.error(f"[InventoryAnalysisService] 获取所有物料编码失败: {str(e)}")

        return material_codes

    async def _get_current_stock(self, warehouse_code: str, material_code: str, tech_id: str) -> List[Dict[str, Any]]:
        """获取指定仓库+物料+tech_id的当前库存数据
        
        数据来源: w_stock_info_0808
        查询字段: 仓库信息、物料信息、当前库存、在途库存
        
        Args:
            warehouse_code: 仓库编码
            material_code: 物料编码
            tech_id: 技术规范书ID
        
        返回:
            库存数据列表，包含当前库存和在途库存
        """
        stocks = []
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

            conn.close()

        except Exception as e:
            logger.error(f"[InventoryAnalysisService] 获取当前库存失败: {str(e)}")

        return stocks

    async def _get_outbound_data(self, warehouse_code: str, material_code: str, tech_id: str,
                                 start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """获取指定仓库+物料+tech_id的历史出库数据
        
        数据来源: mt_historical_outbound
        查询字段: 过账月份、出库数量、出库次数
        
        Args:
            warehouse_code: 仓库编码
            material_code: 物料编码
            tech_id: 技术规范书ID
            start_date: 开始月份（可选，格式YYYYMM）
            end_date: 结束月份（可选，格式YYYYMM）
        
        返回:
            历史出库数据列表，按过账月份降序排列
        """
        outbound_data = []
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            # 转换日期格式：从 YYYYMM 转换为 YYYY-MM
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

            conn.close()

        except Exception as e:
            logger.error(f"[InventoryAnalysisService] 获取历史出库数据失败: {str(e)}")

        return outbound_data

    async def _get_warehouse_info_by_levels(self, inventory_levels: Optional[List[str]] = None) -> Dict[str, Dict[str, str]]:
        """根据库存层级获取仓库信息"""
        warehouse_info = {}
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            query = '''
                SELECT fd_warehouse_code, fd_warehouse_name, fd_stock_level
                FROM mt_base_warehouse_info
                WHERE 1=1
            '''
            params = []

            if inventory_levels and len(inventory_levels) > 0:
                placeholders = ','.join(['%s' for _ in inventory_levels])
                query += f" AND fd_stock_level IN ({placeholders})"
                params.extend(inventory_levels)

            cur.execute(query, params)
            rows = cur.fetchall()

            for row in rows:
                warehouse_code = row['fd_warehouse_code']
                if warehouse_code:
                    warehouse_info[warehouse_code] = {
                        'name': row['fd_warehouse_name'] or '',
                        'level': row['fd_stock_level'] or ''
                    }

            conn.close()

        except Exception as e:
            logger.error(f"[InventoryAnalysisService] 获取仓库信息失败: {str(e)}")

        return warehouse_info

    def _get_last_n_month_qty(self, index: int, sorted_rows: List[Dict[str, Any]], n: int) -> float:
        try:
            if index - n >= 0:
                return sorted_rows[index - n]['outbound_qty']
            else:
                return 0.0
        except:
            return 0.0

    def _calculate_historical_avg(self, index: int, sorted_rows: List[Dict[str, Any]]) -> float:
        try:
            if index == 0:
                return 0.0

            historical_data = sorted_rows[:index]
            if not historical_data:
                return 0.0

            total_qty = sum(row['outbound_qty'] for row in historical_data)
            return round(total_qty / len(historical_data), 2)
        except:
            return 0.0
