# -*- coding: utf-8 -*-
"""供应商匹配服务 - 业务逻辑处理层"""
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='[SVC] %(message)s')


class SupplierMatchService:
    """供应商匹配服务类 - 处理补货供应商匹配业务逻辑
    
    核心职责：
    1. 根据补货计划查询协议商库存
    2. 调用LLM智能体生成三种匹配策略（均衡、成本、配送）
    3. 支持本地计算作为LLM调用失败的兜底
    4. 保存匹配结果到数据库
    
    数据来源：
    - mt_replenishment_plan: 补货计划
    - mt_protocol_stock: 协议商库存
    - w_stock_info_0808: 物料描述补充
    - mt_base_warehouse_info: 仓库所属单位
    - mt_supplier_match_result: 匹配结果存储
    """

    def __init__(self, db, supplier_match_agent):
        self.db = db
        self.agent = supplier_match_agent

    async def match(self, input_plans: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        处理供应商匹配业务 - 对每个plan返回三种策略方案

        Args:
            input_plans: 补货计划列表，如果为None则从数据库查询

        Returns:
            处理结果字典，包含每个计划的三种策略匹配结果
        """
        logger.info(f"[SupplierMatchService] input_plans数量: {len(input_plans) if input_plans else 0}")

        # ==================== 步骤1: 获取补货计划 ====================
        # 如果没有传入plans，从数据库查询
        # 数据来源: mt_replenishment_plan
        if not input_plans or len(input_plans) == 0:
            plans = await self._query_plans()
            logger.info(f"[SupplierMatchService] 从DB查询到plans数量: {len(plans)}")
        else:
            plans = input_plans

        if not plans:
            return {
                "code": 400,
                "message": "error",
                "data": "没有找到计划"
            }

        # 存储所有计划的匹配结果
        all_results = []

        # ==================== 步骤2: 遍历处理每个计划 ====================
        for i, plan in enumerate(plans):
            logger.info(f"[SupplierMatchService] 处理计划 {i+1}/{len(plans)}")

            # 步骤2.1: 获取基础信息
            material_code = plan.get('materialCode', '')
            tech_id = plan.get('techSpecId', '')
            warehouse_code = plan.get('warehouseCode', '')

            # 步骤2.2: 获取物料描述（优先从plan，其次从库存表）
            # 数据来源: plan['fd_desc'] 或 w_stock_info_0808
            material_desc = plan.get('materialDesc', '') or plan.get('fd_desc', '')
            if not material_desc:
                material_desc = await self._get_material_desc_from_stock(material_code, tech_id)

            # 步骤2.3: 获取所属单位（从仓库信息推导）
            # 数据来源: mt_base_warehouse_info
            company = await self._get_company_from_warehouse(warehouse_code)

            # 步骤2.4: 查询协议商库存
            # 数据来源: mt_protocol_stock（必须同时使用物料编码和技术规范书ID）
            suppliers = await self._get_protocol_suppliers(plan)
            logger.info(f"[SupplierMatchService] 查询到协议商数量: {len(suppliers)}")

            # 步骤2.5: 根据是否有供应商生成结果
            if not suppliers:
                # 如果没有供应商，生成空结果
                result = self._build_empty_plan_result(plan)
                result['materialDesc'] = material_desc
                result['company'] = company
                result['projectDef'] = plan.get('projectDef', '') or ''
                result['projectDesc'] = ''
            else:
                # 调用LLM智能体生成三种策略方案
                llm_result = await self._generate_strategy_results_with_llm(plan, suppliers)
                
                # 使用原始计划数据填充核心字段，不依赖LLM返回（避免幻觉）
                result = {
                    'planId': plan.get('planId', ''),
                    'materialCode': plan.get('materialCode', ''),
                    'materialDesc': material_desc,
                    'demandQty': plan.get('demandQty', 0),
                    'warehouseCode': plan.get('warehouseCode', ''),
                    'techSpecId': plan.get('techSpecId', ''),
                    'company': company,
                    'projectDef': plan.get('projectDef', '') or '',
                    'projectDesc': '',
                    'strategies': llm_result.get('strategies', {})
                }

                # 步骤2.6: 保存匹配结果到数据库（每种策略单独保存）
                self._save_match_result(plan, 'balanced', result['strategies']['balanced'], material_desc, company)
                self._save_match_result(plan, 'cost', result['strategies']['cost'], material_desc, company)
                self._save_match_result(plan, 'delivery', result['strategies']['delivery'], material_desc, company)

            all_results.append(result)

        # ==================== 步骤3: 返回所有计划的匹配结果 ====================
        return {
            "code": 200,
            "message": "success",
            "data": {
                "total": len(all_results),
                "results": all_results
            }
        }

    async def _query_plans(self) -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            cur.execute('''
                SELECT id, fd_material_no, fd_material_desc, fd_purchase_qty,
                       fd_purchase_unit, fd_warehouse_code, fd_spec_doc_id,
                       fd_purchase_req_NO, fd_project_def
                FROM mt_replenishment_plan
                WHERE fd_deleted = 0
                ORDER BY id
                LIMIT 100
            ''')
            rows = cur.fetchall()

            plans = []
            for row in rows:
                plans.append({
                    'planId': str(row['id']),
                    'materialCode': row['fd_material_no'] or '',
                    'materialDesc': row['fd_material_desc'] or '',
                    'demandQty': row['fd_purchase_qty'] or 0,
                    'unit': row['fd_purchase_unit'] or '',
                    'warehouseCode': row['fd_warehouse_code'] or '',
                    'techSpecId': row['fd_spec_doc_id'] or '',
                    'purchaseReqNo': row['fd_purchase_req_NO'] or '',
                    'projectDef': row['fd_project_def'] or '',
                })

            return plans
        except Exception as e:
            logger.error(f"[SupplierMatchService] 查询补货计划失败: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    async def _get_protocol_suppliers(self, plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        material_code = plan.get('materialCode', '')
        tech_spec_id = plan.get('techSpecId', '')

        if not material_code or not tech_spec_id:
            logger.warning(f"[SupplierMatchService] 物料编码或技术规范书ID为空，跳过查询: material_code={material_code}, tech_spec_id={tech_spec_id}")
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
            logger.error(f"[SupplierMatchService] 查询协议供应商失败: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    async def _get_material_desc_from_stock(self, material_code: str, tech_id: str = None) -> str:
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
            logger.error(f"[SupplierMatchService] 获取物料描述失败: {str(e)}")
        finally:
            if conn:
                conn.close()

        return ''

    async def _get_company_from_warehouse(self, warehouse_code: str) -> str:
        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            cur.execute('''
                SELECT fd_warehouse_name, fd_city_code
                FROM mt_base_warehouse_info
                WHERE fd_warehouse_code = %s
            ''', (warehouse_code,))

            row = cur.fetchone()

            if row:
                city_code = row['fd_city_code'] or ''
                warehouse_name = row['fd_warehouse_name'] or ''

                if city_code:
                    return f"{city_code}区域库"
                elif warehouse_name:
                    return warehouse_name

        except Exception as e:
            logger.error(f"[SupplierMatchService] 获取所属单位失败: {str(e)}")
        finally:
            if conn:
                conn.close()

        return ''

    def _save_match_result(self, plan: Dict[str, Any], strategy_name: str, strategy_result: Dict[str, Any],
                          material_desc: str, company: str) -> None:
        """保存单个策略的匹配结果到数据库
        
        存储表: mt_supplier_match_result
        逻辑主键: fd_material_code + fd_tech_spec_id + fd_strategy（重复时覆盖）
        """
        conn = None
        try:
            conn = self.db._get_connection()
            cur = conn.cursor()

            suppliers_list = strategy_result.get('suppliers', [])
            has_suppliers = len(suppliers_list) > 0
            match_status = '成功' if has_suppliers else '失败'

            supplier_json = json.dumps(suppliers_list, ensure_ascii=False)

            first_supplier = suppliers_list[0] if suppliers_list else {}
            supplier_code = first_supplier.get('supplierCode', '') if first_supplier else ''
            supplier_name = first_supplier.get('supplierName', '') if first_supplier else ''
            allocated_qty = first_supplier.get('allocatedQty', 0) if first_supplier else 0
            unit_price = first_supplier.get('unitPrice', 0) if first_supplier else 0
            cost = first_supplier.get('cost', 0) if first_supplier else 0
            execution_rate = first_supplier.get('executionRate', 0) if first_supplier else 0
            remain_qty = first_supplier.get('remainQty', 0) if first_supplier else 0

            cur.execute('''
                REPLACE INTO mt_supplier_match_result (
                    fd_material_code, fd_material_desc, fd_match_status, fd_strategy,
                    fd_company, fd_project_def, fd_project_desc,
                    fd_demand_qty, fd_warehouse_code, fd_tech_spec_id,
                    fd_supplier_results, fd_total_cost, fd_unmet_demand,
                    fd_create_time,
                    fd_supplier_code, fd_supplier_name, fd_allocated_qty,
                    fd_unit_price, fd_cost, fd_execution_rate, fd_remain_quantity
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                plan.get('materialCode', ''),
                material_desc,
                match_status,
                strategy_name,
                company,
                plan.get('projectDef', '') or '',
                '',
                plan.get('demandQty', 0),
                plan.get('warehouseCode', ''),
                plan.get('techSpecId', ''),
                supplier_json,
                strategy_result.get('totalCost', 0),
                strategy_result.get('unmetDemand', 0),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                supplier_code,
                supplier_name,
                allocated_qty,
                unit_price,
                cost,
                execution_rate,
                remain_qty
            ))

            conn.commit()
            logger.info(f"[SupplierMatchService] 保存匹配结果: {plan.get('materialCode')} - {strategy_name}")

        except Exception as e:
            logger.error(f"[SupplierMatchService] 保存匹配结果失败: {str(e)}")
        finally:
            if conn:
                conn.close()

    def _generate_strategy_results(self, plan: Dict[str, Any], suppliers: List[Dict[str, Any]]) -> Dict[str, Any]:
        """生成三种供应商匹配策略方案
        
        调用三个策略方法生成结果：
        1. 均衡策略: 按执行比例分配，优先选择与50%差距最小的供应商
        2. 成本策略: 按单价最低选择，计算总成本最低的方案
        3. 配送策略: 优先选择能满足全部需求的单个供应商
        
        Args:
            plan: 补货计划数据
            suppliers: 协议商列表
        
        返回:
            包含三种策略结果的字典
        """
        demand_qty = plan.get('demandQty', 0)

        # 1. 均衡策略：按执行比例分配，优先选择与50%差距最小的
        balanced = self._strategy_balanced(suppliers, demand_qty)

        # 2. 成本策略：按单价最低选择，计算总成本最低的方案
        cost = self._strategy_cost(suppliers, demand_qty)

        # 3. 配送策略：优先满足单个供应商能满足的
        delivery = self._strategy_delivery(suppliers, demand_qty)

        return {
            'planId': plan.get('planId', ''),
            'materialCode': plan.get('materialCode', ''),
            'materialDesc': plan.get('materialDesc', ''),
            'demandQty': demand_qty,
            'warehouseCode': plan.get('warehouseCode', ''),
            'techSpecId': plan.get('techSpecId', ''),
            'strategies': {
                'balanced': balanced,
                'cost': cost,
                'delivery': delivery
            }
        }

    def _strategy_balanced(self, suppliers: List[Dict[str, Any]], demand_qty: float) -> Dict[str, Any]:
        """均衡策略：按执行比例分配，优先选择与50%差距最小的供应商
        
        策略逻辑:
        1. 过滤有库存的供应商
        2. 按执行比例与50%的差距排序（接近50%的优先）
        3. 按顺序分配库存，直到满足需求或库存用尽
        
        Args:
            suppliers: 协议商列表
            demand_qty: 需求数量
        
        返回:
            包含供应商分配列表、总成本、未满足需求的字典
        """
        # 先过滤有库存的供应商
        valid_suppliers = [s for s in suppliers if s['remainQty'] > 0]

        if not valid_suppliers:
            return {'suppliers': [], 'totalCost': 0, 'remark': '无可用供应商'}

        # 按执行比例排序
        valid_suppliers_sorted = sorted(valid_suppliers, key=lambda x: abs(x['executionRate'] - 50))

        allocations = []
        remaining_demand = demand_qty

        for supplier in valid_suppliers_sorted:
            if remaining_demand <= 0:
                break

            available_qty = min(supplier['remainQty'], remaining_demand)
            if available_qty > 0:
                allocations.append({
                    'supplierCode': supplier['supplierCode'],
                    'supplierName': supplier['supplierName'],
                    'allocatedQty': available_qty,
                    'unitPrice': supplier['unitPrice'],
                    'cost': available_qty * supplier['unitPrice'],
                    'executionRate': supplier['executionRate']
                })
                remaining_demand -= available_qty

        total_cost = sum(a['cost'] for a in allocations)

        return {
            'suppliers': allocations,
            'totalCost': round(total_cost, 2),
            'unmetDemand': round(remaining_demand, 2) if remaining_demand > 0 else 0,
            'remark': '优先选择执行比例接近50%的供应商'
        }

    def _strategy_cost(self, suppliers: List[Dict[str, Any]], demand_qty: float) -> Dict[str, Any]:
        """成本策略：按单价最低选择，计算总成本最低的方案
        
        策略逻辑:
        1. 过滤有库存的供应商
        2. 按单价从低到高排序
        3. 优先从单价最低的供应商开始分配，直到满足需求或库存用尽
        
        Args:
            suppliers: 协议商列表
            demand_qty: 需求数量
        
        返回:
            包含供应商分配列表、总成本、未满足需求的字典
        """
        # 先过滤有库存的供应商，按单价排序
        valid_suppliers = [s for s in suppliers if s['remainQty'] > 0]
        valid_suppliers_sorted = sorted(valid_suppliers, key=lambda x: x['unitPrice'])

        if not valid_suppliers_sorted:
            return {'suppliers': [], 'totalCost': 0, 'remark': '无可用供应商'}

        allocations = []
        remaining_demand = demand_qty

        for supplier in valid_suppliers_sorted:
            if remaining_demand <= 0:
                break

            available_qty = min(supplier['remainQty'], remaining_demand)
            if available_qty > 0:
                allocations.append({
                    'supplierCode': supplier['supplierCode'],
                    'supplierName': supplier['supplierName'],
                    'allocatedQty': available_qty,
                    'unitPrice': supplier['unitPrice'],
                    'cost': available_qty * supplier['unitPrice'],
                    'executionRate': supplier['executionRate']
                })
                remaining_demand -= available_qty

        total_cost = sum(a['cost'] for a in allocations)

        return {
            'suppliers': allocations,
            'totalCost': round(total_cost, 2),
            'unmetDemand': round(remaining_demand, 2) if remaining_demand > 0 else 0,
            'remark': '优先选择单价最低的供应商'
        }

    def _strategy_delivery(self, suppliers: List[Dict[str, Any]], demand_qty: float) -> Dict[str, Any]:
        """配送策略：优先选择能满足全部需求的单个供应商，简化配送流程
        
        策略逻辑:
        1. 过滤有库存的供应商
        2. 优先查找能满足全部需求的单个供应商（简化配送）
        3. 如果没有单个供应商能满足，则按库存数量从多到少排序分配
        
        Args:
            suppliers: 协议商列表
            demand_qty: 需求数量
        
        返回:
            包含供应商分配列表、总成本、未满足需求的字典
        """
        valid_suppliers = [s for s in suppliers if s['remainQty'] > 0]

        if not valid_suppliers:
            return {'suppliers': [], 'totalCost': 0, 'remark': '无可用供应商'}

        # 先找能满足全部需求的
        single_supplier = None
        for supplier in valid_suppliers:
            if supplier['remainQty'] >= demand_qty:
                single_supplier = supplier
                break

        if single_supplier:
            cost = demand_qty * single_supplier['unitPrice']
            return {
                'suppliers': [{
                    'supplierCode': single_supplier['supplierCode'],
                    'supplierName': single_supplier['supplierName'],
                    'allocatedQty': demand_qty,
                    'unitPrice': single_supplier['unitPrice'],
                    'cost': cost,
                    'executionRate': single_supplier['executionRate']
                }],
                'totalCost': round(cost, 2),
                'unmetDemand': 0,
                'remark': '单个供应商能满足全部需求，简化配送'
            }

        # 如果没有单个能满足的，按库存多少排序分配
        valid_suppliers_sorted = sorted(valid_suppliers, key=lambda x: -x['remainQty'])

        allocations = []
        remaining_demand = demand_qty

        for supplier in valid_suppliers_sorted:
            if remaining_demand <= 0:
                break

            available_qty = min(supplier['remainQty'], remaining_demand)
            if available_qty > 0:
                allocations.append({
                    'supplierCode': supplier['supplierCode'],
                    'supplierName': supplier['supplierName'],
                    'allocatedQty': available_qty,
                    'unitPrice': supplier['unitPrice'],
                    'cost': available_qty * supplier['unitPrice'],
                    'executionRate': supplier['executionRate']
                })
                remaining_demand -= available_qty

        total_cost = sum(a['cost'] for a in allocations)

        return {
            'suppliers': allocations,
            'totalCost': round(total_cost, 2),
            'unmetDemand': round(remaining_demand, 2) if remaining_demand > 0 else 0,
            'remark': '按库存多少排序分配，尽量减少供应商数量'
        }

    def _sanitize_strategy_result(self, result: Dict[str, Any], plan: Dict[str, Any], suppliers: List[Dict[str, Any]]) -> Dict[str, Any]:
        """修正 LLM 返回的策略结果中的异常数据（兜底处理）
        
        修正逻辑:
        1. 如果allocatedQty <= 0，设为需求数量
        2. 如果cost <= 0 但 unit_price > 0，重新计算 cost = allocatedQty * unitPrice
        3. 如果totalCost <= 0，重新计算
        
        用途: 处理LLM可能产生的幻觉数据，确保计算结果符合业务逻辑
        
        Args:
            result: LLM返回的策略结果
            plan: 原始计划数据
            suppliers: 协议商列表
        
        返回:
            修正后的策略结果
        """
        demand_qty = plan.get('demandQty', 0)

        if demand_qty <= 0:
            return result

        strategy_map = {
            'balanced': '均衡策略',
            'cost': '成本策略',
            'delivery': '配送策略'
        }

        strategies = result.get('strategies', {})
        for strategy_key, strategy_name in strategy_map.items():
            strategy_data = strategies.get(strategy_key, {})
            suppliers_list = strategy_data.get('suppliers', [])

            if not suppliers_list:
                continue

            for supp in suppliers_list:
                unit_price = supp.get('unitPrice', 0) or 0
                allocated_qty = supp.get('allocatedQty', 0) or 0
                cost = supp.get('cost', 0) or 0

                if allocated_qty <= 0:
                    allocated_qty = demand_qty
                    supp['allocatedQty'] = allocated_qty

                if unit_price > 0 and cost <= 0:
                    cost = allocated_qty * unit_price
                    supp['cost'] = round(cost, 2)

            strategy_data['suppliers'] = suppliers_list

            total_cost = sum(s.get('cost', 0) or 0 for s in suppliers_list)
            if total_cost <= 0:
                logger.info(f"total_cost不正常, 开始进行修正")
                total_cost = demand_qty * (suppliers_list[0].get('unitPrice', 0) or 0) if suppliers_list else 0

            strategy_data['totalCost'] = round(total_cost, 2)
            strategies[strategy_key] = strategy_data

            logger.info(f"[SupplierMatchService] [{strategy_name}] 修正后: totalCost={strategy_data['totalCost']}, suppliers数量={len(suppliers_list)}")

        result['strategies'] = strategies
        return result

    def _calculate_supplier_remain(self, strategy_data: Dict[str, Any], suppliers: List[Dict[str, Any]]) -> Dict[str, Any]:
        """计算供应商剩余执行比例和余量

        计算逻辑:
        1. 用需求数量 * unit_price 得到本次匹配金额
        2. 新余量 = 总金额 * (1 - 原有执行率) - 本次匹配金额
        3. 新执行率 = (总金额 - 新余量) / 总金额

        Args:
            strategy_data: 策略结果数据
            suppliers: 协议商列表

        Returns:
            计算后的策略结果数据
        """
        # 创建供应商映射表，按供应商编码查找
        supplier_map = {}
        for s in suppliers:
            supplier_code = s.get('supplierCode', '')
            if supplier_code:
                supplier_map[supplier_code] = s

        suppliers_list = strategy_data.get('suppliers', [])
        for supp in suppliers_list:
            supplier_code = supp.get('supplierCode', '')
            original_supplier = supplier_map.get(supplier_code, {})

            if not original_supplier:
                continue

            # 获取原始数据
            total_amount = original_supplier.get('totalAmount', 0)  # 总金额 fd_amount_net
            unit_price = original_supplier.get('unitPrice', 0)      # 单价 fd_price_net
            original_exec_rate = original_supplier.get('executionRate', 0)  # 原有执行率
            allocated_qty = supp.get('allocatedQty', 0)

            if total_amount <= 0 or unit_price <= 0:
                # 如果数据不完整，使用原有余量
                supp['remainQty'] = original_supplier.get('remainQty', 0)
                supp['executionRate'] = original_exec_rate
                continue

            # 计算本次匹配金额
            match_amount = allocated_qty * unit_price

            # 计算新余量：原有剩余金额(总价*(1-执行率)) - 本次匹配金额
            original_remain_amount = total_amount * (1 - original_exec_rate)
            new_remain_amount = original_remain_amount - match_amount

            # 新余量金额转回为数量（为了简化，这里按单价反算）
            new_remain_qty = max(0, new_remain_amount / unit_price) if unit_price > 0 else 0

            # 计算新执行率：(总金额 - 新余量) / 总金额
            new_exec_rate = (total_amount - new_remain_amount) / total_amount if total_amount > 0 else 0
            new_exec_rate = max(0, min(1, new_exec_rate))  # 限制在 0-1 之间

            # 更新供应商数据
            supp['remainQty'] = round(new_remain_qty, 4)
            supp['executionRate'] = round(new_exec_rate, 6)

            logger.info(f"[供应商余量计算] supplier={supplier_code}, "
                       f"总金额={total_amount}, 原执行率={original_exec_rate}, "
                       f"匹配数量={allocated_qty}, 匹配金额={match_amount}, "
                       f"新余量={new_remain_qty}, 新执行率={new_exec_rate}")

        strategy_data['suppliers'] = suppliers_list
        return strategy_data

    def _build_empty_plan_result(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """构建空的供应商匹配结果（无协议商时使用）
        
        当计划对应的物料没有可用协议商时，生成空结果
        三种策略都返回空的供应商列表
        
        Args:
            plan: 补货计划数据
        
        返回:
            空的策略结果字典
        """
        empty_strategy = {'suppliers': [], 'totalCost': 0, 'remark': '无可用协议商库存'}

        return {
            'planId': plan.get('planId', ''),
            'materialCode': plan.get('materialCode', ''),
            'materialDesc': plan.get('materialDesc', ''),
            'demandQty': plan.get('demandQty', 0),
            'warehouseCode': plan.get('warehouseCode', ''),
            'techSpecId': plan.get('techSpecId', ''),
            'strategies': {
                'balanced': empty_strategy,
                'cost': empty_strategy,
                'delivery': empty_strategy
            }
        }

    async def _generate_strategy_results_with_llm(self, plan: Dict[str, Any], suppliers: List[Dict[str, Any]]) -> Dict[str, Any]:
        """调用 LLM 智能体生成三种策略方案：均衡、成本、配送"""
        try:
            logger.info(f"[SupplierMatchService] 开始调用LLM智能体处理计划: {plan.get('planId')}")
            logger.info(f"[SupplierMatchService] 计划数据: materialCode={plan.get('materialCode')}, demandQty={plan.get('demandQty')}, techSpecId={plan.get('techSpecId')}")
            logger.info(f"[SupplierMatchService] 供应商数量: {len(suppliers)}")
            
            # 准备调用智能体的参数 - 统一使用 demandQty 和 materialDesc 字段名
            replenishment_plan = [{
                'materialCode': plan.get('materialCode', ''),
                'materialDesc': plan.get('materialDesc', ''),
                'demandQty': plan.get('demandQty', 0),
                'unitPrice': suppliers[0].get('unitPrice', 0) if suppliers else 0,
                'planId': plan.get('planId', ''),
                'warehouseCode': plan.get('warehouseCode', ''),
                'techSpecId': plan.get('techSpecId', '')
            }]
            
            # 转换供应商数据格式
            supplier_list = []
            for s in suppliers:
                supplier_list.append({
                    'supplierCode': s.get('supplierCode', ''),
                    'supplierName': s.get('supplierName', ''),
                    'materialCode': s.get('materialCode', ''),
                    'executionRate': s.get('executionRate', 0),
                    'totalAmount': s.get('totalQty', 0) * s.get('unitPrice', 0),
                    'executedAmount': 0,
                    'unitPrice': s.get('unitPrice', 0),
                    'deliveryCycleDays': 30,
                    'remainQty': s.get('remainQty', 0)
                })
            
            logger.info(f"[SupplierMatchService] 准备传递给LLM的数据: replenishment_plan={replenishment_plan}")
            
            # 调用 LLM 智能体
            llm_result = await self.agent.match(replenishment_plan, supplier_list, stream=False)
            
            if llm_result and 'response' in llm_result:
                response = llm_result['response']
                logger.info(f"[SupplierMatchService] LLM响应长度: {len(response)}")
                logger.info(f"[SupplierMatchService] LLM响应内容预览: {response}")
                
                # 使用智能体的解析方法来处理响应
                parsed_results = self.agent.parse_match_response(response)
                
                if parsed_results and len(parsed_results) > 0:
                    logger.info(f"[SupplierMatchService] 成功解析到 {len(parsed_results)} 个结果")
                    result_data = parsed_results[0]
                    logger.info(f"[SupplierMatchService] 解析后结果: demandQty={result_data.get('demandQty')}, materialCode={result_data.get('materialCode')}")

                    # 修正策略结果中的 allocatedQty 和 totalCost
                    result_data = self._sanitize_strategy_result(result_data, plan, suppliers)

                    # 计算供应商剩余执行比例和余量
                    if 'strategies' in result_data and isinstance(result_data['strategies'], dict):
                        strategies = result_data['strategies']
                        for strategy_key in ['balanced', 'cost', 'delivery']:
                            if strategy_key in strategies:
                                strategies[strategy_key] = self._calculate_supplier_remain(
                                    strategies[strategy_key], suppliers
                                )
                        result_data['strategies'] = strategies

                    # 确保结果包含必要的策略字段
                    if 'strategies' in result_data and isinstance(result_data['strategies'], dict):
                        return result_data
            
            # 如果 LLM 调用失败或返回无效结果，回退到agent的本地计算
            logger.info(f"[SupplierMatchService] LLM调用失败或返回无效，回退到agent本地计算")
            return self.agent.fallback_match(plan, suppliers)
            
        except Exception as e:
            logger.error(f"[SupplierMatchService] 调用LLM智能体失败: {str(e)}")
            # 回退到agent的本地计算
            return self.agent.fallback_match(plan, suppliers)
