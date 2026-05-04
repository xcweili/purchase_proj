# -*- coding: utf-8 -*-
"""供应商匹配服务 - 流式版本"""
import json
from typing import List, Dict, Any, Optional
from datetime import datetime

from ..agents.supplier_match_stream_agent import SupplierMatchStreamAgent


class SupplierMatchStreamService:
    """供应商匹配服务 - 流式版本"""
    
    def __init__(self, db, llm_stream_func):
        self.db = db
        self.agent = SupplierMatchStreamAgent(llm_stream_func)
    
    def _query_plans_for_stream(self, input_plans: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """获取计划数据（用于流式接口）"""
        if input_plans and len(input_plans) > 0:
            return input_plans
        
        # 如果没有输入计划，从数据库查询
        conn = self.db._get_connection()
        conn.row_factory = lambda cursor, row: dict(zip([col[0] for col in cursor.description], row))
        cur = conn.cursor()
        
        query = '''
            SELECT 
                fd_plan_id as planId,
                fd_material_code as materialCode,
                fd_material_desc as materialDesc,
                fd_demand_qty as demandQty,
                fd_unit_price as unitPrice,
                fd_warehouse_code as warehouseCode,
                fd_tech_spec_id as techSpecId,
                fd_project_name as projectName
            FROM mt_supplier_plan
            ORDER BY fd_create_time DESC LIMIT 20
        '''
        
        cur.execute(query)
        rows = cur.fetchall()
        conn.close()
        
        return rows
    
    def _query_suppliers_for_stream(self, material_codes: List[str]) -> List[Dict[str, Any]]:
        """查询供应商数据（用于流式接口）"""
        conn = self.db._get_connection()
        conn.row_factory = lambda cursor, row: dict(zip([col[0] for col in cursor.description], row))
        cur = conn.cursor()
        
        if material_codes and len(material_codes) > 0:
            placeholders = ','.join(['%s'] * len(material_codes))
            query = f'''
                SELECT 
                    fd_supplier_code as supplier_code,
                    fd_supplier_name as supplier_name,
                    fd_material_code as material_code,
                    fd_execution_rate as execution_rate,
                    fd_remain_qty as remain_qty,
                    fd_remain_amount as remain_amount,
                    fd_unit_price as unit_price,
                    fd_delivery_cycle as delivery_cycle
                FROM mt_supplier_agreement
                WHERE fd_material_code IN ({placeholders})
                ORDER BY fd_material_code, fd_execution_rate
            '''
            params = material_codes
        else:
            query = '''
                SELECT 
                    fd_supplier_code as supplier_code,
                    fd_supplier_name as supplier_name,
                    fd_material_code as material_code,
                    fd_execution_rate as execution_rate,
                    fd_remain_qty as remain_qty,
                    fd_remain_amount as remain_amount,
                    fd_unit_price as unit_price,
                    fd_delivery_cycle as delivery_cycle
                FROM mt_supplier_agreement
                ORDER BY fd_material_code, fd_execution_rate
            '''
            params = []
        
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()
        
        return rows
    
    async def stream_analyze(self, input_plans: List[Dict[str, Any]] = None):
        """流式分析供应商匹配"""
        # 获取计划数据
        plans = self._query_plans_for_stream(input_plans)
        
        if not plans:
            yield "未查询到符合条件的补货计划。"
            return
        
        # 提取物料编码
        material_codes = list(set(p.get('materialCode', '') for p in plans if p.get('materialCode')))
        
        if not material_codes:
            yield "未提取到有效的物料编码。"
            return
        
        # 查询供应商数据
        suppliers = self._query_suppliers_for_stream(material_codes)
        
        if not suppliers:
            yield "未查询到符合条件的供应商数据。"
            return
        
        # 调用流式LLM分析
        async for chunk in self.agent.stream_analyze(plans, suppliers):
            yield chunk
