# -*- coding: utf-8 -*-
"""调配服务 - 流式版本"""
import json
from typing import List, Dict, Any, Optional
from datetime import datetime

from ..agents.allocation_stream_agent import AllocationStreamAgent


class AllocationStreamService:
    """调配服务 - 流式版本"""
    
    def __init__(self, db, llm_stream_func):
        self.db = db
        self.agent = AllocationStreamAgent(llm_stream_func)
    
    async def _query_stocks_for_stream(self, material_codes: List[str], tech_ids: List[str]) -> List[Dict[str, Any]]:
        """查询库存数据（用于流式接口）"""
        conn = self.db._get_connection()
        conn.row_factory = lambda cursor, row: dict(zip([col[0] for col in cursor.description], row))
        cur = conn.cursor()
        
        placeholders_material = ','.join(['%s'] * len(material_codes))
        placeholders_tech = ','.join(['%s'] * len(tech_ids))
        
        query = f'''
            SELECT 
                material_code,
                MAX(material_desc) as material_desc,
                tech_id,
                loc_code,
                MAX(loc_name) as loc_name,
                SUM(stock_qty) as stock_qty,
                MAX(unit_price) as unit_price,
                GROUP_CONCAT(DISTINCT source_type SEPARATOR '/') as source_type,
                GROUP_CONCAT(DISTINCT factory_name SEPARATOR '/') as factory_name
            FROM w_stock_info_0808
            WHERE material_code IN ({placeholders_material}) 
              AND tech_id IN ({placeholders_tech})
            GROUP BY loc_code, material_code, tech_id
        '''
        
        params = material_codes + tech_ids
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()
        
        return rows
    
    def _query_warehouse_distances(self) -> Dict[str, float]:
        """查询仓库距离映射"""
        conn = self.db._get_connection()
        conn.row_factory = lambda cursor, row: dict(zip([col[0] for col in cursor.description], row))
        cur = conn.cursor()
        
        query = '''
            SELECT fd_source_warehouse_code, fd_distance 
            FROM mt_warehouse_distance
        '''
        
        cur.execute(query)
        rows = cur.fetchall()
        conn.close()
        
        distances = {}
        for row in rows:
            src = row['fd_source_warehouse_code']
            dist = row['fd_distance']
            distances[src] = float(dist) if dist else 0
        
        return distances
    
    def _query_plans_for_stream(self, warehouse_code: str = "", source_type: str = "", 
                               project_unit: str = "", demand_start_date: str = "", 
                               demand_end_date: str = "", plan_type: str = "",
                               material_codes: List[str] = None) -> List[Dict[str, Any]]:
        """查询计划数据（用于流式接口）"""
        conn = self.db._get_connection()
        conn.row_factory = lambda cursor, row: dict(zip([col[0] for col in cursor.description], row))
        cur = conn.cursor()
        
        query = '''
            SELECT 
                fd_plan_id as planId,
                fd_plan_code as planCode,
                fd_material_code as materialCode,
                fd_material_desc as materialDesc,
                fd_demand_qty as demandQty,
                fd_unit as unit,
                fd_unit_name as unitName,
                fd_project_name as projectName,
                fd_warehouse_code as warehouseCode,
                fd_tech_spec_id as techSpecId
            FROM mt_allocation_plan
            WHERE 1=1
        '''
        params = []
        
        if warehouse_code:
            query += ' AND fd_warehouse_code = %s'
            params.append(warehouse_code)
        if source_type:
            query += ' AND fd_source_type = %s'
            params.append(source_type)
        if project_unit:
            query += ' AND fd_project_unit = %s'
            params.append(project_unit)
        if demand_start_date:
            query += ' AND fd_demand_start_date >= %s'
            params.append(demand_start_date)
        if demand_end_date:
            query += ' AND fd_demand_end_date <= %s'
            params.append(demand_end_date)
        if plan_type:
            query += ' AND fd_plan_type = %s'
            params.append(plan_type)
        if material_codes and len(material_codes) > 0:
            placeholders = ','.join(['%s'] * len(material_codes))
            query += f' AND fd_material_code IN ({placeholders})'
            params.extend(material_codes)
        
        query += ' ORDER BY fd_create_time DESC LIMIT 20'
        
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()
        
        return rows
    
    async def stream_analyze(self, strategy: str = "time", warehouse_code: str = "", 
                            source_type: str = "", project_unit: str = "",
                            demand_start_date: str = "", demand_end_date: str = "",
                            plan_type: str = "", material_codes: List[str] = None):
        """流式分析调配方案"""
        # 查询计划
        plans = self._query_plans_for_stream(warehouse_code, source_type, project_unit,
                                           demand_start_date, demand_end_date, 
                                           plan_type, material_codes)
        
        if not plans:
            yield "未查询到符合条件的需求计划。"
            return
        
        # 提取物料编码和技术规范ID
        material_codes_extracted = list(set(p.get('materialCode', '') for p in plans if p.get('materialCode')))
        tech_ids = list(set(p.get('techSpecId', '') for p in plans if p.get('techSpecId')))
        
        if not material_codes_extracted:
            yield "未提取到有效的物料编码。"
            return
        
        # 查询库存
        stocks = self._query_stocks_for_stream(material_codes_extracted, tech_ids)
        
        if not stocks:
            yield "未查询到符合条件的库存数据。"
            return
        
        # 查询距离
        distances_map = self._query_warehouse_distances()
        
        # 调用流式LLM分析
        async for chunk in self.agent.stream_analyze(plans, stocks, distances_map, strategy):
            yield chunk
