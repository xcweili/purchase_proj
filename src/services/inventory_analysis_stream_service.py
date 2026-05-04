# -*- coding: utf-8 -*-
"""库存分析服务 - 流式版本"""
import json
from typing import List, Dict, Any, Optional
from datetime import datetime

from ..agents.inventory_analysis_stream_agent import InventoryAnalysisStreamAgent


class InventoryAnalysisStreamService:
    """库存分析服务 - 流式版本"""
    
    def __init__(self, db, llm_stream_func):
        self.db = db
        self.agent = InventoryAnalysisStreamAgent(llm_stream_func)
    
    def _query_current_stock_for_stream(self, inventory_levels: List[str] = None, 
                                       material_codes: List[str] = None) -> List[Dict[str, Any]]:
        """查询当前库存数据（用于流式接口）"""
        conn = self.db._get_connection()
        conn.row_factory = lambda cursor, row: dict(zip([col[0] for col in cursor.description], row))
        cur = conn.cursor()
        
        query = '''
            SELECT 
                fd_warehouse_code as warehouse_code,
                fd_warehouse_name as warehouse_name,
                fd_material_code as material_code,
                fd_material_desc as material_desc,
                fd_tech_id as tech_id,
                fd_current_stock as current_stock,
                fd_in_transit_stock as in_transit_stock
            FROM mt_inventory_stock
            WHERE 1=1
        '''
        params = []
        
        if inventory_levels and len(inventory_levels) > 0:
            placeholders = ','.join(['%s'] * len(inventory_levels))
            query += f' AND fd_inventory_level IN ({placeholders})'
            params.extend(inventory_levels)
        
        if material_codes and len(material_codes) > 0:
            placeholders = ','.join(['%s'] * len(material_codes))
            query += f' AND fd_material_code IN ({placeholders})'
            params.extend(material_codes)
        
        query += ' ORDER BY fd_warehouse_code, fd_material_code LIMIT 50'
        
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()
        
        return rows
    
    def _query_historical_outbound_for_stream(self, start_date: str = None, end_date: str = None,
                                             inventory_levels: List[str] = None,
                                             material_codes: List[str] = None) -> List[Dict[str, Any]]:
        """查询历史出库数据（用于流式接口）"""
        conn = self.db._get_connection()
        conn.row_factory = lambda cursor, row: dict(zip([col[0] for col in cursor.description], row))
        cur = conn.cursor()
        
        query = '''
            SELECT 
                fd_warehouse_code as warehouse_code,
                fd_material_code as material_code,
                fd_tech_id as tech_id,
                fd_month as month,
                fd_outbound_qty as outbound_qty,
                fd_historical_avg_qty as historical_avg_qty,
                fd_last_1_month_qty as last_1_month_qty,
                fd_last_2_month_qty as last_2_month_qty,
                fd_last_3_month_qty as last_3_month_qty
            FROM mt_inventory_outbound
            WHERE 1=1
        '''
        params = []
        
        if start_date:
            query += ' AND fd_month >= %s'
            params.append(start_date)
        
        if end_date:
            query += ' AND fd_month <= %s'
            params.append(end_date)
        
        if inventory_levels and len(inventory_levels) > 0:
            placeholders = ','.join(['%s'] * len(inventory_levels))
            query += f' AND fd_inventory_level IN ({placeholders})'
            params.extend(inventory_levels)
        
        if material_codes and len(material_codes) > 0:
            placeholders = ','.join(['%s'] * len(material_codes))
            query += f' AND fd_material_code IN ({placeholders})'
            params.extend(material_codes)
        
        query += ' ORDER BY fd_month DESC LIMIT 100'
        
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()
        
        return rows
    
    async def stream_analyze(self, start_date: str = None, end_date: str = None,
                            inventory_levels: List[str] = None, material_codes: List[str] = None,
                            season_factor_weight: float = None, safety_redundancy_ratio: float = None):
        """流式分析库存"""
        # 查询库存数据
        stocks = self._query_current_stock_for_stream(inventory_levels, material_codes)
        
        if not stocks:
            yield "未查询到符合条件的库存数据。"
            return
        
        # 查询出库数据
        outbound = self._query_historical_outbound_for_stream(start_date, end_date, 
                                                            inventory_levels, material_codes)
        
        # 调用流式LLM分析
        async for chunk in self.agent.stream_analyze(stocks, outbound, start_date, end_date,
                                                    inventory_levels, season_factor_weight, 
                                                    safety_redundancy_ratio):
            yield chunk
