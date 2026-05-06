# -*- coding: utf-8 -*-
"""
数据库服务封装 - 提供数据访问能力
"""
import sqlite3
import logging
from typing import Dict, Any, List, Optional
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO, format='[DB] %(message)s')
logger = logging.getLogger(__name__)

SQLITE_DB_PATH = 'purchase_management.db'

# ============================================
# 数据库切换配置
# ============================================
# 当前使用: SQLite (默认)
# 如需切换到MySQL，注释掉SQLite相关代码，取消注释MySQL相关代码，并安装pymysql: pip install pymysql
# ============================================

# MySQL配置
MYSQL_CONFIG = {
    'host': '25.212.252.199',
    'port': 13306,
    'user': 'wztppt230',
    'password': 'HNxt@2025',
    'database': 'ai_project',
    'charset': 'utf8mb4',
    'cursorclass': 'DictCursor'
}

import pymysql
from pymysql.cursors import DictCursor

class RealDB:
    """真实数据库操作类 - 使用 MySQL"""

    def __init__(self):
        self.config = MYSQL_CONFIG

    def _get_connection(self):
        """获取MySQL数据库连接"""
        connection = pymysql.connect(
            host=self.config['host'],
            port=self.config['port'],
            user=self.config['user'],
            password=self.config['password'],
            database=self.config['database'],
            charset=self.config['charset'],
            cursorclass=DictCursor
        )
        return connection


# class RealDB:
#     """真实数据库操作类 - 使用 SQLite"""

#     def __init__(self):
#         self.db_path = SQLITE_DB_PATH

#     def _get_connection(self):
#         """获取数据库连接"""
#         conn = sqlite3.connect(self.db_path)
#         conn.row_factory = sqlite3.Row
#         return conn

    async def fetch_inventory(self, material_code: int, warehouse_code: str):
        """获取库存数据 - 从w_stock_info_0808表查询"""
        logger.info(f"[fetch_inventory] 输入参数: material_code={material_code}, warehouse_code={warehouse_code}")

        with self._get_connection() as conn:
            cur = conn.cursor()

            sql1 = '''SELECT SUM(stock_qty) as total_stock,
                      COUNT(DISTINCT batch_no) as batch_count,
                      MIN(storage_time) as first_storage_time,
                      MAX(storage_time) as last_storage_time
               FROM w_stock_info_0808
               WHERE material_code = %s AND loc_code = %s'''
            params1 = (str(material_code), warehouse_code)
            logger.info(f"[fetch_inventory] SQL: {sql1}")
            logger.info(f"[fetch_inventory] 参数: {params1}")

            cur.execute(sql1, params1)
            result = cur.fetchone()
            logger.info(f"[fetch_inventory] 查询结果: {dict(result) if result else None}")

            if result:
                total_stock = result['total_stock'] or 0
                avg_consumption = 50
                safety_stock = avg_consumption * 2
                reorder_point = avg_consumption * 7
                high_level = avg_consumption * 15
                emergency_line = avg_consumption * 3

                data = {
                    "material_code": str(material_code),
                    "warehouse_code": warehouse_code,
                    "current_qty": total_stock,
                    "in_transit_qty": 0,
                    "safety_stock": safety_stock,
                    "reorder_point": reorder_point,
                    "high_level": high_level,
                    "emergency_line": emergency_line,
                    "last_update_date": result['last_storage_time']
                }
                logger.info(f"[fetch_inventory] 返回数据: {data}")
                return data

            sql2 = '''SELECT SUM(stock_qty) as total_stock
                      FROM w_stock_info_0808
                      WHERE material_code = %s'''
            params2 = (str(material_code),)
            logger.info(f"[fetch_inventory] 备选SQL: {sql2}")
            logger.info(f"[fetch_inventory] 参数: {params2}")

            cur.execute(sql2, params2)
            result = cur.fetchone()
            logger.info(f"[fetch_inventory] 备选查询结果: {dict(result) if result else None}")

            if result:
                total_stock = result['total_stock'] or 0
                data = {
                    "material_code": str(material_code),
                    "warehouse_code": warehouse_code,
                    "current_qty": total_stock,
                    "in_transit_qty": 0,
                    "safety_stock": 100,
                    "reorder_point": 350,
                    "high_level": 750,
                    "emergency_line": 150,
                    "last_update_date": ""
                }
                logger.info(f"[fetch_inventory] 返回数据: {data}")
                return data

            logger.info(f"[fetch_inventory] 未找到数据，返回None")
            return None

    async def fetch_weekly_outbound(self, material_code: int, warehouse_code: str):
        """获取周度出库统计 - 使用mt_historical_outbound表"""
        logger.info(f"[fetch_weekly_outbound] 输入参数: material_code={material_code}, warehouse_code={warehouse_code}")

        with self._get_connection() as conn:
            cur = conn.cursor()

            sql1 = '''SELECT fd_posting_month, SUM(fd_outbound_qty) as total_qty,
                      SUM(fd_outbound_count) as total_count
               FROM mt_historical_outbound
               WHERE fd_material_code = %s AND fd_warehouse_code = %s
               GROUP BY fd_posting_month
               ORDER BY fd_posting_month DESC
               LIMIT 6'''
            params1 = (str(material_code), warehouse_code)
            logger.info(f"[fetch_weekly_outbound] SQL: {sql1}")
            logger.info(f"[fetch_weekly_outbound] 参数: {params1}")

            cur.execute(sql1, params1)
            recent_records = cur.fetchall()
            logger.info(f"[fetch_weekly_outbound] 查询结果数量: {len(recent_records)}")

            if recent_records:
                total_qty = sum(r['total_qty'] or 0 for r in recent_records)
                avg_qty = total_qty / len(recent_records) if recent_records else 0

                months = [r['fd_posting_month'] for r in recent_records]
                quantities = [r['total_qty'] or 0 for r in recent_records]

                data = {
                    "最近月份": months,
                    "各月出库量": quantities,
                    "总出库量": total_qty,
                    "平均月出库": round(avg_qty, 2),
                    "总出库次数": sum(r['total_count'] or 0 for r in recent_records)
                }
                logger.info(f"[fetch_weekly_outbound] 返回数据: {data}")
                return data

            sql2 = '''SELECT fd_posting_month, SUM(fd_outbound_qty) as total_qty
                      FROM mt_historical_outbound
                      WHERE fd_material_code = %s
                      GROUP BY fd_posting_month
                      ORDER BY fd_posting_month DESC
                      LIMIT 6'''
            params2 = (str(material_code),)
            logger.info(f"[fetch_weekly_outbound] 备选SQL: {sql2}")
            logger.info(f"[fetch_weekly_outbound] 参数: {params2}")

            cur.execute(sql2, params2)
            all_warehouses_records = cur.fetchall()
            logger.info(f"[fetch_weekly_outbound] 备选查询结果数量: {len(all_warehouses_records)}")

            if all_warehouses_records:
                total_qty = sum(r['total_qty'] or 0 for r in all_warehouses_records)
                avg_qty = total_qty / len(all_warehouses_records) if all_warehouses_records else 0

                data = {
                    "最近月份": [r['fd_posting_month'] for r in all_warehouses_records],
                    "各月出库量": [r['total_qty'] or 0 for r in all_warehouses_records],
                    "总出库量": total_qty,
                    "平均月出库": round(avg_qty, 2),
                    "总出库次数": 0,
                    "备注": "该仓库无历史数据，使用所有仓库汇总"
                }
                logger.info(f"[fetch_weekly_outbound] 返回数据: {data}")
                return data

            logger.info(f"[fetch_weekly_outbound] 未找到数据，返回None")
            return None

    async def fetch_material(self, material_code: int):
        """获取物料信息 - 从w_stock_info_0808表查询"""
        logger.info(f"[fetch_material] 输入参数: material_code={material_code}")

        with self._get_connection() as conn:
            cur = conn.cursor()

            sql = '''SELECT DISTINCT material_code, material_desc, tech_id,
                      munit, factory_code, factory_name
               FROM w_stock_info_0808
               WHERE material_code = %s'''
            params = (str(material_code),)
            logger.info(f"[fetch_material] SQL: {sql}")
            logger.info(f"[fetch_material] 参数: {params}")

            cur.execute(sql, params)
            result = cur.fetchone()
            logger.info(f"[fetch_material] 查询结果: {dict(result) if result else None}")

            if result:
                data = {
                    "material_code": result['material_code'],
                    "material_name": result['material_desc'],
                    "material_desc": result['material_desc'],
                    "tech_id": result['tech_id'],
                    "unit": result['munit'],
                    "factory_code": result['factory_code'],
                    "factory_name": result['factory_name']
                }
                logger.info(f"[fetch_material] 返回数据: {data}")
                return data

            logger.info(f"[fetch_material] 未找到数据，返回None")
            return None

    async def fetch_suppliers(self):
        """获取供应商列表"""
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                '''SELECT supplier_name, delivery_score, price_score,
                          quality_score, service_score, delivery_cycle_days
                   FROM suppliers
                   ORDER BY delivery_score DESC, quality_score DESC
                   LIMIT 10''')
            results = cur.fetchall()

            return [
                {
                    "supplier_name": row['supplier_name'],
                    "products": "电力相关产品",
                    "certifications": "ISO9001",
                    "lead_time": row['delivery_cycle_days'] or 7,
                    "delivery_score": row['delivery_score'],
                    "price_score": row['price_score'],
                    "quality_score": row['quality_score'],
                    "service_score": row['service_score']
                }
                for row in results
            ]

    async def fetch_all_inventories(self, material_code: int):
        """获取所有仓库库存"""
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                '''SELECT warehouse_code, current_stock, safety_stock,
                          high_water_line, reorder_line, emergency_line
                   FROM inventory
                   WHERE material_code = %s''',
                (material_code,)
            )
            results = cur.fetchall()

            return [
                {
                    "warehouse_code": row['warehouse_code'],
                    "current_qty": row['current_stock'],
                    "safety_stock": row['safety_stock'],
                    "high_water_line": row['high_water_line'],
                    "reorder_line": row['reorder_line'],
                    "emergency_line": row['emergency_line']
                }
                for row in results
            ]

    async def fetch_supplier_materials(self, supplier_code: str):
        """获取供应商可供应的物料"""
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                '''SELECT m.material_code, m.material_name, a.unit_price,
                          a.delivery_cycle_days, si.available_stock
                   FROM agreements a
                   JOIN materials m ON a.material_code = m.material_code
                   LEFT JOIN supplier_inventory si ON a.supplier_code = si.supplier_code
                                                    AND a.material_code = si.material_code
                   WHERE a.supplier_code = %s AND a.status = 'active' ''',
                (supplier_code,)
            )
            results = cur.fetchall()

            return [
                {
                    "material_code": row['material_code'],
                    "material_name": row['material_name'],
                    "unit_price": row['unit_price'],
                    "delivery_cycle_days": row['delivery_cycle_days'],
                    "available_stock": row['available_stock'] or 0
                }
                for row in results
            ]

    async def fetch_warehouse_info(self, warehouse_code: str):
        """获取仓库信息"""
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                'SELECT warehouse_name, region, unit, longitude, latitude FROM warehouses WHERE warehouse_code = %s',
                (warehouse_code,)
            )
            result = cur.fetchone()
            if result:
                return dict(result)
            return None

    async def fetch_related_warehouses(self, warehouse_code: str):
        """获取相关仓库信息"""
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                '''SELECT wr.related_warehouse_code, w.warehouse_name, wr.distance_km, wr.relation_type
                   FROM warehouse_relations wr
                   JOIN warehouses w ON wr.related_warehouse_code = w.warehouse_code
                   WHERE wr.warehouse_code = %s''',
                (warehouse_code,)
            )
            results = cur.fetchall()
            return [dict(row) for row in results]

    async def fetch_monthly_stats(self, material_code: int, warehouse_code: str, months: int = 6):
        """获取月度统计数据"""
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                '''SELECT year_month, total_quantity, yoy_growth, mom_growth
                   FROM monthly_outbound_stats
                   WHERE material_code = %s AND warehouse_code = %s
                   ORDER BY year_month DESC
                   LIMIT %s''',
                (material_code, warehouse_code, months)
            )
            results = cur.fetchall()
            return [dict(row) for row in results]

    async def fetch_supplier_evaluations(self, material_code: int):
        """获取供应商对特定物料的评估"""
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                '''SELECT s.supplier_code, s.supplier_name, se.delivery_performance,
                          se.price_score, se.quality_score, se.service_score,
                          se.delivery_cycle_days, se.overall_score
                   FROM supplier_evaluations se
                   JOIN suppliers s ON se.supplier_code = s.supplier_code
                   WHERE se.material_code = %s
                   ORDER BY se.overall_score DESC''',
                (material_code,)
            )
            results = cur.fetchall()
            return [dict(row) for row in results]

    async def fetch_agreements(self, material_code: int):
        """获取物料相关的协议"""
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                '''SELECT a.agreement_code, a.supplier_code, s.supplier_name,
                          a.unit_price, a.delivery_cycle_days,
                          a.min_order_quantity, a.max_quantity,
                          a.start_date, a.end_date, a.status
                   FROM agreements a
                   JOIN suppliers s ON a.supplier_code = s.supplier_code
                   WHERE a.material_code = %s AND a.status = 'active'
                   ORDER BY a.unit_price''',
                (material_code,)
            )
            results = cur.fetchall()
            return [dict(row) for row in results]

    async def fetch_historical_outbound(self, material_code: str = None, warehouse_code: str = None, limit: int = 100):
        """获取历史出库数据"""
        with self._get_connection() as conn:
            cur = conn.cursor()
            query = "SELECT * FROM mt_historical_outbound WHERE 1=1"
            params = []

            if material_code:
                query += " AND fd_material_code = %s"
                params.append(material_code)
            if warehouse_code:
                query += " AND fd_warehouse_code = %s"
                params.append(warehouse_code)

            query += " ORDER BY fd_posting_month DESC LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            results = cur.fetchall()
            return [dict(row) for row in results]

    async def fetch_stock_by_materials(self, material_codes: List[str]):
        """根据物料编码列表获取库存信息"""
        if not material_codes:
            return []

        with self._get_connection() as conn:
            cur = conn.cursor()
            placeholders = ','.join(['%s' for _ in material_codes])
            cur.execute(
                f'''SELECT material_code, stock_qty, source_type, factory_code, factory_name,
                          loc_code, loc_name, room_code, room_name, wharea_code, wharea_name,
                          company_id, company_name
                   FROM w_stock_info_0808
                   WHERE material_code IN ({placeholders})''',
                material_codes
            )
            results = cur.fetchall()
            return [dict(row) for row in results]

    async def fetch_all_stocks(self):
        """获取所有库存信息"""
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                '''SELECT material_code, stock_qty, source_type, factory_code, factory_name,
                        loc_code, loc_name, room_code, room_name, wharea_code, wharea_name,
                        company_id, company_name
                 FROM w_stock_info_0808'''
            )
            results = cur.fetchall()
            return [dict(row) for row in results]

    async def fetch_warehouse_by_code(self, warehouse_code: str):
        """根据仓库编码获取仓库信息"""
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                '''SELECT fd_warehouse_code, fd_warehouse_name, fd_warehouse_type,
                          fd_address, fd_contact_person, fd_phone_num
                   FROM mt_base_warehouse_info
                   WHERE fd_warehouse_code = %s''',
                (warehouse_code,)
            )
            result = cur.fetchone()
            return dict(result) if result else None

    async def fetch_plan_details(self, plan_ids: List[str] = None):
        """获取计划明细"""
        with self._get_connection() as conn:
            cur = conn.cursor()

            if plan_ids:
                placeholders = ','.join(['%s' for _ in plan_ids])
                cur.execute(
                    f'''SELECT id as plan_id, fd_plan_id, fd_code_use as plan_code,
                              fd_material_code, fd_desc as material_desc,
                              fd_requisition_num as demand_qty, fd_unit,
                              fd_unit_code, fd_unit_name, fd_project_name,
                              fd_requisition_date, fd_plan_batch_code as plan_type
                       FROM mt_stock_use_list_plan_two
                       WHERE id IN ({placeholders})''',
                    plan_ids
                )
            else:
                cur.execute(
                    '''SELECT id as plan_id, fd_plan_id, fd_code_use as plan_code,
                             fd_material_code, fd_desc as material_desc,
                             fd_requisition_num as demand_qty, fd_unit,
                             fd_unit_code, fd_unit_name, fd_project_name,
                             fd_requisition_date, fd_plan_batch_code as plan_type
                      FROM mt_stock_use_list_plan_two LIMIT 10'''
                )

            results = cur.fetchall()
            return [dict(row) for row in results]


real_db = RealDB()
