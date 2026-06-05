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

# 导入数据库配置
from ..config.db_config import (
    CURRENT_DB_TYPE,
    DB_TYPE_SQLITE,
    DB_TYPE_MYSQL,
    MYSQL_CONFIG,
    SQLITE_DB_PATH
)

# ============================================
# 数据库切换配置
# ============================================
# 当前使用: MySQL (默认)
# 如需切换到SQLite，修改 db_config.py 中的 CURRENT_DB_TYPE = DB_TYPE_SQLITE
# ============================================

import pymysql
from pymysql.cursors import DictCursor


class RealDB:
    """真实数据库操作类 - 支持MySQL和SQLite（带优雅降级，连接失败不阻塞启动）"""

    def __init__(self):
        self.config = MYSQL_CONFIG if CURRENT_DB_TYPE == DB_TYPE_MYSQL else {'db_path': SQLITE_DB_PATH}
        self._db_available = False
        self._test_connection()

    def _test_connection(self):
        """测试数据库连接，失败时仅打印日志，不阻塞启动"""
        if CURRENT_DB_TYPE == DB_TYPE_MYSQL:
            self._test_mysql_connection()
        else:
            self._test_sqlite_connection()

    def _test_mysql_connection(self):
        """测试MySQL连接"""
        try:
            conn = pymysql.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database'],
                charset=self.config['charset'],
                cursorclass=DictCursor,
                connect_timeout=self.config.get('connect_timeout', 5),
                read_timeout=self.config.get('read_timeout', 5),
                write_timeout=self.config.get('write_timeout', 5)
            )
            conn.close()
            self._db_available = True
            logger.info(f"[DB] MySQL连接成功: {self.config['host']}:{self.config['port']}/{self.config['database']}")
        except Exception as e:
            self._db_available = False
            logger.warning(f"[DB] MySQL连接失败 ({self.config['host']}:{self.config['port']}), 数据库功能不可用: {str(e)}")
            logger.warning("[DB] 服务将继续启动，但所有数据库查询将返回空数据")

    def _test_sqlite_connection(self):
        """测试SQLite连接"""
        try:
            conn = sqlite3.connect(self.config['db_path'])
            conn.close()
            self._db_available = True
            logger.info(f"[DB] SQLite连接成功: {self.config['db_path']}")
        except Exception as e:
            self._db_available = False
            logger.warning(f"[DB] SQLite连接失败: {str(e)}")
            logger.warning("[DB] 服务将继续启动，但所有数据库查询将返回空数据")

    def _get_connection(self):
        """获取数据库连接"""
        if not self._db_available:
            raise ConnectionError("数据库服务不可用")
        
        if CURRENT_DB_TYPE == DB_TYPE_MYSQL:
            return self._get_mysql_connection()
        else:
            return self._get_sqlite_connection()

    def _get_mysql_connection(self):
        """获取MySQL数据库连接"""
        connection = pymysql.connect(
            host=self.config['host'],
            port=self.config['port'],
            user=self.config['user'],
            password=self.config['password'],
            database=self.config['database'],
            charset=self.config['charset'],
            cursorclass=DictCursor,
            connect_timeout=self.config.get('connect_timeout', 10),
            read_timeout=self.config.get('read_timeout', 30),
            write_timeout=self.config.get('write_timeout', 30)
        )
        return connection

    def _get_sqlite_connection(self):
        """获取SQLite数据库连接"""
        conn = sqlite3.connect(self.config['db_path'])
        conn.row_factory = sqlite3.Row
        return conn

    async def fetch_inventory(self, material_code: int, warehouse_code: str):
        """获取库存数据 - 从w_stock_info_0808表查询"""
        logger.info(f"[fetch_inventory] 输入参数: material_code={material_code}, warehouse_code={warehouse_code}")
        try:
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
        except Exception as e:
            logger.error(f"[DB] fetch_inventory 执行失败: {str(e)}")
            return None

    async def fetch_weekly_outbound(self, material_code: int, warehouse_code: str):
        """获取周度出库统计 - 使用mt_historical_outbound表"""
        logger.info(f"[fetch_weekly_outbound] 输入参数: material_code={material_code}, warehouse_code={warehouse_code}")
        try:
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
        except Exception as e:
            logger.error(f"[DB] fetch_weekly_outbound 执行失败: {str(e)}")
            return None

    async def fetch_material(self, material_code: int):
        """获取物料信息 - 从w_stock_info_0808表查询"""
        logger.info(f"[fetch_material] 输入参数: material_code={material_code}")
        try:
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
        except Exception as e:
            logger.error(f"[DB] fetch_material 执行失败: {str(e)}")
            return None

    async def fetch_suppliers(self):
        """获取供应商列表"""
        try:
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
        except Exception as e:
            logger.error(f"[DB] fetch_suppliers 执行失败: {str(e)}")
            return []

    async def fetch_all_inventories(self, material_code: int):
        """获取所有仓库库存"""
        try:
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
        except Exception as e:
            logger.error(f"[DB] fetch_all_inventories 执行失败: {str(e)}")
            return []

    async def fetch_supplier_materials(self, supplier_code: str):
        """获取供应商可供应的物料"""
        try:
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
        except Exception as e:
            logger.error(f"[DB] fetch_supplier_materials 执行失败: {str(e)}")
            return []

    async def fetch_warehouse_info(self, warehouse_code: str):
        """获取仓库信息"""
        try:
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
        except Exception as e:
            logger.error(f"[DB] fetch_warehouse_info 执行失败: {str(e)}")
            return None

    async def fetch_related_warehouses(self, warehouse_code: str):
        """获取相关仓库信息"""
        try:
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
        except Exception as e:
            logger.error(f"[DB] fetch_related_warehouses 执行失败: {str(e)}")
            return []

    async def fetch_monthly_stats(self, material_code: int, warehouse_code: str, months: int = 6):
        """获取月度统计数据"""
        try:
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
        except Exception as e:
            logger.error(f"[DB] fetch_monthly_stats 执行失败: {str(e)}")
            return []

    async def fetch_supplier_evaluations(self, material_code: int):
        """获取供应商对特定物料的评估"""
        try:
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
        except Exception as e:
            logger.error(f"[DB] fetch_supplier_evaluations 执行失败: {str(e)}")
            return []

    async def fetch_agreements(self, material_code: int):
        """获取物料相关的协议"""
        try:
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
        except Exception as e:
            logger.error(f"[DB] fetch_agreements 执行失败: {str(e)}")
            return []

    async def fetch_historical_outbound(self, material_code: str = None, warehouse_code: str = None, limit: int = 100):
        """获取历史出库数据"""
        try:
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
        except Exception as e:
            logger.error(f"[DB] fetch_historical_outbound 执行失败: {str(e)}")
            return []

    async def fetch_stock_by_materials(self, material_codes: List[str]):
        """根据物料编码列表获取库存信息"""
        if not material_codes:
            return []

        try:
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
        except Exception as e:
            logger.error(f"[DB] fetch_stock_by_materials 执行失败: {str(e)}")
            return []

    async def fetch_all_stocks(self):
        """获取所有库存信息"""
        try:
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
        except Exception as e:
            logger.error(f"[DB] fetch_all_stocks 执行失败: {str(e)}")
            return []

    async def fetch_warehouse_by_code(self, warehouse_code: str):
        """根据仓库编码获取仓库信息"""
        try:
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
        except Exception as e:
            logger.error(f"[DB] fetch_warehouse_by_code 执行失败: {str(e)}")
            return None

    async def fetch_plan_details(self, plan_ids: List[str] = None):
        """获取计划明细"""
        try:
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
        except Exception as e:
            logger.error(f"[DB] fetch_plan_details 执行失败: {str(e)}")
            return []

    async def fetch_deposit_materials_by_category(self, major_category: str = '', medium_category: str = '', small_category: str = '') -> List[Dict]:
        """根据物资分类编码查询储备物资列表（mt_deposit_materials）
        
        Args:
            major_category: 物资大类编码，如 "01"
            medium_category: 物资中类编码，如 "0101"
            small_category: 物资小类编码，如 "010101"
        
        Returns:
            物资列表，包含 fd_material_code, fd_material_desc, fd_tech_spec_id 等
        """
        logger.info(f"[fetch_deposit_materials_by_category] major={major_category}, medium={medium_category}, small={small_category}")
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                
                query = '''
                    SELECT DISTINCT fd_material_code, fd_material_desc, fd_tech_spec_id, fd_identifier,
                           fd_big_class_code, fd_big_class_desc, fd_middle_class_code, fd_middle_class_desc,
                           fd_subclass_code, fd_subclass_desc
                    FROM mt_deposit_materials
                    WHERE 1=1
                '''
                params = []
                
                if major_category:
                    query += " AND fd_big_class_code = %s"
                    params.append(major_category)
                if medium_category:
                    query += " AND fd_middle_class_code = %s"
                    params.append(medium_category)
                if small_category:
                    query += " AND fd_subclass_code = %s"
                    params.append(small_category)
                
                cur.execute(query, params)
                results = cur.fetchall()
                logger.info(f"[fetch_deposit_materials_by_category] 查询到 {len(results)} 条物资记录")
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"[DB] fetch_deposit_materials_by_category 执行失败: {str(e)}")
            return []

    async def fetch_water_level_configs_batch(self, material_codes: List[str], tech_ids: List[str]) -> List[Dict]:
        """批量查询水位配置（mt_water_level_config）
        
        Args:
            material_codes: 物料编码列表
            tech_ids: 技术规范ID列表
        
        Returns:
            水位配置列表，包含各仓库下的水位系数和触发值
        """
        if not material_codes or not tech_ids:
            return []
        
        logger.info(f"[fetch_water_level_configs_batch] materials={len(material_codes)}, techs={len(tech_ids)}")
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                
                material_placeholders = ','.join(['%s'] * len(material_codes))
                tech_placeholders = ','.join(['%s'] * len(tech_ids))
                
                query = f'''
                    SELECT fd_warehouse_code, fd_warehouse_name, fd_material_code, fd_material_name,
                           fd_tech_id, fd_low_water_coefficient, fd_mid_water_coefficient, 
                           fd_high_water_coefficient, fd_replenish_trigger_value,
                           fd_low_water_value, fd_mid_water_value, fd_high_water_value
                    FROM mt_water_level_config
                    WHERE fd_material_code IN ({material_placeholders})
                      AND fd_tech_id IN ({tech_placeholders})
                '''
                params = list(material_codes) + list(tech_ids)
                
                cur.execute(query, params)
                results = cur.fetchall()
                logger.info(f"[fetch_water_level_configs_batch] 查询到 {len(results)} 条水位配置")
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"[DB] fetch_water_level_configs_batch 执行失败: {str(e)}")
            return []

    async def batch_get_stock_for_combos(self, combos: List[tuple]) -> Dict[tuple, Dict]:
        """批量查询指定组合的当前库存和在途库存
        
        Args:
            combos: (warehouse_code, material_code, tech_id) 列表
        
        Returns:
            dict: key=(warehouse_code, material_code, tech_id), value={'current_stock', 'in_transit_stock', 'warehouse_name'}
        """
        if not combos:
            return {}
        
        logger.info(f"[batch_get_stock_for_combos] 查询 {len(combos)} 个组合的库存")
        result = {}
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                
                # 提取唯一的 warehouse_codes, material_codes 用于批量查询
                unique_warehouses = list(set(c[0] for c in combos))
                unique_materials = list(set(str(c[1]) for c in combos))
                unique_techs = list(set(c[2] for c in combos))
                
                w_placeholders = ','.join(['%s'] * len(unique_warehouses))
                m_placeholders = ','.join(['%s'] * len(unique_materials))
                t_placeholders = ','.join(['%s'] * len(unique_techs))
                
                query = f'''
                    SELECT
                        w.loc_code as warehouse_code,
                        w.loc_name as warehouse_name,
                        w.material_code,
                        w.material_desc,
                        w.tech_id,
                        SUM(CASE WHEN w.source_type IS NULL OR w.source_type != '在途' THEN w.stock_qty ELSE 0 END) as current_stock,
                        SUM(CASE WHEN w.source_type = '在途' THEN w.stock_qty ELSE 0 END) as in_transit_stock
                    FROM w_stock_info_0808 w
                    WHERE w.loc_code IN ({w_placeholders})
                      AND w.material_code IN ({m_placeholders})
                      AND w.tech_id IN ({t_placeholders})
                    GROUP BY w.loc_code, w.material_code, w.tech_id
                '''
                params = unique_warehouses + unique_materials + unique_techs
                
                cur.execute(query, params)
                rows = cur.fetchall()
                
                for row in rows:
                    key = (row['warehouse_code'], str(row['material_code']), row['tech_id'])
                    result[key] = {
                        'warehouse_code': row['warehouse_code'],
                        'warehouse_name': row['warehouse_name'] or '',
                        'material_code': str(row['material_code']),
                        'material_desc': row['material_desc'] or '',
                        'tech_id': row['tech_id'],
                        'current_stock': float(row['current_stock'] or 0),
                        'in_transit_stock': float(row['in_transit_stock'] or 0),
                    }
                
                logger.info(f"[batch_get_stock_for_combos] 查询到 {len(rows)} 行, 匹配 {len(result)} 个组合")
        except Exception as e:
            logger.error(f"[DB] batch_get_stock_for_combos 执行失败: {str(e)}")
        
        return result

    async def fetch_warehouse_info_batch(self, warehouse_codes: List[str]) -> Dict[str, Dict]:
        """批量查询仓库信息
        
        Args:
            warehouse_codes: 仓库编码列表
        
        Returns:
            dict: key=warehouse_code, value={'name', 'level', 'type'}
        """
        if not warehouse_codes:
            return {}
        
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                placeholders = ','.join(['%s'] * len(warehouse_codes))
                
                query = f'''
                    SELECT fd_warehouse_code, fd_warehouse_name, fd_warehouse_type, fd_stock_level
                    FROM mt_base_warehouse_info
                    WHERE fd_warehouse_code IN ({placeholders})
                '''
                cur.execute(query, list(warehouse_codes))
                rows = cur.fetchall()
                
                result = {}
                for row in rows:
                    result[row['fd_warehouse_code']] = {
                        'name': row['fd_warehouse_name'] or '',
                        'level': row['fd_stock_level'] or '',
                        'type': row['fd_warehouse_type'] or ''
                    }
                return result
        except Exception as e:
            logger.error(f"[DB] fetch_warehouse_info_batch 执行失败: {str(e)}")
            return {}

    async def batch_insert_inventory_analysis_plan(self, records: List[tuple]) -> int:
        """批量插入库存分析计划（mt_inventory_analysis_plan）
        
        Args:
            records: 记录列表，每个元素为tuple，字段顺序与SQL对应
        
        Returns:
            成功插入的记录数
        """
        if not records:
            return 0
        
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                
                cur.executemany('''
                    REPLACE INTO mt_inventory_analysis_plan (
                        fd_id, fd_warehouse_code, fd_material_code, fd_tech_id,
                        fd_start_date, fd_end_date, fd_match_type, fd_identifier,
                        fd_material_desc,
                        fd_purchase_request_no, fd_purchase_request_item_no, fd_purchase_request_qty, fd_purchase_request_unit,
                        fd_project_description, fd_project_definition, fd_wbs_element, fd_batch,
                        fd_warehouse_name, fd_delivery_location, fd_inventory_level,
                        fd_high_level, fd_replenish_level, fd_emergency_line,
                        fd_current_stock, fd_unit, fd_purchase_request_price,
                        fd_create_time, fd_update_time,
                        fd_warehouse_location, fd_current_water_level,
                        fd_in_transit_qty, fd_stock_status, fd_suggested_action,
                        fd_compare_date, sub_class, fd_stock_level
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', records)
                
                conn.commit()
                logger.info(f"[batch_insert_inventory_analysis_plan] 成功插入 {len(records)} 条记录")
                return len(records)
        except Exception as e:
            logger.error(f"[DB] batch_insert_inventory_analysis_plan 执行失败: {str(e)}")
            return 0

    async def batch_upsert_water_level_config(self, records: List[tuple]) -> int:
        """批量更新/插入水位配置（mt_water_level_config）
        
        Args:
            records: 记录列表，格式为：
                (material_code, material_name, tech_id, warehouse_code, warehouse_name,
                 emergency_factor, replenish_factor, high_factor,
                 replenish_trigger_value, emergency_line, mid_line, high_line,
                 reserve_quota, big_class_desc, middle_class_desc, subclass_desc,
                 month, create_time)
        
        Returns:
            成功操作的记录数
        """
        if not records:
            return 0
        
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                
                cur.executemany('''
                    REPLACE INTO mt_water_level_config (
                        fd_material_code, fd_material_name, fd_tech_id, fd_warehouse_code, fd_warehouse_name,
                        fd_low_water_coefficient, fd_mid_water_coefficient, fd_high_water_coefficient,
                        fd_replenish_trigger_value, fd_low_water_value, fd_mid_water_value, fd_high_water_value,
                        fd_reserve_quota, big_class_desc, middle_class_desc, subclass_desc,
                        fd_month, fd_create_time
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', records)
                
                conn.commit()
                logger.info(f"[batch_upsert_water_level_config] 成功操作 {len(records)} 条记录")
                return len(records)
        except Exception as e:
            logger.error(f"[DB] batch_upsert_water_level_config 执行失败: {str(e)}")
            return 0


real_db = RealDB()
