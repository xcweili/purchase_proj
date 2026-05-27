# -*- coding: utf-8 -*-
"""
数据库配置 - 支持多数据库切换
"""
from typing import Dict

# SQLite配置
SQLITE_DB_PATH = 'purchase_management.db'

# 数据库类型枚举
DB_TYPE_SQLITE = 'sqlite'
DB_TYPE_MYSQL = 'mysql'

# 当前使用的数据库类型
CURRENT_DB_TYPE = DB_TYPE_MYSQL

# MySQL配置
MYSQL_CONFIG: Dict[str, any] = {
    'host': '25.212.252.199',
    'port': 13306,
    'user': 'wztppt230',
    'password': 'HNxt@2025',
    'database': 'ai_project',
    'charset': 'utf8mb4',
    'cursorclass': 'DictCursor',
    'connect_timeout': 5,
    'read_timeout': 5,
    'write_timeout': 5
}

# MySQL配置 (待启用 - 备用配置)
# MYSQL_CONFIG = {
#     'host': '192.168.1.1',
#     'port': 3306,
#     'user': 'root',
#     'password': 'HN@123456',
#     'database': 'ai',
#     'charset': 'utf8mb4',
#     'cursorclass': 'DictCursor'
# }


def get_db_config(db_type: str = None) -> Dict[str, any]:
    """
    获取数据库配置
    
    Args:
        db_type: 数据库类型，不指定则使用当前配置的数据库类型
    
    Returns:
        数据库配置字典
    """
    target_type = db_type or CURRENT_DB_TYPE
    
    if target_type == DB_TYPE_MYSQL:
        return MYSQL_CONFIG
    elif target_type == DB_TYPE_SQLITE:
        return {'db_path': SQLITE_DB_PATH}
    else:
        raise ValueError(f"不支持的数据库类型: {target_type}")


def set_db_type(db_type: str) -> bool:
    """
    切换数据库类型
    
    Args:
        db_type: 数据库类型 (sqlite/mysql)
    
    Returns:
        是否切换成功
    """
    global CURRENT_DB_TYPE
    
    if db_type in [DB_TYPE_SQLITE, DB_TYPE_MYSQL]:
        CURRENT_DB_TYPE = db_type
        return True
    return False
