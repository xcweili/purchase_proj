# -*- coding: utf-8 -*-
"""
数据库配置 - 支持多数据库切换
"""
from typing import Dict

# SQLite配置
SQLITE_DB_PATH = 'purchase_management.db'

# 智能体运行时数据库（SQLite）
# 用于持久化 运行记录/计划步骤/事件日志/检查点，支撑 计划展示、回溯、定点回放
AGENT_SQLITE_DB_PATH = 'agent_runtime.db'

# 数据库类型枚举
DB_TYPE_SQLITE = 'sqlite'
DB_TYPE_MYSQL = 'mysql'

# 当前使用的数据库类型
CURRENT_DB_TYPE = DB_TYPE_MYSQL

# ============================================
# MySQL 多环境配置（切换只需改下面一行）
# ============================================
CURRENT_MYSQL_PROFILE = "local_dev"
# 可选值: "js" | "hn" | "local" | "local_dev"

_MYSQL_PROFILES: Dict[str, dict] = {
    "js": {
        'host': 'rm-j6j2fm50qw676ku2n.mysql.rds.ops.sgmc.sgcc.com.cn',
        'port': 13306,
        'user': 'pewz_user',
        'password': 'pewz$$12RRS',
        'database': 'pewz',
        'charset': 'utf8mb4',
        'cursorclass': 'DictCursor',
        'connect_timeout': 5,
        'read_timeout': 5,
        'write_timeout': 5,
    },
    "hn": {
        'host': '25.212.252.199',
        'port': 13306,
        'user': 'wztppt230',
        'password': 'HNxt@2025',
        'database': 'ai_project',
        'charset': 'utf8mb4',
        'cursorclass': 'DictCursor',
        'connect_timeout': 5,
        'read_timeout': 5,
        'write_timeout': 5,
    },
    "local": {
        'host': '192.168.1.1',
        'port': 3306,
        'user': 'root',
        'password': 'HN@123456',
        'database': 'ai',
        'charset': 'utf8mb4',
        'cursorclass': 'DictCursor',
    },
    "local_dev": {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'password': '123456',
        'database': 'local_db',
        'charset': 'utf8mb4',
        'cursorclass': 'DictCursor',
    },
}

MYSQL_CONFIG = _MYSQL_PROFILES[CURRENT_MYSQL_PROFILE]


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


def get_agent_db_path() -> str:
    """
    获取智能体运行时数据库（SQLite）的文件路径

    该数据库用于持久化：运行记录、计划步骤、事件日志以及 LangGraph 检查点，
    是 计划展示 / 回溯 / 定点回放 / 中断恢复 的数据底座。
    """
    return AGENT_SQLITE_DB_PATH
