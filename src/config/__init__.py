# -*- coding: utf-8 -*-
"""配置模块"""
from .llm_config import llm_config, get_current_provider, set_provider, LLMProviderConfig, LLMConfig
from .db_config import (
    SQLITE_DB_PATH,
    DB_TYPE_SQLITE,
    DB_TYPE_MYSQL,
    CURRENT_DB_TYPE,
    MYSQL_CONFIG,
    get_db_config,
    set_db_type
)

__all__ = [
    # LLM配置
    'llm_config',
    'get_current_provider',
    'set_provider',
    'LLMProviderConfig',
    'LLMConfig',
    # 数据库配置
    'SQLITE_DB_PATH',
    'DB_TYPE_SQLITE',
    'DB_TYPE_MYSQL',
    'CURRENT_DB_TYPE',
    'MYSQL_CONFIG',
    'get_db_config',
    'set_db_type',
]
