# -*- coding: utf-8 -*-
"""
工具基类
"""
from typing import Any, Optional
from pydantic import BaseModel
from src.core.registry import tool_registry


def tool(
    name: Optional[str] = None,
    description: str = "",
    args_schema: Optional[type[BaseModel]] = None,
):
    """快捷工具装饰器（注册到全局 registry）

    Usage:
        @tool(description="打招呼")
        def greet(name: str):
            \"\"\"向某人打招呼\"\"\"
            return f"你好, {name}!"
    """
    return tool_registry.register(name=name, description=description, args_schema=args_schema)


class BaseTool:
    """工具基类（类式定义，适合复杂工具）

    Usage:
        class GreetTool(BaseTool):
            name: str = "greet"
            description: str = "打招呼"

            def run(self, name: str) -> str:
                return f"你好, {name}!"
    """
    name: str = ""
    description: str = ""

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # 自动注册子类
        instance = cls()
        tool_registry.register(
            name=instance.name,
            description=instance.description,
        )(instance.run)

    def run(self, **kwargs) -> Any:
        raise NotImplementedError
