# -*- coding: utf-8 -*-
"""
Tool 注册中心
提供工具注册、查询、执行的统一管理
"""
import inspect
import functools
import logging
from typing import Dict, Any, Callable, Optional, get_type_hints
from pydantic import BaseModel, Field, create_model


logger = logging.getLogger(__name__)


class ToolInfo:
    """工具信息"""
    def __init__(
        self,
        name: str,
        description: str,
        func: Callable,
        args_schema: Optional[type[BaseModel]] = None,
        required_params: Optional[list[str]] = None,
    ):
        self.name = name
        self.description = description
        self.func = func
        self.args_schema = args_schema
        self.required_params = required_params or []

    def to_llm_description(self) -> dict:
        """转换为 LLM tool 描述格式"""
        tool_desc = {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
            }
        }
        if self.args_schema:
            schema = self.args_schema.model_json_schema()
            # 移除不必要的顶层字段
            properties = schema.get("properties", {})
            required = schema.get("required", [])
            tool_desc["function"]["parameters"] = {
                "type": "object",
                "properties": properties,
                "required": required,
            }
        else:
            tool_desc["function"]["parameters"] = {
                "type": "object",
                "properties": {},
            }
        return tool_desc


class ToolRegistry:
    """工具注册中心（单例）"""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._tools: Dict[str, ToolInfo] = {}
        return cls._instance

    def register(
        self,
        name: Optional[str] = None,
        description: str = "",
        args_schema: Optional[type[BaseModel]] = None,
    ) -> Callable:
        """注册工具的装饰器

        Args:
            name: 工具名称，默认使用函数名
            description: 工具描述
            args_schema: 参数 Pydantic 模型
        """
        def decorator(func: Callable) -> Callable:
            tool_name = name or func.__name__
            schema = args_schema or self._infer_schema(func)
            # 从 schema 提取必填参数（无默认值的字段）
            required_params = []
            if schema:
                schema_json = schema.model_json_schema()
                required_params = schema_json.get("required", [])
            self._tools[tool_name] = ToolInfo(
                name=tool_name,
                description=description or func.__doc__ or "",
                func=func,
                args_schema=schema,
                required_params=required_params,
            )
            params_info = ", ".join(schema.model_fields.keys()) if schema else "无参数"
            logger.info("工具已注册: [%s] %s | 参数: %s", tool_name, description, params_info)

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def _infer_schema(self, func: Callable) -> Optional[type[BaseModel]]:
        """从函数签名自动推断参数 schema"""
        sig = inspect.signature(func)
        hints = get_type_hints(func)
        fields = {}
        for name, param in sig.parameters.items():
            if name == "return":
                continue
            param_type = hints.get(name, str)
            default = ... if param.default is inspect.Parameter.empty else param.default
            # 处理 Optional 类型
            origin = getattr(param_type, "__origin__", None)
            if origin is type(None) or origin is Optional:
                args = getattr(param_type, "__args__", (str, None))
                param_type = args[0] if args else str
                if default is ...:
                    default = None
            fields[name] = (param_type, Field(default, description=f"参数 {name}"))
        if not fields:
            return None
        model_name = f"{func.__name__.replace('_', ' ').title().replace(' ', '')}Params"
        return create_model(model_name, **fields)

    def get_tool(self, name: str) -> Optional[ToolInfo]:
        """获取已注册的工具"""
        return self._tools.get(name)

    def get_required_params(self, name: str) -> list[str]:
        """获取工具的必填参数列表"""
        tool = self.get_tool(name)
        if not tool:
            return []
        return tool.required_params

    def check_missing_params(self, name: str, params: dict) -> list[str]:
        """检查哪些必填参数缺失

        Args:
            name: 工具名
            params: 已提供的参数字典

        Returns:
            缺失的必填参数名列表（空列表表示全部齐全）
        """
        required = self.get_required_params(name)
        return [p for p in required if p not in params or params[p] in (None, "", [])]

    def list_tools(self) -> list[dict]:
        """列出所有已注册的工具（LLM 格式）"""
        return [info.to_llm_description() for info in self._tools.values()]

    def list_tools_simple(self) -> list[dict]:
        """列出所有已注册的工具（简洁格式）"""
        return [
            {"name": info.name, "description": info.description}
            for info in self._tools.values()
        ]

    async def execute(self, tool_name: str, **kwargs) -> Any:
        """执行已注册的工具（自动处理同步/异步函数）"""
        tool = self.get_tool(tool_name)
        if not tool:
            raise ValueError(f"未找到工具: {tool_name}")
        result = tool.func(**kwargs)
        if result is not None and hasattr(result, "__await__"):
            result = await result
        return result

    def clear(self):
        """清空所有注册的工具"""
        self._tools.clear()


# 全局工具注册中心实例
tool_registry = ToolRegistry()
