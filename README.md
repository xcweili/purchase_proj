# 采购智能助手 - LangChain Agent 框架

基于 LangChain 的采购管理智能助手，核心能力为**意图识别 → 工具注册 → 工具调用**。

## 架构总览

```
用户输入 → IntentRecognizer(LLM) → ToolRegistry → 执行结果
                │                        │
         识别意图名+参数            匹配并调用注册的工具
```

- **config/** — 配置文件（LLM 提供商切换、数据库切换）
- **core/** — LangChain 核心框架（意图识别、工具注册中心、Agent 编排）
- **tools/** — 具体工具实现
- **services/** — 业务服务层

## 快速开始

```bash
# 启动服务
.venv/bin/uvicorn src.main:app --reload

# 默认运行在 http://localhost:8000
```

## API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 服务状态 |
| GET | `/tools` | 查看已注册工具列表 |
| POST | `/chat` | 对话接口，自动识别意图并调用工具 |

### 示例

```bash
curl -X POST http://localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "你好"}'
```

## 如何添加工具

### 方式一：装饰器（简单工具）

在 `ChatService._register_builtin_tools()` 中直接注册：

```python
@self.agent.register_tool(description="工具描述")
def my_tool(param1: str, param2: int = 0):
    """工具说明"""
    return f"结果: {param1}"
```

也可使用全局装饰器：

```python
from src.tools import tool

@tool(description="工具描述")
def my_tool(param1: str):
    return f"结果: {param1}"
```

### 方式二：独立文件（复杂工具）

见下方「复杂工具实现」一节。

## LLM 提供商切换

编辑 [src/config/llm_config.py](src/config/llm_config.py#L32) 第 32 行：

```python
CURRENT_LLM_PROVIDER = "hn_tongyi"   # 可选: deepseek / hn_tongyi / js_tongyi
```

## 数据库配置

编辑 [src/config/db_config.py](src/config/db_config.py#L20) 第 20 行：

```python
CURRENT_MYSQL_PROFILE = "local_dev"  # 可选: js / hn / local / local_dev
```

## 复杂工具实现

当工具逻辑变复杂时（如多步操作、数据库查询、外部 API 调用），不要写在 `ChatService` 中。结构如下：

```
src/tools/
├── __init__.py
├── base.py               # 工具基类 + @tool 装饰器
├── supplier_tools.py     # 供应商相关工具
├── inventory_tools.py    # 库存相关工具
├── allocation_tools.py   # 分配相关工具
└── ...
```

每个文件可定义一个或多个工具函数，再统一导入注册：

```python
# src/tools/supplier_tools.py
from src.tools import tool
from src.services.supplier_service import SupplierService

@tool(description="查询供应商信息，根据供应商名称搜索")
def query_supplier(name: str) -> str:
    """查询供应商详情"""
    svc = SupplierService()
    result = svc.search(name)
    return format_result(result)


# src/services/chat_service.py
from src.tools.supplier_tools import query_supplier  # 仅导入触发注册即可
```
