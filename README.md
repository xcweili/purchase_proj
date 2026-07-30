# 采购智能助手 - LangChain Agent 框架

基于 LangChain 的采购管理智能助手，核心能力为**意图识别 → 工具注册 → 工具调用**。

## 架构总览

```
用户输入 → 关键词快速匹配 ──→ LLM 意图识别 ──→ 参数检查 ──→ 工具执行 ──→ 返回结果
               │                    │              │
            命中即返回           兜底精确识别   缺失则多轮追问
                                               需要确认则弹出 HITL 对话框
```

## 目录结构

```
src/
├── config/             # 配置文件（LLM 提供商、数据库）
├── core/               # LangChain 核心框架
│   ├── registry.py     # 工具注册中心（单例）
│   ├── intent.py       # 意图识别（混合方案：关键词 + LLM）
│   └── agent.py        # Agent 编排器（含多轮补齐 + HITL）
├── tools/              # 具体工具实现
│   ├── base.py         # @tool 装饰器
│   ├── inventory_tools.py    # 库存查询工具
│   ├── purchase_tools.py     # 采购订单查询工具
│   └── approval_tools.py     # 采购审批工具（Human-in-the-Loop 示例）
├── services/           # 业务服务层
│   ├── chat_service.py       # 对话服务（含 pending 状态管理）
│   └── inventory_service.py  # 库存业务逻辑
└── main.py             # FastAPI 应用入口
static/
└── index.html          # 前端页面
```

## 快速开始

```bash
# 启动服务
.venv/bin/uvicorn src.main:app --reload

# 默认运行在 http://localhost:8000
```

## API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 前端页面 |
| GET | `/api/tools` | 查看已注册工具列表 |
| POST | `/api/chat` | 对话接口（非流式） |
| POST | `/api/chat/stream` | 对话接口（SSE 流式） |
| POST | `/api/confirm` | 人工确认接口（HITL） |

## 核心特性

### 1. 混合意图识别

```
用户输入 → 第一层：关键词快速匹配（零延迟）
        → 第二层：LLM 结构化输出（高准确率兜底）
```

- 关键词匹配成功且参数有效 → 直接返回，不调 LLM
- 关键词匹配失败或参数含停用词 → 交给 LLM 精确识别
- 内置停用词过滤（现在、多少、查询等），避免误匹配

### 2. 多轮参数补齐

当工具需要必填参数而用户未提供时，Agent 自动追问：

```
用户: "查一下采购订单"
Agent: "请提供订单编号"

用户: "PO-2025-001"
Agent: 返回订单详情
```

**实现原理：** 函数参数无默认值即标记为必填参数，无需额外配置。

```python
@tool(description="查询采购订单")
async def query_purchase_order(order_id: str):   # ← 无默认值=必填
    ...

def greet(name: str = "朋友"):                    # ← 有默认值=可选
    ...
```

### 3. Human-in-the-Loop（人工介入确认）

当工具执行到需要人工判断的节点时，弹出确认对话框：

```
用户: "我要采购100吨碳钢钢板"
Agent: 计算总价 ¥450,000 → 弹出确认对话框
       ┌─────────────────────┐
       │  📋 采购申请单        │
       │  物资: 碳钢钢板       │
       │  数量: 100吨         │
       │  总金额: ¥450,000    │
       │                      │
       │  [✅ 确认采购] [❌ 取消] │
       └─────────────────────┘

用户点击"确认采购":
Agent: 执行采购，返回订单号
```

**实现流程：**

1. 工具返回 `{"_requires_confirm": True, ...}` 信号
2. Agent 检测到信号 → 前端弹出对话框
3. 用户选择 → `POST /api/confirm` → 后端执行确认后的操作

## 如何添加工具

### 方式一：装饰器（简单工具）

```python
@self.agent.register_tool(description="工具描述")
def my_tool(param1: str, param2: int = 0):
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

```python
# src/tools/supplier_tools.py
from src.tools import tool
from src.services.supplier_service import SupplierService

@tool(description="查询供应商信息")
async def query_supplier(name: str) -> str:
    """查询供应商详情"""
    svc = SupplierService()
    result = await svc.search(name)
    return format_result(result)


# src/services/chat_service.py 顶部添加导入
import src.tools.supplier_tools   # 导入即触发注册
```

### 工具参数控制

| 参数签名 | 效果 |
|---------|------|
| `def tool(name: str)` | name 为必填，缺失时自动追问 |
| `def tool(name: str = "默认值")` | name 可选，用户没说就用默认值 |
| 返回 `{"_requires_confirm": True, ...}` | 触发 HITL 确认对话框 |

## LLM 提供商切换

编辑 [src/config/llm_config.py](src/config/llm_config.py#L32) 第 32 行：

```python
CURRENT_LLM_PROVIDER = "deepseek"   # 可选: deepseek / hn_tongyi / js_tongyi
```

## 数据库配置

编辑 [src/config/db_config.py](src/config/db_config.py#L20) 第 20 行：

```python
CURRENT_MYSQL_PROFILE = "local_dev"  # 可选: js / hn / local / local_dev
```
