# 采购智能助手 - LangGraph 多 Agent 框架

基于 **LangGraph** 的采购管理智能助手。核心链路为 **意图理解 → 计划生成 → 逐步骤执行 → 协调汇总**，支持**即时中断（interrupt / HITL）**、**回溯（rewind）**、**定点回放（replay）**，并支持**多工具协同**完成一个复杂需求。

## 架构总览

```
用户输入 → 意图理解(thinking) → PlannerAgent 计划生成(plan_created)
        → 逐步骤执行(plan_step_start / plan_step_result) → CoordinatorAgent 汇总(tool_result)
```

LangGraph 图结构（多 Agent 协同）：

```
START → planner ──→ execute ──→ execute ──→ ... ──→ finalize → END
                │                      │
                └──(无步骤)→ finalize ──┘
```

| Agent | 职责 |
|-------|------|
| **PlannerAgent** | 理解意图 + 基于现有工具列出执行计划（多步骤、可含多工具协同、敏感步骤标记 `need_confirm`） |
| **ExecutorAgent** | 图内 execute 节点，逐个执行计划步骤；支持 `interrupt()` 即时中断（HITL 确认 / 参数追问） |
| **CoordinatorAgent** | 无工具步骤的回答 + 多步骤结果汇总，生成最终答复 |

## 核心能力

- **先理解、后计划、再执行**：每次执行前先识别意图，再按现有工具生成计划，前端可看到完整计划列表，逐步执行并实时更新状态。
- **即时中断（interrupt）**：执行到需要人工确认或参数缺失时，LangGraph `interrupt()` 挂起图（检查点落 SQLite），前端弹出对话框，选择后 `Command(resume=...)` 恢复。
- **回溯（rewind）**：从计划中某一步重新执行，该步之后的结果被覆盖（原运行内回退）。
- **定点回放（replay）**：从指定步骤克隆出一个新的运行重新执行，原运行历史保留。
- **多 Agent 协同**：一个需求可能需要多个工具（如「采购100吨碳钢钢板」→ 先查库存，再批量采购），由 PlannerAgent 拆解为多步骤计划。

## 目录结构

```
src/
├── config/             # 配置（LLM 提供商、数据库路径）
├── core/               # 核心框架
│   ├── registry.py     # 工具注册中心（单例，支持 hidden 隐藏工具）
│   ├── intent.py       # 意图识别（混合方案：关键词 + LLM）
│   ├── planner.py      # 计划模型（Plan / PlanStep）
│   ├── agents.py       # PlannerAgent / CoordinatorAgent（Prompt + 计划生成）
│   ├── graph/          # LangGraph 编排
│   │   ├── state.py    # GraphState（TypedDict）
│   │   └── graph.py    # GraphRuntime（StateGraph + AsyncSqliteSaver 检查点）
│   └── agent.py        # （旧 LangChain 版，保留参考）
├── db/                 # 业务 SQLite 持久化层
│   ├── store.py        # RunStore：agent_runs / agent_steps / agent_events / agent_pending
│   └── ...
├── tools/              # 具体工具实现（@tool 装饰器，hidden 参数不对外暴露）
│   ├── base.py         # @tool 装饰器
│   ├── inventory_tools.py    # 库存查询工具
│   ├── purchase_tools.py     # 采购订单查询工具
│   └── approval_tools.py     # 采购审批工具（Human-in-the-Loop 示例 + 隐藏的 _do_purchase）
├── services/
│   ├── chat_service.py       # 对话服务（SSE 事件流 + resume/rewind/replay/stop）
│   └── inventory_service.py  # 库存业务逻辑
└── main.py             # FastAPI 应用入口
static/
└── index.html          # 前端页面（计划列表、确认/参数对话框、回溯/回放按钮）
```

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt

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
| GET | `/api/runs` | 列出最近运行 |
| GET | `/api/runs/{run_id}` | 获取运行详情（计划 + 步骤状态） |
| POST | `/api/runs/{run_id}/resume` | 恢复被中断的运行（HITL 选择 `"confirm"/"cancel"` 或补齐参数 dict） |
| POST | `/api/runs/{run_id}/rewind` | 回溯：从指定 `target_seq` 在原运行内重新执行 |
| POST | `/api/runs/{run_id}/replay` | 定点回放：从指定 `target_seq` 克隆新运行重新执行 |
| POST | `/api/runs/{run_id}/stop` | 即时中断：请求停止当前运行（步骤边界生效） |
| POST | `/api/confirm` | 兼容旧接口（带 `run_id` 时转发到 resume） |

## SSE 事件流

```
thinking_start → thinking_result → plan_created → plan_step_start
→ plan_step_result → (human_confirm | ask_params | step_result) → tool_result
→ run_paused / run_done → done
```

- `plan_created`：推送完整计划（run_id + 步骤列表），前端渲染常驻计划面板（可折叠）
- `plan_step_start / plan_step_result`：逐步骤状态实时更新（pending/running/completed/failed/interrupted/awaiting_confirm）
- `human_confirm`：采购等敏感操作等待人工确认（`interrupt` 即时中断）
- `ask_params`：必填参数缺失，向前端追问
- `run_paused`：图因 `interrupt` 挂起，等待 `resume`

## 双库设计

- **业务 SQLite（`agent_runtime.db`）**：`agent_runs` / `agent_steps` / `agent_events` / `agent_pending` 四表，支撑计划列表展示、状态实时更新、回溯/回放审计。路径在 [src/config/db_config.py](src/config/db_config.py) 中配置。
- **LangGraph 检查点（AsyncSqliteSaver）**：同一 SQLite 文件内的检查点表，支撑 `interrupt()` 中断恢复。

## 核心特性

### 1. 意图理解 + 计划生成

```
用户: "采购100吨碳钢钢板"
PlannerAgent: 意图=batch_purchase → 计划:
  1. 查询碳钢钢板当前库存 (query_inventory_by_name)
  2. 发起100吨碳钢钢板采购 (batch_purchase, need_confirm=true)
```

- 极简意图（打招呼/查时间）走关键词快速通道生成单步计划
- 其余（含复杂采购）由 LLM 基于现有工具列表生成多步骤计划
- 敏感操作（采购、删除等）由规划器标记 `need_confirm=true`

### 2. 即时中断 + 恢复（interrupt / HITL）

```
用户: "采购100吨碳钢钢板"
执行步骤2(batch_purchase) → 工具返回 _requires_confirm 信号
→ interrupt(payload) 挂起图（检查点落库）→ 前端弹出确认对话框
用户点击 [✅ 确认采购] → POST /api/runs/{run_id}/resume {"value":"confirm"}
→ Command(resume="confirm") 恢复 → 执行 _do_purchase → 生成订单号
```

### 3. 参数追问（ask_params）

当工具必填参数缺失时（如「查一下采购订单」缺 `order_id`），execute 节点 `interrupt()` 挂起图，前端弹出参数表单，提交后以 dict 恢复：

```
POST /api/runs/{run_id}/resume {"value":{"order_id":"PO-2025-001"}}
```

### 4. 回溯 / 定点回放

- **回溯**：`POST /api/runs/{run_id}/rewind {"target_seq":2}` —— 原运行内回退，步骤2之后重置为 pending，从步骤2重新执行。
- **回放**：`POST /api/runs/{run_id}/replay {"target_seq":2}` —— 克隆出新的 run（保留目标步之前的已完成结果），从步骤2重新执行，原运行历史保留。

### 5. 多工具协同

```
用户: "采购100吨碳钢钢板"
1. 查库存（query_inventory_by_name）→ 确认库存情况
2. 批量采购（batch_purchase）→ 生成采购申请
最终：CoordinatorAgent 汇总「库存 + 采购订单」生成完整答复
```

## 如何添加工具

```python
from src.tools import tool

@tool(description="查询供应商信息")
async def query_supplier(name: str) -> str:
    """查询供应商详情"""
    ...

# 内部工具（不暴露给规划器/前端，仅供系统流程调用）：
@tool(description="内部执行", hidden=True)
async def _internal_action(x: int) -> str:
    ...
```

在 [src/services/chat_service.py](src/services/chat_service.py) 顶部添加 `import src.tools.supplier_tools` 即可触发注册。

## LLM 提供商切换

编辑 [src/config/llm_config.py](src/config/llm_config.py)：

```python
CURRENT_LLM_PROVIDER = "deepseek"   # 可选: deepseek / hn_tongyi / js_tongyi
```

## 数据库配置

编辑 [src/config/db_config.py](src/config/db_config.py)：

```python
CURRENT_MYSQL_PROFILE = "local_dev"   # 业务库（可选: js / hn / local / local_dev）
AGENT_SQLITE_DB_PATH = 'agent_runtime.db'   # 智能体运行时 SQLite（计划/步骤/事件/检查点）
```
