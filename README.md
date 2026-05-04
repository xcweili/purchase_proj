# 采购管理智能体服务 V10.0

## 项目简介

本项目是一个基于 FastAPI 的电力物料采购管理智能体服务，主要实现三大核心功能：

1. **智能调配** - 根据计划需求从现有库存进行最优仓库调配
2. **库存分析** - 基于历史出库数据进行库存分析和预警
3. **供应商匹配** - 根据协议库存和供应商信息进行供应商匹配

项目采用分层架构设计，包含智能体层、服务层、数据层分离，确保业务逻辑清晰可维护。

---

## 项目结构

```
purchase_proj/
├── src/
│   ├── __init__.py
│   ├── main.py                          # FastAPI主入口，API路由定义
│   ├── services/
│   │   ├── __init__.py
│   │   ├── real_db.py                   # 数据库连接服务
│   │   ├── llm_service.py             # LLM服务（智能体调用服务
│   │   ├── allocation_service.py      # 调配服务
│   │   ├── inventory_analysis_service.py   # 库存分析服务
│   │   └── supplier_match_service.py # 供应商匹配服务
│   └── agents/
│       ├── __init__.py
│       ├── inventory_analysis_agent.py     # 库存分析智能体
│       ├── supplier_match_agent.py       # 供应商匹配智能体
│       └── allocation_agent.py       # 调配智能体
├── purchase_management.db                # SQLite数据库文件
├── requirements.txt                   # Python依赖包
├── 数据库表结构说明.md           # 数据库表详细说明
└── README.md                      # 本文件
```

---

## 环境准备

### 后端环境
1. **Python环境**：Python 3.8+
2. **依赖包安装**：
```bash
pip install -r requirements.txt
```

### 启动服务
```bash
# 开发模式（带热重载）
python -m uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload

# 生产模式
python -m uvicorn src.main:app --host 0.0.0.0 --port 8000
```

服务默认访问地址：http://localhost:8000

---

## 数据库架构

数据库文件：`purchase_management.db`（SQLite）

### 核心数据表

| 表名 | 用途 | 关键字段 |
|------|------|----------|
| **mt_base_warehouse_info | 仓库信息 | fd_warehouse_code, fd_warehouse_name, fd_warehouse_type |
| **mt_stock_use_list_plan_two | 领用单计划 | fd_plan_id, fd_material_code, fd_requisition_num, fd_requisition_date |
| **w_stock_info_0808 | 库存信息 | material_code, stock_qty, loc_code, source_type |
| **mt_allocation_result** | 调配结果 | fd_plan_id (唯一索引), fd_strategy, fd_source_type, fd_project_unit, fd_demand_time |
| **mt_supplier_match_result** | 供应商匹配结果 | fd_material_code, fd_supplier_results |
| **mt_historical_outbound** | 历史出库 | fd_material_code, fd_warehouse_code, fd_outbound_qty, fd_posting_month |
| **mt_historical_analysis** | 历史分析结果 | fd_material_code, fd_warehouse_code, fd_tech_id, fd_historical_avg_qty |
| **mt_protocol_stock** | 协议库存 | fd_material_code, fd_supplier_name, fd_available_qty |
| **mt_framework_agreement** | 框架协议 | fd_agreement_no, fd_supplier, fd_execute_rate |

详细表结构说明请参考 [数据库表结构说明.md](./数据库表结构说明.md)

---

## API 接口说明

---

### 1. 智能调配接口

#### POST /api/allocation/match

**功能说明**：根据计划需求和现有库存进行智能仓库调配，支持多种匹配策略。

**请求参数（Body (JSON)**
| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| strategy | string | 否 | 匹配策略：`time`(按时间)、`cost`(按成本)、`stock`(按库存)、`emerg`(应急)，默认 `time` |
| warehouseCode | string | 否 | 仓库编码筛选 |
| sourceType | string | 否 | 库存类型筛选 |
| projectUnit | string | 否 | 项目单位筛选 |
| demandStartDate | string | 否 | 需求开始时间（格式 YYYY-MM-DD） |
| demandEndDate | string | 否 | 需求结束时间（格式 YYYY-MM-DD） |
| planType | string | 否 | 计划类型筛选 |
| materialCodes | array[string] | 否 | 物料编码列表，不传则查询所有计划 |

**请求示例**
```json
{
  "strategy": "time",
  "materialCodes": ["500050546"]
}
```

**处理流程**
1. 从 `mt_stock_use_list_plan_two` 表查询符合条件的计划
   - 查询条件：项目单位、需求时间范围、计划类型、物料编码
   - 关键字段：fd_plan_id, fd_material_code, fd_requisition_num, fd_unit_name, fd_requisition_date, fd_Item_Type
2. 从 `w_stock_info_0808` 表查询库存信息
   - 查询条件：物料编码列表、仓库编码、库存类型
   - 关键字段：material_code, stock_qty, loc_code, loc_name, source_type
3. 构建物料库存匹配规则，为每个计划匹配最优仓库
4. 根据策略匹配库存，计算匹配得分
5. 保存结果到 `mt_allocation_result` 表
   - 使用 INSERT OR REPLACE，fd_plan_id为唯一索引
   - 字段：fd_plan_id, fd_strategy, fd_source_type, fd_project_unit, fd_demand_time, fd_create_time

**响应示例**
```json
{
  "code": 200,
  "message": "success",
  "data": {
    "total": 10,
    "fullMatchCount": 5,
    "partialMatchCount": 3,
    "noneMatchCount": 2,
    "avgScore": 85.5,
    "suggestion": "建议优先处理完全匹配的5条计划",
    "results": [...]
  }
}
```

---

### 2. 库存分析接口

#### POST /api/inventory/analyze

**功能说明**：基于历史出库数据进行库存分析，支持按时间、仓库层级、物料编码筛选分析。

**请求参数（Body (JSON)**
| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| startDate | string | 否 | 开始日期（格式 YYYYMM），用于筛选历史出库数据 |
| endDate | string | 否 | 结束日期（格式 YYYYMM），用于筛选历史出库数据 |
| inventoryLevels | array[string] | 否 | 库存层级列表，如 ['区域库', '周转库', '终端库'] |
| materialCodes | array[string] | 否 | 物料编码列表 |
| seasonFactorWeight | float | 否 | 季节因子权重（暂未使用） |
| safetyRedundancyRatio | float | 否 | 安全冗余比例（暂未使用） |

**请求示例**
```json
{
  "startDate": "202401",
  "endDate": "202412",
  "inventoryLevels": ["区域库"],
  "materialCodes": ["500050546"]
}
```

**处理流程**
1. 从 `mt_base_warehouse_info` 表按库存层级筛选仓库列表
   - 查询条件：仓库类型
2. 获取物料编码列表（用户输入或从历史出库表获取全部）
3. 仓库 × 物料 × 技术规范ID 交叉组合，查询该组合在 `mt_historical_analysis` 表有数据的组合
4. 对每个组合查询历史出库分析数据
5. 查询 `w_stock_info_0808 表获取当前库存
6. 计算库存预警指标
7. 保存分析结果

**查询的表**
- `mt_base_warehouse_info`
- `mt_historical_outbound`
- `mt_historical_analysis`
- `w_stock_info_0808`

**响应示例**
```json
{
  "code": 200,
  "message": "success",
  "data": {
    "total": 50,
    "results": [...]
  }
}
```

---

### 3. 供应商匹配接口

#### POST /api/supplier/match

**功能说明**：根据协议库存和供应商信息，为补货计划匹配最优供应商，返回三种策略方案。

**请求参数（Body (JSON)**
| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| plans | array[object] | 否 | 补货计划列表，不传则从数据库查询 |
| plans[].planId | string | 否 | 计划ID |
| plans[].materialCode | string | 是 | 物料编码 |
| plans[].materialDesc | string | 否 | 物料描述 |
| plans[].demandQty | float | 是 | 需求数量 |
| plans[].warehouseCode | string | 否 | 仓库编码 |
| plans[].techSpecId | string | 否 | 技术规范ID |

**请求示例**
```json
{
  "plans": [
    {
      "planId": "PLAN001",
      "materialCode": "500050546",
      "materialDesc": "接地铜排,35mm2",
      "demandQty": 100,
      "warehouseCode": "WH001",
      "techSpecId": "TS001"
    }
  ]
}
```

**处理流程**
1. 从 `mt_stock_use_list_plan_two` 表查询计划（如果没传 plans）
2. 对每个计划：
   - 从 `w_stock_info_0808` 表获取物料描述
   - 从 `mt_base_warehouse_info` 表获取所属单位
   - 从 `mt_protocol_stock` 表查询该物料的协议供应商库存
3. 调用 LLM 智能体生成三种策略方案：
   - balanced（均衡策略）
   - cost（成本优先）
   - delivery（交期优先）
4. 保存结果到 `mt_supplier_match_result` 表

**查询的表**
- `mt_stock_use_list_plan_two`
- `mt_base_warehouse_info
- `mt_protocol_stock`
- `mt_framework_agreement`
- `mt_protocol_execution`

**响应示例**
```json
{
  "code": 200,
  "message": "success",
  "data": {
    "results": [
      {
        "planId": "PLAN001",
        "materialCode": "500050546",
        "demandQty": 100,
        "strategies": {
          "balanced": {...},
          "cost": {...},
          "delivery": {...}
        }
      }
    ]
  }
}
```

---

### 4. 查询供应商匹配结果接口

#### GET /api/supplier/match/results

**功能说明**：查询已保存的供应商匹配结果。

**请求参数（Query）**
| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| material_code | string | 否 | 物料编码筛选 |
| match_status | string | 否 | 匹配状态筛选 |
| strategy | string | 否 | 策略筛选 |
| limit | int | 否 | 限制返回数量，默认 100 |

**请求示例**
```
GET /api/supplier/match/results?material_code=500050546&limit=50
```

**查询的表**
- `mt_supplier_match_result`

**响应示例**
```json
{
  "code": 200,
  "message": "success",
  "data": {
    "total": 10,
    "results": [...]
  }
}
```

---

### 5. 通用对话接口

#### POST /api/chat/stream

**功能说明**：流式对话接口，调用 LLM 进行通用对话。

**请求参数（Body (JSON)**
| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| message | string | 是 | 用户消息 |

**请求示例**
```json
{
  "message": "如何进行库存预警分析？"
}
```

**响应**：流式文本流（text/plain）

---

### 6. 健康检查接口

#### GET /api/health

**功能说明**：服务健康检查接口。

**响应示例**
```json
{
  "status": "ok"
}
```

---

## 接口流程总结

| 接口 | 查询的表 | 保存的表 | 功能 |
|------|----------|----------|------|
| `/api/allocation/match | mt_stock_use_list_plan_two, w_stock_info_0808 | mt_allocation_result | 智能调配 |
| `/api/inventory/analyze | mt_base_warehouse_info, mt_historical_outbound, mt_historical_analysis, w_stock_info_0808 | - | 库存分析 |
| `/api/supplier/match` | mt_stock_use_list_plan_two, mt_base_warehouse_info, mt_protocol_stock | mt_supplier_match_result | 供应商匹配 |
| `/api/supplier/match/results` | mt_supplier_match_result | - | 查询匹配结果 |

---

## CURL 测试示例

### 测试智能调配接口
```bash
curl -X POST "http://localhost:8000/api/allocation/match" \
  -H "Content-Type: application/json" \
  -d '{"strategy":"time","materialCodes":["500050546"]}'
```

### 测试库存分析接口
```bash
curl -X POST "http://localhost:8000/api/inventory/analyze" \
  -H "Content-Type: application/json" \
  -d '{"startDate":"202401","endDate":"202412"}'
```

### 测试供应商匹配接口
```bash
curl -X POST "http://localhost:8000/api/supplier/match" \
  -H "Content-Type: application/json" \
  -d '{"plans":[{"materialCode":"500050546","demandQty":100}]}'
```

### 测试供应商匹配结果查询
```bash
curl "http://localhost:8000/api/supplier/match/results?material_code=500050546&limit=10"
```

---

## 配置说明

### LLM 配置（src/services/llm_service.py）
- 模型：`qwen3.5-122b-a10b-fp8`
- API 配置：根据实际环境修改

---

## 开发注意事项

1. **数据库文件**：`purchase_management.db` 为 SQLite 数据库，请确保有读写权限
2. **mt_allocation_result 表**：fd_plan_id 为唯一索引，重复插入时会覆盖旧记录
3. **字段映射**：
   - fd_project_unit 来自 mt_stock_use_list_plan_two.fd_unit_name
   - fd_demand_time 来自 mt_stock_use_list_plan_two.fd_requisition_date
   - fd_plan_type 来自 mt_stock_use_list_plan_two.fd_Item_Type

---

## 版本历史

| 版本 | 日期 | 说明 |
|------|------|------|
| V10.0 | 2026-05-02 | 当前版本 |

