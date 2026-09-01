# -*- coding: utf-8 -*-
"""采购管理智能体服务 - 主入口"""
import asyncio
import json
import sqlite3
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from fastapi.responses import StreamingResponse


app = FastAPI(title="采购管理智能体服务", version="10.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# 服务层导入
# ============================================
from .services import real_db, llm_service
from .services import AllocationService
from .services import InventoryAnalysisService
from .services import SupplierMatchService
from .services.protocol_event_service import (
    protocol_event_service,
    new_task_id,
    EVENT_DONE,
)
from .services import protocol_match_client

# ============================================
# 智能体导入
# ============================================
from .agents.inventory_analysis_agent import InventoryAnalysisAgent
from .agents.supplier_match_agent import SupplierMatchAgent
from .agents.allocation_agent import AllocationAgent

# ============================================
# 智能体实例化
# ============================================
allocation_agent = AllocationAgent(real_db, llm_service.chat_stream, llm_service.chat)
inventory_analysis_agent = InventoryAnalysisAgent(real_db, llm_service.chat_stream, llm_service.chat)
supplier_match_agent = SupplierMatchAgent(real_db, llm_service.chat_stream, llm_service.chat)

# ============================================
# 中间层服务实例化
# ============================================
allocation_service = AllocationService(real_db, allocation_agent)
inventory_analysis_service = InventoryAnalysisService(real_db, inventory_analysis_agent)
supplier_match_service = SupplierMatchService(real_db, supplier_match_agent)

# ============================================
# 请求模型
# ============================================
class AllocationMatchRequest(BaseModel):
    strategy: str = Field(default="time", description="匹配策略：time/cost/stock/emerg")
    warehouseCode: str = Field(default="", description="仓库编码筛选")
    sourceType: str = Field(default="", description="库存类型筛选")
    projectUnit: str = Field(default="", description="项目单位")
    demandStartDate: str = Field(default="", description="需求开始时间")
    demandEndDate: str = Field(default="", description="需求结束时间")
    planType: str = Field(default="", description="计划类型")
    materialCodes: Optional[List[str]] = Field(default=None, description="物料编码列表")

class InventoryAnalysisRequest(BaseModel):
    startDate: Optional[str] = Field(default=None, description="开始日期（格式：YYYYMM）")
    endDate: Optional[str] = Field(default=None, description="结束日期（格式：YYYYMM）")
    inventoryLevels: Optional[List[str]] = Field(default=None, description="库存层级列表：区域库/周转库/终端库")
    materialCodes: Optional[List[str]] = Field(default=None, description="物料编码列表")
    seasonFactorWeight: Optional[float] = Field(default=None, description="季节因子权重")
    safetyRedundancyRatio: Optional[float] = Field(default=None, description="安全冗余比例")

class SupplierMatchRequest(BaseModel):
    plans: Optional[List[Dict[str, Any]]] = Field(default=None, description="补货计划列表")

class ChatRequest(BaseModel):
    message: str = Field(..., description="用户消息")

class ProtocolEventRequest(BaseModel):
    """客户代码 webhook 上报的事件（宽松模型，事件类型不同字段不同）"""
    event: str = Field(..., description="事件类型：step_start/step_update/step_end/reasoning/error/done")
    task_id: str = Field(..., description="任务ID（发起匹配时生成并传给客户，用于关联事件流）")
    step_id: Optional[str] = Field(default=None, description="步骤ID")
    title: Optional[str] = Field(default=None, description="步骤标题")
    desc: Optional[str] = Field(default=None, description="步骤说明")
    status: Optional[str] = Field(default=None, description="步骤状态：running/success/failed")
    summary: Optional[str] = Field(default=None, description="结果摘要")
    content: Optional[str] = Field(default=None, description="思考说明文字")
    progress: Optional[Any] = Field(default=None, description="进度信息（如 {current,total}）")
    result: Optional[Any] = Field(default=None, description="最终结果")
    cost_ms: Optional[float] = Field(default=None, description="步骤耗时（毫秒）")
    message: Optional[str] = Field(default=None, description="错误信息")

class SimulateRequest(BaseModel):
    task_id: Optional[str] = Field(default=None, description="任务ID，不传则自动生成")
    delay: float = Field(default=0.5, description="每步间隔秒数，用于模拟实时推进")

class ProtocolMatchRequest(BaseModel):
    """发起真实协议匹配（我们调用客户的匹配接口）"""
    task_id: Optional[str] = Field(default=None, description="任务ID，不传则自动生成")
    warehouseCode: str = Field(default="WH001", description="仓库编码（按客户接口schema传参）")
    planMonth: str = Field(default="2026-08", description="计划月份（按客户接口schema传参）")

# ============================================
# 智能调配接口
# ============================================
@app.post("/api/allocation/match")
async def allocation_match(request: AllocationMatchRequest):
    """智能调配接口"""
    strategy_val = request.strategy if request.strategy else ""
    return await allocation_service.process_allocation(
        strategy=strategy_val,
        warehouse_code=request.warehouseCode or "",
        source_type=request.sourceType or "",
        project_unit=request.projectUnit or "",
        demand_start_date=request.demandStartDate or "",
        demand_end_date=request.demandEndDate or "",
        plan_type=request.planType or "",
        material_codes=request.materialCodes
    )

# ============================================
# 库存分析接口
# ============================================
@app.post("/api/inventory/analyze")
async def inventory_analyze(request: InventoryAnalysisRequest):
    """库存分析接口 - 根据参数筛选仓库和物料进行分析"""
    return await inventory_analysis_service.analyze(
        start_date=request.startDate,
        end_date=request.endDate,
        inventory_levels=request.inventoryLevels,
        material_codes=request.materialCodes,
        season_factor_weight=request.seasonFactorWeight,
        safety_redundancy_ratio=request.safetyRedundancyRatio
    )

# ============================================
# 供应商匹配接口
# ============================================
@app.post("/api/supplier/match")
async def supplier_match(request: SupplierMatchRequest):
    """供应商匹配接口 - 遍历每个计划动态匹配"""
    return await supplier_match_service.match(
        input_plans=request.plans
    )

@app.get("/api/supplier/match/results")
async def get_supplier_match_results(
    material_code: str = None,
    match_status: str = None,
    strategy: str = None,
    limit: int = 100
):
    """查询供应商匹配结果"""
    conn = real_db._get_connection()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = '''
        SELECT id, fd_material_code, fd_material_desc, fd_match_status, fd_strategy,
               fd_company, fd_project_def, fd_project_desc,
               fd_demand_qty, fd_warehouse_code, fd_tech_spec_id,
               fd_supplier_results, fd_total_cost, fd_unmet_demand,
               fd_create_time
        FROM mt_supplier_match_result
        WHERE 1=1
    '''
    params = []

    if material_code:
        query += ' AND fd_material_code = %s'
        params.append(material_code)

    if match_status:
        query += ' AND fd_match_status = %s'
        params.append(match_status)

    if strategy:
        query += ' AND fd_strategy = %s'
        params.append(strategy)

    query += ' ORDER BY fd_create_time DESC LIMIT %s'
    params.append(limit)

    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    results = []
    for row in rows:
        results.append({
            'id': row['id'],
            'materialCode': row['fd_material_code'],
            'materialDesc': row['fd_material_desc'],
            'matchStatus': row['fd_match_status'],
            'strategy': row['fd_strategy'],
            'company': row['fd_company'],
            'projectDef': row['fd_project_def'],
            'projectDesc': row['fd_project_desc'],
            'demandQty': row['fd_demand_qty'],
            'warehouseCode': row['fd_warehouse_code'],
            'techSpecId': row['fd_tech_spec_id'],
            'supplierResults': json.loads(row['fd_supplier_results']) if row['fd_supplier_results'] else [],
            'totalCost': row['fd_total_cost'],
            'unmetDemand': row['fd_unmet_demand'],
            'createTime': row['fd_create_time']
        })

    return {
        'code': 200,
        'message': 'success',
        'data': {
            'total': len(results),
            'results': results
        }
    }

# ============================================
# 对话接口
# ============================================
@app.post("/api/chat/stream")
async def chat_stream(request: ChatRequest):
    """通用对话接口 - 流式输出"""
    system_prompt = "你是一个专业的电力物料储备、匹配、调配的专家，你需要根据你所掌握的知识，思考分析用户的问题，并进行回答。"

    async def response_generator():
        async for chunk in llm_service.chat_stream(request.message, system_prompt, messages=None):
            yield chunk

    return StreamingResponse(
        response_generator(),
        media_type="text/plain",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no"
        }
    )


# ============================================
# 协议匹配事件接口（形态B：客户代码事件上报 -> SSE转发前端）
# ============================================
@app.post("/api/protocol/events")
async def protocol_event_push(request: ProtocolEventRequest):
    """客户代码 webhook 事件上报接口

    客户匹配代码每执行完一个关键步骤，POST 一条事件到这里；
    服务将事件实时转发给订阅该 task_id 的 SSE 客户端。
    """
    event = request.model_dump(exclude_none=True)
    await protocol_event_service.publish(request.task_id, event)
    return {"code": 200, "message": "success"}


@app.get("/api/protocol/match/stream")
async def protocol_match_stream(task_id: str = Query(..., description="任务ID，订阅对应的事件流")):
    """SSE 事件流接口 - 前端订阅实时思考过程

    订阅后先补发该任务已发生的历史事件，再实时转发后续事件；
    收到 done 事件后正常关闭连接。
    """
    q = protocol_event_service.subscribe(task_id)

    async def event_generator():
        try:
            # 1. 新订阅者先补发历史事件（防止连接时已错过的事件丢失）
            for ev in protocol_event_service.history(task_id):
                yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"
                if ev.get("event") == EVENT_DONE:
                    return

            # 2. 实时转发后续事件
            while True:
                try:
                    ev = await asyncio.wait_for(q.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"  # 心跳保活
                    continue
                yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"
                if ev.get("event") == EVENT_DONE:
                    break
        finally:
            protocol_event_service.unsubscribe(task_id, q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/protocol/match")
async def protocol_match_start(request: ProtocolMatchRequest):
    """发起一次协议匹配（我们后端调用客户匹配接口）

    这是前端点"开始匹配"按钮时调用的接口，职责只有两件事：
    1. 带着 task_id 去调客户的匹配接口（真正匹配逻辑在客户那边跑，
       客户每走一步会上报事件到 /api/protocol/events）
    2. 等客户跑完，把最终结果随本接口响应一起返回给前端

    实时进度不经过本接口：前端先 GET /api/protocol/match/stream?task_id=xxx
    订阅 SSE，即可实时收到客户上报的思考过程事件，跑完收到 done。
    """
    task_id = request.task_id or new_task_id()
    try:
        result = await protocol_match_client.call_customer_match(
            task_id=task_id,
            warehouse_code=request.warehouseCode,
            plan_month=request.planMonth,
        )
    except Exception as e:
        # 客户接口调用失败：通知订阅者并返回错误，避免前端死等
        await protocol_event_service.publish(task_id, {
            "event": "error",
            "message": f"调用客户匹配接口失败: {e}",
        })
        return {"code": 500, "message": f"调用客户匹配接口失败: {e}", "data": {"task_id": task_id}}
    return {"code": 200, "message": "success", "data": {"task_id": task_id, "result": result}}


@app.post("/api/protocol/match/simulate")
async def protocol_match_simulate(request: SimulateRequest):
    """模拟一次协议匹配事件流（自测/演示用，不依赖客户）

    启动后台任务按标准流程逐步产生事件，前端订阅对应 task_id 即可看到完整思考过程。
    """
    task_id = request.task_id or new_task_id()
    protocol_event_service.start_simulate(task_id, request.delay)
    return {"code": 200, "message": "success", "data": {"task_id": task_id}}


# ============================================
# 健康检查接口
# ============================================
@app.get("/api/health")
async def health_check():
    """健康检查接口"""
    return {"status": "ok"}
