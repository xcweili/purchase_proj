# -*- coding: utf-8 -*-
"""采购管理智能体服务 - 主入口"""
import json
import sqlite3
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
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
# 流式服务实例化
# ============================================
from .services import AllocationStreamService, InventoryAnalysisStreamService, SupplierMatchStreamService

allocation_stream_service = AllocationStreamService(real_db, llm_service.chat_stream)
inventory_analysis_stream_service = InventoryAnalysisStreamService(real_db, llm_service.chat_stream)
supplier_match_stream_service = SupplierMatchStreamService(real_db, llm_service.chat_stream)

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
# 智能调配接口 - 流式版本
# ============================================
@app.post("/api/allocation/match/stream")
async def allocation_match_stream(request: AllocationMatchRequest):
    """智能调配接口 - 流式输出"""
    strategy_val = request.strategy if request.strategy else "time"
    
    async def response_generator():
        async for chunk in allocation_stream_service.stream_analyze(
            strategy=strategy_val,
            warehouse_code=request.warehouseCode or "",
            source_type=request.sourceType or "",
            project_unit=request.projectUnit or "",
            demand_start_date=request.demandStartDate or "",
            demand_end_date=request.demandEndDate or "",
            plan_type=request.planType or "",
            material_codes=request.materialCodes
        ):
            yield chunk
    
    return StreamingResponse(
        response_generator(),
        media_type="text/plain; charset=utf-8",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Accel-Buffering": "no",
            "Transfer-Encoding": "chunked"
        }
    )

# ============================================
# 库存分析接口 - 流式版本
# ============================================
@app.post("/api/inventory/analyze/stream")
async def inventory_analyze_stream(request: InventoryAnalysisRequest):
    """库存分析接口 - 流式输出"""
    
    async def response_generator():
        async for chunk in inventory_analysis_stream_service.stream_analyze(
            start_date=request.startDate,
            end_date=request.endDate,
            inventory_levels=request.inventoryLevels,
            material_codes=request.materialCodes,
            season_factor_weight=request.seasonFactorWeight,
            safety_redundancy_ratio=request.safetyRedundancyRatio
        ):
            yield chunk
    
    return StreamingResponse(
        response_generator(),
        media_type="text/plain; charset=utf-8",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Accel-Buffering": "no",
            "Transfer-Encoding": "chunked"
        }
    )

# ============================================
# 供应商匹配接口 - 流式版本
# ============================================
@app.post("/api/supplier/match/stream")
async def supplier_match_stream(request: SupplierMatchRequest):
    """供应商匹配接口 - 流式输出"""
    
    async def response_generator():
        async for chunk in supplier_match_stream_service.stream_analyze(
            input_plans=request.plans
        ):
            yield chunk
    
    return StreamingResponse(
        response_generator(),
        media_type="text/plain; charset=utf-8",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Accel-Buffering": "no",
            "Transfer-Encoding": "chunked"
        }
    )

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
# 健康检查接口
# ============================================
@app.get("/api/health")
async def health_check():
    """健康检查接口"""
    return {"status": "ok"}

# ============================================
# 静态文件服务
# ============================================
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def root():
    """首页 - 流式交互界面"""
    return FileResponse("static/index.html")
