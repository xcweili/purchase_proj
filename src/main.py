# -*- coding: utf-8 -*-
"""采购管理智能体服务 - 主入口"""
import json
import logging
import sqlite3
import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from fastapi.responses import StreamingResponse

logger = logging.getLogger(__name__)
app = FastAPI(title="采购管理智能体服务", version="10.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# 会话管理器导入
# ============================================
from .utils.session_manager import session_manager

# ============================================
# 服务层导入
# ============================================
from .services import real_db, llm_service

# ============================================
# 流式服务实例化
# ============================================
from .services import AllocationStreamService, InventoryAnalysisStreamService, SupplierMatchStreamService

allocation_stream_service = AllocationStreamService(real_db, llm_service.chat_stream, llm_service.chat)
inventory_analysis_stream_service = InventoryAnalysisStreamService(real_db, llm_service.chat_stream, llm_service.chat)
supplier_match_stream_service = SupplierMatchStreamService(real_db, llm_service.chat_stream, llm_service.chat)

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
    analyzeMode: str = Field(default="batch", description="分析模式：batch（批量）/iterative（迭代）")

class InventoryAnalysisRequest(BaseModel):
    startDate: Optional[str] = Field(default=None, description="开始日期（格式：YYYYMM）")
    endDate: Optional[str] = Field(default=None, description="结束日期（格式：YYYYMM）")
    inventoryLevels: Optional[List[str]] = Field(default=None, description="库存层级列表：区域库/周转库/终端库")
    materialCodes: Optional[List[str]] = Field(default=None, description="物料编码列表")
    seasonFactorWeight: Optional[float] = Field(default=None, description="季节因子权重")
    safetyRedundancyRatio: Optional[float] = Field(default=None, description="安全冗余比例")
    analyzeMode: str = Field(default="batch", description="分析模式：batch（批量）/iterative（迭代）")

class SupplierMatchRequest(BaseModel):
    plans: Optional[List[Dict[str, Any]]] = Field(default=None, description="补货计划列表")
    analyzeMode: str = Field(default="batch", description="分析模式：batch（批量）/iterative（迭代）")

class ChatRequest(BaseModel):
    message: str = Field(..., description="用户消息")

# ============================================
# 智能调配接口 - 流式版本
# ============================================
@app.post("/api/allocation/match/stream")
async def allocation_match_stream(request: AllocationMatchRequest):
    """智能调配接口 - 流式输出"""
    logger.info(request.strategy)
    logger.info(request.warehouseCode)
    logger.info(request.sourceType)
    logger.info(request.projectUnit)
    logger.info(request.demandStartDate)
    logger.info(request.demandEndDate)
    logger.info(request.planType)
    logger.info(request.materialCodes)
    logger.info(request.analyzeMode)

    # 创建会话
    session_id = session_manager.create_session("allocation")
    strategy_val = request.strategy if request.strategy else "time"

    async def response_generator():
        try:
            async for chunk in allocation_stream_service.stream_analyze(
                strategy=strategy_val,
                warehouse_code=request.warehouseCode or "",
                source_type=request.sourceType or "",
                project_unit=request.projectUnit or "",
                demand_start_date=request.demandStartDate or "",
                demand_end_date=request.demandEndDate or "",
                plan_type=request.planType or "",
                material_codes=request.materialCodes,
                analyze_mode=request.analyzeMode,
                session_id=session_id
            ):
                # 检查会话是否被取消
                if session_manager.is_session_cancelled(session_id):
                    yield "\n\n❌ 【会话已终止】用户主动取消了当前分析任务\n"
                    logger.info(f"[AllocationStream] 会话 {session_id} 已被终止")
                    break
                yield chunk
        except asyncio.CancelledError:
            logger.info(f"[AllocationStream] 流式响应被取消: {session_id}")
            yield "\n\n❌ 【会话已终止】连接已关闭\n"
        finally:
            # 清理会话
            session_manager.remove_session(session_id)
            logger.info(f"[AllocationStream] 会话已清理: {session_id}")

    return StreamingResponse(
        response_generator(),
        media_type="text/plain; charset=utf-8",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Accel-Buffering": "no",
            "Transfer-Encoding": "chunked",
            "X-Session-Id": session_id
        }
    )

# ============================================
# 库存分析接口 - 流式版本
# ============================================
@app.post("/api/inventory/analyze/stream")
async def inventory_analyze_stream(request: InventoryAnalysisRequest):
    """库存分析接口 - 流式输出"""
    logger.info(request.startDate)
    logger.info(request.endDate)
    logger.info(request.inventoryLevels)
    logger.info(request.materialCodes)
    logger.info(request.seasonFactorWeight)
    logger.info(request.safetyRedundancyRatio)
    logger.info(request.analyzeMode)

    # 创建会话
    session_id = session_manager.create_session("inventory")

    async def response_generator():
        try:
            async for chunk in inventory_analysis_stream_service.stream_analyze(
                start_date=request.startDate,
                end_date=request.endDate,
                inventory_levels=request.inventoryLevels,
                material_codes=request.materialCodes,
                season_factor_weight=request.seasonFactorWeight,
                safety_redundancy_ratio=request.safetyRedundancyRatio,
                analyze_mode=request.analyzeMode,
                session_id=session_id
            ):
                # 检查会话是否被取消
                if session_manager.is_session_cancelled(session_id):
                    yield "\n\n❌ 【会话已终止】用户主动取消了当前分析任务\n"
                    logger.info(f"[InventoryStream] 会话 {session_id} 已被终止")
                    break
                yield chunk
        except asyncio.CancelledError:
            logger.info(f"[InventoryStream] 流式响应被取消: {session_id}")
            yield "\n\n❌ 【会话已终止】连接已关闭\n"
        finally:
            # 清理会话
            session_manager.remove_session(session_id)
            logger.info(f"[InventoryStream] 会话已清理: {session_id}")

    return StreamingResponse(
        response_generator(),
        media_type="text/plain; charset=utf-8",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Accel-Buffering": "no",
            "Transfer-Encoding": "chunked",
            "X-Session-Id": session_id
        }
    )

# ============================================
# 供应商匹配接口 - 流式版本
# ============================================
@app.post("/api/supplier/match/stream")
async def supplier_match_stream(request: SupplierMatchRequest):
    """供应商匹配接口 - 流式输出"""
    logger.info(request.plans)

    # 创建会话
    session_id = session_manager.create_session("supplier")

    async def response_generator():
        try:
            async for chunk in supplier_match_stream_service.stream_analyze(
                input_plans=request.plans,
                analyze_mode=request.analyzeMode,
                session_id=session_id
            ):
                # 检查会话是否被取消
                if session_manager.is_session_cancelled(session_id):
                    yield "\n\n❌ 【会话已终止】用户主动取消了当前分析任务\n"
                    logger.info(f"[SupplierStream] 会话 {session_id} 已被终止")
                    break
                yield chunk
        except asyncio.CancelledError:
            logger.info(f"[SupplierStream] 流式响应被取消: {session_id}")
            yield "\n\n❌ 【会话已终止】连接已关闭\n"
        finally:
            # 清理会话
            session_manager.remove_session(session_id)
            logger.info(f"[SupplierStream] 会话已清理: {session_id}")

    return StreamingResponse(
        response_generator(),
        media_type="text/plain; charset=utf-8",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Accel-Buffering": "no",
            "Transfer-Encoding": "chunked",
            "X-Session-Id": session_id
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
# 会话管理接口
# ============================================
@app.post("/api/session/{session_id}/stop")
async def stop_session(session_id: str):
    """终止指定会话的流式生成

    Args:
        session_id: 会话ID

    Returns:
        终止结果
    """
    success = session_manager.cancel_session(session_id)
    if not success:
        raise HTTPException(status_code=404, detail="会话不存在或已结束")

    return {
        "code": 200,
        "message": "会话终止信号已发送",
        "data": {
            "sessionId": session_id,
            "stopped": True
        }
    }


@app.get("/api/session/{session_id}/status")
async def get_session_status(session_id: str):
    """获取会话状态

    Args:
        session_id: 会话ID

    Returns:
        会话状态信息
    """
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在或已结束")

    return {
        "code": 200,
        "message": "success",
        "data": {
            "sessionId": session.session_id,
            "agentType": session.agent_type,
            "isActive": session.is_active,
            "isCancelled": session.is_cancelled,
            "createdAt": session.created_at.isoformat() if session.created_at else None,
            "cancelledAt": session.cancelled_at.isoformat() if session.cancelled_at else None
        }
    }


# ============================================
# 健康检查接口
# ============================================
@app.get("/api/health")
async def health_check():
    """健康检查接口"""
    return {"status": "ok"}


# ============================================
# 应用启动和关闭事件
# ============================================
@app.on_event("startup")
async def startup_event():
    """应用启动时初始化"""
    await session_manager.start()
    logger.info("[Main] 应用启动完成，会话管理器已初始化")


@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时清理"""
    await session_manager.stop()
    logger.info("[Main] 应用关闭，会话管理器已停止")


# ============================================
# 静态文件服务
# ============================================
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def root():
    """首页 - 流式交互界面"""
    return FileResponse("static/index.html")
