# -*- coding: utf-8 -*-
"""采购管理智能体服务 - 主入口"""
import json
import os
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

# ============================================
# 模拟返回配置
# ============================================
MOCK_RESPONSE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "api_protocol", "mock_responses")
MOCK_FILES = {
    "allocation": os.path.join(MOCK_RESPONSE_DIR, "allocation.md"),
    "inventory": os.path.join(MOCK_RESPONSE_DIR, "inventory.md"),
    "supplier": os.path.join(MOCK_RESPONSE_DIR, "supplier.md"),
}

async def mock_response_generator(agent_type: str):
    """读取模拟返回文件并按批返回（每批约10行，逐字流式输出）"""
    file_path = MOCK_FILES.get(agent_type)
    if not file_path or not os.path.exists(file_path):
        yield f"❌ 未找到 {agent_type} 的模拟返回文件\n"
        return

    batch_size = 10
    batch = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            batch.append(line)
            if len(batch) >= batch_size:
                yield "".join(batch)
                batch = []
                await asyncio.sleep(0.2)
    if batch:
        yield "".join(batch)

app = FastAPI(title="采购管理智能体服务", version="10.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from starlette.responses import Response
from starlette.types import ASGIApp, Scope, Receive, Send

class CORSPreflightMiddleware:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] == "http" and scope["method"] == "OPTIONS":
            response = Response(
                status_code=200,
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "*",
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Allow-Credentials": "true",
                    "Access-Control-Max-Age": "86400",
                    "Content-Length": "0",
                },
            )
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)

app.add_middleware(CORSPreflightMiddleware)

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
    mock: bool = Field(default=True, description="是否启用模拟返回模式，为true时直接读取模拟返回文件并逐字返回")

class InventoryAnalysisRequest(BaseModel):
    startDate: Optional[str] = Field(default=None, description="开始日期（格式：YYYYMMDD）")
    endDate: Optional[str] = Field(default=None, description="结束日期（格式：YYYYMMDD）")
    warehouseCode: str = Field(default="", description="仓库编码，为空时查询所有仓库")
    mock: bool = Field(default=True, description="是否启用模拟返回模式，为true时直接读取模拟返回文件并逐字返回")

class SupplierMatchRequest(BaseModel):
    mock: bool = Field(default=True, description="是否启用模拟返回模式，为true时直接读取模拟返回文件并逐字返回")

class ChatRequest(BaseModel):
    message: str = Field(..., description="用户消息")

# ============================================
# 智能调配接口 - 流式版本
# ============================================
@app.post("/api/allocation/match/stream")
async def allocation_match_stream(request: AllocationMatchRequest):
    """智能调配接口 - 流式输出"""
    logger.info(request.strategy)
    # 创建会话
    session_id = session_manager.create_session("allocation")
    strategy_val = request.strategy if request.strategy else "time"

    if request.mock:
        logger.info(f"[AllocationStream] 使用模拟返回模式")
        return StreamingResponse(
            mock_response_generator("allocation"),
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
    logger.info(request.warehouseCode)

    # 创建会话
    session_id = session_manager.create_session("inventory")

    if request.mock:
        logger.info(f"[InventoryStream] 使用模拟返回模式")
        return StreamingResponse(
            mock_response_generator("inventory"),
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

    async def response_generator():
        try:
            async for chunk in inventory_analysis_stream_service.stream_analyze(
                start_date=request.startDate,
                end_date=request.endDate,
                warehouse_code=request.warehouseCode,
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

    # 创建会话
    session_id = session_manager.create_session("supplier")

    if request.mock:
        logger.info(f"[SupplierStream] 使用模拟返回模式")
        return StreamingResponse(
            mock_response_generator("supplier"),
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

    async def response_generator():
        try:
            async for chunk in supplier_match_stream_service.stream_analyze(
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
