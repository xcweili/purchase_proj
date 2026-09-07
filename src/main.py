# -*- coding: utf-8 -*-
"""采购管理智能体服务 - 主入口"""
import json
import os
import logging
import sqlite3
import asyncio
import httpx
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
# 协议匹配服务导入
# ============================================
from .services.protocol_match_client import EVENT_DONE, new_task_id, KEEPALIVE
from .services.protocol_task_registry import protocol_task_registry, run_worker
from .services.mock_customer_manager import mock_customer_manager

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
    mock: bool = Field(default=False, description="是否启用模拟返回模式，为true时直接读取模拟返回文件并逐字返回")

class InventoryAnalysisRequest(BaseModel):
    startDate: Optional[str] = Field(default=None, description="开始日期（格式：YYYYMMDD）")
    endDate: Optional[str] = Field(default=None, description="结束日期（格式：YYYYMMDD）")
    warehouseCode: Optional[str] = Field(default=None, description="仓库编码，为空时查询所有仓库。传入此字段=水位线模式，不传=补库计划模式")
    majorCategory: str = Field(default="", description="物资大类编码，如'01'，空=全部（补库计划模式）")
    mediumCategory: str = Field(default="", description="物资中类编码，如'0101'，空=全部（补库计划模式）")
    smallCategory: str = Field(default="", description="物资小类编码，如'010101'，空=全部（补库计划模式）")
    mock: bool = Field(default=False, description="是否启用模拟返回模式，为true时直接读取模拟返回文件并逐字返回")

class SupplierMatchRequest(BaseModel):
    mock: bool = Field(default=False, description="是否启用模拟返回模式，为true时直接读取模拟返回文件并逐字返回")

class ChatRequest(BaseModel):
    message: str = Field(..., description="用户消息")

class ProtocolMatchRequest(BaseModel):
    task_id: Optional[str] = Field(default=None, description="任务ID，为空时自动生成；重复携带同一 task_id 调用会断线续传（不重复调用客户接口）")
    params: Optional[Dict[str, Any]] = Field(default=None, description="透传给客户接口的业务参数对象（任意属性，原样进入请求体）")
    url: Optional[str] = Field(default=None, description="客户流式接口地址；为空使用默认配置（测试页可填写真实/模拟地址）")

class CheckUrlRequest(BaseModel):
    url: str = Field(..., description="待检测的客户接口地址")

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
    """库存分析接口 - 流式输出
    根据参数自动判断模式：
    - 水位线模式：传入 warehouseCode（不含分类参数），AI分析历史出库数据生成水位线
    - 补库计划模式：传入 majorCategory/mediumCategory/smallCategory，确定性算法+LLM总结
    """
    # 判断模式：
    # - warehouseCode 在请求中存在 → 水位线模式（不管值为空还是非空）
    # - warehouseCode 不在请求中 → 补库计划模式
    is_replenishment_mode = (request.warehouseCode is None)
    mode_name = "补库计划" if is_replenishment_mode else "水位线"
    logger.info(f"[InventoryStream] 模式={mode_name}, warehouseCode={request.warehouseCode}, "
                f"majorCategory={request.majorCategory}, mediumCategory={request.mediumCategory}, smallCategory={request.smallCategory}")

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
            if is_replenishment_mode:
                # 补库计划模式
                async for chunk in inventory_analysis_stream_service.stream_analyze_replenishment_plan(
                    major_category=request.majorCategory,
                    medium_category=request.mediumCategory,
                    small_category=request.smallCategory,
                    start_date=request.startDate,
                    end_date=request.endDate,
                    session_id=session_id
                ):
                    if session_manager.is_session_cancelled(session_id):
                        yield "\n\n❌ 【会话已终止】用户主动取消了当前分析任务\n"
                        logger.info(f"[InventoryStream] 会话 {session_id} 已被终止")
                        break
                    yield chunk
            else:
                # 水位线模式
                async for chunk in inventory_analysis_stream_service.stream_analyze_water_level(
                    start_date=request.startDate,
                    end_date=request.endDate,
                    warehouse_code=request.warehouseCode,
                    session_id=session_id
                ):
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
    mock_customer_manager.stop()
    logger.info("[Main] 应用关闭，会话管理器已停止")


# ============================================
# 协议匹配接口（单接口流式代理，支持断线重连续传）
# ============================================
@app.post("/api/protocol/match")
async def protocol_match(request: ProtocolMatchRequest):
    """发起协议匹配并实时转发客户过程事件（单一接口，支持断线重连）

    - 新 task_id：调用客户流式接口一次（地址默认 127.0.0.1:8100，
      可由请求体 url 指定），事件实时透传；
    - 已存在且执行中的 task_id：不再次调用客户接口，直接从上次读到的位置续传
      （断线期间新产生的事件会补发），前端刷新/关窗后重新调用即可"接着看"；
    - 已结束的 task_id：回放完整历史事件，方便查看结果。

    匹配过程可能长达 20 分钟以上：读超时不设上限、心跳保活（详见
    protocol_match_client）；任务后台运行，与前端连接解耦。
    """
    task_id = request.task_id or new_task_id()

    task = protocol_task_registry.get(task_id)
    if task is None:
        payload = {"task_id": task_id}
        payload.update(request.params or {})  # 客户端对象属性透传客户接口
        task = protocol_task_registry.create(task_id, payload, url=request.url)
        task.worker = asyncio.create_task(run_worker(task))
        mode = "new"
        logger.info("[Protocol] 新任务启动，调用客户接口 url=%s task_id=%s params=%s",
                    request.url or "默认", task_id, payload)
    elif task.done:
        mode = "finished"
        logger.info("[Protocol] 任务已结束，回放历史事件 task_id=%s err=%s",
                    task_id, task.error)
    else:
        mode = "reconnect"
        logger.info("[Protocol] 任务执行中，直接续传（不再调用8100）task_id=%s 已读%d条",
                    task_id, task.consumed)

    async def event_generator():
        # 已结束的任务从 0 回放（方便刷新后查看完整结果）；执行中的从上次游标续传
        start_pos = 0 if task.done else task.consumed
        count = 0
        # 先给前端一个连接元事件，方便页面区分 新建/重连/已结束
        yield f"data: {json.dumps({'event': 'conn', 'task_id': task_id, 'mode': mode}, ensure_ascii=False)}\n\n"
        try:
            async for item in task.events_from(start_pos):
                if item is KEEPALIVE:
                    yield ": keepalive\n\n"
                else:
                    yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
                    if item.get("event") == EVENT_DONE:
                        break
                count += 1
        except asyncio.CancelledError:
            logger.info("[Protocol] 客户端断开，任务后台继续运行 task_id=%s 本次已读%d条",
                        task_id, count)
            raise
        finally:
            task.consumed = max(task.consumed, start_pos + count)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/protocol/check-url")
async def check_customer_url(request: CheckUrlRequest):
    """检测客户接口地址是否可访问（只做连通性探测，不触发匹配流程）

    说明：对目标地址发一次 GET（短超时），只要拿到任何 HTTP 响应
    （含 4xx/5xx，如 405 表示接口存在但方法不允许）即视为可达；
    只有建连失败/超时等网络错误才算不可达。
    """
    url = request.url.strip()
    if not url.startswith(("http://", "https://")):
        url = "http://" + url
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(url)
        logger.info("[Protocol] 连通性检测 url=%s status=%s", url, resp.status_code)
        return {
            "code": 200,
            "message": "success",
            "data": {"reachable": True, "status": resp.status_code},
        }
    except Exception as e:  # noqa: BLE001 - 探测失败返回给页面展示原因
        logger.warning("[Protocol] 连通性检测失败 url=%s err=%s", url, e)
        return {
            "code": 200,
            "message": "success",
            "data": {"reachable": False, "error": str(e)},
        }


# ============================================
# 模拟客户服务生命周期控制接口（供测试页面使用）
# ============================================
@app.get("/api/mock/customer/status")
async def mock_customer_status():
    """查询模拟客户服务运行状态"""
    return {"code": 200, "message": "success", "data": await asyncio.to_thread(mock_customer_manager.status)}


@app.post("/api/mock/customer/start")
async def mock_customer_start():
    """启动模拟客户服务（127.0.0.1:8100）"""
    try:
        data = await asyncio.to_thread(mock_customer_manager.start)
    except Exception as e:  # noqa: BLE001 - 启动失败时给前端明确提示
        return {"code": 500, "message": str(e), "data": None}
    return {"code": 200, "message": "success", "data": data}


@app.post("/api/mock/customer/stop")
async def mock_customer_stop():
    """停止模拟客户服务"""
    return {"code": 200, "message": "success", "data": await asyncio.to_thread(mock_customer_manager.stop)}


# ============================================
# 静态文件服务
# ============================================
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/favicon.ico")
async def favicon():
    return FileResponse("static/favicon.ico") if os.path.exists("static/favicon.ico") else Response(status_code=204)

@app.get("/purchase")
async def root():
    """首页 - 流式交互界面"""
    return FileResponse("static/index.html")

@app.get("/division")
async def root():
    """首页 - 流式交互界面"""
    return FileResponse("static/division.html")

@app.get("/protocol-test")
async def protocol_test():
    """协议匹配联调测试页面"""
    return FileResponse("static/protocol_test.html")
