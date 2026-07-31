# -*- coding: utf-8 -*-
"""
FastAPI 应用入口
基于 LangGraph 的对话接口（流式/非流式）+ 运行管理（回溯/回放/恢复/停止）
"""
import os
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.services.chat_service import ChatService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================
# 全局服务实例
# ============================================
chat_service = ChatService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    logger.info("应用启动 - 初始化 LangGraph Agent...")
    await chat_service.initialize()
    tools = chat_service.get_available_tools()
    logger.info("Agent 初始化完成，共注册 %d 个工具:", len(tools))
    for t in tools:
        logger.info("  [%s] %s", t["name"], t["description"])
    yield
    logger.info("应用关闭")


app = FastAPI(
    title="采购智能助手",
    description="基于 LangGraph 的采购管理智能助手（意图理解 → 计划编排 → 多Agent 协同执行）",
    version="0.2.0",
    lifespan=lifespan,
)


# ============================================
# 请求/响应模型
# ============================================
class ChatRequest(BaseModel):
    message: str
    history: list[dict] = []


class ConfirmRequest(BaseModel):
    confirm_id: str
    choice: str
    run_id: str | None = None


class ResumeRequest(BaseModel):
    value: str | dict | int | float | bool | None = None


class RestartRequest(BaseModel):
    target_seq: int


class ChatResponse(BaseModel):
    intent: str
    confidence: float
    result: str
    error: str | None = None
    reasoning: str = ""


# ============================================
# 对话接口
# ============================================
@app.post("/api/chat")
async def chat(request: ChatRequest):
    """对话接口 - 非流式"""
    result = await chat_service.process(request.message, history=request.history)
    return result


@app.post("/api/chat/stream")
async def chat_stream(raw: Request):
    """对话接口 - 流式（SSE）"""
    body = await raw.json()
    message = body.get("message", "")
    history = body.get("history", [])

    async def event_generator():
        async for sse_event in chat_service.process_stream(message, history=history):
            yield sse_event

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/tools")
async def list_tools():
    """获取已注册的工具列表"""
    return {"tools": chat_service.get_available_tools()}


# ============================================
# 运行管理（回溯 / 定点回放 / 中断恢复 / 停止）
# ============================================
@app.post("/api/confirm")
async def confirm(request: ConfirmRequest):
    """人工确认接口（HITL），恢复被中断的 LangGraph 运行"""
    if request.run_id:
        return await chat_service.resume_run(request.run_id, request.choice)
    return {"result": "缺少 run_id，无法恢复运行", "error": "missing_run_id"}


@app.post("/api/runs/{run_id}/resume")
async def resume_run(run_id: str, request: ResumeRequest):
    """恢复被中断的运行（确认选择 / 补齐参数）"""
    return await chat_service.resume_run(run_id, request.value)


@app.post("/api/runs/{run_id}/rewind")
async def rewind_run(run_id: str, request: RestartRequest):
    """回溯：从指定步骤在原运行内重新执行"""
    return await chat_service.rewind_run(run_id, request.target_seq)


@app.post("/api/runs/{run_id}/replay")
async def replay_run(run_id: str, request: RestartRequest):
    """定点回放：从指定步骤克隆出新的运行重新执行（原运行保留）"""
    return await chat_service.replay_run(run_id, request.target_seq)


@app.post("/api/runs/{run_id}/stop")
async def stop_run(run_id: str):
    """即时中断：请求停止当前运行（在步骤边界生效）"""
    return chat_service.stop_run(run_id)


@app.get("/api/runs/{run_id}")
async def get_run(run_id: str):
    """获取运行详情（计划 + 步骤状态）"""
    detail = chat_service.get_run_detail(run_id)
    if not detail:
        return {"error": "run_not_found"}
    return detail


@app.get("/api/runs")
async def list_runs():
    """列出最近运行"""
    return {"runs": chat_service.list_runs(20)}


# ============================================
# 静态文件（前端页面）
# ============================================
static_dir = os.path.join(Path(__file__).resolve().parent.parent, "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
