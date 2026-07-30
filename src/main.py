# -*- coding: utf-8 -*-
"""
FastAPI 应用入口
提供基于 LangChain Agent 的对话接口（流式/非流式）
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
    logger.info("应用启动 - 初始化 LangChain Agent...")
    chat_service.initialize()
    tools = chat_service.get_available_tools()
    logger.info("Agent 初始化完成，共注册 %d 个工具:", len(tools))
    for t in tools:
        logger.info("  [%s] %s", t["name"], t["description"])
    yield
    logger.info("应用关闭")


app = FastAPI(
    title="采购智能助手",
    description="基于 LangChain 的采购管理智能助手（意图识别 + 工具调用）",
    version="0.1.0",
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


class ChatResponse(BaseModel):
    intent: str
    confidence: float
    result: str
    error: str | None = None
    reasoning: str = ""


# ============================================
# API 路由（优先于静态文件）
# ============================================
@app.post("/api/chat")
async def chat(request: ChatRequest):
    """对话接口 - 非流式"""
    result = await chat_service.process(request.message, history=request.history)
    resp = {
        "intent": result["intent"],
        "confidence": result["confidence"],
        "result": result.get("result") or "",
        "error": result.get("error"),
        "reasoning": result.get("reasoning", ""),
    }
    # 透传人工确认信号（非流式模式也需要对话框）
    if result.get("_requires_confirm"):
        resp["_requires_confirm"] = True
        resp["confirm_id"] = result["confirm_id"]
        resp["question"] = result.get("result", "")
        resp["options"] = result.get("options", [])
    return resp


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


@app.post("/api/confirm")
async def confirm(request: ConfirmRequest):
    """人工确认接口（HITL）"""
    result = await chat_service.confirm_action(
        confirm_id=request.confirm_id,
        choice=request.choice,
    )
    return result


# ============================================
# 静态文件（前端页面）
# ============================================
static_dir = os.path.join(Path(__file__).resolve().parent.parent, "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
