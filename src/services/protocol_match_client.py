# -*- coding: utf-8 -*-
"""
协议匹配 - 客户流式接口代理（形态C：单接口透明代理）
====================================================
前端点一次，我们后端调用客户部署的【流式(SSE)】匹配接口，持续读取客户在整个
匹配过程中吐出的过程事件，再原样以 SSE 转发给前端，读到 event=done 后断开。

对接关键点（匹配过程可能长达 20 分钟以上）：
1. 读超时设 None（不设上限），只保留连接超时，避免长任务被读超时掐断；
2. 用 httpx 流式读取上游 SSE 的每一行，逐事件 yield 给上层；
3. 上游超过 heartbeat 秒没吐内容，就 yield 一个 KEEPALIVE 标记，由上层向
   前端吐 `: keepalive` 心跳，防止中间网关把空闲连接掐断。

配置可用环境变量覆盖：
    CUSTOMER_MATCH_URL          客户匹配接口地址（缺省 127.0.0.1:8100）
    CUSTOMER_CONNECT_TIMEOUT    建连超时秒数（缺省 10）
    PROXY_HEARTBEAT_TIMEOUT     上游静默多久转发一次心跳（缺省 15）
"""
import asyncio
import json
import logging
import os
import time
import uuid
from typing import Any, AsyncIterator, Dict, Optional, Union

import httpx

logger = logging.getLogger(__name__)

CUSTOMER_MATCH_URL = os.environ.get(
    "CUSTOMER_MATCH_URL",
    "http://127.0.0.1:8100/api/customer/protocol/match",
)
CUSTOMER_CONNECT_TIMEOUT = float(os.environ.get("CUSTOMER_CONNECT_TIMEOUT", "10"))
PROXY_HEARTBEAT_TIMEOUT = float(os.environ.get("PROXY_HEARTBEAT_TIMEOUT", "15"))

EVENT_DONE = "done"


class _KeepAlive:
    """心跳标记：告诉上层该向前端吐一帧 keepalive"""
    __slots__ = ()


# 单例心跳标记
KEEPALIVE = _KeepAlive()

# 每轮 async for 迭代产出的类型：一个事件 dict，或 KEEPALIVE 标记
StreamItem = Union[Dict[str, Any], _KeepAlive]


def new_task_id(prefix: str = "match") -> str:
    """生成任务ID"""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


async def stream_customer_match(
    task_id: str,
    url: Optional[str] = None,
    **extra: Any,
) -> AsyncIterator[StreamItem]:
    """连接客户流式匹配接口，逐事件产出其 SSE 数据帧

    Args:
        task_id: 我们生成的关联ID（传给客户用于关联事件流）
        url: 客户流式接口地址；为空时使用默认配置 CUSTOMER_MATCH_URL
        **extra: 透传给客户的业务参数（原样进入请求体）

    Yields:
        每条事件 dict（已 JSON 解析）；
        上游静默超过 PROXY_HEARTBEAT_TIMEOUT 秒时 yield KEEPALIVE。

    Raises:
        RuntimeError: 客户接口返回非 200
        httpx.HTTPError: 建连/网络错误
    """
    target_url = url or CUSTOMER_MATCH_URL
    payload: Dict[str, Any] = {"task_id": task_id}
    payload.update(extra)

    logger.info(
        "[Protocol] 开始调用客户流式接口 url=%s task_id=%s",
        target_url, task_id,
    )

    # 读超时 None = 不设上限（长任务）；connect 超时保留，避免建连一直等
    timeout = httpx.Timeout(
        connect=CUSTOMER_CONNECT_TIMEOUT,
        read=None,
        write=None,
        pool=None,
    )

    start = time.monotonic()
    silent_seconds = 0.0
    event_count = 0

    async with httpx.AsyncClient(timeout=timeout) as client:
        async with client.stream("POST", target_url, json=payload) as resp:
            if resp.status_code != 200:
                body = await resp.aread()
                detail = body.decode("utf-8", "ignore")[:200]
                logger.error(
                    "[Protocol] 客户接口返回非 200 status=%s body=%s task_id=%s",
                    resp.status_code, detail, task_id,
                )
                raise RuntimeError(f"客户接口返回 HTTP {resp.status_code}: {detail}")

            logger.info("[Protocol] 已连接客户接口并开始接收事件流 task_id=%s (HTTP %s)",
                        task_id, resp.status_code)
            lines = resp.aiter_lines()
            while True:
                try:
                    line = await asyncio.wait_for(lines.__anext__(), timeout=PROXY_HEARTBEAT_TIMEOUT)
                except asyncio.TimeoutError:
                    # 上游长时间无输出 -> 转发心跳保活，并记录累计静默时长（判断"卡住"的关键信号）
                    silent_seconds += PROXY_HEARTBEAT_TIMEOUT
                    logger.warning(
                        "[Protocol] 客户接口已连续 %.0fs 无新事件（可能在下游计算/等待中），转发心跳保活 task_id=%s",
                        silent_seconds, task_id,
                    )
                    yield KEEPALIVE
                    continue
                except StopAsyncIteration:
                    break

                silent_seconds = 0.0
                line = line.strip()
                if not line:
                    continue
                if line.startswith(":"):
                    # 上游自带的心跳 -> 同样传给前端
                    yield KEEPALIVE
                    continue
                if line.startswith("data:"):
                    data = line[5:].strip()
                    try:
                        ev = json.loads(data)
                    except json.JSONDecodeError:  # noqa: PERF203 - 非 JSON 帧直接跳过
                        logger.debug("[Protocol] 忽略非 JSON 帧 task_id=%s data=%s", task_id, data[:100])
                        continue

                    event_count += 1
                    if isinstance(ev, dict):
                        etype = ev.get("event")
                        step_id = ev.get("step_id", "")
                        detail_text = ev.get("title") or ev.get("summary") or ev.get("content") or ev.get("message") or ""
                        if etype == EVENT_DONE:
                            logger.info(
                                "[Protocol] 收到 done 事件，客户匹配流程完成 task_id=%s 共收到 %d 条事件 耗时 %.1fs",
                                task_id, event_count, time.monotonic() - start,
                            )
                        else:
                            logger.info(
                                "[Protocol] 收到客户事件 #%d event=%s step=%s detail=%s task_id=%s",
                                event_count, etype, step_id, detail_text, task_id,
                            )
                    yield ev

            # 走到这里说明上游流已自然结束（没有吐出 done）
            logger.warning(
                "[Protocol] 客户事件流在未收到 done 的情况下结束 task_id=%s 共收到 %d 条事件 耗时 %.1fs",
                task_id, event_count, time.monotonic() - start,
            )