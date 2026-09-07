# -*- coding: utf-8 -*-
"""
协议匹配任务注册表（断线重连续传）
====================================
背景：客户(8100)的匹配流程可能长达 20 分钟以上，期间前端可能刷新页面、
关闭临时窗口等导致 SSE 连接断开。若断开后再次调用 /api/protocol/match
并携带相同的 task_id，期望：

1. 不再重复调用 8100（避免客户侧重复执行、重复副作用/计费）；
2. 直接与"仍在运行"的上游流重新建立连接，续传尚未收到的过程事件。

实现要点：
- 每个 task_id 对应一个 ProtocolTask：持有后台 worker 协程 + 完整事件日志；
- worker 只调用一次 8100，把事件追加进日志并唤醒订阅者；
- 每个 SSE 连接按自己的游标从日志续读：新连接从 task.consumed（上次连接
  读到哪）继续，断线期间新产生的事件也不会丢（下次重连补发）；
- 单事件循环下用 asyncio.Event 做"有新事件"通知，无并发竞争。
"""
import asyncio
import logging
import time
from typing import Dict, List, Optional

from .protocol_match_client import KEEPALIVE, stream_customer_match

logger = logging.getLogger(__name__)

EVENT_DONE = "done"


class ProtocolTask:
    """一个协议匹配任务的运行态：worker + 事件日志 + 订阅者游标"""

    def __init__(self, task_id: str, payload: Dict, url: Optional[str] = None) -> None:
        self.task_id = task_id
        self.payload = payload              # 透传给客户接口的请求参数
        self.url = url                      # 客户接口地址；None=使用默认配置
        self.log: List = []                 # 事件日志（含心跳标记），只追加
        self.consumed: int = 0              # 已交付给最近一个连接的事件条数（日志游标）
        self.done: bool = False             # 是否已结束（done / error / 上游断开）
        self.error: Optional[str] = None
        self.created_at = time.time()
        self.worker: Optional["asyncio.Task"] = None
        self._updated = asyncio.Event()

    def _notify(self) -> None:
        """通知等待中的订阅者：有新事件或任务结束"""
        self._updated.set()

    async def events_from(self, start_pos: int):
        """从日志游标 start_pos 开始逐个产出事件，直到 done 或任务结束

        Args:
            start_pos: 日志下标，从这个位置开始读（0 = 从头回放）
        Yields:
            事件 dict 或 KEEPALIVE 心跳标记
        """
        pos = start_pos
        while True:
            while pos < len(self.log):
                item = self.log[pos]
                pos += 1
                yield item
                if isinstance(item, dict) and item.get("event") == EVENT_DONE:
                    return
            if self.done:
                return
            # 等待新事件：先 clear 再 wait，并二次检查，避免丢失唤醒
            self._updated.clear()
            if pos < len(self.log) or self.done:
                continue
            await self._updated.wait()


async def run_worker(task: ProtocolTask) -> None:
    """后台协程：只调用一次客户流式接口，把事件写进任务日志并唤醒订阅者"""
    try:
        params = dict(task.payload)
        params.pop("task_id", None)
        async for item in stream_customer_match(task_id=task.task_id, url=task.url, **params):
            task.log.append(item)
            task._notify()
            if isinstance(item, dict) and item.get("event") == EVENT_DONE:
                task.done = True
                break
    except Exception as e:  # noqa: BLE001 - 客户接口失败也要让前端看到错误事件
        logger.error("[Protocol] 任务 worker 异常结束 task_id=%s err=%s", task.task_id, e)
        task.error = str(e)
        task.log.append({"event": "error", "task_id": task.task_id, "message": f"客户接口异常: {e}"})
    finally:
        task.done = True
        task._notify()
        task.worker = None
        logger.info("[Protocol] 任务 worker 已结束 task_id=%s done=%s err=%s",
                    task.task_id, task.done, task.error)


class ProtocolTaskRegistry:
    """task_id -> ProtocolTask 的注册表"""

    MAX_TASKS = 200

    def __init__(self) -> None:
        self._tasks: Dict[str, ProtocolTask] = {}

    def get(self, task_id: str) -> Optional[ProtocolTask]:
        return self._tasks.get(task_id)

    def create(self, task_id: str, payload: Dict, url: Optional[str] = None) -> ProtocolTask:
        # 轻量回收：任务过多时优先淘汰已结束的最老任务
        if len(self._tasks) >= self.MAX_TASKS:
            done_tasks = sorted(
                (t for t in self._tasks.values() if t.done),
                key=lambda t: t.created_at,
            )
            for t in done_tasks[: max(1, len(self._tasks) - self.MAX_TASKS + 1)]:
                self._tasks.pop(t.task_id, None)
        task = ProtocolTask(task_id, payload, url)
        self._tasks[task_id] = task
        return task

    def remove(self, task_id: str) -> None:
        self._tasks.pop(task_id, None)


# 全局单例
protocol_task_registry = ProtocolTaskRegistry()
