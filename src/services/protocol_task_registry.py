# -*- coding: utf-8 -*-
"""
协议匹配任务注册表（断线重连续传 + 历史对话留存）
==================================================
背景：客户的匹配流程可能长达 20 分钟以上，期间前端可能刷新页面、关闭临时
窗口等导致 SSE 连接断开。断开后再次调用 /api/protocol/match（action=task）
期望：

1. 不再重复调用客户接口（避免客户侧重复执行、重复副作用/计费）；
2. 直接与"仍在运行"的上游流重新建立连接，续传尚未收到的过程事件。

实现要点：
- 每个任务对应一个 ProtocolTask：持有后台 worker 协程 + 完整事件日志；
- worker 只调用一次客户接口，把事件追加进日志并唤醒订阅者；
- 客户侧的匹配批次号(batch_id)由其 SSE 事件的 task_id 字段给出，worker
  收到后记录到任务上，并随事件一起留存，便于日志与历史对话展示；
- 每个 SSE 连接按自己的游标从日志续读：新连接从 task.consumed（上次连接
  读到哪）继续，断线期间新产生的事件也不会丢（下次重连补发）；
- 内存只保留最近 MAX_TASKS(5) 个任务，用于查看历史对话；
- 单事件循环下用 asyncio.Event 做"有新事件"通知，无并发竞争。
"""
import asyncio
import logging
import time
from typing import Dict, List, Optional

from .protocol_match_client import KEEPALIVE, new_task_id, stream_customer_match

logger = logging.getLogger(__name__)

EVENT_DONE = "done"


class ProtocolTask:
    """一个协议匹配任务的运行态：worker + 事件日志 + 订阅者游标"""

    def __init__(self, task_id: str, payload: Dict, url: Optional[str] = None) -> None:
        self.task_id = task_id               # 本服务任务ID（原有逻辑保留）
        self.batch_id: Optional[str] = None  # 客户返回的匹配批次号（SSE 事件的 task_id）
        self.payload = payload               # 透传给客户接口的请求参数
        self.url = url                       # 客户接口地址；None=使用默认配置
        self.log: List = []                  # 事件日志（含心跳标记），只追加
        self.consumed: int = 0               # 已交付给最近一个连接的事件条数（日志游标）
        self.done: bool = False              # 是否已结束（done / error / 上游断开）
        self.error: Optional[str] = None
        self.created_at = time.time()
        self.worker: Optional["asyncio.Task"] = None
        self._updated = asyncio.Event()

    def _notify(self) -> None:
        """通知等待中的订阅者：有新事件或任务结束"""
        self._updated.set()

    def events(self) -> List[Dict]:
        """返回全部业务事件（过滤心跳标记），用于历史对话展示"""
        return [e for e in self.log if isinstance(e, dict)]

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
        async for item in stream_customer_match(
            payload=task.payload, url=task.url, task_id=task.task_id
        ):
            task.log.append(item)
            if isinstance(item, dict):
                # 记录客户侧匹配批次号（首个事件即携带，全程不变）
                if not task.batch_id and item.get("task_id"):
                    task.batch_id = item["task_id"]
                    logger.info("[Protocol] 已获取客户匹配批次号 batch_id=%s 本服务task_id=%s",
                                task.batch_id, task.task_id)
            task._notify()
            if isinstance(item, dict) and item.get("event") == EVENT_DONE:
                task.done = True
                break
    except Exception as e:  # noqa: BLE001 - 客户接口失败也要让前端看到错误事件
        logger.error("[Protocol] 任务 worker 异常结束 本服务task_id=%s url=%s err_type=%s err=%r",
                     task.task_id, task.url or "默认", type(e).__name__, e)
        task.error = f"{type(e).__name__}: {e}"
        task.log.append({
            "event": "error",
            "task_id": task.batch_id or task.task_id,
            "content": f"客户接口异常: {task.error}",
            "status": "error",
        })
    finally:
        task.done = True
        task._notify()
        task.worker = None
        logger.info("[Protocol] 任务 worker 已结束 本服务task_id=%s 客户批次号=%s done=%s err=%s",
                    task.task_id, task.batch_id, task.done, task.error)


class ProtocolTaskRegistry:
    """内存任务注册表：只保留最近 MAX_TASKS 个任务"""

    MAX_TASKS = 5

    def __init__(self) -> None:
        self._tasks: Dict[str, ProtocolTask] = {}

    def find(self, key: str) -> Optional[ProtocolTask]:
        """按本服务 task_id 或客户匹配批次号查找任务"""
        task = self._tasks.get(key)
        if task is not None:
            return task
        for t in self._tasks.values():
            if t.batch_id and t.batch_id == key:
                return t
        return None

    def latest(self) -> Optional[ProtocolTask]:
        """最近创建的任务（用于 action=task 重连）"""
        if not self._tasks:
            return None
        return max(self._tasks.values(), key=lambda t: t.created_at)

    def history(self) -> List[ProtocolTask]:
        """按创建时间倒序返回全部留存任务"""
        return sorted(self._tasks.values(), key=lambda t: t.created_at, reverse=True)

    def create(self, payload: Dict, url: Optional[str] = None) -> ProtocolTask:
        # 超出上限先淘汰：优先淘汰已结束的最老任务，都未结束则淘汰最老的
        while len(self._tasks) >= self.MAX_TASKS:
            candidates = [t for t in self._tasks.values() if t.done] or list(self._tasks.values())
            oldest = min(candidates, key=lambda t: t.created_at)
            logger.info("[Protocol] 任务数已达上限 %d，淘汰最老任务 task_id=%s",
                        self.MAX_TASKS, oldest.task_id)
            self._tasks.pop(oldest.task_id, None)

        task = ProtocolTask(new_task_id(), payload, url)
        self._tasks[task.task_id] = task
        return task


# 全局单例
protocol_task_registry = ProtocolTaskRegistry()
