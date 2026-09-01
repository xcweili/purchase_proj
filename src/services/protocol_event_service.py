# -*- coding: utf-8 -*-
"""
协议匹配事件服务 - 形态B（客户远程部署）

职责：
1. 接收客户代码通过 webhook 上报的执行过程事件（step_start/step_update/step_end/reasoning/error/done）
2. 通过 SSE 将事件实时转发给前端，用于渲染"智能体思考过程"时间线
3. 提供模拟事件源（simulate），用于不依赖客户的自测/兜底演示

设计要点：
- 以 task_id 为维度组织事件流，每个任务独立订阅/转发
- 事件历史保留最近 N 条，新订阅的 SSE 客户端先补发历史，避免丢事件
- 发布-订阅模型，一个任务可被多个前端同时订阅
"""
import asyncio
import uuid
from collections import deque
from datetime import datetime
from typing import Any, Deque, Dict, Optional, Set

# ============================================
# 事件协议常量（与客户对齐的 6 类事件）
# ============================================
EVENT_STEP_START = "step_start"      # 步骤开始
EVENT_STEP_UPDATE = "step_update"    # 步骤内进度更新
EVENT_STEP_END = "step_end"          # 步骤结束
EVENT_REASONING = "reasoning"        # 思考说明文字
EVENT_ERROR = "error"                # 步骤/流程出错
EVENT_DONE = "done"                  # 全部完成（携带最终结果）

# 协议匹配标准流程（默认模拟/兜底用的步骤骨架）
# 若客户流程分法不同，以客户为准，此处仅用于模拟与兜底
MATCH_FLOW_STEPS = [
    {
        "step_id": "query_plans",
        "title": "查询补货计划",
        "desc": "从计划表读取需求物料清单",
        "summary": "共查询到 12 条补货计划，涉及 8 种物料",
    },
    {
        "step_id": "query_suppliers",
        "title": "查询协议商",
        "desc": "查询物料对应的协议供应商及执行比例",
        "summary": "共 5 家协议供应商可参与匹配",
    },
    {
        "step_id": "ladder_check",
        "title": "执行比例阶梯判断",
        "desc": "按 20%/50%/80% 阶梯确认可选供应商",
        "summary": "2 家供应商执行比例低于 20% 阶梯，优先选择",
    },
    {
        "step_id": "allocate",
        "title": "分配计算",
        "desc": "计算各策略下的分配数量与金额",
        "summary": "均衡/成本/配送 三种策略计算完成",
    },
    {
        "step_id": "output",
        "title": "输出最终结果",
        "desc": "生成协议匹配结果",
        "summary": "匹配完成，结果已保存",
    },
]


def _now_iso() -> str:
    """当前时间 ISO 格式"""
    return datetime.now().isoformat()


def make_event(event_type: str, task_id: str, **kwargs: Any) -> Dict[str, Any]:
    """构造标准事件对象（统一补充 event/task_id/ts 字段）"""
    ev: Dict[str, Any] = {
        "event": event_type,
        "task_id": task_id,
        "ts": _now_iso(),
    }
    ev.update(kwargs)
    return ev


class ProtocolEventService:
    """协议匹配事件服务

    Attributes:
        _history: Dict[task_id, deque[event]]   事件历史（用于新订阅者补发）
        _subscribers: Dict[task_id, set[Queue]] 各任务的活跃订阅队列
        _history_size: 每个任务保留的事件条数上限
    """

    def __init__(self, history_size: int = 200):
        self._history: Dict[str, Deque[Dict[str, Any]]] = {}
        self._subscribers: Dict[str, Set[asyncio.Queue]] = {}
        self._history_size = history_size
        # 后台任务引用集合：防止模拟/兜底任务被垃圾回收导致事件流中断
        self._tasks: Set[asyncio.Task] = set()

    # ------------------------------------------------------------
    # 发布 / 订阅
    # ------------------------------------------------------------
    async def publish(self, task_id: str, event: Dict[str, Any]) -> None:
        """发布一条事件：写入历史 + 推送给所有订阅者（非阻塞）"""
        # 归一化：确保关键字段存在
        event.setdefault("event", "")
        event.setdefault("task_id", task_id)
        event.setdefault("ts", _now_iso())

        # 1. 保存历史
        self._history.setdefault(task_id, deque(maxlen=self._history_size)).append(event)

        # 2. 推送给订阅者
        subs = self._subscribers.get(task_id, set())
        for q in list(subs):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                # 队列满说明客户端消费过慢，丢弃当前事件（保留历史可回溯）
                continue

    def subscribe(self, task_id: str) -> asyncio.Queue:
        """订阅某任务的事件流，返回事件队列"""
        q: asyncio.Queue = asyncio.Queue(maxsize=1024)
        self._subscribers.setdefault(task_id, set()).add(q)
        return q

    def unsubscribe(self, task_id: str, q: asyncio.Queue) -> None:
        """取消订阅（SSE 断开时调用）"""
        subs = self._subscribers.get(task_id)
        if subs:
            subs.discard(q)
            if not subs:
                self._subscribers.pop(task_id, None)

    def history(self, task_id: str) -> list:
        """获取某任务已发生的事件（按时间顺序）"""
        return list(self._history.get(task_id, []))

    def is_done(self, task_id: str) -> bool:
        """该任务是否已发送过 done 事件"""
        hist = self._history.get(task_id)
        return bool(hist) and hist[-1].get("event") == EVENT_DONE

    # ------------------------------------------------------------
    # 模拟事件源（自测/兜底）
    # ------------------------------------------------------------
    async def simulate(self, task_id: str, delay: float = 0.5, steps: Optional[list] = None) -> None:
        """模拟一次完整的协议匹配流程事件流

        Args:
            task_id: 任务ID
            delay: 每个步骤之间的间隔秒数（模拟实时推进）
            steps: 自定义步骤列表，缺省使用 MATCH_FLOW_STEPS
        """
        flow = steps or MATCH_FLOW_STEPS

        for step in flow:
            sid = step["step_id"]
            title = step["title"]
            desc = step.get("desc", "")
            summary = step.get("summary", "")

            # 1. 步骤开始
            await self.publish(task_id, make_event(
                EVENT_STEP_START, task_id,
                step_id=sid, title=title, desc=desc, status="running",
            ))
            await asyncio.sleep(delay)

            # 2. 思考说明文字
            await self.publish(task_id, make_event(
                EVENT_REASONING, task_id,
                step_id=sid, content=f"正在进行「{title}」：{desc}",
            ))
            await asyncio.sleep(delay * 0.5)

            # 3. 分配计算步骤额外模拟循环进度（展示 progress 效果）
            if sid == "allocate":
                for i in range(1, 4):
                    await self.publish(task_id, make_event(
                        EVENT_STEP_UPDATE, task_id,
                        step_id=sid, progress={"current": i, "total": 3},
                        content=f"正在计算第 {i}/3 种策略",
                    ))
                    await asyncio.sleep(delay * 0.6)

            # 4. 步骤结束
            await self.publish(task_id, make_event(
                EVENT_STEP_END, task_id,
                step_id=sid, title=title, status="success",
                summary=summary, cost_ms=int(delay * 1000),
            ))
            await asyncio.sleep(delay)

        # 5. 全部完成
        await self.publish(task_id, make_event(
            EVENT_DONE, task_id,
            status="success",
            summary="协议匹配流程全部完成",
            result={
                "total": 12,
                "matched": 10,
                "partial": 1,
                "unmet": 1,
            },
        ))

    def start_simulate(self, task_id: str, delay: float = 0.5, steps: Optional[list] = None) -> None:
        """启动一次模拟事件流（后台任务），保存引用防止被 GC 回收"""
        task = asyncio.create_task(self.simulate(task_id, delay, steps))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # ------------------------------------------------------------
    # 兜底：按最终结果 + 标准流程模板生成"伪过程"
    # 客户完全不提供事件时使用（非实时，需在界面标注）
    # ------------------------------------------------------------
    async def generate_fallback_flow(self, task_id: str, result: Optional[Dict[str, Any]] = None) -> None:
        """基于流程模板 + 最终结果快速生成一次过程事件流（伪实时，延迟很小）"""
        await self.simulate(task_id, delay=0.05, steps=None)
        if result is not None:
            await self.publish(task_id, make_event(
                EVENT_DONE, task_id,
                status="success",
                summary="协议匹配流程全部完成（推断过程，最终结果由客户接口返回）",
                result=result,
            ))


# 全局单例
protocol_event_service = ProtocolEventService()


def new_task_id(prefix: str = "match") -> str:
    """生成任务ID"""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"
