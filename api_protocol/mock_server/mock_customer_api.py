# -*- coding: utf-8 -*-
"""
模拟客户方服务（客户暴露流式匹配接口，我们后端透明代理转发）
==============================================================
模拟"客户那边部署的协议匹配服务"，对外暴露一个【流式(SSE)】匹配入口：
    POST /api/customer/protocol/match

匹配批次号(task_id)由本服务自行生成，并通过每个 SSE 事件回传；请求体为调用方
透传的业务参数，本模拟服务不强制约束其字段。

事件协议（每个事件是一条 `data:` 帧，内容为 JSON 对象）：
| 字段         | 类型        | 必填 | 取值 / 含义                                                        |
| ------------ | ----------- | ---- | ------------------------------------------------------------------ |
| event        | string      | 是   | start 任务已受理 / stage 进度节点 / done 完成并落库 / error 任务失败 |
| task_id      | string      | 是   | 匹配批次号，全程不变                                                |
| content      | string      | 是   | 人类可读的正文（可直接用于前端展示）                                |
| step_id      | int/null    | 否   | 引擎步骤号（同一分标引擎实例内递增）                                |
| title        | string/null | 否   | 阶段标题（如「数据准备」「Phase0 分段完成」「方案「均衡」开始」）    |
| desc         | string/null | 否   | 结构化描述（当前未使用）                                            |
| status       | string/null | 否   | processing / success（error 事件用 error）                         |
| progress     | int/null    | 否   | 0~100（引擎内单分标粒度；全局进度由各阶段事件顺序体现）             |
| summary      | string/null | 否   | 阶段汇总文案（引擎结束事件使用）                                    |
| cost_ms      | long/null   | 否   | 阶段耗时毫秒                                                        |
| sub_bid_info | string/null | 否   | 分标信息；方案内按分标并行执行，事件可能交错，靠它区分来自哪个分标；空=方案/全局级事件 |

长耗时段落用于验证"客户端断开/刷新后重连续传"，可用环境变量调快慢：
    MOCK_LONG_CALC_ROUNDS / MOCK_LONG_CALC_INTERVAL

运行：
    python mock_customer_api.py        # 默认 127.0.0.1:8100
"""
import asyncio
import json
import os
import time
import uuid
from typing import Any, Dict

from fastapi import Body, FastAPI
from fastapi.responses import StreamingResponse

app = FastAPI(title="模拟客户协议匹配服务", version="3.0")

# 长耗时步骤参数（可用环境变量覆盖，便于自动化测试时缩短时长）
LONG_CALC_ROUNDS = int(os.environ.get("MOCK_LONG_CALC_ROUNDS", "15"))
LONG_CALC_INTERVAL = float(os.environ.get("MOCK_LONG_CALC_INTERVAL", "2"))

# 模拟分标：方案内按分标并行执行，事件会交错
SUB_BIDS = ["分标A", "分标B", "分标C"]


def new_batch_id() -> str:
    """生成匹配批次号（真实场景由客户侧匹配引擎生成，全程不变）"""
    return "BID-" + time.strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:4].upper()


def _sse(ev: dict) -> str:
    """把一个事件对象编码成一条 SSE data 帧"""
    return "data: " + json.dumps(ev, ensure_ascii=False) + "\n\n"


def _keepalive() -> str:
    """SSE 心跳帧（无事件时保活）"""
    return ": keepalive\n\n"


def _ev(event: str, task_id: str, content: str, **kw) -> dict:
    """构造一个符合事件协议的 SSE 事件对象（event/task_id/content 必填）"""
    d = {"event": event, "task_id": task_id, "content": content}
    d.update(kw)
    return d


# ---------------- 以下为【模拟】的客户业务函数（真实场景换成他们的逻辑） ----------------

def do_query_plans() -> list:
    """【模拟】读取补货计划"""
    return [{"plan_id": f"JH-{i:03d}", "material_code": f"M00{i}", "qty": 100 + i * 10}
            for i in range(1, 13)]


def do_query_suppliers() -> list:
    """【模拟】查询协议供应商及执行比例"""
    return [
        {"supplier": "中电电气", "exec_ratio": 0.65},
        {"supplier": "特变电工", "exec_ratio": 0.80},
        {"supplier": "正泰电器", "exec_ratio": 0.45},
        {"supplier": "德力西", "exec_ratio": 0.30},
        {"supplier": "施耐德", "exec_ratio": 0.70},
    ]


# ---------------- 匹配入口（流式 SSE） ----------------

@app.post("/api/customer/protocol/match")
async def protocol_match(payload: Dict[str, Any] = Body(default={})):
    """客户流式匹配接口：按事件协议逐步吐事件，以 done（完成并落库）结束"""

    async def event_generator():
        batch_id = new_batch_id()     # 匹配批次号：客户侧生成，全程不变
        step = 0
        t0 = time.monotonic()

        def cost_ms() -> int:
            """当前阶段累计耗时（毫秒）"""
            return int((time.monotonic() - t0) * 1000)

        params_desc = "、".join(f"{k}={v}" for k, v in payload.items()) or "无"

        try:
            # ① start：任务已受理（同时回显收到的透传参数，便于验证透传链路）
            yield _sse(_ev("start", batch_id,
                           f"任务已受理，匹配批次号 {batch_id}（收到透传参数：{params_desc}）",
                           title="任务受理", status="processing", progress=0))

            # ② 数据准备
            step += 1
            yield _sse(_ev("stage", batch_id, "正在读取补货计划与协议供应商数据",
                           step_id=step, title="数据准备", status="processing"))
            await asyncio.sleep(0.2)
            plans = do_query_plans()
            suppliers = do_query_suppliers()
            yield _sse(_ev("stage", batch_id,
                           f"数据准备完成：补货计划 {len(plans)} 条，协议供应商 {len(suppliers)} 家",
                           step_id=step, title="数据准备", status="success",
                           progress=100, summary="数据准备完成", cost_ms=cost_ms()))

            # ③ Phase0 分段完成
            step += 1
            yield _sse(_ev("stage", batch_id, "需求已按物料分段完成，进入方案匹配",
                           step_id=step, title="Phase0 分段完成", status="success",
                           progress=100, summary="Phase0 分段完成", cost_ms=cost_ms()))

            # ④ 方案「均衡」开始（方案内按分标并行，事件会交错）
            step += 1
            yield _sse(_ev("stage", batch_id, "方案「均衡」开始，分标并行执行匹配",
                           step_id=step, title="方案「均衡」开始", status="processing", progress=0))

            # ⑤ 长时间计算：每轮吐进度 + 心跳，便于测试断开/刷新后重连续传
            for i in range(1, LONG_CALC_ROUNDS + 1):
                yield _keepalive()
                await asyncio.sleep(LONG_CALC_INTERVAL)
                sub_bid = SUB_BIDS[(i - 1) % len(SUB_BIDS)]
                yield _sse(_ev("stage", batch_id,
                               f"{sub_bid} 已完成 {i}/{LONG_CALC_ROUNDS} 轮匹配计算",
                               step_id=step, title="方案「均衡」执行中", status="processing",
                               progress=int(i * 100 / LONG_CALC_ROUNDS), sub_bid_info=sub_bid))

            # ⑥ 方案「均衡」完成
            yield _sse(_ev("stage", batch_id, "方案「均衡」分标匹配全部完成",
                           step_id=step, title="方案「均衡」完成", status="success",
                           progress=100, summary="方案「均衡」完成", cost_ms=cost_ms()))

            # ⑦ done：完成并落库
            yield _sse(_ev("done", batch_id,
                           f"协议匹配流程全部完成，结果已落库（批次号 {batch_id}）",
                           status="success", progress=100,
                           summary="协议匹配流程全部完成", cost_ms=cost_ms()))
        except Exception as e:  # noqa: BLE001 - 客户侧兜底
            yield _sse(_ev("error", batch_id, f"匹配失败: {e}", status="error"))

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8100, log_level="info")
