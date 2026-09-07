# -*- coding: utf-8 -*-
"""
模拟客户方服务（形态C：客户暴露流式匹配接口，我们后端透明代理转发）
====================================================================
模拟"客户那边部署的协议匹配服务"，对外暴露一个【流式(SSE)】匹配入口：
    POST /api/customer/protocol/match

执行完成整流程：查询计划 -> 查协议商 -> 阶梯判断 -> 分配计算 -> 输出结果，
每走一步就把事件以 SSE 的 `data:` 帧实时推出去，直到最后推一帧 `done`（携带最终结果）。

对接说明（对客户而言本文件是"他们需要实现什么"的最小示例）：
- 匹配过程可能长达 20 分钟甚至更久，因此务必：
    1. 用 SSE（text/event-stream）逐步吐事件，别等全部算完才一次性返回；
    2. 长时间无事件时（例如某个步骤要跑十几分钟），主动吐 `: keepalive` 心跳，
       避免中间网关/代理把空闲连接掐断；
    3. 最终必须吐一帧 event=done 表示结束，携带 result 供前端展示。

运行：
    python mock_customer_api.py        # 默认 127.0.0.1:8100
"""
import asyncio
import json
import os

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

app = FastAPI(title="模拟客户协议匹配服务", version="2.1")

# 长耗时步骤参数（可用环境变量覆盖，便于自动化测试时缩短时长）
LONG_CALC_ROUNDS = int(os.environ.get("MOCK_LONG_CALC_ROUNDS", "15"))
LONG_CALC_INTERVAL = float(os.environ.get("MOCK_LONG_CALC_INTERVAL", "2"))


class MatchRequest(BaseModel):
    task_id: str = Field(..., description="任务ID，由采购智能体生成并传入，用于关联事件流")
    warehouse_code: str = Field(default="WH001", description="仓库编码")
    plan_month: str = Field(default="2026-08", description="计划月份（YYYY-MM）")


def _sse(ev: dict) -> str:
    """把一个事件对象编码成一条 SSE data 帧"""
    return "data: " + json.dumps(ev, ensure_ascii=False) + "\n\n"


def _keepalive() -> str:
    """SSE 心跳帧（无事件时保活）"""
    return ": keepalive\n\n"


def _ev(event: str, task_id: str, **kw) -> dict:
    d = {"event": event, "task_id": task_id}
    d.update(kw)
    return d


# ---------------- 以下为【模拟】的客户业务函数（真实场景换成他们的逻辑） ----------------

def do_query_plans(warehouse_code: str, plan_month: str) -> list:
    """【模拟】查询补货计划"""
    return [
        {"plan_id": f"JH-{plan_month.replace('-', '')}-{i:03d}",
         "material_code": f"M00{i}",
         "qty": 100 + i * 10}
        for i in range(1, 13)
    ]


def do_query_suppliers() -> list:
    """【模拟】查询协议供应商及执行比例"""
    return [
        {"supplier": "中电电气", "exec_ratio": 0.65},
        {"supplier": "特变电工", "exec_ratio": 0.80},
        {"supplier": "正泰电器", "exec_ratio": 0.45},
        {"supplier": "德力西", "exec_ratio": 0.30},
        {"supplier": "施耐德", "exec_ratio": 0.70},
    ]


def do_ladder_check(suppliers: list) -> list:
    """【模拟】按 20%/50%/80% 阶梯判断可选供应商"""
    return [s for s in suppliers if s["exec_ratio"] < 0.80]


# ---------------- 匹配入口（流式 SSE） ----------------

@app.post("/api/customer/protocol/match")
async def protocol_match(request: MatchRequest):
    """客户流式匹配接口：逐步吐出过程事件，以 done（携带结果）结束"""

    async def event_generator():
        task_id = request.task_id
        try:
            # ---- 步骤1：查询补货计划 ----
            yield _sse(_ev("step_start", task_id, step_id="query_plans",
                           title="查询补货计划", desc="从计划表读取需求物料清单", status="running"))
            await asyncio.sleep(0.2)
            plans = do_query_plans(request.warehouse_code, request.plan_month)
            yield _sse(_ev("reasoning", task_id, step_id="query_plans",
                           content=f"共查询到 {len(plans)} 条补货计划，涉及 8 种物料"))
            yield _sse(_ev("step_end", task_id, step_id="query_plans", title="查询补货计划",
                           status="success", summary=f"共查询到 {len(plans)} 条补货计划，涉及 8 种物料",
                           cost_ms=200))

            # ---- 步骤2：查询协议商 ----
            yield _sse(_ev("step_start", task_id, step_id="query_suppliers",
                           title="查询协议商", desc="查询物料对应的协议供应商及执行比例", status="running"))
            await asyncio.sleep(0.2)
            suppliers = do_query_suppliers()
            yield _sse(_ev("reasoning", task_id, step_id="query_suppliers",
                           content=f"共 {len(suppliers)} 家协议供应商可参与匹配，下一步按执行比例做阶梯判断"))
            yield _sse(_ev("step_end", task_id, step_id="query_suppliers", title="查询协议商",
                           status="success", summary=f"共 {len(suppliers)} 家协议供应商可参与匹配",
                           cost_ms=200))

            # ---- 步骤3：执行比例阶梯判断 ----
            yield _sse(_ev("step_start", task_id, step_id="ladder_check",
                           title="执行比例阶梯判断", desc="按 20%/50%/80% 阶梯确认可选供应商", status="running"))
            await asyncio.sleep(0.2)
            candidates = do_ladder_check(suppliers)
            yield _sse(_ev("reasoning", task_id, step_id="ladder_check",
                           content=f"其中 {len(candidates)} 家执行比例低于 80% 阶梯，纳入本轮匹配候选"))
            yield _sse(_ev("step_end", task_id, step_id="ladder_check", title="执行比例阶梯判断",
                           status="success", summary=f"{len(candidates)} 家供应商执行比例低于 80% 阶梯，优先选择",
                           cost_ms=200))

            # ---- 步骤4：分配计算（带循环进度，展示 progress 效果） ----
            yield _sse(_ev("step_start", task_id, step_id="allocate",
                           title="分配计算", desc="计算各策略下的分配数量与金额", status="running"))
            for i in range(1, 4):
                yield _sse(_ev("step_update", task_id, step_id="allocate",
                               progress={"current": i, "total": 3},
                               content=f"正在计算第 {i}/3 种分配策略"))
                await asyncio.sleep(0.15)
            alloc = {"strategies": 3, "plans_allocated": len(plans), "suppliers_used": len(candidates)}
            yield _sse(_ev("step_end", task_id, step_id="allocate", title="分配计算",
                           status="success", summary="均衡/成本/配送 三种策略计算完成", cost_ms=450))

            # ---- 长耗时步骤：模拟真实客户"长时间计算"（约 LONG_CALC_ROUNDS*INTERVAL 秒）
            #      每轮吐一个进度事件 + 一个心跳，方便测试页面刷新 / 客户端断开后重连续传 ----
            yield _sse(_ev("step_start", task_id, step_id="deep_calc",
                           title="深度计算", desc=f"模拟长时间计算（约 {LONG_CALC_ROUNDS * LONG_CALC_INTERVAL:.0f}s），"
                                                  "期间可刷新页面/关窗重连验证续传能力", status="running"))
            for i in range(1, LONG_CALC_ROUNDS + 1):
                yield _keepalive()
                await asyncio.sleep(LONG_CALC_INTERVAL)
                yield _sse(_ev("step_update", task_id, step_id="deep_calc",
                               progress={"current": i, "total": LONG_CALC_ROUNDS},
                               content=f"深度计算完成 {i}/{LONG_CALC_ROUNDS} 轮"))
            yield _sse(_ev("step_end", task_id, step_id="deep_calc", title="深度计算",
                           status="success", summary="深度计算完成",
                           cost_ms=int(LONG_CALC_ROUNDS * LONG_CALC_INTERVAL * 1000)))

            # ---- 步骤5：输出最终结果 ----
            yield _sse(_ev("step_start", task_id, step_id="output",
                           title="输出最终结果", desc="生成协议匹配结果", status="running"))
            await asyncio.sleep(0.1)
            result = {
                "task_id": task_id,
                "warehouse_code": request.warehouse_code,
                "plan_month": request.plan_month,
                "plans": len(plans),
                "suppliers": len(suppliers),
                "candidates": len(candidates),
                "allocate": alloc,
                "deep_calc_rounds": LONG_CALC_ROUNDS,
                "matched": 10,
                "partial": 1,
                "unmet": 1,
            }
            yield _sse(_ev("step_end", task_id, step_id="output", title="输出最终结果",
                           status="success", summary="匹配完成，结果已保存", cost_ms=100))

            # ---- 完成（携带最终结果） ----
            yield _sse(_ev("done", task_id, status="success",
                           summary="协议匹配流程全部完成", result=result))
        except Exception as e:  # noqa: BLE001 - 客户侧兜底
            yield _sse(_ev("error", task_id, message=f"匹配失败: {e}"))

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