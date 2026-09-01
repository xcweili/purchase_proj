# -*- coding: utf-8 -*-
"""
模拟客户方服务（形态B演示用）
==============================
模拟"客户那边部署的协议匹配服务"：
- 提供匹配入口：POST /api/customer/protocol/match
- 内部执行完整匹配流程：查询计划 -> 查协议商 -> 阶梯判断 -> 分配计算 -> 输出结果
- 每个关键步骤调用 customer_event_reporter 把执行过程实时上报到我们的后端

对客户而言，本文件就是"他们需要加什么"的最小可运行示例：
真正的客户代码里，只需保留各处的 report_* 调用、替换中间的"【模拟】"业务函数即可。

运行：
    python mock_customer_api.py        # 默认 127.0.0.1:8100
"""
import sys
import time

from fastapi import FastAPI
from pydantic import BaseModel, Field

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from src.services.customer_event_reporter import (
    report_step_start,
    report_step_update,
    report_reasoning,
    report_step_end,
    report_error,
    report_done,
)

app = FastAPI(title="模拟客户协议匹配服务", version="1.0")


class MatchRequest(BaseModel):
    task_id: str = Field(..., description="任务ID，由采购智能体生成并传入，用于关联事件流")
    warehouse_code: str = Field(default="WH001", description="仓库编码")
    plan_month: str = Field(default="2026-08", description="计划月份（YYYY-MM）")


# ---------------- 以下为【模拟】的客户业务函数（真实场景换成他们的逻辑） ----------------

def do_query_plans(warehouse_code: str, plan_month: str) -> list:
    """【模拟】查询补货计划"""
    time.sleep(0.2)
    return [
        {"plan_id": f"JH-{plan_month.replace('-', '')}-{i:03d}",
         "material_code": f"M00{i}",
         "qty": 100 + i * 10}
        for i in range(1, 13)
    ]


def do_query_suppliers() -> list:
    """【模拟】查询协议供应商及执行比例"""
    time.sleep(0.2)
    return [
        {"supplier": "中电电气", "exec_ratio": 0.65},
        {"supplier": "特变电工", "exec_ratio": 0.80},
        {"supplier": "正泰电器", "exec_ratio": 0.45},
        {"supplier": "德力西", "exec_ratio": 0.30},
        {"supplier": "施耐德", "exec_ratio": 0.70},
    ]


def do_ladder_check(suppliers: list) -> list:
    """【模拟】按 20%/50%/80% 阶梯判断可选供应商"""
    time.sleep(0.2)
    return [s for s in suppliers if s["exec_ratio"] < 0.80]


def do_allocate(task_id: str, plans: list, suppliers: list) -> dict:
    """【模拟】分配计算（带循环进度上报，展示 progress 效果）"""
    for i in range(1, 4):
        report_step_update(task_id, "allocate",
                           progress={"current": i, "total": 3},
                           content=f"正在计算第 {i}/3 种分配策略")
        time.sleep(0.15)
    return {"strategies": 3, "plans_allocated": len(plans), "suppliers_used": len(suppliers)}


# ---------------- 匹配入口（演示"客户代码里应该长什么样"） ----------------

@app.post("/api/customer/protocol/match")
async def protocol_match(request: MatchRequest):
    task_id = request.task_id
    try:
        # ---- 步骤1：查询补货计划 ----
        report_step_start(task_id, "query_plans", "查询补货计划", "从计划表读取需求物料清单")
        t0 = time.time()
        plans = do_query_plans(request.warehouse_code, request.plan_month)
        report_reasoning(task_id, "query_plans",
                         f"共查询到 {len(plans)} 条补货计划，涉及 8 种物料")
        report_step_end(task_id, "query_plans", title="查询补货计划", status="success",
                        summary=f"共查询到 {len(plans)} 条补货计划，涉及 8 种物料",
                        cost_ms=int((time.time() - t0) * 1000))

        # ---- 步骤2：查询协议商 ----
        report_step_start(task_id, "query_suppliers", "查询协议商", "查询物料对应的协议供应商及执行比例")
        t0 = time.time()
        suppliers = do_query_suppliers()
        report_reasoning(task_id, "query_suppliers",
                         f"共 {len(suppliers)} 家协议供应商可参与匹配，下一步按执行比例做阶梯判断")
        report_step_end(task_id, "query_suppliers", title="查询协议商", status="success",
                        summary=f"共 {len(suppliers)} 家协议供应商可参与匹配",
                        cost_ms=int((time.time() - t0) * 1000))

        # ---- 步骤3：执行比例阶梯判断 ----
        report_step_start(task_id, "ladder_check", "执行比例阶梯判断", "按 20%/50%/80% 阶梯确认可选供应商")
        t0 = time.time()
        candidates = do_ladder_check(suppliers)
        report_reasoning(task_id, "ladder_check",
                         f"其中 {len(candidates)} 家执行比例低于 80% 阶梯，纳入本轮匹配候选")
        report_step_end(task_id, "ladder_check", title="执行比例阶梯判断", status="success",
                        summary=f"{len(candidates)} 家供应商执行比例低于 80% 阶梯，优先选择",
                        cost_ms=int((time.time() - t0) * 1000))

        # ---- 步骤4：分配计算 ----
        report_step_start(task_id, "allocate", "分配计算", "计算各策略下的分配数量与金额")
        t0 = time.time()
        alloc = do_allocate(task_id, plans, candidates)
        report_step_end(task_id, "allocate", title="分配计算", status="success",
                        summary="均衡/成本/配送 三种策略计算完成",
                        cost_ms=int((time.time() - t0) * 1000))

        # ---- 步骤5：输出最终结果 ----
        report_step_start(task_id, "output", "输出最终结果", "生成协议匹配结果")
        t0 = time.time()
        result = {
            "task_id": task_id,
            "warehouse_code": request.warehouse_code,
            "plan_month": request.plan_month,
            "plans": len(plans),
            "suppliers": len(suppliers),
            "candidates": len(candidates),
            "allocate": alloc,
            "matched": 10,
            "partial": 1,
            "unmet": 1,
        }
        report_step_end(task_id, "output", title="输出最终结果", status="success",
                        summary="匹配完成，结果已保存",
                        cost_ms=int((time.time() - t0) * 1000))

        # ---- 上报完成（携带最终结果）----
        report_done(task_id, result)

        return {"code": 200, "message": "success", "data": result}
    except Exception as e:  # noqa: BLE001 - 客户侧兜底
        report_error(task_id, "unknown", str(e))
        return {"code": 500, "message": f"匹配失败: {e}"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8100, log_level="info")
