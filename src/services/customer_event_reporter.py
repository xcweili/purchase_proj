# -*- coding: utf-8 -*-
"""
客户侧事件上报模块（交付给客户）
==========================================================
这份文件是要【直接发给客户】的。客户只需要做 3 件事：

  1. 把本文件复制到他们的协议匹配项目里；
  2. 把下面的 WEBHOOK_URL 改成我们（采购智能体后端）的事件接收地址；
  3. 在流程的每个关键步骤调用对应的 report_* 函数（见下方"接入示例"）。

对客户的要求：
  - 环境已有 requests 库（没有则 pip install requests）
  - 上报失败不影响主流程（内部已静默吞掉异常），可放心接入

接入示例（客户伪代码）：

    from customer_event_reporter import (
        report_step_start, report_step_end, report_reasoning,
        report_step_update, report_error, report_done,
    )

    def match_protocol(task_id, warehouse_code, plan_month):
        # 步骤1：查询补货计划
        report_step_start(task_id, "query_plans", "查询补货计划", "从计划表读取需求物料清单")
        plans = do_query_plans(...)                       # ← 客户真实业务逻辑
        report_reasoning(task_id, "query_plans",
                         f"共查询到 {len(plans)} 条补货计划")
        report_step_end(task_id, "query_plans", status="success",
                        summary=f"共查询到 {len(plans)} 条补货计划", cost_ms=320)

        # 步骤2...步骤N 同理
        # step_id 建议用英文标识，如：query_plans / query_suppliers / ladder_check / allocate / output

        # 最后：上报完成事件，携带最终结果
        report_done(task_id, {"total": 12, "matched": 10})
==========================================================
"""
from typing import Any, Dict

try:
    import requests
except ImportError:  # pragma: no cover - 客户环境无 requests 时静默降级
    requests = None

# ==========================================================
# 客户只需修改这里的配置
# ==========================================================
WEBHOOK_URL = "http://127.0.0.1:8000/api/protocol/events"  # ← 改为我们的事件接收地址
REPORT_ENABLED = True                                      # 是否上报（联调时可临时关掉）
REPORT_TIMEOUT = 1.0                                       # 单次上报超时（秒）


def _post(task_id: str, event: str, **payload: Any) -> None:
    """内部方法：组装事件 JSON 并 POST 到我们的 webhook（失败静默，不影响主流程）"""
    if not REPORT_ENABLED or requests is None:
        return
    body: Dict[str, Any] = {"task_id": task_id, "event": event}
    body.update(payload)
    try:
        requests.post(WEBHOOK_URL, json=body, timeout=REPORT_TIMEOUT)
    except Exception:
        pass


# ==========================================================
# 对外事件上报函数（6 类事件，与我们的协议一致）
# ==========================================================
def report_step_start(task_id: str, step_id: str, title: str, desc: str = "") -> None:
    """步骤开始：task_id 由我们生成并传入，step_id/title 自行命名"""
    _post(task_id, "step_start", step_id=step_id, title=title, desc=desc, status="running")


def report_step_update(task_id: str, step_id: str, progress: Dict = None, content: str = "") -> None:
    """步骤内进度更新：progress 建议形如 {"current": 2, "total": 5}"""
    _post(task_id, "step_update", step_id=step_id, progress=progress, content=content)


def report_reasoning(task_id: str, step_id: str, content: str) -> None:
    """思考说明文字：给前端展示这一步在想什么 / 发现了什么"""
    _post(task_id, "reasoning", step_id=step_id, content=content)


def report_step_end(task_id: str, step_id: str, status: str = "success",
                    summary: str = "", cost_ms: float = 0, title: str = "") -> None:
    """步骤结束：summary 建议用结果摘要（不要发全量明细，避免敏感数据上界面）"""
    _post(task_id, "step_end", step_id=step_id, title=title,
          status=status, summary=summary, cost_ms=cost_ms)


def report_error(task_id: str, step_id: str, message: str) -> None:
    """出错上报"""
    _post(task_id, "error", step_id=step_id, message=message)


def report_done(task_id: str, result: Dict) -> None:
    """全部完成：携带最终结果"""
    _post(task_id, "done", status="success", summary="协议匹配流程全部完成", result=result)
