# -*- coding: utf-8 -*-
"""
协议匹配 - 客户接口调用适配层（形态B：我们调客户的匹配接口）
==============================================================
在真实对接时，匹配的发起方是【我们】：前端点"开始匹配" -> 我们后端
调用客户部署的匹配接口，同时把 task_id 传给客户，客户在流程中通过
customer_event_reporter 把过程事件实时上报到我们的 webhook，我们再
通过 SSE 转发给前端。

本模块只负责一件事：调用客户匹配接口并拿最终结果。
过程事件不经过这里（事件走 /api/protocol/events 那条通道）。
"""
import os
from typing import Any, Dict

import httpx

# ==========================================================
# 配置：客户匹配接口地址
# 联调时用环境变量覆盖，例如：
#   set CUSTOMER_MATCH_URL=http://1.2.3.4:8080/api/match/protocol
# 缺省指向本地模拟客户服务（mock_customer_api.py）
# ==========================================================
CUSTOMER_MATCH_URL = os.environ.get(
    "CUSTOMER_MATCH_URL",
    "http://127.0.0.1:8100/api/customer/protocol/match",
)
CUSTOMER_MATCH_TIMEOUT = float(os.environ.get("CUSTOMER_MATCH_TIMEOUT", "120"))


async def call_customer_match(
    task_id: str,
    warehouse_code: str = "",
    plan_month: str = "",
    **extra: Any,
) -> Dict[str, Any]:
    """调用客户匹配接口，携带 task_id，返回客户的最终匹配结果

    Args:
        task_id: 我们生成的关联ID（客户流程中用它上报事件）
        warehouse_code: 仓库编码（按客户接口 schema 调整字段名）
        plan_month: 计划月份（按客户接口 schema 调整字段名）
        **extra: 客户接口要求的其它业务参数

    Raises:
        httpx.HTTPError: 网络/超时错误
        RuntimeError: 客户接口业务码非 200
    """
    payload: Dict[str, Any] = {
        "task_id": task_id,
        "warehouse_code": warehouse_code,
        "plan_month": plan_month,
    }
    payload.update(extra)

    async with httpx.AsyncClient(timeout=CUSTOMER_MATCH_TIMEOUT) as client:
        resp = await client.post(CUSTOMER_MATCH_URL, json=payload)
        resp.raise_for_status()
        body = resp.json()

    if body.get("code") != 200:
        raise RuntimeError(f"客户接口返回异常: {body.get('message', body)}")
    return body.get("data") or {}
