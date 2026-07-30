# -*- coding: utf-8 -*-
"""
采购订单查询工具 - 演示多轮参数补齐
这个工具要求必填参数 order_id，用户没说清楚时 agent 会追问
"""
import logging
from datetime import datetime

from src.tools import tool

logger = logging.getLogger(__name__)

# 模拟采购订单数据
_mock_orders = {
    "PO-2025-001": {
        "order_id": "PO-2025-001",
        "material": "碳钢钢板",
        "quantity": 100,
        "unit": "吨",
        "supplier": "宝钢集团",
        "status": "已发货",
        "order_date": "2025-06-15",
        "expected_delivery": "2025-07-10",
        "amount": "¥450,000",
    },
    "PO-2025-002": {
        "order_id": "PO-2025-002",
        "material": "不锈钢管材",
        "quantity": 50,
        "unit": "吨",
        "supplier": "太钢集团",
        "status": "待审核",
        "order_date": "2025-06-20",
        "expected_delivery": "2025-07-25",
        "amount": "¥320,000",
    },
    "PO-2025-003": {
        "order_id": "PO-2025-003",
        "material": "铜芯电缆",
        "quantity": 2000,
        "unit": "米",
        "supplier": "远东电缆",
        "status": "已到货",
        "order_date": "2025-05-10",
        "expected_delivery": "2025-06-01",
        "amount": "¥180,000",
    },
}


@tool(description="查询采购订单的详细信息，需要提供订单编号")
async def query_purchase_order(order_id: str) -> str:
    """查询采购订单

    这是一个演示多轮参数补齐的工具。
    因为 order_id 是必填参数（没有默认值），如果用户只问"查一下采购订单"
    但没有提供订单编号，Agent 会自动追问。

    Args:
        order_id: 采购订单编号，如 PO-2025-001
    """
    logger.info("查询采购订单: order_id=%s", order_id)

    order_id = order_id.strip().upper()
    order = _mock_orders.get(order_id)

    if not order:
        return f"未找到订单编号为 **{order_id}** 的采购订单"

    # 格式化返回
    lines = [
        f"📋 **采购订单详情** — {order_id}\n",
        f"| 项目 | 内容 |",
        f"|------|------|",
        f"| 物资名称 | {order['material']} |",
        f"| 数量 | {order['quantity']}{order['unit']} |",
        f"| 供应商 | {order['supplier']} |",
        f"| 状态 | {order['status']} |",
        f"| 下单日期 | {order['order_date']} |",
        f"| 预计到货 | {order['expected_delivery']} |",
        f"| 金额 | {order['amount']} |",
    ]

    return "\n".join(lines)
