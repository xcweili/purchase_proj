# -*- coding: utf-8 -*-
"""
审批工具 - 演示 Human-in-the-Loop（人工介入）
在执行批量采购时，需要人工确认后才能继续
"""
import uuid
import logging
from datetime import datetime

from src.tools import tool

logger = logging.getLogger(__name__)

# 模拟物资单价（元/单位）
_UNIT_PRICES = {
    "碳钢钢板": 4500,
    "不锈钢管材": 6400,
    "铜芯电缆": 90,
    "标准螺栓M20": 3,
    "轴承6205": 120,
}


@tool(description="批量采购物资，需要人工确认后才能执行")
async def batch_purchase(material: str, quantity: float, unit: str = "吨") -> dict:
    """批量采购物资

    这个工具演示 Human-in-the-Loop 模式：
    1. 先计算采购金额并展示给用户
    2. 需要用户在前端点击"确认采购"或"取消"
    3. 用户确认后，才真正"执行"采购

    Args:
        material: 物资名称，如 碳钢钢板
        quantity: 采购数量
        unit: 单位，默认 吨
    """
    logger.info("发起采购申请: material=%s, quantity=%s%s", material, quantity, unit)

    unit_price = _UNIT_PRICES.get(material, 5000)
    total_amount = quantity * unit_price

    # 返回需要确认的信号
    confirm_id = str(uuid.uuid4())[:8]
    summary = (
        f"📋 **采购申请单**\n\n"
        f"| 项目 | 内容 |\n"
        f"|------|------|\n"
        f"| 物资名称 | {material} |\n"
        f"| 数量 | {quantity} {unit} |\n"
        f"| 单价 | ¥{unit_price:,}/{unit} |\n"
        f"| **总金额** | **¥{total_amount:,}** |\n"
        f"| 申请时间 | {datetime.now().strftime('%Y-%m-%d %H:%M')} |\n"
    )

    return {
        "_requires_confirm": True,      # 标记需要人工确认
        "confirm_id": confirm_id,       # 确认会话 ID
        "question": summary,             # 展示给用户的确认信息
        "options": [                     # 可选操作
            {"value": "confirm", "label": "✅ 确认采购"},
            {"value": "cancel",  "label": "❌ 取消"},
        ],
        # 确认后需要执行的信息
        "on_confirm": {
            "tool": "_do_purchase",
            "params": {
                "material": material,
                "quantity": quantity,
                "unit": unit,
                "total_amount": total_amount,
            },
        },
    }


async def _do_purchase(material: str, quantity: float, unit: str, total_amount: float) -> str:
    """确认后的实际采购执行（模拟）"""
    logger.info("执行采购: %s %s%s, 金额=¥%s", material, quantity, unit, total_amount)

    order_no = f"PO-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:4].upper()}"
    return (
        f"✅ **采购已确认并执行**\n\n"
        f"| 项目 | 内容 |\n"
        f"|------|------|\n"
        f"| 订单号 | {order_no} |\n"
        f"| 物资 | {material} |\n"
        f"| 数量 | {quantity} {unit} |\n"
        f"| 总金额 | ¥{total_amount:,} |\n"
        f"| 状态 | 🔄 已提交至审批流程 |\n"
        f"| 执行时间 | {datetime.now().strftime('%Y-%m-%d %H:%M')} |\n"
    )
