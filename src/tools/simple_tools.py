# -*- coding: utf-8 -*-
"""
基础工具 - 社交/时间类

- greet            : 向用户问好（无参数）
- get_current_time : 查询指定时区的当前时间（timezone 必填，缺失时触发 ask_params 追问）
"""
import logging
import re
from datetime import datetime, timedelta, timezone

from src.tools import tool

logger = logging.getLogger(__name__)


@tool(description="向用户问好")
async def greet() -> str:
    """向用户打招呼"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    return (
        f"您好！我是采购智能助手，很高兴为您服务。\n"
        f"📅 现在是 {now}\n\n"
        f"我可以帮您：\n"
        f"- 查询物资库存（如：碳钢钢板库存）\n"
        f"- 发起采购（如：采购100吨碳钢钢板）\n"
        f"- 查询采购订单（如：查一下订单 PO-2025-001）"
    )


def _resolve_timezone(value: str):
    """把常见时区写法归一化为 tzinfo 对象

    支持：东八区 / 西五区 / UTC+8 / UTC-5 / +08:00 / Asia/Shanghai 等。
    解析失败时默认东八区（UTC+8）。
    """
    value = (value or "").strip()
    if not value:
        return timezone(timedelta(hours=8))

    # 东八区 / 西五区
    m = re.search(r'([东西])\s*(\d+)\s*区', value)
    if m:
        sign = 1 if m.group(1) == "东" else -1
        return timezone(timedelta(hours=sign * int(m.group(2))))

    # UTC+8 / GMT-5 / +08:00 / -05:30
    m = re.search(r'(?:UTC|GMT)?\s*([+-])\s*(\d{1,2})(?::(\d{2}))?', value)
    if m:
        hours = int(m.group(2))
        minutes = int(m.group(3) or 0)
        sign = 1 if m.group(1) == "+" else -1
        return timezone(timedelta(hours=hours, minutes=minutes) * sign)

    # IANA 时区名（Asia/Shanghai 等）
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(value)
    except Exception:
        logger.warning("无法解析时区 %r，默认东八区", value)
        return timezone(timedelta(hours=8))


@tool(description="查询当前时间，需要提供时区（如 东八区、UTC+8、Asia/Shanghai）")
async def get_current_time(timezone: str) -> str:
    """查询指定时区的当前时间

    Args:
        timezone: 时区，如 东八区 / UTC+8 / +08:00 / Asia/Shanghai
    """
    logger.info("查询时间: timezone=%s", timezone)
    tz = _resolve_timezone(timezone)
    now = datetime.now(tz)
    return (
        f"🕐 **当前时间**（{timezone}）\n\n"
        f"| 项目 | 内容 |\n"
        f"|------|------|\n"
        f"| 日期 | {now.strftime('%Y-%m-%d')} |\n"
        f"| 时间 | {now.strftime('%H:%M:%S')} |\n"
        f"| 星期 | {now.strftime('%A')} |\n\n"
        f"📅 查询时间: {now.strftime('%Y-%m-%d %H:%M:%S')}"
    )
