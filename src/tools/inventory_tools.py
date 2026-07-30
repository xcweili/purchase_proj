# -*- coding: utf-8 -*-
"""
库存查询工具 - 复杂工具示例
"""
import logging
from datetime import datetime

from src.tools import tool
from src.services.inventory_service import InventoryService

logger = logging.getLogger(__name__)

# 服务实例（或使用依赖注入）
inventory_service = InventoryService()


@tool(description="按物资名称查询库存，支持模糊搜索，可选按供应商过滤")
async def query_inventory_by_name(
    name: str,
    supplier: str = "",
) -> str:
    """查询物资库存

    按物资名称模糊搜索库存，如果有 supplier 参数则进一步按供应商过滤。

    Args:
        name: 物资名称，如 碳钢钢板、电缆
        supplier: 供应商名称（可选），如 宝钢
    """
    logger.info("查询库存: name=%s, supplier=%s", name, supplier)

    # 1. 参数校验
    if not name or len(name.strip()) == 0:
        return "请输入有效的物资名称"

    # 2. 调用业务服务层
    results = await inventory_service.search_by_name(
        name=name.strip(),
        supplier=supplier.strip() if supplier else None,
    )

    # 3. 结果格式化
    if not results:
        return f"未找到与「{name}」相关的库存记录"

    lines = [f"📦 **「{name}」库存查询结果**（共 {len(results)} 条）\n"]
    lines.append("| 状态 | 物资名称 | 编码 | 库存 | 仓库 | 供应商 |")
    lines.append("|------|----------|------|------|------|--------|")
    for item in results:
        status = "🔴" if item["stock"] == 0 else ("🟡" if item["stock"] < 100 else "🟢")
        lines.append(
            f"| {status} | {item['material_name']} | {item['material_code']} "
            f"| {item['stock']}{item['unit']} | {item['warehouse']} | {item['supplier']} |"
        )

    lines.append(f"\n📅 查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    return "\n".join(lines)


@tool(description="查询库存不足的物资，可指定库存阈值（默认低于100）")
async def query_low_stock(threshold: int = 100) -> str:
    """查询低库存物资

    Args:
        threshold: 库存阈值，低于此值的物资会被列出，默认 100
    """
    logger.info("查询低库存: threshold=%d", threshold)

    # 1. 调用业务服务
    items = await inventory_service.get_low_stock_items(threshold=threshold)

    # 2. 格式化结果
    if not items:
        return f"✅ 所有物资库存均高于 {threshold}，库存充足"

    lines = [f"⚠️ **库存不足预警**（阈值: {threshold}，共 {len(items)} 项）\n"]
    for item in items:
        lines.append(
            f"- **{item['material_name']}** {item['stock']}/{item['unit']}"
            f"（{item['warehouse']}）"
        )

    lines.append(f"\n📅 查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    return "\n".join(lines)
