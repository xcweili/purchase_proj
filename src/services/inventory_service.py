# -*- coding: utf-8 -*-
"""
库存服务 - 模拟数据库查询和业务逻辑
实际使用时替换为真实数据库查询
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class InventoryService:
    """库存查询服务"""

    # 模拟数据
    _mock_db = [
        {"material_code": "MAT-001", "material_name": "碳钢钢板", "stock": 500, "unit": "吨", "warehouse": "主仓库-A", "supplier": "宝钢集团"},
        {"material_code": "MAT-002", "material_name": "不锈钢管材", "stock": 200, "unit": "吨", "warehouse": "主仓库-B", "supplier": "太钢集团"},
        {"material_code": "MAT-003", "material_name": "铜芯电缆", "stock": 1000, "unit": "米", "warehouse": "电缆仓库", "supplier": "远东电缆"},
        {"material_code": "MAT-004", "material_name": "标准螺栓M20", "stock": 5000, "unit": "个", "warehouse": "五金仓库", "supplier": "标准件厂"},
        {"material_code": "MAT-005", "material_name": "轴承6205", "stock": 50, "unit": "套", "warehouse": "精密件仓库", "supplier": "洛阳轴承"},
        {"material_code": "MAT-006", "material_name": "碳钢钢板", "stock": 0, "unit": "吨", "warehouse": "主仓库-A", "supplier": "鞍钢集团"},
    ]

    async def search_by_name(self, name: str, supplier: Optional[str] = None) -> list[dict]:
        """按物资名称查询库存（模糊匹配）

        Args:
            name: 物资名称
            supplier: 可选的供应商过滤

        Returns:
            匹配的库存记录列表
        """
        # 模拟异步查询数据库
        results = [
            item for item in self._mock_db
            if name in item["material_name"]
        ]
        if supplier:
            results = [r for r in results if supplier in r["supplier"]]
        return results

    async def search_by_code(self, code: str) -> Optional[dict]:
        """按物资编码精确查询"""
        for item in self._mock_db:
            if item["material_code"] == code:
                return item
        return None

    async def get_low_stock_items(self, threshold: int = 100) -> list[dict]:
        """获取库存低于阈值的物资"""
        return [
            item for item in self._mock_db
            if item["stock"] <= threshold
        ]
