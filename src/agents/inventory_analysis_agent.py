# -*- coding: utf-8 -*-
"""库存分析智能体"""
import json
import re
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, Any, List


def _convert_for_json(obj):
    """将无法序列化的类型转换为可序列化格式"""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(obj, date):
        return obj.strftime("%Y-%m-%d")
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _extract_minimal_stock(stock: Dict[str, Any]) -> Dict[str, Any]:
    """提取库存关键字段"""
    return {
        "warehouse_code": stock.get('warehouse_code', ''),
        "warehouse_name": stock.get('warehouse_name', ''),
        "material_code": stock.get('material_code', ''),
        "material_name": stock.get('material_desc', ''),
        "tech_id": stock.get('tech_id', ''),
        "current_stock": stock.get('current_stock', 0),
        "in_transit_stock": stock.get('in_transit_stock', 0),
    }


def _extract_minimal_outbound(outbound: Dict[str, Any]) -> Dict[str, Any]:
    """提取历史出库关键字段"""
    return {
        "warehouse_code": outbound.get('warehouse_code', ''),
        "material_code": outbound.get('material_code', ''),
        "tech_id": outbound.get('tech_id', ''),
        "month": outbound.get('month', ''),
        "outbound_qty": outbound.get('outbound_qty', 0),
        "historical_avg_qty": outbound.get('historical_avg_qty', 0),
        "last_1_month_qty": outbound.get('last_1_month_qty', 0),
        "last_2_month_qty": outbound.get('last_2_month_qty', 0),
        "last_3_month_qty": outbound.get('last_3_month_qty', 0),
    }


def _calculate_fallback_analysis(warehouse_code: str, warehouse_name: str, material_code: str, tech_id: str,
                                 current_stock: List[Dict[str, Any]], historical_outbound: List[Dict[str, Any]]) -> Dict[str, Any]:
    """根据已有数据计算兜底分析结果（当LLM调用失败或解析失败时使用）

    计算逻辑:
    1. 从current_stock获取当前库存数量和在途库存数量
    2. 计算实际可用库存 = 当前库存 + 在途库存
    3. 从historical_outbound计算历史平均月消耗量（用于计算水位线）
    4. 水位线计算（基于历史数据，与实际库存无关）：
       - 高位线 = 平均月消耗量 * 1.5
       - 补库线 = 平均月消耗量
       - 应急线 = 平均月消耗量 * 0.5
    5. 判断库存状态（基于实际可用库存）：
       - 低水位：实际可用库存 <= 补库线
       - 中水位：实际可用库存 <= 高位线 且 > 补库线
       - 高水位：实际可用库存 > 高位线
    6. 计算建议补货数量：purchaseQty = 高位线 - 实际可用库存（如果实际可用库存 < 高位线）

    Args:
        warehouse_code: 仓库编码
        warehouse_name: 仓库名称
        material_code: 物料编码
        tech_id: 技术规范书ID
        current_stock: 当前库存数据列表（包含 current_stock 和 in_transit_stock 字段）
        historical_outbound: 历史出库数据列表

    Returns:
        分析结果字典
    """
    stock_qty = 0
    in_transit_qty = 0
    material_desc = ''
    unit = ''
    if current_stock and len(current_stock) > 0:
        stock_qty = float(current_stock[0].get('current_stock', 0) or 0)
        in_transit_qty = float(current_stock[0].get('in_transit_stock', 0) or 0)
        material_desc = current_stock[0].get('material_desc', '') or ''
        unit = current_stock[0].get('unit', '') or ''

    available_stock = stock_qty + in_transit_qty

    avg_monthly_qty = 0
    if historical_outbound and len(historical_outbound) > 0:
        total_qty = 0
        for item in historical_outbound:
            qty = item.get('outbound_qty', 0) or 0
            if isinstance(qty, (int, float)):
                total_qty += qty
        avg_monthly_qty = total_qty / len(historical_outbound) if historical_outbound else 0
    else:
        avg_monthly_qty = 100

    high_level = avg_monthly_qty * 1.5
    replenish_level = avg_monthly_qty
    emergency_line = avg_monthly_qty * 0.5

    if available_stock <= emergency_line:
        stock_status = '低水位'
        suggested_action = '立即补库'
    elif available_stock <= replenish_level:
        stock_status = '低水位'
        suggested_action = '建议补库'
    elif available_stock <= high_level:
        stock_status = '中水位'
        suggested_action = '正常'
    else:
        stock_status = '高水位'
        suggested_action = '库存充足'

    if available_stock < high_level:
        purchase_qty = high_level - available_stock
    else:
        purchase_qty = 0

    return {
        'warehouseName': warehouse_name,
        'materialCode': material_code,
        'techId': tech_id,
        'purchaseQty': round(purchase_qty, 2),
        'highLevel': round(high_level, 2),
        'replenishLevel': round(replenish_level, 2),
        'emergencyLine': round(emergency_line, 2),
        'currentStock': round(stock_qty, 2),
        'inTransitStock': round(in_transit_qty, 2),
        'availableStock': round(available_stock, 2),
        'stockStatus': stock_status,
        'suggestedAction': suggested_action,
        'unit': unit,
        'materialDesc': material_desc
    }


class InventoryAnalysisAgent:
    """库存分析智能体 - 负责库存分析与预警"""

    def __init__(self, db, llm_stream_func, llm_func):
        self.db = db
        self.llm_stream_func = llm_stream_func
        self.llm_func = llm_func

    async def analyze(self, warehouse_code: str = "", warehouse_name: str = "",
                     material_code: str = "", tech_id: str = "",
                     data1: List[Dict[str, Any]] = None, data2: List[Dict[str, Any]] = None,
                     stream: bool = False) -> Dict[str, Any]:
        """执行库存分析"""
        if not data1:
            data1 = []
        if not data2:
            data2 = []

        prompt = self._build_prompt(warehouse_code, warehouse_name, material_code, tech_id, data1, data2)

        if stream:
            response_generator = self.llm_stream_func(prompt)
            return {"response_generator": response_generator}
        else:
            response = await self.llm_func(prompt)
            return {"response": response}

    def fallback_analyze(self, warehouse_code: str, warehouse_name: str, material_code: str, tech_id: str,
                       current_stock: List[Dict[str, Any]], historical_outbound: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """当LLM调用失败或解析失败时，使用本地计算返回兜底结果

        调用时机:
        1. LLM调用抛出异常
        2. parse_analysis_response 返回空列表

        Args:
            warehouse_code: 仓库编码
            warehouse_name: 仓库名称
            material_code: 物料编码
            tech_id: 技术规范书ID
            current_stock: 当前库存数据列表
            historical_outbound: 历史出库数据列表

        Returns:
            分析结果列表（包含兜底计算的结果）
        """
        result = _calculate_fallback_analysis(
            warehouse_code, warehouse_name, material_code, tech_id,
            current_stock, historical_outbound
        )
        return [result]

    def _build_prompt(self, warehouse_code: str, warehouse_name: str, material_code: str, tech_id: str,
                     current_stock: List[Dict[str, Any]], historical_outbound: List[Dict[str, Any]]) -> str:
        """构建库存分析prompt"""
        minimal_stock = [_extract_minimal_stock(s) for s in current_stock]
        minimal_outbound = [_extract_minimal_outbound(o) for o in historical_outbound]

        stock_text = json.dumps(minimal_stock, ensure_ascii=False, indent=2, default=_convert_for_json)
        outbound_text = json.dumps(minimal_outbound, ensure_ascii=False, indent=2, default=_convert_for_json)

        prompt = f"""你是一个专业的采购管理分析助手，你的任务是根据历史采购数据、目前库存，进行库存分析与预警，流程大概是：
1. 查看有哪些仓库+物料+技术规范书id的组合，它们的历史数据跟目前存储数据分别是多少；
2. 遍历每一个分析对象，根据影响因子，对已有数据进行分析；
3. 得出每个对象的三级水位库存，是否要补货，要补多少货这些信息，按照约定的输出格式，输出一个json对象，注意每一个字段的值不要错

## 基础信息
1、仓库编码：{warehouse_code}
2、仓库名称：{warehouse_name}
3、物料编码：{material_code}
4、技术规范书ID：{tech_id}
5、当前库存数据（currentStock）：{stock_text}
6、历史出库数据（historicalOutbound）：{outbound_text}
7、补货频率：周
8、供货周期：15-45天

## 关键概念说明
【实际可用库存】= 当前库存(current_stock) + 在途库存(in_transit_stock)
注意：判断是否需要补库、以及计算补库数量时，应该使用【实际可用库存】
但计算水位线（高位线、补库线、应急线）时，必须基于历史出库数据，不要考虑在途库存

## 三级水位库存预警
高位线：目前仓库库存已经到高点了，不需要再准备相应物料（基于历史数据计算）
补库线：目前仓库还有一些库存，但是需要补货了，应该按历史相关的出库量数据推测（基于历史数据计算）
应急线：目前仓库的库存量严重不足，并且时间上来看，已经等不到补货的供货周期（15-45天）了（基于历史数据计算）

## 分析要求
1. 只分析一组数据：仓库+物料+tech_id，不要分析多个
2. 先分析历史出库数据（historicalOutbound）的趋势：
   - 计算历史平均月消耗量
   - 分析同比、环比情况（与上月、去年同期对比）
   - 判断是否有季节性波动特征
3. 基于以下因素综合设定高位线、补库线、应急线（不要考虑in_transit_stock）：
   - 物料种类（例如：常用物料、核心物料、备品备件、季节性物料等）
   - 历史消耗量的稳定性
   - 供货周期（15-45天）
   - 季节性因素（是否为季节消耗大的物料）
   - 同比环比增长/下降趋势
   - 历史最大/最小消耗量
4. 判断库存状态时，使用【实际可用库存】= current_stock + in_transit_stock
5. 判断库存状态（不足/正常/充足）：比较实际可用库存与水位线
6. 计算补货数量时，使用公式：补货数量 = 高位线 - 实际可用库存（如果实际可用库存 < 高位线）

## 输出格式
严格遵循以下json格式输出，不要带任何其他信息：
```json
{{
    "warehouseName": "仓库名称",
    "materialCode": "物料编码",
    "techId": "技术规范书ID",
    "purchaseQty": 采购申请数量（实际可用库存不足高位线时为正数，否则为0）,
    "highLevel": 高位线（综合历史数据、季节、同比环比等因素制定）,
    "replenishLevel": 补库线（综合历史数据、季节、同比环比等因素制定）,
    "emergencyLine": 应急线（综合历史数据、季节、同比环比等因素制定）,
    "currentStock": 当前库存数量,
    "inTransitStock": 在途库存数量,
    "availableStock": 实际可用库存（currentStock + inTransitStock）,
    "unit": "单位",
    "materialDesc": "物料描述"
}}
```

说明：
- warehouseName: 仓库名称
- materialCode: 物料编码
- techId: 技术规范书ID
- purchaseQty: 需要采购的数量，基于【实际可用库存】计算得出
- highLevel: 高位线（综合历史数据、季节性、同比环比等因素制定，不是简单公式）
- replenishLevel: 补库线（综合历史数据、季节性、同比环比等因素制定，不是简单公式）
- currentStock: 当前库存数量（不含在途）
- inTransitStock: 在途库存数量
- availableStock: 实际可用库存 = currentStock + inTransitStock
- emergencyLine: 应急线（综合历史数据、季节性、同比环比等因素制定，不是简单公式）
- unit: 计量单位
- materialDesc: 物料描述
"""
        return prompt

    def parse_analysis_response(self, response: str) -> List[Dict[str, Any]]:
        """解析模型返回的分析结果"""
        if not response:
            return []

        response = response.strip()

        json_match = re.search(r'\{[\s\S]*\}', response)
        if json_match:
            json_str = json_match.group(0)
            try:
                result = json.loads(json_str)
                if isinstance(result, dict) and ('warehouseName' in result or 'warehouseCode' in result):
                    return [result]
            except json.JSONDecodeError:
                pass

        code_blocks = re.findall(r'```(?:json)?\s*([\s\S]*?)\s*```', response, re.IGNORECASE)
        for block in code_blocks:
            try:
                result = json.loads(block.strip())
                if isinstance(result, dict) and ('warehouseName' in result or 'warehouseCode' in result):
                    return [result]
            except json.JSONDecodeError:
                continue

        return []
