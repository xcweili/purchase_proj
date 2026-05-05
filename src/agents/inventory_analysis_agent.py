# -*- coding: utf-8 -*-
"""库存分析智能体"""
import json
import re
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, Any, List, Optional


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
                                 current_stock: List[Dict[str, Any]], historical_outbound: List[Dict[str, Any]],
                                 outbound_stats: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """根据已有数据计算兜底分析结果（当LLM调用失败或解析失败时使用）

    计算逻辑:
    1. 从current_stock获取当前库存数量和在途库存数量
    2. 计算实际可用库存 = 当前库存 + 在途库存
    3. 使用统计数据计算水位线（优先使用传入的统计数据，否则从历史数据计算）：
       - 高位线：基于历史最高出库或中位数计算
       - 补库线：基于中位数计算
       - 应急线：基于历史最低出库或中位数计算
    4. 判断库存状态（基于实际可用库存）：
       - 低水位：实际可用库存 <= 应急线（需要紧急补库）
       - 较低水位：实际可用库存 <= 补库线（建议补库）
       - 中水位：实际可用库存 <= 高位线 且 > 补库线（正常）
       - 高水位：实际可用库存 > 高位线（库存充足）
    5. 计算建议补货数量：purchaseQty = 高位线 - 实际可用库存（如果实际可用库存 < 高位线）

    Args:
        warehouse_code: 仓库编码
        warehouse_name: 仓库名称
        material_code: 物料编码
        tech_id: 技术规范书ID
        current_stock: 当前库存数据列表（包含 current_stock 和 in_transit_stock 字段）
        historical_outbound: 历史出库数据列表
        outbound_stats: 历史出库统计数据（包含中位数、同比环比等）

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

    # 优先使用传入的统计数据
    if outbound_stats:
        median_qty = float(outbound_stats.get('median_qty', 0)) or 0
        max_qty = float(outbound_stats.get('max_qty', 0)) or 0
        min_qty = float(outbound_stats.get('min_qty', 0)) or 0
        avg_qty = float(outbound_stats.get('avg_qty', 0)) or 0
    else:
        # 从历史数据计算统计值
        qty_list = []
        if historical_outbound and len(historical_outbound) > 0:
            for item in historical_outbound:
                qty = item.get('outbound_qty', 0) or 0
                if isinstance(qty, (int, float)):
                    qty_list.append(qty)
        
        if qty_list:
            median_qty = statistics.median(qty_list) if len(qty_list) >= 2 else qty_list[0]
            max_qty = max(qty_list)
            min_qty = min(qty_list)
            avg_qty = sum(qty_list) / len(qty_list)
        else:
            median_qty = 100
            max_qty = 150
            min_qty = 50
            avg_qty = 100

    # 使用中位数和统计数据计算水位线（与LLM分析逻辑一致）
    # 应急线：仓库存储的最低标准，可以走应急补库的方式去补库了
    # 补库线：可以开始补库了，库存量可能有一些风险了
    # 高位线：现在仓库的库存量已经处于高点了，完全不用再补库
    
    # 基于中位数计算，更稳健
    emergency_line = max(min_qty * 1.5, median_qty * 0.3)
    replenish_level = median_qty
    high_level = min(max_qty * 1.2, median_qty * 1.8)

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
                     data3: Dict[str, Any] = None,
                     stream: bool = False) -> Dict[str, Any]:
        """执行库存分析"""
        if not data1:
            data1 = []
        if not data2:
            data2 = []
        if not data3:
            data3 = {}

        prompt = self._build_prompt(warehouse_code, warehouse_name, material_code, tech_id, data1, data2, data3)

        if stream:
            response_generator = self.llm_stream_func(prompt)
            return {"response_generator": response_generator}
        else:
            response = await self.llm_func(prompt)
            return {"response": response}

    def fallback_analyze(self, warehouse_code: str, warehouse_name: str, material_code: str, tech_id: str,
                       current_stock: List[Dict[str, Any]], historical_outbound: List[Dict[str, Any]],
                       outbound_stats: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
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
            outbound_stats: 历史出库统计数据（包含中位数、同比环比等）

        Returns:
            分析结果列表（包含兜底计算的结果）
        """
        result = _calculate_fallback_analysis(
            warehouse_code, warehouse_name, material_code, tech_id,
            current_stock, historical_outbound, outbound_stats
        )
        return [result]

    def _build_prompt(self, warehouse_code: str, warehouse_name: str, material_code: str, tech_id: str,
                     current_stock: List[Dict[str, Any]], historical_outbound: List[Dict[str, Any]],
                     outbound_stats: Dict[str, Any] = None) -> str:
        """构建库存分析prompt"""
        if outbound_stats is None:
            outbound_stats = {}

        minimal_stock = [_extract_minimal_stock(s) for s in current_stock]
        minimal_outbound = [_extract_minimal_outbound(o) for o in historical_outbound]

        stock_text = json.dumps(minimal_stock, ensure_ascii=False, indent=2, default=_convert_for_json)
        outbound_text = json.dumps(minimal_outbound, ensure_ascii=False, indent=2, default=_convert_for_json)

        stats_text = self._format_stats_text(outbound_stats)

        prompt = f"""你是一个专业的采购管理分析助手，你的任务是根据历史采购数据、目前库存，进行库存分析与预警。

## 基础信息
1、仓库编码：{warehouse_code}
2、仓库名称：{warehouse_name}
3、物料编码：{material_code}
4、技术规范书ID：{tech_id}
5、当前库存数据（currentStock）：{stock_text}
6、历史出库数据（historicalOutbound）：{outbound_text}

## 历史出库数据统计（重要！）
{stats_text}

## 关键概念说明
【实际可用库存】= 当前库存(current_stock) + 在途库存(in_transit_stock)
注意：判断是否需要补库、以及计算补库数量时，应该使用【实际可用库存】
但计算水位线（高位线、补库线、应急线）时，必须基于历史出库数据，不要考虑在途库存

## 三级水位库存预警
高位线：目前仓库库存已经到高点了，不需要再准备相应物料（基于历史数据计算）
补库线：目前仓库还有一些库存，但是需要补货了，应该按历史相关的出库量数据推测（基于历史数据计算）
应急线：目前仓库的库存量严重不足，并且时间上来看，已经等不到补货的供货周期（15-45天）了（基于历史数据计算）

## 分析参数说明

### 1. 正态分布分析
- **中位数（median_outbound）**：是分布的中心点，50%的数据在此值以下
- 如果 **实际可用库存 > 中位数**，说明库存相对充足
- 如果 **实际可用库存 < 中位数**，说明库存相对紧张
- **标准差（std_dev）**：反映数据离散程度，标准差大说明消耗不稳定

### 2. 同比分析（yoy_change）
- **同比 > 0**：最近消耗相比去年同期增长，可能需要增加库存
- **同比 < 0**：最近消耗相比去年同期下降，可以适当减少库存
- **同比 = 0或N/A**：消耗相对稳定

### 3. 环比分析（mom_change）
- **环比 > 0**：本月消耗相比上月增长
- **环比 < 0**：本月消耗相比上月下降

### 4. 季节性判断
- **波动较大**：存在明显的季节性，需要考虑季节因素设置水位线
- **波动适中**：有一定的变化，但不是季节性的
- **波动较小**：消耗相对稳定

## 水位线分析方法

### 水位线制定原则：
请根据历史出库数据统计，综合考虑以下因素，自主分析判断合适的水位线值：
- 历史消耗量趋势（最高、最低、平均、中位数）
- 数据离散程度（标准差）和消耗稳定性
- 同比环比变化趋势
- 季节性特征
- 供货周期（15-45天）
- 物料重要程度和使用场景

### 水位线定义：
- **应急线**：仓库存储的最低标准，低于此线必须走应急补库流程，不能再低了
- **补库线**：可以开始补库了，库存量可能有一定风险
- **高位线**：仓库库存已处于高点，完全不用再补库，可以考虑利库

### 水位判断标准：
- **紧急状态**: 实际可用库存 <= 应急线 → **立即紧急补货**
- **低水位**: 实际可用库存 > 应急线 且 <= 补库线 → **立即补库**
- **中水位**: 实际可用库存 > 补库线 且 <= 高位线 → **建议补库**
- **高水位**: 实际可用库存 > 高位线 → **正常**，无需补库

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
"""
        return prompt

    def _format_stats_text(self, stats: Dict[str, Any]) -> str:
        """格式化统计数据为文本"""
        if not stats:
            return "无统计数据"

        lines = []
        lines.append("| 指标 | 数值 | 说明 |")
        lines.append("|------|------|------|")
        lines.append(f"| 历史最高月出库 | {stats.get('max_outbound', 0):.0f} | 历史单月最大出库量 |")
        lines.append(f"| 历史最低月出库 | {stats.get('min_outbound', 0):.0f} | 历史单月最小出库量 |")
        lines.append(f"| 平均月出库 | {stats.get('avg_outbound', 0):.2f} | 所有月份的平均值 |")
        lines.append(f"| 中位数出库 | {stats.get('median_outbound', 0):.2f} | 50%的月份出库量低于此值 |")

        std_dev = stats.get('std_dev')
        std_dev_text = "N/A" if not std_dev else f"{std_dev:.2f}"
        lines.append(f"| 标准差 | {std_dev_text} | 出库量的离散程度 |")

        yoy = stats.get('yoy_change')
        yoy_text = "N/A" if yoy is None else f"{yoy:+.1f}%"
        lines.append(f"| 同比变化 | {yoy_text} | 去年同期对比 |")

        mom = stats.get('mom_change')
        mom_text = "N/A" if mom is None else f"{mom:+.1f}%"
        lines.append(f"| 环比变化 | {mom_text} | 上月对比 |")

        seasonality = stats.get('seasonality', '数据不足')
        lines.append(f"| 季节性特征 | {seasonality} | 出库波动特征 |")
        lines.append(f"| 数据记录数 | {stats.get('total_records', 0)} | 历史出库记录条数 |")

        return "\n".join(lines)

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
