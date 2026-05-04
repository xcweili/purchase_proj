# -*- coding: utf-8 -*-
"""仓库调配智能体"""
import json
import logging
import re
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


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
    """提取库存关键字段，减少prompt上下文"""
    return {
        "material_code": stock.get('material_code') or stock.get('materialCode', ''),
        "tech_id": stock.get('tech_id') or '',
        "stock_qty": stock.get('stock_qty', 0),
        "loc_code": stock.get('loc_code') or stock.get('warehouseCode', ''),
        "loc_name": stock.get('loc_name') or stock.get('warehouseName', ''),
        "factory_name": stock.get('factory_name', ''),
        "source_type": stock.get('source_type', ''),
    }


def _enrich_result(result: Dict[str, Any], original_stocks: List[Dict[str, Any]],
                   material_code: str, warehouse_code: str) -> Dict[str, Any]:
    """根据原始库存数据补全结果中的仓库名称等信息"""
    source_warehouse = result.get('warehouseCode', '')

    if source_warehouse:
        for stock in original_stocks:
            stock_loc = stock.get('loc_code') or stock.get('warehouseCode', '') or ''
            stock_material = stock.get('material_code') or stock.get('materialCode', '') or ''

            if stock_loc == source_warehouse and stock_material == material_code:
                if not result.get('warehouseName'):
                    result['warehouseName'] = stock.get('loc_name') or stock.get('warehouseName', '')
                if not result.get('availableStock'):
                    result['availableStock'] = stock.get('stock_qty', 0)
                if not result.get('sourceType'):
                    result['sourceType'] = stock.get('source_type', '') or stock.get('factory_name', '')
                break

    if not result.get('warehouseName'):
        for stock in original_stocks:
            stock_material = stock.get('material_code') or stock.get('materialCode', '') or ''
            if stock_material == material_code:
                result['warehouseName'] = stock.get('loc_name') or stock.get('warehouseName', '')
                break

    return result


def _calculate_fallback_allocation(plan: Dict[str, Any], all_stocks: List[Dict[str, Any]],
                                  warehouse_code: str, strategy: str) -> Dict[str, Any]:
    """根据已有数据和策略计算兜底调配结果（当LLM调用失败或解析失败时使用）

    计算逻辑:
    1. 优先级顺序：本库 > 单位库 > 区域库 > 全仓库
    2. 按策略选择：time(时效)优先距离近的，cost(花销)优先成本低的，stock(库存)优先库存多的，emerg(应急)直接申请

    Args:
        plan: 计划数据（包含 demandQty, materialCode, techSpecId 等）
        all_stocks: 库存数据列表
        warehouse_code: 当前仓库编码
        strategy: 调配策略（time/cost/stock/emerg）

    Returns:
        调配结果字典
    """
    material_code = plan.get('materialCode') or plan.get('material_code', '')
    tech_spec_id = plan.get('techSpecId') or ''
    demand_qty = plan.get('demandQty') or plan.get('demand_qty', 0)
    plan_id = plan.get('planId') or plan.get('plan_id', '')

    logger.info(f"[Fallback] 开始计算: material={material_code}, tech_id={tech_spec_id}, demand={demand_qty}, target_wh={warehouse_code}, strategy={strategy}")

    # 过滤出对应物料的库存（同时匹配 tech_id 如果计划中有的话）
    def _stock_matches_plan(stock):
        stock_material = stock.get('material_code') or stock.get('materialCode', '')
        stock_tech_id = stock.get('tech_id') or ''
        if stock_material != material_code:
            return False
        if tech_spec_id and stock_tech_id and stock_tech_id != tech_spec_id:
            return False
        return True

    material_stocks = [s for s in all_stocks if _stock_matches_plan(s)]
    logger.info(f"[Fallback] 匹配到的库存记录数: {len(material_stocks)}")
    
    if not material_stocks:
        return {
            'planId': plan_id,
            'planCode': plan.get('planCode', ''),
            'materialCode': material_code,
            'materialDesc': plan.get('materialDesc', ''),
            'demandQty': demand_qty,
            'unit': plan.get('unit', ''),
            'unitName': plan.get('unitName', ''),
            'projectName': plan.get('projectName', ''),
            'warehouseCode': '',
            'warehouseName': '',
            'matchedQty': 0,
            'availableStock': 0,
            'score': 0,
            'status': 'none',
            'statusName': '无匹配',
            'sourceType': '',
            'reason': '无可用库存'
        }
    
    # 排除当前仓库的库存（因为是调入仓库）
    other_stocks = [s for s in material_stocks 
                   if (s.get('loc_code') or s.get('warehouseCode', '')) != warehouse_code]
    
    if not other_stocks:
        other_stocks = material_stocks
    
    # 按策略排序
    def _get_sort_key(x):
        distance = x.get('distance')
        if distance is None:
            # 距离未知时，降级为按库存数量排序（库存多的优先）
            return (999999, -float(x.get('stock_qty', 0) or 0))
        elif distance == 0:
            # 距离为0表示同仓库，最高优先级
            return (0, -float(x.get('stock_qty', 0) or 0))
        else:
            # 有距离数据时，按距离排序（距离近的优先）
            return (float(distance),)

    # 核心逻辑：优先选择能完全满足需求的仓库
    # 1. 先筛选出能完全满足需求的仓库
    full_match_stocks = [s for s in other_stocks if float(s.get('stock_qty', 0) or 0) >= demand_qty]
    
    logger.info(f"[Fallback] 能完全满足需求的仓库数: {len(full_match_stocks)}")
    
    if full_match_stocks:
        # 有能完全满足需求的仓库，在这些仓库中按策略排序
        if strategy in ('time', 'cost'):
            # 时效/成本策略：按距离排序
            full_match_stocks = sorted(full_match_stocks, key=_get_sort_key)
        elif strategy in ('stock', 'emerg'):
            # 库存/应急策略：按库存数量排序
            full_match_stocks = sorted(full_match_stocks, key=lambda x: float(x.get('stock_qty', 0) or 0), reverse=True)
        
        logger.info(f"[Fallback] 完全匹配仓库排序后: {[(s.get('loc_code'), s.get('distance'), s.get('stock_qty')) for s in full_match_stocks[:3]]}")
        best_stock = full_match_stocks[0]
    else:
        # 没有能完全满足需求的仓库，按策略排序所有仓库
        logger.info(f"[Fallback] 没有能完全满足需求的仓库，按策略排序")
        if strategy in ('time', 'cost'):
            other_stocks = sorted(other_stocks, key=_get_sort_key)
        elif strategy in ('stock', 'emerg'):
            other_stocks = sorted(other_stocks, key=lambda x: float(x.get('stock_qty', 0) or 0), reverse=True)
        
        logger.info(f"[Fallback] 所有仓库排序后: {[(s.get('loc_code'), s.get('distance'), s.get('stock_qty')) for s in other_stocks[:3]]}")
        best_stock = other_stocks[0] if other_stocks else None

    if not best_stock:
        return {
            'planId': plan_id,
            'planCode': plan.get('planCode', ''),
            'materialCode': material_code,
            'materialDesc': plan.get('materialDesc', ''),
            'demandQty': demand_qty,
            'unit': plan.get('unit', ''),
            'unitName': plan.get('unitName', ''),
            'projectName': plan.get('projectName', ''),
            'warehouseCode': '',
            'warehouseName': '',
            'matchedQty': 0,
            'availableStock': 0,
            'score': 0,
            'status': 'none',
            'statusName': '无匹配',
            'sourceType': '',
            'reason': '无可用库存'
        }

    source_warehouse = best_stock.get('loc_code') or best_stock.get('warehouseCode', '')
    source_warehouse_name = best_stock.get('loc_name') or best_stock.get('warehouseName', '')
    source_stock = float(best_stock.get('stock_qty', 0) or 0)

    logger.info(f"[Fallback] 最终选择仓库: {source_warehouse}, 库存: {source_stock}, 需求: {demand_qty}")

    source_type = best_stock.get('factory_name', '') or best_stock.get('source_type', '')

    # 计算匹配数量
    matched_qty = min(demand_qty, source_stock)
    
    # 确定状态
    if matched_qty >= demand_qty:
        status = 'full'
        status_name = '完全匹配'
    elif matched_qty > 0:
        status = 'partial'
        status_name = '部分匹配'
    else:
        status = 'none'
        status_name = '无匹配'
    
    return {
        'planId': plan_id,
        'planCode': plan.get('planCode', ''),
        'materialCode': material_code,
        'materialDesc': plan.get('materialDesc', ''),
        'demandQty': demand_qty,
        'unit': plan.get('unit', ''),
        'unitName': plan.get('unitName', ''),
        'projectName': plan.get('projectName', ''),
        'warehouseCode': source_warehouse,
        'warehouseName': source_warehouse_name,
        'matchedQty': matched_qty,
        'availableStock': source_stock,
        'score': 80 if matched_qty >= demand_qty else (60 if matched_qty > 0 else 0),
        'status': status,
        'statusName': status_name,
        'sourceType': source_type,
        'reason': '库存充足，可直接满足需求' if matched_qty >= demand_qty else ('部分满足需求' if matched_qty > 0 else '库存不足')
    }


class AllocationAgent:
    """仓库调配智能体 - 负责仓库间库存调配"""

    def __init__(self, db, llm_stream_func, llm_func):
        self.db = db
        self.llm_stream_func = llm_stream_func
        self.llm_func = llm_func

    async def process_single_plan(self, plan: Dict[str, Any], all_stocks: List[Dict[str, Any]],
                                   warehouse_code: str, strategy: str) -> Dict[str, Any]:
        """处理单个计划，与智能体交互获取调配信息"""
        try:
            prompt = self._build_single_plan_prompt(plan, all_stocks, warehouse_code, strategy)

            response = await self.llm_func(prompt)

            results = self.parse_allocation_response(response)

            if results and len(results) > 0:
                return results[0]
            
            logger.info(f"[AllocationAgent] LLM解析结果为空，使用本地兜底计算")
        except Exception as e:
            logger.error(f"[AllocationAgent] LLM调用失败: {str(e)}，使用本地兜底计算")
        
        return self.fallback_process_single_plan(plan, all_stocks, warehouse_code, strategy)

    def fallback_process_single_plan(self, plan: Dict[str, Any], all_stocks: List[Dict[str, Any]],
                                    warehouse_code: str, strategy: str) -> Dict[str, Any]:
        """当LLM调用失败或解析失败时，使用本地计算返回兜底结果
        
        调用时机:
        1. LLM调用抛出异常
        2. parse_allocation_response 返回空列表
        
        Args:
            plan: 计划数据
            all_stocks: 库存数据列表
            warehouse_code: 当前仓库编码
            strategy: 调配策略
        
        Returns:
            根据已有数据和策略计算的兜底结果
        """
        return _calculate_fallback_allocation(plan, all_stocks, warehouse_code, strategy)

    def _build_single_plan_prompt(self, plan: Dict[str, Any], all_stocks: List[Dict[str, Any]],
                                  warehouse_code: str, strategy: str) -> str:
        """构建单个计划的调配分析prompt"""
        minimal_stocks = [_extract_minimal_stock(s) for s in all_stocks]

        plan_text = json.dumps(plan, ensure_ascii=False, indent=2, default=_convert_for_json)
        stocks_text = json.dumps(minimal_stocks, ensure_ascii=False, indent=2, default=_convert_for_json)

        # 从 all_stocks 中提取仓库距离信息
        distances_map = {}
        for stock in all_stocks:
            loc = stock.get('loc_code', '')
            dist = stock.get('distance')
            if loc and loc not in distances_map:
                # 将 Decimal 或其他数值类型转换为 float
                if dist is not None:
                    distances_map[loc] = float(dist)
                else:
                    distances_map[loc] = None
        distances_text = json.dumps(distances_map, ensure_ascii=False, default=_convert_for_json) if distances_map else "{}"

        prompt = f"""你是一个专业的采购管理分析助手，负责仓库间的库存调配决策。

================================================================================
【任务目标】
================================================================================

根据领用计划的需求，从其他仓库中选择最优的调出仓库，确保货物能够及时、准确地调配到位。

================================================================================
【输入数据说明】
================================================================================

1. **当前仓库编码**（warehouse_code）
   - 含义：需求方所在的仓库编码，即货物需要调入的目标仓库
   - 用途：用于排除从本仓库调货的情况（不能从自己调给自己）

2. **当前需求计划内容**（plan_text）
   - 包含字段：
     - planId: 计划唯一标识
     - planCode: 计划编号
     - materialCode: 物料编码（物料的唯一标识）
     - materialDesc: 物料描述（物料的名称、规格等信息）
     - techSpecId: 技术规范书ID（物料的技术规格标识，必须完全匹配）
     - demandQty: 需求数量（需要调配的货物数量）
     - unit: 计量单位
     - unitName: 需求单位名称
     - projectName: 项目名称
   - 用途：明确需要调配什么物料、需要多少、给谁用

3. **当前各个仓库的库存信息**（stocks_text）
   - 包含字段：
     - material_code: 物料编码（与需求计划的materialCode对应）
     - tech_id: 技术规范书ID（与需求计划的techSpecId对应，已确保匹配）
     - stock_qty: 库存数量（该仓库中该物料的可用数量）
     - loc_code: 仓库编码（调出货物的来源仓库）
     - loc_name: 仓库名称
     - factory_name: 工厂名称
     - source_type: 库存类型（如：在途、专业仓、库存、供应商、成品等）
   - 重要说明：
     - 数据已按物料编码+技术规范书ID过滤，所有记录都匹配需求
     - 同一个仓库可能有多条记录（不同工厂、不同库存类型）
     - 需要累加同一仓库的所有库存数量来计算总可用库存
   - 用途：了解哪些仓库有货、有多少货

4. **当前仓库与其他仓库的距离**（distances_text）
   - 格式：{{"仓库编码": 距离数值, ...}}
   - 含义：从该仓库到目标仓库（需求方仓库）的运输距离
   - 单位：公里（km）
   - 特殊值：
     - null 或 不存在：距离未知
     - 0：同一仓库（通常不会出现，因为已排除目标仓库）
   - 用途：计算运输时间、成本，用于时效策略和成本策略

5. **当前传入的调配策略类型**（strategy）
   - 可选值：
     - time（时效优先）：优先选择距离最近的仓库，缩短运输时间
     - cost（成本优先）：优先选择运输成本最低的仓库（通常也是距离近的）
     - stock（库存优先）：优先选择库存最充足的仓库
     - emerg（应急优先）：紧急情况，直接走应急申请流程

================================================================================
【决策规则】（必须严格遵守，按优先级从高到低执行）
================================================================================

### 规则1：首要条件 - 库存充足性
选择的仓库必须能够提供足够的货物：
- 该仓库的库存数量 >= 需求数量（demandQty）
- 如果单个仓库库存不足，可以考虑从多个仓库调配（返回多条记录）

### 规则2：排除目标仓库
不能从当前仓库（warehouse_code）调货给自己，需要从其他仓库调配。

### 规则3：按策略选择最优仓库

**时效策略（time）**：
1. 在能完全满足需求的仓库中，选择距离最近的
2. 距离相同时，选择库存更多的
3. 距离未知时，降级为按库存数量排序

**成本策略（cost）**：
1. 在能完全满足需求的仓库中，选择运输成本最低的（通常距离最近）
2. 距离相同时，选择库存更多的

**库存策略（stock）**：
1. 在能完全满足需求的仓库中，选择库存最充足的
2. 库存相同时，选择距离更近的

**应急策略（emerg）**：
1. 直接标记为应急申请
2. 如果有库存，选择库存最多的仓库

### 规则4：状态判定
- full（完全匹配）：匹配数量 >= 需求数量
- partial（部分匹配）：0 < 匹配数量 < 需求数量
- none（无匹配）：没有可用库存

================================================================================
【输出格式要求】
================================================================================

1. 严格遵循JSON格式输出，不要包含任何其他文字说明
2. 返回一个数组，每个元素代表一个调配方案
3. 字段说明：
   - planId: 从需求计划中复制
   - planCode: 从需求计划中复制
   - materialCode: 从需求计划中复制
   - materialDesc: 从需求计划中复制
   - demandQty: 从需求计划中复制
   - unit: 从需求计划中复制
   - unitName: 从需求计划中复制
   - projectName: 从需求计划中复制
   - warehouseCode: 选择的调出仓库编码
   - warehouseName: 选择的调出仓库名称
   - matchedQty: 实际匹配数量（不超过可用库存）
   - availableStock: 该仓库的可用库存数量
   - score: 匹配得分（0-100，完全匹配给90-100，部分匹配给60-89，无匹配给0）
   - status: 状态（full/partial/none）
   - statusName: 状态名称（完全匹配/部分匹配/无匹配）
   - sourceType: 库存类型（从库存信息中获取）
   - reason: 选择理由（简要说明为什么选择这个仓库）

================================================================================
【输入数据】
================================================================================

1、当前仓库编码：{warehouse_code or ""}

2、当前需求计划内容：
```json
{plan_text}
```

3、当前各个仓库的库存信息（已按物料编码+技术规范书ID过滤）：
```json
{stocks_text}
```

4、当前仓库与其他仓库的距离（数值越小距离越近，单位：公里）：
```json
{distances_text}
```

5、当前传入的调配策略类型：{strategy or "time"}

================================================================================
【输出示例】
================================================================================

```json
[
    {{
        "planId": "03a9f5df8e4c4319955952d3b6d31422",
        "planCode": "20250303025156021203",
        "materialCode": "500029525",
        "materialDesc": "耐张线夹-楔型绝缘,NXL-3",
        "demandQty": 197.0,
        "unit": "付",
        "unitName": "国网湖南省电力有限公司张家界供电分公司",
        "projectName": "湖南张家界桑植县10kV廖上线盐井塘柱上变压器台区电压越限治理工程",
        "warehouseCode": "MGB0",
        "warehouseName": "长沙区域库",
        "matchedQty": 197.0,
        "availableStock": 449.0,
        "score": 95,
        "status": "full",
        "statusName": "完全匹配",
        "sourceType": "库存",
        "reason": "距离最近（20公里），库存充足（449>197），可完全满足需求"
    }}
]
```

请根据以上规则和数据，输出调配结果：
"""
        return prompt

    def parse_allocation_response(self, response: str) -> List[Dict[str, Any]]:
        """解析模型返回的调配结果"""
        if not response:
            return []

        response = response.strip()

        json_match = re.search(r'\[.*\]', response, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
            try:
                results = json.loads(json_str)
                if isinstance(results, list):
                    return results
            except json.JSONDecodeError:
                pass

        code_blocks = re.findall(r'```(?:json)?\s*([\s\S]*?)\s*```', response, re.IGNORECASE)
        for block in code_blocks:
            try:
                results = json.loads(block.strip())
                if isinstance(results, list):
                    return results
            except json.JSONDecodeError:
                continue

        brace_matches = re.findall(r'\{[\s\S]*?\}', response)
        for match in brace_matches:
            try:
                result = json.loads(match)
                if isinstance(result, dict) and 'materialCode' in result:
                    return [result]
            except json.JSONDecodeError:
                continue

        return []
