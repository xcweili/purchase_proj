# -*- coding: utf-8 -*-
"""供应商匹配智能体"""
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

SUPPLIER_MATCH_PROMPT = """你是一个专业的采购管理分析助手，你的任务是分析物料补库的需求，根据供应商的协议执行情况，选择最合适的供应商进行协议补货：
1. 查看需求补货列表，依次分析需求，确认需要哪些物料，然后计算补充这批物料需要多少；
2. 查看物料对应的供货商（协议商），首先确认哪些供货商可选，其次确认每家供货商的执行比例是多少；
3. 根据执行比例的具体策略，确认这一次的补货需求，应该怎么从各个供货商进行补货（可以同时从多家补货）；
4. 制定了合理的补货策略，按照约定的输出格式，输出一个json对象，注意每一个字段的值不要错

## 基础信息：
```
1、补货计划列表（需要哪些物料，需要多少）
2、供货商列表（有哪些供应商，各个供货商的执行比例是什么样的）
3、补货频率：周
4、供货周期长：15-45天
5、执行比例的计算方式：已执行金额 / 执行金额总额度
6、供货商的执行比例阶梯：20%、50%、80%
```

## 影响因子
-- 以下是匹配时，均衡策略的详细情况
```
1、必须从规定的供货商中选择
2、严格遵循供货商执行比例阶梯，举个例子，有3家供货商，其中两家现在20%以上，有一家是20%以下，而20%是阶梯，因此当前供货商，只能选择20%以下的这一家，只有当他的执行比例满足跨越20%阶梯时，才可以选择其他的供货商，同理对于50%阶梯的执行比例也是相同算法
3、allocateAmount（分配金额）必须等于 allocateQty × unitPrice，绝不能把 unitPrice 当作 allocateAmount！
4、allocateAmount 不能超过 remainAmount（剩余可用金额）
```

-- 三种策略说明：
1、 **均衡策略**：按执行比例分配
2、 **成本策略**：根据供应商单价计算，选择总成本最低的方案
3、 **配送策略**：优先满足单个供应商能满足全部需求的，简化配送


## 字段含义说明（非常重要！请仔细阅读）
### 输入数据字段说明：
- `materialCode`: 物料编码
- `materialDesc`: 物料描述
- `demandQty`: 补货计划需求数量
- `unitPrice`: 物料单价
- `executionRate`: 已执行比例（百分比数值）
- `remainQty`: 供应商剩余可用库存

### 输出数据字段说明：

**重要约束：每个策略只能使用一个供应商！**

每个补货计划需要返回一个对象，包含三种策略：
```json
{{
    "planId": "计划ID",
    "materialCode": "物料编码",
    "materialDesc": "物料描述",
    "demandQty": 需求数量,
    "warehouseCode": "仓库编码",
    "techSpecId": "技术规范ID",
    "strategies": {{
        "balanced": {{
            "suppliers": [
                {{
                    "supplierCode": "供应商编码",
                    "supplierName": "供应商名称",
                    "unitPrice": 该供应商的单价,
                    "allocatedQty": 分配数量（必须等于demandQty）,
                    "executionRate": 执行比例
                }}
            ],
            "totalCost": 总花费（计算公式：allocatedQty × unitPrice，必须>0）,
            "remark": "策略说明"
        }},
        "cost": {{
            "suppliers": [
                {{
                    "supplierCode": "供应商编码",
                    "supplierName": "供应商名称",
                    "unitPrice": 该供应商的单价,
                    "allocatedQty": 分配数量（必须等于demandQty）,
                    "executionRate": 执行比例
                }}
            ],
            "totalCost": 总花费（计算公式：allocatedQty × unitPrice，必须>0）,
            "remark": "优先选择单价最低的供应商"
        }},
        "delivery": {{
            "suppliers": [
                {{
                    "supplierCode": "供应商编码",
                    "supplierName": "供应商名称",
                    "unitPrice": 该供应商的单价,
                    "allocatedQty": 分配数量（必须等于demandQty）,
                    "executionRate": 执行比例
                }}
            ],
            "totalCost": 总花费（计算公式：allocatedQty × unitPrice，必须>0）,
            "remark": "优先选择单个供应商能满足全部需求的"
        }}
    }}
}}
```

**计算规则（必须遵守）：**
- `allocatedQty` = `demandQty`（每个策略分配的数量等于需求数量）
- `totalCost` = `allocatedQty` × `unitPrice`（allocatedQty 乘以该供应商的单价）
- `totalCost` 必须大于 0，绝不能为 0！

## 输入
```
1、当前仓库补货计划：{replenishment_plans}
2、当前供货商列表（包含了执行比例、单价、库存的信息）：{suppliers}
3、供货商的执行比例阶梯：20%、50%、80%
4、注意初始的"demandQty": 需求数量,不可能为0，注意计算的准确性
```

## 输出
1、严格遵循json的格式进行输出，不要带其它任何信息
"""


def _extract_minimal_replenishment(plan: Dict[str, Any]) -> Dict[str, Any]:
    """提取补货计划关键字段（统一驼峰命名）"""
    return {
        "materialCode": plan.get('materialCode', ''),
        "materialDesc": plan.get('materialDesc', ''),
        "demandQty": plan.get('demandQty', 0),
        "unitPrice": plan.get('unitPrice', 0),
    }


def _extract_minimal_supplier(supplier: Dict[str, Any]) -> Dict[str, Any]:
    """提取供应商关键字段（统一驼峰命名）"""
    return {
        "supplierCode": supplier.get('supplierCode', ''),
        "supplierName": supplier.get('supplierName', ''),
        "materialCode": supplier.get('materialCode', ''),
        "executionRate": supplier.get('executionRate', 0),
        "totalAmount": supplier.get('totalAmount', 0),
        "executedAmount": supplier.get('executedAmount', 0),
        "unitPrice": supplier.get('unitPrice', 0),
        "deliveryCycleDays": supplier.get('deliveryCycleDays', 0),
    }


def _agent_calculate_supplier_remain(strategy_data: Dict[str, Any], suppliers: List[Dict[str, Any]]) -> Dict[str, Any]:
    """(Agent内)计算供应商剩余执行比例和余量

    计算逻辑:
    1. 用需求数量 * unit_price 得到本次匹配金额
    2. 新余量 = 总金额 * (1 - 原有执行率) - 本次匹配金额
    3. 新执行率 = (总金额 - 新余量) / 总金额

    Args:
        strategy_data: 策略结果数据
        suppliers: 协议商列表

    Returns:
        计算后的策略结果数据
    """
    # 创建供应商映射表，按供应商编码查找
    supplier_map = {}
    for s in suppliers:
        supplier_code = s.get('supplierCode', '')
        if supplier_code:
            supplier_map[supplier_code] = s

    suppliers_list = strategy_data.get('suppliers', [])
    for supp in suppliers_list:
        supplier_code = supp.get('supplierCode', '')
        original_supplier = supplier_map.get(supplier_code, {})

        if not original_supplier:
            continue

        # 获取原始数据
        total_amount = original_supplier.get('totalAmount', 0)  # 总金额 fd_amount_net
        unit_price = original_supplier.get('unitPrice', 0)      # 单价 fd_price_net
        original_exec_rate = original_supplier.get('executionRate', 0)  # 原有执行率
        allocated_qty = supp.get('allocatedQty', 0)

        if total_amount <= 0 or unit_price <= 0:
            # 如果数据不完整，使用原有余量
            supp['remainQty'] = original_supplier.get('remainQty', 0)
            supp['executionRate'] = original_exec_rate
            continue

        # 计算本次匹配金额
        match_amount = allocated_qty * unit_price

        # 计算新余量：原有剩余金额(总价*(1-执行率)) - 本次匹配金额
        original_remain_amount = total_amount * (1 - original_exec_rate)
        new_remain_amount = original_remain_amount - match_amount

        # 新余量金额转回为数量（为了简化，这里按单价反算）
        new_remain_qty = max(0, new_remain_amount / unit_price) if unit_price > 0 else 0

        # 计算新执行率：(总金额 - 新余量) / 总金额
        new_exec_rate = (total_amount - new_remain_amount) / total_amount if total_amount > 0 else 0
        new_exec_rate = max(0, min(1, new_exec_rate))  # 限制在 0-1 之间

        # 更新供应商数据
        supp['remainQty'] = round(new_remain_qty, 4)
        supp['executionRate'] = round(new_exec_rate, 6)

        logger.info(f"[Agent供应商余量计算] supplier={supplier_code}, "
                   f"总金额={total_amount}, 原执行率={original_exec_rate}, "
                   f"匹配数量={allocated_qty}, 匹配金额={match_amount}, "
                   f"新余量={new_remain_qty}, 新执行率={new_exec_rate}")

    strategy_data['suppliers'] = suppliers_list
    return strategy_data


def _calculate_fallback_result(plan: Dict[str, Any], suppliers: List[Dict[str, Any]]) -> Dict[str, Any]:
    """根据已有数据和策略计算兜底结果（当LLM调用失败或解析失败时使用）

    计算逻辑:
    1. 均衡策略(balanced): 按执行比例分配，优先选择比例最低的供应商
    2. 成本策略(cost): 按单价从低到高排序，选择单价最低的供应商
    3. 配送策略(delivery): 优先选择能满足全部需求的单个供应商

    Args:
        plan: 补货计划数据（包含 demandQty, materialCode 等）
        suppliers: 供应商列表（包含 executionRate, unitPrice, remainQty 等）

    Returns:
        包含三种策略计算结果的字典
    """
    demand_qty = plan.get('demandQty', 0)
    material_code = plan.get('materialCode', '')
    material_desc = plan.get('materialDesc', '')
    plan_id = plan.get('planId', '')
    warehouse_code = plan.get('warehouseCode', '')
    tech_spec_id = plan.get('techSpecId', '')

    valid_suppliers = [s for s in suppliers if s.get('remainQty', 0) > 0]

    if not valid_suppliers:
        empty_strategy = {'suppliers': [], 'totalCost': 0, 'remark': '无可用供应商库存'}
        return {
            'planId': plan_id,
            'materialCode': material_code,
            'materialDesc': material_desc,
            'demandQty': demand_qty,
            'warehouseCode': warehouse_code,
            'techSpecId': tech_spec_id,
            'strategies': {
                'balanced': empty_strategy,
                'cost': empty_strategy,
                'delivery': empty_strategy
            }
        }

    balanced_sorted = sorted(valid_suppliers, key=lambda x: x.get('executionRate', 0))
    balanced_supplier = balanced_sorted[0]
    balanced_qty = min(demand_qty, balanced_supplier.get('remainQty', 0))
    balanced_cost = balanced_qty * balanced_supplier.get('unitPrice', 0)
    balanced_result = {
        'suppliers': [{
            'supplierCode': balanced_supplier.get('supplierCode', ''),
            'supplierName': balanced_supplier.get('supplierName', ''),
            'unitPrice': balanced_supplier.get('unitPrice', 0),
            'allocatedQty': demand_qty,
            'cost': round(balanced_cost, 2),
            'executionRate': balanced_supplier.get('executionRate', 0)
        }],
        'totalCost': round(balanced_cost, 2),
        'remark': '按执行比例均衡分配'
    }

    cost_sorted = sorted(valid_suppliers, key=lambda x: x.get('unitPrice', 0))
    cost_supplier = cost_sorted[0]
    cost_qty = min(demand_qty, cost_supplier.get('remainQty', 0))
    cost_total = cost_qty * cost_supplier.get('unitPrice', 0)
    cost_result = {
        'suppliers': [{
            'supplierCode': cost_supplier.get('supplierCode', ''),
            'supplierName': cost_supplier.get('supplierName', ''),
            'unitPrice': cost_supplier.get('unitPrice', 0),
            'allocatedQty': demand_qty,
            'cost': round(cost_total, 2),
            'executionRate': cost_supplier.get('executionRate', 0)
        }],
        'totalCost': round(cost_total, 2),
        'remark': '优先选择单价最低的供应商'
    }

    delivery_supplier = None
    for s in valid_suppliers:
        if s.get('remainQty', 0) >= demand_qty:
            delivery_supplier = s
            break

    if not delivery_supplier:
        delivery_sorted = sorted(valid_suppliers, key=lambda x: x.get('remainQty', 0), reverse=True)
        delivery_supplier = delivery_sorted[0]

    delivery_qty = min(demand_qty, delivery_supplier.get('remainQty', 0))
    delivery_cost = delivery_qty * delivery_supplier.get('unitPrice', 0)
    delivery_result = {
        'suppliers': [{
            'supplierCode': delivery_supplier.get('supplierCode', ''),
            'supplierName': delivery_supplier.get('supplierName', ''),
            'unitPrice': delivery_supplier.get('unitPrice', 0),
            'allocatedQty': demand_qty,
            'cost': round(delivery_cost, 2),
            'executionRate': delivery_supplier.get('executionRate', 0)
        }],
        'totalCost': round(delivery_cost, 2),
        'remark': '优先选择单个供应商满足全部需求'
    }

    # 计算三种策略的供应商余量和执行率
    balanced_result = _agent_calculate_supplier_remain(balanced_result, suppliers)
    cost_result = _agent_calculate_supplier_remain(cost_result, suppliers)
    delivery_result = _agent_calculate_supplier_remain(delivery_result, suppliers)

    return {
        'planId': plan_id,
        'materialCode': material_code,
        'materialDesc': material_desc,
        'demandQty': demand_qty,
        'warehouseCode': warehouse_code,
        'techSpecId': tech_spec_id,
        'strategies': {
            'balanced': balanced_result,
            'cost': cost_result,
            'delivery': delivery_result
        }
    }


import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='[AGENT] %(message)s')


class SupplierMatchAgent:
    """供应商匹配智能体 - 负责补货供应商选择"""

    def __init__(self, db, llm_stream_func, llm_func):
        self.db = db
        self.llm_stream_func = llm_stream_func
        self.llm_func = llm_func

    async def match(self, replenishment_plans: List[Dict[str, Any]] = None,
                   suppliers: List[Dict[str, Any]] = None,
                   stream: bool = False) -> Dict[str, Any]:
        """执行供应商匹配"""
        logger.info(f"[SupplierMatchAgent] 进入match方法, replenishment_plans数量: {len(replenishment_plans) if replenishment_plans else 0}, suppliers数量: {len(suppliers) if suppliers else 0}, stream: {stream}")

        if not replenishment_plans:
            replenishment_plans = []
        if not suppliers:
            suppliers = []

        prompt = self._build_prompt(replenishment_plans, suppliers)
        logger.info(f"[SupplierMatchAgent] prompt构建完成, 长度: {len(prompt)} 字符")

        if stream:
            logger.info(f"[SupplierMatchAgent] 开始流式调用LLM")
            response_generator = self.llm_stream_func(prompt)
            return {"response_generator": response_generator}
        else:
            logger.info(f"[SupplierMatchAgent] 开始非流式调用LLM")
            response = await self.llm_func(prompt)
            logger.info(f"[SupplierMatchAgent] LLM调用完成, 响应长度: {len(response) if response else 0} 字符")
            return {"response": response}

    def fallback_match(self, plan: Dict[str, Any], suppliers: List[Dict[str, Any]]) -> Dict[str, Any]:
        """当LLM调用失败或解析失败时，使用本地计算返回兜底结果
        
        调用时机:
        1. LLM调用抛出异常
        2. parse_match_response 返回空列表
        
        Args:
            plan: 补货计划数据
            suppliers: 供应商列表
        
        Returns:
            根据已有数据和策略计算的兜底结果
        """
        logger.info(f"[SupplierMatchAgent] 使用本地兜底计算，planId={plan.get('planId')}, 供应商数量={len(suppliers)}")
        return _calculate_fallback_result(plan, suppliers)

    def _build_prompt(self, replenishment_plans: List[Dict[str, Any]], suppliers: List[Dict[str, Any]]) -> str:
        """构建供应商匹配prompt"""
        minimal_plans = [_extract_minimal_replenishment(p) for p in replenishment_plans]
        minimal_suppliers = [_extract_minimal_supplier(s) for s in suppliers]

        plans_text = json.dumps(minimal_plans, ensure_ascii=False, indent=2, default=_convert_for_json)
        suppliers_text = json.dumps(minimal_suppliers, ensure_ascii=False, indent=2, default=_convert_for_json)

        prompt = SUPPLIER_MATCH_PROMPT.format(
            replenishment_plans=plans_text,
            suppliers=suppliers_text
        )

        return prompt

    def parse_match_response(self, response: str) -> List[Dict[str, Any]]:
        """解析模型返回的匹配结果"""
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

        try:
            results = json.loads(response)
            if isinstance(results, list):
                return results
            elif isinstance(results, dict) and 'materialCode' in results:
                return [results]
        except json.JSONDecodeError:
            pass

        return []
