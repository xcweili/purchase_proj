# -*- coding: utf-8 -*-
"""供应商匹配智能体 - 流式版本"""
import json
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


def _extract_plan_for_stream(plan: Dict[str, Any]) -> Dict[str, Any]:
    """提取计划关键字段用于流式接口"""
    return {
        "计划ID": plan.get('planId', ''),
        "物料编码": plan.get('materialCode', ''),
        "物料描述": plan.get('materialDesc', ''),
        "需求数量": float(plan.get('demandQty', 0) or 0),
        "单价": float(plan.get('unitPrice', 0) or 0),
        "仓库编码": plan.get('warehouseCode', ''),
        "技术规范ID": plan.get('techSpecId', ''),
        "项目名称": plan.get('projectName', ''),
        "需求金额": float(plan.get('demandQty', 0) or 0) * float(plan.get('unitPrice', 0) or 0),
    }


def _extract_supplier_for_stream(supplier: Dict[str, Any]) -> Dict[str, Any]:
    """提取供应商关键字段用于流式接口"""
    return {
        "供应商编码": supplier.get('supplier_code', ''),
        "供应商名称": supplier.get('supplier_name', ''),
        "物料编码": supplier.get('material_code', ''),
        "执行比例(%)": float(supplier.get('execution_rate', 0) or 0),
        "剩余可用数量": float(supplier.get('remain_qty', 0) or 0),
        "剩余可用金额": float(supplier.get('remain_amount', 0) or 0),
        "协议单价": float(supplier.get('unit_price', 0) or 0),
        "供货周期(天)": supplier.get('delivery_cycle', ''),
    }


STREAM_SUPPLIER_MATCH_PROMPT = """你是一个专业的电力物料采购供应商匹配专家。我将提供补货需求计划和供应商协议数据，请你分析并给出最优的供应商选择方案。

## 任务说明
请分析以下补货需求计划，根据供应商的协议执行情况和可用库存，为每个需求选择最合适的供应商，并详细说明你的分析过程和理由。

## 输入数据

### 补货计划列表
{plans_json}

### 供应商协议数据
{suppliers_json}

### 匹配规则说明
1. **执行比例阶梯**：20%、50%、80%
2. **执行比例计算**：已执行金额 / 执行金额总额度
3. **阶梯规则**：优先选择执行比例未达到下一阶梯的供应商
4. **补货频率**：周
5. **供货周期**：15-45天

### 三种匹配策略
1. **均衡策略**：按执行比例阶梯分配，优先选择执行比例较低的供应商
2. **成本策略**：选择单价最低的供应商，追求总成本最小化
3. **配送策略**：优先选择能满足全部需求的单个供应商，简化配送流程

## 分析要求

请按照以下结构输出详细的分析报告：

1. **需求概览**：简要描述本次需要补货的物料和数量

2. **供应商分析**：
   - 各供应商的执行比例情况
   - 各供应商的可用库存和金额
   - 各供应商的单价对比

3. **匹配方案**：
   - 针对每个需求计划，分析各策略下的最佳供应商选择
   - 详细说明选择理由
   - 计算分配数量和金额

4. **方案对比**：
   - 三种策略的优缺点对比
   - 推荐策略建议

5. **结果汇总**：总结本次供应商匹配的总体情况

请用自然、清晰的语言进行分析，让用户能够理解你的决策过程。
"""


class SupplierMatchStreamAgent:
    """供应商匹配智能体 - 流式版本"""
    
    def __init__(self, llm_stream_func):
        self.llm_stream_func = llm_stream_func
    
    def _build_stream_prompt(self, plans: List[Dict[str, Any]], suppliers: List[Dict[str, Any]]) -> str:
        """构建流式接口的prompt"""
        
        # 构建prompt
        prompt = STREAM_SUPPLIER_MATCH_PROMPT.format(
            plans_json=json.dumps([_extract_plan_for_stream(p) for p in plans], ensure_ascii=False, indent=2),
            suppliers_json=json.dumps([_extract_supplier_for_stream(s) for s in suppliers], ensure_ascii=False, indent=2)
        )
        
        return prompt
    
    async def stream_analyze(self, plans: List[Dict[str, Any]], suppliers: List[Dict[str, Any]]):
        """流式分析供应商匹配"""
        prompt = self._build_stream_prompt(plans, suppliers)
        async for chunk in self.llm_stream_func(prompt, "你是一个专业的电力物料采购供应商匹配专家，擅长分析供应商协议数据并给出最优的供应商选择方案。"):
            yield chunk
