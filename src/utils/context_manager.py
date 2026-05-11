# -*- coding: utf-8 -*-
"""上下文管理器 - 处理超长prompt的代码沙盒执行"""
import json
import logging
from typing import Dict, Any
from .code_sandbox import (
    code_sandbox, 
    INVENTORY_ANALYSIS_SCRIPT, 
    ALLOCATION_ANALYSIS_SCRIPT, 
    SUPPLIER_MATCH_SCRIPT,
    batch_analyze_inventory,
    analyze_allocation,
    match_suppliers
)

logger = logging.getLogger(__name__)


class ContextManager:
    """上下文管理器 - 处理超长prompt的代码沙盒"""

    # Token估算：1 token ≈ 4 个中文字符
    TOKEN_LIMIT = 64000  # 64k token限制
    TOKEN_PER_CHAR = 0.25  # 每个字符约0.25 token
    SAFETY_MARGIN = 0.8  # 安全边际，使用80%的token

    def __init__(self, llm_func, llm_stream_func):
        self.llm_func = llm_func
        self.llm_stream_func = llm_stream_func
        self.code_sandbox = code_sandbox

    def estimate_tokens(self, text: str) -> int:
        """估算文本的token数量"""
        return int(len(text) * self.TOKEN_PER_CHAR)

    def is_too_long(self, text: str) -> bool:
        """判断文本是否超长"""
        return self.estimate_tokens(text) > self.TOKEN_LIMIT * self.SAFETY_MARGIN

    async def process_with_context_management(self,
                                            prompt: str,
                                            system_prompt: str = "",
                                            stream: bool = False,
                                            max_retries: int = 3,
                                            data_type: str = 'inventory',
                                            original_data: Dict[str, Any] = None) -> Any:
        """
        使用上下文管理处理超长prompt

        Args:
            prompt: 原始prompt
            system_prompt: 系统提示
            stream: 是否流式输出
            max_retries: 最大重试次数
            data_type: 数据类型 ('inventory', 'allocation', 'supplier')
            original_data: 原始数据，用于构建完整输出

        Returns:
            分析结果（流式返回生成器或直接返回结果）
        """
        if not self.is_too_long(prompt):
            # 正常处理，直接调用LLM
            if stream:
                return self.llm_stream_func(prompt, system_prompt)
            else:
                return await self.llm_func(prompt, system_prompt)

        # 超长prompt，使用代码沙盒模式
        if stream:
            return self._streaming_sandbox_execution(prompt, system_prompt, data_type, original_data)
        else:
            return await self._sandbox_execution(prompt, system_prompt, data_type, original_data)

    async def _sandbox_execution(self, prompt: str, system_prompt: str, data_type: str, original_data: Dict[str, Any]) -> dict:
        """
        代码沙盒执行模式：先执行代码计算，再让LLM总结

        步骤：
        1. 从prompt中提取数据
        2. 使用预定义算法执行计算（直接获取结构化数据）
        3. 让LLM基于计算摘要进行分析总结

        Returns:
            dict: {
                'llm_summary': LLM生成的分析报告,
                'structured_data': 代码计算的结构化数据（可直接入库）,
                'summary': 计算摘要统计
            }
        """
        # 第一步：提取数据
        data = self._extract_data_from_prompt(prompt)

        # 第二步：执行代码计算（使用直接函数调用，而非脚本执行）
        result = await self._execute_analysis_algorithm(data, data_type, original_data)

        # 第三步：构建分析prompt（只包含处理摘要，不包含完整原始数据）
        analysis_prompt = self._build_sandbox_analysis_prompt(prompt, result, original_data, data_type)
        llm_summary = await self.llm_func(analysis_prompt, system_prompt)

        # 返回结果：结构化数据直接来自代码计算，不需要LLM生成
        return {
            'llm_summary': llm_summary,
            'structured_data': result.get('results', []),
            'summary': result.get('summary', {})
        }

    async def _streaming_sandbox_execution(self, prompt: str, system_prompt: str, data_type: str, original_data: Dict[str, Any]):
        """
        流式代码沙盒执行模式

        步骤：
        1. 从prompt中提取数据
        2. 使用预定义算法执行计算（直接获取结构化数据）
        3. 输出计算结果摘要
        4. 输出结构化数据（用于入库）
        5. 让LLM基于计算摘要进行流式分析

        Yields:
            流式输出，包含计算摘要、结构化数据和LLM分析报告
        """
        # 第一步：提取数据
        data = self._extract_data_from_prompt(prompt)

        # 第二步：执行代码计算（使用直接函数调用）
        yield "\n🔢 [数据计算阶段] 正在使用代码沙盒进行数据计算...\n"
        yield "────────────────────────────────────────\n"

        result = await self._execute_analysis_algorithm(data, data_type, original_data)

        # 输出计算结果摘要
        summary = result.get('summary', {})
        structured_data = result.get('results', [])
        
        yield f"\n📊 计算完成！\n"
        yield f"  ├─ 处理记录数: {summary.get('total_items', summary.get('total_plans', 'N/A'))}\n"
        
        if 'emergency_count' in summary:
            yield f"  ├─ 紧急库存: {summary.get('emergency_count', 0)} 项\n"
            yield f"  ├─ 低库存: {summary.get('low_count', 0)} 项\n"
            yield f"  ├─ 正常库存: {summary.get('medium_count', 0)} 项\n"
            yield f"  ├─ 高库存: {summary.get('high_count', 0)} 项\n"
            yield f"  ├─ 库存充足: {summary.get('sufficient_count', 0)} 项\n"
        elif 'coverage_rate' in summary:
            yield f"  ├─ 覆盖率: {summary.get('coverage_rate', 0) * 100:.1f}%\n"
            yield f"  ├─ 总需求量: {summary.get('total_demand', 0)}\n"
            yield f"  ├─ 已调配量: {summary.get('total_allocated', 0)}\n"
            yield f"  ├─ 策略类型: {summary.get('strategy', 'time')}\n"
        elif 'matched_plans' in summary:
            yield f"  ├─ 已匹配: {summary.get('matched_plans', 0)} 项\n"
            yield f"  ├─ 部分匹配: {summary.get('partially_matched', 0)} 项\n"
            yield f"  ├─ 未匹配: {summary.get('no_match', 0)} 项\n"
            yield f"  ├─ 策略类型: {summary.get('strategy', 'balance')}\n"
        
        yield f"  └─ 计算状态: 成功\n\n"

        # 输出结构化数据（可直接入库）
        yield "📋 [结构化数据] 以下数据可直接入库：\n"
        yield "────────────────────────────────────────\n"
        yield "```json\n"
        yield json.dumps(structured_data, ensure_ascii=False, indent=2)
        yield "\n```\n\n"

        # 第三步：让LLM基于计算摘要进行分析
        yield "🤖 [分析总结阶段] 正在生成分析报告...\n"
        yield "────────────────────────────────────────\n"

        analysis_prompt = self._build_sandbox_analysis_prompt(prompt, result, original_data, data_type)
        async for chunk in self.llm_stream_func(analysis_prompt, system_prompt):
            yield chunk

    async def _execute_analysis_algorithm(self, data: Dict[str, Any], data_type: str, original_data: Dict[str, Any]) -> Any:
        """
        执行分析算法（直接调用Python函数，而非脚本执行）
        
        这是核心改进：不再通过沙盒执行字符串脚本，而是直接调用已实现的算法函数
        """
        try:
            if data_type == 'inventory':
                # 库存分析：使用原始数据进行分析
                inventory_data = original_data if original_data else data
                if isinstance(inventory_data, dict):
                    inventory_data = inventory_data.get('data', inventory_data.get('combo_data', []))
                if not isinstance(inventory_data, list):
                    inventory_data = [inventory_data]
                
                # 调用库存分析算法
                result = batch_analyze_inventory(inventory_data)
                
            elif data_type == 'allocation':
                # 调配分析
                plans_data = data.get('plans', [])
                stocks_data = data.get('stocks', [])
                strategy = data.get('strategy', 'time')
                
                # 如果original_data包含更完整的数据
                if original_data:
                    plans_data = original_data.get('plans', plans_data)
                    stocks_data = original_data.get('stocks', stocks_data)
                    strategy = original_data.get('strategy', strategy)
                
                # 调用调配算法
                result = analyze_allocation(plans_data, stocks_data, strategy)
                
            elif data_type == 'supplier':
                # 供应商匹配
                plans_data = data.get('plans', [])
                suppliers_data = data.get('suppliers', [])
                strategy = data.get('strategy', 'balance')
                
                # 如果original_data包含更完整的数据
                if original_data:
                    plans_data = original_data.get('plans', plans_data)
                    suppliers_data = original_data.get('suppliers', suppliers_data)
                    strategy = original_data.get('strategy', strategy)
                
                # 调用供应商匹配算法
                result = match_suppliers(plans_data, suppliers_data, strategy)
                
            else:
                # 默认返回原始数据
                result = {'summary': {}, 'results': data}

            return result

        except Exception as e:
            logger.error(f"执行分析算法失败: {str(e)}")
            return {'summary': {'error': str(e)}, 'results': data}

    def _extract_data_from_prompt(self, prompt: str) -> Dict[str, Any]:
        """从prompt中提取JSON数据部分"""
        # 尝试找到JSON数据块
        start_idx = prompt.find('{')
        end_idx = prompt.rfind('}')

        if start_idx != -1 and end_idx != -1 and start_idx < end_idx:
            try:
                json_str = prompt[start_idx:end_idx+1]
                return json.loads(json_str)
            except:
                pass

        # 如果找不到JSON，尝试找列表
        start_idx = prompt.find('[')
        end_idx = prompt.rfind(']')

        if start_idx != -1 and end_idx != -1 and start_idx < end_idx:
            try:
                json_str = prompt[start_idx:end_idx+1]
                return json.loads(json_str)
            except:
                pass

        return {'raw_prompt': prompt}

    def _build_sandbox_analysis_prompt(self, original_prompt: str, calculation_result: Any, original_data: Dict[str, Any], data_type: str = 'inventory') -> str:
        """构建基于代码计算结果的分析prompt（不包含原始数据，只传递处理摘要）"""
        # 提取分析要求
        analysis_requirements = self._extract_analysis_requirements(original_prompt)

        # 获取智能体信息摘要
        agent_summary = self._get_agent_summary(data_type, calculation_result)

        # 构建完整的prompt - 只传递处理摘要，不传递完整原始数据
        full_prompt = "## 智能体处理摘要\n\n"
        full_prompt += agent_summary
        full_prompt += "\n\n## 分析要求\n"
        full_prompt += analysis_requirements
        full_prompt += "\n\n请基于以上处理摘要，进行深度分析并生成分析报告，包括：\n"
        full_prompt += "- 数据摘要和关键指标\n"
        full_prompt += "- 问题识别和风险评估\n"
        full_prompt += "- 具体建议和优化方案\n"
        full_prompt += "- 下一步行动计划\n"

        return full_prompt

    def _get_agent_summary(self, data_type: str, calculation_result: Any) -> str:
        """生成智能体处理摘要，不包含原始数据详情"""
        summary = calculation_result.get('summary', {})
        
        agent_info = {
            'inventory': {
                'name': '库存分析智能体',
                'purpose': '基于历史出库数据计算水位线，判断库存状态，给出补货建议',
                'algorithm': '统计分析算法（均值、中位数、标准差计算）',
                'basis': '基于近12个月的出库历史数据，采用中位数法计算水位线：应急线=中位数×0.5，补库线=中位数×2，高位线=中位数×4',
                'fields': '仓库编码、物料编码、当前库存、在途库存、水位线、库存状态、建议操作'
            },
            'allocation': {
                'name': '库存调配智能体',
                'purpose': '多仓库间物料最优调配，满足需求同时最小化成本或时间',
                'algorithm': '运输问题贪心算法，支持时间优先、成本优先、均衡策略三种策略',
                'basis': '基于仓库库存数据和计划需求数据，采用贪心算法进行最优调配',
                'fields': '计划ID、物料编码、仓库编码、调配数量、来源仓库、目标仓库、状态'
            },
            'supplier': {
                'name': '供应商匹配智能体',
                'purpose': '多对多供应商匹配，优化执行比例，实现均衡分配',
                'algorithm': '多目标评分模型（价格40%、交付30%、质量30%）+ 均衡分配策略',
                'basis': '基于供应商历史表现评分，实现最优匹配和均衡分配，最多选择3个供应商',
                'fields': '计划ID、物料编码、供应商编码、匹配分数、单价、交期、执行比例'
            }
        }
        
        info = agent_info.get(data_type, agent_info['inventory'])
        
        # 构建详细摘要
        result = f"### 智能体信息\n\n"
        result += f"- **智能体名称**: {info['name']}\n"
        result += f"- **核心目的**: {info['purpose']}\n"
        result += f"- **使用算法**: {info['algorithm']}\n"
        result += f"- **计算依据**: {info['basis']}\n"
        result += f"- **输出字段**: {info['fields']}\n\n"
        
        # 添加处理统计
        result += "### 处理统计\n\n"
        if 'total_items' in summary:
            result += f"- 处理记录数: {summary.get('total_items', 0)} 条\n"
        elif 'total_plans' in summary:
            result += f"- 处理计划数: {summary.get('total_plans', 0)} 条\n"
        
        # 根据数据类型添加特定统计
        if data_type == 'inventory':
            result += f"- 紧急库存: {summary.get('emergency_count', 0)} 项\n"
            result += f"- 低库存: {summary.get('low_count', 0)} 项\n"
            result += f"- 正常库存: {summary.get('medium_count', 0)} 项\n"
            result += f"- 高库存: {summary.get('high_count', 0)} 项\n"
            result += f"- 库存充足: {summary.get('sufficient_count', 0)} 项\n"
            
        elif data_type == 'allocation':
            result += f"- 总需求量: {summary.get('total_demand', 0)}\n"
            result += f"- 已调配量: {summary.get('total_allocated', 0)}\n"
            result += f"- 覆盖率: {summary.get('coverage_rate', 0) * 100:.1f}%\n"
            result += f"- 策略类型: {summary.get('strategy', 'time')}\n"
            
        elif data_type == 'supplier':
            result += f"- 已匹配: {summary.get('matched_plans', 0)} 项\n"
            result += f"- 部分匹配: {summary.get('partially_matched', 0)} 项\n"
            result += f"- 未匹配: {summary.get('no_match', 0)} 项\n"
            result += f"- 策略类型: {summary.get('strategy', 'balance')}\n"
        
        # 添加计算状态
        result += f"\n### 计算状态\n\n"
        result += f"- 计算结果: 成功\n"
        if 'error' in summary:
            result += f"- 错误信息: {summary.get('error')}\n"
        
        return result

    def _get_json_template(self, data_type: str) -> str:
        """根据数据类型返回对应的JSON模板（与原智能体期望格式一致）"""
        if data_type == 'inventory':
            # 库存分析智能体期望的格式 - 与原智能体完全兼容
            return '''{
  "emergencyLine": 0,
  "replenishLevel": 0,
  "highLevel": 0,
  "currentStock": 0,
  "inTransitQty": 0,
  "stockStatus": "紧急/低/中/高",
  "suggestedAction": "补货/正常/调整",
  "inventoryLevel": "A/B/C/D/E",
  "warehouseName": "仓库名称",
  "materialDesc": "物料描述",
  "统计数据": {
    "avg_outbound": 0,
    "total_orders": 0,
    "total_amount": 0
  }
}'''
        elif data_type == 'allocation':
            # 调配智能体期望的格式
            return '''{
  "allocationPlan": [
    {
      "planId": "计划ID",
      "materialCode": "物料编码",
      "warehouseCode": "仓库编码",
      "allocationQty": 0,
      "sourceWarehouse": "来源仓库",
      "targetWarehouse": "目标仓库",
      "status": "待执行/执行中/已完成"
    }
  ],
  "summary": {
    "totalPlans": 0,
    "totalQty": 0,
    "successCount": 0,
    "failCount": 0
  },
  "suggestions": ["建议1", "建议2"]
}'''
        elif data_type == 'supplier':
            # 供应商匹配智能体期望的格式
            return '''{
  "supplierMatches": [
    {
      "planId": "计划ID",
      "materialCode": "物料编码",
      "supplierCode": "供应商编码",
      "supplierName": "供应商名称",
      "matchScore": 0,
      "unitPrice": 0,
      "deliveryDays": 0,
      "executionRatio": 0
    }
  ],
  "summary": {
    "totalPlans": 0,
    "matchedPlans": 0,
    "unmatchedPlans": 0
  },
  "strategy": "均衡/成本/配送",
  "suggestions": ["建议1", "建议2"]
}'''
        else:
            # 默认通用格式
            return '''{
  "summary": {
    "total_records": 0,
    "key_metrics": {}
  },
  "analysis": {
    "status": "状态",
    "risk_level": "风险等级",
    "issues": [],
    "recommendations": []
  },
  "actions": []
}'''

    def _extract_analysis_requirements(self, prompt: str) -> str:
        """从prompt中提取分析要求部分"""
        start_markers = ["## 分析参数说明", "## 水位线分析方法", "## 分析要求", "请分析", "请给出", "基于以上"]
        start_idx = -1

        for marker in start_markers:
            idx = prompt.find(marker)
            if idx != -1:
                start_idx = idx
                break

        if start_idx == -1:
            return prompt[-3000:]

        return prompt[start_idx:]


# 全局上下文管理器实例
_context_manager = None

def get_context_manager(llm_func=None, llm_stream_func=None):
    """获取全局上下文管理器实例"""
    global _context_manager
    if _context_manager is None and llm_func and llm_stream_func:
        _context_manager = ContextManager(llm_func, llm_stream_func)
    return _context_manager

def init_context_manager(llm_func, llm_stream_func):
    """初始化全局上下文管理器"""
    global _context_manager
    _context_manager = ContextManager(llm_func, llm_stream_func)
    return _context_manager
