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
    TOKEN_LIMIT = 64  # 64k token限制
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
        yield "🔢 **[数据计算阶段] 正在使用代码沙盒进行数据计算...**\n"
        result = await self._execute_analysis_algorithm(data, data_type, original_data)

        # 输出计算结果摘要（一次性拼接，让 \\n 保留在字符串中间不被 strip 吃掉）
        summary = result.get('summary', {})
        structured_data = result.get('results', [])

        summary_lines = []
        summary_lines.append("")
        summary_lines.append("📊 **计算完成**")
        summary_lines.append(f"- 处理记录数: {summary.get('total_items', summary.get('total_plans', 'N/A'))}")
        
        if 'emergency_count' in summary:
            summary_lines.append(f"- 紧急库存: {summary.get('emergency_count', 0)} 项")
            summary_lines.append(f"- 低库存: {summary.get('low_count', 0)} 项")
            summary_lines.append(f"- 正常库存: {summary.get('medium_count', 0)} 项")
            summary_lines.append(f"- 高库存: {summary.get('high_count', 0)} 项")
            summary_lines.append(f"- 库存充足: {summary.get('sufficient_count', 0)} 项")
        elif 'coverage_rate' in summary:
            summary_lines.append(f"- 覆盖率: {summary.get('coverage_rate', 0) * 100:.1f}%")
            summary_lines.append(f"- 总需求量: {summary.get('total_demand', 0)}")
            summary_lines.append(f"- 已调配量: {summary.get('total_allocated', 0)}")
            summary_lines.append(f"- 策略类型: {summary.get('strategy', 'time')}")
        elif 'matched_plans' in summary:
            summary_lines.append(f"- 已匹配: {summary.get('matched_plans', 0)} 项")
            summary_lines.append(f"- 部分匹配: {summary.get('partially_matched', 0)} 项")
            summary_lines.append(f"- 未匹配: {summary.get('no_match', 0)} 项")
            summary_lines.append(f"- 策略类型: {summary.get('strategy', 'balance')}")
        
        summary_lines.append(f"- 计算状态: 成功")
        summary_lines.append("")

        yield "\n".join(summary_lines)

        # 第三步：让LLM基于计算摘要进行分析
        yield "────────────────────────────────────────\n"
        yield "🤖 **[分析总结阶段] 正在生成分析报告...**\n"

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
                
                # 归一化字段名：将服务层的中文/下划线字段名映射为 sandbox 函数期望的英文驼峰字段名
                normalized_data = []
                for item in inventory_data:
                    normalized_item = dict(item)
                    # '历史出库' → 'outbound_history'
                    if '历史出库' in normalized_item and 'outbound_history' not in normalized_item:
                        normalized_item['outbound_history'] = normalized_item.pop('历史出库')
                    normalized_data.append(normalized_item)
                
                # 调用库存分析算法
                result = batch_analyze_inventory(normalized_data)
                
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
                
                # 归一化计划字段名：下划线 → 驼峰（sandbox 函数期望的格式）
                normalized_plans = []
                for p in plans_data:
                    normalized_plans.append({
                        'planId': p.get('planId') or p.get('plan_id', ''),
                        'id': p.get('plan_id', ''),
                        'materialCode': p.get('materialCode') or p.get('material_code', ''),
                        'demandQty': p.get('demandQty') or p.get('demand_qty', 0),
                        'targetWarehouse': p.get('targetWarehouse') or p.get('target_warehouse', ''),
                        'materialDesc': p.get('materialDesc') or p.get('material_desc', ''),
                    })
                
                # 调用调配算法（使用归一化后的数据）
                result = analyze_allocation(normalized_plans, stocks_data, strategy)
                
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
                
                # 归一化计划字段名
                normalized_plans = []
                for p in plans_data:
                    normalized_plans.append({
                        'planId': p.get('planId') or p.get('plan_id', ''),
                        'materialCode': p.get('materialCode') or p.get('material_code', ''),
                        'demandQty': p.get('demandQty') or p.get('demand_qty', 0),
                        'materialDesc': p.get('materialDesc') or p.get('material_desc', ''),
                    })
                
                # 归一化供应商字段名：实际字段 → sandbox 函数期望的字段
                normalized_suppliers = []
                for s in suppliers_data:
                    normalized_suppliers.append({
                        'material_code': s.get('material_code', ''),
                        'supplier_code': s.get('supplierCode') or s.get('supplier_code', ''),
                        'supplier_name': s.get('supplierName') or s.get('supplier_name', ''),
                        'price': float(s.get('unitPrice', 0) or 0),
                        'available_qty': float(s.get('remainQty', 0) or 0),
                        'delivery_days': int(s.get('deliveryDays', 30) or 30),
                        'quality_score': float(s.get('qualityScore', 80) or 80),
                    })
                
                # 调用供应商匹配算法（使用归一化后的数据）
                result = match_suppliers(normalized_plans, normalized_suppliers, strategy)
                
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
        analysis_requirements = self._extract_analysis_requirements(original_prompt)
        agent_summary = self._get_agent_summary(data_type, calculation_result, original_data)

        full_prompt = "## 智能体处理摘要\n\n"
        full_prompt += agent_summary
        full_prompt += "\n\n## 分析要求\n"
        full_prompt += analysis_requirements
        full_prompt += "\n\n请严格基于以上提供的处理摘要中的数据和统计结果，撰写一份详尽的深度分析报告，报告结构如下：\n\n"
        full_prompt += "### 1. 总体概况\n"
        full_prompt += "用一段话高屋建瓴地概括本次分析的整体情况，说明分析背景、范围，并提炼最核心的发现。\n\n"
        full_prompt += "### 2. 关键数据解读\n"
        full_prompt += "对本次分析的各项关键指标进行详细解读，阐释每个指标的含义、数值分布情况，并结合业务场景说明其反映的实际问题。可以从以下维度展开：\n"
        full_prompt += "- 整体数据分布特征（均值、极值、集中趋势等）\n"
        full_prompt += "- 不同类别间的数据对比分析\n"
        full_prompt += "- 数据异常点识别与成因分析\n"
        full_prompt += "- 数据的动态变化趋势（如有历史数据可对比）\n"
        full_prompt += "- 指标间的关联性分析\n\n"
        full_prompt += "### 3. 明细数据及建议\n"
        full_prompt += "用表格列出各条处理结果的详细数据和对应建议，表格格式参考（列名根据实际数据字段调整）：\n"
        full_prompt += "| 序号 | 仓库 | 物料 | 关键指标 | 当前状态 | 建议操作 |\n"
        full_prompt += "|------|------|------|----------|----------|----------|\n"
        full_prompt += "表格中每行对应一条处理记录，关键指标根据数据类型展示水位线/覆盖率/评分等核心数值。\n\n"
        full_prompt += "### 4. 重点关注与风险提示\n"
        full_prompt += "详细标识出存在风险或需要紧急处理的记录，逐条说明风险等级、风险原因以及不及时处理的潜在后果。同时给出风险优先级排序，帮助决策者快速抓住重点。\n\n"
        full_prompt += "### 5. 深层问题诊断\n"
        full_prompt += "透过数据表象，深入分析可能存在的深层次问题，例如：\n"
        full_prompt += "- 库存结构是否合理\n"
        full_prompt += "- 调配方案是否存在效率瓶颈\n"
        full_prompt += "- 供应链各环节是否存在衔接不畅\n"
        full_prompt += "- 是否存在系统性风险或周期性波动规律\n\n"
        full_prompt += "### 6. 优化建议与行动计划\n"
        full_prompt += "将建议分为三个层级：\n"
        full_prompt += "- **立即执行**：需要马上处理的事项（紧急补货、紧急调配等）\n"
        full_prompt += "- **短期优化**：1-2周内可执行的改进措施\n"
        full_prompt += "- **长期策略**：需要系统性和制度性优化的方向\n"
        full_prompt += "每个建议应说明预期效果和可量化的改善目标。\n\n"
        full_prompt += "### 7. 总结\n"
        full_prompt += "用精炼的语言再次概括本次分析的核心结论和最重要的行动建议，强化报告的可执行性。\n\n"
        full_prompt += "【注意】\n"
        full_prompt += "- 请直接输出分析报告，不要输出原始数据或字段映射说明\n"
        full_prompt += "- 尽可能展开分析，覆盖多个维度和角度\n"
        full_prompt += "- 语言风格要保持专业性和逻辑性，确保读者能快速理解并采取行动"

        return full_prompt

    def _get_agent_summary(self, data_type: str, calculation_result: Any, original_data: Dict[str, Any] = None) -> str:
        """生成智能体处理摘要，包含字段映射和样例数据"""
        summary = calculation_result.get('summary', {})
        results = None

        # 定义各智能体的元信息
        agent_meta = {
            'inventory': {
                'name': '库存分析智能体',
                'purpose': '基于仓库物料的历史出库数据，智能计算库存水位线（应急线、补库线、高位线），评估库存健康状态，给出补货建议',
                'data_source': 'mt_historical_outbound（历史出库表）、w_stock_info_0808（库存表）',
                'field_mapping': [
                    ('warehouse_code', '仓库编码', '仓库的唯一标识'),
                    ('material_code', '物料编码', '物料的唯一标识'),
                    ('tech_id', '技术规范ID', '物料的技术规格标识'),
                    ('material_desc', '物料描述', '物料的名称/规格描述'),
                    ('current_stock', '当前库存', '仓库中该物料的实时库存数量'),
                    ('in_transit_stock', '在途库存', '已采购未到库的数量'),
                    ('available_stock', '实际可用库存', '当前库存+在途库存'),
                    ('历史出库', '历史出库数据', '过去N个月的月度出库记录，含月份和出库数量'),
                ],
                'output_fields': [
                    ('emergency_line', '应急线', '库存最低安全线，低于此值必须紧急补货'),
                    ('replenish_line', '补库线', '触发补货的水位线，低于此值应启动补库'),
                    ('high_line', '高位线', '库存上限，高于此值无需补库可考虑利库'),
                    ('stock_status', '库存状态', '紧急/低/中/高，表示当前库存健康程度'),
                    ('suggested_action', '建议操作', '立即紧急补货/立即补货/建议补货/正常无需补货'),
                    ('recommended_qty', '建议补货数量', '建议本次补货的数量（补库线-可用库存）'),
                ],
                'result_key': 'results',
                'sample_fields': ['warehouse_code', 'material_code', 'material_desc', 'current_stock', 'in_transit_stock', 'available_stock', 'emergency_line', 'replenish_line', 'high_line', 'stock_status'],
                'sample_labels': ['仓库编码', '物料编码', '物料描述', '当前库存', '在途库存', '可用库存', '应急线', '补库线', '高位线', '库存状态'],
            },
            'allocation': {
                'name': '库存调配智能体',
                'purpose': '多仓库间物料最优调配，基于需求计划和库存数据，为每个计划匹配最优仓库，综合考虑距离、成本等因素',
                'data_source': 'mt_stock_use_list_plan_two（需求计划表）、w_stock_info_0808（库存表）',
                'field_mapping': [
                    ('plan_id', '计划ID', '需求计划的唯一标识'),
                    ('material_code', '物料编码', '物料唯一标识'),
                    ('material_desc', '物料描述', '物料名称/规格'),
                    ('demand_qty', '需求数量', '本次计划的需求量'),
                    ('target_warehouse', '目标仓库', '需求方指定的目标仓库'),
                    ('matching_stocks', '可匹配库存', '该物料在备选仓库中的库存列表，含仓库编码、库存数量、距离'),
                ],
                'output_fields': [
                    ('source_location', '来源仓库', '实际调配出货的仓库编码'),
                    ('allocate_qty', '调配数量', '从该仓库调出的数量'),
                    ('distance', '距离(km)', '来源仓库到目标仓库的距离'),
                    ('total_cost', '运输成本', '本次调配的运输成本'),
                    ('status', '匹配状态', '完全匹配/部分匹配/未满足'),
                ],
                'result_key': 'allocation_results',
                'sample_fields': ['plan_id', 'material_code', 'source_location', 'destination', 'allocate_qty', 'distance'],
                'sample_labels': ['计划ID', '物料编码', '来源仓库', '目标仓库', '调配数量', '距离(km)'],
            },
            'supplier': {
                'name': '供应商匹配智能体',
                'purpose': '为补货计划智能匹配最优供应商，基于协议价格、执行比例、剩余可用数量等因素，提供均衡、成本、配送三种策略方案',
                'data_source': 'mt_replenishment_plan（补货计划表）、mt_framework_agreement（框架协议表）',
                'field_mapping': [
                    ('plan_id', '计划ID', '补货计划的唯一标识'),
                    ('material_code', '物料编码', '物料唯一标识'),
                    ('material_desc', '物料描述', '物料名称/规格'),
                    ('demand_qty', '需求数量', '本次补货的需求量'),
                    ('company', '所属单位', '计划所属的项目单位'),
                ],
                'supplier_fields': [
                    ('supplierCode', '供应商编码', '供应商的唯一标识'),
                    ('supplierName', '供应商名称', '供应商名称'),
                    ('executionRate', '执行比例(%)', '该供应商已执行的协议比例'),
                    ('remainQty', '剩余可用数量', '该供应商剩余可用的协议数量'),
                    ('unitPrice', '协议单价', '该供应商的协议单价'),
                ],
                'output_fields': [
                    ('supplier_name', '供应商名称', '匹配的供应商名称'),
                    ('price', '协议单价', '该供应商的协议单价'),
                    ('score', '匹配评分', '综合评分（0-1），越高越优'),
                    ('allocation_qty', '分配数量', '分配给该供应商的数量'),
                    ('allocation_ratio', '分配比例(%)', '分配给该供应商的比例'),
                ],
                'result_key': 'match_results',
                'sample_fields': ['plan_id', 'material_code', 'demand_qty', 'suppliers', 'status'],
                'sample_labels': ['计划ID', '物料编码', '需求数量', '匹配供应商列表', '状态'],
            },
        }

        info = agent_meta.get(data_type, agent_meta['inventory'])
        result_key = info.get('result_key', 'results')

        # 从 calculation_result 中获取 results 列表
        if result_key == 'allocation_results':
            results = calculation_result.get(result_key, [])
        elif result_key == 'match_results':
            results = calculation_result.get(result_key, [])
        else:
            results = calculation_result.get(result_key, [])

        # === 第一部分：智能体身份 ===
        text = f"### 1. 智能体身份\n\n"
        text += f"- **名称**: {info['name']}\n"
        text += f"- **核心目的**: {info['purpose']}\n"
        text += f"- **数据来源**: {info['data_source']}\n\n"

        # === 第二部分：数据处理概况 ===
        text += "### 2. 数据处理概况\n\n"
        text += f"本次分析共处理 **{summary.get('total_items', summary.get('total_plans', 0))}** 条记录。\n\n"

        if data_type == 'inventory':
            text += "| 指标 | 数值 |\n"
            text += "|------|------|\n"
            text += f"| 总处理组合数 | {summary.get('total_items', 0)} |\n"
            text += f"| 当前库存总量 | {summary.get('total_current_stock', 0):.1f} |\n"
            text += f"| 在途库存总量 | {summary.get('total_in_transit', 0):.1f} |\n"
            text += f"| 可用库存总量 | {summary.get('total_available', 0):.1f} |\n"
            text += f"| 建议补货总量 | {summary.get('total_recommended_qty', 0):.1f} |\n"
            text += f"| 🔴 紧急库存 | {summary.get('emergency_count', 0)} 项 |\n"
            text += f"| 🟠 低库存 | {summary.get('low_count', 0)} 项 |\n"
            text += f"| 🟡 正常库存 | {summary.get('medium_count', 0)} 项 |\n"
            text += f"| 🟢 高库存 | {summary.get('high_count', 0)} 项 |\n\n"
        elif data_type == 'allocation':
            text += "| 指标 | 数值 |\n"
            text += "|------|------|\n"
            text += f"| 总计划数 | {summary.get('total_plans', 0)} |\n"
            text += f"| 物料种类数 | {summary.get('total_materials', 0)} |\n"
            text += f"| 总需求量 | {summary.get('total_demand', 0):.1f} |\n"
            text += f"| 已调配量 | {summary.get('total_allocated', 0):.1f} |\n"
            text += f"| 覆盖率 | {summary.get('coverage_rate', 0)*100:.1f}% |\n"
            text += f"| 总运输成本 | {summary.get('total_cost', 0):.2f} |\n"
            match_summary = calculation_result.get('match_summary', {})
            text += f"| ✅ 完全匹配 | {match_summary.get('fully_matched', 0)} 个 |\n"
            text += f"| ⚠️ 部分匹配 | {match_summary.get('partially_matched', 0)} 个 |\n"
            text += f"| ❌ 无匹配 | {match_summary.get('no_match', 0)} 个 |\n"
            text += f"| 策略类型 | {summary.get('strategy', 'time')} |\n\n"
        elif data_type == 'supplier':
            text += "| 指标 | 数值 |\n"
            text += "|------|------|\n"
            text += f"| 总计划数 | {summary.get('total_plans', 0)} |\n"
            text += f"| ✅ 已匹配 | {summary.get('matched_plans', 0)} 个 |\n"
            text += f"| ⚠️ 部分匹配 | {summary.get('partially_matched', 0)} 个 |\n"
            text += f"| ❌ 无匹配 | {summary.get('no_match', 0)} 个 |\n"
            text += f"| 总需求量 | {summary.get('total_demand', 0):.1f} |\n"
            text += f"| 已分配量 | {summary.get('total_allocated', 0):.1f} |\n"
            text += f"| 覆盖率 | {summary.get('coverage_rate', 0)*100:.1f}% |\n"
            text += f"| 平均供应商评分 | {summary.get('avg_supplier_score', 0):.3f} |\n"
            text += f"| 策略类型 | {summary.get('strategy', 'balance')} |\n\n"

        # === 第三部分：字段映射说明 ===
        text += "### 3. 字段说明\n\n"
        text += "#### 3.1 输入字段（从原始数据提取）\n\n"
        text += "| 字段名 | 中文含义 | 说明 |\n"
        text += "|--------|----------|------|\n"
        for field_name, label, desc in info.get('field_mapping', []):
            text += f"| `{field_name}` | {label} | {desc} |\n"

        if data_type == 'supplier':
            text += "\n#### 供应商数据字段\n\n"
            text += "| 字段名 | 中文含义 | 说明 |\n"
            text += "|--------|----------|------|\n"
            for field_name, label, desc in info.get('supplier_fields', []):
                text += f"| `{field_name}` | {label} | {desc} |\n"

        text += "\n#### 3.2 输出字段（算法计算结果）\n\n"
        text += "| 字段名 | 中文含义 | 说明 |\n"
        text += "|--------|----------|------|\n"
        for field_name, label, desc in info.get('output_fields', []):
            text += f"| `{field_name}` | {label} | {desc} |\n"

        # === 第四部分：样例数据 ===
        text += "\n### 4. 数据样例\n\n"
        text += f"以下是处理结果中的前 {min(3, len(results))} 条数据样例（共 {len(results)} 条）：\n\n"

        if data_type == 'inventory':
            for i, item in enumerate(results[:3], 1):
                text += f"#### 样例 {i}：{item.get('warehouse_code', '')} × {item.get('material_code', '')}\n\n"
                text += "| 字段 | 值 |\n"
                text += "|------|------|\n"
                text += f"| 仓库编码 | {item.get('warehouse_code', '')} |\n"
                text += f"| 仓库名称 | {item.get('warehouse_name', '')} |\n"
                text += f"| 物料编码 | {item.get('material_code', '')} |\n"
                text += f"| 技术规范ID | {item.get('tech_id', '')} |\n"
                text += f"| 物料描述 | {item.get('material_desc', '')} |\n"
                text += f"| 当前库存 | {item.get('current_stock', 0)} |\n"
                text += f"| 在途库存 | {item.get('in_transit_stock', 0)} |\n"
                text += f"| 可用库存 | {item.get('available_stock', 0)} |\n"
                text += f"| 🔴 应急线 | {item.get('emergency_line', 0):.2f} |\n"
                text += f"| 🟠 补库线 | {item.get('replenish_line', 0):.2f} |\n"
                text += f"| 🟢 高位线 | {item.get('high_line', 0):.2f} |\n"
                text += f"| 库存状态 | **{item.get('stock_status', '')}** |\n"
                text += f"| 建议操作 | {item.get('suggested_action', '')} |\n"
                text += f"| 建议补货数量 | {item.get('recommended_qty', 0):.2f} |\n\n"

                stats = item.get('statistics', {})
                text += "历史消耗统计：\n\n"
                text += f"- 月均出库: {stats.get('avg_outbound', 0):.2f}\n"
                text += f"- 中位数出库: {stats.get('median_outbound', 0):.2f}\n"
                text += f"- 标准差: {stats.get('std_outbound', 0):.2f}\n"
                text += f"- 消耗趋势: {stats.get('trend', '平稳')}\n"
                text += f"- 波动特征: {stats.get('seasonality', '稳定')}\n\n"

        elif data_type == 'allocation':
            # 按 plan_id 分组展示
            plan_groups = {}
            for item in results:
                pid = item.get('plan_id', '')
                if pid not in plan_groups:
                    plan_groups[pid] = []
                plan_groups[pid].append(item)

            for i, (pid, items) in enumerate(list(plan_groups.items())[:3], 1):
                text += f"#### 样例 {i}：计划 {pid}\n\n"
                successful = [it for it in items if it.get('source_location')]
                unmet = [it for it in items if it.get('status') == '未满足']

                if successful:
                    text += "**调配方案：**\n\n"
                    text += "| 来源仓库 | 目标仓库 | 调配数量 | 距离(km) | 运输成本 |\n"
                    text += "|----------|----------|----------|----------|----------|\n"
                    for it in successful[:3]:
                        text += f"| {it.get('source_location', '')} | {it.get('destination', '')} | {it.get('allocate_qty', 0):.1f} | {it.get('distance', 0):.1f} | {it.get('total_cost', 0):.2f} |\n"

                if unmet:
                    text += f"\n**未满足需求：** {unmet[0].get('unmet_demand', 0):.1f} 个单位\n"
                text += "\n"

        elif data_type == 'supplier':
            for i, item in enumerate(results[:3], 1):
                text += f"#### 样例 {i}：计划 {item.get('plan_id', '')}（物料 {item.get('material_code', '')}）\n\n"
                text += f"- 需求数量: {item.get('demand_qty', 0)}\n"
                text += f"- 匹配状态: **{item.get('status', '')}**\n"
                if item.get('unmet_qty', 0) > 0:
                    text += f"- 未满足数量: {item.get('unmet_qty', 0)}\n"

                suppliers = item.get('suppliers', [])
                if suppliers:
                    text += "\n**匹配供应商：**\n\n"
                    text += "| 供应商名称 | 协议单价 | 匹配评分 | 分配数量 | 分配比例 |\n"
                    text += "|------------|----------|----------|----------|----------|\n"
                    for s in suppliers[:3]:
                        text += f"| {s.get('supplier_name', '')} | {s.get('price', 0):.2f} | {s.get('score', 0):.3f} | {s.get('allocation_qty', 0):.1f} | {s.get('allocation_ratio', 0)*100:.1f}% |\n"
                text += "\n"

        # === 第五部分：补充说明 ===
        text += "### 5. 补充说明\n\n"
        text += "- 以上数据均由算法计算得出，数据真实可靠\n"
        text += "- 水位线/调配方案/匹配结果可直接用于决策参考\n"
        text += "- 如需查看完整数据，请参考前文输出的JSON结构化数据\n"

        return text

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
