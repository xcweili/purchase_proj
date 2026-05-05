# -*- coding: utf-8 -*-
"""供应商匹配服务 - 流式版本（复用原服务逻辑）"""
import json
from typing import List, Dict, Any, Optional

from ..services.supplier_match_service import SupplierMatchService
from ..services.context_manager import ContextManager


class SupplierMatchStreamService:
    """供应商匹配服务 - 流式版本（复用SupplierMatchService的数据查询逻辑）"""

    def __init__(self, db, llm_stream_func, llm_func=None):
        self.db = db
        self.llm_stream_func = llm_stream_func
        self.llm_func = llm_func
        self._service = SupplierMatchService.__new__(SupplierMatchService)
        self._service.db = db
        if llm_func:
            self.context_manager = ContextManager(llm_func, llm_stream_func)
        else:
            self.context_manager = None

    async def stream_analyze(self, input_plans: List[Dict[str, Any]] = None, analyze_mode: str = "iterative"):
        """流式分析供应商匹配

        Args:
            input_plans: 输入的补货计划列表，如果为None则从数据库查询
            analyze_mode: 分析模式，"batch"一次性分析所有组合，"iterative"逐个分析(默认)
        """
        # 立即输出第一个消息，让用户知道服务正在处理
        yield "🚀 供应商匹配服务启动...\n"
        
        try:
            yield "🔍 [步骤 1/2] 正在获取补货计划...\n"
            if input_plans and len(input_plans) > 0:
                plans = input_plans
                yield f"✅ [步骤 1/2] 已获取 {len(plans)} 条输入计划\n\n"
            else:
                plans = await self._service._query_plans()
                yield f"✅ [步骤 1/2] 已从数据库获取 {len(plans)} 条补货计划\n\n"

            if not plans:
                yield "📋 未查询到补货计划。\n"
                return

            if analyze_mode == "batch":
                yield "📦 [批量分析模式] 将一次性分析所有计划...\n\n"
                async for chunk in self._batch_analyze(plans):
                    yield chunk
            else:
                yield "🔄 [迭代分析模式] 将逐个分析每个计划...\n\n"
                async for chunk in self._iterative_analyze(plans):
                    yield chunk

        except Exception as e:
            yield f"❌ 整体分析失败: {str(e)}\n"
            import traceback
            yield f"详细信息: {traceback.format_exc()}\n"

    async def _batch_analyze(self, plans: List[Dict[str, Any]]):
        """批量分析模式 - 一次性分析所有计划"""
        try:
            all_plan_data = []
            all_suppliers = []

            yield "🔍 正在收集所有计划和供应商数据...\n"

            for idx, plan in enumerate(plans, 1):
                plan_id = plan.get('planId') or plan.get('plan_id', f'plan_{idx}')
                material_code = plan.get('materialCode', '')
                tech_id = plan.get('techSpecId', '')
                demand_qty = float(plan.get('demandQty', 0) or 0)
                warehouse_code = plan.get('warehouseCode', '')

                material_desc = plan.get('materialDesc', '') or plan.get('fd_desc', '')
                if not material_desc:
                    material_desc = await self._service._get_material_desc_from_stock(material_code, tech_id)

                company = await self._service._get_company_from_warehouse(warehouse_code)
                suppliers = await self._service._get_protocol_suppliers(plan)

                plan_data = {
                    'plan_id': plan_id,
                    'material_code': material_code,
                    'tech_id': tech_id,
                    'demand_qty': demand_qty,
                    'warehouse_code': warehouse_code,
                    'material_desc': material_desc or '',
                    'company': company or '',
                    'suppliers': suppliers
                }
                all_plan_data.append(plan_data)

                for s in suppliers:
                    s['plan_id'] = plan_id
                    all_suppliers.append(s)

                yield f"✅ [计划 {idx}/{len(plans)}] {plan_id} - 找到 {len(suppliers)} 个供应商\n"

            yield f"\n📊 共收集到 {len(all_plan_data)} 个计划和 {len(all_suppliers)} 个供应商数据\n\n"

            yield "🤖 开始AI批量分析...\n"
            yield "────────────────────────────────────────\n"

            prompt = self._build_batch_prompt(all_plan_data, all_suppliers)
            system_prompt = "你是一个专业的电力物料采购供应商匹配专家，擅长分析供应商协议数据并给出最优的供应商选择方案。请用清晰的中文进行分析。"

            if self.context_manager and self.context_manager.is_too_long(prompt):
                yield "⚠️ 检测到数据量较大，将采用分层推理模式...\n"
                async for chunk in self.context_manager._streaming_hierarchical_reasoning(prompt, system_prompt):
                    content = self._parse_llm_chunk(chunk)
                    if content:
                        yield content
            else:
                async for chunk in self.llm_stream_func(prompt, system_prompt):
                    content = self._parse_llm_chunk(chunk)
                    if content:
                        yield content

            matched_count = sum(1 for p in all_plan_data if p['suppliers'])
            yield "\n📊 供应商匹配汇总报告\n"
            yield "────────────────────────────────────────\n"
            yield f"   总计划数: {len(all_plan_data)}\n"
            yield f"   有供应商匹配: {matched_count}\n"
            yield f"   无供应商匹配: {len(all_plan_data) - matched_count}\n"
            yield "────────────────────────────────────────\n"

        except Exception as e:
            yield f"❌ 批量分析失败: {str(e)}\n"
            import traceback
            yield f"详细信息: {traceback.format_exc()}\n"

    async def _iterative_analyze(self, plans: List[Dict[str, Any]]):
        """迭代分析模式 - 逐个分析每个计划"""
        total_count = len(plans)
        matched_count = 0
        unmatched_count = 0

        for idx, plan in enumerate(plans, 1):
            plan_id = plan.get('planId') or plan.get('plan_id', f'plan_{idx}')
            material_code = plan.get('materialCode', '')
            tech_id = plan.get('techSpecId', '')
            demand_qty = float(plan.get('demandQty', 0) or 0)
            warehouse_code = plan.get('warehouseCode', '')

            yield "\n📋 [处理 {idx}/{total_count}] 开始处理计划\n".format(idx=idx, total_count=total_count)
            yield "────────────────────────────────────────\n"
            yield f"   计划ID: {plan_id}\n"
            yield f"   物料编码: {material_code}\n"
            yield f"   技术规范ID: {tech_id}\n"
            yield f"   需求数量: {demand_qty}\n"
            yield f"   目标仓库: {warehouse_code}\n"
            yield "────────────────────────────────────────\n"

            try:
                yield "🔍 [子步骤 1/3] 获取物料描述...\n"
                material_desc = plan.get('materialDesc', '') or plan.get('fd_desc', '')
                if not material_desc:
                    material_desc = await self._service._get_material_desc_from_stock(material_code, tech_id)
                yield f"✅ [子步骤 1/3] 物料描述: {material_desc or '未获取到'}\n"

                yield "🔍 [子步骤 2/3] 获取所属单位...\n"
                company = await self._service._get_company_from_warehouse(warehouse_code)
                yield f"✅ [子步骤 2/3] 所属单位: {company or '未获取到'}\n"

                yield "🔍 [子步骤 3/3] 查询协议商库存...\n"
                suppliers = await self._service._get_protocol_suppliers(plan)

                if suppliers:
                    yield f"✅ [子步骤 3/3] 找到 {len(suppliers)} 个协议供应商\n"
                    for i, s in enumerate(suppliers[:3], 1):
                        yield f"   [{i}] 供应商: {s.get('supplierName', '')}, 执行率: {s.get('executionRate', 0)}%, 可用量: {s.get('remainQty', 0)}\n"
                    if len(suppliers) > 3:
                        yield f"   ... 还有 {len(suppliers) - 3} 个供应商\n"
                    matched_count += 1
                else:
                    yield "⚠️ [子步骤 3/3] 未找到匹配的协议供应商\n"
                    unmatched_count += 1

                yield "\n🤖 开始AI智能分析...\n"
                yield "────────────────────────────────────────\n"

                prompt = self._build_single_plan_prompt(plan, suppliers, material_desc, company)
                system_prompt = "你是一个专业的电力物料采购供应商匹配专家，擅长分析供应商协议数据并给出最优的供应商选择方案。请用清晰的中文进行分析。"

                if self.context_manager and self.context_manager.is_too_long(prompt):
                    yield "⚠️ 检测到数据量较大，将采用分层推理模式...\n"
                    async for chunk in self.context_manager._streaming_hierarchical_reasoning(prompt, system_prompt):
                        content = self._parse_llm_chunk(chunk)
                        if content:
                            yield content
                else:
                    async for chunk in self.llm_stream_func(prompt, system_prompt):
                        content = self._parse_llm_chunk(chunk)
                        if content:
                            yield content

                status = '有匹配' if suppliers else '无匹配'
                yield f"\n✅ [处理完成] 供应商匹配状态: {status}\n"

            except Exception as e:
                yield f"\n❌ [处理失败] {str(e)}\n"
                unmatched_count += 1

            yield "\n────────────────────────────────────────\n\n"

        yield "\n📊 供应商匹配汇总报告\n"
        yield "────────────────────────────────────────\n"
        yield f"   总计划数: {total_count}\n"
        yield f"   有供应商匹配: {matched_count}\n"
        yield f"   无供应商匹配: {unmatched_count}\n"
        yield "────────────────────────────────────────\n"

    def _parse_llm_chunk(self, chunk: str) -> Optional[str]:
        """解析LLM返回的JSON格式chunk，提取内容和思考过程"""
        try:
            data = json.loads(chunk)
            choices = data.get('choices', [])
            if choices:
                delta = choices[0].get('delta', {})
                reasoning = delta.get('reasoning_content', '')
                content = delta.get('content', '')

                if reasoning:
                    return f"{reasoning}"
                elif content:
                    return content
        except json.JSONDecodeError:
            return chunk.strip()
        return None

    def _build_batch_prompt(self, all_plan_data: List[Dict[str, Any]], all_suppliers: List[Dict[str, Any]]) -> str:
        """为批量分析构建prompt"""
        plans_formatted = []
        for p in all_plan_data:
            plans_formatted.append({
                '计划ID': p['plan_id'],
                '物料编码': p['material_code'],
                '物料描述': p['material_desc'],
                '技术规范ID': p['tech_id'],
                '需求数量': p['demand_qty'],
                '目标仓库': p['warehouse_code'],
                '所属单位': p['company']
            })

        suppliers_formatted = []
        for s in all_suppliers:
            suppliers_formatted.append({
                '计划ID': s.get('plan_id', ''),
                '供应商编码': s.get('supplierCode', ''),
                '供应商名称': s.get('supplierName', ''),
                '执行比例(%)': float(s.get('executionRate', 0) or 0),
                '剩余可用数量': float(s.get('remainQty', 0) or 0),
                '剩余可用金额': float(s.get('remainAmount', 0) or 0),
                '协议单价': float(s.get('unitPrice', 0) or 0),
            })

        prompt = f"""你是一个专业的电力物料采购供应商匹配专家。我将提供多个补货需求计划和供应商协议数据，请你一次性分析所有计划并给出最优的供应商选择方案。

## 任务说明
请一次性分析以下所有补货需求计划（共 {len(plans_formatted)} 个计划），根据每个计划的供应商协议执行情况和可用库存，为每个需求计划单独选择最合适的供应商。

**重要**：你必须为**每一个补货计划**单独输出一份完整的分析结果，按照下面规定的格式，一个计划一个计划地列出结果。

## 输入数据

### 补货计划列表（共 {len(plans_formatted)} 个计划）
{json.dumps(plans_formatted, ensure_ascii=False, indent=2)}

### 供应商协议数据
{json.dumps(suppliers_formatted, ensure_ascii=False, indent=2)}

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

## 输出格式要求

**重要**：你必须按照以下格式，为每一个补货计划单独输出一份完整的分析结果。

请使用Markdown格式输出，使用##、###标题，表格使用|分隔。

**输出结构必须包含以下内容，并严格按照顺序输出**：

---

## 【计划 1/{len(plans_formatted)}】供应商匹配分析

### 一、需求概况
- 计划ID: [从输入数据中获取]
- 物料编码: [从输入数据中获取]
- 物料描述: [从输入数据中获取]
- 需求数量: [从输入数据中获取]
- 目标仓库: [从输入数据中获取]

### 二、供应商评估
- 可用供应商数量
- 各供应商执行比例分析
- 可用库存和金额对比

### 三、推荐方案
| 策略 | 推荐供应商 | 分配数量 | 单价 | 理由 |
|------|-----------|---------|------|------|

### 四、分析结论与建议
- 是否有合适的供应商
- 推荐的供应商选择策略
- 后续处理建议

---

**然后继续输出计划2，计划3...直到所有{len(plans_formatted)}个计划都分析完毕**

请用简洁、清晰的语言进行分析。
"""
        return prompt

    def _build_single_plan_prompt(self, plan: Dict[str, Any], suppliers: List[Dict[str, Any]], material_desc: str, company: str) -> str:
        """为单个计划构建供应商匹配分析prompt"""
        plan_id = plan.get('planId') or plan.get('plan_id', '')
        material_code = plan.get('materialCode', '')
        demand_qty = float(plan.get('demandQty', 0) or 0)
        tech_id = plan.get('techSpecId', '')
        warehouse_code = plan.get('warehouseCode', '')

        suppliers_formatted = []
        for s in suppliers[:10]:
            suppliers_formatted.append({
                '供应商编码': s.get('supplierCode', ''),
                '供应商名称': s.get('supplierName', ''),
                '执行比例(%)': float(s.get('executionRate', 0) or 0),
                '剩余可用数量': float(s.get('remainQty', 0) or 0),
                '剩余可用金额': float(s.get('remainAmount', 0) or 0),
                '协议单价': float(s.get('unitPrice', 0) or 0),
            })

        prompt = f"""你是一个专业的电力物料采购供应商匹配专家。请根据以下补货计划和供应商协议数据，进行智能匹配分析。

## 任务说明
分析单个补货计划的供应商匹配情况，为其选择最合适的供应商，并给出多种策略方案。

## 当前计划信息
- 计划ID: {plan_id}
- 物料编码: {material_code}
- 物料描述: {material_desc}
- 技术规范ID: {tech_id}
- 需求数量: {demand_qty}
- 目标仓库: {warehouse_code}
- 所属单位: {company}

## 协议供应商数据
{json.dumps(suppliers_formatted, ensure_ascii=False, indent=2)}

## 分析要求

**输出格式要求：**
- 使用Markdown格式输出
- 使用##、### 标题
- 列表使用 - 开头
- 表格使用 | 分隔

**输出结构：**

## 供应商匹配分析结果

### 一、需求概况
- 计划ID、物料编码、需求数量等基本信息

### 二、供应商评估
- 供应商数量和整体情况
- 各供应商执行率分析

### 三、推荐方案
| 策略 | 推荐供应商 | 数量 | 单价 | 理由 |
|------|-----------|------|------|------|
| 均衡 | 示例供应商 | 100 | 100 | 综合最优 |

### 四、分析结论与建议
- 是否有合适的供应商
- 推荐的供应商选择策略
- 后续处理建议

请用简洁、清晰的语言进行分析。
"""
        return prompt
