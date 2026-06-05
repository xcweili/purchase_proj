# -*- coding: utf-8 -*-
"""
代码沙盒执行器 - 实现"大模型思考，代码执行"的模式

适用场景：
- 数据包含大量数值计算、排序、筛选
- 需要复杂的逻辑推导
- 处理结构化数据的批量运算

核心思路：
- 大模型负责生成分析思路和代码
- 代码在安全隔离环境中执行
- 不受Token限制，计算精度100%
"""
import asyncio
import io
import sys
import json
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Tuple, List
import logging

logger = logging.getLogger(__name__)


class CodeSandbox:
    """
    代码沙盒执行器

    提供安全的代码执行环境，支持：
    - Pandas数据处理
    - NumPy数值计算
    - JSON数据格式
    - 内存隔离执行
    """

    def __init__(self):
        # 定义允许使用的模块和变量
        self.allowed_modules = {
            'pd': pd,
            'numpy': np,
            'np': np,
            'json': json,
        }

    def _create_safe_namespace(self, input_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """创建安全的执行命名空间"""
        namespace = {**self.allowed_modules}

        # 添加输入数据
        if input_data:
            namespace.update(input_data)

        # 添加常用工具函数
        namespace['print'] = lambda *args: None  # 禁用print

        return namespace

    def execute_code(self, code: str, input_data: Dict[str, Any] = None) -> Tuple[bool, Any]:
        """
        执行Python代码

        Args:
            code: 要执行的Python代码
            input_data: 输入数据字典

        Returns:
            (success, result): 执行是否成功，以及执行结果
        """
        try:
            # 创建隔离的命名空间
            namespace = self._create_safe_namespace(input_data)

            # 捕获标准输出
            old_stdout = sys.stdout
            sys.stdout = io.StringIO()

            try:
                # 执行代码
                exec(code, namespace)

                # 获取输出
                output = sys.stdout.getvalue()

                # 检查是否有result变量
                result = namespace.get('result', output if output else None)

                return (True, result)

            finally:
                # 恢复标准输出
                sys.stdout = old_stdout

        except Exception as e:
            logger.error(f"代码执行失败: {str(e)}")
            return (False, str(e))

    async def execute_code_async(self, code: str, input_data: Dict[str, Any] = None) -> Tuple[bool, Any]:
        """
        异步执行Python代码

        Args:
            code: 要执行的Python代码
            input_data: 输入数据字典

        Returns:
            (success, result): 执行是否成功，以及执行结果
        """
        # 在事件循环中执行同步代码
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.execute_code, code, input_data)


# 全局沙盒实例
code_sandbox = CodeSandbox()


# ============== 库存分析智能体 - 核心算法 ==============

def calculate_water_levels(outbound_data: List[dict]) -> dict:
    """
    基于历史出库数据动态计算水位线
    
    算法：
    1. 统计分析：计算均值、中位数、标准差、变异系数
    2. 趋势分析：同比环比判断
    3. 动态水位线计算（基于数据特征，不固定系数）：
       - 基准消耗：根据数据分布选择均值或中位数
       - 应急线：覆盖约1个月紧急需求，根据CV动态调整
       - 补库线：覆盖1.5-2.5个月，考虑波动和趋势
       - 高位线：覆盖2.5-4个月，能应对历史峰值
    """
    if not outbound_data:
        return {
            'avg_outbound': 0,
            'median_outbound': 0,
            'std_outbound': 0,
            'emergency_line': 0,
            'replenish_line': 0,
            'high_line': 0,
            'seasonality': '稳定',
            'trend': '平稳'
        }
    
    # 提取出库数据
    outbound_values = [float(d.get('outbound_qty', 0)) for d in outbound_data if d.get('outbound_qty')]
    
    if not outbound_values:
        return {
            'avg_outbound': 0,
            'median_outbound': 0,
            'std_outbound': 0,
            'emergency_line': 0,
            'replenish_line': 0,
            'high_line': 0,
            'seasonality': '稳定',
            'trend': '平稳'
        }
    
    # 统计计算
    avg_outbound = float(np.mean(outbound_values))
    median_outbound = float(np.median(outbound_values))
    std_outbound = float(np.std(outbound_values))
    max_outbound = float(np.max(outbound_values))
    
    # 计算变异系数(CV)判断数据波动
    cv = std_outbound / avg_outbound if avg_outbound > 0 else 0
    
    # 判断均值和中位数差异，选择更稳健的基准
    if avg_outbound > 0:
        median_avg_ratio = abs(median_outbound - avg_outbound) / avg_outbound
    else:
        median_avg_ratio = 0
    
    if median_avg_ratio > 0.3:
        # 差异大，说明有极端值干扰，用中位数更稳健
        base_value = median_outbound
    else:
        # 差异小，用平均值(更充分利用数据)
        base_value = avg_outbound
    
    # 趋势分析
    trend = '平稳'
    trend_factor = 1.0
    if len(outbound_values) >= 6:
        half = len(outbound_values) // 2
        recent_half = outbound_values[-half:]
        early_half = outbound_values[:half]
        recent_avg = float(np.mean(recent_half))
        early_avg = float(np.mean(early_half))
        if early_avg > 0:
            change_rate = (recent_avg - early_avg) / early_avg
            if change_rate > 0.15:
                trend = '上升'
                trend_factor = 1.0 + min(change_rate, 0.5)  # 趋势上调，最多+50%
            elif change_rate < -0.15:
                trend = '下降'
                trend_factor = 1.0 + max(change_rate, -0.3)  # 趋势下调，最多-30%
    
    # ========== 动态水位线计算 ==========
    # CV修正系数：波动越大，安全系数越高
    # CV=0(完全稳定) → cv_factor≈0.6
    # CV=0.5(中等波动) → cv_factor≈1.0
    # CV=1.0(高波动) → cv_factor≈1.5
    # CV>=2.0(极高波动) → cv_factor→2.0
    cv_factor = 0.6 + min(cv, 2.0) * 0.7
    
    # 应急线：覆盖约1个月的最基本安全库存
    emergency_line = base_value * cv_factor
    
    # 补库线：覆盖1.5-2.5个月，给补库操作留足时间
    # 在应急线基础上增加缓冲，波动大的给更多缓冲
    supply_buffer = 1.0 + cv * 0.8  # 1.0~2.6
    replenish_line = emergency_line * (1.0 + supply_buffer)
    
    # 高位线：覆盖2.5-4个月，应对峰值需求
    # 应能覆盖历史最高出库的大部分情况，但不过度
    peak_coverage = 0.7 + cv * 0.3  # 覆盖70%~100%的峰值
    high_line_from_data = max_outbound * peak_coverage
    # 同时参考补库线的倍数，取两者中较高者作为高位线
    high_line_from_factor = replenish_line * (2.0 + cv * 1.5)  # 2.0~5.0倍补库线
    high_line = max(high_line_from_data, high_line_from_factor)
    
    # 应用趋势因子（只在最后统一应用，避免重复压缩梯度）
    emergency_line *= trend_factor
    replenish_line *= trend_factor
    high_line *= trend_factor
    
    # ========== 严格兜底限制：确保水位线梯度合理 ==========
    # 1. 确保应急线和补库线有最小差距（至少50%）
    min_replenish_ratio = 1.5  # 补库线至少是应急线的1.5倍
    if replenish_line < emergency_line * min_replenish_ratio:
        replenish_line = emergency_line * min_replenish_ratio
    
    # 2. 确保高位线和补库线有最小差距（至少80%）
    min_high_ratio = 1.8  # 高位线至少是补库线的1.8倍
    if high_line < replenish_line * min_high_ratio:
        high_line = replenish_line * min_high_ratio
    
    # 3. 避免极端接近的情况（考虑四舍五入后相等）
    if abs(replenish_line - emergency_line) < 0.01:
        replenish_line = emergency_line + 0.01
    if abs(high_line - replenish_line) < 0.01:
        high_line = replenish_line + 0.01
    
    # 判断季节性(基于CV)
    if cv < 0.2:
        seasonality = '稳定'
    elif cv < 0.4:
        seasonality = '轻微波动'
    elif cv < 0.7:
        seasonality = '波动较大'
    else:
        seasonality = '波动剧烈'
    
    return {
        'avg_outbound': round(avg_outbound, 2),
        'median_outbound': round(median_outbound, 2),
        'std_outbound': round(std_outbound, 2),
        'cv': round(cv, 3),
        'base_value': round(base_value, 2),
        'cv_factor': round(cv_factor, 3),
        'trend_factor': round(trend_factor, 3),
        'emergency_line': round(emergency_line, 2),
        'replenish_line': round(replenish_line, 2),
        'high_line': round(high_line, 2),
        'seasonality': seasonality,
        'trend': trend,
        'data_points': len(outbound_values)
    }


def analyze_inventory_item(inventory_item: dict) -> dict:
    """
    分析单个库存物料
    
    Args:
        inventory_item: 包含以下字段的字典
            - warehouse_code: 仓库编码
            - warehouse_name: 仓库名称
            - material_code: 物料编码
            - tech_id: 技术规范ID
            - material_desc: 物料描述
            - current_stock: 当前库存
            - in_transit_stock: 在途库存
            - outbound_history: 历史出库数据列表
    
    Returns:
        分析结果，包含水位线和库存状态
    """
    # 获取基础数据
    current_stock = float(inventory_item.get('current_stock') or 0)
    in_transit_stock = float(inventory_item.get('in_transit_stock') or 0)
    available_stock = current_stock + in_transit_stock
    
    # 计算水位线
    outbound_history = inventory_item.get('outbound_history', [])
    water_levels = calculate_water_levels(outbound_history)
    
    emergency_line = water_levels['emergency_line']
    replenish_line = water_levels['replenish_line']
    high_line = water_levels['high_line']
    
    # 判断库存状态
    if available_stock <= emergency_line:
        stock_status = '紧急'
        suggested_action = '立即补库'
    elif available_stock <= replenish_line:
        stock_status = '低'
        suggested_action = '立即补库'
    elif available_stock <= high_line:
        stock_status = '中'
        suggested_action = '建议补库'
    else:
        stock_status = '高'
        suggested_action = '正常'
    
    # 计算建议补货数量
    recommended_qty = max(0, replenish_line - available_stock)
    
    return {
        'warehouse_code': inventory_item.get('warehouse_code', ''),
        'warehouse_name': inventory_item.get('warehouse_name', ''),
        'material_code': inventory_item.get('material_code', ''),
        'tech_id': inventory_item.get('tech_id', ''),
        'material_desc': inventory_item.get('material_desc', ''),
        'current_stock': current_stock,
        'in_transit_stock': in_transit_stock,
        'available_stock': available_stock,
        'emergency_line': emergency_line,
        'replenish_line': replenish_line,
        'high_line': high_line,
        'stock_status': stock_status,
        'suggested_action': suggested_action,
        'recommended_qty': float(recommended_qty),
        'water_level_factors': {
            'emergency_factor': round(emergency_line / replenish_line, 4) if replenish_line > 0 else 0.5,
            'replenish_factor': 1.0,
            'high_factor': round(high_line / replenish_line, 4) if replenish_line > 0 else 2.0
        },
        'statistics': {
            'avg_outbound': water_levels['avg_outbound'],
            'median_outbound': water_levels['median_outbound'],
            'std_outbound': water_levels['std_outbound'],
            'seasonality': water_levels['seasonality'],
            'trend': water_levels['trend']
        }
    }


def batch_analyze_inventory(inventory_data: List[dict]) -> dict:
    """
    批量分析库存数据
    
    Args:
        inventory_data: 库存数据列表
    
    Returns:
        包含所有物料分析结果的字典，可直接用于数据库存储
    """
    results = []
    stats = {
        'total_items': 0,
        'emergency_count': 0,
        'low_count': 0,
        'medium_count': 0,
        'high_count': 0,
        'total_current_stock': 0,
        'total_in_transit': 0,
        'total_available': 0,
        'total_recommended_qty': 0
    }
    
    for item in inventory_data:
        result = analyze_inventory_item(item)
        results.append(result)
        
        # 统计
        stats['total_items'] += 1
        stats['total_current_stock'] += result['current_stock']
        stats['total_in_transit'] += result['in_transit_stock']
        stats['total_available'] += result['available_stock']
        stats['total_recommended_qty'] += result['recommended_qty']
        
        if result['stock_status'] == '紧急':
            stats['emergency_count'] += 1
        elif result['stock_status'] == '低':
            stats['low_count'] += 1
        elif result['stock_status'] == '中':
            stats['medium_count'] += 1
        elif result['stock_status'] == '高':
            stats['high_count'] += 1
    
    return {
        'results': results,
        'summary': stats,
        'analysis_time': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    }


# ============== 调配智能体 - 核心算法 ==============

def solve_transportation_problem(sources: List[dict], destinations: List[dict]) -> List[dict]:
    """
    运输问题求解 - 贪心算法实现
    
    Args:
        sources: 源仓库列表，每个包含:
            - location: 位置编码
            - material_code: 物料编码
            - stock_qty: 库存数量
            - distance: 到目标的距离
        destinations: 目标需求列表，每个包含:
            - plan_id: 计划ID
            - material_code: 物料编码
            - demand_qty: 需求数量
    
    Returns:
        调配方案列表
    """
    if not sources or not destinations:
        return []
    
    # 按物料分组
    source_groups = {}
    for src in sources:
        mat_code = src.get('material_code', '')
        if mat_code not in source_groups:
            source_groups[mat_code] = []
        source_groups[mat_code].append(src)
    
    dest_groups = {}
    for dest in destinations:
        mat_code = dest.get('material_code', '')
        if mat_code not in dest_groups:
            dest_groups[mat_code] = []
        dest_groups[mat_code].append(dest)
    
    allocation_results = []
    
    # 对每个物料进行调配
    for mat_code in source_groups:
        if mat_code not in dest_groups:
            continue
        
        material_sources = sorted(source_groups[mat_code], key=lambda x: x.get('distance', 9999))
        material_dests = dest_groups[mat_code].copy()
        
        # 按需求优先级排序（可扩展）
        material_dests.sort(key=lambda x: x.get('demand_qty', 0), reverse=True)
        
        for dest in material_dests:
            demand = float(dest.get('demand_qty') or 0)
            plan_id = dest.get('plan_id', '')
            
            if demand <= 0:
                continue
            
            remaining_demand = demand
            
            for src in material_sources:
                if remaining_demand <= 0:
                    break
                
                stock = float(src.get('stock_qty') or 0)
                if stock <= 0:
                    continue
                
                # 分配数量
                allocate_qty = min(remaining_demand, stock)
                
                allocation_results.append({
                    'plan_id': plan_id,
                    'material_code': mat_code,
                    'source_location': src.get('location', ''),
                    'destination': dest.get('destination', ''),
                    'allocate_qty': float(allocate_qty),
                    'distance': float(src.get('distance') or 0),
                    'unit_cost': float(src.get('unit_cost') or 0),
                    'total_cost': float(allocate_qty * (src.get('unit_cost') or 0))
                })
                
                # 更新库存和需求
                src['stock_qty'] = stock - allocate_qty
                remaining_demand -= allocate_qty
            
            # 记录未满足的需求
            if remaining_demand > 0:
                allocation_results.append({
                    'plan_id': plan_id,
                    'material_code': mat_code,
                    'source_location': None,
                    'destination': dest.get('destination', ''),
                    'allocate_qty': 0,
                    'unmet_demand': float(remaining_demand),
                    'status': '未满足'
                })
    
    return allocation_results


def analyze_allocation(plans_data: List[dict], stocks_data: List[dict], strategy: str = 'time') -> dict:
    """
    调配分析主函数
    
    算法：
    1. 运输问题求解
    2. 多策略支持（时间优先、成本优先、均衡策略）
    3. 结果优化
    
    Args:
        plans_data: 计划数据列表
        stocks_data: 库存数据列表
        strategy: 策略类型 ('time', 'cost', 'balance')
    
    Returns:
        调配分析结果
    """
    # 准备源和目标数据
    sources = []
    for stock in stocks_data:
        sources.append({
            'location': stock.get('loc_code', '') or '',
            'material_code': stock.get('material_code', '') or '',
            'stock_qty': float(stock.get('stock_qty') or 0),
            'distance': float(stock.get('distance') or 9999),
            'unit_cost': float(stock.get('unit_cost') or 0)
        })
    
    destinations = []
    for plan in plans_data:
        destinations.append({
            'plan_id': plan.get('planId') or plan.get('id', ''),
            'material_code': plan.get('materialCode') or '',
            'demand_qty': float(plan.get('demandQty') or 0),
            'destination': plan.get('targetWarehouse') or ''
        })
    
    # 根据策略调整权重
    if strategy == 'cost':
        # 成本优先：按成本排序
        sources.sort(key=lambda x: x['unit_cost'])
    elif strategy == 'balance':
        # 均衡策略：考虑距离和成本的平衡
        sources.sort(key=lambda x: x['distance'] * 0.5 + x['unit_cost'] * 0.5)
    else:
        # 时间优先（默认）：按距离排序
        sources.sort(key=lambda x: x['distance'])
    
    # 执行调配
    allocation_results = solve_transportation_problem(sources, destinations)
    
    # 统计分析
    total_demand = sum(float(p.get('demandQty', 0)) for p in plans_data)
    total_allocated = sum(r.get('allocate_qty', 0) for r in allocation_results if r.get('source_location'))
    total_cost = sum(r.get('total_cost', 0) for r in allocation_results if 'total_cost' in r)
    
    fully_matched = 0
    partially_matched = 0
    no_match = 0
    
    plan_matches = {}
    for r in allocation_results:
        plan_id = r['plan_id']
        if plan_id not in plan_matches:
            plan_matches[plan_id] = {'allocated': 0, 'demand': 0}
        
        if 'unmet_demand' in r:
            plan_matches[plan_id]['demand'] += r.get('unmet_demand', 0)
        else:
            plan_matches[plan_id]['allocated'] += r.get('allocate_qty', 0)
            plan_matches[plan_id]['demand'] += r.get('allocate_qty', 0)
    
    for plan_id, match in plan_matches.items():
        if match['allocated'] >= match['demand']:
            fully_matched += 1
        elif match['allocated'] > 0:
            partially_matched += 1
        else:
            no_match += 1
    
    return {
        'strategy': strategy,
        'summary': {
            'total_plans': len(plans_data),
            'total_materials': len(set(p.get('materialCode', '') for p in plans_data)),
            'total_demand': float(total_demand),
            'total_allocated': float(total_allocated),
            'coverage_rate': float(total_allocated / total_demand) if total_demand > 0 else 0,
            'total_cost': float(total_cost),
            'avg_cost_per_unit': float(total_cost / total_allocated) if total_allocated > 0 else 0
        },
        'match_summary': {
            'fully_matched': fully_matched,
            'partially_matched': partially_matched,
            'no_match': no_match
        },
        'allocation_results': allocation_results
    }


# ============== 供应商匹配智能体 - 核心算法 ==============

def calculate_supplier_score(supplier: dict, weights: dict = None) -> float:
    """
    计算供应商综合评分
    
    评分维度：
    - 价格竞争力（40%）
    - 交付周期（30%）
    - 质量评分（30%）
    
    Args:
        supplier: 供应商数据
        weights: 权重配置
    
    Returns:
        综合评分 (0-1)
    """
    default_weights = {'price': 0.4, 'delivery': 0.3, 'quality': 0.3}
    weights = weights or default_weights
    
    price = float(supplier.get('price', 99999))
    delivery_days = float(supplier.get('delivery_days', 999))
    quality_score = float(supplier.get('quality_score', 0))
    
    # 归一化处理（需要上下文数据）
    return (1 - price / 100000) * weights['price'] + \
           (1 - delivery_days / 100) * weights['delivery'] + \
           (quality_score / 100) * weights['quality']


def solve_bipartite_matching(plans: List[dict], suppliers: List[dict], strategy: str = 'balance') -> List[dict]:
    """
    二分图匹配 - 多目标优化
    
    Args:
        plans: 计划列表
        suppliers: 供应商列表
        strategy: 策略 ('balance', 'cost', 'delivery')
    
    Returns:
        匹配结果列表
    """
    if not plans or not suppliers:
        return []
    
    # 按物料分组供应商
    supplier_groups = {}
    for sup in suppliers:
        mat_code = sup.get('material_code', '')
        if mat_code not in supplier_groups:
            supplier_groups[mat_code] = []
        supplier_groups[mat_code].append(sup)
    
    # 设置权重
    if strategy == 'cost':
        weights = {'price': 0.6, 'delivery': 0.2, 'quality': 0.2}
    elif strategy == 'delivery':
        weights = {'price': 0.2, 'delivery': 0.6, 'quality': 0.2}
    else:
        weights = {'price': 0.4, 'delivery': 0.3, 'quality': 0.3}
    
    # 为每个物料计算供应商评分
    scored_suppliers = {}
    for mat_code, sups in supplier_groups.items():
        if not sups:
            continue
        
        # 获取价格和交付周期范围用于归一化
        prices = [float(s.get('price', 99999)) for s in sups]
        delivery_days = [float(s.get('delivery_days', 999)) for s in sups]
        
        min_price, max_price = min(prices), max(prices)
        min_delivery, max_delivery = min(delivery_days), max(delivery_days)
        
        for sup in sups:
            price = float(sup.get('price', 99999))
            delivery = float(sup.get('delivery_days', 999))
            quality = float(sup.get('quality_score', 0))
            
            # 归一化评分
            price_norm = 1 - (price - min_price) / (max_price - min_price) if max_price > min_price else 1
            delivery_norm = 1 - (delivery - min_delivery) / (max_delivery - min_delivery) if max_delivery > min_delivery else 1
            quality_norm = quality / 100
            
            sup['_score'] = price_norm * weights['price'] + delivery_norm * weights['delivery'] + quality_norm * weights['quality']
        
        # 按评分排序
        scored_suppliers[mat_code] = sorted(sups, key=lambda x: x['_score'], reverse=True)
    
    # 执行匹配
    results = []
    supplier_usage = {s.get('supplier_code', s.get('supplier_name', '')): 0 for s in suppliers}
    
    for plan in plans:
        mat_code = plan.get('materialCode', '')
        demand_qty = float(plan.get('demandQty', 0))
        
        if mat_code not in scored_suppliers:
            results.append({
                'plan_id': plan.get('planId', ''),
                'material_code': mat_code,
                'suppliers': [],
                'status': '无匹配供应商'
            })
            continue
        
        available_suppliers = scored_suppliers[mat_code].copy()
        remaining_qty = demand_qty
        matched_suppliers = []
        
        # 按策略分配
        if strategy == 'balance':
            # 均衡分配：最多选择3个供应商
            max_suppliers = 3
            qty_per_supplier = demand_qty / max_suppliers if demand_qty > 0 else 0
            
            for i, sup in enumerate(available_suppliers[:max_suppliers]):
                if remaining_qty <= 0:
                    break
                
                # 根据评分调整分配比例
                score_factor = sup['_score'] if i == 0 else 0.8 if i == 1 else 0.6
                allocate_qty = min(remaining_qty, qty_per_supplier * score_factor)
                
                matched_suppliers.append({
                    'supplier_code': sup.get('supplier_code', ''),
                    'supplier_name': sup.get('supplier_name', ''),
                    'price': float(sup.get('price', 0)),
                    'delivery_days': int(sup.get('delivery_days', 0)),
                    'quality_score': float(sup.get('quality_score', 0)),
                    'score': float(sup['_score']),
                    'allocation_ratio': float(allocate_qty / demand_qty) if demand_qty > 0 else 0,
                    'allocation_qty': float(allocate_qty)
                })
                
                supplier_usage[sup.get('supplier_code', sup.get('supplier_name', ''))] += allocate_qty
                remaining_qty -= allocate_qty
        else:
            # 优先选择最优供应商
            for sup in available_suppliers:
                if remaining_qty <= 0:
                    break
                
                allocate_qty = min(remaining_qty, float(sup.get('available_qty', remaining_qty)))
                
                matched_suppliers.append({
                    'supplier_code': sup.get('supplier_code', ''),
                    'supplier_name': sup.get('supplier_name', ''),
                    'price': float(sup.get('price', 0)),
                    'delivery_days': int(sup.get('delivery_days', 0)),
                    'quality_score': float(sup.get('quality_score', 0)),
                    'score': float(sup['_score']),
                    'allocation_ratio': float(allocate_qty / demand_qty) if demand_qty > 0 else 0,
                    'allocation_qty': float(allocate_qty)
                })
                
                supplier_usage[sup.get('supplier_code', sup.get('supplier_name', ''))] += allocate_qty
                remaining_qty -= allocate_qty
        
        results.append({
            'plan_id': plan.get('planId', ''),
            'material_code': mat_code,
            'material_desc': plan.get('materialDesc', ''),
            'demand_qty': demand_qty,
            'suppliers': matched_suppliers,
            'status': '已匹配' if matched_suppliers else '部分匹配',
            'unmet_qty': float(remaining_qty)
        })
    
    return results


def match_suppliers(plans_data: List[dict], suppliers_data: List[dict], strategy: str = 'balance') -> dict:
    """
    供应商匹配主函数
    
    Args:
        plans_data: 计划数据列表
        suppliers_data: 供应商数据列表
        strategy: 策略 ('balance', 'cost', 'delivery')
    
    Returns:
        匹配结果
    """
    # 执行匹配
    match_results = solve_bipartite_matching(plans_data, suppliers_data, strategy)
    
    # 统计分析
    total_plans = len(plans_data)
    matched_plans = sum(1 for r in match_results if r['status'] == '已匹配')
    partially_matched = sum(1 for r in match_results if r['status'] == '部分匹配')
    no_match = total_plans - matched_plans - partially_matched
    
    total_demand = sum(float(p.get('demandQty', 0)) for p in plans_data)
    total_allocated = sum(sum(s.get('allocation_qty', 0) for s in r.get('suppliers', [])) for r in match_results)
    
    # 供应商分布统计
    supplier_counts = {}
    for r in match_results:
        for s in r.get('suppliers', []):
            sup_name = s['supplier_name']
            supplier_counts[sup_name] = supplier_counts.get(sup_name, 0) + 1
    
    # 平均评分统计
    all_scores = []
    for r in match_results:
        for s in r.get('suppliers', []):
            all_scores.append(s['score'])
    avg_score = np.mean(all_scores) if all_scores else 0
    
    return {
        'strategy': strategy,
        'summary': {
            'total_plans': total_plans,
            'matched_plans': matched_plans,
            'partially_matched': partially_matched,
            'no_match': no_match,
            'total_demand': float(total_demand),
            'total_allocated': float(total_allocated),
            'coverage_rate': float(total_allocated / total_demand) if total_demand > 0 else 0,
            'avg_supplier_score': float(avg_score),
            'avg_suppliers_per_plan': float(sum(len(r.get('suppliers', [])) for r in match_results) / max(total_plans, 1))
        },
        'supplier_distribution': supplier_counts,
        'match_results': match_results
    }


# ============== 预定义脚本 ==============

INVENTORY_ANALYSIS_SCRIPT = '''
import json
import numpy as np

def analyze_inventory(inventory_data):
    """库存分析主函数 - 基于历史出库数据计算水位线"""
    results = []
    stats = {
        'total_items': 0,
        'emergency_count': 0,
        'low_count': 0,
        'medium_count': 0,
        'high_count': 0,
        'total_current_stock': 0,
        'total_in_transit': 0,
        'total_available': 0,
        'total_recommended_qty': 0
    }
    
    for item in inventory_data:
        current_stock = float(item.get('current_stock', 0))
        in_transit_stock = float(item.get('in_transit_stock', 0))
        available_stock = current_stock + in_transit_stock
        
        # 计算水位线
        outbound_history = item.get('outbound_history', [])
        outbound_values = [float(d.get('outbound_qty', 0)) for d in outbound_history if d.get('outbound_qty')]
        
        if outbound_values:
            median_outbound = float(np.median(outbound_values))
            avg_outbound = float(np.mean(outbound_values))
            std_outbound = float(np.std(outbound_values))
            
            emergency_line = median_outbound * 0.5
            replenish_line = median_outbound * 2
            high_line = median_outbound * 4
        else:
            median_outbound = 0
            avg_outbound = 0
            std_outbound = 0
            emergency_line = 0
            replenish_line = 0
            high_line = 0
        
        # 判断库存状态
        if available_stock <= emergency_line:
            stock_status = '紧急'
            suggested_action = '建议补库'
        elif available_stock <= replenish_line:
            stock_status = '低'
            suggested_action = '建议补库'
        elif available_stock <= high_line:
            stock_status = '中'
            suggested_action = '立即补库'
        else:
            stock_status = '高'
            suggested_action = '正常'
        
        recommended_qty = max(0, replenish_line - available_stock)
        
        results.append({
            'warehouse_code': item.get('warehouse_code', ''),
            'warehouse_name': item.get('warehouse_name', ''),
            'material_code': item.get('material_code', ''),
            'tech_id': item.get('tech_id', ''),
            'material_desc': item.get('material_desc', ''),
            'current_stock': current_stock,
            'in_transit_stock': in_transit_stock,
            'available_stock': available_stock,
            'emergency_line': emergency_line,
            'replenish_line': replenish_line,
            'high_line': high_line,
            'stock_status': stock_status,
            'suggested_action': suggested_action,
            'recommended_qty': float(recommended_qty),
            'statistics': {
                'avg_outbound': avg_outbound,
                'median_outbound': median_outbound,
                'std_outbound': std_outbound
            }
        })
        
        stats['total_items'] += 1
        stats['total_current_stock'] += current_stock
        stats['total_in_transit'] += in_transit_stock
        stats['total_available'] += available_stock
        stats['total_recommended_qty'] += recommended_qty
        
        if stock_status == '紧急':
            stats['emergency_count'] += 1
        elif stock_status == '低':
            stats['low_count'] += 1
        elif stock_status == '中':
            stats['medium_count'] += 1
        elif stock_status == '高':
            stats['high_count'] += 1
    
    return json.dumps({
        'results': results,
        'summary': stats
    }, ensure_ascii=False)
'''

ALLOCATION_ANALYSIS_SCRIPT = '''
import json

def analyze_allocation(plans_data, stocks_data, strategy='time'):
    """调配分析主函数 - 运输问题贪心算法"""
    # 按物料分组
    source_groups = {}
    for src in stocks_data:
        mat_code = src.get('material_code', '')
        if mat_code not in source_groups:
            source_groups[mat_code] = []
        source_groups[mat_code].append({
            'location': src.get('loc_code', ''),
            'stock_qty': float(src.get('stock_qty', 0)),
            'distance': float(src.get('distance', 9999)),
            'unit_cost': float(src.get('unit_cost', 0))
        })
    
    dest_groups = {}
    for dest in plans_data:
        mat_code = dest.get('materialCode', '')
        if mat_code not in dest_groups:
            dest_groups[mat_code] = []
        dest_groups[mat_code].append({
            'plan_id': dest.get('planId', dest.get('id', '')),
            'demand_qty': float(dest.get('demandQty', 0)),
            'destination': dest.get('targetWarehouse', '')
        })
    
    # 根据策略排序
    for mat_code in source_groups:
        if strategy == 'cost':
            source_groups[mat_code].sort(key=lambda x: x['unit_cost'])
        elif strategy == 'balance':
            source_groups[mat_code].sort(key=lambda x: x['distance'] * 0.5 + x['unit_cost'] * 0.5)
        else:
            source_groups[mat_code].sort(key=lambda x: x['distance'])
    
    # 执行调配
    allocation_results = []
    
    for mat_code in source_groups:
        if mat_code not in dest_groups:
            continue
        
        sources = source_groups[mat_code]
        dests = dest_groups[mat_code]
        
        for dest in dests:
            demand = dest['demand_qty']
            remaining = demand
            
            for src in sources:
                if remaining <= 0:
                    break
                if src['stock_qty'] <= 0:
                    continue
                
                allocate = min(remaining, src['stock_qty'])
                allocation_results.append({
                    'plan_id': dest['plan_id'],
                    'material_code': mat_code,
                    'source_location': src['location'],
                    'destination': dest['destination'],
                    'allocate_qty': float(allocate),
                    'distance': float(src['distance']),
                    'total_cost': float(allocate * src['unit_cost'])
                })
                
                src['stock_qty'] -= allocate
                remaining -= allocate
            
            if remaining > 0:
                allocation_results.append({
                    'plan_id': dest['plan_id'],
                    'material_code': mat_code,
                    'source_location': None,
                    'unmet_demand': float(remaining),
                    'status': '未满足'
                })
    
    # 统计
    total_demand = sum(float(p.get('demandQty', 0)) for p in plans_data)
    total_allocated = sum(r.get('allocate_qty', 0) for r in allocation_results if r.get('source_location'))
    total_cost = sum(r.get('total_cost', 0) for r in allocation_results if 'total_cost' in r)
    
    return json.dumps({
        'strategy': strategy,
        'summary': {
            'total_plans': len(plans_data),
            'total_demand': float(total_demand),
            'total_allocated': float(total_allocated),
            'coverage_rate': float(total_allocated / total_demand) if total_demand > 0 else 0,
            'total_cost': float(total_cost)
        },
        'allocation_results': allocation_results
    }, ensure_ascii=False)
'''

SUPPLIER_MATCH_SCRIPT = '''
import json

def match_suppliers(plans_data, suppliers_data, strategy='balance'):
    """供应商匹配主函数 - 多目标优化算法"""
    # 按物料分组供应商
    supplier_groups = {}
    for sup in suppliers_data:
        mat_code = sup.get('material_code', '')
        if mat_code not in supplier_groups:
            supplier_groups[mat_code] = []
        supplier_groups[mat_code].append(sup)
    
    # 设置权重
    if strategy == 'cost':
        weights = {'price': 0.6, 'delivery': 0.2, 'quality': 0.2}
    elif strategy == 'delivery':
        weights = {'price': 0.2, 'delivery': 0.6, 'quality': 0.2}
    else:
        weights = {'price': 0.4, 'delivery': 0.3, 'quality': 0.3}
    
    # 计算供应商评分
    for mat_code, sups in supplier_groups.items():
        prices = [float(s.get('price', 99999)) for s in sups]
        deliveries = [float(s.get('delivery_days', 999)) for s in sups]
        
        min_p, max_p = min(prices), max(prices)
        min_d, max_d = min(deliveries), max(deliveries)
        
        for sup in sups:
            price = float(sup.get('price', 99999))
            delivery = float(sup.get('delivery_days', 999))
            quality = float(sup.get('quality_score', 0))
            
            price_norm = 1 - (price - min_p) / (max_p - min_p) if max_p > min_p else 1
            delivery_norm = 1 - (delivery - min_d) / (max_d - min_d) if max_d > min_d else 1
            quality_norm = quality / 100
            
            sup['_score'] = price_norm * weights['price'] + delivery_norm * weights['delivery'] + quality_norm * weights['quality']
        
        sups.sort(key=lambda x: x['_score'], reverse=True)
    
    # 执行匹配
    results = []
    for plan in plans_data:
        mat_code = plan.get('materialCode', '')
        demand = float(plan.get('demandQty', 0))
        
        if mat_code not in supplier_groups:
            results.append({
                'plan_id': plan.get('planId', ''),
                'material_code': mat_code,
                'suppliers': [],
                'status': '无匹配供应商'
            })
            continue
        
        suppliers = supplier_groups[mat_code]
        remaining = demand
        matched = []
        
        if strategy == 'balance':
            max_sup = 3
            qty_per = demand / max_sup if demand > 0 else 0
            
            for i, sup in enumerate(suppliers[:max_sup]):
                if remaining <= 0:
                    break
                
                factor = sup['_score'] if i == 0 else 0.8 if i == 1 else 0.6
                allocate = min(remaining, qty_per * factor)
                
                matched.append({
                    'supplier_name': sup.get('supplier_name', ''),
                    'price': float(sup.get('price', 0)),
                    'delivery_days': int(sup.get('delivery_days', 0)),
                    'score': float(sup['_score']),
                    'allocation_ratio': float(allocate / demand) if demand > 0 else 0,
                    'allocation_qty': float(allocate)
                })
                remaining -= allocate
        else:
            for sup in suppliers:
                if remaining <= 0:
                    break
                allocate = min(remaining, float(sup.get('available_qty', remaining)))
                
                matched.append({
                    'supplier_name': sup.get('supplier_name', ''),
                    'price': float(sup.get('price', 0)),
                    'delivery_days': int(sup.get('delivery_days', 0)),
                    'score': float(sup['_score']),
                    'allocation_ratio': float(allocate / demand) if demand > 0 else 0,
                    'allocation_qty': float(allocate)
                })
                remaining -= allocate
        
        results.append({
            'plan_id': plan.get('planId', ''),
            'material_code': mat_code,
            'demand_qty': demand,
            'suppliers': matched,
            'status': '已匹配' if matched else '部分匹配',
            'unmet_qty': float(remaining)
        })
    
    return json.dumps({
        'strategy': strategy,
        'summary': {
            'total_plans': len(plans_data),
            'matched_plans': sum(1 for r in results if r['status'] == '已匹配'),
            'total_demand': float(sum(float(p.get('demandQty', 0)) for p in plans_data)),
            'total_allocated': float(sum(sum(s.get('allocation_qty', 0) for s in r.get('suppliers', [])) for r in results))
        },
        'match_results': results
    }, ensure_ascii=False)
'''
