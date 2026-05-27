# -*- coding: utf-8 -*-
"""JSON 修复工具 - 提供双层保障机制"""
import json
import logging
from typing import Any, Optional, Dict, List

logger = logging.getLogger(__name__)

# 尝试导入 json-repair 包，如果失败则使用内置修复方案
try:
    from json_repair import repair_json as json_repair_func
    JSON_REPAIR_AVAILABLE = True
    logger.info("json-repair 包已安装，将使用外部 JSON 修复方案")
except ImportError:
    JSON_REPAIR_AVAILABLE = False
    logger.warning("json-repair 包未安装，将使用内置 JSON 修复方案")


class JSONRepair:
    """JSON 修复工具类 - 提供双层保障机制
    
    第一层保障：使用 json-repair 包或内置方法修复损坏的 JSON
    第二层保障：调用代码沙盒模式进行兜底处理
    """

    @staticmethod
    def repair_json(json_str: str) -> Optional[Any]:
        """
        修复损坏的 JSON 字符串
        
        第一层保障：尝试多种方法修复 JSON
        1. 尝试直接解析
        2. 使用 json-repair 包修复（如果可用）
        3. 使用内置修复方法
        
        Args:
            json_str: 可能损坏的 JSON 字符串
            
        Returns:
            解析后的 Python 对象（dict 或 list），如果无法修复返回 None
        """
        # 方法 1：直接解析
        try:
            result = json.loads(json_str)
            return result
        except json.JSONDecodeError:
            pass

        # 方法 2：使用 json-repair 包（如果可用）
        if JSON_REPAIR_AVAILABLE:
            try:
                repaired = json_repair_func(json_str)
                if repaired:
                    result = json.loads(repaired) if isinstance(repaired, str) else repaired
                    logger.info("JSON 修复成功（使用 json-repair 包）")
                    return result
            except Exception as e:
                logger.warning(f"json-repair 包修复失败：{str(e)}")

        # 方法 3：内置修复方法
        try:
            repaired = JSONRepair._simple_repair(json_str)
            if repaired:
                result = json.loads(repaired)
                logger.info("JSON 修复成功（使用内置方法）")
                return result
        except Exception as e:
            logger.warning(f"内置修复方法失败：{str(e)}")

        # 方法 4：尝试提取 JSON 片段
        try:
            result = JSONRepair._extract_json_fragment(json_str)
            if result:
                logger.info("JSON 修复成功（使用片段提取）")
                return result
        except Exception as e:
            logger.warning(f"片段提取失败：{str(e)}")

        logger.error(f"无法修复 JSON: {json_str[:200]}...")
        return None

    @staticmethod
    def _simple_repair(json_str: str) -> Optional[str]:
        """
        简单的 JSON 修复方法
        
        修复常见问题：
        1. 修复不完整的引号
        2. 修复缺少的闭合括号
        3. 移除多余的逗号
        4. 修复转义问题
        """
        if not json_str:
            return None

        # 去除首尾空白
        s = json_str.strip()

        # 修复引号问题
        s = JSONRepair._fix_quotes(s)

        # 修复括号配对
        s = JSONRepair._fix_brackets(s)

        # 移除多余逗号
        s = JSONRepair._remove_trailing_commas(s)

        return s

    @staticmethod
    def _fix_quotes(s: str) -> str:
        """修复引号问题"""
        # 处理未闭合的字符串
        quote_stack = []
        result = []
        
        i = 0
        while i < len(s):
            char = s[i]
            
            if char == '"' and (i == 0 or s[i-1] != '\\'):
                if quote_stack and quote_stack[-1] == '"':
                    quote_stack.pop()
                else:
                    quote_stack.append('"')
                result.append(char)
            elif char == "'" and (i == 0 or s[i-1] != '\\'):
                # 将单引号转换为双引号（JSON 标准）
                if quote_stack and quote_stack[-1] == "'":
                    quote_stack.pop()
                    result.append('"')
                else:
                    quote_stack.append("'")
                    result.append('"')
            else:
                result.append(char)
            i += 1

        # 闭合未闭合的引号
        for quote in quote_stack:
            result.append('"')

        return ''.join(result)

    @staticmethod
    def _fix_brackets(s: str) -> str:
        """修复括号配对问题"""
        bracket_map = {'(': ')', '[': ']', '{': '}'}
        bracket_stack = []
        
        for char in s:
            if char in bracket_map.keys():
                bracket_stack.append(char)
            elif char in bracket_map.values():
                if bracket_stack:
                    expected = bracket_map.get(bracket_stack[-1])
                    if expected == char:
                        bracket_stack.pop()
                    else:
                        # 括号不匹配，尝试修复
                        bracket_stack.pop()
        
        # 添加缺少的闭合括号
        result = s
        while bracket_stack:
            last_open = bracket_stack.pop()
            result += bracket_map.get(last_open, '')
        
        return result

    @staticmethod
    def _remove_trailing_commas(s: str) -> str:
        """移除多余的逗号"""
        import re
        # 移除 ] 或 } 前的逗号
        s = re.sub(r',\s*([\]}])', r'\1', s)
        return s

    @staticmethod
    def _extract_json_fragment(json_str: str) -> Optional[Any]:
        """尝试从文本中提取 JSON 片段"""
        # 找到第一个 { 或 [
        start_idx = min(
            json_str.find('{') if '{' in json_str else len(json_str),
            json_str.find('[') if '[' in json_str else len(json_str)
        )
        
        if start_idx == len(json_str):
            return None

        # 找到对应的闭合括号
        s = json_str[start_idx:]
        bracket_map = {'{': '}', '[': ']'}
        bracket_stack = []
        
        for i, char in enumerate(s):
            if char in bracket_map.keys():
                bracket_stack.append(char)
            elif char in bracket_map.values():
                if bracket_stack:
                    expected = bracket_map.get(bracket_stack[-1])
                    if expected == char:
                        bracket_stack.pop()
                        if not bracket_stack:
                            # 找到匹配的闭合括号
                            fragment = s[:i+1]
                            return json.loads(fragment)
        
        return None


class SmartJSONParser:
    """智能 JSON 解析器 - 提供 JSON 修复能力"""

    def __init__(self, context_manager=None):
        """
        Args:
            context_manager: ContextManager 实例（保留用于未来扩展）
        """
        self.context_manager = context_manager

    def parse(self, json_str: str) -> Optional[Any]:
        """
        解析 JSON 字符串（使用第一层保障：JSON 修复）
        
        Args:
            json_str: JSON 字符串
            
        Returns:
            解析结果，如果无法修复返回 None
        """
        return JSONRepair.repair_json(json_str)
