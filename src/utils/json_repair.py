# -*- coding: utf-8 -*-
"""JSON修复工具 - 提供双层保障机制"""
import json
import logging
from typing import Any, Optional, Dict, List

logger = logging.getLogger(__name__)

# 尝试导入repair包，如果失败则使用内置修复方案
try:
    import repair
    REPAIR_AVAILABLE = True
except ImportError:
    REPAIR_AVAILABLE = False
    logger.warning("repair包未安装，将使用内置JSON修复方案")


class JSONRepair:
    """JSON修复工具类 - 提供双层保障机制
    
    第一层保障：使用repair包或内置方法修复损坏的JSON
    第二层保障：调用代码沙盒模式进行兜底处理
    """

    @staticmethod
    def repair_json(json_str: str) -> Optional[Any]:
        """
        修复损坏的JSON字符串
        
        第一层保障：尝试多种方法修复JSON
        1. 尝试直接解析
        2. 使用repair包修复（如果可用）
        3. 使用内置修复方法
        
        Args:
            json_str: 可能损坏的JSON字符串
            
        Returns:
            解析后的Python对象（dict或list），如果无法修复返回None
        """
        # 方法1：直接解析
        try:
            result = json.loads(json_str)
            return result
        except json.JSONDecodeError:
            pass

        # 方法2：使用repair包（如果可用）
        if REPAIR_AVAILABLE:
            try:
                repaired = repair.fix(json_str)
                if repaired:
                    result = json.loads(repaired)
                    logger.info("JSON修复成功（使用repair包）")
                    return result
            except Exception as e:
                logger.warning(f"repair包修复失败: {str(e)}")

        # 方法3：内置修复方法
        try:
            repaired = JSONRepair._simple_repair(json_str)
            if repaired:
                result = json.loads(repaired)
                logger.info("JSON修复成功（使用内置方法）")
                return result
        except Exception as e:
            logger.warning(f"内置修复方法失败: {str(e)}")

        # 方法4：尝试提取JSON片段
        try:
            result = JSONRepair._extract_json_fragment(json_str)
            if result:
                logger.info("JSON修复成功（使用片段提取）")
                return result
        except Exception as e:
            logger.warning(f"片段提取失败: {str(e)}")

        logger.error(f"无法修复JSON: {json_str[:200]}...")
        return None

    @staticmethod
    def _simple_repair(json_str: str) -> Optional[str]:
        """
        简单的JSON修复方法
        
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
                # 将单引号转换为双引号（JSON标准）
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
        """尝试从文本中提取JSON片段"""
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
    """智能JSON解析器 - 集成双层保障机制"""

    def __init__(self, context_manager=None):
        """
        Args:
            context_manager: ContextManager实例，用于第二层兜底
        """
        self.context_manager = context_manager

    async def parse_with_fallback(self, 
                                json_str: str, 
                                data_type: str = 'inventory',
                                original_data: Optional[Dict[str, Any]] = None,
                                prompt: str = "",
                                system_prompt: str = "") -> Any:
        """
        使用双层保障机制解析JSON
        
        第一层保障：修复并解析JSON
        第二层保障：如果解析失败，使用代码沙盒模式兜底
        
        Args:
            json_str: JSON字符串
            data_type: 数据类型（inventory/allocation/supplier）
            original_data: 原始数据，用于兜底时传递给代码沙盒
            prompt: 原始prompt，用于兜底时重新构建
            system_prompt: 系统prompt
            
        Returns:
            解析结果
        """
        # 第一层保障：尝试修复并解析JSON
        result = JSONRepair.repair_json(json_str)
        
        if result is not None:
            logger.info(f"JSON解析成功（第一层保障），数据类型: {data_type}")
            return result

        # 第二层保障：使用代码沙盒模式兜底
        logger.warning(f"JSON解析失败，触发第二层保障（代码沙盒模式），数据类型: {data_type}")
        
        if self.context_manager and original_data:
            try:
                # 调用代码沙盒执行模式
                result = await self.context_manager._sandbox_execution(
                    prompt, 
                    system_prompt, 
                    data_type, 
                    original_data
                )
                logger.info("代码沙盒兜底成功")
                return result
            except Exception as e:
                logger.error(f"代码沙盒兜底失败: {str(e)}")
                raise
        else:
            logger.error("无法执行第二层保障：context_manager未配置或缺少原始数据")
            raise ValueError("JSON解析失败且无法执行兜底")

    def parse_simple(self, json_str: str) -> Optional[Any]:
        """
        简单解析JSON（仅使用第一层保障）
        
        Args:
            json_str: JSON字符串
            
        Returns:
            解析结果，如果无法修复返回None
        """
        return JSONRepair.repair_json(json_str)


# 全局JSON解析器实例
_json_parser = None


def get_json_parser(context_manager=None):
    """获取全局JSON解析器实例"""
    global _json_parser
    if _json_parser is None:
        _json_parser = SmartJSONParser(context_manager)
    return _json_parser


def init_json_parser(context_manager=None):
    """初始化全局JSON解析器"""
    global _json_parser
    _json_parser = SmartJSONParser(context_manager)
    return _json_parser
