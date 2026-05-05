# -*- coding: utf-8 -*-
"""上下文管理器 - 处理超长prompt的分层推理机制"""
import json
import math
from typing import List, Dict, Any, Optional, Callable


class ContextManager:
    """上下文管理器 - 处理超长prompt的分层推理机制"""
    
    # Token估算：1 token ≈ 4 个中文字符
    TOKEN_LIMIT = 64000  # 64k token限制
    TOKEN_PER_CHAR = 0.25  # 每个字符约0.25 token
    SAFETY_MARGIN = 0.8  # 安全边际，使用80%的token
    
    def __init__(self, llm_func, llm_stream_func):
        self.llm_func = llm_func
        self.llm_stream_func = llm_stream_func
    
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
                                            max_retries: int = 3) -> Any:
        """
        使用上下文管理处理超长prompt
        
        Args:
            prompt: 原始prompt
            system_prompt: 系统提示
            stream: 是否流式输出
            max_retries: 最大重试次数
            
        Returns:
            分析结果（流式返回生成器或直接返回结果）
        """
        if not self.is_too_long(prompt):
            # 正常处理，直接调用LLM
            if stream:
                return self.llm_stream_func(prompt, system_prompt)
            else:
                return await self.llm_func(prompt, system_prompt)
        
        # 超长prompt，使用分层推理
        if stream:
            return self._streaming_hierarchical_reasoning(prompt, system_prompt)
        else:
            return await self._hierarchical_reasoning(prompt, system_prompt)
    
    async def _hierarchical_reasoning(self, prompt: str, system_prompt: str) -> str:
        """
        分层推理：先摘要再分析
        
        步骤：
        1. 提取关键数据特征（统计数据已经包含）
        2. 让LLM总结数据摘要
        3. 基于摘要进行最终分析
        """
        # 第一步：生成数据摘要
        summary_prompt = self._build_summary_prompt(prompt)
        summary_result = await self.llm_func(summary_prompt, "你是一个数据分析师，擅长提取和总结关键数据特征。")
        
        # 第二步：基于摘要进行深度分析
        analysis_prompt = self._build_analysis_prompt_with_summary(prompt, summary_result)
        final_result = await self.llm_func(analysis_prompt, system_prompt)
        
        return final_result
    
    async def _streaming_hierarchical_reasoning(self, prompt: str, system_prompt: str):
        """
        流式分层推理
        
        步骤：
        1. 先生成数据摘要（非流式，因为摘要需要完整上下文）
        2. 然后基于摘要进行流式分析
        """
        # 第一步：生成数据摘要（非流式）
        summary_prompt = self._build_summary_prompt(prompt)
        summary_result = await self.llm_func(summary_prompt, "你是一个数据分析师，擅长提取和总结关键数据特征。")
        
        # 输出摘要信息
        yield "\n📊 [数据摘要阶段] 正在总结关键数据特征...\n"
        yield "────────────────────────────────────────\n"
        yield f"{summary_result}\n"
        yield "────────────────────────────────────────\n\n"
        
        # 第二步：基于摘要进行流式分析
        analysis_prompt = self._build_analysis_prompt_with_summary(prompt, summary_result)
        yield "🤖 [深度分析阶段] 正在进行库存分析...\n"
        yield "────────────────────────────────────────\n"
        
        async for chunk in self.llm_stream_func(analysis_prompt, system_prompt):
            yield chunk
    
    def _build_summary_prompt(self, original_prompt: str) -> str:
        """构建数据摘要prompt"""
        return f"""请总结以下库存分析数据的关键特征：

{original_prompt[:5000]}  # 截取前5000字符作为参考

请按照以下格式输出数据摘要：

## 数据摘要

### 基础信息
- 仓库数量：X个
- 物料数量：X个
- 时间范围：XXXX年XX月 - XXXX年XX月

### 库存状况
- 当前库存总量：XXX
- 在途库存总量：XXX
- 实际可用库存：XXX

### 历史出库特征
- 数据记录数：XXX条
- 最高月出库：XXX
- 最低月出库：XXX
- 平均月出库：XXX
- 中位数出库：XXX
- 标准差：XXX
- 同比变化：XXX%
- 环比变化：XXX%
- 季节性特征：XXX

### 关键发现
- 库存充足/紧张的物料比例
- 消耗趋势（增长/下降/稳定）
- 潜在风险点

请用简洁的语言总结，不超过300字。
"""
    
    def _build_analysis_prompt_with_summary(self, original_prompt: str, summary: str) -> str:
        """构建带摘要的分析prompt"""
        # 从原始prompt中提取分析要求部分
        # 移除原始数据，保留分析要求
        analysis_requirements = self._extract_analysis_requirements(original_prompt)
        
        return f"""根据以下数据摘要进行库存分析：

## 数据摘要
{summary}

## 分析要求
{analysis_requirements}

请基于以上数据摘要，进行深度分析并给出专业建议。
"""
    
    def _extract_analysis_requirements(self, prompt: str) -> str:
        """从prompt中提取分析要求部分"""
        # 找到"分析参数说明"或类似部分开始提取
        start_markers = ["## 分析参数说明", "## 水位线分析方法", "## 分析要求"]
        start_idx = -1
        
        for marker in start_markers:
            idx = prompt.find(marker)
            if idx != -1:
                start_idx = idx
                break
        
        if start_idx == -1:
            # 如果找不到标记，返回后半部分
            return prompt[-3000:]
        
        return prompt[start_idx:]
    
    async def batch_process(self, 
                          data_chunks: List[str], 
                          process_func: Callable,
                          summary_prompt: str = "请汇总以下分析结果") -> str:
        """
        分批处理并汇总结果
        
        Args:
            data_chunks: 数据块列表
            process_func: 处理函数
            summary_prompt: 汇总提示
            
        Returns:
            汇总结果
        """
        results = []
        
        for i, chunk in enumerate(data_chunks):
            result = await process_func(chunk)
            results.append({"batch": i + 1, "result": result})
        
        # 汇总所有结果
        if len(results) == 1:
            return results[0]["result"]
        
        # 生成汇总prompt
        results_json = json.dumps(results, ensure_ascii=False)
        final_prompt = f"""{summary_prompt}：

{results_json}

请综合以上分析结果，给出最终结论。
"""
        
        return await self.llm_func(final_prompt, "你是一个数据汇总专家，擅长综合多个分析结果。")


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
