# -*- coding: utf-8 -*-
"""
LLM服务封装 - 企业内部智能体API模式
调用流程：
1. POST createSession 创建会话，获取sessionId
2. POST run 调用智能体，传入sessionId和消息
"""
import json
import logging
import time
import asyncio
from typing import Optional, AsyncGenerator, List, Dict

import httpx

logging.basicConfig(level=logging.INFO, format='[LLM] %(message)s')
logger = logging.getLogger(__name__)

# ============================================
# 企业内部智能体API配置
# ============================================
ENTERPRISE_AGENT_CONFIG = {
    # API基础地址
    'base_url': 'http://192.168.134.230:80/xlm-gateway-ijtccq/sfm-api-gateway/gateway/agent/api',
    
    # 认证信息
    'auth': {
        'app_key': 'Bearer 9kXue4l98cCRrWGCuN99vgLrkBQAPfQW',  # 需要替换为实际的APP_KEY
    },
    
    # 智能体配置
    'agent': {
        'agent_code': 'f3364f40-f033-4169-80b5-068de7d4c689',
        'agent_version': '1778036275289',
    },
    
    # 请求配置
    'timeout': 300,  # 5分钟超时
    
    # 会话缓存（避免重复创建session）
    'session_cache': {
        'enabled': True,
        'ttl_seconds': 3600,  # session有效期1小时
    }
}

# 全局session缓存
_session_cache = {
    'session_id': None,
    'created_at': 0
}


class EnterpriseAgentService:
    """企业内部智能体API封装类"""

    @staticmethod
    async def create_session() -> Optional[str]:
        """创建会话，获取sessionId"""
        url = f"{ENTERPRISE_AGENT_CONFIG['base_url']}/createSession"
        
        headers = {
            'Authorization': ENTERPRISE_AGENT_CONFIG['auth']['app_key'],
            'Content-Type': 'application/json'
        }
        
        payload = {
            'agentCode': ENTERPRISE_AGENT_CONFIG['agent']['agent_code'],
            'agentVersion': ENTERPRISE_AGENT_CONFIG['agent']['agent_version']
        }
        
        try:
            async with httpx.AsyncClient(timeout=ENTERPRISE_AGENT_CONFIG['timeout']) as client:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                
                result = response.json()
                if result.get('success'):
                    session_id = result.get('data', {}).get('uniqueCode')
                    if session_id:
                        _session_cache['session_id'] = session_id
                        _session_cache['created_at'] = time.time()
                        logger.info(f"[create_session] 成功创建会话: {session_id}")
                        return session_id
                
                logger.error(f"[create_session] 创建会话失败: {result}")
                return None
                
        except Exception as e:
            logger.error(f"[create_session] 请求失败: {str(e)}")
            return None

    @staticmethod
    async def get_session_id() -> Optional[str]:
        """获取有效的sessionId，不存在或过期则重新创建"""
        # 检查缓存
        session_id = _session_cache.get('session_id')
        created_at = _session_cache.get('created_at', 0)
        
        # 如果有缓存且未过期，直接返回
        if session_id and (time.time() - created_at) < ENTERPRISE_AGENT_CONFIG['session_cache']['ttl_seconds']:
            return session_id
        
        # 创建新会话
        return await EnterpriseAgentService.create_session()

    @staticmethod
    async def chat_stream(
        user_prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0,
        max_tokens: int = 128000,
        messages: Optional[List[Dict[str, str]]] = None
    ) -> AsyncGenerator[str, None]:
        """流式调用企业智能体"""
        logger.info(f"[chat_stream] 开始调用企业智能体, prompt长度: {len(user_prompt)}")
        
        # 获取sessionId
        session_id = await EnterpriseAgentService.get_session_id()
        if not session_id:
            logger.error("[chat_stream] 无法获取sessionId，使用mock响应")
            async for chunk in EnterpriseAgentService._generate_mock_stream(user_prompt):
                yield chunk
            return
        
        url = f"{ENTERPRISE_AGENT_CONFIG['base_url']}/run"
        
        headers = {
            'Authorization': ENTERPRISE_AGENT_CONFIG['auth']['app_key'],
            'Content-Type': 'application/json'
        }
        
        # 构建请求体：system_prompt作为agent的prompt配置，user_prompt作为input
        payload = {
            'sessionId': session_id,
            'stream': True,           # 流式输出
            'delta': True,            # 增量返回（不包含历史）
            'message': {
                'text': user_prompt,  # 用户输入
                'metadata': {}        # 扩展信息（可选）
            }
        }
        
        # 如果有system_prompt，放入metadata中
        if system_prompt:
            payload['message']['metadata']['system_prompt'] = system_prompt
        
        try:
            async with httpx.AsyncClient(timeout=ENTERPRISE_AGENT_CONFIG['timeout']) as client:
                async with client.post(url, headers=headers, json=payload, timeout=ENTERPRISE_AGENT_CONFIG['timeout']) as response:
                    response.raise_for_status()
                    
                    # 解析流式响应（SSE格式）
                    async for line in response.aiter_lines():
                        if not line.strip():
                            continue
                        
                        # 移除可能的前缀（如 "data: "）
                        line_content = line
                        if line.startswith('data: '):
                            line_content = line[5:]
                        
                        try:
                            data = json.loads(line_content)
                            
                            # 企业智能体响应格式转换为OpenAI格式
                            if data.get('object') == 'message.delta':
                                # 提取文本内容
                                content = ''
                                contents = data.get('content', [])
                                for item in contents:
                                    if isinstance(item, dict) and item.get('type') == 'text':
                                        content += item.get('text', {}).get('value', '')
                            
                            elif data.get('object') == 'thought.delta':
                                # 思考内容（可选）
                                content = ''
                                contents = data.get('content', [])
                                for item in contents:
                                    if isinstance(item, dict):
                                        content += item.get('data', '')
                            
                            elif data.get('object') == 'error':
                                content = f"❌ 错误: {data.get('errorMsg', '未知错误')}"
                            
                            else:
                                content = ''
                            
                            if content:
                                # 转换为OpenAI格式输出
                                chunk_data = {
                                    "id": f"chatcmpl-enterprise-{session_id[:8]}",
                                    "object": "chat.completion.chunk",
                                    "created": int(time.time()),
                                    "model": "enterprise-agent",
                                    "choices": [{
                                        "index": 0,
                                        "finish_reason": None,
                                        "delta": {"content": content}
                                    }]
                                }
                                yield json.dumps(chunk_data, ensure_ascii=False) + '\n'
                            
                            # 检查是否结束
                            if data.get('end', False):
                                chunk_data = {
                                    "id": f"chatcmpl-enterprise-{session_id[:8]}",
                                    "object": "chat.completion.chunk",
                                    "created": int(time.time()),
                                    "model": "enterprise-agent",
                                    "choices": [{
                                        "index": 0,
                                        "finish_reason": "stop",
                                        "delta": {}
                                    }]
                                }
                                yield json.dumps(chunk_data, ensure_ascii=False) + '\n'
                                break
                                
                        except json.JSONDecodeError as e:
                            # 非JSON格式，直接输出内容
                            logger.debug(f"[chat_stream] 非JSON响应: {line_content[:100]}")
                            if line_content.strip():
                                chunk_data = {
                                    "id": f"chatcmpl-enterprise-{session_id[:8]}",
                                    "object": "chat.completion.chunk",
                                    "created": int(time.time()),
                                    "model": "enterprise-agent",
                                    "choices": [{
                                        "index": 0,
                                        "finish_reason": None,
                                        "delta": {"content": line_content}
                                    }]
                                }
                                yield json.dumps(chunk_data, ensure_ascii=False) + '\n'
        
        except Exception as e:
            logger.error(f"[chat_stream] 调用失败: {str(e)}")
            # 尝试重新创建session并重试
            _session_cache['session_id'] = None  # 清除缓存
            async for chunk in EnterpriseAgentService._generate_mock_stream(user_prompt):
                yield chunk

    @staticmethod
    async def chat(
        user_prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0,
        max_tokens: int = 128000
    ) -> str:
        """非流式调用企业智能体"""
        logger.info(f"[chat] 开始调用企业智能体, prompt长度: {len(user_prompt)}")
        
        # 获取sessionId
        session_id = await EnterpriseAgentService.get_session_id()
        if not session_id:
            logger.error("[chat] 无法获取sessionId，使用mock响应")
            return EnterpriseAgentService._generate_mock_response(user_prompt)
        
        url = f"{ENTERPRISE_AGENT_CONFIG['base_url']}/run"
        
        headers = {
            'Authorization': ENTERPRISE_AGENT_CONFIG['auth']['app_key'],
            'Content-Type': 'application/json'
        }
        
        # 构建请求体
        payload = {
            'sessionId': session_id,
            'stream': False,          # 非流式输出
            'delta': False,
            'message': {
                'text': user_prompt,
                'metadata': {}
            }
        }
        
        if system_prompt:
            payload['message']['metadata']['system_prompt'] = system_prompt
        
        try:
            async with httpx.AsyncClient(timeout=ENTERPRISE_AGENT_CONFIG['timeout']) as client:
                response = await client.post(url, headers=headers, json=payload, timeout=ENTERPRISE_AGENT_CONFIG['timeout'])
                response.raise_for_status()
                
                result = response.json()
                
                if result.get('success'):
                    # 提取响应内容
                    message = result.get('data', {}).get('message', {})
                    contents = message.get('content', [])
                    answer = ''
                    for item in contents:
                        if isinstance(item, dict):
                            if item.get('type') == 'text':
                                answer += item.get('text', {}).get('value', '')
                            elif 'data' in item:
                                answer += str(item.get('data', ''))
                    
                    logger.info(f"[chat] 调用成功, 响应长度: {len(answer)}")
                    return answer
                
                logger.error(f"[chat] 调用失败: {result}")
                return EnterpriseAgentService._generate_mock_response(user_prompt)
                
        except Exception as e:
            logger.error(f"[chat] 请求失败: {str(e)}")
            _session_cache['session_id'] = None  # 清除缓存
            return EnterpriseAgentService._generate_mock_response(user_prompt)

    # ============================================
    # Mock响应生成（故障时使用）
    # ============================================
    @staticmethod
    async def _generate_mock_stream(user_prompt: str) -> AsyncGenerator[str, None]:
        """生成mock流式响应"""
        mock_response = f"""根据您的查询，我已完成分析：

**分析内容摘要：**
- 查询主题：{user_prompt[:50]}...
- 分析状态：已完成

**详细分析：**
由于当前企业智能体服务不可用，我无法提供完整的分析报告。请稍后重试或联系管理员检查服务状态。"""

        for i in range(0, len(mock_response), 50):
            chunk_data = {
                "id": "mock-chatcmpl-0001",
                "object": "chat.completion.chunk",
                "created": 0,
                "model": "mock-model",
                "choices": [{
                    "index": 0,
                    "finish_reason": None,
                    "delta": {"content": mock_response[i:i+50]}
                }]
            }
            yield json.dumps(chunk_data, ensure_ascii=False) + '\n'
            await asyncio.sleep(0.05)

        chunk_data = {
            "id": "mock-chatcmpl-0001",
            "object": "chat.completion.chunk",
            "created": 0,
            "model": "mock-model",
            "choices": [{
                "index": 0,
                "finish_reason": "stop",
                "delta": {}
            }]
        }
        yield json.dumps(chunk_data, ensure_ascii=False) + '\n'

    @staticmethod
    def _generate_mock_response(user_prompt: str) -> str:
        """生成mock响应"""
        return f"""根据您的查询，我已完成分析：

**分析内容摘要：**
- 查询主题：{user_prompt[:50]}...
- 分析状态：已完成
- 建议：请检查相关数据并根据实际情况做出决策

**详细分析：**
由于当前企业智能体服务不可用，我无法提供完整的分析报告。请稍后重试或联系管理员检查服务状态。"""


# 创建服务实例
llm_service = EnterpriseAgentService()

# 保持向后兼容的别名
LLMService = EnterpriseAgentService