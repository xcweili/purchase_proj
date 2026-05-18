# -*- coding: utf-8 -*-
"""
LLM服务封装 - 支持多后端切换和快速中断
"""
import json
import logging
import time
import asyncio
import threading
from typing import Optional, AsyncGenerator, List, Dict

logging.basicConfig(level=logging.INFO, format='[LLM] %(message)s')
logger = logging.getLogger(__name__)

# 导入配置
from ..config.llm_config import get_current_provider, llm_config

# 延迟初始化客户端
_clients = {}


def get_client(provider_name: str = None):
    """延迟获取客户端"""
    provider_name = provider_name or llm_config.current_provider
    
    if provider_name not in _clients:
        try:
            import openai
            provider_config = llm_config.providers.get(provider_name)
            if provider_config:
                # 只有deepseek使用OpenAI SDK
                if provider_name == "deepseek":
                    _clients[provider_name] = openai.Client(
                        api_key=provider_config.api_key,
                        base_url=provider_config.base_url,
                    )
                    logger.info(f"[LLM] {provider_name} 客户端初始化成功")
                else:
                    # 其他提供商使用None，表示使用原生HTTP调用
                    _clients[provider_name] = None
            else:
                logger.error(f"[LLM] 未找到提供商配置: {provider_name}")
                _clients[provider_name] = None
        except Exception as e:
            logger.error(f"[LLM] {provider_name} 客户端初始化失败: {str(e)}")
            _clients[provider_name] = None
    return _clients[provider_name]


def parse_tongyi_stream_response(chunk_str: str) -> Optional[str]:
    """
    解析通义千问流式响应格式
    输入格式: data:{"id":"xxx","appId":"xxx","globalTraceId":"xxx","object":"chat.completion.chunk","created":xxx,"choices":[...],"logprobs":null,"index":0,"delta":{"role":"assistant","content":"xxx"},"isSentitiveWord":false}
    
    转换为标准格式: {"id":"xxx","object":"chat.completion.chunk","created":xxx,"model":"xxx","choices":[{"index":0,"finish_reason":null,"delta":{"content":"xxx"}}]}
    """
    try:
        # 移除 data: 前缀
        if chunk_str.startswith("data:"):
            chunk_str = chunk_str[5:]
        
        # 处理 [DONE] 标记
        if chunk_str.strip() == "[DONE]":
            return json.dumps({
                "id": "tongyi-chatcmpl-0001",
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": "通义千问2.5-72B",
                "choices": [{
                    "index": 0,
                    "finish_reason": "stop",
                    "delta": {}
                }]
            }, ensure_ascii=False)
        
        data = json.loads(chunk_str)
        
        # 提取关键信息
        result = {
            "id": data.get("id", "tongyi-chatcmpl-0001"),
            "object": data.get("object", "chat.completion.chunk"),
            "created": data.get("created", int(time.time())),
            "model": data.get("model", "通义千问2.5-72B"),
            "choices": []
        }
        
        choices = data.get("choices", [])
        if isinstance(choices, list) and len(choices) > 0:
            choice = choices[0]
            delta = choice.get("delta", {})
            # 如果delta是字符串，尝试解析
            if isinstance(delta, str):
                try:
                    delta = json.loads(delta)
                except:
                    delta = {"content": delta}
            
            result["choices"].append({
                "index": choice.get("index", 0),
                "finish_reason": choice.get("finish_reason", None),
                "delta": {
                    "content": delta.get("content", "")
                }
            })
        else:
            # 某些响应格式中delta可能在顶层
            delta = data.get("delta", {})
            if isinstance(delta, str):
                try:
                    delta = json.loads(delta)
                except:
                    delta = {"content": delta}
            
            result["choices"].append({
                "index": data.get("index", 0),
                "finish_reason": None,
                "delta": {
                    "content": delta.get("content", "")
                }
            })
        
        return json.dumps(result, ensure_ascii=False)
    
    except Exception as e:
        logger.error(f"[LLM] 解析通义千问响应失败: {str(e)}, 原始数据: {chunk_str[:200]}")
        return None


async def tongyi_stream_request(provider_config, messages, temperature, max_tokens, queue, cancel_event):
    """使用原生HTTP请求调用通义千问流式API"""
    import aiohttp
    
    url = provider_config.base_url
    headers = {
        "Authorization": provider_config.api_key,
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": provider_config.model,
        "messages": messages,
        "stream": True,
        "temperature": temperature,
        "top_p": 0.7,
        "presence_penalty": 1,
        "modelVersion": ""
    }
    
    if max_tokens:
        payload["max_tokens"] = max_tokens
    
    logger.info(f"[tongyi_stream] 发起请求: {url}")
    
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=provider_config.timeout)) as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"[tongyi_stream] 请求失败: {response.status}, {error_text}")
                    queue.put_nowait((False, f"请求失败: {response.status}"))
                    return
                
                async for chunk in response.content.iter_chunks():
                    if cancel_event and cancel_event.is_set():
                        logger.info("[tongyi_stream] 检测到取消信号")
                        break
                    
                    if chunk[1]:
                        chunk_str = chunk[1].decode('utf-8', errors='ignore')
                        parsed = parse_tongyi_stream_response(chunk_str)
                        if parsed:
                            await queue.put((True, parsed + '\n'))
        
        await queue.put((False, None))
    except asyncio.CancelledError:
        logger.info("[tongyi_stream] 请求被取消")
        await queue.put((False, "请求被取消"))
    except Exception as e:
        logger.error(f"[tongyi_stream] 请求异常: {str(e)}")
        await queue.put((False, str(e)))


async def tongyi_non_stream_request(provider_config, messages, temperature, max_tokens):
    """使用原生HTTP请求调用通义千问非流式API"""
    import aiohttp
    
    url = provider_config.base_url
    headers = {
        "Authorization": provider_config.api_key,
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": provider_config.model,
        "messages": messages,
        "stream": False,
        "temperature": temperature,
        "top_p": 0.7,
        "presence_penalty": 1,
        "modelVersion": ""
    }
    
    if max_tokens:
        payload["max_tokens"] = max_tokens
    
    logger.info(f"[tongyi_chat] 发起请求: {url}")
    
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=provider_config.timeout)) as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"[tongyi_chat] 请求失败: {response.status}, {error_text}")
                    return None
                
                data = await response.json()
                if data.get("choices") and len(data["choices"]) > 0:
                    message = data["choices"][0].get("message", {})
                    return message.get("content", "")
        
        return None
    except Exception as e:
        logger.error(f"[tongyi_chat] 请求异常: {str(e)}")
        return None


class LLMService:
    """LLM服务封装类 - 支持多后端切换和快速中断"""

    @staticmethod
    async def chat_stream(
        user_prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = None,
        max_tokens: int = None,
        messages: Optional[List[Dict[str, str]]] = None,
        cancel_event: Optional[asyncio.Event] = None,
        provider: Optional[str] = None
    ) -> AsyncGenerator[str, None]:
        """流式调用LLM模型 - 支持多后端切换和快速中断

        Args:
            user_prompt: 用户提示
            system_prompt: 系统提示
            temperature: 温度参数
            max_tokens: 最大token数
            messages: 消息列表
            cancel_event: 取消事件，用于中途取消请求
            provider: 指定LLM提供商，不指定则使用配置的默认提供商
        """
        current_provider = provider or llm_config.current_provider
        provider_config = llm_config.providers.get(current_provider)
        
        if not provider_config:
            logger.error(f"[chat_stream] 未找到提供商配置: {current_provider}")
            async for chunk in LLMService._mock_response_generator(user_prompt):
                yield chunk
            return
        
        default_system = system_prompt or "你是一个专业的采购管理分析助手，擅长库存预警、供应商匹配和仓库调配分析。"
        logger.info(f"[chat_stream] 开始调用LLM, provider: {current_provider}, model: {provider_config.model}, prompt长度: {len(user_prompt)} 字符")

        start_time = time.time()
        temp = temperature if temperature is not None else provider_config.temperature
        tokens = max_tokens if max_tokens is not None else provider_config.max_tokens

        # 构建消息列表
        if messages:
            msg_list = [{"role": "system", "content": default_system}]
            msg_list.extend(messages)
            msg_list.append({"role": "user", "content": user_prompt})
        else:
            msg_list = [
                {"role": "system", "content": default_system},
                {"role": "user", "content": user_prompt},
            ]

        # 通义千问使用原生HTTP调用
        if current_provider == "tongyi":
            async for chunk in LLMService._tongyi_chat_stream(msg_list, temp, tokens, provider_config, cancel_event):
                yield chunk
            elapsed = time.time() - start_time
            logger.info(f"[chat_stream] LLM调用完成, 耗时: {elapsed:.2f} 秒")
            return

        # 其他提供商使用OpenAI SDK
        client = get_client(current_provider)
        if not client:
            logger.warning(f"[chat_stream] {current_provider} 客户端不可用，返回模拟响应")
            async for chunk in LLMService._mock_response_generator(user_prompt):
                yield chunk
            return

        queue = asyncio.Queue()
        stream_closed = False

        def stream_generator():
            nonlocal stream_closed
            try:
                stream = client.chat.completions.create(
                    model=provider_config.model,
                    messages=msg_list,
                    stream=True,
                    temperature=temp,
                    max_tokens=tokens,
                )

                for chunk in stream:
                    if cancel_event and cancel_event.is_set():
                        logger.info("[chat_stream] 检测到取消信号，停止处理LLM响应")
                        break

                    try:
                        chunk_dict = chunk.dict()
                        choices = chunk_dict.get('choices')
                        if not choices or len(choices) == 0:
                            continue

                        delta = choices[0].get('delta', {})

                        reasoning_content = delta.get('reasoning', '')
                        actual_content = delta.get('content', '')

                        if reasoning_content:
                            delta['content'] = reasoning_content
                            delta['reasoning'] = None
                            choices[0]['delta'] = delta
                        
                        queue.put_nowait((True, json.dumps(chunk_dict, ensure_ascii=False) + '\n'))
                    
                    except Exception as e:
                        logger.error(f"[chat_stream] 处理chunk失败: {str(e)}, chunk: {chunk}")
                        continue

                stream_closed = True
                queue.put_nowait((False, None))
            except Exception as e:
                logger.error(f"[chat_stream] 线程流式请求失败: {str(e)}")
                stream_closed = True
                queue.put_nowait((False, str(e)))

        try:
            if cancel_event and cancel_event.is_set():
                logger.info("[chat_stream] 调用前已检测到取消信号")
                return

            logger.info(f"[chat_stream] 启动线程执行流式请求")
            thread = threading.Thread(target=stream_generator, daemon=True)
            thread.start()

            while True:
                try:
                    success, data = await asyncio.wait_for(queue.get(), timeout=1.0)
                    if success:
                        yield data
                    else:
                        if data:
                            if cancel_event and cancel_event.is_set():
                                logger.info("[chat_stream] 流式请求因取消而终止")
                            else:
                                logger.error(f"[chat_stream] 流式请求出错: {data}")
                        break
                except asyncio.CancelledError:
                    logger.info("[chat_stream] 流式生成器被取消")
                    if cancel_event:
                        cancel_event.set()
                    break
                except asyncio.TimeoutError:
                    if cancel_event and cancel_event.is_set():
                        logger.info("[chat_stream] 等待时检测到取消信号")
                        break

            elapsed = time.time() - start_time
            logger.info(f"[chat_stream] LLM调用完成, 耗时: {elapsed:.2f} 秒")

        except asyncio.TimeoutError:
            logger.error(f"[chat_stream] 流式请求超时")
        except Exception as e:
            logger.error(f"[chat_stream] LLM服务调用失败: {str(e)}")
            async for chunk in LLMService._mock_response_generator(user_prompt):
                yield chunk

    @staticmethod
    async def _tongyi_chat_stream(msg_list, temperature, max_tokens, provider_config, cancel_event):
        """通义千问流式调用封装"""
        queue = asyncio.Queue()
        
        # 启动异步请求任务
        task = asyncio.create_task(
            tongyi_stream_request(provider_config, msg_list, temperature, max_tokens, queue, cancel_event)
        )
        
        try:
            while True:
                try:
                    success, data = await asyncio.wait_for(queue.get(), timeout=1.0)
                    if success:
                        yield data
                    else:
                        if data:
                            if cancel_event and cancel_event.is_set():
                                logger.info("[_tongyi_chat_stream] 流式请求因取消而终止")
                            else:
                                logger.error(f"[_tongyi_chat_stream] 流式请求出错: {data}")
                        break
                except asyncio.CancelledError:
                    logger.info("[_tongyi_chat_stream] 流式生成器被取消")
                    if cancel_event:
                        cancel_event.set()
                    task.cancel()
                    break
                except asyncio.TimeoutError:
                    if cancel_event and cancel_event.is_set():
                        logger.info("[_tongyi_chat_stream] 等待时检测到取消信号")
                        task.cancel()
                        break
        finally:
            if not task.done():
                task.cancel()

    @staticmethod
    async def _mock_response_generator(user_prompt: str):
        """生成模拟响应"""
        mock_response = f"""根据您的查询，我已完成分析：

**分析内容摘要：**
- 查询主题：{user_prompt[:50]}...
- 分析状态：已完成
- 建议：请检查相关数据并根据实际情况做出决策

**详细分析：**
由于当前LLM服务不可用，我无法提供完整的分析报告。请稍后重试或联系管理员检查服务状态。

如需进一步帮助，请提供更多详细信息。"""

        for i in range(0, len(mock_response), 50):
            chunk_data = {
                "id": "mock-chatcmpl-0001",
                "object": "chat.completion.chunk",
                "created": 0,
                "model": "mock-model",
                "choices": [{
                    "index": 0,
                    "finish_reason": None,
                    "delta": {
                        "content": mock_response[i:i+50]
                    }
                }]
            }
            yield json.dumps(chunk_data, ensure_ascii=False)
            await asyncio.sleep(0.1)

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
        yield json.dumps(chunk_data, ensure_ascii=False)

    @staticmethod
    async def chat(
        user_prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = None,
        max_tokens: int = None,
        provider: Optional[str] = None
    ) -> str:
        """非流式调用LLM模型"""
        current_provider = provider or llm_config.current_provider
        provider_config = llm_config.providers.get(current_provider)
        
        if not provider_config:
            logger.error(f"[chat] 未找到提供商配置: {current_provider}")
            return LLMService._mock_response_text(user_prompt)
        
        default_system = system_prompt or "你是一个专业的采购管理分析助手，擅长库存预警、供应商匹配和仓库调配分析。"
        logger.info(f"[chat] 开始调用LLM, provider: {current_provider}, model: {provider_config.model}, prompt长度: {len(user_prompt)} 字符")

        start_time = time.time()
        temp = temperature if temperature is not None else provider_config.temperature
        tokens = max_tokens if max_tokens is not None else provider_config.max_tokens

        # 构建消息列表
        msg_list = [
            {"role": "system", "content": default_system},
            {"role": "user", "content": user_prompt},
        ]

        # 通义千问使用原生HTTP调用
        if current_provider == "tongyi":
            result = await tongyi_non_stream_request(provider_config, msg_list, temp, tokens)
            if result:
                elapsed = time.time() - start_time
                logger.info(f"[chat] LLM调用完成, 耗时: {elapsed:.2f} 秒, 响应长度: {len(result)} 字符")
                return result
            logger.warning(f"[chat] {current_provider} 调用失败，返回模拟响应")
            return LLMService._mock_response_text(user_prompt)

        # 其他提供商使用OpenAI SDK
        client = get_client(current_provider)
        if not client:
            logger.warning(f"[chat] {current_provider} 客户端不可用，返回模拟响应")
            return LLMService._mock_response_text(user_prompt)

        try:
            response = client.chat.completions.create(
                model=provider_config.model,
                messages=msg_list,
                stream=False,
                temperature=temp,
                max_tokens=tokens,
            )

            elapsed = time.time() - start_time
            result = response.choices[0].message.content if response.choices else ""
            logger.info(f"[chat] LLM调用完成, 耗时: {elapsed:.2f} 秒, 响应长度: {len(result)} 字符")

            return result

        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"[chat] LLM服务调用失败, 耗时: {elapsed:.2f} 秒, 错误: {str(e)}")
            return LLMService._mock_response_text(user_prompt)

    @staticmethod
    def _mock_response_text(user_prompt: str) -> str:
        """生成模拟响应文本"""
        return f"""根据您的查询，我已完成分析：

**分析内容摘要：**
- 查询主题：{user_prompt[:50]}...
- 分析状态：已完成
- 建议：请检查相关数据并根据实际情况做出决策

**详细分析：**
由于当前LLM服务不可用，我无法提供完整的分析报告。请稍后重试或联系管理员检查服务状态。

如需进一步帮助，请提供更多详细信息。"""


llm_service = LLMService()
