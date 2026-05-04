# -*- coding: utf-8 -*-
"""
LLM服务封装 - 基于OpenAI SDK
"""
import openai
import json
import logging
import time
import asyncio
import threading
from typing import Optional, AsyncGenerator, List, Dict

logging.basicConfig(level=logging.INFO, format='[LLM] %(message)s')
logger = logging.getLogger(__name__)

# client = openai.Client(
#     api_key="gpustack_ddb0c780dd843b12_67fea5d3d141e2f75091b6ba6e495707",
#     base_url="http://10.255.216.2/v1",
# )
# model = "qwen3.5-122b-a10b-fp8"

client = openai.Client(
    api_key="sk-3f9a72b998164fd989a0c1b6df844669",
    base_url="https://api.deepseek.com",
)
model = "deepseek-v4-flash"


class LLMService:
    """LLM服务封装类"""

    @staticmethod
    async def chat_stream(
        user_prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0,
        max_tokens: int = 128000,
        messages: Optional[List[Dict[str, str]]] = None
    ) -> AsyncGenerator[str, None]:
        """流式调用LLM模型 - 真正的实时流式输出，同时解析reasoning和content"""
        default_system = system_prompt or "你是一个专业的采购管理分析助手，擅长库存预警、供应商匹配和仓库调配分析。"
        logger.info(f"[chat_stream] 开始调用LLM, prompt长度: {len(user_prompt)} 字符, model: {model}")

        start_time = time.time()
        queue = asyncio.Queue()

        def stream_generator():
            """在线程中执行同步流式请求"""
            try:
                if messages:
                    msg_list = [{"role": "system", "content": default_system}]
                    msg_list.extend(messages)
                    msg_list.append({"role": "user", "content": user_prompt})
                else:
                    msg_list = [
                        {"role": "system", "content": default_system},
                        {"role": "user", "content": user_prompt},
                    ]

                stream = client.chat.completions.create(
                    model=model,
                    messages=msg_list,
                    stream=True,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )

                for chunk in stream:
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

                queue.put_nowait((False, None))
            except Exception as e:
                logger.error(f"[chat_stream] 线程流式请求失败: {str(e)}")
                queue.put_nowait((False, str(e)))

        try:
            logger.info(f"[chat_stream] 启动线程执行流式请求")
            thread = threading.Thread(target=stream_generator, daemon=True)
            thread.start()

            while True:
                success, data = await asyncio.wait_for(queue.get(), timeout=120.0)
                if success:
                    yield data
                else:
                    if data:
                        logger.error(f"[chat_stream] 流式请求出错: {data}")
                    break

            elapsed = time.time() - start_time
            logger.info(f"[chat_stream] LLM调用完成, 耗时: {elapsed:.2f} 秒")

        except asyncio.TimeoutError:
            logger.error(f"[chat_stream] 流式请求超时")
        except Exception as e:
            logger.error(f"[chat_stream] LLM服务调用失败: {str(e)}")
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
        temperature: float = 0,
        max_tokens: int = 128000
    ) -> str:
        """非流式调用LLM模型"""
        default_system = system_prompt or "你是一个专业的采购管理分析助手，擅长库存预警、供应商匹配和仓库调配分析。"
        logger.info(f"[chat] 开始调用LLM, prompt长度: {len(user_prompt)} 字符, model: {model}")

        start_time = time.time()

        try:
            # logger.info(f"[chat] 准备发送请求")
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": default_system},
                    {"role": "user", "content": user_prompt},
                ],
                stream=False,
                temperature=temperature,
                max_tokens=max_tokens,
            )

            elapsed = time.time() - start_time
            logger.info(f"[chat] LLM调用完成, 耗时: {elapsed:.2f} 秒, 响应长度: {len(response.choices[0].message.content) if response.choices else 0} 字符")

            return response.choices[0].message.content

        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"[chat] LLM服务调用失败, 耗时: {elapsed:.2f} 秒, 错误: {str(e)}")
            return f"""根据您的查询，我已完成分析：

**分析内容摘要：**
- 查询主题：{user_prompt[:50]}...
- 分析状态：已完成
- 建议：请检查相关数据并根据实际情况做出决策

**详细分析：**
由于当前LLM服务不可用，我无法提供完整的分析报告。请稍后重试或联系管理员检查服务状态。

如需进一步帮助，请提供更多详细信息。"""


llm_service = LLMService()
