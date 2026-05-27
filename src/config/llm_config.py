# -*- coding: utf-8 -*-
"""
LLM服务配置 - 支持多后端切换
"""
from typing import Dict, Optional


class LLMProviderConfig:
    """单个LLM提供商的配置"""
    def __init__(
        self,
        name: str,
        api_key: str,
        base_url: str,
        model: str,
        timeout: int = 60,
        max_tokens: int = 128000,
        temperature: float = 0.95
    ):
        self.name = name
        self.api_key = api_key
        self.base_url = base_url
        self.model = model
        self.timeout = timeout
        self.max_tokens = max_tokens
        self.temperature = temperature


class LLMConfig:
    """LLM服务配置"""
    def __init__(self):
        # 当前使用的LLM提供商
        self.current_provider: str = "deepseek"
        # self.current_provider: str = "tongyi"
        
        # 各提供商配置
        self.providers: Dict[str, LLMProviderConfig] = {
            "deepseek": LLMProviderConfig(
                name="deepseek",
                api_key="sk-3f9a72b998164fd989a0c1b6df844669",
                base_url="https://api.deepseek.com",
                model="deepseek-v4-flash",
            ),
            "tongyi": LLMProviderConfig(
                name="tongyi",
                api_key="7dee7bb6b44242538317ee52c544e0a4",
                base_url="http://25.212.230.14:80/lmp-cloud-ias-server/api/LLm/chat/completions/V2",
                model="通义千问2.5-72B",
                temperature=0.95,
            ),
        }


# 全局配置实例
llm_config = LLMConfig()


def get_current_provider() -> Optional[LLMProviderConfig]:
    """获取当前配置的LLM提供商"""
    return llm_config.providers.get(llm_config.current_provider)


def set_provider(provider_name: str) -> bool:
    """切换LLM提供商"""
    if provider_name in llm_config.providers:
        llm_config.current_provider = provider_name
        return True
    return False
