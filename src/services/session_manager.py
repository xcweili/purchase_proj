# -*- coding: utf-8 -*-
"""会话管理器 - 用于管理流式会话的终止状态"""
import asyncio
import logging
import uuid
from typing import Dict, Set, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class SessionState:
    """会话状态"""
    session_id: str
    created_at: datetime
    agent_type: str  # 'allocation', 'inventory', 'supplier'
    is_active: bool = True
    is_cancelled: bool = False
    cancelled_at: Optional[datetime] = None
    # 用于通知生成器停止的事件
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)


class SessionManager:
    """会话管理器 - 单例模式"""
    _instance = None
    _lock = asyncio.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._sessions: Dict[str, SessionState] = {}
        self._cleanup_task: Optional[asyncio.Task] = None
        self._session_timeout = timedelta(minutes=30)  # 会话超时时间

    async def start(self):
        """启动会话管理器，启动清理任务"""
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_expired_sessions())
            logger.info("[SessionManager] 会话清理任务已启动")

    async def stop(self):
        """停止会话管理器"""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            logger.info("[SessionManager] 会话清理任务已停止")

    def create_session(self, agent_type: str) -> str:
        """创建新会话

        Args:
            agent_type: 智能体类型 ('allocation', 'inventory', 'supplier')

        Returns:
            session_id: 会话唯一标识
        """
        session_id = str(uuid.uuid4())
        session = SessionState(
            session_id=session_id,
            created_at=datetime.now(),
            agent_type=agent_type
        )
        self._sessions[session_id] = session
        logger.info(f"[SessionManager] 创建会话: {session_id}, 类型: {agent_type}")
        return session_id

    def get_session(self, session_id: str) -> Optional[SessionState]:
        """获取会话状态"""
        return self._sessions.get(session_id)

    def cancel_session(self, session_id: str) -> bool:
        """取消会话

        Args:
            session_id: 会话ID

        Returns:
            bool: 是否成功取消
        """
        session = self._sessions.get(session_id)
        if not session:
            logger.warning(f"[SessionManager] 会话不存在: {session_id}")
            return False

        if session.is_cancelled:
            logger.info(f"[SessionManager] 会话已处于取消状态: {session_id}")
            return True

        session.is_cancelled = True
        session.cancelled_at = datetime.now()
        session.cancel_event.set()
        logger.info(f"[SessionManager] 会话已取消: {session_id}, 类型: {session.agent_type}")
        return True

    def is_session_cancelled(self, session_id: str) -> bool:
        """检查会话是否已被取消"""
        session = self._sessions.get(session_id)
        return session.is_cancelled if session else True

    def remove_session(self, session_id: str):
        """移除会话"""
        if session_id in self._sessions:
            del self._sessions[session_id]
            logger.info(f"[SessionManager] 会话已移除: {session_id}")

    def get_cancel_event(self, session_id: str) -> Optional[asyncio.Event]:
        """获取会话的取消事件"""
        session = self._sessions.get(session_id)
        return session.cancel_event if session else None

    async def _cleanup_expired_sessions(self):
        """定期清理过期会话"""
        while True:
            try:
                await asyncio.sleep(60)  # 每分钟检查一次
                now = datetime.now()
                expired_sessions = []

                for session_id, session in self._sessions.items():
                    # 检查是否超时或已完成很久
                    if now - session.created_at > self._session_timeout:
                        expired_sessions.append(session_id)
                    # 如果已取消超过5分钟，也清理掉
                    elif session.is_cancelled and session.cancelled_at:
                        if now - session.cancelled_at > timedelta(minutes=5):
                            expired_sessions.append(session_id)

                for session_id in expired_sessions:
                    self.remove_session(session_id)
                    logger.info(f"[SessionManager] 清理过期会话: {session_id}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[SessionManager] 清理会话时出错: {str(e)}")


# 全局会话管理器实例
session_manager = SessionManager()
