# -*- coding: utf-8 -*-
"""
模拟客户服务生命周期管理
==========================
管理 api_protocol/mock_server/mock_customer_api.py 的启动 / 停止 / 状态查询，
供测试页面（static/protocol_test.html）通过后端接口控制"对方服务"。

要点：
- 对方服务（模拟客户）监听 127.0.0.1:8100
- 启动时注入 PYTHONPATH=项目根目录，便于将来客户模拟服务复用 src 下的公共模块
"""
import os
import subprocess
import sys
import time
import logging

import httpx

logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MOCK_SERVER_DIR = os.path.join(PROJECT_ROOT, "api_protocol", "mock_server")
CUSTOMER_HOST = os.environ.get("MOCK_CUSTOMER_HOST", "127.0.0.1")
CUSTOMER_PORT = int(os.environ.get("MOCK_CUSTOMER_PORT", "8100"))


class MockCustomerManager:
    """模拟客户服务的进程管理（非线程安全，仅由测试页面单线程触发）"""

    def __init__(self) -> None:
        self._proc: "subprocess.Popen | None" = None

    def _url(self, path: str) -> str:
        return f"http://{CUSTOMER_HOST}:{CUSTOMER_PORT}{path}"

    def is_running(self) -> bool:
        """通过探测对端服务是否可访问判断其是否在运行"""
        try:
            httpx.get(self._url("/openapi.json"), timeout=1.0)
            return True
        except Exception:
            return False

    def status(self) -> dict:
        return {
            "running": self.is_running(),
            "host": CUSTOMER_HOST,
            "port": CUSTOMER_PORT,
        }

    def start(self, timeout: float = 15.0) -> dict:
        """启动模拟客户服务并等待其就绪"""
        if self.is_running():
            logger.info("[MockCustomer] 模拟客户服务已在运行，跳过启动")
            return {"started": False, "running": True, "host": CUSTOMER_HOST, "port": CUSTOMER_PORT}

        env = os.environ.copy()
        env["PYTHONPATH"] = PROJECT_ROOT + os.pathsep + env.get("PYTHONPATH", "")

        logger.info("[MockCustomer] 启动模拟客户服务 127.0.0.1:8100")
        self._proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "mock_customer_api:app",
             "--host", CUSTOMER_HOST, "--port", str(CUSTOMER_PORT), "--log-level", "warning"],
            cwd=MOCK_SERVER_DIR,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # 等待服务就绪
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._proc.poll() is not None:
                break
            if self.is_running():
                logger.info("[MockCustomer] 模拟客户服务启动完成")
                return {"started": True, "running": True, "host": CUSTOMER_HOST, "port": CUSTOMER_PORT}
            time.sleep(0.3)

        # 启动失败：回收进程并报错
        self.stop()
        raise RuntimeError(f"模拟客户服务启动超时（{timeout}s），请检查 mock_customer_api.py")

    def stop(self) -> dict:
        """停止模拟客户服务"""
        if self._proc is not None and self._proc.poll() is None:
            logger.info("[MockCustomer] 停止模拟客户服务")
            self._proc.terminate()
            try:
                self._proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=3)
        self._proc = None
        return {"running": False, "host": CUSTOMER_HOST, "port": CUSTOMER_PORT}


# 全局单例
mock_customer_manager = MockCustomerManager()