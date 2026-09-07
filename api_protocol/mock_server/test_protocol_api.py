# -*- coding: utf-8 -*-
"""
协议匹配全链路联调脚本（单接口流式代理 + 断线重连续传）
========================================================
链路：本脚本(模拟前端) -> 我们后端 /api/protocol/match（单接口）
    -> 客户流式接口 /api/customer/protocol/match -> 事件实时透传 -> done 断开

跑法：一条命令，自动起后端(8000) + 模拟客户(8100)，跑完自动关。
    python test_protocol_api.py

验证点：
1. 首次调用：实时收到客户整个过程事件时间线，并拿到最终结果；
2. 断线重连：读取过程中主动断开（模拟前端刷新/关窗），随后用同一 task_id
   重新调用，后端不重复调用 8100（不再出现 step_start 从头开始），
   而是从断点续传剩余事件直至 done。
"""
import json
import os
import subprocess
import sys
import time
import uuid

import httpx

BACKEND_HOST = "127.0.0.1"
BACKEND_PORT = 8000
CUSTOMER_PORT = 8100

# 脚本所在目录 = api_protocol/mock_server；项目根目录 = 其上级的上级
MOCK_SERVER_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(MOCK_SERVER_DIR))


def start_backend() -> subprocess.Popen:
    """启动我们的后端服务"""
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "src.main:app",
         "--host", BACKEND_HOST, "--port", str(BACKEND_PORT), "--log-level", "warning"],
        cwd=PROJECT_ROOT,
    )


def start_customer(long_calc_rounds=None, long_calc_interval=None) -> subprocess.Popen:
    """启动模拟客户服务（可用环境变量缩短长耗时步骤，加快测试）"""
    env = os.environ.copy()
    if long_calc_rounds is not None:
        env["MOCK_LONG_CALC_ROUNDS"] = str(long_calc_rounds)
    if long_calc_interval is not None:
        env["MOCK_LONG_CALC_INTERVAL"] = str(long_calc_interval)
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "mock_customer_api:app",
         "--host", BACKEND_HOST, "--port", str(CUSTOMER_PORT), "--log-level", "warning"],
        cwd=MOCK_SERVER_DIR,
        env=env,
    )


def wait_ready(port: int, timeout: float = 30.0) -> None:
    """轮询等待服务可访问"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            httpx.get(f"http://{BACKEND_HOST}:{port}/docs", timeout=1)
            return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError(f"端口 {port} 服务启动超时")


def read_events_until(client, url, payload, stop_fn=None):
    """发起匹配并读取事件，直到 stop_fn(ev) 为真（含该事件）或 done 为止"""
    events = []
    with client.stream("POST", url, json=payload) as r:
        if r.status_code != 200:
            raise RuntimeError(f"发起匹配失败 HTTP {r.status_code}")
        for line in r.iter_lines():
            if not line.startswith("data: "):
                continue
            ev = json.loads(line[6:])
            events.append(ev)
            tag = ev.get("event")
            step = ev.get("step_id", "")
            title = ev.get("title") or ev.get("content") or ev.get("summary") or ev.get("message") or ""
            if tag == "step_update":
                p = ev.get("progress", {})
                title = f"进度 {p.get('current')}/{p.get('total')}：" + title
            if tag == "conn":
                print(f"   [{tag:<10}] mode={ev.get('mode')} task={ev.get('task_id')}")
            elif tag in ("step_start", "step_end", "step_update", "reasoning", "done", "error"):
                print(f"   [{tag:<10}] {step:<12} | {title}")
            if stop_fn and stop_fn(ev):
                return events, {}
            if tag == "done":
                return events, ev.get("result", {})
    return events, {}


def main():
    backend = start_backend()
    # 长耗时步骤缩到 5 轮 * 0.5s，保证断点发生在任务执行中
    customer = start_customer(long_calc_rounds=5, long_calc_interval=0.5)
    try:
        wait_ready(BACKEND_PORT)
        wait_ready(CUSTOMER_PORT)

        url = f"http://{BACKEND_HOST}:{BACKEND_PORT}/api/protocol/match"
        task_id = "test-" + uuid.uuid4().hex[:8]
        # 业务参数放 params 里原样透传给客户接口
        payload = {"task_id": task_id, "params": {"warehouseCode": "WH001", "planMonth": "2026-08"}}

        print(f"===== 第一段：发起匹配，读到「深度计算」第一次进度后断开（模拟刷新/关窗） =====")
        with httpx.Client(timeout=None) as client:  # 长任务：不设读超时
            read_events_until(
                client, url, payload,
                stop_fn=lambda ev: ev.get("event") == "step_update" and ev.get("step_id") == "deep_calc",
            )

        print(f"===== 第二段：同一 task_id={task_id} 重连，应续传（不再调用8100）直至 done =====")
        with httpx.Client(timeout=None) as client:
            events2, result = read_events_until(client, url, payload)

        # 校验
        conn2 = next((e for e in events2 if e.get("event") == "conn"), {})
        restarted_from_begin = any(
            e.get("event") == "step_start" and e.get("step_id") == "query_plans"
            for e in events2
        )
        has_done = any(e.get("event") == "done" for e in events2)
        ok = (
            conn2.get("mode") == "reconnect"      # 重连模式
            and not restarted_from_begin          # 未从头再来 => 8100 未被重复调用
            and has_done and bool(result)         # 最终拿到 done + 结果
        )
        print("[PASS] 全部通过（断线重连续传正常）" if ok else "[FAIL] 检查失败")
    finally:
        backend.terminate()
        customer.terminate()


if __name__ == "__main__":
    main()
