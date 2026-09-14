# -*- coding: utf-8 -*-
"""
协议匹配全链路联调脚本（action=start / action=task + 断线重连续传）
==================================================================
链路：本脚本(模拟前端) -> 我们后端 /api/protocol/match
    -> 客户流式接口 /api/customer/protocol/match -> 事件实时透传 -> done 断开

跑法：一条命令，自动起后端(8000) + 模拟客户(8100)，跑完自动关。
    python test_protocol_api.py

验证点：
1. action=start：实时收到客户整个过程事件（start/stage/done），并留存客户批次号；
2. action=task ：读取过程中主动断开（模拟前端刷新/关窗），随后重连——
   后端不重复调用客户接口（不再出现 start 事件），而是从断点续传剩余事件直至 done；
3. GET /api/protocol/history：能查到该任务的历史对话（按客户批次号归档）。
"""
import json
import os
import subprocess
import sys
import time

import httpx

BACKEND_HOST = "127.0.0.1"
BACKEND_PORT = 8000
CUSTOMER_PORT = 8100

# 后端默认的客户地址指向真实客户服务，本地联调需显式把 url 覆盖成本模拟服务
MOCK_MATCH_URL = f"http://{BACKEND_HOST}:{CUSTOMER_PORT}/api/customer/protocol/match"

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


def print_event(ev: dict) -> None:
    """按事件协议打印一条事件，便于观察过程"""
    tag = ev.get("event")
    if tag == "conn":
        print(f"   [{tag:<6}] mode={ev.get('mode')} 本服务task={ev.get('task_id')} "
              f"客户批次号={ev.get('batch_id')}")
        return
    meta = []
    if ev.get("step_id") is not None:
        meta.append(f"step={ev['step_id']}")
    if ev.get("progress") is not None:
        meta.append(f"progress={ev['progress']}")
    if ev.get("sub_bid_info"):
        meta.append(f"sub_bid={ev['sub_bid_info']}")
    if ev.get("cost_ms") is not None:
        meta.append(f"cost={ev['cost_ms']}ms")
    title = ev.get("title") or ""
    print(f"   [{str(tag):<6}] {' '.join(meta):<38} | {title} {ev.get('content') or ''}")


def read_events(client, url: str, payload: dict, stop_fn=None):
    """请求 /api/protocol/match 并读取事件，直到 stop_fn(ev) 为真或 done/error

    Returns:
        (events, finished_ok)：事件列表，以及是否以 done 正常结束
    """
    events = []
    with client.stream("POST", url, json=payload) as r:
        if r.status_code != 200:
            raise RuntimeError(f"请求失败 HTTP {r.status_code}")
        for line in r.iter_lines():
            if not line.startswith("data: "):
                continue
            ev = json.loads(line[6:])
            events.append(ev)
            print_event(ev)
            if stop_fn and stop_fn(ev):
                return events, False
            if ev.get("event") in ("done", "error"):
                return events, ev.get("event") == "done"
    return events, False


def main():
    backend = start_backend()
    # 长耗时步骤缩到 5 轮 * 0.5s，保证断点发生在任务执行中
    customer = start_customer(long_calc_rounds=5, long_calc_interval=0.5)
    try:
        wait_ready(BACKEND_PORT)
        wait_ready(CUSTOMER_PORT)

        match_url = f"http://{BACKEND_HOST}:{BACKEND_PORT}/api/protocol/match"

        print("===== 第一段：action=start 发起匹配，读到「方案均衡」第一次进度后断开（模拟刷新/关窗） =====")
        with httpx.Client(timeout=None) as client:  # 长任务：不设读超时
            ev1, _ = read_events(
                client, match_url,
                {"action": "start", "params": {"warehouseCode": "WH001", "planMonth": "2026-08"},
                 "url": MOCK_MATCH_URL},
                stop_fn=lambda ev: bool(ev.get("sub_bid_info")),
            )
        batch_id = next((e.get("task_id") for e in ev1 if e.get("event") == "start"), None)
        print(f"   -> 客户匹配批次号 = {batch_id}")

        print("===== 第二段：action=task 重连当前任务，应续传（不再调用客户接口）直至 done =====")
        with httpx.Client(timeout=None) as client:
            ev2, finished_ok = read_events(client, match_url, {"action": "task"})

        print("===== 第三段：查询历史任务对话 =====")
        with httpx.Client(timeout=5) as client:
            hist = client.get(f"http://{BACKEND_HOST}:{BACKEND_PORT}/api/protocol/history").json()
        tasks = (hist.get("data") or {}).get("tasks") or []
        hist_hit = next((t for t in tasks if t.get("batch_id") == batch_id), None)
        print(f"   -> 历史任务数={len(tasks)}，命中批次号={batch_id}："
              f"{'是' if hist_hit else '否'}（事件 {len((hist_hit or {}).get('events') or [])} 条）")

        # 校验
        conn2 = next((e for e in ev2 if e.get("event") == "conn"), {})
        restarted = any(e.get("event") == "start" for e in ev2)   # 重连不应再出现 start（下游未被重复调用）
        ok = (
            conn2.get("mode") == "reconnect"                 # 重连模式
            and conn2.get("batch_id") == batch_id            # 仍关联同一客户批次
            and not restarted                                # 未从头再来 => 客户接口未被重复调用
            and finished_ok                                  # 续传后收到 done
            and hist_hit is not None                         # 历史对话已归档
            and len((hist_hit or {}).get("events") or []) > 0
        )
        print("[PASS] 全部通过（断线重连续传 + 历史对话留存正常）" if ok else "[FAIL] 检查失败")
    finally:
        backend.terminate()
        customer.terminate()


if __name__ == "__main__":
    main()
