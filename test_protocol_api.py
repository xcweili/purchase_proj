# -*- coding: utf-8 -*-
"""
协议匹配全链路联调脚本
======================
链路（当前形态：我们后端调客户接口）：
    前端(本脚本) -> 我们后端 /api/protocol/match -> 客户接口 /api/customer/protocol/match
    -> 客户流程中上报事件到我们后端 -> SSE 实时转发给前端

跑法：一条命令，自动起后端(8000) + 模拟客户(8100)，跑完自动关。
    python test_protocol_api.py

验证点：
1. 通过后端发起接口触发匹配（后端 -> 客户接口），实时收到思考过程事件时间线
2. 发起接口返回客户最终结果
3. 新订阅者补发历史事件（防丢事件）
"""
import json
import queue
import subprocess
import sys
import threading
import time

import httpx

BACKEND_HOST = "127.0.0.1"
BACKEND_PORT = 8000
CUSTOMER_PORT = 8100


def start_backend() -> subprocess.Popen:
    """启动我们的后端服务"""
   # print(f"启动后端服务    http://{BACKEND_HOST}:{BACKEND_PORT}")
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "src.main:app",
         "--host", BACKEND_HOST, "--port", str(BACKEND_PORT), "--log-level", "warning"],
        cwd=".",
    )


def start_customer() -> subprocess.Popen:
    """启动模拟客户服务"""
    #print(f"启动模拟客户服务 http://{BACKEND_HOST}:{CUSTOMER_PORT}")
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "mock_customer_api:app",
         "--host", BACKEND_HOST, "--port", str(CUSTOMER_PORT), "--log-level", "warning"],
        cwd=".",
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


def subscribe(task_id: str, out_q: "queue.Queue"):
    """SSE 订阅线程：把收到的每个事件放进 out_q"""
    url = f"http://{BACKEND_HOST}:{BACKEND_PORT}/api/protocol/match/stream?task_id={task_id}"
    try:
        with httpx.Client(timeout=None) as client:
            with client.stream("GET", url) as r:
                for line in r.iter_lines():
                    if line.startswith("data: "):
                        out_q.put(json.loads(line[6:]))
    except Exception as e:  # noqa: BLE001
        out_q.put({"event": "sse_closed", "message": str(e)})


def main():
    backend = start_backend()
    customer = start_customer()
    try:
        wait_ready(BACKEND_PORT)
        wait_ready(CUSTOMER_PORT)

        #print("=" * 64)
        #print("1. 订阅 SSE 实时事件流")
        task_id = f"match-{int(time.time() * 1000)}"
        out_q: "queue.Queue" = queue.Queue()
        threading.Thread(target=subscribe, args=(task_id, out_q), daemon=True).start()
        time.sleep(0.6)  # 等 SSE 订阅建立

        #print("=" * 64)
       # print("2. 通过后端发起匹配（前端 -> 我们后端 -> 客户接口）")
        resp = httpx.post(
            f"http://{BACKEND_HOST}:{BACKEND_PORT}/api/protocol/match",
            json={"task_id": task_id, "warehouseCode": "WH001", "planMonth": "2026-08"},
            timeout=120,
        )
        body = resp.json()
       # print(f"   发起接口返回 -> HTTP {resp.status_code} | {body.get('message', '')}")
        result = body.get("data", {}).get("result", {})
       # if result:
          #  print(f"   客户最终结果 -> task_id={result.get('task_id')} | "
           #       f"计划数={result.get('plans')} | 供应商数={result.get('suppliers')} | "
            #      f"候选数={result.get('candidates')} | 分配策略数={result.get('allocate', {}).get('strategies')}")

        #print("=" * 64)
        #print("3. 实时思考过程事件时间线")
        events = []
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                ev = out_q.get(timeout=1)
            except queue.Empty:
                continue
            events.append(ev)
            tag = ev.get("event")
            step = ev.get("step_id", "")
            title = ev.get("title") or ev.get("content") or ev.get("summary") or ev.get("message") or ""
            if tag == "step_update":
                p = ev.get("progress", {})
                title = f"正在计算第 {p.get('current')}/{p.get('total')} 种分配策略"
            if tag in ("step_start", "step_end", "step_update", "reasoning", "done", "error"):
                print(f"   [{tag:<12}] {step:<15} | {title}")
            if tag == "done":
                break
        if not events or events[-1].get("event") != "done":
            print("   未收到 done 事件，链路未走通（超时）")

        #print("=" * 64)
       # print("4. 收到事件类型统计")
        #print("   ", {e.get("event") for e in events})

        #print("=" * 64)
        #print("5. 新订阅者补发历史事件（防丢事件）")
        replay_count = 0
        url = f"http://{BACKEND_HOST}:{BACKEND_PORT}/api/protocol/match/stream?task_id={task_id}"
        with httpx.Client(timeout=30) as client:
            with client.stream("GET", url) as r:
                for line in r.iter_lines():
                    if line.startswith("data: "):
                        replay_count += 1
                        if json.loads(line[6:]).get("event") == "done":
                            break
       # print(f"   补发历史事件 {replay_count} 条（应为 {len(events)} 条）")

       # print("=" * 64)
        ok = (resp.status_code == 200
              and result
              and events and events[-1].get("event") == "done"
              and replay_count == len(events))
       # print("[PASS] 全部通过" if ok else "[FAIL] 检查失败")
    finally:
        backend.should_exit = True
        customer.should_exit = True


if __name__ == "__main__":
    main()
