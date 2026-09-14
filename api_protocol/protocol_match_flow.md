# 协议匹配接口 · 客户端使用说明

> 服务端对外只暴露 **一个匹配接口**：发起匹配 + 实时接收过程事件流，全程以 SSE 流式返回，收到 `done` 即结束。
> 支持**断线重连**：中途刷新页面/关窗后，再调用一次即可从断点续传，不会重复发起底层匹配。

---

## 一、一句话说明

客户端调用 `POST /api/protocol/match`，通过 `action` 指定本次是**发起**还是**重连**：

- `action=start`：发起一次新匹配。服务端会代理调用底层匹配服务，把匹配过程的每一步事件**实时流式推送**给客户端；
- `action=task`：重连当前任务。**不再调用底层服务**，直接从断点继续把未收到的事件推过来（任务已结束则回放完整对话）；
- 匹配可能持续 **20 分钟以上**，事件一条一条推过来，客户端按行解析即可；
- 收到 `done` 事件表示流程完成，此时可断开连接；
- 匹配批次号（`task_id`）由**底层匹配服务生成**，随每个事件返回，客户端直接取用即可，无需自己传。

---

## 二、接口定义

### `POST /api/protocol/match`

**请求头**：`Content-Type: application/json`

**请求体：**

| 字段       | 类型   | 必填 | 默认      | 说明                                                                    |
| ---------- | ------ | ---- | --------- | ----------------------------------------------------------------------- |
| `action` | string | 否   | `start` | `start`=发起新匹配；`task`=重连当前任务（续传/回放）              |
| `params` | object | 否   | `{}`    | 业务参数对象，**原样透传给底层匹配服务**（按业务约定填写，任意属性） |

请求示例：

```json
{
  "action": "start",
  "params": { "warehouseCode": "WH001", "planMonth": "2026-08", "priority": "high" }
}
```

**响应**：`200 OK`，`Content-Type: text/event-stream`（SSE 事件流，不设读超时）。

> 说明：请求体还支持一个可选的 `url`（底层匹配服务地址），**仅供联调时临时指向模拟服务**，正式对接无需传。

---

## 三、事件流格式

每帧格式：`data: {json}\n\n`（数据帧），或 `: keepalive\n\n`（心跳帧，冒号开头，仅保活，无业务含义）。

### 3.1 第一帧：连接元事件 `conn`

由本服务产生，用于告知本次连接是「新建」还是「续传」：

```json
{"event":"conn","task_id":"match-abc123","batch_id":"BID-20260910150937-8CF7","mode":"new"}
```

| 字段         | 含义                                                       |
| ------------ | ---------------------------------------------------------- |
| `task_id`  | 本服务任务ID（定位本次连接对应的后台任务）                 |
| `batch_id` | 底层匹配服务的批次号；新建连接时为 `null`（待底层返回） |
| `mode`     | 见下表                                                     |

| mode          | 含义                                                   |
| ------------- | ------------------------------------------------------ |
| `new`       | 本次为新任务，底层匹配已开始执行                       |
| `reconnect` | 任务仍在执行，本次为断点续传，事件从上次断开处继续     |
| `finished`  | 任务已结束，本次为历史事件回放（查看已完成任务的结果） |

### 3.2 业务事件（事件类型在 JSON 的 `event` 字段）

| 字段             | 类型        | 必填 | 取值 / 含义                                                                |
| ---------------- | ----------- | ---- | -------------------------------------------------------------------------- |
| `event`        | string      | 是   | `start` 任务已受理 / `stage` 进度节点 / `done` 完成并落库 / `error` 失败 |
| `task_id`      | string      | 是   | 匹配批次号，全程不变                                                       |
| `content`      | string      | 是   | 人类可读的正文（**可直接用于前端展示**）                             |
| `step_id`      | int/null    | 否   | 引擎步骤号（同一分标引擎实例内递增）                                       |
| `title`        | string/null | 否   | 阶段标题（如「数据准备」「Phase0 分段完成」「方案「均衡」开始」）           |
| `desc`         | string/null | 否   | 结构化描述（当前未使用）                                                   |
| `status`       | string/null | 否   | `processing` / `success`（`error` 事件用 `error`）                  |
| `progress`     | int/null    | 否   | 0~100（引擎内单分标粒度；全局进度由各阶段事件顺序体现）                    |
| `summary`      | string/null | 否   | 阶段汇总文案（引擎结束事件使用）                                           |
| `cost_ms`      | long/null   | 否   | 阶段耗时毫秒                                                               |
| `sub_bid_info` | string/null | 否   | 分标信息；**方案内按分标并行执行，事件可能交错**，靠它区分来自哪个分标；空 = 方案/全局级事件 |

事件示例：

```json
{"event":"start","task_id":"BID-20260910150937-8CF7","content":"任务已受理，匹配批次号 BID-...","title":"任务受理","status":"processing","progress":0}
{"event":"stage","task_id":"BID-20260910150937-8CF7","content":"分标A 已完成 1/5 轮匹配计算","step_id":3,"title":"方案「均衡」执行中","status":"processing","progress":20,"sub_bid_info":"分标A"}
{"event":"done","task_id":"BID-20260910150937-8CF7","content":"协议匹配流程全部完成，结果已落库（批次号 BID-...）","status":"success","progress":100,"summary":"协议匹配流程全部完成","cost_ms":2705}
```

> 注意：`sub_bid_info` 非空的事件来自不同分标，可能交错到达；展示时建议按 `step_id` + `sub_bid_info` 分组，或直接按到达顺序追加。

### 3.3 查看历史任务对话 `GET /api/protocol/history`

查看最近的任务及其完整事件对话（服务端内存保留最近 **5** 个任务）。

| 参数        | 类型   | 必填 | 说明                                                       |
| ----------- | ------ | ---- | ---------------------------------------------------------- |
| `task_id` | string | 否   | 指定任务（可传 `batch_id` 或 `conn.task_id`）；不传则返回全部留存任务 |

响应示例：

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "tasks": [
      {
        "task_id": "match-abc123",
        "batch_id": "BID-20260910150937-8CF7",
        "done": true,
        "error": null,
        "created_at": 1789000000.0,
        "event_count": 12,
        "events": [ { "event": "start", "task_id": "BID-...", "content": "..." } ]
      }
    ]
  }
}
```

---

## 四、典型使用场景

### 场景 1：正常发起匹配

1. 调用 `/api/protocol/match`，`action=start`，`params` 按业务传参；
2. 持续读取事件流，实时渲染 `content`（可结合 `title`/`progress`/`step_id`/`sub_bid_info`）；
3. 收到 `done` 或 `error` 后断开连接。

### 场景 2：中途刷新页面 / 关闭窗口

1. 连接断开后（任务仍在底层继续执行），**再次调用** `/api/protocol/match`，`action=task`；
2. 响应第一帧 `mode=reconnect`：事件从断开处继续推送（**断线期间漏掉的事件会补发**），直到 `done`；
3. 服务端**不会**因此重复发起底层匹配。若第一帧是 `mode=finished`，说明任务已结束，服务端会回放完整对话。

> 由于批次号由底层服务生成，客户端无需自己保存 ID，刷新后直接 `action=task` 即可。

### 场景 3：想查看之前的任务对话

- 调用 `GET /api/protocol/history` 获取最近 5 个任务的完整事件对话；
- 或带 `?task_id=BID-...` 只查指定任务。

### 场景 4：长时间无进度，判断是否"卡住"

- 服务端与底层服务之间有心跳保活，空闲时客户端会持续收到 `: keepalive`；
- 若长时间未收到任何业务事件（建议超过业务预期），可断开后用 `action=task` 重新连接获取最新状态；
- 若始终收不到 `done`/`error` 且连接被关闭，说明底层服务异常，建议通过历史接口查看已收到的事件定位卡在哪一步。

---

## 五、调用示例（JavaScript）

```javascript
// 发起匹配：action=start
const resp = await fetch('/api/protocol/match', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ action: 'start', params: { warehouseCode: 'WH001', planMonth: '2026-08' } })
});
if (!resp.ok) throw new Error('发起匹配失败 HTTP ' + resp.status);

const reader = resp.body.getReader();
const decoder = new TextDecoder();
let buf = '', finished = false;

while (!finished) {
  const { done, value } = await reader.read();
  if (done) break;
  buf += decoder.decode(value, { stream: true });
  const lines = buf.split('\n');
  buf = lines.pop();
  for (const raw of lines) {
    const line = raw.trim();
    if (!line.startsWith('data: ')) continue;   // 心跳帧 `: keepalive` 自动跳过
    const ev = JSON.parse(line.slice(6));
    if (ev.event === 'conn') {
      console.log('连接方式：', ev.mode);        // new / reconnect / finished
    } else if (ev.event === 'done') {
      console.log('流程完成：', ev.content);      // 完成并落库
      finished = true;
    } else if (ev.event === 'error') {
      console.error('匹配失败：', ev.content);
      finished = true;
    } else {
      renderProgress(ev);                        // start / stage
    }
  }
}

// 页面刷新后重连：action=task（不需要任何 ID）
async function reconnect() {
  const resp = await fetch('/api/protocol/match', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action: 'task' })
  });
  // 后续读取逻辑同上
}
```

> 客户端注意：读取到 `done` / `error` 后主动结束读取；不要依赖服务端关闭，也无需手动"取消任务"。

---

## 六、客户端注意事项

| 事项     | 说明                                                                 |
| -------- | -------------------------------------------------------------------- |
| 连接保持 | 服务端不设读超时；客户端读取也不应设超时，否则长任务会被客户端自行掐断 |
| 心跳帧   | `: keepalive` 无业务含义，解析时直接跳过即可                       |
| 事件类型 | 事件类型在 JSON `data` 的 `event` 字段中（非 SSE 标准 `event:` 行） |
| 多分标   | 事件可能交错，用 `sub_bid_info` 区分来源                           |
| 断线续传 | 任意时刻用 `action=task` 重连即可继续，安全幂等                     |
| 历史留存 | 服务端内存只保留最近 5 个任务，需要长期留存请自行落库               |
