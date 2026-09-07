# 协议匹配接口 · 客户端使用说明

> 服务端对外只暴露 **一个接口**：发起匹配 + 实时接收过程事件流，全程以 SSE 流式返回，收到 `done` 即结束。
> 支持断线重连：同一 `task_id` 重复调用即可续传，不必担心刷新页面/关窗导致任务中断。

---

## 一、一句话说明

客户端调用 `POST /api/protocol/match`，传入任务参数（可携带 `task_id`）：

- 服务端会**代理调用底层匹配服务**，把匹配过程的每一步事件**实时流式推送**给客户端；
- 匹配可能持续 **20 分钟以上**，事件会一条一条推过来，客户端按行解析即可；
- 收到 `done` 事件表示流程完成（携带最终结果），此时可断开连接；
- 如果连接中途断开（刷新/关窗/网络抖动），**用同一 `task_id` 再次调用**即可从断点继续，不会重复发起底层匹配。

---

## 二、接口定义

### `POST /api/protocol/match`

**请求头**：`Content-Type: application/json`

**请求体：**

| 字段        | 类型   | 必填 | 默认           | 说明                                                                                   |
| ----------- | ------ | ---- | -------------- | -------------------------------------------------------------------------------------- |
| `task_id` | string | 否   | 服务端自动生成 | 任务标识。**重复携带同一 `task_id` 调用 = 断线续传**（不会重复发起底层匹配）   |
| `params`  | object | 否   | `{}`         | 业务参数对象，**原样透传给底层匹配服务**（按实际业务约定填写，任意属性）               |

请求示例：

```json
{
  "task_id": "match-abc123",
  "params": { "warehouseCode": "WH001", "planMonth": "2026-08", "priority": "high" }
}
```

**响应**：`200 OK`，`Content-Type: text/event-stream`（SSE 事件流，不设读超时）。

---

## 三、事件流格式

每帧格式：`data: {json}\n\n`（数据帧），或 `\n: keepalive\n\n`（心跳帧，冒号开头，仅保活，无业务含义）。

### 3.1 第一帧：连接元事件 `conn`

用于告知本次连接是「新建」还是「续传」：

```json
{"event":"conn","task_id":"match-abc123","mode":"new"}
```

| mode          | 含义                                                   |
| ------------- | ------------------------------------------------------ |
| `new`       | 本次为新任务，底层匹配已开始执行                       |
| `reconnect` | 任务仍在执行，本次为断点续传，事件从上次断开处继续     |
| `finished`  | 任务已结束，本次为历史事件回放（查看已完成任务的结果） |

### 3.2 业务事件（事件类型在 JSON 的 `event` 字段）

| event           | 含义                               | 主要字段                                      |
| --------------- | ---------------------------------- | --------------------------------------------- |
| `step_start`  | 步骤开始                           | `step_id, title, desc, status`              |
| `reasoning`   | 中间说明/思考                      | `step_id, content`                          |
| `step_update` | 步骤进度更新                       | `step_id, progress{current,total}, content` |
| `step_end`    | 步骤结束                           | `step_id, title, status, summary, cost_ms`  |
| `done`        | **流程完成**（携带最终结果） | `status, summary, result`                   |
| `error`       | 出错                               | `message`                                   |

`done` 事件示例：

```json
{
  "event": "done",
  "task_id": "match-abc123",
  "status": "success",
  "summary": "协议匹配流程全部完成",
  "result": { "matched": 10, "partial": 1, "unmet": 1 }
}
```

---

## 四、典型使用场景

### 场景 1：正常发起匹配

1. 调用 `/api/protocol/match`（不带或带自生成的 `task_id`）；
2. 持续读取事件流，实时渲染进度；
3. 收到 `done` 或 `error` 后断开连接，展示 `result`。

### 场景 2：中途刷新页面 / 关闭窗口

1. 首次调用前生成一个 `task_id` 并保存（如 localStorage / 本地变量）；
2. 连接断开后（任务仍在底层继续执行），**再次调用** `/api/protocol/match`，携带**同一个 `task_id`**；
3. 响应第一帧 `mode=reconnect`：事件从断开处继续推送（断线期间漏掉的事件会补发），直到 `done`。
   - 服务端**不会**因此重复发起底层匹配。

### 场景 3：任务已完成，想重新查看结果

- 携带已完成任务的 `task_id` 再次调用，第一帧 `mode=finished`，随后回放完整历史事件（含最终 `done`）。

### 场景 4：长时间无进度，判断是否"卡住"

- 服务端与底层服务之间有心跳保活；客户端侧若长时间（建议超过业务预期）未收到任何帧，可断开后携带同一 `task_id` 重新连接，获取最新状态。

---

## 五、调用示例（JavaScript）

```javascript
// 生成并保存 task_id（同一 ID 重连 = 续传）
const taskId = localStorage.getItem('pt_task_id') || 'match-' + Date.now().toString(36);

const resp = await fetch('/api/protocol/match', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ task_id: taskId, params: { warehouseCode: 'WH001', planMonth: '2026-08' } })
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
      // mode: new / reconnect / finished
      console.log('连接方式：', ev.mode);
    } else if (ev.event === 'done') {
      console.log('最终结果：', ev.result);       // 流程完成，可以断开
      finished = true;
    } else if (ev.event === 'error') {
      console.error('匹配出错：', ev.message);
      finished = true;
    } else {
      renderProgress(ev);                        // step_start/reasoning/step_update/step_end
    }
  }
}
```

> 客户端注意：读取到 `done` / `error` 后主动结束读取；不要依赖服务端关闭，也无需手动"取消任务"。

---

## 六、客户端注意事项

| 事项     | 说明                                                                     |
| -------- | ------------------------------------------------------------------------ |
| 连接保持 | 服务端不设读超时；客户端读取也不应设超时，否则长任务会被客户端自行掐断   |
| 心跳帧   | `: keepalive` 无业务含义，解析时直接跳过即可                           |
| 事件类型 | 事件类型在 JSON`data` 的 `event` 字段中（非 SSE 标准 `event:` 行） |
| 断线续传 | 任意时刻携带同一`task_id` 重连即可继续，安全幂等                       |
| 结果获取 | 以`done` 事件携带的 `result` 为最终结果                              |
