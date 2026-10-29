# 运维助手频道插件

该插件把当前 QwenPaw Agent 注册成一个可由外部服务同步调用的“运维助手”。
外部服务发送 QwenPaw 聊天格式请求，插件调用绑定 Agent，并返回 AG-UI 1.0
SSE 事件流。

运维助手是受信任的自动化回调频道。插件对纯文本和文件聊天统一构造
`AgentRequest`，并固定传入：

```json
{"request_context":{"approval_level":"off"}}
```

因此通过本频道产生的工具调用不会等待人工审批；外部调用方无需在聊天请求体中
重复传该字段。该设置仅影响运维助手频道，不修改 Agent 的全局审批配置。

每次聊天都会按 `sessionId + userId + operations-assistant` 在 QwenPaw
会话目录中创建或刷新记录，因此可以在控制台查看由该频道产生的聊天历史。

聊天回调直接使用 QwenPaw 的原生 `workspace.stream_query()`，按照原始执行
顺序流式发送以下事件：

- `THINKING_START`、`THINKING_TEXT_MESSAGE_*`、`THINKING_END`：思考过程；
- `TOOL_CALL_START/ARGS/END`：工具名称及参数；
- `TOOL_CALL_RESULT`：工具执行结果；
- `TEXT_MESSAGE_START/CONTENT/END`：最终回答；
- `CUSTOM`（`name=WIDGET`）：`<widget>...</widget>` 中的完整 JSON；
- `RUN_STARTED/RUN_FINISHED/RUN_ERROR`：本次执行状态。

页面中的“显示思考过程”“显示工具调用信息”“显示工具结果信息”和最大显示
长度会控制相应事件。流结束后，`RUN_FINISHED.result.answer` 提供剥离 Widget
标签后的普通文本回答。

## Widget 自定义事件

Agent 按 Skill 要求返回以下内容时：

```xml
<widget>{"component":"faultList","list":[{"level":"CRITICAL","count":1}]}</widget>
```

插件支持标签和 JSON 被拆散到任意多个文本增量。收到闭合标签后输出一帧：

```text
data: {"type":"CUSTOM","timestamp":1793000000000,"runId":"run-xxx","name":"WIDGET","value":{"component":"faultList","list":[{"level":"CRITICAL","count":1}]}}
```

Widget 标签和内容不会再作为 `TEXT_MESSAGE_CONTENT` 输出，也不会进入
`RUN_FINISHED.result.answer`。同一次回答可以包含多个 Widget；每个闭合标签
分别产生一条 `CUSTOM/WIDGET` 事件。若标签内容不是合法 JSON，插件不会静默
丢弃内容，而是保留为普通文本，便于调用方发现 Agent 输出格式问题。

插件还会移除 Widget 标签前后的纯空白流式分片（例如单独的 `\n\n`），避免
前端在卡片上下渲染多余空白消息块；普通正文内部的空格和换行保持不变。

## 文件上传与聊天文件引用

### 1. 上传文件

```bash
curl -X POST \
  'http://10.12.40.1:8089/api/service-channel/files?agentId=default' \
  -H 'Authorization: Bearer <聊天回调 Token>' \
  -F 'file=@/path/to/report.log' \
  -F 'userId=zhangsan'
```

- `file`（必填）：上传的文件内容；
- `userId`（可选）：记录文件归属用户；填写后聊天引用该文件时必须携带
  相同的 `userId`，否则返回 403；不填写则该 Agent 的任何调用方都可用；
- `sessionId`（可选）：当前文件注册不使用该值；聊天历史以聊天请求中的
  `sessionId` 为准。

成功响应：

```json
{"fileId": "9f2c...", "filename": "report.log", "size": 12345}
```

错误码：401 回调 Token 校验失败；404 Agent 未启用或 workspace 不可用；
400 文件名非法或内容为空；413 超过“单文件上传上限（MB）”配置（默认 50）。

文件保存在 Agent workspace 的 `media/service-channel/<fileId>/` 目录下；
fileId 注册表保存在进程内存中，QwenPaw 重启后旧 fileId 失效。

### 2. 聊天中引用文件

在 `input` 的 content 数组中加入 `{"type":"file","fileId":"..."}` 块即可，
可以与文本块混用，也可以只发文件：

```bash
curl -N -X POST \
  'http://10.12.40.1:8089/api/service-channel/chat?agentId=default' \
  -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' \
  --data-raw '{
    "userId":"zhangsan",
    "sessionId":"session-zhangsan-001",
    "input":[
      {
        "role":"user",
        "type":"message",
        "content":[
          {"type":"text","text":"请分析这份日志里 ERR 的原因"},
          {"type":"file","fileId":"9f2c..."}
        ]
      }
    ]
  }'
```

引用校验：fileId 必须属于当前 Agent（否则 404）；上传时记录了归属用户
且本次 `userId` 不同（403）；文件已被磁盘清理（410）。SSE 事件格式与
纯文本聊天完全一致。

携带文件块时插件改走 QwenPaw 原生 `workspace.stream_query(AgentRequest)`
路径，文件以 file:// 块进入执行前媒体处理钩子，由 QwenPaw 注入
“用户上传文件，已经下载到 <路径>”提示，Agent 即可读取该文件。注意：如果
Agent 的工具审批模式为“严格”，Agent 读取文件仍可能触发审批；建议使用
“自动”或“关闭”模式。

插件只接受 `fileId` 文件块，不接受远程 `fileUrl` 文件模式，也不提供文件
下载接口。纯文本与文件请求均固定归入 `operations-assistant` 频道。

插件只修改自身代码，不修改 QwenPaw 源码。

## 页面配置

假设服务注册中心为 `10.12.40.1:7070`，QwenPaw 为
`10.12.40.1:8089`：

```text
服务注册接口 URL：http://10.12.40.1:7070
注册接口 Bearer Token：没有认证时留空
QwenPaw 对外访问地址：http://10.12.40.1:8089
聊天回调 Bearer Token：测试阶段可留空
创建于 created_on：QwenPaw（可以修改；请求字段为 createdOn）
注册请求超时（秒）：30
Agent 最大处理时间（秒）：1800
单文件上传上限（MB）：50
```

注册和注销使用同一个服务基础地址。页面只填写协议、IP 和端口，插件固定追加以下路径：

- 注册：`POST /channel/register`
- 注销：`POST /channel/deregister`
- QwenPaw 聊天：`POST /api/service-channel/chat?agentId=<agentId>`
- QwenPaw 文件上传：`POST /api/service-channel/files?agentId=<agentId>`
- QwenPaw 健康检查：`GET /api/service-channel/health?agentId=<agentId>`

服务代码和名称由插件固定为：

```text
serviceCode = operations-assistant
serviceName = Operations Assistant
```

无需配置注册标识或服务显示名称。每个 Agent 通过自己的 `agentId` 唯一注册；
`agentName` 会从当前 QwenPaw Agent 配置中自动读取。
`agentDescription` 会从当前 QwenPaw Agent 配置的描述字段中自动读取；
未配置描述时传空字符串。

## 启用时的注册请求

点击保存且“已启用”时，插件向注册中心发送：

```json
{
  "serviceCode": "operations-assistant",
  "serviceName": "Operations Assistant",
  "agentId": "default",
  "agentName": "Default Agent",
  "agentDescription": "负责基础设施巡检和故障分析",
  "channel": "operations-assistant",
  "chatWebhook": "http://10.12.40.1:8089/api/service-channel/chat?agentId=default",
  "fileUploadWebhook": "http://10.12.40.1:8089/api/service-channel/files?agentId=default",
  "healthUrl": "http://10.12.40.1:8089/api/service-channel/health?agentId=default",
  "requestFormat": "QWENPAW_CHAT",
  "responseFormat": "AG_UI_SSE_1_0",
  "callbackToken": "",
  "registerFrom": "QwenPaw",
  "createdOn": "QwenPaw"
}
```

## 停用时的取消请求

关闭“已启用”并保存后，旧活动频道停止时向注销接口发送：

```json
{
  "agentId": "default",
  "registerFrom": "QwenPaw",
  "createdOn": "QwenPaw"
}
```

QwenPaw 热更新采用“先启动新实例，再停止旧实例”。插件会识别这种情况，
普通配置更新不会让旧实例误取消新实例的注册。

## 聊天请求

```bash
curl -N \
  -X POST \
  'http://10.12.40.1:8089/api/service-channel/chat?agentId=default' \
  -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' \
  --data-raw '{
    "userId":"zhangsan",
    "sessionId":"session-zhangsan-001",
    "input":[
      {
        "role":"user",
        "type":"message",
        "content":[{"type":"text","text":"你好"}]
      }
    ]
  }'
```

如果页面配置了“聊天回调 Bearer Token”，请求还需携带：

```text
Authorization: Bearer <聊天回调 Token>
```

## healthUrl 的用途

`healthUrl` 不会执行 Agent，也不是聊天接口。注册中心可定时调用它，确认：

- QwenPaw 进程可以访问；
- 指定 Agent 的运维助手频道仍处于启用状态；
- 当前回调 URL 对应的进程内绑定有效。

如果 QwenPaw 被强制杀死、机器断电或网络中断，插件来不及调用取消接口，
注册中心仍可通过健康检查把陈旧记录标为离线或清理。

## 本地验证

```bash
cd /Users/hanhan/Downloads/qwenpaw-service-channel
uv sync
uv run pytest -q
```
