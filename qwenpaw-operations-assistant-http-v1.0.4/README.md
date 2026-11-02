# 运维助手 HTTP API

该插件只负责 HTTP 接口，不注册 QwenPaw 频道，也不执行外部服务注册或注销。
因此 Agent 频道热重载、停用或重新注册不会移除聊天接口。

接口路径保持不变：

- `POST /api/service-channel/chat?agentId=<agentId>`：请求体省略
  `stream` 或传 `true` 时返回 `text/event-stream`；传 `false` 时等待 Agent
  完成后仍返回 `text/event-stream`。有思考时依次输出
  `REASONING_MESSAGE_CHUNK`、`TEXT_MESSAGE_CHUNK` 两帧，没有思考时只输出
  一帧 `TEXT_MESSAGE_CHUNK`；
- `POST /api/service-channel/chat/stop?agentId=<agentId>`，请求体为
  `{"sessionId":"..."}`；停止该会话下全部运行中的聊天；
- `POST /api/service-channel/files?agentId=<agentId>`

健康检查接口由“运维助手频道”插件提供。每次聊天或上传请求都根据
`agentId` 直接取得对应 Agent Workspace，不读取频道配置，也不校验回调
Bearer Token。Agent 超时固定为 1800 秒，单文件上传上限固定为 1024 MB；
思考、工具调用和工具结果使用插件内置输出策略。

## 非流式调用示例

```json
{
  "userId": "user-1",
  "sessionId": "session-1",
  "stream": false,
  "input": [
    {
      "role": "user",
      "type": "message",
      "content": [{"type": "text", "text": "你是谁"}]
    }
  ]
}
```

如果 QwenPaw 最终响应同时包含思考和回答，HTTP 200 的
`Content-Type` 为 `text/event-stream`，响应内容为：

```text
data: {"type":"REASONING_MESSAGE_CHUNK","timestamp":1790000000000,"messageId":"msg-reasoning","delta":"完整思考内容"}

data: {"type":"TEXT_MESSAGE_CHUNK","timestamp":1790000000001,"messageId":"msg-answer","role":"assistant","delta":"完整最终回答"}

```

没有思考消息时，SSE 中只有一帧 `TEXT_MESSAGE_CHUNK`。插件内部仍调用
QwenPaw 的流式接口，但不会向调用方转发中间碎片，而是等待最后一个
`object=response`，从 `output` 中提取完整非增量文本后再逐帧输出。
