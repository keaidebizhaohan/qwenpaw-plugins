# 运维助手 HTTP API

该插件只负责 HTTP 接口，不注册 QwenPaw 频道，也不执行外部服务注册或注销。
因此 Agent 频道热重载、停用或重新注册不会移除聊天接口。

接口路径保持不变：

- `POST /api/service-channel/chat?agentId=<agentId>`
- `POST /api/service-channel/chat/stop?agentId=<agentId>`，请求体为
  `{"sessionId":"..."}`；停止该会话下全部运行中的聊天；
- `POST /api/service-channel/files?agentId=<agentId>`

健康检查接口由“运维助手频道”插件提供。每次聊天或上传请求都根据
`agentId` 直接取得对应 Agent Workspace，不读取频道配置，也不校验回调
Bearer Token。Agent 超时固定为 1800 秒，单文件上传上限固定为 1024 MB；
思考、工具调用和工具结果使用插件内置输出策略。
