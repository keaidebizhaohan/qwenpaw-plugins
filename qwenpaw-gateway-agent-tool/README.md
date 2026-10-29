# K8s Agent 调用工具

该插件向 QwenPaw 注册 `call_k8s_agent`，用于调用经 JiuwenSwarm Gateway 发布到 Kubernetes 中运行的 Agent。

该插件只支持 `hanhan-agent-runtime-test` 命名空间中 `jiuwenswarm-gateway` 当前暴露的 Web HTTP 接口：

- `POST /api/v1/sessions`：首次调用时创建会话。
- `POST /api/v1/chat/completions`：使用创建的 `session_id` 发送消息。

插件不再提供 `api_style`，也不兼容 `/api/agent/chat`。目标 Gateway 地址是当前 NodePort 地址 `http://10.12.40.4:30092`。

## 配置原则

- Gateway 地址、Bot ID、接口模式和业务用途写在工作区 `AGENTS.md`，让模型据此选择远程 Agent。
- Authorization 属于敏感信息，只放在插件的前端配置中，不要写入 `AGENTS.md`。
- 首次调用不传 `session_id`；继续同一段对话时复用工具结果中的 `sessionId`。

## AGENTS.md 示例

```md
## 可调用的 Gateway Agents

- 代码审查 Agent
  - gateway_base_url: http://10.12.40.4:30092
  - bot_id: 请填写 Manager「Agent 资源」列表中资源名称下面的资源 ID
  - user_id: qwenpaw-gateway-caller
  - group_id: test-group
  - 用途: 使用已绑定的 code-reviewer skill 审查代码

当用户的问题符合 Gateway Agent 的用途时，调用 `call_k8s_agent`：

1. 使用对应的 gateway_base_url、bot_id、user_id 和 group_id。
2. 将用户问题和必要上下文放入 prompt。
3. 首次调用不传 session_id。
4. 继续追问时复用工具返回的 sessionId。
5. 工具失败时如实说明，不能假装调用成功。
```

这里的 `bot_id` 是 Agent 资源 ID，不是运行时模板 ID、Pod 名称或模型 ID。

## 返回结果

工具成功时返回 JSON 文本，主要字段如下：

```json
{
  "gatewayBaseUrl": "http://10.12.40.4:30092",
  "botId": "...",
  "sessionId": "...",
  "answer": "远程 Agent 的最终回答"
}
```
