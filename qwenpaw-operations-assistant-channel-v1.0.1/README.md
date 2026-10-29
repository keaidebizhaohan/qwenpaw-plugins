# 运维助手频道插件

这个包只负责运维助手的频道生命周期：

- 启用/保存时调用 `{registration_service_url}/channel/register`；
- 停用时调用 `{registration_service_url}/channel/deregister`；
- 向注册中心提交聊天、文件上传地址以及本插件提供的健康检查地址；
- 注册数据包含 `stopWebhook`，指向 HTTP 插件的 `/chat/stop`；
- `GET /api/operations-assistant-channel/health?agentId=...` 始终返回频道注册状态；
- 未启用或未注册时健康接口仍返回 HTTP 200，响应体为 `unhealthy`。

它只注册 `/api/operations-assistant-channel/health`，不与 HTTP 插件使用的
`/api/service-channel` 前缀冲突，也不注册聊天和文件上传路由。

页面只包含服务注册地址、注册 Token、QwenPaw 对外地址、created_on 和注册
请求超时。聊天及文件上传没有页面配置；HTTP 插件固定使用 1800 秒处理超时
和 1024 MB 单文件上限，并且不校验聊天回调 Token。

