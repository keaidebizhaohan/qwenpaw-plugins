# 远程 QwenPaw Agent 调用工具

本插件给当前 QwenPaw Agent 注册工具：

```text
call_remote_qwenpaw_agent
```

它调用另一套 QwenPaw 的原生后台聊天接口，并把远程 Agent 的最终文本回答返回给当前 Agent。它不依赖“运维助手”频道插件。

## 工具参数

| 参数 | 必填 | 说明 |
|---|---:|---|
| `base_url` | 是 | 远程 QwenPaw 根地址，如 `http://10.12.40.1:8088`。不要附加 `/api`。 |
| `agent_id` | 是 | 远程 QwenPaw 中的 Agent ID，如 `default`。 |
| `prompt` | 是 | 交给远程 Agent 的完整任务。当前 Agent 应提供足够上下文。 |
| `session_id` | 否 | 相同值会延续远程上下文；省略时插件生成全新随机会话。 |
| `user_id` | 否 | 远程会话的用户标识；省略时使用插件设置里的默认值。 |

成功时工具向当前 Agent 返回 JSON：

```json
{
  "baseUrl": "http://10.12.40.1:8088",
  "agentId": "default",
  "sessionId": "remote:default:...",
  "userId": "qwenpaw-remote-caller",
  "truncated": false,
  "answer": "远程 Agent 的最终回答"
}
```

## 原生接口调用

插件实际调用：

```text
POST {base_url}/api/agents/{agent_id}/console/chat/task
GET  {base_url}/api/agents/{agent_id}/console/chat/task/{task_id}
```

发送给远程 QwenPaw 的聊天体：

```json
{
  "channel": "console",
  "user_id": "qwenpaw-remote-caller",
  "session_id": "remote:default:...",
  "input": [
    {
      "role": "user",
      "type": "message",
      "content": [{"type": "text", "text": "要处理的问题"}]
    }
  ],
  "timeout": 1800
}
```

## AGENTS.md 写法示例

服务地址和 Agent ID 属于路由信息，建议写在当前 Agent 工作区的 `AGENTS.md`。Token 不要写在这里，应放在插件设置中。

```markdown
## 可调用的远程 QwenPaw Agents

- 数据分析 Agent
  - base_url: http://10.12.40.1:8088
  - agent_id: data-analysis
  - 用途: 数据分析、指标解释、SQL 结果归纳。

- 告警诊断 Agent
  - base_url: http://10.12.40.20:8088
  - agent_id: alarm-diagnosis
  - 用途: 分析告警、日志和故障现象。

当用户的任务符合某个远程 Agent 的用途时：
1. 调用 `call_remote_qwenpaw_agent`，使用上面对应的 `base_url` 和 `agent_id`。
2. `prompt` 必须包含远程 Agent 完成任务所需的上下文。
3. 首次调用省略 `session_id`；若要围绕同一问题继续追问，复用工具返回的 `sessionId`。
4. 工具失败时如实告诉用户，不能假装已经调用成功。
```

## 安装

在 QwenPaw 的“扩展 → 插件 → 安装插件”上传 zip。安装后可在插件配置中设置可选 Authorization 请求头、超时、轮询间隔和默认 User ID。

