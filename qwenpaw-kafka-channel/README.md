# QwenPaw Kafka Channel

独立的 QwenPaw Channel 插件。插件监听上游 Kafka 的请求 Topic，消费消息后调用当前
绑定的 Agent；Agent 处理完成后，插件把结果发送到下游 Kafka 的回复 Topic。
上游和下游可以使用同一集群，也可以使用完全独立的集群和认证配置。
插件不会修改 QwenPaw 源码。

## 安装

```bash
qwenpaw plugin install /Users/hanhan/Downloads/qwenpaw-kafka-channel
```

安装完成后重启 QwenPaw，在 **控制 → 频道 → Kafka** 中填写配置并启用。

如果插件安装器没有自动安装依赖，可在运行 QwenPaw 的 Python 环境执行：

```bash
python -m pip install -r /Users/hanhan/Downloads/qwenpaw-kafka-channel/requirements.txt
```

## 最小配置

| 端 | 配置 | 示例 | 说明 |
| --- | --- | --- | --- |
| 上游 · 请求输入 | Bootstrap Servers | `upstream-kafka:9092` | 插件监听请求的 Kafka 集群 |
| 上游 · 请求输入 | 请求 Topic | `qwenpaw.requests` | 插件消费消息的 Topic |
| 上游 · 请求输入 | QwenPaw 消费组 | `qwenpaw-agent-default` | QwenPaw 消费请求时使用；每个 Agent 建议不同 |
| 上游 · 请求输入 | QwenPaw Client ID | `qwenpaw-kafka-consumer` | 标识 QwenPaw 的请求监听客户端 |
| 消息处理 | 最大并发处理数 | `5` | 最多同时交给 Agent 处理的请求数 |
| 下游 · 响应输出 | Bootstrap Servers | `downstream-kafka:9092` | Agent 结果发送到的 Kafka 集群，可以与上游不同 |
| 下游 · 响应输出 | 默认回复 Topic | `qwenpaw.responses` | 没有 `reply_to` 时发送到这里 |
| 下游 · 响应输出 | QwenPaw Client ID | `qwenpaw-kafka-producer` | QwenPaw 发送结果时使用 |

两侧分别支持 `PLAINTEXT`、SASL 和 SSL 配置。若使用同一个 Kafka 集群，只需在两套
Bootstrap Servers 和认证字段中填入相同内容。

保存并启用后，该配置只绑定到当前选中的 Agent。

## 原样输入输出（默认开启）

页面的“是否原样输入输出”默认开启：

- 上游 Kafka record value 解码为 UTF-8 文本后直接交给 Agent，不解析 JSON，
  也不删除首尾空格；
- 每条 Kafka 消息都会生成新的随机 `session_id`，不会继承上一条消息的上下文；
- 下游 Kafka record value 只包含 Agent 最终回答的 UTF-8 文本，不包装 JSON；
- 下游固定发送到“下游 · 默认回复 Topic”；消息映射和动态 `reply_to` 不参与处理。

例如，上游消息正文是：

```text
你是谁
```

Agent 最终回答为“我是 QwenPaw”时，下游消息正文就是：

```text
我是 QwenPaw
```

如果需要请求 ID、会话、动态回复 Topic、思考过程或工具详情，请关闭该开关，使用下面的
结构化 JSON 模式。

页面只使用“上游/下游”区分方向。为了兼容已经保存的 1.3.0 配置，JSON 内部字段名保持
不变：

- `producer_*`：上游请求输入端的集群和认证（历史字段名）。
- `consumer_*`：下游响应输出端的集群和认证（历史字段名）。
- `qwenpaw_consumer_group`、`qwenpaw_consumer_client_id`：QwenPaw 消费请求使用。
- `qwenpaw_producer_client_id`：QwenPaw 发布响应使用。

### 从 1.2.x 升级

1.2.x 的 `producer_*`、`consumer_*` 使用的是 QwenPaw 内部角色，含义与页面相反。
1.3.x 及 1.4.x 在运行时会识别旧配置，升级后不会把两个集群反接。保存前请确认：

- `上游 · Bootstrap Servers` 填请求输入集群；
- `下游 · Bootstrap Servers` 填响应输出集群；
- `上游 · QwenPaw 消费组` 填 QwenPaw 监听请求使用的消费组。

## 结构化 JSON 输入（关闭原样开关时）

推荐发送 JSON：

```json
{
  "message_id": "msg-001",
  "correlation_id": "request-001",
  "sender_id": "device-1001",
  "session_id": "order-20261015-001",
  "text": "请分析这个异常",
  "reply_to": "qwenpaw.responses.device-1001",
  "metadata": {
    "business": "device-monitor"
  }
}
```

字段说明：

- `text`：传给 Agent 的文本，必填。
- `sender_id`：调用方身份；省略时使用 Kafka record key。
- `session_id`：Agent 会话；相同值会延续上下文。省略时按 `sender_id` 建立会话。
- `message_id`：消息唯一标识；省略时使用 `topic:partition:offset`。
- `correlation_id`：请求响应关联标识；省略时等于 `message_id`。
- `reply_to`：本条消息的动态回复 Topic；省略时使用页面中的默认输出 Topic。该 Topic
  始终位于下游 Kafka 集群。
- `metadata`：可选透传元数据。

也可以直接发送纯文本。纯文本消息会用 Kafka record key 作为 `sender_id`。

## 结构化 JSON 输出（关闭原样开关时）

每个 Agent 请求只发布一条响应记录。开启“显示思考过程”时，思考过程和最终回答分别
写入同一个 JSON 的 `reasoning`、`answer` 字段；关闭时 `reasoning` 为空字符串。
可见的工具调用、工具结果和模型切换提示统一放在 `details` 数组中。

```json
{
  "type": "agent.response",
  "response_id": "2ee9b7d1-...",
  "message_id": "msg-001",
  "correlation_id": "request-001",
  "sender_id": "device-1001",
  "session_id": "kafka:order-20261015-001",
  "reasoning": "Agent 的思考过程",
  "answer": "Agent 的最终回复",
  "details": [],
  "timestamp_ms": 1792031400000
}
```

默认输出 Topic 支持以下模板变量：

```text
responses.{sender_id}
responses.{session_id}
responses.{correlation_id}
responses.{message_id}
```

如果输入消息含有 `reply_to`，它优先于默认输出 Topic。

## 消费和提交策略

- `enable_auto_commit=False`。
- 默认最多同时处理 5 条消息；可通过页面的“消息处理 · 最大并发处理数”调整。
- 并发任务可以按处理速度先后返回，因此下游回复顺序不保证与上游请求顺序一致。
- 每个分区只提交从最早待处理消息开始、已经连续完成的 offset；后面的消息先完成不会误确认前面仍在处理的消息。
- Agent 调用和下游发送都完成后，才会把对应消息标记为可提交。
- Kafka producer 开启 `acks=all` 和幂等发送。
- 同一进程内缓存最近 10000 个已处理 `message_id`，减少“处理成功但 offset 提交失败”造成的重复 Agent 调用。
- JSON 格式错误或没有文本字段的毒消息会记录错误并跳过，以免永久阻塞分区。

这不是跨进程的 exactly-once 事务：如果进程在 Agent 已执行、但 offset 尚未提交时崩溃，Kafka
可能重新投递，消费者应当使用 `message_id`/`correlation_id` 做最终幂等。

## 两套 SASL / SSL

支持：

- `PLAINTEXT`
- `SASL_PLAINTEXT`
- `SSL`
- `SASL_SSL`
- SASL `PLAIN`、`SCRAM-SHA-256`、`SCRAM-SHA-512`

上游与下游可以分别选择协议、机制、用户名、密码和证书。这里填写的认证信息均由
QwenPaw 使用：上游认证用于监听请求，下游认证用于发送 Agent 结果。
启用 SSL 时填写的是 QwenPaw 容器内可访问的绝对路径，而不是浏览器或 Kafka 服务器
上的路径。

版本 1.6.0 仍会读取 1.0.0 的单集群字段、1.2.x 的旧角色字段和 1.3.0 配置作为兼容
回退；升级后建议按上述上游/下游方向重新确认并保存配置。

## 本地单元测试

测试不需要 Kafka Broker，但需要在 QwenPaw 的 Python 环境中运行：

```bash
PYTHONPATH="/Users/hanhan/Downloads/object by myself/QwenPaw/src" \
  "/Users/hanhan/Downloads/object by myself/QwenPaw/.venv/bin/python" \
  /Users/hanhan/Downloads/qwenpaw-kafka-channel/tests/test_channel.py -v
```
