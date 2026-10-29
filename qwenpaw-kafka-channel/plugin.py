"""Kafka Channel plugin entry point."""

from qwenpaw.plugins.api import PluginApi


def _security_fields(prefix: str, zh_role: str, en_role: str) -> list[dict]:
    """生成一组带上游/下游前缀的 Kafka 认证配置。"""
    label_prefix = f"{zh_role} · "
    en_prefix = f"{en_role} · "
    return [
        {
            "name": f"{prefix}_security_protocol",
            "label": {
                "zh": label_prefix + "安全协议",
                "en": en_prefix + "Security Protocol",
            },
            "type": "select",
            "default": "PLAINTEXT",
            "options": ["PLAINTEXT", "SASL_PLAINTEXT", "SSL", "SASL_SSL"],
        },
        {
            "name": f"{prefix}_sasl_mechanism",
            "label": {
                "zh": label_prefix + "SASL 机制",
                "en": en_prefix + "SASL Mechanism",
            },
            "type": "select",
            "default": "PLAIN",
            "options": ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"],
        },
        {
            "name": f"{prefix}_sasl_username",
            "label": {
                "zh": label_prefix + "SASL 用户名",
                "en": en_prefix + "SASL Username",
            },
            "type": "text",
            "required": False,
        },
        {
            "name": f"{prefix}_sasl_password",
            "label": {
                "zh": label_prefix + "SASL 密码",
                "en": en_prefix + "SASL Password",
            },
            "type": "password",
            "required": False,
        },
        {
            "name": f"{prefix}_ssl_cafile",
            "label": {
                "zh": label_prefix + "CA 证书路径",
                "en": en_prefix + "CA Certificate Path",
            },
            "type": "text",
            "required": False,
            "placeholder": "/path/to/ca.pem",
        },
        {
            "name": f"{prefix}_ssl_certfile",
            "label": {
                "zh": label_prefix + "客户端证书路径",
                "en": en_prefix + "Client Certificate Path",
            },
            "type": "text",
            "required": False,
            "placeholder": "/path/to/client.pem",
        },
        {
            "name": f"{prefix}_ssl_keyfile",
            "label": {
                "zh": label_prefix + "客户端私钥路径",
                "en": en_prefix + "Client Key Path",
            },
            "type": "text",
            "required": False,
            "placeholder": "/path/to/client.key",
        },
    ]


class KafkaChannelPlugin:
    """Register the Kafka-backed messaging channel."""

    def register(self, api: PluginApi) -> None:
        from .channel import KafkaChannel

        api.register_channel(
            channel_class=KafkaChannel,
            label="Kafka",
            description=(
                "上游：监听 Kafka 请求并交给 Agent 处理；"
                "下游：把 Agent 的处理结果发送到目标 Kafka。"
            ),
            doc_url={
                "zh": "https://kafka.apache.org/documentation/",
                "en": "https://kafka.apache.org/documentation/",
            },
            config_fields=[
                {
                    "name": "producer_bootstrap_servers",
                    "label": {
                        "zh": "上游 · Bootstrap Servers",
                        "en": "Upstream · Bootstrap Servers",
                    },
                    "type": "text",
                    "required": True,
                    "placeholder": "input-kafka-1:9092,input-kafka-2:9092",
                    "help": {
                        "zh": "请求输入所在的 Kafka 集群；QwenPaw 会连接并监听这个集群。",
                        "en": "Kafka cluster containing incoming requests; QwenPaw consumes from this cluster.",
                    },
                },
                {
                    "name": "input_topic",
                    "label": {
                        "zh": "上游 · 请求 Topic",
                        "en": "Upstream · Request Topic",
                    },
                    "type": "text",
                    "required": True,
                    "placeholder": "qwenpaw.agent.requests",
                },
                {
                    "name": "qwenpaw_consumer_group",
                    "label": {
                        "zh": "上游 · QwenPaw 消费组",
                        "en": "Upstream · QwenPaw Consumer Group",
                    },
                    "type": "text",
                    "required": True,
                    "default": "qwenpaw-agent",
                    "help": {
                        "zh": "QwenPaw 监听上游请求时使用的 Consumer Group；每个需要独立收到全部请求的 Agent 应使用不同的值。",
                        "en": "Use a distinct group for each Agent that must receive every request.",
                    },
                },
                {
                    "name": "qwenpaw_consumer_client_id",
                    "label": {
                        "zh": "上游 · QwenPaw Client ID",
                        "en": "Upstream · QwenPaw Client ID",
                    },
                    "type": "text",
                    "default": "qwenpaw-kafka-consumer",
                },
                {
                    "name": "auto_offset_reset",
                    "label": {
                        "zh": "上游 · 初始 Offset",
                        "en": "Upstream · Auto Offset Reset",
                    },
                    "type": "select",
                    "default": "latest",
                    "options": ["latest", "earliest"],
                },
                *_security_fields("producer", "上游", "Upstream"),
                {
                    "name": "publish_to_downstream",
                    "label": {
                        "zh": "是否开启下游",
                        "en": "Downstream · Publish Agent Results",
                    },
                    "type": "switch",
                    "default": True,
                    "help": {
                        "zh": "关闭后仍会监听上游 Kafka 并交给 Agent 处理，但不会连接下游 Kafka，也不会推送处理结果。处理成功后仍会提交上游 offset。",
                        "en": "When disabled, upstream requests are still consumed and processed by the Agent, but no downstream Kafka connection or result publish is performed. Successfully processed upstream offsets are still committed.",
                    },
                },
                {
                    "name": "consumer_bootstrap_servers",
                    "label": {
                        "zh": "下游 · Bootstrap Servers",
                        "en": "Downstream · Bootstrap Servers",
                    },
                    "type": "text",
                    "required": False,
                    "placeholder": "output-kafka-1:9092,output-kafka-2:9092",
                    "help": {
                        "zh": "Agent 处理结果要发送到的 Kafka 集群；可以与上游集群不同。",
                        "en": "Kafka cluster for Agent replies; it may differ from the input cluster.",
                    },
                },
                {
                    "name": "output_topic",
                    "label": {
                        "zh": "下游 · 默认回复 Topic",
                        "en": "Downstream · Default Reply Topic",
                    },
                    "type": "text",
                    "required": False,
                    "placeholder": "qwenpaw.agent.responses",
                    "help": {
                        "zh": "上游消息未提供 reply_to 时使用；动态 reply_to 指定的 Topic 也必须位于下游集群。",
                        "en": "Used when reply_to is empty. Dynamic reply_to topics must also exist in the downstream cluster.",
                    },
                },
                {
                    "name": "qwenpaw_producer_client_id",
                    "label": {
                        "zh": "下游 · QwenPaw Client ID",
                        "en": "Downstream · QwenPaw Client ID",
                    },
                    "type": "text",
                    "default": "qwenpaw-kafka-producer",
                    "help": {
                        "zh": "QwenPaw 向下游发送结果时使用的 Producer Client ID；接收方的消费组由下游系统自行配置。",
                        "en": "Client ID used by QwenPaw to publish replies; the downstream system owns its own consumer group.",
                    },
                },
                *_security_fields("consumer", "下游", "Downstream"),
                {
                    "name": "text_field",
                    "label": {
                        "zh": "消息映射 · 文本字段",
                        "en": "Message Mapping · Text Field",
                    },
                    "type": "text",
                    "default": "text",
                },
                {
                    "name": "reply_topic_field",
                    "label": {
                        "zh": "消息映射 · 动态回复 Topic 字段",
                        "en": "Message Mapping · Reply Topic Field",
                    },
                    "type": "text",
                    "default": "reply_to",
                },
                {
                    "name": "sender_id_field",
                    "label": {
                        "zh": "消息映射 · 发送者字段",
                        "en": "Message Mapping · Sender ID Field",
                    },
                    "type": "text",
                    "default": "sender_id",
                },
                {
                    "name": "session_id_field",
                    "label": {
                        "zh": "消息映射 · 会话字段",
                        "en": "Message Mapping · Session ID Field",
                    },
                    "type": "text",
                    "default": "session_id",
                },
                {
                    "name": "message_id_field",
                    "label": {
                        "zh": "消息映射 · 消息 ID 字段",
                        "en": "Message Mapping · Message ID Field",
                    },
                    "type": "text",
                    "default": "message_id",
                },
                {
                    "name": "correlation_id_field",
                    "label": {
                        "zh": "消息映射 · 关联 ID 字段",
                        "en": "Message Mapping · Correlation ID Field",
                    },
                    "type": "text",
                    "default": "correlation_id",
                },
                {
                    "name": "max_poll_interval_ms",
                    "label": {
                        "zh": "消息处理 · 最大处理时间（毫秒）",
                        "en": "Processing · Max Poll Interval (ms)",
                    },
                    "type": "number",
                    "default": 1800000,
                    "help": {
                        "zh": "单条 Agent 请求允许的最长处理时间，默认 30 分钟。",
                        "en": "Maximum Agent processing time per request; defaults to 30 minutes.",
                    },
                },
                {
                    "name": "max_concurrent_messages",
                    "label": {
                        "zh": "消息处理 · 最大并发处理数",
                        "en": "Processing · Maximum Concurrency",
                    },
                    "type": "number",
                    "default": 5,
                    "help": {
                        "zh": "最多同时交给 Agent 处理的 Kafka 消息数，默认 5。并发时回复顺序可能与请求顺序不同。",
                        "en": "Maximum Kafka messages processed by the Agent at once. Defaults to 5; replies may complete out of order.",
                    },
                },
                {
                    "name": "raw_input_output",
                    "label": {
                        "zh": "是否原样输入输出",
                        "en": "Raw Input and Output",
                    },
                    "type": "switch",
                    "default": True,
                    "help": {
                        "zh": "默认开启：上游 Kafka 消息正文原样交给 Agent，每条消息使用全新的随机会话；下游只发送 Agent 最终回答文本，不包装 JSON。关闭后使用上面的消息字段映射和 JSON 响应格式。",
                        "en": "Enabled by default: pass the upstream Kafka value directly to the Agent using a fresh random session for every record, and publish only the final answer as plain text. Disable it to use field mapping and the JSON response envelope.",
                    },
                },
            ],
        )


plugin = KafkaChannelPlugin()
