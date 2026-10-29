"""注册“运维助手”频道及其 HTTP 聊天回调路由。"""

from qwenpaw.plugins.api import PluginApi

from .channel import ServiceCallbackChannel
from .http_api import router


class ServiceChannelPlugin:
    def register(self, api: PluginApi) -> None:
        api.register_channel(
            channel_class=ServiceCallbackChannel,
            label="运维助手",
            description=(
                "启用时自动注册当前 Agent 的运维助手地址；关闭时自动取消注册。"
            ),
            config_fields=[
                {
                    "name": "registration_service_url",
                    "label": "服务注册接口 URL",
                    "type": "text",
                    "required": True,
                    "placeholder": "http://10.12.40.1:7070",
                    "help": (
                        "只填写协议、地址和端口；插件自动追加 "
                        "/channel/register。"
                    ),
                },
                {
                    "name": "registration_token",
                    "label": "注册接口 Bearer Token",
                    "type": "password",
                    "required": False,
                    "help": (
                        "注册和注销请求共用该 Token；服务未启用认证时留空。"
                    ),
                },
                {
                    "name": "qwenpaw_public_base_url",
                    "label": "QwenPaw 对外访问地址",
                    "type": "text",
                    "required": True,
                    "placeholder": "http://10.12.40.1:8089",
                    "help": (
                        "只填写外部服务能访问的 QwenPaw 协议、IP 和端口；"
                        "聊天、文件上传和健康检查路径由插件自动生成。"
                    ),
                },
                {
                    "name": "callback_token",
                    "label": "聊天回调 Bearer Token",
                    "type": "password",
                    "required": False,
                    "help": "外部服务调用聊天地址时使用；测试阶段可以留空。",
                },
                {
                    "name": "createdOn",
                    "label": "创建于 created_on",
                    "type": "text",
                    "required": False,
                    "default": "QwenPaw",
                    "placeholder": "QwenPaw",
                    "help": "注册来源描述；不填写时默认传 QwenPaw。",
                },
                {
                    "name": "request_timeout_seconds",
                    "label": "注册请求超时（秒）",
                    "type": "number",
                    "required": False,
                    "default": 30,
                    "min": 1,
                    "max": 300,
                },
                {
                    "name": "agent_timeout_seconds",
                    "label": "Agent 最大处理时间（秒）",
                    "type": "number",
                    "required": False,
                    "default": 1800,
                    "min": 1,
                    "max": 7200,
                },
                {
                    "name": "max_upload_mb",
                    "label": "单文件上传上限（MB）",
                    "type": "number",
                    "required": False,
                    "default": 50,
                    "min": 1,
                    "max": 2048,
                    "help": (
                        "文件上传接口（/api/service-channel/files）单个文件"
                        "的大小上限；超出时返回 HTTP 413。"
                    ),
                },
            ],
        )
        api.register_http_router(
            router,
            prefix="/service-channel",
            tags=["Operations Assistant"],
        )


plugin = ServiceChannelPlugin()
