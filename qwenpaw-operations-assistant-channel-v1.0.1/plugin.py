"""注册运维助手频道及其始终可访问的健康检查路由。"""

from qwenpaw.plugins.api import PluginApi

from .channel import ServiceCallbackChannel
from .health_api import router


class OperationsAssistantChannelPlugin:
    """负责启停时向外部服务注册/注销当前 Agent。"""

    def register(self, api: PluginApi) -> None:
        api.register_channel(
            channel_class=ServiceCallbackChannel,
            label="运维助手",
            description=(
                "启用时注册当前 Agent 的运维助手 HTTP 地址；"
                "关闭时取消外部注册。HTTP 地址由独立插件持续提供。"
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
                        "/channel/register 或 /channel/deregister。"
                    ),
                },
                {
                    "name": "registration_token",
                    "label": "注册接口 Bearer Token",
                    "type": "password",
                    "required": False,
                    "help": "注册与注销共用；外部服务未启用认证时留空。",
                },
                {
                    "name": "qwenpaw_public_base_url",
                    "label": "QwenPaw 对外访问地址",
                    "type": "text",
                    "required": True,
                    "placeholder": "http://10.12.40.1:8089",
                    "help": (
                        "外部服务可访问的 QwenPaw 地址；插件只用它生成"
                        "聊天、文件上传和健康检查 URL。"
                    ),
                },
                {
                    "name": "created_on",
                    "label": "创建于 created_on",
                    "type": "text",
                    "required": False,
                    "default": "QwenPaw",
                    "placeholder": "QwenPaw",
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
            ],
        )
        api.register_http_router(
            router,
            prefix="/operations-assistant-channel",
            tags=["Operations Assistant Channel Health"],
        )


plugin = OperationsAssistantChannelPlugin()

