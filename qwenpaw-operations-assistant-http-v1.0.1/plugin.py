"""注册始终可用的运维助手 HTTP 路由。"""

from qwenpaw.plugins.api import PluginApi

from .http_api import router


class OperationsAssistantHttpPlugin:
    """只注册 HTTP API，不注册频道，也不参与注册/注销生命周期。"""

    def register(self, api: PluginApi) -> None:
        api.register_http_router(
            router,
            prefix="/service-channel",
            tags=["Operations Assistant HTTP API"],
        )


plugin = OperationsAssistantHttpPlugin()
