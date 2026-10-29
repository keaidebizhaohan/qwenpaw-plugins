"""运维助手频道的生命周期与外部注册逻辑。

本模块主要负责读取页面配置、取得当前绑定的 Agent ID 和名称、生成唯一的
聊天回调地址，并将这些信息提交给外部注册中心。真正接收 HTTP 聊天请求、
调用 Agent 的逻辑位于 ``http_api.py``。
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from qwenpaw.app.channels.base import BaseChannel, OnReplySent, ProcessHandler
from qwenpaw.app.channels.renderer import ChannelDisplayConfig
from qwenpaw.schemas import ContentType, TextContent

logger = logging.getLogger(__name__)

# 固定服务协议。页面只填写地址和端口，插件负责追加接口路径。
SERVICE_CODE = "operations-assistant"
SERVICE_NAME = "Operations Assistant"
REGISTER_FROM = "QwenPaw"
REGISTER_PATH = "/channel/register"
UNREGISTER_PATH = "/channel/deregister"

# 当前 QwenPaw 进程中已经激活的 Agent 映射。
# http_api.py 根据请求中的 agentId 在这里找到对应的频道实例。
ACTIVE_CHANNELS: dict[str, "ServiceCallbackChannel"] = {}


# 上传文件的进程内注册表：fileId -> 文件元信息。
# 文件本体保存在 Agent workspace 的 media/service-channel/<fileId>/ 目录下，
# 注册表只保存定位与校验所需的最小信息。QwenPaw 重启后注册表会清空，
# 磁盘上的孤儿文件不再能被 chat 引用，可由运维定期清理。
FILE_REGISTRY: dict[str, dict[str, Any]] = {}


class FileResolveError(Exception):
    """chat 引用 fileId 校验失败；字段用于映射 HTTP 状态码。"""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def sanitize_filename(filename: Any) -> str:
    """把上传的文件名清洗成可以安全写入磁盘的名字。

    只保留最后一个路径段（同时兼容 Windows 反斜杠与 URL 分隔符），
    去掉控制字符与不可打印字符，排除 "." 和 ".."，并限制长度；
    清洗后为空时返回空字符串，由调用方决定如何报错。
    """
    name = str(filename or "").strip().replace("\\", "/")
    name = name.rsplit("/", 1)[-1].strip()
    name = "".join(
        char for char in name if char.isprintable() and char != "\x00"
    )
    name = name.strip()
    if name in {"", ".", ".."}:
        return ""
    # 超长时尽量保留扩展名，便于 Agent 识别文件类型。
    max_length = 200
    if len(name) > max_length:
        stem, dot, ext = name.rpartition(".")
        if dot and 0 < len(ext) < 20:
            name = stem[: max_length - len(ext) - 1] + "." + ext
        else:
            name = name[:max_length]
    return name


def register_uploaded_file(
    *,
    file_id: str,
    path: Path,
    filename: str,
    size: int,
    agent_id: str,
    user_id: str | None,
) -> dict[str, Any]:
    """把一个已落盘的上传文件登记进进程内注册表，返回注册条目。"""
    entry = {
        "file_id": file_id,
        "path": path,
        "filename": filename,
        "size": size,
        "agent_id": agent_id,
        # user_id 为空表示“不限定归属用户”，该 Agent 的任何调用方都可用。
        "user_id": (user_id or "").strip() or None,
        "created_at": time.time(),
    }
    FILE_REGISTRY[file_id] = entry
    return entry


def resolve_file_block(
    file_id: str,
    agent_id: str,
    user_id: str | None,
) -> dict[str, Any]:
    """校验 chat 中引用的 fileId，返回注册表条目。

    校验规则：

    - fileId 不存在，或不属于当前 Agent：404（不泄露其他 Agent 的文件）；
    - 上传时记录了归属用户，且本次请求携带了不同的 userId：403；
    - 磁盘文件已被清理：410，并顺手把失效条目移出注册表。
    """
    entry = FILE_REGISTRY.get(str(file_id or ""))
    if entry is None or entry["agent_id"] != agent_id:
        raise FileResolveError(404, f"Unknown fileId for this Agent: {file_id}")
    owner = entry["user_id"]
    if owner and user_id and owner != user_id:
        raise FileResolveError(403, "fileId does not belong to this user")
    if not Path(entry["path"]).is_file():
        FILE_REGISTRY.pop(str(file_id), None)
        raise FileResolveError(410, "Uploaded file is no longer available")
    return entry


def _value(config: Any, name: str, default: Any = None) -> Any:
    """同时兼容字典配置和 Pydantic/对象配置。"""
    if isinstance(config, dict):
        return config.get(name, default)
    return getattr(config, name, default)


def _text(config: Any, name: str, default: str = "") -> str:
    """读取配置并转换成去除首尾空格的字符串。"""
    value = _value(config, name, default)
    return str(default if value is None else value).strip()


def _integer(config: Any, name: str, default: int) -> int:
    """读取整数配置；格式不正确时回退到默认值。"""
    try:
        return int(_value(config, name, default))
    except (TypeError, ValueError):
        return default


def _endpoint(base_url: str, path: str) -> str:
    """将页面填写的协议、地址和端口与代码内固定路径拼接。"""
    return f"{base_url.rstrip('/')}{path}"


def _request_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None,
    token: str,
    timeout: int,
) -> tuple[int, Any]:
    """使用标准库向注册中心发送 JSON HTTP 请求。

    token 不为空时自动添加 ``Authorization: Bearer <token>``。使用 urllib
    是为了让插件不依赖 requests/httpx 等额外安装包。
    """
    headers = {"Accept": "application/json"}
    data = None
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(url, data=data, headers=headers, method=method)
    with urlopen(request, timeout=timeout) as response:  # noqa: S310
        raw = response.read(1024 * 1024)
        text = raw.decode(
            response.headers.get_content_charset() or "utf-8",
            errors="replace",
        )
        try:
            body = json.loads(text) if text else None
        except json.JSONDecodeError:
            body = text
        return response.status, body


class ServiceCallbackChannel(BaseChannel):
    """将当前 Agent 注册为一个可通过 HTTP 调用的运维助手服务。"""

    # 使用稳定的 ASCII 频道键，供 QwenPaw 配置保存接口和运行时识别。
    # 插件的人类可读名称仍由 plugin.json 和 plugin.py 声明为“运维助手”。
    channel = "operations-assistant"

    # 不使用 QwenPaw 普通频道消息队列；HTTP 路由会直接调用 Agent。
    uses_manager_queue = False

    def __init__(
        self,
        process: ProcessHandler,
        *,
        enabled: bool,
        registration_service_url: str,
        qwenpaw_public_base_url: str,
        registration_token: str = "",
        callback_token: str = "",
        created_on: str = "QwenPaw",
        request_timeout_seconds: int = 30,
        agent_timeout_seconds: int = 1800,
        max_upload_mb: int = 50,
        on_reply_sent: OnReplySent = None,
        display_config: ChannelDisplayConfig | None = None,
        no_text_debounce: bool = True,
        **kwargs: Any,
    ) -> None:
        """保存页面配置并初始化频道；此时还没有执行外部注册。"""
        del kwargs
        super().__init__(
            process,
            on_reply_sent=on_reply_sent,
            display_config=display_config,
            no_text_debounce=no_text_debounce,
        )
        self.enabled = enabled
        self.registration_service_url = registration_service_url.rstrip("/")
        self.qwenpaw_public_base_url = qwenpaw_public_base_url.rstrip("/")

        # registration_token 保护“QwenPaw → 注册中心”的注册请求；
        # callback_token 保护“注册中心/调用方 → QwenPaw”的聊天请求。
        self.registration_token = registration_token
        self.callback_token = callback_token
        self.created_on = created_on.strip() or "QwenPaw"

        # 注册请求和 Agent 聊天分别使用不同的超时配置，并限制合理范围。
        self.request_timeout_seconds = max(1, min(request_timeout_seconds, 300))
        self.agent_timeout_seconds = max(1, min(agent_timeout_seconds, 7200))
        # 单个上传文件的大小上限（MB）；文件上传接口按它做 413 拦截。
        self.max_upload_mb = max(1, min(int(max_upload_mb), 2048))
        self.agent_id = ""
        self.agent_name = ""
        self.agent_description = ""
        self.callback_url = ""
        self.file_upload_url = ""
        self.health_url = ""

        # 只有全部外部注册成功后才会设置为 True。
        self._registered = False

    @classmethod
    def from_config(
        cls,
        process: ProcessHandler,
        config: Any,
        on_reply_sent: OnReplySent = None,
        display_config: ChannelDisplayConfig | None = None,
        no_text_debounce: bool = True,
        workspace_dir: Path | None = None,
        **kwargs: Any,
    ) -> "ServiceCallbackChannel":
        """由 QwenPaw 调用，将页面配置转换成频道实例。"""
        del workspace_dir, kwargs
        return cls(
            process,
            enabled=bool(_value(config, "enabled", False)),
            registration_service_url=_text(config, "registration_service_url"),
            qwenpaw_public_base_url=_text(config, "qwenpaw_public_base_url"),
            registration_token=_text(config, "registration_token"),
            callback_token=_text(config, "callback_token"),
            created_on=(
                _text(config, "createdOn")
                or _text(config, "created_on", "QwenPaw")
                or "QwenPaw"
            ),
            request_timeout_seconds=_integer(
                config,
                "request_timeout_seconds",
                30,
            ),
            agent_timeout_seconds=_integer(
                config,
                "agent_timeout_seconds",
                1800,
            ),
            max_upload_mb=_integer(config, "max_upload_mb", 50),
            on_reply_sent=on_reply_sent,
            display_config=display_config or ChannelDisplayConfig.from_config(config),
            no_text_debounce=no_text_debounce,
        )

    @classmethod
    def from_env(
        cls,
        process: ProcessHandler,
        on_reply_sent: OnReplySent = None,
    ) -> "ServiceCallbackChannel":
        """本插件必须通过页面配置，不支持只从环境变量创建。"""
        raise RuntimeError("运维助手频道必须通过页面配置")

    def _validate(self) -> None:
        """连接服务中心前检查必填地址。"""
        missing = [
            name
            for name, value in (
                ("registration_service_url", self.registration_service_url),
                ("qwenpaw_public_base_url", self.qwenpaw_public_base_url),
            )
            if not value
        ]
        if missing:
            raise ValueError("缺少必填配置：" + ", ".join(missing))
        if not self.registration_service_url.startswith(("http://", "https://")):
            raise ValueError("服务注册接口必须是 http:// 或 https:// 地址")
        if not self.qwenpaw_public_base_url.startswith(("http://", "https://")):
            raise ValueError("QwenPaw 对外地址必须是 http:// 或 https:// 地址")

    def _registration_payload(self) -> dict[str, Any]:
        """生成提交给服务注册接口的完整 Agent 服务信息。"""
        return {
            "serviceCode": SERVICE_CODE,
            "serviceName": SERVICE_NAME,
            "agentId": self.agent_id,
            "agentName": self.agent_name,
            "agentDescription": self.agent_description,
            "channel": self.channel,
            "chatWebhook": self.callback_url,
            "fileUploadWebhook": self.file_upload_url,
            "healthUrl": self.health_url,
            "requestFormat": "QWENPAW_CHAT",
            "responseFormat": "AG_UI_SSE_1_0",
            "callbackToken": self.callback_token,
            "registerFrom": REGISTER_FROM,
            "createdOn": self.created_on,
        }

    def _unregistration_payload(self) -> dict[str, Any]:
        """生成服务注销接口要求的请求体。"""
        return {
            "agentId": self.agent_id,
            "registerFrom": REGISTER_FROM,
            "createdOn": self.created_on,
        }

    def _load_agent_identity(self) -> None:
        """从当前 Workspace 自动取得 Agent ID 和人类可读名称。"""
        workspace = getattr(self, "_workspace", None)
        self.agent_id = str(getattr(workspace, "agent_id", "") or "default")
        config = getattr(workspace, "config", None)
        self.agent_name = str(getattr(config, "name", "") or self.agent_id)
        self.agent_description = str(getattr(config, "description", "") or "")

    def _build_public_urls(self) -> None:
        """根据 QwenPaw 对外地址和 Agent ID 生成对外服务 URL。"""
        query = urlencode({"agentId": self.agent_id})
        self.callback_url = _endpoint(
            self.qwenpaw_public_base_url,
            f"/api/service-channel/chat?{query}",
        )
        self.file_upload_url = _endpoint(
            self.qwenpaw_public_base_url,
            f"/api/service-channel/files?{query}",
        )
        self.health_url = _endpoint(
            self.qwenpaw_public_base_url,
            f"/api/service-channel/health?{query}",
        )

    async def start(self) -> None:
        """启用时向服务中心注册当前 Agent。"""
        if self._registered:
            # 防止同一个频道实例重复注册。
            return
        self._validate()

        self._load_agent_identity()
        self._build_public_urls()

        # QwenPaw 不会为 enabled=false 的配置创建频道实例。关闭保存产生的
        # 外部注销由旧活动实例的 stop() 完成。
        if not self.enabled:
            return

        existing = ACTIVE_CHANNELS.get(self.agent_id)
        if existing is not None and existing is not self:
            if existing.agent_id != self.agent_id:
                raise RuntimeError(f"Agent {self.agent_id!r} 已被另一个频道实例使用")
        try:
            status, body = await asyncio.to_thread(
                _request_json,
                "POST",
                _endpoint(self.registration_service_url, REGISTER_PATH),
                self._registration_payload(),
                self.registration_token,
                self.request_timeout_seconds,
            )
            if status < 200 or status >= 300:
                raise RuntimeError(f"服务注册接口返回 HTTP {status}: {body}")
        except (HTTPError, URLError, TimeoutError) as exc:
            raise RuntimeError(f"调用外部注册服务失败：{exc}") from exc

        # 外部注册成功后才发布到进程内映射。
        ACTIVE_CHANNELS[self.agent_id] = self
        self._registered = True
        logger.info(
            "Operations Assistant registered: agent=%s name=%s",
            self.agent_id,
            self.agent_name,
        )

    async def stop(self) -> None:
        """停止频道；仅当前活动实例负责向服务中心注销。

        保存配置时，QwenPaw 会先启动新工作区，再停止旧工作区。因此：

        - 启用配置更新：新实例已经接管映射，旧实例不能注销；
        - 启用改为停用：没有新实例接管，旧实例在这里执行注销。
        """
        if ACTIVE_CHANNELS.get(self.agent_id) is not self:
            self._registered = False
            return

        try:
            status, body = await asyncio.to_thread(
                _request_json,
                "POST",
                _endpoint(
                    self.registration_service_url,
                    UNREGISTER_PATH,
                ),
                self._unregistration_payload(),
                self.registration_token,
                self.request_timeout_seconds,
            )
            if status < 200 or status >= 300:
                raise RuntimeError(
                    f"服务取消接口返回 HTTP {status}: {body}",
                )
            logger.info(
                "Operations Assistant unregistered: agent=%s name=%s",
                self.agent_id,
                self.agent_name,
            )
        except Exception:
            # 停止工作区不能因为外部服务暂时不可用而失败。错误写入日志；
            # 注册中心可以通过 health_url 发现并清理陈旧注册记录。
            logger.exception(
                "Failed to unregister Operations Assistant for agent '%s'",
                self.agent_id,
            )
        finally:
            ACTIVE_CHANNELS.pop(self.agent_id, None)
            self._registered = False

    def build_agent_request_from_native(self, native_payload: Any) -> Any:
        """将普通原生消息转换成 BaseChannel 的 AgentRequest。

        当前 HTTP 回调直接使用 ctx.chat_stream()，通常不会走该方法；保留它是为了
        满足标准频道接口，也方便以后接入普通频道消息队列。
        """
        payload = native_payload if isinstance(native_payload, dict) else {}
        sender_id = str(payload.get("sender_id") or "service-user")
        session_id = str(payload.get("session_id") or f"service:{sender_id}")
        text = str(payload.get("text") or "")
        request = self.build_agent_request_from_user_content(
            channel_id=self.channel,
            sender_id=sender_id,
            session_id=session_id,
            content_parts=[TextContent(type=ContentType.TEXT, text=text)],
            channel_meta=payload.get("meta") or {},
        )
        request.channel_meta = payload.get("meta") or {}
        return request

    async def send(
        self,
        to_handle: str,
        text: str,
        meta: dict[str, Any] | None = None,
    ) -> None:
        """标准频道主动发送接口；同步 HTTP 回调模式不需要发送实现。"""
        del to_handle, text, meta

    async def health_check(self) -> dict[str, Any]:
        """返回运维助手注册状态和当前 Agent 服务地址。"""
        return {
            "channel": self.channel,
            "status": "healthy" if self._registered else "unhealthy",
            "detail": "registered" if self._registered else "not registered",
            "serviceCode": SERVICE_CODE,
            "serviceName": SERVICE_NAME,
            "agentId": self.agent_id,
            "agentName": self.agent_name,
            "chatWebhook": self.callback_url,
            "fileUploadWebhook": self.file_upload_url,
            "healthUrl": self.health_url,
        }
