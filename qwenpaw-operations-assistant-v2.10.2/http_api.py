"""服务回调频道对外暴露的 HTTP API。

本模块根据 URL 查询参数中的 Agent ID 找到已激活的频道实例，校验回调 Token，
从 QwenPaw 请求体提取用户文本与文件引用并调用绑定的 Agent，最后将回答转换成
AG-UI 1.0 SSE 事件流。

除 /health 和 /chat 外，本插件还提供：

- POST /files：multipart 文件上传，返回 fileId 供聊天引用；
- /chat 的 input 中支持 ``{"type": "file", "fileId": ...}`` 内容块；
  携带文件块时改走 ``workspace.stream_query(AgentRequest)`` 路径，
  其余请求行为与 2.7.0 完全一致。
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
import uuid
from pathlib import Path
from typing import Any, Optional

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    Header,
    HTTPException,
    Query,
    UploadFile,
)
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from qwenpaw.pawapp import get_ctx

from .channel import (
    ACTIVE_CHANNELS,
    FileResolveError,
    register_uploaded_file,
    resolve_file_block,
    sanitize_filename,
)
from .widget_stream import ParsedPart, WidgetStreamParser

logger = logging.getLogger(__name__)

# plugin.py 会将这个路由器挂载到 /api/service-channel 前缀下。
router = APIRouter()


class QwenPawChatRequest(BaseModel):
    """外部服务调用聊天回调时提交的 QwenPaw 格式请求体。"""

    # 忽略当前版本不认识的额外字段，便于以后兼容扩展后的请求格式。
    model_config = ConfigDict(extra="ignore", populate_by_name=False)

    # 调用方用户标识，用于把 QwenPaw 会话归属到具体用户。
    user_id: str = Field(alias="userId", min_length=1)

    # 会话标识；同一个值可以延续同一段 Agent 对话上下文。
    session_id: str = Field(alias="sessionId", min_length=1)

    # QwenPaw 消息数组，当前实现从中选取最后一条 user 文本消息。
    input: list[dict[str, Any]]


def _now_ms() -> int:
    """返回 AG-UI 事件使用的毫秒时间戳。"""
    return int(time.time() * 1000)


def _event(event_type: str, **fields: Any) -> dict[str, Any]:
    """创建包含事件类型和时间戳的 AG-UI 事件。"""
    return {"type": event_type, "timestamp": _now_ms(), **fields}


def _sse(event: dict[str, Any]) -> str:
    """将一个 JSON 事件编码成 Server-Sent Events 的一帧。"""
    return "data: " + json.dumps(
        event,
        ensure_ascii=False,
        separators=(",", ":"),
    ) + "\n\n"


def _content_text(content: Any) -> str:
    """提取一条 message.content 中所有 text 类型内容。"""
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    return "\n".join(
        str(part.get("text"))
        for part in content
        if isinstance(part, dict)
        and part.get("type") == "text"
        and part.get("text") is not None
    )


def _latest_user_text(messages: list[dict[str, Any]]) -> str:
    """从 input 末尾向前查找最近一条非空用户文本。"""
    for message in reversed(messages):
        if isinstance(message, dict) and message.get("role") == "user":
            text = _content_text(message.get("content")).strip()
            if text:
                return text
    return ""


def _extract_file_ids(messages: list[dict[str, Any]]) -> list[str]:
    """按顺序收集 input 中所有 file 块引用的 fileId（去重保序）。

    文件块格式：``{"type": "file", "fileId": "<上传接口返回的 ID>"}``。
    其他类型的块、非用户消息和格式不完整的块都会被忽略。
    """
    file_ids: list[str] = []
    for message in messages:
        if not isinstance(message, dict) or message.get("role") != "user":
            continue
        content = message.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            file_id = (
                part.get("fileId")
                if isinstance(part, dict)
                and part.get("type") == "file"
                else None
            )
            if isinstance(file_id, str) and file_id and file_id not in file_ids:
                file_ids.append(file_id)
    return file_ids


def _value(item: Any, name: str, default: Any = None) -> Any:
    """同时读取 QwenPaw Pydantic 事件和兼容的字典事件字段。"""
    if isinstance(item, dict):
        return item.get(name, default)
    return getattr(item, name, default)


def _enum_value(value: Any) -> Any:
    """把枚举值转换为协议中的字符串值。"""
    return getattr(value, "value", value)


def _as_dict(value: Any) -> dict[str, Any]:
    """把 Pydantic 数据对象或普通字典统一转换成字典。"""
    if isinstance(value, dict):
        return value
    if hasattr(value, "model_dump"):
        dumped = value.model_dump(exclude_none=True)
        return dumped if isinstance(dumped, dict) else {}
    return {}


def _text_preview(value: Any, limit: int) -> str:
    """将工具参数/结果转换为文本，并按页面配置限制展示长度。"""
    if isinstance(value, str):
        text = value
    else:
        text = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if limit <= 0 or len(text) <= limit:
        return text
    return text[:limit] + "..."


class _AgUiStreamBridge:
    """把 QwenPaw 原生流事件逐条转换为外部服务使用的 AG-UI 事件。"""

    def __init__(self, run_id: str, metadata: dict[str, Any], channel: Any):
        self.run_id = run_id
        self.metadata = metadata
        display = getattr(channel, "_display_config", None)
        self.show_thinking = bool(getattr(display, "show_thinking", True))
        self.show_tool_calls = bool(getattr(display, "show_tool_calls", True))
        self.show_tool_results = bool(getattr(display, "show_tool_results", True))
        self.tool_call_max_length = int(
            getattr(display, "tool_call_max_length", 200),
        )
        self.tool_result_max_length = int(
            getattr(display, "tool_result_max_length", 500),
        )
        self.message_types: dict[str, str] = {}
        self.active_thinking: set[str] = set()
        self.active_text: set[str] = set()
        self.tool_calls: dict[str, dict[str, Any]] = {}
        self.tool_results: dict[str, dict[str, Any]] = {}
        self.answer_parts: list[str] = []
        self.last_answer_message_id = ""
        # 每条助手消息独立维护标签解析状态，支持标签跨任意文本分片。
        self.widget_parsers: dict[str, WidgetStreamParser] = {}
        self.saw_assistant_content = False

    def event(self, event_type: str, **fields: Any) -> dict[str, Any]:
        """创建带当前 runId 的 AG-UI 事件。"""
        return _event(event_type, runId=self.run_id, **fields)

    def _start_thinking(self, message_id: str) -> list[dict[str, Any]]:
        if not self.show_thinking or message_id in self.active_thinking:
            return []
        self.active_thinking.add(message_id)
        return [
            self.event("THINKING_START", messageId=message_id),
            self.event("THINKING_TEXT_MESSAGE_START", messageId=message_id),
        ]

    def _end_thinking(self, message_id: str) -> list[dict[str, Any]]:
        if message_id not in self.active_thinking:
            return []
        self.active_thinking.remove(message_id)
        return [
            self.event("THINKING_TEXT_MESSAGE_END", messageId=message_id),
            self.event("THINKING_END", messageId=message_id),
        ]

    def _start_text(self, message_id: str) -> list[dict[str, Any]]:
        if message_id in self.active_text:
            return []
        self.active_text.add(message_id)
        self.last_answer_message_id = message_id
        return [
            self.event(
                "TEXT_MESSAGE_START",
                messageId=message_id,
                role="ASSISTANT",
                metadata=self.metadata,
            ),
        ]

    def _end_text(self, message_id: str) -> list[dict[str, Any]]:
        if message_id not in self.active_text:
            return []
        self.active_text.remove(message_id)
        return [self.event("TEXT_MESSAGE_END", messageId=message_id)]

    def _parsed_part_events(
        self,
        message_id: str,
        parts: list[ParsedPart],
    ) -> list[dict[str, Any]]:
        """把标签解析结果转换为普通文本事件或 CUSTOM/WIDGET 事件。"""
        events: list[dict[str, Any]] = []
        for part in parts:
            if part.kind == "widget":
                # 与 JiuWenChunkConverter 的标签块行为保持一致：自定义块不能
                # 插进一个尚未结束的普通文本消息中间。
                events.extend(self._end_text(message_id))
                events.append(
                    self.event(
                        "CUSTOM",
                        name="WIDGET",
                        value=part.value,
                    ),
                )
                continue

            text = str(part.value)
            if not text:
                continue
            events.extend(self._start_text(message_id))
            self.answer_parts.append(text)
            events.append(
                self.event(
                    "TEXT_MESSAGE_CONTENT",
                    messageId=message_id,
                    delta=text,
                ),
            )
        return events

    def _answer_delta_events(
        self,
        message_id: str,
        delta: str,
    ) -> list[dict[str, Any]]:
        """解析一段助手回答；Widget 不混入普通文本或最终 answer。"""
        self.saw_assistant_content = True
        parser = self.widget_parsers.setdefault(
            message_id,
            WidgetStreamParser(),
        )
        return self._parsed_part_events(message_id, parser.feed(delta))

    def _finish_answer_message(self, message_id: str) -> list[dict[str, Any]]:
        """刷新标签残留并关闭已经真正输出过的普通文本消息。"""
        events: list[dict[str, Any]] = []
        parser = self.widget_parsers.pop(message_id, None)
        if parser is not None:
            events.extend(
                self._parsed_part_events(message_id, parser.finish()),
            )
        events.extend(self._end_text(message_id))
        return events

    def _tool_call_events(
        self,
        message_id: str,
        data: dict[str, Any],
        is_delta: bool,
    ) -> list[dict[str, Any]]:
        state = self.tool_calls.setdefault(
            message_id,
            {"callId": "", "name": "tool", "args": "", "started": False},
        )
        call_id = str(data.get("call_id") or state["callId"] or message_id)
        name = str(data.get("name") or state["name"] or "tool")
        state["callId"], state["name"] = call_id, name
        events: list[dict[str, Any]] = []
        if self.show_tool_calls and not state["started"]:
            state["started"] = True
            events.append(
                self.event(
                    "TOOL_CALL_START",
                    toolCallId=call_id,
                    toolCallName=name,
                ),
            )

        arguments = str(data.get("arguments") or "")
        if is_delta and arguments:
            old_length = len(state["args"])
            state["args"] += arguments
            if self.show_tool_calls:
                limit = self.tool_call_max_length
                if limit <= 0 or old_length < limit:
                    remaining = arguments if limit <= 0 else arguments[: limit - old_length]
                    if remaining:
                        if limit > 0 and len(state["args"]) > limit:
                            remaining += "..."
                        events.append(
                            self.event(
                                "TOOL_CALL_ARGS",
                                toolCallId=call_id,
                                delta=remaining,
                            ),
                        )
        elif not is_delta:
            if not state["args"] and arguments and self.show_tool_calls:
                events.append(
                    self.event(
                        "TOOL_CALL_ARGS",
                        toolCallId=call_id,
                        delta=_text_preview(arguments, self.tool_call_max_length),
                    ),
                )
            if self.show_tool_calls and not state.get("ended"):
                state["ended"] = True
                events.append(self.event("TOOL_CALL_END", toolCallId=call_id))
        return events

    def _tool_result_events(
        self,
        message_id: str,
        data: dict[str, Any],
        status: str,
    ) -> list[dict[str, Any]]:
        state = self.tool_results.setdefault(message_id, {"sent": False})
        state["data"] = data
        if status != "completed" or state["sent"] or not self.show_tool_results:
            return []
        state["sent"] = True
        content = _text_preview(
            data.get("output", ""),
            self.tool_result_max_length,
        )
        return [
            self.event(
                "TOOL_CALL_RESULT",
                toolCallId=str(data.get("call_id") or message_id),
                messageId=message_id,
                content=content,
            ),
        ]

    def convert(self, raw_event: Any) -> list[dict[str, Any]]:
        """转换一个 QwenPaw 事件，可能生成零到多条 AG-UI 事件。"""
        if isinstance(raw_event, str):
            message_id = self.last_answer_message_id or f"msg-{uuid.uuid4()}"
            self.last_answer_message_id = message_id
            return self._answer_delta_events(message_id, raw_event)

        object_type = _value(raw_event, "object")
        item_type = str(_enum_value(_value(raw_event, "type")) or "")
        status = str(_enum_value(_value(raw_event, "status")) or "")

        if object_type == "message":
            message_id = str(_value(raw_event, "id") or "")
            if not message_id:
                return []
            self.message_types[message_id] = item_type
            if item_type == "reasoning":
                if status == "completed":
                    return self._end_thinking(message_id)
                return self._start_thinking(message_id)
            if item_type == "message":
                if status in {"completed", "cancelled", "failed"}:
                    return self._finish_answer_message(message_id)
                # 等到出现第一段普通文本后再发 TEXT_MESSAGE_START。
                # Widget-only 回答因此不会产生空的文本消息。
                self.last_answer_message_id = message_id
                return []
            return []

        if object_type != "content":
            return []

        message_id = str(_value(raw_event, "msg_id") or "")
        parent_type = self.message_types.get(message_id, "")
        if item_type == "text" and bool(_value(raw_event, "delta", False)):
            delta = str(_value(raw_event, "text") or "")
            if not delta:
                return []
            if parent_type == "reasoning":
                events = self._start_thinking(message_id)
                if self.show_thinking:
                    events.append(
                        self.event(
                            "THINKING_TEXT_MESSAGE_CONTENT",
                            messageId=message_id,
                            delta=delta,
                        ),
                    )
                return events
            self.last_answer_message_id = message_id
            return self._answer_delta_events(message_id, delta)

        if item_type != "data":
            return []
        data = _as_dict(_value(raw_event, "data", {}))
        if parent_type == "plugin_call":
            return self._tool_call_events(
                message_id,
                data,
                bool(_value(raw_event, "delta", False)),
            )
        if parent_type == "plugin_call_output":
            return self._tool_result_events(message_id, data, status)
        return []

    def finish(self) -> list[dict[str, Any]]:
        """补齐异常后端未显式关闭的消息事件。"""
        events: list[dict[str, Any]] = []
        for message_id, parser in list(self.widget_parsers.items()):
            events.extend(
                self._parsed_part_events(message_id, parser.finish()),
            )
        self.widget_parsers.clear()
        for message_id in list(self.active_thinking):
            events.extend(self._end_thinking(message_id))
        for message_id in list(self.active_text):
            events.extend(self._end_text(message_id))
        return events


def _completed_answer(event: Any) -> str:
    """从最终 AgentResponse 中读取完整助手回答，作为非流式兼容后备。"""
    output = _value(event, "output")
    if not isinstance(output, list):
        return ""

    answers: list[str] = []
    for message in output:
        role = _enum_value(_value(message, "role"))
        message_type = _enum_value(_value(message, "type"))
        if role not in (None, "assistant") or message_type not in (
            None,
            "message",
        ):
            continue
        content = _value(message, "content", [])
        if not isinstance(content, list):
            continue
        parts: list[str] = []
        for item in content:
            if _enum_value(_value(item, "type")) != "text":
                continue
            if bool(_value(item, "delta", False)):
                continue
            text = _value(item, "text", "")
            if text:
                parts.append(str(text))
        if parts:
            answers.append("".join(parts))
    return answers[-1] if answers else ""


def _get_channel(agent_id: str):
    """根据 Agent ID 取得当前进程中已经启动的运维助手频道实例。"""
    channel = ACTIVE_CHANNELS.get(agent_id)
    if channel is None:
        # 路由存在但映射不存在，通常说明该 Agent 的频道未启用或启动失败。
        raise HTTPException(
            status_code=404,
            detail="Operations Assistant is not active for this Agent",
        )
    return channel


def _authorize(channel: Any, authorization: Optional[str]) -> None:
    """校验聊天回调 Bearer Token。

    页面未配置 callback_token 时跳过认证；配置后，请求必须携带：
    ``Authorization: Bearer <callback_token>``。
    """
    expected = str(channel.callback_token or "")
    if not expected:
        return
    prefix = "Bearer "
    supplied = (
        authorization[len(prefix) :]
        if authorization and authorization.startswith(prefix)
        else ""
    )
    if not secrets.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Invalid callback token")


async def _stream_agent_request(
    payload: QwenPawChatRequest,
    channel: Any,
    ctx: Any,
    prompt: str,
    file_entries: list[dict[str, Any]],
):
    """构造关闭审批的 AgentRequest，交给 workspace.stream_query。

    纯文本和文件请求统一走本函数，确保每次运维助手调用都携带
    ``request_context.approval_level=off``。存在文件时，content 中额外携带
    ``{"type": "file", "file_url": "file://...", "filename": ...}``
    块。QwenPaw 的 MediaProcessHook 会在执行前把 file:// 块解析为本地路径，
    并向 Agent 注入"用户上传文件，已经下载到 <path>"提示。
    """
    workspace = await ctx._get_workspace()
    if workspace is None or not hasattr(workspace, "stream_query"):
        raise RuntimeError("Agent workspace does not support stream_query")

    from qwenpaw.schemas import AgentRequest

    content_parts: list[dict[str, Any]] = []
    if prompt:
        content_parts.append({"type": "text", "text": prompt})
    for entry in file_entries:
        content_parts.append(
            {
                "type": "file",
                # as_uri() 生成 file:/// 绝对地址；中文与空格会被正确转义。
                "file_url": Path(entry["path"]).resolve().as_uri(),
                "filename": str(entry["filename"]),
            },
        )

    request = AgentRequest(
        input=[
            {
                "role": "user",
                "content": content_parts,
            },
        ],
        session_id=payload.session_id,
        user_id=payload.user_id,
        # 运维助手请求必须归入自己的频道，不能落入 get_ctx 的 console 默认值。
        channel=channel.channel,
        agent_id=channel.agent_id,
        # 运维助手属于受信任的自动化回调频道，工具调用不进入人工审批流程。
        request_context={"approval_level": "off"},
    )
    async for event in workspace.stream_query(request):
        yield event


async def _ensure_visible_chat(
    payload: QwenPawChatRequest,
    channel: Any,
    ctx: Any,
    prompt: str,
    file_entries: list[dict[str, Any]],
) -> None:
    """在 QwenPaw 会话目录中创建或刷新当前运维助手会话。

    Agent 的 SessionSaveHook 负责保存实际消息上下文；ChatManager 负责控制台
    可见的会话目录。HTTP 回调没有经过 BaseChannel._consume_with_tracker，因而
    必须在这里补上与普通频道一致的目录登记步骤。
    """
    workspace = await ctx._get_workspace()
    if workspace is None:
        raise RuntimeError("Agent workspace is not available")
    manager = getattr(workspace, "chat_manager", None)
    if manager is None:
        raise RuntimeError("Agent workspace does not provide chat history")

    name = prompt.strip()
    if not name and file_entries:
        name = f"文件：{file_entries[0]['filename']}"
    chat = await manager.get_or_create_chat(
        payload.session_id,
        payload.user_id,
        channel.channel,
        name=name or "运维助手会话",
    )
    await manager.touch_chat(chat.id)


async def _stream_agent(
    payload: QwenPawChatRequest,
    channel: Any,
    ctx: Any,
    file_entries: list[dict[str, Any]] | None = None,
):
    """流式调用 Agent，并把每个文本增量立即转换为 AG-UI SSE。

    纯文本和文件请求统一构造 AgentRequest，并固定关闭工具审批；事件转换逻辑
    共用。
    """

    # 一次请求对应一个 run；一次助手回答对应一个 message。
    thread_id = payload.session_id
    run_id = f"run-{uuid.uuid4()}"
    metadata = {
        "source": "qwenpaw",
        "channel": channel.channel,
        "serviceCode": "operations-assistant",
        "agentId": channel.agent_id,
        "agentName": channel.agent_name,
        "userId": payload.user_id,
        "sessionId": payload.session_id,
    }
    # 第一个事件表示本次 Agent 执行已经开始。
    yield _sse(
        _event(
            "RUN_STARTED",
            threadId=thread_id,
            runId=run_id,
            protocolVersion="1.0",
            metadata=metadata,
        ),
    )
    try:
        bridge = _AgUiStreamBridge(run_id, metadata, channel)
        completed_answer = ""
        prompt = _latest_user_text(payload.input)

        # 同一 sessionId + userId + operations-assistant 对应同一段可见会话。
        await _ensure_visible_chat(
            payload,
            channel,
            ctx,
            prompt,
            file_entries or [],
        )

        # 限制整个 Agent 流的最大处理时间。每个 QwenPaw 原生事件都会立即
        # 转换成对应的思考、工具调用、工具结果或回答 AG-UI 事件。
        async with asyncio.timeout(channel.agent_timeout_seconds):
            async for event in _stream_agent_request(
                payload,
                channel,
                ctx,
                prompt,
                file_entries or [],
            ):
                for agui_event in bridge.convert(event):
                    yield _sse(agui_event)
                final_text = _completed_answer(event)
                if final_text:
                    completed_answer = final_text

        # 某些兼容后端只返回最终完整文本；仍转换成标准 START/CONTENT/END。
        if not bridge.saw_assistant_content and completed_answer:
            for agui_event in bridge.convert(completed_answer):
                yield _sse(agui_event)
        for agui_event in bridge.finish():
            yield _sse(agui_event)

        # 流式内容已经过 Widget 剥离，应优先作为最终纯文本回答；只有后端
        # 完全没有流式文本时，才使用同样经过解析器处理的完整回答后备。
        answer = "".join(bridge.answer_parts)
        message_id = bridge.last_answer_message_id

        # RUN_FINISHED 表示整个执行结束。result.answer 保存最终完整回答，
        # 普通 HTTP 客户端可以直接读取这个字段。
        yield _sse(
            _event(
                "RUN_FINISHED",
                threadId=thread_id,
                runId=run_id,
                outcome={"type": "success"},
                result={
                    "answer": answer,
                    "messageId": message_id,
                    "threadId": thread_id,
                    "runId": run_id,
                    "userId": payload.user_id,
                    "sessionId": payload.session_id,
                    "serviceCode": "operations-assistant",
                    "agentId": channel.agent_id,
                    "agentName": channel.agent_name,
                },
                metadata=metadata,
            ),
        )
    except TimeoutError:
        # 超时仍通过 SSE 返回 RUN_ERROR，而不是中途改成普通 JSON 响应。
        yield _sse(
            _event(
                "RUN_ERROR",
                message="QwenPaw Agent processing timed out",
                code="QWENPAW_TIMEOUT",
                metadata={**metadata, "threadId": thread_id, "runId": run_id},
            ),
        )
    except Exception as exc:  # noqa: BLE001
        # Agent、模型或工具执行产生的其他异常统一转换成 RUN_ERROR。
        yield _sse(
            _event(
                "RUN_ERROR",
                message=str(exc) or exc.__class__.__name__,
                code="QWENPAW_AGENT_ERROR",
                metadata={**metadata, "threadId": thread_id, "runId": run_id},
            ),
        )


@router.get("/health")
async def health(
    agent_id: str = Query(alias="agentId", min_length=1),
    ctx: Any = Depends(get_ctx),
) -> dict[str, Any]:
    """检查 URL 中 agentId 对应的运维助手频道是否已激活。"""
    ctx.agent_id = agent_id
    channel = _get_channel(agent_id)
    return await channel.health_check()


@router.post("/chat")
async def chat(
    payload: QwenPawChatRequest,
    authorization: Optional[str] = Header(default=None),
    agent_id: str = Query(alias="agentId", min_length=1),
    ctx: Any = Depends(get_ctx),
) -> StreamingResponse:
    """聊天回调入口，调用绑定的 Agent 并返回 AG-UI SSE。

    ``payload`` 来自 JSON 请求体；``authorization`` 来自请求头；
    ``ctx`` 由 FastAPI 先调用 QwenPaw 的 ``get_ctx(request)`` 自动创建。
    插件从 ``?agentId=...`` 读取 Agent ID，并写入 QwenPaw 上下文。
    """

    # 等价理解：FastAPI 在进入本函数前，已经大致执行了下面这一步：
    # ctx = await get_ctx(request)
    # 当前插件使用驼峰查询参数 ?agentId=default。

    # 1. 把插件的驼峰查询参数写入 QwenPaw 上下文，再查找对应频道实例。
    ctx.agent_id = agent_id
    request_agent_id = agent_id
    channel = _get_channel(request_agent_id)

    # 2. 如果配置了聊天回调 Token，则校验 Authorization 请求头。
    _authorize(channel, authorization)

    # 3. 再检查一次请求 Agent 与频道绑定 Agent 是否一致。
    if request_agent_id != channel.agent_id:
        raise HTTPException(status_code=409, detail="Callback Agent binding mismatch")

    # 4. 解析 input 中的文件块引用，并校验 fileId 归属。
    # 校验在返回 StreamingResponse 之前完成，失败时调用方收到普通 JSON 错误。
    file_entries: list[dict[str, Any]] = []
    for file_id in _extract_file_ids(payload.input):
        try:
            file_entries.append(
                resolve_file_block(file_id, channel.agent_id, payload.user_id),
            )
        except FileResolveError as exc:
            raise HTTPException(
                status_code=exc.status_code,
                detail=exc.detail,
            ) from exc

    # 5. 文本与文件至少要有一样；纯文本请求与 2.7.0 行为完全一致。
    if not _latest_user_text(payload.input) and not file_entries:
        raise HTTPException(
            status_code=400,
            detail=(
                "input must contain a non-empty user text message "
                "or a valid fileId file block"
            ),
        )
    # 6. 将 _stream_agent 生成的 SSE 帧逐条写回调用方。
    return StreamingResponse(
        _stream_agent(payload, channel, ctx, file_entries),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/files")
async def upload_file(
    authorization: Optional[str] = Header(default=None),
    agent_id: str = Query(alias="agentId", min_length=1),
    file: UploadFile = File(...),
    user_id: Optional[str] = Form(default=None, alias="userId"),
    session_id: Optional[str] = Form(default=None, alias="sessionId"),
    ctx: Any = Depends(get_ctx),
) -> dict[str, Any]:
    """上传文件接口，返回可被 /chat 引用的 fileId。

    请求为 multipart/form-data：

    - ``file``（必填）：上传的文件内容；
    - ``userId``（可选）：记录文件归属用户；填写后 /chat 引用该文件时
      必须携带相同的 userId，否则返回 403；
    - ``sessionId``（可选）：仅记录用途，不参与校验。

    返回 ``{"fileId", "filename", "size"}``。鉴权复用聊天回调 Token。
    ``del session_id`` 仅表示当前版本不把它写入注册表。
    """
    del session_id

    # 1. 定位频道实例并校验回调 Token，与 /chat 完全一致。
    ctx.agent_id = agent_id
    channel = _get_channel(agent_id)
    _authorize(channel, authorization)

    # 2. 文件名清洗；拒绝空文件名与空内容。
    safe_name = sanitize_filename(file.filename)
    if not safe_name:
        raise HTTPException(
            status_code=400,
            detail="Uploaded file must have a valid filename",
        )

    # 3. 找到 Agent workspace，文件保存在其媒体目录下。
    workspace = await ctx._get_workspace()
    workspace_dir = getattr(workspace, "workspace_dir", None)
    if not workspace_dir:
        raise HTTPException(
            status_code=404,
            detail="Agent workspace is not available for file upload",
        )

    # 4. 分块写盘并统计大小，超过 max_upload_mb 时删除半成品并返回 413。
    file_id = uuid.uuid4().hex
    dest_dir = (
        Path(workspace_dir).expanduser()
        / "media"
        / "service-channel"
        / file_id
    )
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / safe_name
    max_bytes = channel.max_upload_mb * 1024 * 1024
    size = 0
    try:
        with dest.open("wb") as out:
            while chunk := await file.read(1024 * 1024):
                size += len(chunk)
                if size > max_bytes:
                    raise HTTPException(
                        status_code=413,
                        detail=(
                            "Uploaded file exceeds the configured "
                            f"max size of {channel.max_upload_mb} MB"
                        ),
                    )
                out.write(chunk)
    except BaseException:
        # 413、网络中断或任务取消时都清掉半成品文件再向外抛。
        dest.unlink(missing_ok=True)
        raise
    if size == 0:
        dest.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    # 5. 登记进进程内注册表，等待 /chat 引用。
    register_uploaded_file(
        file_id=file_id,
        path=dest,
        filename=safe_name,
        size=size,
        agent_id=agent_id,
        user_id=user_id,
    )
    logger.info(
        "Operations Assistant file uploaded: agent=%s file=%s size=%d owner=%s",
        agent_id,
        safe_name,
        size,
        (user_id or "").strip() or "<any user>",
    )
    return {"fileId": file_id, "filename": safe_name, "size": size}
