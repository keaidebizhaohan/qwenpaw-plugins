"""始终可用的运维助手 HTTP API。

本模块直接根据 URL 中的 Agent ID 获取 Workspace，不依赖频道运行实例。
频道启停、热重载、注册或注销都不会移除聊天和文件上传路由。

本插件提供：

- POST /chat：无论 ``stream`` 为 true 还是 false 都返回 AG-UI SSE；
  ``stream=false`` 会等待 Agent 完成后，再一次性逐帧输出完整思考和最终回答；
- POST /chat/stop：按 Agent ID 和 Session ID 停止运行中的聊天；
- POST /files：multipart 文件上传，返回服务器内 file_url 供聊天引用；
- /chat 的 input 中支持 ``{"type": "file", "file_url": ...}`` 内容块；
  携带文件块时改走 ``workspace.stream_query(AgentRequest)`` 路径，
  其余请求行为与 2.7.0 完全一致。
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    UploadFile,
)
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from qwenpaw.pawapp import get_ctx

from .storage import sanitize_filename
from .widget_stream import ParsedPart, WidgetStreamParser

logger = logging.getLogger(__name__)

# plugin.py 会将这个路由器挂载到 /api/service-channel 前缀下。
router = APIRouter()

# 当前进程内正在执行的运维助手聊天。键为 (agentId, sessionId, runId)，值为
# 执行 StreamingResponse 的 asyncio Task。停止接口按 agentId + sessionId
# 找到全部任务并取消；runId 仍会出现在 SSE 事件中供调用方追踪。
ACTIVE_RUNS: dict[tuple[str, str, str], asyncio.Task[Any]] = {}


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

    # true（默认）实时转发 AG-UI SSE；false 等待 QwenPaw 最终
    # AgentResponse，再通过 SSE 输出最多一条思考 Chunk 和一条回答 Chunk。
    stream: bool = True


class StopChatRequest(BaseModel):
    """停止某个 Agent 会话下所有运行中聊天的请求体。"""

    model_config = ConfigDict(extra="ignore", populate_by_name=False)
    session_id: str = Field(alias="sessionId", min_length=1)


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


def _approval_value(pending: Any) -> dict[str, Any]:
    """把 QwenPaw 审批记录转换为稳定的 CUSTOM/APPROVAL value。"""
    extra = getattr(pending, "extra", {})
    if not isinstance(extra, dict):
        extra = {}
    display = extra.get("display", {})
    if not isinstance(display, dict):
        display = {}
    tool_name = str(getattr(pending, "tool_name", "") or "")
    return {
        "request_id": str(getattr(pending, "request_id", "") or ""),
        "root_session_id": str(
            getattr(pending, "root_session_id", "") or "",
        ),
        "tool_name": tool_name,
        "tool_display_name": str(display.get("tool_name") or tool_name),
        "tool_source": str(display.get("tool_source") or "No rule hit"),
        "severity": str(getattr(pending, "severity", "medium") or "medium"),
        "findings_count": int(getattr(pending, "findings_count", 0) or 0),
        "summary": str(getattr(pending, "result_summary", "") or ""),
        "tool_params": extra.get("tool_call", {}).get("input", {})
        if isinstance(extra.get("tool_call"), dict)
        else {},
        "reasoning": str(extra.get("reasoning") or ""),
        "source_type": str(extra.get("source_type") or "tool_guard"),
        "driver": extra.get("driver"),
        "created_at": float(getattr(pending, "created_at", 0.0) or 0.0),
        "timeout_seconds": float(
            getattr(pending, "timeout_seconds", 300.0) or 300.0,
        ),
        "exact_target": str(display.get("exact_target") or ""),
        "similar_target": str(display.get("similar_target") or ""),
        "is_generalized": bool(display.get("is_generalized")),
    }


def _approval_custom_events(
    bridge: Any,
    pending: list[Any],
    known: set[str],
) -> list[dict[str, Any]]:
    """生成新增审批事件，并按 request_id 去重。"""
    events: list[dict[str, Any]] = []
    for item in pending:
        request_id = str(getattr(item, "request_id", "") or "")
        if not request_id:
            continue
        if request_id in known:
            continue
        known.add(request_id)
        events.append(
            bridge.event(
                "CUSTOM",
                name="APPROVAL",
                value=_approval_value(item),
            ),
        )
    return events


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


def _extract_file_paths(messages: list[dict[str, Any]]) -> list[str]:
    """按顺序收集 input 中所有 file 块引用的 file_url（去重保序）。

    文件块格式：``{"type": "file", "file_url": "<上传返回值>"}``。
    其他类型的块、非用户消息和格式不完整的块都会被忽略。
    """
    file_paths: list[str] = []
    for message in messages:
        if not isinstance(message, dict) or message.get("role") != "user":
            continue
        content = message.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            file_path = (
                part.get("file_url")
                if isinstance(part, dict)
                and part.get("type") == "file"
                else None
            )
            if (
                isinstance(file_path, str)
                and file_path.strip()
                and file_path not in file_paths
            ):
                file_paths.append(file_path)
    return file_paths


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


def _tool_result_succeeded(value: Any) -> bool:
    """把 QwenPaw 工具结果的 state 统一转换为成功标记。

    不同工具可能返回字符串（success/failed/error）或布尔值。未知或缺失
    state 沿用 QwenPaw 的兼容行为，视为成功；明确的失败状态才标记失败。
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return True
    normalized = str(_enum_value(value)).strip().lower()
    return normalized not in {
        "false",
        "failed",
        "failure",
        "error",
        "cancelled",
        "canceled",
        "denied",
        "interrupted",
    }


def _tool_result_content(data: dict[str, Any], limit: int) -> str:
    """读取真实工具输出；失败时也保证 AG-UI content 不为空。

    正常结果优先使用 QwenPaw ``data.output``。部分失败工具只返回 error、
    message 或 detail，因此依次回退；再没有可读错误时返回完整 data，避免
    TOOL_CALL_RESULT 只剩一个空字符串。
    """
    value = data.get("output")
    if value is None or value == "":
        for key in ("error", "message", "detail"):
            candidate = data.get(key)
            if candidate is not None and candidate != "":
                value = candidate
                break
    if value is None or value == "":
        value = data
    return _text_preview(value, limit)


def _json_object(value: Any) -> dict[str, Any]:
    """把工具 arguments 的 JSON 字符串或字典统一转换为字典。"""
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


_FILE_ARTIFACT_TOOLS = {
    "writefile",
    "editfile",
    "appendfile",
    "sendfiletouser",
}


def _normalized_tool_name(value: Any) -> str:
    """使用与 QwenPaw 页面一致的方式规范化工具名。"""
    return "".join(character for character in str(value or "") if character.isalnum()).lower()


def _artifact_file_call(data: dict[str, Any]) -> tuple[str, str]:
    """从页面支持的文件工具参数中读取规范化工具名和文件路径。"""
    tool_name = _normalized_tool_name(data.get("name"))
    if tool_name not in _FILE_ARTIFACT_TOOLS:
        return "", ""
    arguments = _json_object(data.get("arguments"))
    file_path = arguments.get("file_path")
    return tool_name, str(file_path).strip() if file_path else ""


def _resolve_artifact_file_path(
    file_path: str,
    project_dir: str | Path | None,
) -> str:
    """按 QwenPaw 工具相同语义把相对文件路径解析到主项目目录。"""
    path = Path(file_path).expanduser()
    if path.is_absolute() or not project_dir:
        return str(path)
    return str((Path(project_dir).expanduser() / path).resolve())

# 这次文件工具调用是否真的成功生成了一个可以下载的文件？
def _tool_result_created_file(
    data: dict[str, Any],
    tool_name: str,
) -> bool:
    """按 QwenPaw 页面规则确认文件工具是否成功生成可下载文件。"""
    if not _tool_result_succeeded(data.get("state")):
        return False
    # write/edit/append 的成功结果只有文本描述，没有 DataBlock。页面也是根据
    # 工具名和成功 state 直接显示这些文件。
    if tool_name in {"writefile", "editfile", "appendfile"}:
        return True
    if tool_name != "sendfiletouser":
        return False

    # send_file_to_user 即使失败也可能返回 state=success，只有实际 DataBlock
    # 才代表文件真的发送成功；DataBlock 既可能是 file URL，也可能是 base64。
    output = data.get("output")
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
    if not isinstance(output, list):
        return False
    for item in output:
        if not isinstance(item, dict) or item.get("type") != "data":
            continue
        if isinstance(item, dict) and item.get("type") == "data":
            return True
    return False


def _tool_result_text(data: dict[str, Any]) -> str:
    """读取工具结果中的文本，兼容纯字符串和序列化 Block 数组。"""
    output = data.get("output")
    if isinstance(output, str):
        try:
            parsed = json.loads(output)
        except (TypeError, ValueError, json.JSONDecodeError):
            return output
        output = parsed
    if not isinstance(output, list):
        return str(output or "")
    return "\n".join(
        str(item.get("text"))
        for item in output
        if isinstance(item, dict)
        and item.get("type") == "text"
        and item.get("text")
    )


def _actual_file_path_from_result(
    data: dict[str, Any],
    tool_name: str,
    fallback_path: str,
) -> str:
    """优先从文件工具执行结果读取其真正操作的绝对路径。"""
    text = _tool_result_text(data).strip()
    patterns = {
        "writefile": r"^Wrote\s+\d+\s+bytes\s+to\s+(.+)\.$",
        "appendfile": r"^Appended\s+\d+\s+bytes\s+to\s+(.+)\.$",
        "editfile": r"^Successfully replaced text in\s+(.+)\.$",
    }
    pattern = patterns.get(tool_name)
    if pattern:
        matched = re.match(pattern, text, flags=re.DOTALL)
        if matched:
            result_path = matched.group(1).strip()
            if result_path:
                return result_path
    return fallback_path


def _file_custom_event(
    file_path: str,
    event_factory: Any = _event,
) -> dict[str, Any]:
    """把 Agent 生成的文件转换为约定的 CUSTOM/FILE 事件。"""
    return event_factory(
        "CUSTOM",
        name="FILE",
        value={
            # 这里只返回 Agent 生成文件的原始路径。调用方下载时再把该值作为
            # path 参数传给 /api/service-channel/files/download。
            "path": file_path,
            "name": Path(file_path).name,
        },
    )


def _completed_file_events(
    response: Any,
    project_dir: str | Path | None = None,
) -> list[dict[str, Any]]:
    """从最终 AgentResponse 配对文件工具调用和成功结果。"""
    output = _value(response, "output", [])
    if not isinstance(output, list):
        return []

    calls: dict[str, tuple[str, str]] = {}
    successful_call_ids: list[str] = []
    for message in output:
        message_type = str(_enum_value(_value(message, "type")) or "")
        content = _value(message, "content", [])
        if not isinstance(content, list):
            continue
        for item in content:
            if str(_enum_value(_value(item, "type")) or "") != "data":
                continue
            data = _as_dict(_value(item, "data", {}))
            call_id = str(data.get("call_id") or "")
            if not call_id:
                continue
            if message_type == "plugin_call":
                tool_name, file_path = _artifact_file_call(data)
                if file_path:
                    calls[call_id] = (
                        tool_name,
                        _resolve_artifact_file_path(file_path, project_dir),
                    )
            elif (
                message_type == "plugin_call_output"
                and call_id in calls
                and _tool_result_created_file(data, calls[call_id][0])
            ):
                tool_name, fallback_path = calls[call_id]
                calls[call_id] = (
                    tool_name,
                    _actual_file_path_from_result(
                        data,
                        tool_name,
                        fallback_path,
                    ),
                )
                successful_call_ids.append(call_id)

    events: list[dict[str, Any]] = []
    emitted: set[str] = set()
    for call_id in successful_call_ids:
        if call_id in emitted:
            continue
        emitted.add(call_id)
        events.append(_file_custom_event(calls[call_id][1]))
    return events


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
        self.project_dir = getattr(channel, "project_dir", None)
        self.message_types: dict[str, str] = {}
        self.active_thinking: set[str] = set()
        self.active_text: set[str] = set()
        self.tool_calls: dict[str, dict[str, Any]] = {}
        self.tool_results: dict[str, dict[str, Any]] = {}
        # send_file_to_user 的调用参数和结果分属两条 message，用 call_id 配对。
        self.file_calls: dict[str, tuple[str, str]] = {}
        self.emitted_file_calls: set[str] = set()
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
            had_streamed_arguments = bool(state["args"])
            full_arguments = str(state["args"] or arguments)
            state["args"] = full_arguments
            tool_name, file_path = _artifact_file_call(
                {"name": name, "arguments": full_arguments},
            )
            if file_path:
                self.file_calls[call_id] = (
                    tool_name,
                    _resolve_artifact_file_path(
                        file_path,
                        self.project_dir,
                    ),
                )
            if not had_streamed_arguments and arguments and self.show_tool_calls:
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
        result_state = self.tool_results.setdefault(message_id, {"sent": False})
        result_state["data"] = data
        # 工具失败也是一次完整的工具执行结果，不能丢弃。只等待进行中的事件，
        # completed/failed/cancelled 都属于可以发送给调用方的终态。
        terminal = status in {"completed", "failed", "cancelled", "canceled"}
        if not terminal or result_state["sent"] or not self.show_tool_results:
            return []
        result_state["sent"] = True
        tool_state = data.get("state")
        succeeded = _tool_result_succeeded(tool_state)
        content = _tool_result_content(data, self.tool_result_max_length)
        events = [
            self.event(
                "TOOL_CALL_RESULT",
                toolCallId=str(data.get("call_id") or message_id),
                messageId=message_id,
                content=content,
                role="tool",
                metadata={
                    **self.metadata,
                    "toolState": tool_state,
                    "success": succeeded,
                },
            ),
        ]
        call_id = str(data.get("call_id") or message_id)
        tool_name, file_path = self.file_calls.get(call_id, ("", ""))
        if (
            file_path
            and call_id not in self.emitted_file_calls
            and _tool_result_created_file(data, tool_name)
        ):
            file_path = _actual_file_path_from_result(
                data,
                tool_name,
                file_path,
            )
            self.file_calls[call_id] = (tool_name, file_path)
            self.emitted_file_calls.add(call_id)
            events.append(
                _file_custom_event(
                    file_path,
                    self.event,
                ),
            )
        return events

    def _embedded_data_events(
        self,
        message_id: str,
        parent_type: str,
        content: Any,
        parent_status: str,
    ) -> list[dict[str, Any]]:
        """转换 message.content 中内嵌的工具参数或结果事件。

        QwenPaw 的最终 ``plugin_call_output`` 消息会把真实结果放在
        ``content[].data.output`` 中，而且该内嵌项的 msg_id 可能为 null。
        因此必须使用外层 message.id 建立 AG-UI messageId，不能只等待随后
        到来的独立 content 事件。
        """
        if not isinstance(content, list):
            return []
        events: list[dict[str, Any]] = []
        for item in content:
            if str(_enum_value(_value(item, "type")) or "") != "data":
                continue
            data = _as_dict(_value(item, "data", {}))
            item_status = str(
                _enum_value(_value(item, "status")) or parent_status,
            )
            if parent_type == "plugin_call":
                events.extend(
                    self._tool_call_events(
                        message_id,
                        data,
                        bool(_value(item, "delta", False)),
                    ),
                )
            elif parent_type == "plugin_call_output":
                events.extend(
                    self._tool_result_events(message_id, data, item_status),
                )
        return events

    def convert(self, raw_event: Any) -> list[dict[str, Any]]:
        """转换一个 QwenPaw 事件，可能生成零到多条 AG-UI 事件。"""
        if isinstance(raw_event, str):
            message_id = self.last_answer_message_id or f"msg-{uuid.uuid4()}"
            self.last_answer_message_id = message_id
            return self._answer_delta_events(message_id, raw_event)

        object_type = _value(raw_event, "object")
        item_type = str(_enum_value(_value(raw_event, "type")) or "")
        status = str(_enum_value(_value(raw_event, "status")) or "")

        if object_type == "response":
            # 最终 AgentResponse 会再次携带全部工具消息。工具状态本身负责去重，
            # 因而这里既能补齐后端没有逐条流出的文件事件，也不会重复发送。
            events: list[dict[str, Any]] = []
            output = _value(raw_event, "output", [])
            if isinstance(output, list):
                for message in output:
                    message_type = str(
                        _enum_value(_value(message, "type")) or "",
                    )
                    if message_type in {"plugin_call", "plugin_call_output"}:
                        events.extend(self.convert(message))
            return events

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
            if item_type in {"plugin_call", "plugin_call_output"}:
                return self._embedded_data_events(
                    message_id,
                    item_type,
                    _value(raw_event, "content", []),
                    status,
                )
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


def _completed_message_text(message: Any) -> str:
    """读取最终 AgentResponse 中一条消息的完整非增量文本。"""
    content = _value(message, "content", [])
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for item in content:
        if _enum_value(_value(item, "type")) != "text":
            continue
        # 最终 response.output 理论上都是 delta=false；显式过滤可以避免兼容
        # 后端把早先的流式碎片也塞进最终响应时造成重复。
        if bool(_value(item, "delta", False)):
            continue
        text = _value(item, "text", "")
        if text:
            parts.append(str(text))
    return "".join(parts).strip()


def _non_stream_agui_events(
    response: Any,
    project_dir: str | Path | None = None,
) -> list[dict[str, Any]]:
    """把 QwenPaw 最终 AgentResponse 压缩为一到两条 AG-UI Chunk。

    所有 reasoning 消息合并成第一条 REASONING_MESSAGE_CHUNK；最后一条完整
    assistant message 作为 TEXT_MESSAGE_CHUNK。没有思考时只返回最终回答。
    """
    output = _value(response, "output", [])
    if not isinstance(output, list):
        return []

    reasoning_parts: list[str] = []
    reasoning_message_id = ""
    answer = ""
    answer_message_id = ""

    for message in output:
        role = str(_enum_value(_value(message, "role")) or "")
        message_type = str(_enum_value(_value(message, "type")) or "")
        text = _completed_message_text(message)
        if not text:
            continue

        if message_type == "reasoning" and role in {"", "assistant"}:
            if not reasoning_message_id:
                reasoning_message_id = str(
                    _value(message, "id") or f"msg-{uuid.uuid4()}",
                )
            reasoning_parts.append(text)
            continue

        if message_type == "message" and role in {"", "assistant"}:
            # 最终 output 中可能存在多条历史助手消息，取最后一条作为回答。
            answer = text
            answer_message_id = str(
                _value(message, "id") or f"msg-{uuid.uuid4()}",
            )

    events: list[dict[str, Any]] = []
    if reasoning_parts:
        events.append(
            _event(
                "REASONING_MESSAGE_CHUNK",
                messageId=reasoning_message_id,
                delta="\n".join(reasoning_parts),
            ),
        )
    events.extend(_completed_file_events(response, project_dir))
    if answer:
        events.append(
            _event(
                "TEXT_MESSAGE_CHUNK",
                messageId=answer_message_id,
                role="assistant",
                delta=answer,
            ),
        )
    return events


async def _get_runtime(agent_id: str, ctx: Any) -> Any:
    """直接取得 Agent Workspace 并构造 HTTP 运行参数。

    返回对象只提供 HTTP 桥接层需要的字段；它不是频道实例，因此不会因频道
    stop()、deregister 或热重载而从全局映射中消失。
    """
    ctx.agent_id = agent_id
    workspace = await ctx._get_workspace()
    if workspace is None:
        raise HTTPException(status_code=404, detail="Agent workspace not found")

    agent_config = getattr(workspace, "config", None)
    try:
        from qwenpaw.app.agent_context import get_agent_project_dir

        project_dir = get_agent_project_dir(workspace)
    except Exception:
        # 兼容没有新项目目录服务的 QwenPaw 版本；旧版本至少提供 workspace_dir。
        project_dir = getattr(workspace, "workspace_dir", None)
    # HTTP 插件没有配置页面，也不读取频道配置。以下值是插件固定策略：
    # 完整输出思考、工具调用与结果；单次 Agent 最长 30 分钟；单文件 1 GB。
    display = SimpleNamespace(
        show_thinking=True,
        show_tool_calls=True,
        show_tool_results=True,
        tool_call_max_length=200,
        # AG-UI ToolCallResult.content 应携带实际完整 output；0 表示不截断。
        tool_result_max_length=0,
    )
    return SimpleNamespace(
        channel="operations-assistant",
        agent_id=agent_id,
        agent_name=str(getattr(agent_config, "name", "") or agent_id),
        agent_timeout_seconds=1800,
        max_upload_mb=1024,
        project_dir=project_dir,
        _display_config=display,
        workspace=workspace,
    )


async def _stream_agent_request(
    payload: QwenPawChatRequest,
    channel: Any,
    ctx: Any,
    prompt: str,
    file_entries: list[dict[str, Any]],
):
    """构造 AgentRequest，交给 workspace.stream_query。

    纯文本和文件请求统一走本函数。审批等级由 Agent 的安全设置决定；存在
    文件时，content 中额外携带
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
    )
    async for event in workspace.stream_query(request):
        yield event


async def _owned_pending_approvals(
    payload: QwenPawChatRequest,
    channel: Any,
) -> list[Any]:
    """读取属于当前外部聊天身份及会话树的待审批记录。"""
    from qwenpaw.app.approvals import get_approval_service

    service = get_approval_service()
    by_root = getattr(service, "get_pending_by_root_session", None)
    pending = (
        await by_root(payload.session_id)
        if by_root is not None
        else await service.list_pending_by_session(payload.session_id)
    )
    return [
        item
        for item in pending
        if item.root_session_id == payload.session_id
        and item.owner_agent_id == channel.agent_id
        and item.user_id == payload.user_id
        and item.channel == channel.channel
    ]


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


async def _non_stream_agent(
    payload: QwenPawChatRequest,
    channel: Any,
    ctx: Any,
    file_entries: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """消费完整 QwenPaw 流，只返回最终 response 中的一到两条 AG-UI Chunk。"""
    run_id = f"run-{uuid.uuid4()}"
    run_key = (channel.agent_id, payload.session_id, run_id)
    current_task = asyncio.current_task()
    if current_task is not None:
        ACTIVE_RUNS[run_key] = current_task

    try:
        prompt = _latest_user_text(payload.input)
        await _ensure_visible_chat(
            payload,
            channel,
            ctx,
            prompt,
            file_entries or [],
        )

        final_response: Any = None
        async with asyncio.timeout(channel.agent_timeout_seconds):
            async for event in _stream_agent_request(
                payload,
                channel,
                ctx,
                prompt,
                file_entries or [],
            ):
                if str(_enum_value(_value(event, "object")) or "") == "response":
                    # 始终保留最后一个 response，而不是依赖它恰好位于倒数第几帧。
                    final_response = event

        if final_response is None:
            raise HTTPException(
                status_code=502,
                detail="QwenPaw did not return a final AgentResponse",
            )

        events = _non_stream_agui_events(
            final_response,
            getattr(channel, "project_dir", None),
        )
        if not any(event["type"] == "TEXT_MESSAGE_CHUNK" for event in events):
            raise HTTPException(
                status_code=502,
                detail="QwenPaw final AgentResponse has no assistant answer",
            )
        return events
    except asyncio.CancelledError as exc:
        raise HTTPException(
            status_code=409,
            detail="QwenPaw Agent run was cancelled",
        ) from exc
    except TimeoutError as exc:
        raise HTTPException(
            status_code=504,
            detail="QwenPaw Agent processing timed out",
        ) from exc
    finally:
        if ACTIVE_RUNS.get(run_key) is current_task:
            ACTIVE_RUNS.pop(run_key, None)


async def _non_stream_sse(events: list[dict[str, Any]]):
    """把已经收集完成的 AG-UI 事件逐帧输出为 SSE。

    ``stream=false`` 只关闭 QwenPaw 中间增量的实时转发，不改变本插件对外的
    ``text/event-stream`` 协议。事件已经由 ``_non_stream_agent`` 收集完整，
    因此这里通常依次 yield 一条思考事件和一条最终回答事件。
    """
    for event in events:
        yield _sse(event)
        # 主动把执行权交还事件循环，使 StreamingResponse 有机会立即发送刚刚
        # yield 的一帧，然后再请求下一帧，而不是在同一轮循环内连续处理完。
        await asyncio.sleep(0)


async def _stream_agent(
    payload: QwenPawChatRequest,
    channel: Any,
    ctx: Any,
    file_entries: list[dict[str, Any]] | None = None,
):
    """流式调用 Agent，并把每个文本增量立即转换为 AG-UI SSE。

    纯文本和文件请求统一构造 AgentRequest。等待工具审批期间轮询当前会话树，
    将待审批记录转换为 AG-UI ``CUSTOM/APPROVAL`` 事件。
    """

    # 一次请求对应一个 run；一次助手回答对应一个 message。
    thread_id = payload.session_id
    run_id = f"run-{uuid.uuid4()}"
    run_key = (channel.agent_id, payload.session_id, run_id)
    current_task = asyncio.current_task()
    if current_task is not None:
        ACTIVE_RUNS[run_key] = current_task
    metadata = {
        "source": "qwenpaw",
        "channel": channel.channel,
        "serviceCode": "operations-assistant",
        "agentId": channel.agent_id,
        "agentName": channel.agent_name,
        "userId": payload.user_id,
        "sessionId": payload.session_id,
    }
    bridge = _AgUiStreamBridge(run_id, metadata, channel)
    known_approvals: set[str] = set()
    try:
        # 第一个事件表示本次 Agent 执行已经开始。ACTIVE_RUNS 已经先登记，
        # 因此调用方一收到 runId 就可以立即使用停止接口。
        yield _sse(
            _event(
                "RUN_STARTED",
                threadId=thread_id,
                runId=run_id,
                protocolVersion="1.0",
                metadata=metadata,
            ),
        )
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
            source = _stream_agent_request(
                payload,
                channel,
                ctx,
                prompt,
                file_entries or [],
            )
            next_event: asyncio.Task[Any] | None = asyncio.create_task(
                anext(source),
            )
            try:
                while next_event is not None:
                    done, _ = await asyncio.wait(
                        {next_event},
                        timeout=0.25,
                    )
                    pending = await _owned_pending_approvals(payload, channel)
                    for approval_event in _approval_custom_events(
                        bridge,
                        pending,
                        known_approvals,
                    ):
                        yield _sse(approval_event)
                    if not done:
                        continue
                    try:
                        event = next_event.result()
                    except StopAsyncIteration:
                        next_event = None
                        break
                    for agui_event in bridge.convert(event):
                        yield _sse(agui_event)
                    final_text = _completed_answer(event)
                    if final_text:
                        completed_answer = final_text
                    next_event = asyncio.create_task(anext(source))
            finally:
                if next_event is not None and not next_event.done():
                    next_event.cancel()
                    await asyncio.gather(next_event, return_exceptions=True)
                await source.aclose()

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
    except asyncio.CancelledError:
        # stop 接口取消任务后，不把它当成错误；先关闭尚未结束的文本事件，
        # 再用 RUN_FINISHED/cancelled 正常结束原 SSE。
        for agui_event in bridge.finish():
            yield _sse(agui_event)
        yield _sse(
            _event(
                "RUN_FINISHED",
                threadId=thread_id,
                runId=run_id,
                outcome={"type": "cancelled"},
                result={
                    "cancelled": True,
                    "sessionId": payload.session_id,
                    "runId": run_id,
                    "agentId": channel.agent_id,
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
    finally:
        if ACTIVE_RUNS.get(run_key) is current_task:
            ACTIVE_RUNS.pop(run_key, None)


@router.post("/chat")
async def chat(
    payload: QwenPawChatRequest,
    agent_id: str = Query(alias="agentId", min_length=1),
    ctx: Any = Depends(get_ctx),
) -> Any:
    """聊天回调入口；两种模式都返回 AG-UI SSE。

    ``payload`` 来自 JSON 请求体；``ctx`` 由 FastAPI 先调用 QwenPaw 的
    ``get_ctx(request)`` 自动创建。
    插件从 ``?agentId=...`` 读取 Agent ID，并写入 QwenPaw 上下文。

    ``stream=true`` 实时转发增量事件；``stream=false`` 先等待完整
    AgentResponse，再把完整思考和最终回答作为一到两帧 SSE 输出。
    """

    # 等价理解：FastAPI 在进入本函数前，已经大致执行了下面这一步：
    # ctx = await get_ctx(request)
    # 当前插件使用驼峰查询参数 ?agentId=default。

    # 1. 直接按 agentId 获取 Workspace；不查询活动频道实例。
    request_agent_id = agent_id
    channel = await _get_runtime(request_agent_id, ctx)

    # 2. 再检查一次请求 Agent 与运行时绑定 Agent 是否一致。
    if request_agent_id != channel.agent_id:
        raise HTTPException(status_code=409, detail="Callback Agent binding mismatch")

    # 4. 解析 input 中的文件路径，并限制在当前 Agent Workspace 或 QwenPaw
    #    工作目录内。校验在返回 StreamingResponse 之前完成，失败时调用方收到
    #    普通 JSON 错误。
    file_entries: list[dict[str, Any]] = []
    for file_path in _extract_file_paths(payload.input):
        target = _resolve_agent_download_file(channel, file_path)
        file_entries.append(
            {
                "path": target,
                "filename": target.name,
                "size": target.stat().st_size,
                "agent_id": channel.agent_id,
            },
        )

    # 5. 文本与文件至少要有一样；纯文本请求与 2.7.0 行为完全一致。
    if not _latest_user_text(payload.input) and not file_entries:
        raise HTTPException(
            status_code=400,
            detail=(
                "input must contain a non-empty user text message "
                "or a valid file_url file block"
            ),
        )
    # 6. 非流式模式先在当前请求中等待 QwenPaw 执行完成，但对外仍使用 SSE。
    #    调用方不会收到 QwenPaw 的中间增量，只会收到最终整理好的 1～2 帧。
    if not payload.stream:
        events = await _non_stream_agent(payload, channel, ctx, file_entries)
        return StreamingResponse(
            _non_stream_sse(events),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "X-Accel-Buffering": "no",
            },
        )

    # 7. 默认流式模式将 _stream_agent 生成的 SSE 帧逐条写回调用方。
    return StreamingResponse(
        _stream_agent(payload, channel, ctx, file_entries),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/chat/stop")
async def stop_chat(
    payload: StopChatRequest,
    agent_id: str = Query(alias="agentId", min_length=1),
) -> dict[str, Any]:
    """停止指定 Agent + Session 下的全部运行任务。

    接口本身始终返回 200。找到任务时 ``stopped=true``；没有运行中任务时
    ``stopped=false``，方便调用方安全地重复调用。
    """
    matched: list[asyncio.Task[Any]] = []
    for (running_agent_id, session_id, _run_id), task in list(
        ACTIVE_RUNS.items(),
    ):
        if running_agent_id != agent_id or session_id != payload.session_id:
            continue
        if task.done():
            continue
        matched.append(task)

    for task in matched:
        task.cancel("Stopped through Operations Assistant HTTP API")

    response: dict[str, Any] = {
        "stopped": bool(matched),
        "agentId": agent_id,
        "sessionId": payload.session_id,
        "cancelledRuns": len(matched),
    }
    if not matched:
        response["message"] = "No active run found"
    return response


def _resolve_agent_download_file(channel: Any, file_path: str) -> Path:
    """解析下载目标，并限制在 Agent Workspace 或 QwenPaw 工作目录内。"""
    workspace_dir = getattr(channel.workspace, "workspace_dir", None)
    if not workspace_dir:
        raise HTTPException(
            status_code=404,
            detail="Agent workspace is not available for file download",
        )

    workspace_root = Path(workspace_dir).expanduser().resolve()
    project_root = Path(
        getattr(channel, "project_dir", None) or workspace_root,
    ).expanduser().resolve()
    allowed_roots = [workspace_root]
    if project_root not in allowed_roots:
        allowed_roots.append(project_root)
    try:
        from qwenpaw.constant import WORKING_DIR

        working_root = Path(WORKING_DIR).expanduser().resolve()
        if working_root not in allowed_roots:
            allowed_roots.append(working_root)
    except Exception:  # pragma: no cover - 兼容精简版 QwenPaw 运行环境
        pass

    candidate = Path(file_path).expanduser()
    if not candidate.is_absolute():
        candidate = project_root / candidate
    try:
        target = candidate.resolve()
    except (OSError, RuntimeError, ValueError) as exc:
        raise HTTPException(
            status_code=403,
            detail="Invalid file path",
        ) from exc
    if not any(
        target == root or target.is_relative_to(root)
        for root in allowed_roots
    ):
        raise HTTPException(
            status_code=403,
            detail="Requested file is outside the QwenPaw working directory",
        )
    if not target.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return target


@router.get("/files/download")
async def download_file(
    agent_id: str = Query(alias="agentId", min_length=1),
    path: str = Query(min_length=1),
    ctx: Any = Depends(get_ctx),
) -> FileResponse:
    """下载 Agent 通过 send_file_to_user 生成的 Workspace 文件。

    ``agentId`` 用来定位并校验 Agent Workspace；``path`` 可以是 CUSTOM/FILE
    事件对应的绝对文件路径，也可以是相对于 Workspace 的路径。
    """
    channel = await _get_runtime(agent_id, ctx)
    target = _resolve_agent_download_file(channel, path)
    return FileResponse(
        path=target,
        filename=target.name,
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@router.post("/files")
async def upload_file(
    agent_id: str = Query(alias="agentId", min_length=1),
    file: UploadFile = File(...),
    user_id: Optional[str] = Form(default=None, alias="userId"),
    session_id: Optional[str] = Form(default=None, alias="sessionId"),
    ctx: Any = Depends(get_ctx),
) -> dict[str, Any]:
    """上传文件接口，返回可被 /chat 直接引用的服务器内路径。

    请求为 multipart/form-data：

    - ``file``（必填）：上传的文件内容；
    - ``userId``（可选）：兼容旧调用方，路径模式不参与文件校验；
    - ``sessionId``（可选）：兼容旧调用方，路径模式不参与文件校验。

    返回 ``{"file_url", "name", "size"}``。聊天文件块直接传入返回的
    file_url。
    HTTP 插件不校验回调 Token。
    """
    del user_id, session_id

    # 1. 直接定位 Agent Workspace，与频道运行状态无关。
    channel = await _get_runtime(agent_id, ctx)

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
    upload_dir_name = uuid.uuid4().hex
    dest_dir = (
        Path(workspace_dir).expanduser()
        / "media"
        / "service-channel"
        / upload_dir_name
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

    # 5. 通过 file_url 返回可直接用于 /chat 的绝对路径，不创建内存映射。
    resolved_dest = dest.resolve()
    logger.info(
        "Operations Assistant file uploaded: agent=%s file=%s size=%d path=%s",
        agent_id,
        safe_name,
        size,
        resolved_dest,
    )
    return {"file_url": str(resolved_dest), "name": safe_name, "size": size}
