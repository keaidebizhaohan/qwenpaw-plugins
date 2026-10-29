"""Call an Agent hosted by another QwenPaw service."""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import time
import uuid
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

from agentscope.message import TextBlock, ToolResultState
from agentscope.tool import ToolChunk
from qwenpaw.plugins import get_tool_config

logger = logging.getLogger(__name__)

_TOOL_NAME = "call_remote_qwenpaw_agent"


class RemoteAgentError(RuntimeError):
    """A controlled remote-agent invocation failure."""


def _error(message: str) -> ToolChunk:
    return ToolChunk(
        state=ToolResultState.ERROR,
        content=[TextBlock(type="text", text=message)],
    )


def _success(message: str) -> ToolChunk:
    return ToolChunk(
        state=ToolResultState.SUCCESS,
        content=[TextBlock(type="text", text=message)],
    )


def _as_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def _normalize_base_url(base_url: str) -> str:
    """Validate and normalize the caller-selected QwenPaw address."""
    normalized = str(base_url or "").strip().rstrip("/")
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("base_url 必须是有效的 http:// 或 https:// 地址")
    if parsed.query or parsed.fragment:
        raise ValueError("base_url 只能填写服务根地址，不能包含查询参数或片段")
    return normalized


def _request_json(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None,
    timeout_seconds: float,
    authorization_header: str,
) -> dict[str, Any]:
    """Send one JSON request using only Python's standard library."""
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if authorization_header:
        headers["Authorization"] = authorization_header

    request = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
            raw = response.read(10 * 1024 * 1024 + 1)
            if len(raw) > 10 * 1024 * 1024:
                raise RemoteAgentError("远程 QwenPaw 返回的任务数据超过 10 MiB")
            charset = response.headers.get_content_charset() or "utf-8"
            text = raw.decode(charset, errors="replace")
    except HTTPError as exc:
        detail = exc.read(4096).decode("utf-8", errors="replace")
        raise RemoteAgentError(
            f"远程 QwenPaw 请求失败：HTTP {exc.code}，{detail}",
        ) from exc
    except (URLError, socket.timeout, TimeoutError) as exc:
        raise RemoteAgentError(f"远程 QwenPaw 连接失败或超时：{exc}") from exc

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RemoteAgentError(
            f"远程 QwenPaw 返回的不是 JSON：{text[:500]}",
        ) from exc
    if not isinstance(parsed, dict):
        raise RemoteAgentError("远程 QwenPaw 返回格式错误：顶层必须是 JSON 对象")
    return parsed


def _content_text(value: Any) -> str:
    """Extract final visible text from a QwenPaw response fragment."""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts = [_content_text(item) for item in value]
        return "".join(part for part in parts if part)
    if not isinstance(value, dict):
        return ""

    value_type = str(value.get("type") or "").lower()
    if value_type in {"text", "output_text"} and isinstance(
        value.get("text"),
        str,
    ):
        return value["text"]

    content = value.get("content")
    if content is not None:
        extracted = _content_text(content)
        if extracted:
            return extracted
    return ""


def _extract_final_answer(result: dict[str, Any]) -> str:
    """Extract the assistant's final answer from a completed task result."""
    direct_answer = result.get("answer")
    if isinstance(direct_answer, str) and direct_answer.strip():
        return direct_answer.strip()

    output = result.get("output")
    if isinstance(output, list):
        # Prefer the last assistant message. Some versions omit ``role``;
        # those entries remain valid fallbacks.
        fallback = ""
        for item in reversed(output):
            text = _content_text(item).strip()
            if not text:
                continue
            if isinstance(item, dict) and str(item.get("role") or "").lower() == "assistant":
                return text
            if not fallback:
                fallback = text
        if fallback:
            return fallback

    # Be tolerant of a future response wrapper while remaining selective:
    # only recurse into known result/response keys, not arbitrary metadata.
    for key in ("result", "response"):
        nested = result.get(key)
        if isinstance(nested, dict):
            answer = _extract_final_answer(nested)
            if answer:
                return answer
    return ""


def _call_remote_task(
    *,
    base_url: str,
    agent_id: str,
    prompt: str,
    session_id: str,
    user_id: str,
    authorization_header: str,
    timeout_seconds: int,
    poll_interval_ms: int,
) -> tuple[str, dict[str, Any]]:
    """Submit and poll a native QwenPaw background chat task."""
    encoded_agent_id = quote(agent_id, safe="")
    task_url = f"{base_url}/api/agents/{encoded_agent_id}/console/chat/task"
    payload = {
        "channel": "console",
        "user_id": user_id,
        "session_id": session_id,
        "input": [
            {
                "role": "user",
                "type": "message",
                "content": [{"type": "text", "text": prompt}],
            },
        ],
        "timeout": timeout_seconds,
    }
    submitted = _request_json(
        "POST",
        task_url,
        payload=payload,
        timeout_seconds=min(timeout_seconds, 60),
        authorization_header=authorization_header,
    )
    task_id = str(submitted.get("task_id") or "").strip()
    if not task_id:
        raise RemoteAgentError("远程 QwenPaw 未返回 task_id")

    status_url = f"{task_url}/{quote(task_id, safe='')}"
    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RemoteAgentError(
                f"远程 Agent 在 {timeout_seconds} 秒内没有完成处理",
            )
        status = _request_json(
            "GET",
            status_url,
            payload=None,
            timeout_seconds=min(max(remaining, 1), 30),
            authorization_header=authorization_header,
        )
        if status.get("status") == "finished":
            result = status.get("result")
            if not isinstance(result, dict):
                raise RemoteAgentError("远程任务已结束，但没有返回 result")
            if result.get("status") == "failed":
                error = result.get("error")
                if isinstance(error, dict):
                    detail = str(error.get("message") or error)
                else:
                    detail = str(error or "未知错误")
                raise RemoteAgentError(f"远程 Agent 执行失败：{detail}")
            answer = _extract_final_answer(result)
            if not answer:
                raise RemoteAgentError("远程 Agent 已完成，但未找到最终文本回答")
            return answer, result
        if status.get("status") not in {"running", "submitted"}:
            raise RemoteAgentError(
                f"远程任务返回未知状态：{status.get('status')!r}",
            )
        time.sleep(poll_interval_ms / 1000)


async def call_remote_qwenpaw_agent(
    base_url: str,
    agent_id: str,
    prompt: str,
    session_id: str = "",
    user_id: str = "",
) -> ToolChunk:
    """调用外部 QwenPaw 服务中的 Agent。

    Args:
        base_url: 外部 QwenPaw 服务根地址，例如 ``http://10.12.40.1:8088``。
            应从当前工作区 AGENTS.md 的远程 Agent 路由表中选择。
        agent_id: 外部 QwenPaw 中要调用的 Agent ID，例如 ``default``。
        prompt: 要交给远程 Agent 处理的完整问题或任务说明。
        session_id: 可选。复用同一个值可延续远程会话；不传时自动创建
            一个全新会话，避免继承旧上下文。
        user_id: 可选。远程会话的用户标识；不传时使用插件配置默认值。

    Returns:
        包含远程 Agent 最终回答、实际 sessionId 和目标信息的 ToolChunk。
    """
    config = get_tool_config(_TOOL_NAME) or {}
    try:
        normalized_base_url = _normalize_base_url(base_url)
    except ValueError as exc:
        return _error(str(exc))

    normalized_agent_id = str(agent_id or "").strip()
    normalized_prompt = str(prompt or "").strip()
    if not normalized_agent_id:
        return _error("agent_id 不能为空")
    if not normalized_prompt:
        return _error("prompt 不能为空")

    effective_session_id = str(session_id or "").strip()
    if not effective_session_id:
        effective_session_id = f"remote:{normalized_agent_id}:{uuid.uuid4().hex}"
    effective_user_id = str(user_id or "").strip() or str(
        config.get("default_user_id") or "qwenpaw-remote-caller",
    ).strip()
    authorization_header = str(
        config.get("authorization_header") or "",
    ).strip()
    timeout_seconds = _as_int(
        config.get("timeout_seconds"),
        1800,
        10,
        7200,
    )
    poll_interval_ms = _as_int(
        config.get("poll_interval_ms"),
        500,
        100,
        5000,
    )
    max_answer_chars = _as_int(
        config.get("max_answer_chars"),
        100_000,
        1_000,
        1_000_000,
    )

    try:
        answer, result = await asyncio.to_thread(
            _call_remote_task,
            base_url=normalized_base_url,
            agent_id=normalized_agent_id,
            prompt=normalized_prompt,
            session_id=effective_session_id,
            user_id=effective_user_id,
            authorization_header=authorization_header,
            timeout_seconds=timeout_seconds,
            poll_interval_ms=poll_interval_ms,
        )
    except RemoteAgentError as exc:
        logger.warning(
            "Remote QwenPaw agent call failed for %s agent=%s: %s",
            normalized_base_url,
            normalized_agent_id,
            exc,
        )
        return _error(str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected remote QwenPaw agent call failure")
        return _error(f"调用远程 QwenPaw Agent 失败：{exc}")

    truncated = len(answer) > max_answer_chars
    if truncated:
        answer = answer[:max_answer_chars] + "\n...[远程回答已截断]"
    remote_session_id = str(result.get("session_id") or effective_session_id)
    output = {
        "baseUrl": normalized_base_url,
        "agentId": normalized_agent_id,
        "sessionId": remote_session_id,
        "userId": effective_user_id,
        "truncated": truncated,
        "answer": answer,
    }
    return _success(json.dumps(output, ensure_ascii=False, separators=(",", ":")))

