"""Tool for calling agents exposed by JiuwenSwarm Gateway."""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from typing import Any

from agentscope.message import TextBlock, ToolResultState
from agentscope.tool import ToolChunk
from qwenpaw.plugins import get_tool_config

TOOL_NAME = "call_k8s_agent"


def _config() -> dict[str, Any]:
    value = get_tool_config(TOOL_NAME) or {}
    return value if isinstance(value, dict) else {}


def _normalise_base_url(value: str) -> str:
    url = str(value or "").strip().rstrip("/")
    if not url.startswith(("http://", "https://")):
        raise ValueError("gateway_base_url 必须以 http:// 或 https:// 开头")
    return url


def _headers(config: dict[str, Any], user_id: str, group_id: str, bot_id: str) -> dict[str, str]:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "X-User-Id": user_id,
        "X-Group-Id": group_id,
        "X-Bot-Id": bot_id,
    }
    authorization = str(config.get("authorization_header") or "").strip()
    if authorization:
        headers["Authorization"] = authorization
    return headers


def _http_request(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout: float,
) -> tuple[int, str, str]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return (
                int(response.status),
                response.read().decode("utf-8", errors="replace"),
                response.headers.get("Content-Type", ""),
            )
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Gateway HTTP {exc.code}: {body[:2000]}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"无法连接 Gateway: {exc.reason}") from exc


async def _request(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout: float,
) -> tuple[int, str, str]:
    return await asyncio.to_thread(_http_request, url, payload, headers, timeout)


def _json_object(body: str) -> dict[str, Any] | None:
    try:
        value = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        return None
    return value if isinstance(value, dict) else None


def _find_session_id(value: Any) -> str | None:
    if isinstance(value, dict):
        for key in ("session_id", "sessionId"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        for key in ("data", "payload", "result"):
            found = _find_session_id(value.get(key))
            if found:
                return found
    return None


def _error_message(value: dict[str, Any]) -> str | None:
    if value.get("ok") is False:
        error = value.get("error")
        if isinstance(error, dict):
            return str(error.get("message") or error.get("error") or error)
        return str(error or "Gateway 返回 ok=false")
    return None


def _extract_text(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts = [text for item in value if (text := _extract_text(item))]
        return "".join(parts) or None
    if not isinstance(value, dict):
        return None
    for key in ("content", "answer", "text", "output", "response"):
        if key in value and (text := _extract_text(value[key])):
            return text
    for key in ("data", "payload", "result", "message"):
        if key in value and (text := _extract_text(value[key])):
            return text
    return None


def _sse_answer(body: str) -> tuple[str, str | None]:
    chunks: list[str] = []
    final_text: str | None = None
    error_text: str | None = None
    event_name = ""
    data_lines: list[str] = []

    def consume() -> None:
        nonlocal event_name, data_lines, final_text, error_text
        raw = "\n".join(data_lines).strip()
        data_lines = []
        if not raw or raw == "[DONE]":
            event_name = ""
            return
        parsed = _json_object(raw)
        event = parsed.get("payload", parsed) if isinstance(parsed, dict) else None
        kind = event_name or (str(event.get("event_type") or "") if isinstance(event, dict) else "")
        text = _extract_text(event) if event is not None else raw
        if "error" in kind:
            error_text = text or raw
        elif kind in {"chat.delta", "delta"}:
            if text:
                chunks.append(text)
        elif kind in {"chat.final", "final", "chat.completed", "completed"}:
            if text:
                final_text = text
        elif text and not chunks and final_text is None:
            final_text = text
        event_name = ""

    for line in body.splitlines() + [""]:
        if not line.strip():
            consume()
        elif line.startswith("event:"):
            event_name = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    return final_text if final_text is not None else "".join(chunks), error_text


def _response_answer(body: str, content_type: str) -> str:
    if "text/event-stream" in content_type.lower() or body.lstrip().startswith(("data:", "event:")):
        answer, error = _sse_answer(body)
        if error:
            raise RuntimeError(error)
        if answer:
            return answer
        raise RuntimeError("Gateway 流式响应结束，但没有返回回答内容")
    parsed = _json_object(body)
    if parsed is None:
        if body.strip():
            return body.strip()
        raise RuntimeError("Gateway 返回了空响应")
    if error := _error_message(parsed):
        raise RuntimeError(error)
    answer = _extract_text(parsed)
    if not answer:
        raise RuntimeError(f"无法从 Gateway 响应中提取回答: {body[:2000]}")
    return answer


async def _call_k8s_gateway(
    base_url: str,
    bot_id: str,
    prompt: str,
    session_id: str | None,
    user_id: str,
    group_id: str,
    config: dict[str, Any],
    timeout: float,
) -> tuple[str, str]:
    headers = _headers(config, user_id, group_id, bot_id)
    active_session = (session_id or "").strip()
    if not active_session:
        _, body, _ = await _request(
            f"{base_url}/api/v1/sessions",
            {"title": "QwenPaw remote Gateway call", "workMode": "work"},
            headers,
            timeout,
        )
        result = _json_object(body)
        if result is None:
            raise RuntimeError(f"Gateway 建会话响应不是 JSON: {body[:2000]}")
        if error := _error_message(result):
            raise RuntimeError(error)
        active_session = _find_session_id(result) or ""
        if not active_session:
            raise RuntimeError(f"Gateway 建会话成功但未返回 session_id: {body[:2000]}")
    _, body, content_type = await _request(
        f"{base_url}/api/v1/chat/completions",
        {"session_id": active_session, "query": prompt, "enable_streaming": False},
        headers,
        timeout,
    )
    return active_session, _response_answer(body, content_type)


async def call_k8s_agent(
    gateway_base_url: str,
    bot_id: str,
    prompt: str,
    session_id: str = "",
    user_id: str = "",
    group_id: str = "",
) -> ToolChunk:
    """Call a Gateway-hosted Agent and return its final answer.

    Args:
        gateway_base_url: Gateway base URL, e.g. ``http://10.12.40.4:30092``.
        bot_id: Manager Agent resource ID used as ``X-Bot-Id``/``bot_id``.
        prompt: The question and any necessary context for the remote Agent.
        session_id: Reuse the previous returned sessionId for a follow-up.
        user_id: Optional Gateway user ID.
        group_id: Optional Gateway group ID.
    """
    try:
        config = _config()
        base_url = _normalise_base_url(gateway_base_url)
        active_bot_id = str(bot_id or "").strip()
        active_prompt = str(prompt or "").strip()
        if not active_bot_id:
            raise ValueError("bot_id 不能为空")
        if not active_prompt:
            raise ValueError("prompt 不能为空")
        active_user_id = str(user_id or config.get("default_user_id") or "qwenpaw-gateway-caller").strip()
        active_group_id = str(group_id or config.get("default_group_id") or "default").strip()
        timeout = max(10.0, min(float(config.get("timeout_seconds") or 1800), 7200.0))
        max_chars = max(1000, min(int(config.get("max_answer_chars") or 100000), 1000000))
        active_session, answer = await _call_k8s_gateway(
            base_url,
            active_bot_id,
            active_prompt,
            session_id,
            active_user_id,
            active_group_id,
            config,
            timeout,
        )
        truncated = len(answer) > max_chars
        if truncated:
            answer = answer[:max_chars] + "\n...[回答因长度限制被截断]"
        result = {
            "gatewayBaseUrl": base_url,
            "botId": active_bot_id,
            "sessionId": active_session,
            "userId": active_user_id,
            "groupId": active_group_id,
            "truncated": truncated,
            "answer": answer,
        }
        return ToolChunk(
            content=[TextBlock(type="text", text=json.dumps(result, ensure_ascii=False))],
            state=ToolResultState.SUCCESS,
        )
    except Exception as exc:  # Tool failures must be returned to the Agent, not crash it.
        return ToolChunk(
            content=[TextBlock(type="text", text=f"Gateway Agent 调用失败: {exc}")],
            state=ToolResultState.ERROR,
        )
