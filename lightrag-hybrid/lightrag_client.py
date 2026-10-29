# -*- coding: utf-8 -*-
"""Shared HTTP client copied into each standalone LightRAG plugin."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import httpx
from agentscope.message import TextBlock, ToolResultState
from agentscope.tool import ToolChunk

logger = logging.getLogger(__name__)

# The user intentionally wants this LightRAG deployment fixed in every plugin.
LIGHTRAG_WEBUI_URL = "http://10.12.40.32:9621/webui/"
LIGHTRAG_BASE_URL = LIGHTRAG_WEBUI_URL.removesuffix("webui/").rstrip("/")
REQUEST_TIMEOUT = 120.0

_NUMBER_FIELDS = (
    "top_k",
    "chunk_top_k",
    "max_entity_tokens",
    "max_relation_tokens",
    "max_total_tokens",
)


def _chunk(state: ToolResultState, text: str) -> ToolChunk:
    return ToolChunk(
        state=state,
        content=[TextBlock(type="text", text=text)],
    )


def _error(text: str) -> ToolChunk:
    return _chunk(
        ToolResultState.ERROR,
        f"LightRAG query failed: {text}",
    )


def _positive_int(value: Any, field: str) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a positive integer")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a positive integer") from exc
    if normalized < 1:
        raise ValueError(f"{field} must be a positive integer")
    return normalized


def _normalize_workspaces(workspaces: list[str] | None) -> list[str]:
    """Normalize an Agent-selected list while retaining its order."""
    selected: list[str] = []
    for value in workspaces or []:
        name = str(value).strip()
        if name and name not in selected:
            selected.append(name)
    return selected


async def _get_available_workspaces(client: httpx.AsyncClient) -> list[str]:
    response = await client.get(f"{LIGHTRAG_BASE_URL}/workspaces")
    if response.is_error:
        raise ValueError(
            f"cannot list knowledge bases: HTTP {response.status_code}",
        )
    try:
        data = response.json()
    except json.JSONDecodeError as exc:
        raise ValueError("workspace endpoint returned invalid JSON") from exc
    items = data.get("workspaces", []) if isinstance(data, dict) else []
    names = [
        str(item.get("name") or "").strip()
        for item in items
        if isinstance(item, dict) and str(item.get("name") or "").strip()
    ]
    return list(dict.fromkeys(names))


def _workspace_selection_message(names: list[str], mode: str) -> str:
    if not names:
        return "No LightRAG knowledge bases are currently available."
    choices = "\n".join(f"- {name}" for name in names)
    return (
        f"Available LightRAG knowledge bases for {mode} mode:\n"
        f"{choices}\n\n"
        "Choose one or more knowledge bases that best match the user's "
        "question, then call this same tool again with the `workspaces` "
        "argument. Do not ask the user unless the names are ambiguous."
    )


def _format_one_result(data: dict[str, Any], workspace: str) -> str:
    answer = str(data.get("response") or "").strip()
    if not answer:
        answer = "No relevant context was returned."
    lines = [f"## Knowledge base: {workspace}", answer]
    references = data.get("references")
    if isinstance(references, list) and references:
        lines.append("\nReferences:")
        for item in references:
            if not isinstance(item, dict):
                continue
            ref_id = str(item.get("reference_id") or "").strip()
            file_path = str(item.get("file_path") or "").strip()
            label = " — ".join(part for part in (ref_id, file_path) if part)
            if label:
                lines.append(f"- {label}")
    return "\n".join(lines)


async def _query_one_workspace(
    client: httpx.AsyncClient,
    workspace: str,
    payload: dict[str, Any],
) -> tuple[str, dict[str, Any] | None, str | None]:
    response = await client.post(
        f"{LIGHTRAG_BASE_URL}/query",
        headers={
            "Content-Type": "application/json",
            "LIGHTRAG-WORKSPACE": workspace,
        },
        json=payload,
    )
    if response.is_error:
        detail = response.text.strip()
        try:
            detail = str(response.json().get("detail") or detail)
        except (ValueError, AttributeError):
            pass
        return (
            workspace,
            None,
            f"HTTP {response.status_code}: "
            f"{(detail or 'no error details')[:1000]}",
        )
    try:
        data = response.json()
    except json.JSONDecodeError:
        return workspace, None, "server returned invalid JSON"
    if not isinstance(data, dict):
        return workspace, None, "server returned an unexpected response"
    return workspace, data, None


async def query_lightrag(
    *,
    tool_name: str,
    mode: str,
    query: str,
    workspaces: list[str] | None = None,
    top_k: int | None = 40,
    chunk_top_k: int | None = 20,
    max_entity_tokens: int | None = 6000,
    max_relation_tokens: int | None = 8000,
    max_total_tokens: int | None = 30000,
    response_type: str | None = None,
    include_references: bool = True,
    include_chunk_content: bool = False,
    enable_rerank: bool = True,
) -> ToolChunk:
    """Discover workspaces or query one/many selected by the Agent."""
    del tool_name  # Kept in the API so each plugin has a uniform contract.
    try:
        cleaned_query = query.strip()
        if len(cleaned_query) < 3:
            raise ValueError("query must contain at least 3 characters")

        selected = _normalize_workspaces(workspaces)
        async with httpx.AsyncClient(
            timeout=REQUEST_TIMEOUT,
            trust_env=False,
        ) as client:
            available = await _get_available_workspaces(client)
            if not selected:
                return _chunk(
                    ToolResultState.SUCCESS,
                    _workspace_selection_message(available, mode),
                )

            unknown = [name for name in selected if name not in available]
            if unknown:
                raise ValueError(
                    "unknown knowledge base(s): "
                    f"{', '.join(unknown)}; available: "
                    f"{', '.join(available) or '(none)'}",
                )

            runtime_values = {
                "top_k": top_k,
                "chunk_top_k": chunk_top_k,
                "max_entity_tokens": max_entity_tokens,
                "max_relation_tokens": max_relation_tokens,
                "max_total_tokens": max_total_tokens,
            }
            payload: dict[str, Any] = {
                "query": cleaned_query,
                "mode": mode,
                "include_references": include_references,
                "include_chunk_content": include_chunk_content,
                "enable_rerank": enable_rerank,
            }
            for field in _NUMBER_FIELDS:
                normalized = _positive_int(runtime_values[field], field)
                if normalized is not None:
                    payload[field] = normalized
            resolved_response_type = str(response_type or "").strip()
            if resolved_response_type:
                payload["response_type"] = resolved_response_type

            results = await asyncio.gather(
                *(
                    _query_one_workspace(client, workspace, payload)
                    for workspace in selected
                ),
            )

        sections: list[str] = []
        success_count = 0
        for workspace, data, error in results:
            if error:
                sections.append(
                    f"## Knowledge base: {workspace}\nQuery failed: {error}",
                )
                continue
            success_count += 1
            sections.append(_format_one_result(data or {}, workspace))
        state = (
            ToolResultState.SUCCESS
            if success_count
            else ToolResultState.ERROR
        )
        sections.append(f"\n[LightRAG mode={mode}]")
        return _chunk(state, "\n\n".join(sections))
    except httpx.TimeoutException:
        return _error("the LightRAG server timed out")
    except httpx.RequestError as exc:
        logger.warning("LightRAG request error: %s", exc)
        return _error(f"unable to reach the LightRAG server: {exc}")
    except (TypeError, ValueError) as exc:
        return _error(str(exc))
    except Exception as exc:
        logger.exception("Unexpected LightRAG plugin failure")
        return _error(f"unexpected error: {exc}")
