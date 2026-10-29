"""Datasource SQL query tool implementation."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import socket
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from agentscope.message import TextBlock, ToolResultState
from agentscope.tool import ToolChunk
from qwenpaw.plugins import get_tool_config

logger = logging.getLogger(__name__)

_TOOL_NAME = "query_datasource"
_DEFAULT_MARKET_URL = "http://10.12.40.35:7055/market/datasource/execute-sql"
_DEFAULT_CUSTOM_URL = "http://10.12.40.35:7055/custom/datasource/execute-sql"
_READ_PREFIXES = {"SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "WITH"}
_WRITE_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|REPLACE|MERGE|"
    r"GRANT|REVOKE|CALL|EXECUTE)\b",
    re.IGNORECASE,
)


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


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _as_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def _validate_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("接口 URL 必须是有效的 http:// 或 https:// 地址")


def _validate_read_only_sql(sql: str) -> None:
    stripped = sql.strip()
    if not stripped:
        raise ValueError("SQL 不能为空")
    without_trailing_semicolon = (
        stripped[:-1].rstrip() if stripped.endswith(";") else stripped
    )
    if ";" in without_trailing_semicolon:
        raise ValueError("只允许执行单条 SQL")
    first_word_match = re.match(r"^[A-Za-z]+", without_trailing_semicolon)
    first_word = first_word_match.group(0).upper() if first_word_match else ""
    if first_word not in _READ_PREFIXES or _WRITE_KEYWORDS.search(
        without_trailing_semicolon
    ):
        raise ValueError("当前配置仅允许 SELECT/SHOW/DESCRIBE/EXPLAIN/WITH 只读查询")


def _post_json(
    url: str,
    payload: dict[str, str],
    timeout_seconds: int,
    max_response_chars: int,
) -> tuple[int, str, bool]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    request = Request(url, data=body, headers=headers, method="POST")
    max_bytes = max_response_chars * 4
    with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
        raw = response.read(max_bytes + 1)
        truncated = len(raw) > max_bytes
        if truncated:
            raw = raw[:max_bytes]
        charset = response.headers.get_content_charset() or "utf-8"
        return response.status, raw.decode(charset, errors="replace"), truncated


async def query_datasource(
    datasource_name: str,
    sql: str,
    scope: Literal["MARKET", "CUSTOM"],
) -> ToolChunk:
    """通过指定数据源执行 SQL 查询。

    Args:
        datasource_name: 数据源名称。该值会作为接口的 datasourceName 字段发送。
        sql: 要执行的查询语句。默认仅允许只读 SQL。
        scope: 数据源范围。MARKET 调用市场数据源接口；CUSTOM 调用自定义
            数据源接口。

    Returns:
        包含接口查询结果的 ToolChunk。
    """
    config = get_tool_config(_TOOL_NAME) or {}
    normalized_scope = str(scope).strip().upper()
    if normalized_scope not in {"MARKET", "CUSTOM"}:
        return _error("scope 只能是 MARKET 或 CUSTOM")

    datasource_name = str(datasource_name).strip()
    sql = str(sql).strip()
    if not datasource_name:
        return _error("datasource_name 不能为空")
    if not sql:
        return _error("sql 不能为空")

    if _as_bool(config.get("read_only"), True):
        try:
            _validate_read_only_sql(sql)
        except ValueError as exc:
            return _error(str(exc))

    configured_endpoint = (
        config.get("market_url")
        if normalized_scope == "MARKET"
        else config.get("custom_url")
    )
    endpoint = str(configured_endpoint or "").strip()
    if not endpoint:
        endpoint = (
            _DEFAULT_MARKET_URL if normalized_scope == "MARKET" else _DEFAULT_CUSTOM_URL
        )
    try:
        _validate_url(endpoint)
    except ValueError as exc:
        return _error(str(exc))

    timeout_seconds = _as_int(config.get("timeout_seconds"), 30, 1, 600)
    max_response_chars = _as_int(
        config.get("max_response_chars"),
        100_000,
        1_000,
        1_000_000,
    )
    payload = {"datasourceName": datasource_name, "sql": sql}

    try:
        status, response_text, truncated = await asyncio.to_thread(
            _post_json,
            endpoint,
            payload,
            timeout_seconds,
            max_response_chars,
        )
    except HTTPError as exc:
        detail = exc.read(4096).decode("utf-8", errors="replace")
        logger.warning("Datasource API returned HTTP %s: %s", exc.code, detail)
        return _error(f"数据源接口请求失败：HTTP {exc.code}，{detail}")
    except (URLError, socket.timeout, TimeoutError) as exc:
        logger.warning("Datasource API connection failed: %s", exc)
        return _error(f"数据源接口连接失败或超时：{exc}")
    except Exception as exc:  # noqa: BLE001
        logger.exception("Datasource query failed")
        return _error(f"数据查询失败：{exc}")

    try:
        result: Any = json.loads(response_text) if not truncated else response_text
    except json.JSONDecodeError:
        result = response_text

    output = {
        "scope": normalized_scope,
        "datasourceName": datasource_name,
        "httpStatus": status,
        "truncated": truncated,
        "result": result,
    }
    return _success(json.dumps(output, ensure_ascii=False, separators=(",", ":")))
