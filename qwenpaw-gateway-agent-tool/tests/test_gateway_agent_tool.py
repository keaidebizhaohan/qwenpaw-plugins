from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import types
from enum import Enum
from pathlib import Path


class ToolResultState(str, Enum):
    SUCCESS = "success"
    ERROR = "error"


class TextBlock(dict):
    def __init__(self, *, type: str, text: str):
        super().__init__(type=type, text=text)
        self.type = type
        self.text = text


class ToolChunk:
    def __init__(self, *, content, state):
        self.content = content
        self.state = state


agentscope = types.ModuleType("agentscope")
agentscope_message = types.ModuleType("agentscope.message")
agentscope_message.TextBlock = TextBlock
agentscope_message.ToolResultState = ToolResultState
agentscope_tool = types.ModuleType("agentscope.tool")
agentscope_tool.ToolChunk = ToolChunk
qwenpaw = types.ModuleType("qwenpaw")
qwenpaw_plugins = types.ModuleType("qwenpaw.plugins")
qwenpaw_plugins.get_tool_config = lambda _name: {}
sys.modules.setdefault("agentscope", agentscope)
sys.modules.setdefault("agentscope.message", agentscope_message)
sys.modules.setdefault("agentscope.tool", agentscope_tool)
sys.modules.setdefault("qwenpaw", qwenpaw)
sys.modules.setdefault("qwenpaw.plugins", qwenpaw_plugins)

MODULE_PATH = Path(__file__).resolve().parents[1] / "gateway_agent_tool.py"
SPEC = importlib.util.spec_from_file_location("gateway_agent_tool_under_test", MODULE_PATH)
assert SPEC and SPEC.loader
tool = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tool)


def _result(chunk: ToolChunk) -> dict:
    return json.loads(chunk.content[0].text)


def test_find_session_id_in_envelope():
    assert tool._find_session_id({"data": {"sessionId": "web_123"}}) == "web_123"


def test_parse_gateway_sse_envelope():
    body = "\n".join(
        [
            'data: {"payload":{"event_type":"chat.delta","content":"你"}}',
            "",
            'data: {"payload":{"event_type":"chat.delta","content":"好"}}',
            "",
            "data: [DONE]",
            "",
        ]
    )
    answer, error = tool._sse_answer(body)
    assert answer == "你好"
    assert error is None


def test_k8s_gateway_creates_session_and_returns_answer(monkeypatch):
    calls = []

    async def fake_request(url, payload, headers, timeout):
        calls.append((url, payload, headers, timeout))
        if url.endswith("/api/v1/sessions"):
            return 201, '{"ok":true,"data":{"sessionId":"web_abc"}}', "application/json"
        return 200, '{"ok":true,"data":{"event_type":"chat.final","content":"审查完成"}}', "application/json"

    monkeypatch.setattr(tool, "_request", fake_request)
    chunk = asyncio.run(
        tool.call_k8s_agent(
            "http://gateway.example/",
            "resource-id",
            "审查代码",
            group_id="test-group",
        )
    )
    value = _result(chunk)
    assert chunk.state == ToolResultState.SUCCESS
    assert value["sessionId"] == "web_abc"
    assert value["answer"] == "审查完成"
    assert calls[0][0] == "http://gateway.example/api/v1/sessions"
    assert calls[1][1]["session_id"] == "web_abc"
    assert calls[1][2]["X-Bot-Id"] == "resource-id"


def test_k8s_gateway_reuses_session(monkeypatch):
    calls = []

    async def fake_request(url, payload, headers, timeout):
        calls.append((url, payload))
        return 200, '{"answer":"继续回答"}', "application/json"

    monkeypatch.setattr(tool, "_request", fake_request)
    chunk = asyncio.run(
        tool.call_k8s_agent(
            "http://gateway.example",
            "resource-id",
            "继续",
            session_id="web_existing",
        )
    )
    assert _result(chunk)["sessionId"] == "web_existing"
    assert len(calls) == 1
    assert calls[0][0].endswith("/api/v1/chat/completions")


def test_invalid_url_is_controlled_tool_error():
    chunk = asyncio.run(tool.call_k8s_agent("gateway.example", "resource-id", "你好"))
    assert chunk.state == ToolResultState.ERROR
    assert "http://" in chunk.content[0].text
