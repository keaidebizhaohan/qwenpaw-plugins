"""Unit tests for ``call_remote_qwenpaw_agent``."""

from __future__ import annotations

import asyncio
import importlib.util
import json
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(__file__).resolve().parents[1] / "remote_agent_tool.py"
SPEC = importlib.util.spec_from_file_location("remote_agent_tool_tested", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
tool = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tool)


def _tool_text(chunk) -> str:
    return chunk.content[0]["text"] if isinstance(chunk.content[0], dict) else chunk.content[0].text


def test_extract_final_answer_prefers_last_assistant_message() -> None:
    result = {
        "output": [
            {"role": "assistant", "content": [{"type": "text", "text": "old"}]},
            {"role": "assistant", "content": [{"type": "text", "text": "final"}]},
        ],
    }
    assert tool._extract_final_answer(result) == "final"


def test_call_uses_explicit_route_and_generated_session() -> None:
    captured = {}

    def fake_call(**kwargs):
        captured.update(kwargs)
        return "远程回答", {"session_id": kwargs["session_id"]}

    with (
        patch.object(tool, "get_tool_config", return_value={}),
        patch.object(tool, "_call_remote_task", side_effect=fake_call),
    ):
        chunk = asyncio.run(
            tool.call_remote_qwenpaw_agent(
                "http://10.12.40.1:8088/",
                "analysis-agent",
                "分析这个问题",
            ),
        )

    output = json.loads(_tool_text(chunk))
    assert captured["base_url"] == "http://10.12.40.1:8088"
    assert captured["agent_id"] == "analysis-agent"
    assert captured["session_id"].startswith("remote:analysis-agent:")
    assert output["answer"] == "远程回答"
    assert output["sessionId"] == captured["session_id"]


def test_call_reuses_supplied_session_and_user() -> None:
    captured = {}

    def fake_call(**kwargs):
        captured.update(kwargs)
        return "继续回答", {"session_id": kwargs["session_id"]}

    with (
        patch.object(tool, "get_tool_config", return_value={}),
        patch.object(tool, "_call_remote_task", side_effect=fake_call),
    ):
        asyncio.run(
            tool.call_remote_qwenpaw_agent(
                "http://remote:8088",
                "default",
                "继续",
                session_id="fixed-session",
                user_id="parent-agent",
            ),
        )
    assert captured["session_id"] == "fixed-session"
    assert captured["user_id"] == "parent-agent"


def test_remote_failure_becomes_tool_error() -> None:
    with (
        patch.object(tool, "get_tool_config", return_value={}),
        patch.object(
            tool,
            "_call_remote_task",
            side_effect=tool.RemoteAgentError("目标 Agent 不存在"),
        ),
    ):
        chunk = asyncio.run(
            tool.call_remote_qwenpaw_agent(
                "http://remote:8088",
                "missing",
                "hello",
            ),
        )
    assert "目标 Agent 不存在" in _tool_text(chunk)


def test_invalid_base_url_is_rejected() -> None:
    with patch.object(tool, "get_tool_config", return_value={}):
        chunk = asyncio.run(
            tool.call_remote_qwenpaw_agent(
                "not-a-url",
                "default",
                "hello",
            ),
        )
    assert "base_url" in _tool_text(chunk)
