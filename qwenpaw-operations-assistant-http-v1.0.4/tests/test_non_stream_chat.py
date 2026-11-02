"""运维助手 stream=false 非流式响应测试。"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


PACKAGE_ROOT = Path(__file__).parents[1]
PACKAGE_NAME = "operations_assistant_non_stream_test"
SPEC = importlib.util.spec_from_file_location(
    PACKAGE_NAME,
    PACKAGE_ROOT / "__init__.py",
    submodule_search_locations=[str(PACKAGE_ROOT)],
)
assert SPEC is not None and SPEC.loader is not None
PACKAGE = importlib.util.module_from_spec(SPEC)
sys.modules[PACKAGE_NAME] = PACKAGE
SPEC.loader.exec_module(PACKAGE)
HTTP_API = importlib.import_module(f"{PACKAGE_NAME}.http_api")


def final_response(with_reasoning: bool = True) -> dict:
    output = []
    if with_reasoning:
        output.append(
            {
                "id": "msg-reasoning",
                "type": "reasoning",
                "role": "assistant",
                "content": [
                    {
                        "type": "text",
                        "delta": False,
                        "text": "\n分析用户问题\n",
                    },
                ],
                "object": "message",
            },
        )
    output.append(
        {
            "id": "msg-answer",
            "type": "message",
            "role": "assistant",
            "content": [
                {
                    "type": "text",
                    "delta": False,
                    "text": "\n最终回答\n",
                },
            ],
            "object": "message",
        },
    )
    return {
        "id": "response-1",
        "output": output,
        "status": "completed",
        "object": "response",
    }


class NonStreamChatTest(unittest.TestCase):
    def tearDown(self) -> None:
        HTTP_API.ACTIVE_RUNS.clear()

    def test_request_defaults_to_streaming(self) -> None:
        payload = HTTP_API.QwenPawChatRequest(
            userId="user-1",
            sessionId="session-1",
            input=[],
        )
        self.assertIs(payload.stream, True)
        payload = HTTP_API.QwenPawChatRequest(
            userId="user-1",
            sessionId="session-1",
            stream=False,
            input=[],
        )
        self.assertIs(payload.stream, False)

    def test_reasoning_precedes_final_answer(self) -> None:
        events = HTTP_API._non_stream_agui_events(final_response())
        self.assertEqual(
            [event["type"] for event in events],
            ["REASONING_MESSAGE_CHUNK", "TEXT_MESSAGE_CHUNK"],
        )
        self.assertEqual(events[0]["messageId"], "msg-reasoning")
        self.assertEqual(events[0]["delta"], "分析用户问题")
        self.assertEqual(events[1]["messageId"], "msg-answer")
        self.assertEqual(events[1]["role"], "assistant")
        self.assertEqual(events[1]["delta"], "最终回答")

    def test_without_reasoning_returns_one_text_chunk(self) -> None:
        events = HTTP_API._non_stream_agui_events(
            final_response(with_reasoning=False),
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["type"], "TEXT_MESSAGE_CHUNK")

    def test_non_stream_events_are_encoded_as_sse_frames(self) -> None:
        events = HTTP_API._non_stream_agui_events(final_response())

        async def collect_frames():
            return [
                frame
                async for frame in HTTP_API._non_stream_sse(events)
            ]

        frames = asyncio.run(collect_frames())
        self.assertEqual(len(frames), 2)
        self.assertTrue(all(frame.startswith("data: ") for frame in frames))
        self.assertTrue(all(frame.endswith("\n\n") for frame in frames))
        decoded = [json.loads(frame[6:].strip()) for frame in frames]
        self.assertEqual(
            [event["type"] for event in decoded],
            ["REASONING_MESSAGE_CHUNK", "TEXT_MESSAGE_CHUNK"],
        )

    def test_non_stream_waits_for_final_response(self) -> None:
        async def no_visible_chat(*_args, **_kwargs):
            return None

        async def qwenpaw_stream(*_args, **_kwargs):
            # 先发普通流式片段，最后再发完整 response；非流式输出只能读取后者。
            yield {
                "object": "content",
                "type": "text",
                "delta": True,
                "text": "不应直接返回的流式碎片",
            }
            yield final_response()

        payload = HTTP_API.QwenPawChatRequest(
            userId="user-1",
            sessionId="session-1",
            stream=False,
            input=[
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "你是谁"}],
                },
            ],
        )
        channel = SimpleNamespace(
            agent_id="agent-1",
            agent_timeout_seconds=30,
        )

        async def run():
            with (
                patch.object(
                    HTTP_API,
                    "_ensure_visible_chat",
                    no_visible_chat,
                ),
                patch.object(
                    HTTP_API,
                    "_stream_agent_request",
                    qwenpaw_stream,
                ),
            ):
                return await HTTP_API._non_stream_agent(
                    payload,
                    channel,
                    SimpleNamespace(),
                )

        events = asyncio.run(run())
        self.assertEqual(len(events), 2)
        self.assertNotIn("不应直接返回", str(events))
        self.assertEqual(HTTP_API.ACTIVE_RUNS, {})


if __name__ == "__main__":
    unittest.main()
