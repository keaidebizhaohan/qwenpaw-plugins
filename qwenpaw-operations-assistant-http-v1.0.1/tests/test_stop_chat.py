"""运维助手聊天停止接口测试。"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


PACKAGE_ROOT = Path(__file__).parents[1]
PACKAGE_NAME = "operations_assistant_stop_test"
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


class StopChatTest(unittest.TestCase):
    def tearDown(self) -> None:
        HTTP_API.ACTIVE_RUNS.clear()

    def test_stop_cancels_all_runs_for_agent_and_session(self) -> None:
        async def scenario():
            async def wait_forever():
                await asyncio.Event().wait()

            matching_one = asyncio.create_task(wait_forever())
            matching_two = asyncio.create_task(wait_forever())
            other_session = asyncio.create_task(wait_forever())
            HTTP_API.ACTIVE_RUNS.update(
                {
                    ("agent-1", "session-1", "run-1"): matching_one,
                    ("agent-1", "session-1", "run-2"): matching_two,
                    ("agent-1", "session-2", "run-3"): other_session,
                },
            )

            result = await HTTP_API.stop_chat(
                HTTP_API.StopChatRequest(sessionId="session-1"),
                "agent-1",
            )
            await asyncio.sleep(0)

            self.assertTrue(result["stopped"])
            self.assertEqual(result["cancelledRuns"], 2)
            self.assertTrue(matching_one.cancelled())
            self.assertTrue(matching_two.cancelled())
            self.assertFalse(other_session.done())
            other_session.cancel()
            await asyncio.gather(other_session, return_exceptions=True)

        asyncio.run(scenario())

    def test_stop_is_idempotent_when_no_run_exists(self) -> None:
        result = asyncio.run(
            HTTP_API.stop_chat(
                HTTP_API.StopChatRequest(sessionId="missing"),
                "agent-1",
            ),
        )

        self.assertFalse(result["stopped"])
        self.assertEqual(result["cancelledRuns"], 0)
        self.assertEqual(result["message"], "No active run found")

    def test_original_sse_finishes_with_cancelled_outcome(self) -> None:
        async def scenario():
            started = asyncio.Event()
            blocker = asyncio.Event()
            frames: list[str] = []

            async def no_visible_chat(*_args, **_kwargs):
                return None

            async def blocked_agent_stream(*_args, **_kwargs):
                await blocker.wait()
                if False:  # pragma: no cover - 保持为异步生成器
                    yield None

            payload = HTTP_API.QwenPawChatRequest(
                userId="user-1",
                sessionId="session-cancel",
                input=[
                    {
                        "role": "user",
                        "type": "message",
                        "content": [{"type": "text", "text": "等待"}],
                    },
                ],
            )
            channel = SimpleNamespace(
                channel="operations-assistant",
                agent_id="agent-1",
                agent_name="Agent One",
                agent_timeout_seconds=1800,
                _display_config=SimpleNamespace(
                    show_thinking=True,
                    show_tool_calls=True,
                    show_tool_results=True,
                    tool_call_max_length=200,
                    tool_result_max_length=500,
                ),
            )

            async def collect_sse():
                async for frame in HTTP_API._stream_agent(
                    payload,
                    channel,
                    SimpleNamespace(),
                ):
                    frames.append(frame)
                    if '"type":"RUN_STARTED"' in frame:
                        started.set()

            with (
                patch.object(
                    HTTP_API,
                    "_ensure_visible_chat",
                    no_visible_chat,
                ),
                patch.object(
                    HTTP_API,
                    "_stream_agent_request",
                    blocked_agent_stream,
                ),
            ):
                collector = asyncio.create_task(collect_sse())
                await started.wait()
                result = await HTTP_API.stop_chat(
                    HTTP_API.StopChatRequest(sessionId="session-cancel"),
                    "agent-1",
                )
                await collector

            self.assertTrue(result["stopped"])
            self.assertTrue(
                any(
                    '"type":"RUN_FINISHED"' in frame
                    and '"type":"cancelled"' in frame
                    for frame in frames
                ),
            )
            self.assertEqual(HTTP_API.ACTIVE_RUNS, {})

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()

