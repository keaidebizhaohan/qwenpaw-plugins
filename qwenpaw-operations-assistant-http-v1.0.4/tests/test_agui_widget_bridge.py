"""运维助手 AG-UI 桥接层的 Widget 事件测试。"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


PACKAGE_ROOT = Path(__file__).parents[1]
PACKAGE_NAME = "operations_assistant_widget_test"
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


def make_bridge():
    display = SimpleNamespace(
        show_thinking=True,
        show_tool_calls=True,
        show_tool_results=True,
        tool_call_max_length=200,
        tool_result_max_length=500,
    )
    channel = SimpleNamespace(_display_config=display)
    return HTTP_API._AgUiStreamBridge("run-test", {"source": "test"}, channel)


class AgUiWidgetBridgeTest(unittest.TestCase):
    def test_embedded_tool_output_becomes_agui_tool_call_result(self):
        bridge = make_bridge()
        events = bridge.convert(
            {
                "id": "msg-tool-result-1",
                "type": "plugin_call_output",
                "role": "tool",
                "content": [
                    {
                        "type": "data",
                        "delta": False,
                        "status": "completed",
                        "object": "content",
                        # 真实 QwenPaw 数据中这里可能就是 null。
                        "msg_id": None,
                        "data": {
                            "call_id": "call-1",
                            "name": "get_current_time",
                            "output": "2026-10-29 16:03:34 Asia/Shanghai",
                            "state": "success",
                        },
                    },
                ],
                "status": "completed",
                "object": "message",
            },
        )

        self.assertEqual(len(events), 1)
        result = events[0]
        self.assertEqual(result["type"], "TOOL_CALL_RESULT")
        self.assertEqual(result["toolCallId"], "call-1")
        self.assertEqual(result["messageId"], "msg-tool-result-1")
        self.assertEqual(
            result["content"],
            "2026-10-29 16:03:34 Asia/Shanghai",
        )
        self.assertEqual(result["role"], "tool")
        self.assertEqual(result["metadata"]["toolState"], "success")
        self.assertIs(result["metadata"]["success"], True)

    def test_false_tool_state_still_emits_failure_result(self):
        bridge = make_bridge()
        events = bridge.convert(
            {
                "id": "msg-tool-result-failed",
                "type": "plugin_call_output",
                "content": [
                    {
                        "type": "data",
                        "delta": False,
                        "status": "failed",
                        "data": {
                            "call_id": "call-failed",
                            "output": "远程工具执行失败",
                            "state": False,
                        },
                    },
                ],
                "status": "failed",
                "object": "message",
            },
        )

        self.assertEqual(len(events), 1)
        result = events[0]
        self.assertEqual(result["content"], "远程工具执行失败")
        self.assertIs(result["metadata"]["toolState"], False)
        self.assertIs(result["metadata"]["success"], False)

    def test_failed_tool_without_output_uses_error_message(self):
        bridge = make_bridge()
        events = bridge.convert(
            {
                "id": "msg-tool-error",
                "type": "plugin_call_output",
                "content": [
                    {
                        "type": "data",
                        "delta": False,
                        "status": "completed",
                        "data": {
                            "call_id": "call-error",
                            "message": "连接远程服务失败",
                            "state": "error",
                        },
                    },
                ],
                "status": "completed",
                "object": "message",
            },
        )

        self.assertEqual(events[0]["content"], "连接远程服务失败")
        self.assertIs(events[0]["metadata"]["success"], False)

    def test_custom_widget_event_and_clean_answer(self):
        bridge = make_bridge()
        events = []
        for chunk in (
            "回答前<wi",
            'dget>{"component":"faultList","list":[',
            '{"level":"CRITICAL","count":1}]}</wid',
            "get>回答后",
        ):
            events.extend(bridge.convert(chunk))
        events.extend(bridge.finish())

        self.assertEqual(
            [event["type"] for event in events],
            [
                "TEXT_MESSAGE_START",
                "TEXT_MESSAGE_CONTENT",
                "TEXT_MESSAGE_END",
                "CUSTOM",
                "TEXT_MESSAGE_START",
                "TEXT_MESSAGE_CONTENT",
                "TEXT_MESSAGE_END",
            ],
        )
        widget = next(event for event in events if event["type"] == "CUSTOM")
        self.assertEqual(widget["name"], "WIDGET")
        self.assertEqual(
            widget["value"],
            {
                "component": "faultList",
                "list": [{"level": "CRITICAL", "count": 1}],
            },
        )
        self.assertEqual("".join(bridge.answer_parts), "回答前回答后")

    def test_widget_only_has_no_empty_text_message(self):
        bridge = make_bridge()
        events = bridge.convert(
            '<widget>{"component":"faultList","list":[]}</widget>',
        )
        events.extend(bridge.finish())
        self.assertEqual([event["type"] for event in events], ["CUSTOM"])
        self.assertEqual(bridge.answer_parts, [])

    def test_newline_chunk_before_widget_does_not_create_blank_message(self):
        bridge = make_bridge()
        events = bridge.convert("\n\n")
        events.extend(
            bridge.convert(
                '<widget>{"component":"systemMap"}</widget>',
            ),
        )
        events.extend(bridge.finish())
        self.assertEqual([event["type"] for event in events], ["CUSTOM"])
        self.assertEqual(bridge.answer_parts, [])

    def test_normal_answer_drops_only_final_newlines(self):
        bridge = make_bridge()
        events = bridge.convert("第一行\n第二行")
        events.extend(bridge.convert("\n\n"))
        events.extend(bridge.finish())
        deltas = [
            event["delta"]
            for event in events
            if event["type"] == "TEXT_MESSAGE_CONTENT"
        ]
        self.assertEqual("".join(deltas), "第一行\n第二行")
        self.assertEqual("".join(bridge.answer_parts), "第一行\n第二行")

    def test_agent_request_always_disables_approval(self):
        class FakeWorkspace:
            def __init__(self):
                self.request = None

            async def stream_query(self, request):
                self.request = request
                if False:
                    yield None

        workspace = FakeWorkspace()

        class FakeContext:
            async def _get_workspace(self):
                return workspace

        payload = HTTP_API.QwenPawChatRequest(
            userId="user-1",
            sessionId="session-1",
            input=[],
        )
        channel = SimpleNamespace(
            channel="operations-assistant",
            agent_id="agent-1",
        )

        async def run():
            return [
                event
                async for event in HTTP_API._stream_agent_request(
                    payload,
                    channel,
                    FakeContext(),
                    "查询 CPU",
                    [],
                )
            ]

        self.assertEqual(asyncio.run(run()), [])
        self.assertIsNotNone(workspace.request)
        self.assertEqual(
            workspace.request.request_context,
            {"approval_level": "off"},
        )
        self.assertEqual(workspace.request.channel, "operations-assistant")


if __name__ == "__main__":
    unittest.main()
