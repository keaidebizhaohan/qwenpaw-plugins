"""运维助手 AG-UI 桥接层的 Widget 事件测试。"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import json
import sys
import tempfile
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


def make_bridge(project_dir="/app/working"):
    display = SimpleNamespace(
        show_thinking=True,
        show_tool_calls=True,
        show_tool_results=True,
        tool_call_max_length=200,
        tool_result_max_length=500,
    )
    channel = SimpleNamespace(
        _display_config=display,
        project_dir=project_dir,
    )
    return HTTP_API._AgUiStreamBridge(
        "run-test",
        {"source": "test", "agentId": "default"},
        channel,
    )


def send_file_response(success: bool = True):
    output = (
        json.dumps(
            [
                {
                    "type": "data",
                    "source": {
                        "type": "url",
                        "url": "file:///app/working/workspaces/default/%E5%92%8F%E6%A2%85.txt",
                        "media_type": "text/plain",
                    },
                },
                {"type": "text", "text": "File sent successfully."},
            ],
            ensure_ascii=False,
        )
        if success
        else "Error: file does not exist"
    )
    return {
        "object": "response",
        "output": [
            {
                "id": "msg-file-call",
                "type": "plugin_call",
                "status": "completed",
                "object": "message",
                "content": [
                    {
                        "type": "data",
                        "delta": False,
                        "status": "completed",
                        "data": {
                            "call_id": "call-file-1",
                            "name": "send_file_to_user",
                            "arguments": json.dumps(
                                {
                                    "file_path": "/app/working/workspaces/default/咏梅.txt",
                                },
                                ensure_ascii=False,
                            ),
                        },
                    },
                ],
            },
            {
                "id": "msg-file-result",
                "type": "plugin_call_output",
                "status": "completed",
                "object": "message",
                "content": [
                    {
                        "type": "data",
                        "delta": False,
                        "status": "completed",
                        "data": {
                            "call_id": "call-file-1",
                            "name": "send_file_to_user",
                            "output": output,
                            "state": "success",
                        },
                    },
                ],
            },
            {
                "id": "msg-answer",
                "type": "message",
                "role": "assistant",
                "status": "completed",
                "object": "message",
                "content": [
                    {
                        "type": "text",
                        "delta": False,
                        "text": "文件已经生成。",
                    },
                ],
            },
        ],
    }


def write_file_response(
    argument_path="/app/working/hello.txt",
    actual_path="/app/working/hello.txt",
):
    return {
        "object": "response",
        "output": [
            {
                "id": "msg-write-call",
                "type": "plugin_call",
                "status": "completed",
                "object": "message",
                "content": [
                    {
                        "type": "data",
                        "delta": False,
                        "status": "completed",
                        "data": {
                            "call_id": "call-write-1",
                            "name": "write_file",
                            "arguments": json.dumps(
                                {
                                    "file_path": argument_path,
                                    "content": "hello",
                                },
                            ),
                        },
                    },
                ],
            },
            {
                "id": "msg-write-result",
                "type": "plugin_call_output",
                "status": "completed",
                "object": "message",
                "content": [
                    {
                        "type": "data",
                        "delta": False,
                        "status": "completed",
                        "data": {
                            "call_id": "call-write-1",
                            "name": "write_file",
                            "output": f"Wrote 5 bytes to {actual_path}.",
                            "state": "success",
                        },
                    },
                ],
            },
            {
                "id": "msg-answer",
                "type": "message",
                "role": "assistant",
                "status": "completed",
                "object": "message",
                "content": [
                    {
                        "type": "text",
                        "delta": False,
                        "text": "文件已经创建。",
                    },
                ],
            },
        ],
    }


class AgUiWidgetBridgeTest(unittest.TestCase):
    def test_chat_file_blocks_use_paths_only(self):
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "file", "file_url": "/app/working/a.txt"},
                    {"type": "file", "file_url": "/app/working/a.txt"},
                    {"type": "file", "fileId": "legacy-id-is-ignored"},
                    {"type": "file", "path": "/app/working/legacy-path-is-ignored"},
                    {"type": "file", "file_url": "/app/working/b.txt"},
                ],
            },
        ]
        self.assertEqual(
            HTTP_API._extract_file_paths(messages),
            ["/app/working/a.txt", "/app/working/b.txt"],
        )

    def test_write_file_emits_same_custom_file_event_as_console(self):
        events = make_bridge().convert(write_file_response())
        file_events = [
            event
            for event in events
            if event.get("type") == "CUSTOM"
            and event.get("name") == "FILE"
        ]
        self.assertEqual(len(file_events), 1)
        self.assertEqual(file_events[0]["value"]["name"], "hello.txt")
        self.assertEqual(
            file_events[0]["value"]["path"],
            "/app/working/hello.txt",
        )

    def test_non_stream_write_file_event_precedes_answer(self):
        events = HTTP_API._non_stream_agui_events(
            write_file_response(),
            "/app/working",
        )
        self.assertEqual(
            [event["type"] for event in events],
            ["CUSTOM", "TEXT_MESSAGE_CHUNK"],
        )
        self.assertEqual(events[0]["value"]["name"], "hello.txt")

    def test_actual_write_result_path_overrides_inferred_relative_path(self):
        response = write_file_response(
            argument_path="hello.txt",
            actual_path="/app/working/hello.txt",
        )

        stream_events = make_bridge(
            "/app/working/workspaces/default",
        ).convert(response)
        stream_file = next(
            event
            for event in stream_events
            if event.get("type") == "CUSTOM"
            and event.get("name") == "FILE"
        )
        self.assertEqual(stream_file["value"]["path"], "/app/working/hello.txt")

        non_stream_events = HTTP_API._non_stream_agui_events(
            response,
            "/app/working/workspaces/default",
        )
        self.assertEqual(
            non_stream_events[0]["value"]["path"],
            "/app/working/hello.txt",
        )

    def test_final_response_emits_custom_file_event(self):
        events = make_bridge().convert(send_file_response())
        file_event = next(
            event
            for event in events
            if event["type"] == "CUSTOM" and event["name"] == "FILE"
        )
        self.assertEqual(file_event["value"]["name"], "咏梅.txt")
        self.assertEqual(
            file_event["value"]["path"],
            "/app/working/workspaces/default/咏梅.txt",
        )

    def test_non_stream_response_includes_file_before_answer(self):
        events = HTTP_API._non_stream_agui_events(
            send_file_response(),
        )
        self.assertEqual(
            [event["type"] for event in events],
            ["CUSTOM", "TEXT_MESSAGE_CHUNK"],
        )
        self.assertEqual(events[0]["name"], "FILE")
        self.assertEqual(events[0]["value"]["name"], "咏梅.txt")

    def test_failed_send_file_does_not_emit_file_event(self):
        events = make_bridge().convert(send_file_response(success=False))
        self.assertFalse(
            any(
                event.get("type") == "CUSTOM"
                and event.get("name") == "FILE"
                for event in events
            ),
        )

    def test_download_path_must_remain_inside_agent_workspace(self):
        with tempfile.TemporaryDirectory() as workspace_dir:
            workspace = Path(workspace_dir)
            generated = workspace / "咏梅.txt"
            generated.write_text("咏梅", encoding="utf-8")
            channel = SimpleNamespace(
                workspace=SimpleNamespace(workspace_dir=workspace),
                project_dir=workspace,
            )
            self.assertEqual(
                HTTP_API._resolve_agent_download_file(
                    channel,
                    str(generated),
                ),
                generated.resolve(),
            )
            self.assertEqual(
                HTTP_API._resolve_agent_download_file(
                    channel,
                    "咏梅.txt",
                ),
                generated.resolve(),
            )
            with self.assertRaises(HTTP_API.HTTPException) as caught:
                HTTP_API._resolve_agent_download_file(channel, "/etc/passwd")
            self.assertEqual(caught.exception.status_code, 403)

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

    def test_agent_request_uses_configured_approval_policy(self):
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
        self.assertIsNone(getattr(workspace.request, "request_context", None))
        self.assertEqual(workspace.request.channel, "operations-assistant")

    def test_approval_uses_custom_event_without_widget_tag(self):
        bridge = make_bridge()
        pending = SimpleNamespace(
            request_id="approval-1",
            session_id="session-1",
            root_session_id="session-1",
            owner_agent_id="agent-1",
            agent_id="agent-1",
            tool_name="LightragGlobalQuery",
            status="pending",
            severity="HIGH",
            findings_count=1,
            result_summary="即将执行全局知识库查询",
            created_at=100.0,
            timeout_seconds=300.0,
            extra={
                "tool_call": {"input": {"query": "查询 CPU"}},
                "display": {
                    "tool_name": "LightRAG 全局查询",
                    "tool_source": "policy",
                    "exact_target": "query=查询 CPU",
                    "similar_target": "query=*",
                    "is_generalized": True,
                },
                "reasoning": "需要查询知识库",
                "source_type": "tool_guard",
            },
        )
        known = set()

        events = HTTP_API._approval_custom_events(
            bridge,
            [pending],
            known,
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["type"], "CUSTOM")
        self.assertEqual(events[0]["name"], "APPROVAL")
        self.assertEqual(events[0]["value"]["request_id"], "approval-1")
        self.assertEqual(events[0]["value"]["root_session_id"], "session-1")
        self.assertNotIn("session_id", events[0]["value"])
        self.assertNotIn("component", events[0]["value"])
        self.assertNotIn("event", events[0]["value"])
        self.assertNotIn("status", events[0]["value"])
        self.assertEqual(
            events[0]["value"]["tool_name"],
            "LightragGlobalQuery",
        )
        self.assertEqual(
            events[0]["value"]["tool_params"],
            {"query": "查询 CPU"},
        )
        self.assertEqual(events[0]["value"]["tool_source"], "policy")
        self.assertTrue(events[0]["value"]["is_generalized"])

        # 同一个 pending 在后续轮询中不会重复发送。
        self.assertEqual(
            HTTP_API._approval_custom_events(bridge, [pending], known),
            [],
        )
        # 审批消失后不发送 resolved 事件，原聊天流直接继续。
        self.assertEqual(
            HTTP_API._approval_custom_events(bridge, [], known),
            [],
        )


if __name__ == "__main__":
    unittest.main()
