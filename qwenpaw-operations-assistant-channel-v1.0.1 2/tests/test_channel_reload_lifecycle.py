"""运维助手频道在 Agent 重载期间的注册生命周期测试。"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


PACKAGE_ROOT = Path(__file__).parents[1]
PACKAGE_NAME = "operations_assistant_lifecycle_test"
SPEC = importlib.util.spec_from_file_location(
    PACKAGE_NAME,
    PACKAGE_ROOT / "__init__.py",
    submodule_search_locations=[str(PACKAGE_ROOT)],
)
assert SPEC is not None and SPEC.loader is not None
PACKAGE = importlib.util.module_from_spec(SPEC)
sys.modules[PACKAGE_NAME] = PACKAGE
SPEC.loader.exec_module(PACKAGE)
CHANNEL = importlib.import_module(f"{PACKAGE_NAME}.channel")


class ChannelReloadLifecycleTest(unittest.TestCase):
    def setUp(self):
        CHANNEL.ACTIVE_CHANNELS.clear()
        self.temporary_directory = tempfile.TemporaryDirectory()

    def tearDown(self):
        CHANNEL.ACTIVE_CHANNELS.clear()
        self.temporary_directory.cleanup()

    def make_channel(self):
        channel = CHANNEL.ServiceCallbackChannel(
            lambda *_args, **_kwargs: None,
            enabled=True,
            registration_service_url="http://registry.example",
            qwenpaw_public_base_url="http://qwenpaw.example",
        )
        channel.agent_id = "agent-1"
        channel.agent_name = "Agent 1"
        channel._registered = True
        channel._workspace = SimpleNamespace(
            workspace_dir=Path(self.temporary_directory.name),
        )
        CHANNEL.ACTIVE_CHANNELS[channel.agent_id] = channel
        return channel

    def test_reload_does_not_deregister_external_service(self):
        old_channel = self.make_channel()
        old_channel._write_registration_lease()

        # 模拟新 Workspace 注册成功并覆盖共享租约，但旧模块仍认为旧实例
        # 是 ACTIVE_CHANNELS 当前值——这正是曾导致“先更新、后删除”的场景。
        new_channel = self.make_channel()
        new_channel._write_registration_lease()
        CHANNEL.ACTIVE_CHANNELS["agent-1"] = old_channel

        with patch.object(CHANNEL, "_request_json") as request_json:
            asyncio.run(old_channel.stop())

        request_json.assert_not_called()
        self.assertNotIn("agent-1", CHANNEL.ACTIVE_CHANNELS)
        self.assertFalse(old_channel._registered)
        self.assertTrue(new_channel._registration_lease_path().is_file())

    def test_explicit_disable_still_deregisters(self):
        channel = self.make_channel()
        channel._write_registration_lease()

        with patch.object(
                CHANNEL,
                "_request_json",
                return_value=(200, {"ok": True}),
            ) as request_json:
            asyncio.run(channel.stop())

        request_json.assert_called_once()
        method, url, payload, _token, _timeout = request_json.call_args.args
        self.assertEqual(method, "POST")
        self.assertEqual(url, "http://registry.example/channel/deregister")
        self.assertEqual(payload["agentId"], "agent-1")
        self.assertNotIn("agent-1", CHANNEL.ACTIVE_CHANNELS)
        self.assertFalse(channel._registration_lease_path().exists())

    def test_snake_case_created_on_overrides_legacy_camel_case_value(self):
        channel = CHANNEL.ServiceCallbackChannel.from_config(
            lambda *_args, **_kwargs: None,
            {
                "enabled": True,
                "registration_service_url": "http://registry.example",
                "qwenpaw_public_base_url": "http://qwenpaw.example",
                "created_on": "GALAXY_ADP",
                "createdOn": "QwenPaw",
            },
        )

        self.assertEqual(channel.created_on, "GALAXY_ADP")

    def test_public_urls_use_separate_http_prefixes(self):
        channel = self.make_channel()
        channel._build_public_urls()

        self.assertEqual(
            channel.callback_url,
            "http://qwenpaw.example/api/service-channel/chat?agentId=agent-1",
        )
        self.assertEqual(
            channel.stop_url,
            "http://qwenpaw.example/api/service-channel/chat/stop"
            "?agentId=agent-1",
        )
        self.assertEqual(
            channel.file_upload_url,
            "http://qwenpaw.example/api/service-channel/files?agentId=agent-1",
        )
        self.assertEqual(
            channel.health_url,
            "http://qwenpaw.example/api/operations-assistant-channel/health"
            "?agentId=agent-1",
        )

        payload = channel._registration_payload()
        self.assertEqual(payload["stopWebhook"], channel.stop_url)

    def test_changing_registration_service_does_not_delete_new_registration(self):
        old_channel = self.make_channel()
        old_channel._write_registration_lease()

        new_channel = self.make_channel()
        new_channel.registration_service_url = "http://new-registry.example"
        new_channel._write_registration_lease()
        CHANNEL.ACTIVE_CHANNELS["agent-1"] = old_channel

        with patch.object(CHANNEL, "_request_json") as request_json:
            asyncio.run(old_channel.stop())

        # 新旧 URL 可能是同一注册服务的不同入口；新实例已接管后，
        # 旧实例不能再调用 deregister 删除同一 Agent 的新记录。
        request_json.assert_not_called()
        self.assertTrue(new_channel._registration_lease_path().is_file())


if __name__ == "__main__":
    unittest.main()
