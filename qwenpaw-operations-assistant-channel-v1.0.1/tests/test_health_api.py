"""频道健康路由必须在未注册时仍然可以访问。"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


PACKAGE_ROOT = Path(__file__).parents[1]
PACKAGE_NAME = "operations_assistant_channel_health_test"
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
HEALTH_API = importlib.import_module(f"{PACKAGE_NAME}.health_api")


class ChannelHealthApiTest(unittest.TestCase):
    def setUp(self) -> None:
        CHANNEL.ACTIVE_CHANNELS.clear()

    def tearDown(self) -> None:
        CHANNEL.ACTIVE_CHANNELS.clear()

    def test_inactive_agent_returns_unhealthy_body_instead_of_404(self) -> None:
        result = asyncio.run(HEALTH_API.health("agent-inactive"))

        self.assertEqual(result["status"], "unhealthy")
        self.assertEqual(result["detail"], "not registered")
        self.assertEqual(result["agentId"], "agent-inactive")

    def test_active_agent_returns_channel_health(self) -> None:
        expected = {
            "status": "healthy",
            "detail": "registered",
            "agentId": "agent-active",
        }

        async def health_check():
            return expected

        CHANNEL.ACTIVE_CHANNELS["agent-active"] = SimpleNamespace(
            health_check=health_check,
        )

        result = asyncio.run(HEALTH_API.health("agent-active"))

        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()

