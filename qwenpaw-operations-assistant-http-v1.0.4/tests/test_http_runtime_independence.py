"""HTTP 运行时不得依赖运维助手频道是否处于活动状态。"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


PACKAGE_ROOT = Path(__file__).parents[1]
PACKAGE_NAME = "operations_assistant_http_independence_test"
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


class FakeContext:
    """只提供 QwenPaw HTTP 上下文取得 Workspace 所需的最小接口。"""

    def __init__(self, workspace: object) -> None:
        self.agent_id = ""
        self.workspace = workspace

    async def _get_workspace(self) -> object:
        return self.workspace


class HttpRuntimeIndependenceTest(unittest.TestCase):
    def test_runtime_works_without_active_channel_instance(self) -> None:
        channels = SimpleNamespace(
            __pydantic_extra__={
                "operations-assistant": {
                    "callback_token": "must-not-be-read",
                    "agent_timeout_seconds": 1,
                    "max_upload_mb": 1,
                },
            },
        )
        workspace = SimpleNamespace(
            config=SimpleNamespace(name="Agent One", channels=channels),
        )
        context = FakeContext(workspace)

        runtime = asyncio.run(HTTP_API._get_runtime("agent-1", context))

        self.assertEqual(context.agent_id, "agent-1")
        self.assertEqual(runtime.agent_id, "agent-1")
        self.assertEqual(runtime.agent_name, "Agent One")
        self.assertFalse(hasattr(runtime, "callback_token"))
        self.assertEqual(runtime.agent_timeout_seconds, 1800)
        self.assertEqual(runtime.max_upload_mb, 1024)

    def test_runtime_uses_safe_defaults_without_channel_config(self) -> None:
        workspace = SimpleNamespace(
            config=SimpleNamespace(
                name="Agent Two",
                channels=SimpleNamespace(__pydantic_extra__={}),
            ),
        )

        runtime = asyncio.run(
            HTTP_API._get_runtime("agent-2", FakeContext(workspace)),
        )

        self.assertFalse(hasattr(runtime, "callback_token"))
        self.assertEqual(runtime.agent_timeout_seconds, 1800)
        self.assertEqual(runtime.max_upload_mb, 1024)


if __name__ == "__main__":
    unittest.main()

