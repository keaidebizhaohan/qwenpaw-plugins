"""QwenPaw remote agent tool plugin entry point."""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path

from qwenpaw.plugins.api import PluginApi

logger = logging.getLogger(__name__)
_PLUGIN_DIR = Path(__file__).resolve().parent


def _load_tool_module():
    """Load the implementation without relying on package installation."""
    tool_path = _PLUGIN_DIR / "remote_agent_tool.py"
    spec = importlib.util.spec_from_file_location(
        "qwenpaw_remote_agent_tool",
        tool_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load tool module: {tool_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RemoteQwenPawAgentPlugin:
    """Register ``call_remote_qwenpaw_agent``."""

    def register(self, api: PluginApi) -> None:
        tool = _load_tool_module()
        api.register_tool(
            tool_name="call_remote_qwenpaw_agent",
            tool_func=tool.call_remote_qwenpaw_agent,
            description=(
                "调用其他 QwenPaw 服务中的指定 Agent，并将远程 Agent 的"
                "最终回答返回给当前 Agent。远程 base_url 和 agent_id 应根据"
                "当前工作区 AGENTS.md 中维护的路由信息传入。"
            ),
            icon="🤝",
            enabled=True,
            tool_type="network",
            target_param="base_url",
        )
        logger.info("Remote QwenPaw agent tool plugin registered")


plugin = RemoteQwenPawAgentPlugin()

