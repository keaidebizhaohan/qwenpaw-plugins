"""QwenPaw Gateway agent tool plugin entry point."""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path

from qwenpaw.plugins.api import PluginApi

logger = logging.getLogger(__name__)
_PLUGIN_DIR = Path(__file__).resolve().parent


def _load_tool_module():
    tool_path = _PLUGIN_DIR / "gateway_agent_tool.py"
    spec = importlib.util.spec_from_file_location("qwenpaw_gateway_agent_tool", tool_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load tool module: {tool_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class K8sAgentPlugin:
    """Register ``call_k8s_agent``."""

    def register(self, api: PluginApi) -> None:
        tool = _load_tool_module()
        api.register_tool(
            tool_name="call_k8s_agent",
            tool_func=tool.call_k8s_agent,
            description=(
                "调用经 JiuwenSwarm Gateway 发布到 Kubernetes 的远程 Agent。Gateway 地址、"
                "Bot/Agent 资源 ID 和用途应按工作区 AGENTS.md 中的路由信息传入。"
            ),
            icon="🌉",
            enabled=True,
            tool_type="network",
            target_param="gateway_base_url",
        )
        logger.info("K8s agent tool plugin registered")


plugin = K8sAgentPlugin()
