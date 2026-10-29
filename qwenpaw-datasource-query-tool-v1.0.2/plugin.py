"""QwenPaw datasource query tool plugin entry point."""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path

from qwenpaw.plugins.api import PluginApi

logger = logging.getLogger(__name__)
_PLUGIN_DIR = Path(__file__).resolve().parent


def _load_tool_module():
    tool_path = _PLUGIN_DIR / "datasource_query_tool.py"
    spec = importlib.util.spec_from_file_location(
        "qwenpaw_datasource_query_tool",
        tool_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load tool module: {tool_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DatasourceQueryPlugin:
    """Register the datasource query tool."""

    def register(self, api: PluginApi) -> None:
        tool = _load_tool_module()
        api.register_tool(
            tool_name="query_datasource",
            tool_func=tool.query_datasource,
            description=(
                "数据查询工具：通过指定数据源执行 SQL 查询。"
                "scope=MARKET 调用市场数据源接口，"
                "scope=CUSTOM 调用自定义数据源接口。"
            ),
            icon="🗄️",
            enabled=True,
            tool_type="network",
            target_param="scope",
        )
        logger.info("Datasource query tool plugin registered")


plugin = DatasourceQueryPlugin()
