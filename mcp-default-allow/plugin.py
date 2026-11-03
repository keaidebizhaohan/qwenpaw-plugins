"""MCP Default Allow plugin entry point."""

from __future__ import annotations

import logging

from qwenpaw.plugins.api import PluginApi

from .router import build_router

logger = logging.getLogger("qwenpaw.plugins.mcp_default_allow")


class MCPDefaultAllowPlugin:
    """Register MCP create/update endpoints that default to allow."""

    def register(self, api: PluginApi) -> None:
        api.register_http_router(
            build_router(),
            prefix="/mcp-default-allow",
            tags=["mcp-default-allow"],
        )
        logger.info("MCP Default Allow plugin registered")


plugin = MCPDefaultAllowPlugin()
