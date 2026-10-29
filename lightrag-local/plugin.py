# -*- coding: utf-8 -*-
"""Standalone LightRAG Local query tool plugin."""

import importlib.util
from pathlib import Path

from qwenpaw.plugins.api import PluginApi

_DIR = Path(__file__).resolve().parent


def _client():
    spec = importlib.util.spec_from_file_location(
        "lightrag_local_query_client",
        _DIR / "lightrag_client.py",
    )
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load LightRAG client")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def lightrag_local_query(
    query: str,
    workspaces: list[str] | None = None,
    top_k: int | None = 40,
    chunk_top_k: int | None = 20,
    max_entity_tokens: int | None = 6000,
    max_relation_tokens: int | None = 8000,
    max_total_tokens: int | None = 30000,
    response_type: str | None = None,
    include_references: bool = True,
    include_chunk_content: bool = False,
    enable_rerank: bool = True,
):
    """知识图谱实体优先检索，聚焦实体及其邻近关系。

    Leave workspaces empty to discover available knowledge bases, then select
    one or more based on the user's question and call again.
    Retrieval limits have useful defaults and may be replaced dynamically
    by the Agent for a specific question.
    """
    return await _client().query_lightrag(
        tool_name="lightrag_local_query",
        mode="local",
        query=query,
        workspaces=workspaces,
        top_k=top_k,
        chunk_top_k=chunk_top_k,
        max_entity_tokens=max_entity_tokens,
        max_relation_tokens=max_relation_tokens,
        max_total_tokens=max_total_tokens,
        response_type=response_type,
        include_references=include_references,
        include_chunk_content=include_chunk_content,
        enable_rerank=enable_rerank,
    )


class LightRAGLocalPlugin:
    def register(self, api: PluginApi) -> None:
        api.register_tool(
            tool_name="lightrag_local_query",
            tool_func=lightrag_local_query,
            description=(
                "知识图谱实体优先检索，重点聚焦具体实体及其邻近关系。"
                "适合人物、组织、产品、术语、概念属性和局部事实；"
                "问题围绕明确名称或对象时优先使用。"
            ),
            icon="🔎",
            tool_type="network",
        )


plugin = LightRAGLocalPlugin()
