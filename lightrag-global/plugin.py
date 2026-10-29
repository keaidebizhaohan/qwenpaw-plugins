# -*- coding: utf-8 -*-
"""Standalone LightRAG Global query tool plugin."""

import importlib.util
from pathlib import Path

from qwenpaw.plugins.api import PluginApi

_DIR = Path(__file__).resolve().parent


def _client():
    spec = importlib.util.spec_from_file_location(
        "lightrag_global_query_client",
        _DIR / "lightrag_client.py",
    )
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load LightRAG client")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def lightrag_global_query(
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
    """知识图谱关系优先检索，强调全局关系和综合推理。

    Leave workspaces empty to discover available knowledge bases, then select
    one or more based on the user's question and call again.
    Retrieval limits have useful defaults and may be replaced dynamically
    by the Agent for a specific question.
    """
    return await _client().query_lightrag(
        tool_name="lightrag_global_query",
        mode="global",
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


class LightRAGGlobalPlugin:
    def register(self, api: PluginApi) -> None:
        api.register_tool(
            tool_name="lightrag_global_query",
            tool_func=lightrag_global_query,
            description=(
                "知识图谱关系优先检索，强调跨实体关系、全局主题与综合推理。"
                "适合趋势、关联、整体结构、跨文档总结，以及“为什么”或"
                "“有什么联系”等全局问题。"
            ),
            icon="🌐",
            tool_type="network",
        )


plugin = LightRAGGlobalPlugin()
