# -*- coding: utf-8 -*-
"""Standalone LightRAG Mix query tool plugin."""

import importlib.util
from pathlib import Path

from qwenpaw.plugins.api import PluginApi

_DIR = Path(__file__).resolve().parent


def _client():
    spec = importlib.util.spec_from_file_location(
        "lightrag_mix_query_client",
        _DIR / "lightrag_client.py",
    )
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load LightRAG client")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def lightrag_mix_query(
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
    """原始 Chunk、知识图谱实体与关系三路混合检索。

    Leave workspaces empty to discover available knowledge bases, then select
    one or more based on the user's question and call again.
    Retrieval limits have useful defaults and may be replaced dynamically
    by the Agent for a specific question.
    """
    return await _client().query_lightrag(
        tool_name="lightrag_mix_query",
        mode="mix",
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


class LightRAGMixPlugin:
    def register(self, api: PluginApi) -> None:
        api.register_tool(
            tool_name="lightrag_mix_query",
            tool_func=lightrag_mix_query,
            description=(
                "原始 Chunk 向量 + 知识图谱实体 + 关系三路混合检索"
                "（Naive + Local + Global）。覆盖最全面，适合复杂综合问题、"
                "多证据回答或模式不明确时优先使用。"
            ),
            icon="🧠",
            tool_type="network",
        )


plugin = LightRAGMixPlugin()
