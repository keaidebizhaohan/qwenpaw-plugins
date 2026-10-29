"""运维助手频道对外健康检查路由。

路由由插件注册，不依赖某个 Agent 的频道实例是否已经启动。因此查询一个
未启用或尚未注册的 Agent 时仍返回 HTTP 200，并在响应体中表达真实状态。
完整地址为 ``/api/operations-assistant-channel/health?agentId=...``。
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from .channel import ACTIVE_CHANNELS, SERVICE_CODE, SERVICE_NAME


router = APIRouter()


@router.get("/health")
async def health(
    agent_id: str = Query(alias="agentId", min_length=1),
) -> dict[str, Any]:
    """返回频道注册状态；无活动频道时也返回 200，而不是抛出 404。"""
    channel = ACTIVE_CHANNELS.get(agent_id)
    if channel is None:
        return {
            "channel": "operations-assistant",
            "status": "unhealthy",
            "detail": "not registered",
            "serviceCode": SERVICE_CODE,
            "serviceName": SERVICE_NAME,
            "agentId": agent_id,
            "agentName": "",
            "chatWebhook": "",
            "fileUploadWebhook": "",
            "healthUrl": "",
        }
    return await channel.health_check()

