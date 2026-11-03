"""HTTP routes for allow-by-default MCP client management."""

from __future__ import annotations

import asyncio
from typing import Annotated, Any

from fastapi import APIRouter, Body, Path, Request

from qwenpaw.app.agent_context import get_agent_for_request
from qwenpaw.app.mcp.config_service import (
    MCPConfigService,
    ensure_mcp_display_name_unique,
    merge_update_with_existing,
    normalize_mcp_display_name,
    validate_client_key,
)
from qwenpaw.app.mcp.schemas import (
    MCPClientCreateRequest,
    MCPClientInfo,
    MCPClientUpdateRequest,
)
from qwenpaw.drivers.adapters.mcp_console import (
    build_mcp_credential_record,
    build_mcp_driver_card,
    mcp_credential_ref,
)
from qwenpaw.drivers.constants import POLICY_EFFECT_ALLOW, PROTOCOL_MCP
from qwenpaw.drivers.policy_types import DriverPolicy


def _allow_policy() -> DriverPolicy:
    """Return the policy applied by both plugin endpoints."""
    return DriverPolicy(default_effect=POLICY_EFFECT_ALLOW, rules=[])


async def create_client_default_allow(
    workspace: Any,
    client_key: str,
    client: MCPClientCreateRequest,
) -> MCPClientInfo:
    """Create an MCP client whose first persisted policy is already allow."""
    service = MCPConfigService(workspace)
    validate_client_key(client_key)
    path = service._driver_config.card_path(  # pylint: disable=protected-access
        client_key,
        protocol=PROTOCOL_MCP,
    )
    if await asyncio.to_thread(path.is_file):
        from fastapi import HTTPException

        raise HTTPException(
            400,
            detail=(
                f"MCP client '{client_key}' already exists. Use PUT to update."
            ),
        )

    await ensure_mcp_display_name_unique(
        service,
        normalize_mcp_display_name(client.name, fallback=client_key),
        client_key=client_key,
    )

    credential = build_mcp_credential_record(client_key, client)
    card = build_mcp_driver_card(
        client_key,
        client,
        mcp_credential_ref(client_key),
        credential_record=credential,
    )
    card.policy = _allow_policy()

    driver_config = service._driver_config  # pylint: disable=protected-access
    if credential.secrets:
        await driver_config.credential_store.put(credential)
    else:
        await driver_config.credential_store.delete(credential.ref)
    await driver_config.save_card(card)
    return await service.build_info_from_card(card)


async def update_client_default_allow(
    workspace: Any,
    client_key: str,
    updates: MCPClientUpdateRequest,
) -> MCPClientInfo:
    """Update an MCP client and replace its access policy with allow-all."""
    service = MCPConfigService(workspace)
    existing_card = await service.load_card(client_key)
    existing_info = await service.build_info_from_card(existing_card)
    merged_client = merge_update_with_existing(existing_info, updates)
    await ensure_mcp_display_name_unique(
        service,
        normalize_mcp_display_name(
            merged_client.name,
            fallback=client_key,
        ),
        client_key=client_key,
    )

    driver_config = service._driver_config  # pylint: disable=protected-access
    existing_credential = await driver_config.load_optional_credential(
        mcp_credential_ref(client_key),
    )
    credential = build_mcp_credential_record(
        client_key,
        merged_client,
        existing=existing_credential,
    )
    card = build_mcp_driver_card(
        client_key,
        merged_client,
        mcp_credential_ref(client_key),
        credential_record=credential,
        existing=existing_card,
    )
    card.policy = _allow_policy()

    if credential.secrets:
        await driver_config.credential_store.put(credential)
    else:
        await driver_config.credential_store.delete(credential.ref)
    await driver_config.save_card(card)
    return await service.build_info_from_card(card)


def build_router() -> APIRouter:
    """Build agent-scoped MCP routes with native request/response schemas."""
    router = APIRouter()

    @router.post(
        "/agents/{agentId}/mcp",
        response_model=MCPClientInfo,
        status_code=201,
        summary="Create an allow-by-default MCP client",
    )
    async def create_mcp_client(
        request: Request,
        agent_id: Annotated[str, Path(alias="agentId")],
        client_key: Annotated[str, Body(embed=True)],
        client: Annotated[MCPClientCreateRequest, Body(embed=True)],
    ) -> MCPClientInfo:
        workspace = await get_agent_for_request(request, agent_id=agent_id)
        return await create_client_default_allow(workspace, client_key, client)

    @router.put(
        "/agents/{agentId}/mcp/{client_key:path}",
        response_model=MCPClientInfo,
        summary="Update an MCP client and set its policy to allow",
    )
    async def update_mcp_client(
        request: Request,
        updates: Annotated[MCPClientUpdateRequest, Body()],
        agent_id: Annotated[str, Path(alias="agentId")],
        client_key: str,
    ) -> MCPClientInfo:
        workspace = await get_agent_for_request(request, agent_id=agent_id)
        return await update_client_default_allow(
            workspace,
            client_key,
            updates,
        )

    return router
