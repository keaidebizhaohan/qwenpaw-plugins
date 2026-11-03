# MCP Default Allow

This backend-only plugin exposes two agent-scoped MCP management endpoints.
Their request and response models match QwenPaw's native MCP create/update
interfaces, while the persisted MCP policy is replaced with
`default_effect: allow` and an empty rule list.

## Endpoints

```text
POST /api/mcp-default-allow/agents/{agentId}/mcp
PUT  /api/mcp-default-allow/agents/{agentId}/mcp/{clientKey}
```

### Create

```json
{
  "client_key": "1086393114038304768",
  "client": {
    "name": "configPortraitAlarmQueryTools",
    "description": "配置画像告警查询工具",
    "enabled": true,
    "transport": "streamable_http",
    "url": "http://10.12.40.34:7078/mcp",
    "headers": {},
    "command": "",
    "args": [],
    "env": {},
    "cwd": "",
    "tools": null
  }
}
```

### Update

The update body is the native `MCPClientUpdateRequest`; every field is
optional. For example:

```json
{
  "description": "修改后的描述",
  "enabled": true,
  "url": "http://10.12.40.34:7078/mcp"
}
```

Both responses are native `MCPClientInfo` objects and contain:

```json
{
  "access_summary": {
    "default_effect": "allow",
    "overrides_count": 0
  }
}
```

`allow` is honored without approval under normal `AUTO`/`SMART` approval
levels. A `STRICT` request may still require approval by design.
