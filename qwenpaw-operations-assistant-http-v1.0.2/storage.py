"""HTTP 插件独立维护的上传文件注册表。

该模块不依赖运维助手频道实例，因此频道启停和 Agent 频道热重载不会影响
已经注册的 HTTP 路由或文件引用解析。
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any


FILE_REGISTRY: dict[str, dict[str, Any]] = {}


class FileResolveError(Exception):
    """fileId 解析失败；状态码可直接映射为 HTTP 响应。"""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def sanitize_filename(filename: Any) -> str:
    """清理上传文件名，阻止路径穿越并限制名称长度。"""
    name = str(filename or "").strip().replace("\\", "/")
    name = name.rsplit("/", 1)[-1].strip()
    name = "".join(
        char for char in name if char.isprintable() and char != "\x00"
    ).strip()
    if name in {"", ".", ".."}:
        return ""
    max_length = 200
    if len(name) > max_length:
        stem, dot, ext = name.rpartition(".")
        if dot and 0 < len(ext) < 20:
            name = stem[: max_length - len(ext) - 1] + "." + ext
        else:
            name = name[:max_length]
    return name


def register_uploaded_file(
    *,
    file_id: str,
    path: Path,
    filename: str,
    size: int,
    agent_id: str,
    user_id: str | None,
) -> dict[str, Any]:
    """登记一个已经保存到 Agent workspace 的文件。"""
    entry = {
        "file_id": file_id,
        "path": path,
        "filename": filename,
        "size": size,
        "agent_id": agent_id,
        "user_id": (user_id or "").strip() or None,
        "created_at": time.time(),
    }
    FILE_REGISTRY[file_id] = entry
    return entry


def resolve_file_block(
    file_id: str,
    agent_id: str,
    user_id: str | None,
) -> dict[str, Any]:
    """校验 fileId 的 Agent、用户归属和磁盘文件状态。"""
    entry = FILE_REGISTRY.get(str(file_id or ""))
    if entry is None or entry["agent_id"] != agent_id:
        raise FileResolveError(404, f"Unknown fileId for this Agent: {file_id}")
    owner = entry["user_id"]
    if owner and user_id and owner != user_id:
        raise FileResolveError(403, "fileId does not belong to this user")
    if not Path(entry["path"]).is_file():
        FILE_REGISTRY.pop(str(file_id), None)
        raise FileResolveError(410, "Uploaded file is no longer available")
    return entry
