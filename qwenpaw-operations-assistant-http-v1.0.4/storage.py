"""运维助手上传文件名的安全清理工具。"""

from __future__ import annotations

from typing import Any


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
