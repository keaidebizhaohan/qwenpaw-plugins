"""流式解析 Agent 回答中的 ``<widget>...</widget>`` 标签。

解析器会跨 QwenPaw 文本分片保存状态：普通文字尽快返回给调用方，Widget
内容则缓存到闭合标签出现，再一次性解析为 JSON。这样既保持普通回答的流式体验，
也能保证下游收到的 WIDGET value 是完整 JSON。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class ParsedPart:
    """解析后的一个输出片段。"""

    kind: Literal["text", "widget"]
    value: Any


class WidgetStreamParser:
    """识别可能被任意拆分到多个流式分片中的 Widget 标签。"""

    OPEN_TAG = "<widget>"
    CLOSE_TAG = "</widget>"

    def __init__(self) -> None:
        self._outside_buffer = ""
        self._widget_buffer = ""
        self._inside_widget = False
        # 普通文本末尾的空白先不发送；若下一项是 Widget，它属于标签边界
        # 的排版换行，应当丢弃，而不是生成一个空白 Markdown 消息块。
        self._pending_whitespace = ""
        self._strip_leading_after_widget = False

    @staticmethod
    def _partial_tag_suffix_length(text: str, tag: str) -> int:
        """返回 text 末尾与 tag 开头重合的最大长度。"""
        max_length = min(len(text), len(tag) - 1)
        for length in range(max_length, 0, -1):
            if text.endswith(tag[:length]):
                return length
        return 0

    def _append_text(self, text: str, parts: list[ParsedPart]) -> None:
        """发送确定属于正文的内容，并暂存可能紧邻 Widget 的尾部空白。"""
        if self._strip_leading_after_widget:
            text = text.lstrip()
            if not text:
                return
            self._strip_leading_after_widget = False

        combined = self._pending_whitespace + text
        self._pending_whitespace = ""
        if not combined:
            return

        without_trailing = combined.rstrip()
        if not without_trailing:
            self._pending_whitespace = combined
            return

        self._pending_whitespace = combined[len(without_trailing) :]
        parts.append(ParsedPart("text", without_trailing))

    def feed(self, text: str) -> list[ParsedPart]:
        """输入一个文本增量，返回当前已经可以安全发出的片段。"""
        if not text:
            return []

        parts: list[ParsedPart] = []
        remaining = text
        while remaining:
            if self._inside_widget:
                self._widget_buffer += remaining
                close_index = self._widget_buffer.find(self.CLOSE_TAG)
                if close_index < 0:
                    break

                widget_text = self._widget_buffer[:close_index]
                remaining = self._widget_buffer[
                    close_index + len(self.CLOSE_TAG) :
                ]
                self._widget_buffer = ""
                self._inside_widget = False
                try:
                    value = json.loads(widget_text)
                except (TypeError, ValueError):
                    # 非法 JSON 不静默丢失：按原始普通文本返回，便于发现模型输出问题。
                    self._append_text(
                        self.OPEN_TAG + widget_text + self.CLOSE_TAG,
                        parts,
                    )
                else:
                    parts.append(ParsedPart("widget", value))
                    self._strip_leading_after_widget = True
                continue

            combined = self._outside_buffer + remaining
            self._outside_buffer = ""
            open_index = combined.find(self.OPEN_TAG)
            if open_index >= 0:
                if open_index:
                    self._append_text(combined[:open_index], parts)
                # 标签前仅用于排版的空格和换行不作为普通文本事件输出。
                self._pending_whitespace = ""
                remaining = combined[open_index + len(self.OPEN_TAG) :]
                self._inside_widget = True
                continue

            partial_length = self._partial_tag_suffix_length(
                combined,
                self.OPEN_TAG,
            )
            if partial_length:
                safe_text = combined[:-partial_length]
                self._outside_buffer = combined[-partial_length:]
            else:
                safe_text = combined
            if safe_text:
                self._append_text(safe_text, parts)
            break

        return parts

    def finish(self) -> list[ParsedPart]:
        """结束流并返回尚未闭合的内容，保证异常输出不会被静默丢弃。"""
        parts: list[ParsedPart] = []
        if self._inside_widget:
            self._append_text(
                self.OPEN_TAG + self._widget_buffer,
                parts,
            )
        elif self._outside_buffer:
            self._append_text(self._outside_buffer, parts)

        self._outside_buffer = ""
        self._widget_buffer = ""
        self._inside_widget = False
        self._pending_whitespace = ""
        self._strip_leading_after_widget = False
        return parts
