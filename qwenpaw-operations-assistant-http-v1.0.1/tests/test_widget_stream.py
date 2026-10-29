"""Widget 流式标签解析单元测试。"""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "widget_stream.py"
SPEC = importlib.util.spec_from_file_location("widget_stream", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)
WidgetStreamParser = MODULE.WidgetStreamParser


class WidgetStreamParserTest(unittest.TestCase):
    def parse(self, *chunks: str):
        parser = WidgetStreamParser()
        parts = []
        for chunk in chunks:
            parts.extend(parser.feed(chunk))
        parts.extend(parser.finish())
        return [(part.kind, part.value) for part in parts]

    def test_widget_split_across_arbitrary_chunks(self):
        parts = self.parse(
            "回答前<wi",
            "dget>{\"component\":\"faultList\",\"list\":[",
            "{\"level\":\"CRITICAL\",\"count\":1}]}</wid",
            "get>回答后",
        )
        self.assertEqual(
            parts,
            [
                ("text", "回答前"),
                (
                    "widget",
                    {
                        "component": "faultList",
                        "list": [{"level": "CRITICAL", "count": 1}],
                    },
                ),
                ("text", "回答后"),
            ],
        )

    def test_multiple_widgets(self):
        parts = self.parse(
            '<widget>{"component":"a"}</widget>',
            "中间",
            '<widget>{"component":"b"}</widget>',
        )
        self.assertEqual(
            parts,
            [
                ("widget", {"component": "a"}),
                ("text", "中间"),
                ("widget", {"component": "b"}),
            ],
        )

    def test_invalid_json_is_preserved_as_text(self):
        self.assertEqual(
            self.parse("<widget>not-json</widget>"),
            [("text", "<widget>not-json</widget>")],
        )

    def test_unclosed_widget_is_preserved_as_text(self):
        self.assertEqual(
            self.parse('前面<widget>{"component":"faultList"}'),
            [
                ("text", "前面"),
                ("text", '<widget>{"component":"faultList"}'),
            ],
        )

    def test_partial_non_tag_text_is_not_lost(self):
        self.assertEqual(
            self.parse("普通文本<wid", "X后续"),
            [("text", "普通文本"), ("text", "<widX后续")],
        )

    def test_whitespace_only_chunk_before_widget_is_removed(self):
        self.assertEqual(
            self.parse(
                "\n\n",
                '<widget>{"component":"systemMap"}</widget>',
            ),
            [("widget", {"component": "systemMap"})],
        )

    def test_adjacent_newlines_around_widget_are_removed(self):
        self.assertEqual(
            self.parse(
                "正文\n\n",
                '<widget>{"component":"systemMap"}</widget>',
                "\n\n后续正文",
            ),
            [
                ("text", "正文"),
                ("widget", {"component": "systemMap"}),
                ("text", "后续正文"),
            ],
        )

    def test_whitespace_between_normal_text_chunks_is_preserved(self):
        parts = self.parse("hello ", "world", "\nnext line")
        self.assertEqual(
            "".join(value for kind, value in parts if kind == "text"),
            "hello world\nnext line",
        )

    def test_trailing_newlines_are_removed_from_normal_answer(self):
        parts = self.parse("第一行\n第二行", "\n\n")
        self.assertEqual(
            "".join(value for kind, value in parts if kind == "text"),
            "第一行\n第二行",
        )


if __name__ == "__main__":
    unittest.main()
