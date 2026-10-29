"""Unit tests for the datasource query tool."""

from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

PLUGIN_DIR = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "qwenpaw_datasource_query_tool_tests",
    PLUGIN_DIR / "datasource_query_tool.py",
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def result_text(chunk) -> str:
    return chunk.content[0].text


class DatasourceQueryToolTests(unittest.IsolatedAsyncioTestCase):
    async def test_custom_scope_routes_to_custom_url(self):
        config = {
            "market_url": "http://market/query",
            "custom_url": "http://custom/query",
            "read_only": True,
        }
        with (
            patch.object(MODULE, "get_tool_config", return_value=config),
            patch.object(
                MODULE,
                "_post_json",
                return_value=(200, '[{"id":1}]', False),
            ) as post,
        ):
            result = await MODULE.query_datasource(
                "testMysql",
                "select * from mysql_source",
                "CUSTOM",
            )

        post.assert_called_once_with(
            "http://custom/query",
            {
                "datasourceName": "testMysql",
                "sql": "select * from mysql_source",
            },
            30,
            100_000,
        )
        output = json.loads(result_text(result))
        self.assertEqual(output["scope"], "CUSTOM")
        self.assertEqual(output["datasourceName"], "testMysql")
        self.assertEqual(output["result"], [{"id": 1}])

    async def test_market_scope_routes_to_market_url(self):
        config = {
            "market_url": "http://market/query",
            "custom_url": "http://custom/query",
        }
        with (
            patch.object(MODULE, "get_tool_config", return_value=config),
            patch.object(
                MODULE,
                "_post_json",
                return_value=(200, "[]", False),
            ) as post,
        ):
            await MODULE.query_datasource("market-source", "show tables", "MARKET")

        self.assertEqual(post.call_args.args[0], "http://market/query")

    async def test_invalid_scope_is_rejected(self):
        with patch.object(MODULE, "get_tool_config", return_value={}):
            result = await MODULE.query_datasource("source", "select 1", "OTHER")
        self.assertIn("MARKET 或 CUSTOM", result_text(result))

    async def test_write_sql_is_rejected_by_default(self):
        with patch.object(MODULE, "get_tool_config", return_value={}):
            result = await MODULE.query_datasource(
                "source",
                "delete from users",
                "CUSTOM",
            )
        self.assertIn("仅允许", result_text(result))

    async def test_multiple_statements_are_rejected(self):
        with patch.object(MODULE, "get_tool_config", return_value={}):
            result = await MODULE.query_datasource(
                "source",
                "select 1; select 2",
                "CUSTOM",
            )
        self.assertIn("单条 SQL", result_text(result))


if __name__ == "__main__":
    unittest.main()
