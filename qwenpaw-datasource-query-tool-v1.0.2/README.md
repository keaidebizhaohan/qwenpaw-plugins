# QwenPaw 数据查询工具

独立 Tool 插件，不修改 QwenPaw 源码。Agent 根据工具入参 `scope` 选择接口：

- `MARKET`：市场数据源接口；
- `CUSTOM`：自定义数据源接口。

工具名：`query_datasource`（页面名称“数据查询工具”）。

## 配置

安装后进入 **工具 → 数据查询工具 → 配置**，可修改：

- 市场数据源接口 URL；
- 自定义数据源接口 URL；
- 请求超时；
- 是否仅允许只读 SQL；
- 最大返回字符数。

默认接口：

```text
http://10.12.40.35:7055/market/datasource/execute-sql
http://10.12.40.35:7055/custom/datasource/execute-sql
```

工具向目标接口发送：

```json
{
  "datasourceName": "testMysql",
  "sql": "select * from mysql_source"
}
```

## 工具参数

| 参数 | 说明 |
| --- | --- |
| `datasource_name` | 数据源名称，作为接口 `datasourceName` 发送 |
| `sql` | 查询 SQL |
| `scope` | `MARKET` 或 `CUSTOM` |

默认开启只读保护，只允许单条 `SELECT`、`SHOW`、`DESCRIBE`、`EXPLAIN` 或
`WITH` 查询。若确实需要写 SQL，可由管理员在工具配置中关闭只读保护。
