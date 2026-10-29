# LightRAG Local Query

独立 QwenPaw 工具插件。调用 LightRAG `/query` 的 `local` 模式，执行知识图谱实体优先检索，重点聚焦具体实体及其邻近事实和关系。

插件无需任何配置，固定连接 `http://10.12.40.32:9621/webui/`。Agent 会先发现可用知识库，再根据用户问题选择一个或多个 workspace。

Top K 和 token 上限不在设置界面配置；代码默认值为 `40 / 20 / 6000 / 8000 / 30000`，Agent 每次调用时可以动态替换。
