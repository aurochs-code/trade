# A-Stock Trading 终局架构

```
MySQL 事件内核 + 6 个粗粒度业务 Context + 可重建 Projection 的模块化单体
```

CLI / MCP Server 优先。Runtime 数据库由 `ASTOCK_DATABASE_URL=mysql+pymysql://...` 提供；不再维护 SQLite 运行、测试替身或历史迁移入口。

## 文档

| 文档 | 内容 |
|------|------|
| [Agent 架构上下文](./AGENT_ARCHITECTURE_CONTEXT.md) | 后续 agent 开发需求优先读取的架构边界、入口、数据流和定位规则 |
| [架构总览](./ARCHITECTURE.md) | 终局架构图、6 个 Context、运行链路、目录结构 |
| [数据模型](./DATA_MODEL.md) | SQLAlchemy/MySQL schema、事件类型、投影表 |
| [运维手册](../operations/RUNBOOK.md) | 健康检查、备份、launchd 模板 |
