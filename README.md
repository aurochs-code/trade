# trade

当前仓库采用“同仓分目录”结构：

- `src/`、`config/`、`data/` 放代码、配置和运行数据
- `trade-vault/` 放 Obsidian 内容区
- `bin/trade` 作为统一 CLI 入口，默认使用 `.venv/`

常用约定：

- 默认 vault 路径由 `config/paths.yaml` 指向 `trade-vault/`
- 如需临时覆盖，可设置环境变量 `AStockVault`
- 运行自检可用：`bin/trade doctor --json`
- 业务日期、日报归档、run 幂等判断统一按 `Asia/Shanghai` 处理；审计时间戳仍保存为 UTC ISO

常用命令：

- `bin/trade doctor --json`：环境自检
- `bin/trade db migrate`：初始化或升级数据库 schema
- `bin/trade db status`：查看数据库状态
- `bin/trade run-pipeline morning`：执行盘前 pipeline
- `bin/trade run-pipeline scoring`：执行评分 pipeline
- `bin/trade screener run --query "近20日放量突破，市值100亿以上" --json`：执行选股筛选
- `bin/trade screener candidates --json`：查看候选池
- `bin/trade status --json`：查看持仓
- `bin/trade paper status --json`：查看模拟盘
- `bin/trade fetch-history 600036 --count 500`：拉取历史 K 线
- `bin/trade backtest 600036,000001 2025-01-01 2025-12-31`：运行回测
- `bin/trade continuation-validate 600036,000001 --start 2026-01-01 --end 2026-03-31 --json`：运行短线续涨验证
- `bin/trade continuation-backtest 600036,000001 2026-01-01 2026-03-31 --hold-days 2 --top-n 3`：运行短线续涨回测
- `bin/trade mcp`：启动 MCP Server

`trade-vault/` 结构示例：

- `00-系统`：仪表盘、使用指南、模板
- `01-状态`：持仓、账户、池子
- `02-巡检`：每日收盘后由 Agent 生成的巡检报告
- `03-分析`：周复盘、月复盘、专题分析、策略体检
- `04-决策`：今日决策、候选池和交易结论
