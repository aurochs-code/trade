# A-Stock Trading Agent Guide

Agents must operate this project through stable command surfaces only.

Before changing code, read `docs/architecture/AGENT_ARCHITECTURE_CONTEXT.md`
for the current system boundary, runtime graph, data flow, and module lookup
rules. Use it as the lightweight architecture context instead of re-reading the
entire repository.

## Language and presentation

中文是本项目的默认面向用户语言。新增或修改代码、注释、文档、报告模板、
Discord 推送、Obsidian 输出和 agent-facing 说明时，优先使用简体中文。

Python/JSON 字段名、枚举值、数据库字段、CLI 参数、环境变量、第三方 API 名称、
测试断言中的协议值可以保留英文；但凡是给用户或运营人员看的标题、说明、
注释、报告正文和错误解释，应写成中文。

Discord、Obsidian 和其他人读报告里不要直接展示内部信号名，除非它本身是
协议字段。常见内部值应转义为中文展示，例如：

- `BUY` → `买入意向`
- `SELL` → `卖出意向`
- `WATCH` → `观察`
- `NO_TRADE` → `不操作`
- `GREEN` → `偏强`
- `YELLOW` → `震荡`
- `RED` → `转弱`
- `CLEAR` → `观望`
- `entry_signal` → `入场信号`
- `veto` / `hard_veto` → `否决`
- `warning_signals` → `预警信号`
- `data_quality` → `数据质量`

Allowed entrypoints:

- `atrade ...`
- `atrade mcp`
- `bin/trade ...`
- `bin/trade mcp`

Do not execute Python files under `src/astock_trading/**/*.py` directly. Those files are internal modules, not operational entrypoints.

CLI is the primary product surface. New operational capability must be exposed
first as `atrade ... --json` / `bin/trade ... --json`; MCP tools are thin
agent-client adapters and must not be the only way to access a capability.
If a useful capability exists only in MCP, add the matching CLI command before
expanding MCP further.

Use JSON output for automation:

- `atrade agent-context --json`
- `atrade doctor --json`
- `atrade health --json`
- `atrade diagnose health --json`
- `atrade diagnose strategy --json`
- `atrade digest --json`
- `atrade events query --json`
- `atrade events evidence CODE --json`
- `atrade events backfill-evidence --json`
- `atrade runs list --json`
- `atrade status --json`
- `atrade screener candidates --json`
- `atrade screener explain --json`
- `atrade screener iterate --json`
- `atrade screener refresh --json`
- `atrade screener run --query "..." --json`
- `atrade stock analyze CODE_OR_NAME --json`
- `atrade suggest --json`
- `atrade explain CODE --json`
- `atrade risk check CODE --json`
- `atrade risk portfolio --json`
- `atrade risk position CODE SCORE PRICE --json`
- `atrade market-intel brief --query "..." --json`
- `atrade market-intel hot-stocks --json`
- `atrade market-intel northbound --json`
- `atrade market-intel fund-flow CODE --json`
- `atrade record-buy CODE SHARES PRICE --yes --json`
- `atrade record-sell CODE SHARES PRICE --yes --json`
- `atrade review shadow --json`
- `atrade review trades --json`
- `atrade manual-trades list --json`
- `atrade paper status --json`
- `atrade db status --json`
- `atrade db tables --json`
- `atrade db check --json`

Runtime database access requires `ASTOCK_DATABASE_URL`. Production should point to MySQL, for example:

```bash
export ASTOCK_DATABASE_URL='mysql+pymysql://user:password@host:3306/a_stock_trading'
```

SQLite is only for tests and archived one-time migration sources. The historical
`data/astock_trading.db` source has been migrated to MySQL and is no longer kept
in the checkout. The only operational command that reads SQLite is:

- `atrade db migrate-sqlite-to-mysql --sqlite-path PATH_TO_ARCHIVED_SQLITE_DB`

Do not use `--db-path`; runtime commands must use `ASTOCK_DATABASE_URL`.

For source checkout development, `bin/trade ...` remains valid. For installed or
Hermes/OpenClaw usage, prefer global `atrade ...`, which loads `.env` from the
runtime config locations and does not require `cd` into the repository.

Strategy parameters can be switched with `ASTOCK_CONFIG_PROFILE`:
`trend_swing`, `short_continuation`, or `defensive_watch`. Do not switch
profiles for execution tasks without explicit user approval.
