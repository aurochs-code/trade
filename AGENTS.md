# A-Stock Trading Agent Guide

Agents must operate this project through stable command surfaces only.

Allowed entrypoints:

- `bin/trade ...`
- `bin/trade mcp`

Do not execute Python files under `src/astock_trading/**/*.py` directly. Those files are internal modules, not operational entrypoints.

Use JSON output for automation:

- `bin/trade agent-context --json`
- `bin/trade doctor --json`
- `bin/trade health --json`
- `bin/trade events query --json`
- `bin/trade runs list --json`
- `bin/trade status --json`
- `bin/trade screener candidates --json`
- `bin/trade screener run --query "..." --json`
- `bin/trade record-buy CODE SHARES PRICE --yes --json`
- `bin/trade record-sell CODE SHARES PRICE --yes --json`
- `bin/trade manual-trades list --json`
- `bin/trade paper status --json`
- `bin/trade db status --json`
- `bin/trade db tables --json`
- `bin/trade db check --json`

Runtime database access requires `ASTOCK_DATABASE_URL`. Production should point to MySQL, for example:

```bash
export ASTOCK_DATABASE_URL='mysql+pymysql://user:password@host:3306/a_stock_trading'
```

SQLite is only for tests and one-time migration from `data/astock_trading.db`.
The only operational command that reads SQLite is:

- `bin/trade db migrate-sqlite-to-mysql --sqlite-path data/astock_trading.db`

Do not use `--db-path`; runtime commands must use `ASTOCK_DATABASE_URL`.
