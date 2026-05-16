# A-Stock Trading Agent Guide

Agents must operate this project through stable command surfaces only.

Allowed entrypoints:

- `atrade ...`
- `atrade mcp`
- `bin/trade ...`
- `bin/trade mcp`

Do not execute Python files under `src/astock_trading/**/*.py` directly. Those files are internal modules, not operational entrypoints.

Use JSON output for automation:

- `atrade agent-context --json`
- `atrade doctor --json`
- `atrade health --json`
- `atrade diagnose health --json`
- `atrade diagnose strategy --json`
- `atrade events query --json`
- `atrade runs list --json`
- `atrade status --json`
- `atrade screener candidates --json`
- `atrade screener refresh --json`
- `atrade screener run --query "..." --json`
- `atrade record-buy CODE SHARES PRICE --yes --json`
- `atrade record-sell CODE SHARES PRICE --yes --json`
- `atrade manual-trades list --json`
- `atrade paper status --json`
- `atrade db status --json`
- `atrade db tables --json`
- `atrade db check --json`

Runtime database access requires `ASTOCK_DATABASE_URL`. Production should point to MySQL, for example:

```bash
export ASTOCK_DATABASE_URL='mysql+pymysql://user:password@host:3306/a_stock_trading'
```

SQLite is only for tests and one-time migration from `data/astock_trading.db`.
The only operational command that reads SQLite is:

- `atrade db migrate-sqlite-to-mysql --sqlite-path data/astock_trading.db`

Do not use `--db-path`; runtime commands must use `ASTOCK_DATABASE_URL`.

For source checkout development, `bin/trade ...` remains valid. For installed or
Hermes/OpenClaw usage, prefer global `atrade ...`, which loads `.env` from the
runtime config locations and does not require `cd` into the repository.

Strategy parameters can be switched with `ASTOCK_CONFIG_PROFILE`:
`trend_swing`, `short_continuation`, or `defensive_watch`. Do not switch
profiles for execution tasks without explicit user approval.
