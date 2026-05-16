# trade

当前仓库采用“同仓分目录”结构：

- `src/` 放代码；全局安装后配置默认在 `~/.config/a-stock-trading/`
- `config/`、`data/` 是源码 checkout 的开发默认配置和运行数据
- `trade-vault/` 放 Obsidian 内容区
- `atrade` 是安装后的全局 CLI；`bin/trade` 是源码 checkout 内的开发入口

常用约定：

- 默认 vault 路径由 `config/paths.yaml` 指向 `trade-vault/`
- 如需临时覆盖，可设置环境变量 `AStockVault`
- 运行自检可用：`atrade doctor --json`
- 业务日期、日报归档、run 幂等判断统一按 `Asia/Shanghai` 处理；审计时间戳仍保存为 UTC ISO

安装和初始化：

```bash
uv tool install /path/to/a-stock-trading
atrade init
```

`atrade init` 会创建 `~/.config/a-stock-trading/`、`~/.local/share/a-stock-trading/`、
`~/.local/state/a-stock-trading/logs/` 和 `~/.cache/a-stock-trading/`，并写入配置模板。
编辑 `~/.config/a-stock-trading/.env` 后即可在任意目录执行 `atrade ...`。

常用命令：

- `atrade doctor --json`：环境自检
- `atrade db migrate`：初始化或升级数据库 schema
- `atrade db status`：查看数据库状态
- `atrade run-pipeline morning --json`：执行盘前 pipeline
- `atrade run-pipeline scoring --json`：执行评分 pipeline
- `atrade screener refresh --json`：刷新候选池、评分并更新 projection
- `atrade screener candidates --json`：查看候选池
- `atrade status --json`：查看持仓
- `atrade paper status --json`：查看模拟盘
- `atrade fetch-history 600036 --count 500 --json`：拉取历史 K 线
- `atrade backtest 600036,000001 2025-01-01 2025-12-31 --json`：运行回测
- `atrade continuation-validate 600036,000001 --start 2026-01-01 --end 2026-03-31 --json`：运行短线续涨验证
- `atrade continuation-backtest 600036,000001 2026-01-01 2026-03-31 --hold-days 2 --top-n 3 --json`：运行短线续涨回测
- `atrade mcp`：启动 MCP Server

`trade-vault/` 结构示例：

- `00-系统`：仪表盘、使用指南、模板
- `01-状态`：持仓、账户、池子
- `02-巡检`：每日收盘后由 Agent 生成的巡检报告
- `03-分析`：周复盘、月复盘、专题分析、策略体检
- `04-决策`：今日决策、候选池和交易结论
