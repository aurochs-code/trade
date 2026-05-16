# A-Stock Trading 运维手册

## 安装与初始化

面向本机和开源用户的正式入口是 `atrade`：

```bash
uv tool install /path/to/a-stock-trading
atrade init
```

`atrade init` 会创建 XDG 运行目录和配置模板：

- `~/.config/a-stock-trading/`
- `~/.local/share/a-stock-trading/`
- `~/.local/state/a-stock-trading/logs/`
- `~/.cache/a-stock-trading/`

编辑 `~/.config/a-stock-trading/.env`，至少设置 `ASTOCK_DATABASE_URL`。如需临时覆盖配置目录，可设置 `ASTOCK_CONFIG_DIR`；如需指定单个 env 文件，可设置 `ASTOCK_ENV_FILE`。

## 每日健康检查

```bash
atrade health --json
atrade diagnose strategy --json
atrade stock analyze 600703 --json
atrade data-sources status --json
atrade check-data-sources 000858 --trade-date 2026-05-15 --json
atrade runs failed --days 3
atrade runs cleanup-stale --older-than-hours 6 --json
```

策略参数可以通过 `ASTOCK_CONFIG_PROFILE` 切换，内置建议 profile：
`trend_swing`、`short_continuation`、`defensive_watch`。不设置时使用默认配置。

`check-data-sources` 返回 `status`、`checks`、`required_missing`、`optional_missing`。核心源缺失时为 `failed`；只缺行业对比、公告、研报、新闻、基本面等辅助源时为 `warning`。

`data-sources status` 从 `market_observations` 聚合最近观测，按时间新鲜度和 `payload_count` 判断健康。核心源包括热股、北向实时、资金流；辅助源为空或过期时只降级为 `warning`。

`run-pipeline` 默认会读取数据源健康：

- 核心源 `failed`：`morning`、`noon`、`intraday_monitor`、`evening`、`scoring`、`auto_trade` 会跳过并退出。
- 辅助源 `warning`：pipeline 继续运行，但 CLI 会打印降级提示。
- 明确要强制运行时使用 `--ignore-data-source-health`。

## 数据库维护

Runtime 数据库是 MySQL，通过 `ASTOCK_DATABASE_URL` 配置。日常运维只使用 MySQL 命令：

```bash
atrade db status --json
atrade db tables --json
atrade db check --json
atrade db backup --output data/backups/astock_trading.sql --yes --json
```

可选低频维护：

```bash
atrade db optimize --yes --json
```

`db backup` 调用本机 `mysqldump`，密码通过 `MYSQL_PWD` 环境变量传给子进程，不放在命令行参数里。生产环境如有 RDS/云数据库快照，优先使用托管备份。

历史 SQLite 只允许作为一次性迁移源读取：

```bash
atrade db migrate-sqlite-to-mysql --sqlite-path data/astock_trading.db --json
```

`runs cleanup-stale` 默认 dry-run。确认历史 running run 可以清理时再加 `--yes`。

## launchd 安装

模板在 `config/launchd/`。复制到 `~/Library/LaunchAgents/` 后加载：

```bash
mkdir -p logs/launchd
cp config/launchd/com.astock_trading.trade.*.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.astock_trading.trade.health.plist
```

盘前/收盘模板中的 `StartCalendarInterval` 只示范周一。生产使用时建议为周一到周五各建一个 plist，或继续使用 `config/astock_trading_crontab_v2`。

## 何时考虑服务化

当前推荐保持 CLI + MCP + MySQL。只有出现以下情况时再引入 HTTP 服务：

- 多用户或远程 Web API
- 常驻实时行情推送
- 数据库达到百万级以上事件且查询明显变慢

不需要 FastAPI 时，Agent 和人工操作统一走 `atrade` / `atrade mcp`。源码 checkout 内开发验证可以继续用 `bin/trade`。

## MCP 本地配置与秘密管理

MCP Server 的稳定入口是：

```bash
atrade mcp
```

本机 Agent 配置可参考 `config/mcp.example.json`，复制为工作区外部或本地未跟踪的 `.mcp.json` 后再填入真实环境变量。不要提交 `.mcp.json`、cookie、session、token、runtime cache、日志或数据库 dump。

`config/mcp_server.yaml` 是本项目的 MCP 治理配置：

- `read_only` / `analysis` tools 可自动批准，但不得下单。
- `state_change` tools 会写入本地状态、行情缓存、运行记录或报告产物，必须确认。
- `high_risk` tools 可能触发模拟盘买卖、撤单或自动交易，必须人工确认。
- 未分类的新 tool 默认按需要确认处理，直到补齐治理分类。
