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
atrade calibrate --json
atrade risk adaptive --json
atrade strategy profiles --json
atrade screener explain --json
atrade screener iterate --json
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
atrade db backup --output ~/.local/state/a-stock-trading/backups/astock_trading.sql --yes --json
```

可选低频维护：

```bash
atrade db optimize --yes --json
```

`db backup` 调用本机 `mysqldump`，密码通过 `MYSQL_PWD` 环境变量传给子进程，不放在命令行参数里。生产环境如有 RDS/云数据库快照，优先使用托管备份。

历史 SQLite 已迁入 MySQL，不再保存在 checkout 内。如需重放迁移，只能显式传入外部归档的 SQLite 文件：

```bash
atrade db migrate-sqlite-to-mysql --sqlite-path PATH_TO_ARCHIVED_SQLITE_DB --json
```

`runs cleanup-stale` 默认 dry-run。确认历史 running run 可以清理时再加 `--yes`。

## 证据链查询与人工成交记录

AI 摘要只作为报告层输出，不能当作事实来源。需要复盘某只股票时，先拉取事件证据链：

```bash
atrade events evidence 002138 --json
atrade history signal --date 2026-05-19 --code 002138 --json
```

该命令会按股票代码汇总评分、决策、人工确认、订单、持仓和交易复盘事件。
历史信号镜像会按 `snapshot_date / history_group_id` 还原某次 screener 或 scoring
运行当时看到的 market / pool / candidates / decision，用于回答“当时为什么没进池、
没过分数、被否决或只给观察”。如果没有指定 `--history-group-id`，默认读取当天最新
一组镜像。

`atrade stock analyze CODE --json` 会在 `history_signal` 字段返回最近历史镜像中的
单股命中/漏判解释，并把真实 miss reason 写入 `findings`。`atrade backtest ...`
默认优先读取历史镜像；需要只看代理回放时显式加 `--no-history-mirror`。

历史旧事件缺少新证据字段时，不要手工改写 `event_log`。使用 append-only 回填：

```bash
atrade events backfill-evidence --json
atrade events backfill-evidence --code 002138 --apply --json
```

回填事件会标记 `legacy_partial`，只说明“旧事件当时缺什么、旧 payload 是什么”，不能把事后总结伪造成交易前证据。

人工买卖补录时可以同时写入交易前假设和来源事件：

```bash
atrade record-buy 002138 100 15.00 --yes --json \
  --source-event-id DECISION_OR_MANUAL_EVENT_ID \
  --source-score-event-id SCORE_EVENT_ID \
  --hypothesis "突破后回踩不破，资金流仍为正" \
  --invalidation "跌破 MA20 或主力连续流出" \
  --review-after-days 3
```

`record-buy` / `record-sell` 会额外写入 `trade.hypothesis.recorded` 和
`trade.outcome.recorded`，用于把交易前假设、成交后结果和后续复盘证据串起来。

到达 `--review-after-days` 后，使用历史 K 线计算 MFE/MAE 并写入复盘证据：

```bash
atrade review trades --json
atrade review trades --record --json
atrade review trades --code 002138 --as-of 2026-05-18 --record --json
```

`--record` 会追加 `trade.review.recorded`；不加 `--record` 只预览。复盘依赖
`market_bars`，没有 K 线时会返回 `insufficient_market_bars`，不要让 LLM 自行猜测 MFE/MAE。

P5 参数校准只输出建议，不自动改配置：

```bash
atrade calibrate --json
atrade calibrate --record --json
```

校准报告读取 `trade.review.recorded`、来源 `score.calculated`、候选池事件和 `market_bars`，
输出止盈/止损/时间止损建议、四维评分权重方向、veto/选股条件复核建议。样本不足时返回
`insufficient_data`；`--record` 会追加 `strategy.calibration.proposed` 并落一份 Markdown
报告 artifact。

P6-1 自适应风控同样只输出建议，不自动改配置或下单：

```bash
atrade risk adaptive --json
atrade risk adaptive --record --json
```

该命令读取 `market_bars`、`balance.*` 事件 / 余额投影和 `trade.review.recorded`，
根据近期市场波动、账户回撤和连续盈亏状态输出止损宽度、仓位上限、买入阈值建议。
样本不足时返回 `insufficient_data`；`--record` 会追加
`risk.adaptive_suggestion.proposed` 并落一份 Markdown 报告 artifact。该建议只供人工
复核和后续参数校准使用，不会写 `strategy.yaml`，也不会触发真实交易。

P6-2 多策略 profile 对比只读运行，不自动切换执行 profile：

```bash
atrade strategy profiles --json
atrade strategy profiles --record --json
```

该命令读取 `trend_swing`、`short_continuation`、`defensive_watch` 等配置 profile，
对比买入阈值、数据质量门禁、仓位上限、短线续涨参数，并匹配 `config_versions` /
`run_log` / `decision.suggested` / `trade.review.recorded` 判断每个 profile 是否已有
运行与复盘证据。样本不足时返回 `needs_shadow_validation`；`--record` 会追加
`strategy.profile_comparison.proposed` 和 Markdown artifact。执行前仍必须显式设置并确认
`ASTOCK_CONFIG_PROFILE`，不要让 agent 自动切换。

模拟盘 vs 实盘逐笔对账：

```bash
atrade review shadow --date 2026-05-18 --json
atrade review shadow --date 2026-05-18 --record --json
```

对账优先使用 `signal_id`，缺失时回退到 `code + side + event_date`，并在明细里保留
`order_id`。`--record` 会把偏离写成 `rule_deviation.recorded`，偏离类型包括
`not_executed`、`extra_real_trade`、`partial_fill`、`price_slippage` 和
`manual_override`。

Hermes 轻量查询：

```bash
atrade digest --json
atrade suggest --json
atrade explain 002138 --json
```

这三类命令只读事件和投影：`digest` 给一句话状态，`suggest` 给下一步建议，
`explain` 解释单只股票最近评分和决策。它们不会下单，也不会自动调低买入门槛。

实盘试运行护栏审计：

```bash
atrade risk trial-guard --json
atrade risk trial-guard --capital 500000 --amount 60000 --json
```

该命令只读配置，不执行交易。默认试运行单票上限为正式单票上限的一半
（`risk.position.single_max * trial_single_max_ratio`），同时明确真实交易必须人工确认，
系统没有券商实盘下单接口。

## launchd 安装

模板在 `config/launchd/`。复制到 `~/Library/LaunchAgents/` 后加载：

```bash
mkdir -p logs/launchd
cp config/launchd/com.astock_trading.trade.*.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.astock_trading.trade.health.plist
```

盘前/收盘模板中的 `StartCalendarInterval` 只示范周一。生产使用时建议为周一到周五各建一个 plist，或继续使用 `config/astock_trading_crontab_v2`。

## Hermes LLM 摘要

Hermes 定时任务分为两层：原有 `no_agent: true` 任务继续跑确定性流水，LLM 摘要任务只通过 `atrade llm-context --mode ...` 读取上下文后生成中文总结。

安装和任务创建步骤见 `docs/operations/HERMES_LLM_SUMMARIES.md`。Hermes 不应进入交易系统 checkout 或直接运行仓库脚本；不要用 LLM 摘要任务替代盘中风控、止损/止盈、人工确认、pipeline 失败和核心数据源严重异常告警。

`atrade llm-context --mode morning|close|weekly` 会输出“证据编号清单”。Hermes LLM 最终摘要每个判断段落必须写 `evidence_id: ...`；`atrade notify llm-summary-card` 默认会拒绝缺少 `evidence_id` 的摘要。

完整调度节奏和精简目标见 `docs/operations/HERMES_SCHEDULE.md`。

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
