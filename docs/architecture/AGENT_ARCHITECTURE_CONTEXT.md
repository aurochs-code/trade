# Agent 架构上下文

> 给后续 Codex / Hermes / OpenClaw agent 开发需求使用。开始改代码前先读本文件，再按需求补读相邻模块和测试，避免每次全量扫仓库。

## 当前定位

本项目是 `CLI + MCP + MySQL` 的模块化单体交易辅助系统。

系统负责选股、评分、风控、交易建议、人工确认、模拟盘、本地成交记录、报告和投影重建；没有真实券商实盘接口，真实交易仍以人工确认为边界。评估闭环时要区分“工作流闭环”和“真实券商自动化”。

## 必须遵守的入口

- 安装、调度、Hermes、OpenClaw：优先使用 `atrade ...` 和 `atrade mcp`
- 源码 checkout 内开发验证：可以使用 `bin/trade ...` 和 `bin/trade mcp`
- 自动化输出：优先使用 `--json`
- 不要直接执行 `src/astock_trading/**/*.py`
- 运行库只通过 `ASTOCK_DATABASE_URL` 连接 MySQL
- SQLite 只用于测试替身和历史迁移源：`atrade db migrate-sqlite-to-mysql --sqlite-path PATH_TO_ARCHIVED_SQLITE_DB`
- 执行类任务不要自行切换 `ASTOCK_CONFIG_PROFILE`，除非用户明确批准

快速自检入口：

```bash
atrade agent-context --json
atrade doctor --json
atrade health --json
atrade diagnose health --json
atrade db check --json
```

## 六个业务 Context

| Context | 职责 | 主要读写 |
|---------|------|----------|
| `platform` | DB、事件、配置版本、运行日志、CLI、MCP、pipeline 编排 | MySQL / SQLAlchemy |
| `market` | 行情、财报、资金流、舆情适配器和市场缓存 | 外部数据源 + `market_*` 表 |
| `strategy` | 评分、决策、风格分类、择时 | 纯函数为主，服务层写策略事件 |
| `risk` | 止损、止盈、仓位、组合风控 | 纯函数为主，服务层写风控事件 |
| `execution` | 订单、持仓、人工成交记录、一致性审计、模拟 broker | 事件 + 投影 |
| `reporting` | 投影重建、Discord、Obsidian、报告产物 | 事件 + 文件/报告投影 |

## 统一运行服务图

核心组装点是 `src/astock_trading/platform/service_factory.py` 的 `build_runtime_services()`。

它负责创建：

- `EventStore`
- `RunJournal`
- `ConfigRegistry.freeze()` 后的配置快照
- `MarketService`
- `StrategyService`
- `RiskService`
- `ExecutionService`
- `ProjectionUpdater`
- `ReportGenerator`
- `ObsidianProjector`

新增 CLI、MCP、pipeline 或调度能力时，优先复用这套服务图。不要在新入口里重复拼 DB 连接、配置加载、market provider 链和业务 service。

## 数据主线

运行数据库是 MySQL，schema 由 SQLAlchemy Core 定义。

核心事实和治理表：

- `event_log`：append-only 业务事实
- `event_streams`：事件流版本
- `config_versions`：冻结后的规则版本
- `run_log`：运行生命周期和 artifacts
- `signal_history_snapshots`：历史信号镜像，按 `snapshot_date / history_group_id`
  保留 market / pool / candidates / decision 四段运行证据

市场数据表：

- `market_observations`
- `market_bars`

可重建投影表：

- `projection_positions`
- `projection_orders`
- `projection_balances`
- `projection_candidate_pool`
- `projection_market_state`
- `report_artifacts`

设计约束：

- 金额字段用 `_cents` 整数
- JSON 放 `*_json`
- `projection_*` 表应能从 `event_log` 重建
- `reporting` 只消费事实和写报告产物，不反写业务事实

## 核心运行链路

1. `RunJournal.start_run()` 创建 `run_id` 并绑定 `config_version`
2. pipeline 先检查交易日和今日是否已完成
3. pipeline 走共享数据源健康门禁
4. `MarketService` 采集并标准化行情、财报、资金流、舆情
5. `StrategyService.evaluate()` 写入 `score.calculated` 和 `decision.suggested`
6. 若决策为 `BUY`，额外写入 `manual_trade.requested` 并触发人工确认通知
7. `screener` / `scoring` 把 market、候选池、评分候选和决策归档为同一组历史信号镜像
8. `RiskService` 写入 `risk.*` 风控事件
9. `ExecutionService` 记录人工买卖或模拟成交，并做一致性审计
10. `ProjectionUpdater.rebuild_all()` 从事件重建投影
11. `ReportGenerator`、Discord、Obsidian 输出中文报告
12. `RunJournal.complete_run()` 或 `fail_run()` 记录运行结果

## Pipeline 入口

共享执行入口是 `src/astock_trading/platform/pipeline_runner.py`。

有效 pipeline：

- `morning`
- `noon`
- `intraday_monitor`
- `evening`
- `scoring`
- `weekly`
- `monthly`
- `sentiment`
- `auto_trade`

数据源门禁原则：

- 核心源失败：关键 pipeline 应跳过或失败，并留下 run artifact
- 辅助源降级：pipeline 可以继续，但要在 CLI/报告中清楚提示
- 真实场景验证时优先看 `atrade data-sources status --json`、`atrade diagnose health --json`、`atrade health --json`

## 人工确认边界

`BUY` 决策不等于真实买入。

正确链路是：

1. 策略产生 `decision.suggested`
2. `BUY` 触发 `manual_trade.requested`
3. Discord / 报告展示为“买入意向”或“待人工确认”
4. 人工确认后使用 `atrade record-buy CODE SHARES PRICE --yes --json`
5. 本地写入 `order.*`、`position.*`，再重建投影
6. `ExecutionService.audit_manual_trade_consistency(order_id)` 可审计本地记录是否一致

不要把 `watch`、`core`、`BUY` 混为同一个交易强度。弱信号应表达为“观察”或“等待”，不要包装成看多结论。

## 开发定位规则

常见需求的首读位置：

- 命令面、JSON 输出、MCP：`src/astock_trading/platform/cli/`、`src/astock_trading/platform/mcp_server.py`
- 服务组装：`src/astock_trading/platform/service_factory.py`
- pipeline：`src/astock_trading/platform/pipeline_runner.py`、`src/astock_trading/pipeline/`
- 数据源健康：`src/astock_trading/market/health.py`、`src/astock_trading/platform/pipeline_policy.py`
- 选股和评分：`src/astock_trading/platform/cli/screener.py`、`src/astock_trading/strategy/`
- P5 参数校准：`src/astock_trading/pipeline/param_calibration.py`、`atrade calibrate --json`
- P6 自适应风控建议：`src/astock_trading/pipeline/adaptive_risk.py`、`atrade risk adaptive --json`
- P6 多策略 profile 对比和隔离资金建议：`src/astock_trading/pipeline/strategy_profiles.py`、`atrade strategy profiles --json`、`atrade strategy allocation --json`
- P6 策略体检和深度归因：`src/astock_trading/pipeline/strategy_health.py`、`atrade strategy health --json`
- P6 仪表盘数据契约：`src/astock_trading/platform/dashboard.py`、`atrade dashboard snapshot --json`
- 历史信号镜像：`src/astock_trading/platform/history_mirror.py`、`src/astock_trading/platform/cli/history.py`、`src/astock_trading/backtest/engine.py`
- 人工确认：`src/astock_trading/strategy/service.py`、`src/astock_trading/platform/cli/manual_trades.py`、`src/astock_trading/platform/cli/trading.py`
- 成交和持仓：`src/astock_trading/execution/`
- 投影和报告：`src/astock_trading/reporting/`
- LLM 摘要：`src/astock_trading/platform/llm_context.py`、`docs/operations/HERMES_LLM_SUMMARIES.md`

## 输出语言

新增或修改用户可见内容时默认中文。内部字段、数据库字段、枚举值、CLI 参数、环境变量和第三方 API 名称可以保留英文。

Discord、Obsidian、报告和 agent-facing 说明不要直接暴露内部信号名，除非它是协议字段。常见展示转义见根目录 `AGENTS.md`。

## 参考文档

- `AGENTS.md`
- `docs/architecture/ARCHITECTURE.md`
- `docs/architecture/DATA_MODEL.md`
- `docs/operations/RUNBOOK.md`
