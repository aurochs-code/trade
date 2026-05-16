# 架构总览

## 架构图

```
CLI (typer) / MCP Server (FastMCP stdio via bin/trade mcp)
         │
         ▼
┌────────────────────────────────────────────────────────┐
│                    platform                             │
│  EventStore · ConfigRegistry · RunJournal · CLI · MCP   │
│                    MySQL Event Kernel                    │
│  event_log · config_versions · run_log                  │
└────────────────────────┬───────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│   market     │ │  strategy    │ │    risk      │
│ AkShare/MX   │ │ Scorer 纯函数│ │ Rules 纯函数 │
│ adapters     │ │ Decider      │ │ Sizing       │
│ MarketStore  │ │ Classifier   │ │ RiskService  │
│ MarketService│ │ Timer        │ │              │
│              │ │ StrategyServ │ │              │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       └────────────────┼────────────────┘
                        ▼
              ┌──────────────────┐
              │   execution      │
              │ OrderManager     │
              │ PositionManager  │
              │ PositionProjector│
              │ ExecutionService │
              │ SimulatedBroker  │
              │ MXBroker         │
              └────────┬─────────┘
                       ▼
              ┌──────────────────┐
              │   reporting      │
              │ ProjectionUpdater│
              │ ReportGenerator  │
              │ ObsidianProjector│
              │ Discord 格式化   │
              └──────────────────┘
```

## 6 个 Context

| Context | 职责 | IO |
|---------|------|----|
| platform | DB、config 版本、run lifecycle、事件分发、CLI/MCP | MySQL / SQLAlchemy |
| market | 行情/财报/资金流/舆情抓取与标准化 | AkShare/MX HTTP |
| strategy | 评分、决策、风格分类、择时 | 无（纯函数） |
| risk | 止损/止盈/仓位 sizing/组合风控 | 无（纯函数） |
| execution | 订单、持仓、投影重建 | MySQL |
| reporting | 报告生成、Obsidian/Discord 投影 | MySQL + 文件 |

## 核心运行链路

```
1. CreateRun → run_id + freeze config_version
2. CollectMarketData → MarketService.collect_batch() → market_observations
3. RunStrategy → StrategyService.evaluate() → score.calculated + decision.suggested 事件
4. RunRisk → RiskService.assess_position() → risk.* 事件
5. Execute → ExecutionService.execute_buy/sell() → order.* + position.* 事件
6. UpdateProjections → ProjectionUpdater.rebuild_all()
7. EmitReports → ReportGenerator → report_artifacts
8. CompleteRun → run_log status=completed
```

## 目录结构

```
src/astock_trading/
├── platform/
│   ├── database.py        # ASTOCK_DATABASE_URL / SQLAlchemy engine
│   ├── schema.py          # SQLAlchemy Core schema
│   ├── db.py              # runtime connect/init + legacy migration helpers
│   ├── events.py          # EventStore (append-only)
│   ├── config.py          # ConfigRegistry (版本化 freeze)
│   ├── runs.py            # RunJournal (幂等 lifecycle)
│   ├── cli/               # typer CLI command modules
│   └── mcp_server.py      # FastMCP Server tools
├── market/
│   ├── models.py          # StockQuote, TechnicalIndicators, StockSnapshot, ...
│   ├── adapters.py        # Protocol + AkShare/MX adapters
│   ├── store.py           # MarketStore (observations + bars + TTL cache)
│   ├── service.py         # MarketService (并发 + fallback + 限流)
│   └── mx_async.py        # httpx async MX client
├── strategy/
│   ├── models.py          # ScoreResult, DecisionIntent, StyleResult, ...
│   ├── scorer.py          # Scorer 四维评分 (纯函数)
│   ├── decider.py         # Decider 综合决策 (纯函数)
│   ├── classifier.py      # 风格判定 (纯函数)
│   ├── timer.py           # 大盘择时 (纯函数)
│   └── service.py         # StrategyService (评分+决策+事件写入)
├── risk/
│   ├── models.py          # ExitSignal, RiskParams, PositionSize, ...
│   ├── rules.py           # 止损/止盈/时间止损/MA离场 (纯函数)
│   ├── sizing.py          # 仓位计算 (纯函数)
│   └── service.py         # RiskService (风控+事件写入)
├── execution/
│   ├── models.py          # Order, Position, Balance, TradeEvent
│   ├── orders.py          # OrderManager (事件化)
│   ├── positions.py       # PositionManager + PositionProjector
│   └── service.py         # ExecutionService + SimulatedBroker + MXBroker
└── reporting/
    ├── projectors.py      # ProjectionUpdater (event → projection)
    ├── reports.py         # ReportGenerator (盘前/收盘/评分/周报)
    ├── obsidian.py        # ObsidianProjector (vault 投影)
    └── discord.py         # Discord embed 格式化

tests/astock_trading/
├── platform/              # EventStore, Config, Runs, MCP tools
├── strategy/              # Scorer, Decider, Classifier, Timer, StrategyService
├── risk/                  # Rules, Sizing, RiskService
├── market/                # MarketStore, MarketService
├── execution/             # Orders, Positions, Projections, ExecutionService
└── reporting/             # Projectors, Reports, Discord, Obsidian
```

## MCP Tools

稳定入口是 `bin/trade mcp`。不要直接运行 `src/astock_trading/platform/mcp_server.py` 或其他内部模块。

MCP 工具按治理风险分类，具体清单和审批策略由 `config/mcp_server.yaml` 维护：

| 分类 | 说明 |
|------|------|
| read_only | 只读取本地投影、运行状态、交易记录或模拟盘状态 |
| analysis | 执行评分、风控、仓位、选股或外部市场信息分析，不下单 |
| state_change | 写入本地状态、行情缓存、运行记录、watchlist、回测或报告产物 |
| high_risk | 自动交易、模拟盘买入/卖出/撤单等可能改变账户状态的操作 |

## 设计约束

- strategy/ 和 risk/ 不 import HTTP/SQL/YAML/文件系统
- 所有 projection_* 表可从 event_log 完全重建
- 金额字段用 _cents 整数
- 每次 run 冻结 config_version + run_id
- reporting/ 不反写业务表
- Runtime 只通过 `ASTOCK_DATABASE_URL` 连接 MySQL
- SQLite 只用于测试替身和 `migrate-sqlite-to-mysql` 的历史源读取
