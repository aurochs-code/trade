# 交易推荐优化 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 解决“系统长期没有股票推荐”的可运营问题：在不降低正式买入承接安全边界的前提下，补强数据源稳定性、扩大可解释入场路线、增加弱推荐/影子跟踪层，并让 CLI、Discord、Obsidian 都能清楚说明为什么当前没有正式买入、哪些票值得继续盯。

**Architecture:** 继续遵守 `CLI + MCP + MySQL` 模块化单体边界。数据源通过 `MarketService + SourceRouter` 接入；策略仍由 `StrategyService + Scorer + Decider` 产生评分与决策；正式模拟承接仍必须通过 `core` 候选、`BUY` 买入意向、新鲜评分、买入窗口、profile 和风控门禁。新增“推荐可见层”只能输出观察/试买意向/复盘转强，不得写入 `manual_trade.requested`，不得提交 MX 模拟盘订单。

**Tech Stack:** Python、Typer CLI、SQLAlchemy/MySQL、pytest、ruff、现有 `bin/trade` / `atrade` 命令面。

---

## Current Evidence

本计划基于 2026-06-13 的只读诊断结果：

- `paper auto-readiness` 阻断项：买入窗口关闭、核心池为空、下个窗口前需重新刷新候选评分、没有新鲜买入信号。
- 当前候选池：总数 3，`core=0`、`watch=1`、`radar=2`，最近评分日期为 2026-06-12，非交易日检查时已超过 24 小时新鲜度阈值。
- 当前入场信号：0。
- `trend_swing` 历史决策：`BUY=0`、`WATCH=98`、`CLEAR=158`、`TRIAL_BUY=4`，说明正式买入极严，但已有少量低置信试买意向链路。
- 数据源诊断：必需源没有缺失；近期 provider failure 主要集中在资金流增强源，`BaiduFundFlowAdapter=19`、`AkShareFlowAdapter=1`；最近筛选 L1 行情/技术/财务/资金流覆盖率为 1.0，L2 舆情/行业覆盖不足。
- 代码事实：已接入 Tushare SDK provider；`ASTOCK_TUSHARE_TOKEN` 存在时，MX 和 Tushare 作为付费主源，其他 provider 作为 fallback。

结论：系统不是单纯“数据坏了”，而是四个链路同时偏紧：候选晋级核心偏难、入场信号路线偏少、正式买入承接只在买入窗口内看新鲜 `BUY`、报告层没有充分展示“弱但值得跟踪”的候选。

## Optimization Principles

- 不降低正式 `BUY` 线，不绕过 `core` / 买入窗口 / 风控 / 人工确认。
- 不把 `TRIAL_BUY`、`watch`、`radar` 当作正式买入。
- 把“推荐给人看”和“提交模拟盘订单”分离。
- Tushare 和 MX 作为付费主源：没有 token 时系统必须继续运行；有 token 时优先使用 Tushare SDK 的常规积分接口，并在诊断中展示启用状态和已配置接口清单。
- 所有新增运营能力先暴露为 `bin/trade ... --json` / `atrade ... --json`，MCP 只做薄适配。

## Target Outcomes

- 数据源：资金流增强源 unresolved failure 降低，Tushare token 可用性在 `data-sources diagnose --json` 中可见。
- 推荐层：即使没有正式 `BUY`，CLI/Discord 也能稳定输出“观察候选、试买意向、复盘转强候选、缺口原因”。
- 入场信号：新增保守可解释路线后，活跃交易周至少能看到可复核的入场路线候选，而不是只有“入场信号不足”。
- 安全性：`TRIAL_BUY`、弱推荐、正向影子复盘都不得产生 `manual_trade.requested` 或 MX 模拟盘订单。

---

## File Map

### Likely New Files

- `src/astock_trading/market/tushare_adapters.py`
- `src/astock_trading/platform/recommendation_diagnostics.py`
- `tests/astock_trading/market/test_tushare_adapters.py`
- `tests/astock_trading/platform/test_recommendation_diagnostics.py`

### Likely Modified Files

- `src/astock_trading/market/adapters.py`
- `src/astock_trading/market/protocols.py`
- `src/astock_trading/market/health.py`
- `src/astock_trading/market/service.py`
- `src/astock_trading/platform/service_factory.py`
- `src/astock_trading/platform/cli/data_sources.py`
- `src/astock_trading/platform/cli/diagnostics.py`
- `src/astock_trading/platform/cli/agent.py`
- `src/astock_trading/platform/agent_diagnostics.py`
- `src/astock_trading/platform/paper_trial.py`
- `src/astock_trading/platform/stock_analysis.py`
- `src/astock_trading/platform/hermes_commands.py`
- `src/astock_trading/platform/llm_context.py`
- `src/astock_trading/reporting/discord.py`
- `src/astock_trading/strategy/scorer.py`
- `src/astock_trading/strategy/models.py`
- `src/astock_trading/strategy/decider.py`
- `src/astock_trading/templates/config/strategy.yaml`
- `src/astock_trading/templates/config/profiles/trend_swing.yaml`
- `tests/astock_trading/market/test_data_source_health.py`
- `tests/astock_trading/platform/test_agent_diagnostics_cli.py`
- `tests/astock_trading/platform/test_cli.py`
- `tests/astock_trading/platform/test_stock_analysis.py`
- `tests/astock_trading/strategy/test_scorer.py`
- `tests/astock_trading/strategy/test_decider.py`
- `tests/astock_trading/strategy/test_services.py`
- `tests/astock_trading/reporting/test_reporting.py`

---

## Task 1: Freeze Recommendation Baseline

**Purpose:** 先把当前“不推荐”的根因变成可回归指标，避免后续只凭体感判断策略是否变好。

**Files:**
- Create: `src/astock_trading/platform/recommendation_diagnostics.py`
- Modify: `src/astock_trading/platform/cli/diagnostics.py`
- Modify: `src/astock_trading/platform/cli/agent.py`
- Test: `tests/astock_trading/platform/test_recommendation_diagnostics.py`
- Test: `tests/astock_trading/platform/test_agent_diagnostics_cli.py`

- [ ] 写失败测试：`bin/trade diagnose recommendations --json` 输出候选池、入场信号、最近决策、数据源、买入窗口、策略阈值五段结构。
- [ ] 输出必须包含 `root_causes`，至少能拆出 `core_pool_empty`、`entry_signal_insufficient`、`buy_window_closed`、`strategy_threshold_strict`、`candidate_refresh_required_before_next_window`。
- [ ] 输出必须包含 `actionability`，区分 `正式模拟承接`、`试买意向跟踪`、`观察候选复核`。
- [ ] 在 `atrade commands --json` 注册 `diagnose_recommendations`，标记 `risk_level=read_only`。
- [ ] 运行：

```bash
.venv/bin/pytest tests/astock_trading/platform/test_recommendation_diagnostics.py tests/astock_trading/platform/test_agent_diagnostics_cli.py -q
bin/trade diagnose recommendations --json
bin/trade commands --json
```

**Expected JSON shape:**

```json
{
  "diagnostic": "recommendations",
  "status": "blocked",
  "root_causes": [
    {"type": "core_pool_empty", "severity": "high", "summary": "核心候选池为空"},
    {"type": "entry_signal_insufficient", "severity": "high", "summary": "当前候选暂无入场信号"}
  ],
  "actionability": {
    "formal_buy_ready": false,
    "trial_tracking_available": true,
    "watch_review_available": true
  },
  "next_actions": [
    {"command": "atrade screener refresh --json", "risk_level": "read_only"}
  ]
}
```

---

## Task 2: Add Tushare Paid Primary Provider

**Purpose:** 用 Tushare SDK 作为 A 股日线、复权 K 线、指数、财务、资金流和常规增强接口主源，MX 作为行情/选股主源，AkShare/Baidu/BaoStock/OpenCli/Mootdx 作为 fallback。该任务只提升数据稳定性，不承诺直接产生 `BUY`。

**Files:**
- Create: `src/astock_trading/market/tushare_adapters.py`
- Modify: `src/astock_trading/market/adapters.py`
- Modify: `src/astock_trading/market/protocols.py`
- Modify: `src/astock_trading/platform/service_factory.py`
- Modify: `src/astock_trading/platform/cli/data_sources.py`
- Modify: `src/astock_trading/market/health.py`
- Test: `tests/astock_trading/market/test_tushare_adapters.py`
- Test: `tests/astock_trading/platform/test_service_factory.py`
- Test: `tests/astock_trading/market/test_data_source_health.py`

- [ ] 支持环境变量：优先 `ASTOCK_TUSHARE_TOKEN`，兼容 `TUSHARE_TOKEN`；缺失 token 时 provider 状态为 `enabled=false`，不得报错。
- [ ] 使用官方 `tushare` SDK，延迟初始化 `tushare.pro_api(token)`，避免 token 进入日志或测试 fixture。
- [ ] adapter 初期接入常规积分接口：
  - `TushareMarketAdapter`：`daily`、`pro_bar`、`index_daily`。
  - `TushareFinancialAdapter`：`daily_basic`、`fina_indicator`。
  - `TushareFlowAdapter`：`moneyflow`。
  - `TushareMarketAdapter` 增强方法：`stock_basic`、`top_list`、`share_float`、`hk_hold`。
- [ ] provider 顺序建议：
  - 行情/K 线：`TushareMarketAdapter -> MXMarketAdapter -> 其他 fallback`。
  - 财务：`TushareFinancialAdapter -> TencentFinancialAdapter -> AkShare`。
  - 资金流：`TushareFlowAdapter -> BaiduFundFlowAdapter -> AkShareFlowAdapter`。
- [ ] 每次 Tushare 成功观测由 `MarketService` 写入对应 `market_observations`；失败经 `SourceRouter` 或 provider 失败记录进入结构化诊断。
- [ ] `data-sources diagnose --json` 增加 `optional_providers.tushare`：

```json
{
  "provider": "tushare",
  "enabled": true,
  "token_present": true,
  "checked_endpoints": {
    "daily": "ok",
    "moneyflow": "ok",
    "fina_indicator": "permission_denied"
  },
  "permission_note": "以当前 token 实测结果为准"
}
```

- [ ] 运行：

```bash
.venv/bin/pytest tests/astock_trading/market/test_tushare_adapters.py tests/astock_trading/platform/test_service_factory.py tests/astock_trading/market/test_data_source_health.py -q
bin/trade data-sources diagnose --json
```

---

## Task 3: Separate Human Recommendation From Formal Buy

**Purpose:** 让系统“有东西可推荐给人看”，但仍不把弱推荐变成自动买入。

**Files:**
- Modify: `src/astock_trading/platform/recommendation_diagnostics.py`
- Modify: `src/astock_trading/platform/agent_diagnostics.py`
- Modify: `src/astock_trading/platform/paper_trial.py`
- Modify: `src/astock_trading/platform/stock_analysis.py`
- Modify: `src/astock_trading/platform/hermes_commands.py`
- Modify: `src/astock_trading/platform/llm_context.py`
- Modify: `src/astock_trading/reporting/discord.py`
- Test: `tests/astock_trading/platform/test_recommendation_diagnostics.py`
- Test: `tests/astock_trading/platform/test_stock_analysis.py`
- Test: `tests/astock_trading/reporting/test_reporting.py`

- [ ] 新增推荐层级字段，使用中文展示：
  - `formal_buy_ready`：正式买入可承接，仅来自 `core + BUY + 新鲜评分 + 买入窗口 + 风控`。
  - `trial_buy_watch`：试买意向跟踪，来自 `TRIAL_BUY` 或入场信号接近买入线。
  - `strong_watch`：强观察，来自高分 `watch` / `radar`，但缺核心池或入场信号。
  - `positive_review_watch`：影子复盘转强，来自 `paper trial-review` 正向证据且仍在当前候选池。
- [ ] `opportunity`、`diagnose flow`、`llm-context --mode close` 顶层保留这组 recommendation tiers，避免 agent 只能看到“无买入”。
- [ ] Discord 机会卡必须展示：
  - 当前无正式买入的阻断项。
  - 2-5 个“继续盯”的候选，按 `core > watch > radar > 已移出候选池`、当前入场信号、正向复盘、分数排序。
  - 每只候选的复核命令：`atrade stock analyze CODE --json`。
- [ ] 保留安全断言：弱推荐不得写 `manual_trade.requested`，不得调用 `auto_trade` 下单路径。
- [ ] 运行：

```bash
.venv/bin/pytest tests/astock_trading/platform/test_recommendation_diagnostics.py tests/astock_trading/platform/test_stock_analysis.py tests/astock_trading/reporting/test_reporting.py -q
bin/trade diagnose flow --json
bin/trade opportunity --json
bin/trade llm-context --mode close --json
```

---

## Task 4: Expand Conservative Entry Routes

**Purpose:** 当前入场信号为 0，说明路线过窄。新增更贴近趋势波段的保守路线，让“能解释的入场信号”增加，但仍受评分和风控约束。

**Files:**
- Modify: `src/astock_trading/strategy/scorer.py`
- Modify: `src/astock_trading/strategy/models.py` if route evidence needs extra fields
- Modify: `src/astock_trading/templates/config/strategy.yaml`
- Modify: `src/astock_trading/templates/config/profiles/trend_swing.yaml`
- Test: `tests/astock_trading/strategy/test_scorer.py`
- Test: `tests/astock_trading/strategy/test_services.py`

- [ ] 新增 `pullback_to_ma20`：均线回踩转强。
  - 条件建议：收盘价在 MA20 上方或轻微回踩后收回，MA20 斜率为正，RSI 40-68，量比 0.8-2.5，5 日动量不为负，硬否决为空。
- [ ] 新增 `volume_breakout_followthrough`：放量突破延续。
  - 条件建议：当日涨幅大于配置阈值、量比大于配置阈值、收盘接近日内高位、价格在 MA5/MA20 上方，RSI 不过热。
- [ ] 强化已有 `flow_confirmed_trend`：资金确认趋势路线允许相对量比略低，但要求资金、趋势斜率、动量同时成立。
- [ ] 新增 `sector_relative_strength_watch`：板块相对强势观察路线，默认只形成观察/试买意向，不直接形成正式 `BUY`。
- [ ] 每条路线都必须输出 `display_name`、`family`、`entry_signal`、`confidence`、`evidence`，中文展示不得暴露内部字段名。
- [ ] 写测试覆盖：
  - 路线条件满足时 `entry_signal=True` 或观察路线进入 `strategy_routes`。
  - RSI 过热、数据质量降级、关键字段缺失时不产生正式入场。
  - 路线证据能同步到 `decision.suggested`。
- [ ] 运行：

```bash
.venv/bin/pytest tests/astock_trading/strategy/test_scorer.py tests/astock_trading/strategy/test_services.py -q
```

---

## Task 5: Tune Candidate Pool Without Lowering Formal Buy

**Purpose:** 解决 `core_pool_empty`，但不简单把低质量票推入核心池。优化候选池是为了扩大可跟踪范围和提高晋级机会，正式买入仍由 `BUY` 决策控制。

**Files:**
- Modify: `src/astock_trading/platform/cli/screener.py`
- Modify: `src/astock_trading/reporting/projectors.py` if projection needs new fields
- Modify: `src/astock_trading/templates/config/strategy.yaml`
- Modify: `src/astock_trading/templates/config/profiles/trend_swing.yaml`
- Test: `tests/astock_trading/platform/test_screener_governance.py`
- Test: `tests/astock_trading/platform/test_cli.py`

- [ ] 保持 `promote_min_score=6.0` 和 `promote_streak_days=2` 作为正式核心晋级主线。
- [ ] 增加“入场路线优先重评”：`screener refresh` 在预算有限时先重评当前候选池、最近有入场路线的候选、正向影子复盘候选，再补热点召回。
- [ ] 增加“推荐池”概念或输出层汇总，不要求写入新的交易池层级：
  - `recommendation_watch_min_score` 建议 4.8-5.0，仅用于展示继续观察。
  - `entry_signal_watch_min_score` 建议 5.2-5.5，用于展示“有入场路线但未达买入线”。
  - 这两个阈值不得影响 `core` 晋级和 `BUY` 决策。
- [ ] 对 L2 舆情/行业缺失不直接淘汰候选；L1 行情、技术、财务、资金流缺失仍应降级并阻断买入。
- [ ] `diagnose flow` 的候选池阶段展示：
  - `core_count`、`watch_count`、`radar_count`。
  - `recommendation_watch_count`。
  - `entry_signal_count`。
  - “为什么没有 core”的结构化原因。
- [ ] 运行：

```bash
.venv/bin/pytest tests/astock_trading/platform/test_screener_governance.py tests/astock_trading/platform/test_cli.py -q
bin/trade screener refresh --json
bin/trade diagnose flow --json
```

---

## Task 6: Add Shadow Review Promotion Signals

**Purpose:** 已经有 `paper trial-plan` 和 `paper trial-review`，下一步让正向影子复盘成为“人工复核优先级”，而不是自动晋级。

**Files:**
- Modify: `src/astock_trading/platform/paper_trial.py`
- Modify: `src/astock_trading/platform/agent_diagnostics.py`
- Modify: `src/astock_trading/platform/recommendation_diagnostics.py`
- Test: `tests/astock_trading/platform/test_agent_diagnostics_cli.py`
- Test: `tests/astock_trading/platform/test_manual_followup.py`

- [ ] 影子复盘正向候选进入 `positive_review_watch`，排序规则沿用：当前 `core` 优先、同层级有当前入场信号优先、再看收益率。
- [ ] 若正向复盘候选已移出候选池，只能作为复核证据，不能压过当前池内候选。
- [ ] 若正向复盘候选价格异常，标记“价格异常”，不进入推荐层。
- [ ] 输出下一步只读命令：

```json
{
  "type": "review_positive_trial_candidate",
  "command": "atrade stock analyze 301196 --json",
  "risk_level": "read_only"
}
```

- [ ] 运行：

```bash
.venv/bin/pytest tests/astock_trading/platform/test_agent_diagnostics_cli.py tests/astock_trading/platform/test_manual_followup.py -q
bin/trade paper trial-review --json
bin/trade diagnose recommendations --json
```

---

## Task 7: Keep Auto-Trade Safety Boundaries Explicit

**Purpose:** 优化推荐后，最容易引入的风险是把“推荐”误接到订单路径。必须用测试锁死。

**Files:**
- Modify: `src/astock_trading/strategy/decider.py` only if needed
- Modify: `src/astock_trading/strategy/service.py` only if needed
- Modify: `src/astock_trading/pipeline/auto_trade.py`
- Modify: `src/astock_trading/platform/paper_trial.py`
- Test: `tests/astock_trading/strategy/test_decider.py`
- Test: `tests/astock_trading/strategy/test_services.py`
- Test: `tests/astock_trading/platform/test_auto_trade.py` or existing auto-trade tests

- [ ] 测试 `TRIAL_BUY` 不产生 `manual_trade.requested`。
- [ ] 测试 `strong_watch` / `positive_review_watch` 不被 `auto_trade` 识别为可买入。
- [ ] 测试只有 `core + BUY + 新鲜评分 + 买入窗口 + 风控通过` 才能进入模拟承接。
- [ ] `paper auto-readiness --json` 阻断项保留中文摘要，且顶层状态不被推荐层覆盖。
- [ ] 运行：

```bash
.venv/bin/pytest tests/astock_trading/strategy/test_decider.py tests/astock_trading/strategy/test_services.py tests/astock_trading/platform -k "auto_trade or auto_readiness or trial_buy" -q
bin/trade paper auto-readiness --json
```

---

## Task 8: Report and Operator UX

**Purpose:** 用户最关心的是“为什么一个月没推荐、下一步看什么”。报告面必须直接回答，而不是让人解 JSON。

**Files:**
- Modify: `src/astock_trading/reporting/discord.py`
- Modify: `src/astock_trading/platform/hermes_commands.py`
- Modify: `src/astock_trading/platform/llm_context.py`
- Modify: `src/astock_trading/platform/stock_analysis.py`
- Test: `tests/astock_trading/reporting/test_reporting.py`
- Test: `tests/astock_trading/platform/test_stock_analysis.py`

- [ ] Discord 机会卡新增四段：
  - “正式买入状态”：是否可承接、阻断项。
  - “继续盯”：弱推荐候选。
  - “影子复盘”：正向/异常/已移出候选池。
  - “下一步命令”：只读复核命令和刷新命令。
- [ ] 所有用户可见文案使用中文，不直接展示 `BUY`、`TRIAL_BUY`、`entry_signal` 等内部字段名。
- [ ] `stock analyze CODE --json` 对弱推荐候选展示：
  - 当前层级。
  - 评分拆解。
  - 入场路线。
  - 缺口：距离核心晋级、距离买入线、缺失数据、风控阻断。
- [ ] 运行：

```bash
.venv/bin/pytest tests/astock_trading/reporting/test_reporting.py tests/astock_trading/platform/test_stock_analysis.py -q
bin/trade stock analyze 301196 --json
bin/trade opportunity --json
```

---

## Task 9: Rollout and Measurement

**Purpose:** 用 5 个交易日验证优化是否真的改善“可推荐性”，而不是当天刚好有/没有行情。

**Files:**
- Modify: `docs/operations/RUNBOOK.md` if present
- Modify: `docs/architecture/DATA_SOURCE_STABILITY_REFACTOR.md`
- Modify: `docs/architecture/AGENT_ARCHITECTURE_CONTEXT.md`

- [ ] 部署前运行全量质量门禁：

```bash
.venv/bin/ruff check src tests
.venv/bin/pytest tests/astock_trading/market tests/astock_trading/strategy tests/astock_trading/platform tests/astock_trading/reporting -q
```

- [ ] 部署后每日记录这些只读命令：

```bash
atrade data-sources diagnose --json
atrade screener refresh --json
atrade diagnose recommendations --json
atrade diagnose flow --json
atrade paper auto-readiness --json
atrade paper trial-review --json
```

- [ ] 5 个交易日验收指标：
  - `data_sources.provider_failures.unresolved_recent` 不持续扩大。
  - Tushare enabled 时，`optional_providers.tushare.checked_endpoints` 有明确 `ok` / `permission_denied` / `disabled_optional`。
  - `candidate_summary.total` 不长期为 0。
  - `recommendation_watch_count` 或 `positive_review_watch` 能稳定给出人工复核对象。
  - `current_entry_signals` 较优化前增加，且每个信号有中文路线证据。
  - `manual_trade.requested` 仍只由正式买入产生。
  - `paper auto-readiness` 仍能清楚展示买入窗口、核心池、新鲜买入意向和 profile 阻断。

---

## Implementation Order

1. Task 1：先做推荐诊断基线。
2. Task 2：接 Tushare SDK 付费主源，并在诊断中展示主源/fallback 策略。
3. Task 3：分离推荐可见层和正式买入。
4. Task 4：扩展保守入场路线。
5. Task 5：优化候选池刷新和推荐池输出。
6. Task 6：接入影子复盘转强信号。
7. Task 7：锁死自动交易安全边界。
8. Task 8：更新 Discord/Hermes/LLM/个股分析展示。
9. Task 9：5 个交易日观测和文档固化。

## Non-Goals

- 不接真实券商实盘接口。
- 不自动切换 `ASTOCK_CONFIG_PROFILE`。
- 不为了每天产生股票推荐而降低正式 `BUY` 阈值。
- 不把 Tushare token 作为系统启动硬依赖，也不把 token 写入代码、文档、测试或日志。
- 不把热点榜、舆情或影子复盘单独当作买入依据。

## Final Verification Checklist

- [ ] `bin/trade diagnose recommendations --json` 能解释没有推荐的根因。
- [ ] `bin/trade data-sources diagnose --json` 能展示 Tushare 状态、SDK、已配置常规接口和不默认假设的独立权限接口。
- [ ] `bin/trade diagnose flow --json` 同时展示正式承接状态和弱推荐候选。
- [ ] `bin/trade opportunity --json` 在无 `BUY` 时仍能给出继续盯的候选和复核命令。
- [ ] `bin/trade paper auto-readiness --json` 没有被弱推荐绕过。
- [ ] `TRIAL_BUY` / `strong_watch` / `positive_review_watch` 不写 `manual_trade.requested`。
- [ ] ruff 和聚焦 pytest 通过。
