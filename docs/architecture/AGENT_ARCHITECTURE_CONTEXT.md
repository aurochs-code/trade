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
atrade commands --json
atrade doctor --json
atrade health --json
atrade opportunity --json
atrade opportunity-watch --json
atrade llm-context --mode close --json
atrade paper auto-readiness --json
atrade paper trial-plan --json
atrade paper trial-plan --record --json
atrade paper trial-review --json
atrade strategy profile-activation --target trend_swing --json
atrade diagnose flow --json
atrade diagnose schedule --json
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

事件证据要求：

- `decision.suggested` 不能只保存 `BUY/WATCH/CLEAR` 和分数；必须把来源评分里的
  `entry_signal`、`primary_strategy_route_label`、`strategy_routes`、`technical_detail`
  和 `data_quality` 同步写入，保证单看决策事件也能解释买入意向为什么出现。
- `manual_trade.requested` 要保留同一套入场证据，Discord 人工确认卡、事件审计和
  `paper auto-readiness.fresh_buy_signal.top` 才不需要临时反查评分事件。

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
- 逐票 L1 覆盖不足：pipeline 可以完成，但 `atrade suggest --json` /
  `atrade propose-plan --json` 应返回 `data_source_blockers` 并暂停新增交易判断
- 候选池为空且核心源健康：应表述为“暂无合格候选”，继续观察，不降低买入线
- `radar` 是“强势观察”层，来自接近观察线或热点召回的候选；它只用于跟踪和提醒，
  不等同于 `watch` / `core`，也不能触发模拟买入
- `paper trial-plan` 只把 `watch` / `radar` / `core` 转成影子试运行清单和复核命令，
  不提交模拟盘订单，不绕过 `core` / `BUY` / 人工确认边界；加 `--record` 时只写
  `paper.trial.recorded` 影子试运行事件，供后续复盘和 agent 跟踪
- `paper trial-plan --json` 本身必须输出 `candidate_summary` 和 `current_entry_signals`，
  让 agent 直接看到影子候选数量、核心/观察/强势观察分布和当前入场信号，不要只靠遍历
  `candidates` 列表推断候选流是否形成
- `paper trial-review` 复盘 `paper.trial.recorded` 影子候选的起始价、当前价和表现状态，
  只输出复核证据和下一步命令，不自动晋级、不提交模拟盘订单
- `paper trial-review --json` 本身必须输出 `review_summary` 和 `positive_reviews`；
  `positive_reviews` 只是正向影子复盘证据，排序按当前可操作性优先，不能触发自动晋级或
  模拟下单
- 影子候选复盘时必须同时展示当前候选池状态；如果候选已从 `radar` / `watch` /
  `core` 掉出，应标记为“已移出候选池”和候选变化，不能只按正收益提示人工晋级
- `diagnose flow` / `llm-context --mode close` 展示影子复盘时，必须输出近期
  `review_summary` 和 `positive_reviews`。正收益候选复核顺序按当前可操作性排序：
  当前 `core` 优先于 `watch` / `radar` / 已移出候选池；同层级里有当前入场信号的优先；
  再看收益率。这样影子链路会优先指向仍在核心池且有入场证据的票，而不是单纯按事件时间。
- `diagnose flow` / `llm-context --mode close` 里的影子复盘事件必须同时带
  `event_id`、`evidence_id` 和 `event_type`。`evidence_id` 要和顶层
  `evidence_registry` 可匹配，保证 LLM 摘要引用影子试运行证据时不会被证据门禁拒绝。
- `opportunity` 的主状态和下一步只能被仍在当前候选池内的影子正收益候选占用；
  已移出候选池的影子正收益只作为复核证据和阻断说明，不能压过当前核心/观察候选流
- `opportunity.positive_trial_candidates` / `active_positive_trial_candidates` 的排序口径要和
  `diagnose flow` 一致：当前 `core` 优先、同层级里当前入场信号优先、再看收益率；
  不要让单纯涨幅更高但只在观察层的影子候选压过当前核心入场候选
- `opportunity` 和 Discord 机会卡的摘要必须显式展示 `core` 与 `watch` 的数量；
  不要把包含核心候选的候选池统称为“观察候选”，否则会掩盖最接近模拟承接的层级
- `opportunity.candidate_summary` 必须结构化展示候选池总数、核心/观察/强势观察数量、
  当前入场信号数量，以及各层级最高分候选和 `atrade stock analyze CODE --json`
  复核命令；agent 和报告不应再靠拆自然语言 summary 来判断候选流是否形成
- `opportunity.current_entry_signals` 必须在顶层保留当前候选池里的入场信号明细；
  即使同时存在 profile 阻断、过期买入意向或下个窗口计划，agent 也应能直接看到
  “核心候选已有入场信号，但还缺同日新鲜买入意向”这条链路。
- `opportunity.watch_candidates` / Discord 机会卡的候选行必须复用最新评分事件补全
  `entry_signal`、`primary_strategy_route_label`、`technical_detail` 等入场证据；不要只展示
  候选池投影里的层级和分数，否则强势核心候选会被误读成普通观察票。
- 如果过期买入意向对应的股票仍在当前 `core` 候选池，`opportunity` 应优先指向
  `atrade paper auto-readiness --json`，让 agent 看到“核心候选 + 买入证据 + 窗口拦截”
  这条模拟承接链路，而不是退回普通 `paper trial-plan`
- 影子复盘遇到同日价格跳变超过护栏时，应标记为“价格异常”并先核查行情证据；
  不能把异常收益当作“表现为正”，也不能据此晋级候选或触发模拟买入
- 行情 quote 写入前要和同票日 K 最新收盘价做一致性校验；明显背离的 quote
  应跳过并继续 fallback，避免 provider 串价污染候选评分和影子复盘
- `screener refresh` 是调度型深度刷新，默认用 `strategy.screening.refresh_scan_limit`
  控制逐票评分预算；手工放大全量刷新时显式传 `--limit`
- `screener refresh` 在评分预算有限时必须优先重评当前候选池，再补热点和主召回；
  这样旧候选不会因为热榜占满预算而继续停留在过期分数/过期层级。
- `pool_management.entry_signal_promote_streak_days` 只缩短“已有可执行入场路线”的高分票晋级
  核心候选所需连续确认天数；没有入场路线的高分票仍要留在观察层并记录阻断原因。
- `flow_confirmed_trend` 是趋势波段下的资金确认路线：当相对量比略低但金叉、强资金、
  高成交额、趋势斜率和动量同时成立时，可形成入场信号；模拟承接仍必须通过
  `core` / `BUY` / profile / 买入窗口 / 风控门禁。
- 盘中自动模拟的关键顺序是“候选刷新 -> 决策事件 -> auto_trade”。`14:12` 的
  `a_stock_intraday_execution_cycle_silent.sh` 用于补足 14 点后才出现的买入意向；
  `14:24` 的 `a_stock_pipeline_auto_trade_silent.sh` 在买入窗口结束前再做一次兜底承接；
  `auto_trade.dry_run=false` 时会提交 MX 模拟盘委托。运行前可用
  `atrade paper auto-readiness --json` 只读检查当前是影子记录模式还是 MX 模拟盘委托模式，
  以及买入侧被时间窗口、核心候选、买入意向或异常保护拦截的具体原因。
- `paper auto-readiness` 的顶层 `status` 必须反映买入侧是否真正可承接；如果
  `buy_side.status=waiting_window`，顶层也应是 `waiting_window`，并用中文摘要说明
  “已有买入意向但当前不在模拟买入窗口，不会提交模拟买入”，不要只把配置模式显示成 `ready`
- 非交易日检查时，候选池评分超过 `max_age_hours` 不能简单展示成“候选池评分已过期”。
  `paper auto-readiness` 应把 `candidate_pool.freshness_status` 标为
  `refresh_required_before_next_window`，阻断项使用“下个买入窗口前需要重新刷新候选评分”；
  这表示下个窗口前必须重新评分，不等同于当前数据源坏了或候选池失效。
- `paper auto-readiness` 还必须展示 `execution_profile`：如果当前仍是 `default`
  且配置里混有趋势波段、短线延续和回测 preset，应返回 `profile_review_required`
  阻断项，要求人工确认 `ASTOCK_CONFIG_PROFILE=trend_swing` 后再自动模拟；agent
  不能自行切换执行 profile。没有可承接买入意向时，顶层状态应优先暴露
  `profile_review_required` 而不是泛化成 `blocked`；但已有新鲜买入意向且仅错过买入窗口时，
  顶层仍保持 `waiting_window`，同时在 blockers 中保留 profile 审批项。
- `atrade strategy profile-activation --target trend_swing --apply-env --yes --json`
  是唯一允许把 profile 写入运行 `.env` 的 CLI 入口；必须由用户明确批准后才可执行。
  该命令只写 `ASTOCK_CONFIG_PROFILE`，会备份 `.env` 并追加
  `strategy.profile_activation.applied`，不改 Hermes 调度、不提交订单。
- `atrade strategy profile-activation --target trend_swing --json` 必须在顶层展示
  `approval_gate`、`after_approval_preview` 和 `post_approval_checklist`：先让用户批准写入运行 profile，再执行
  `diagnose schedule`、`paper auto-readiness`、`risk trial-guard` 等只读核查。它可以展示
  `run-pipeline auto_trade` 作为后续模拟承接命令，但必须标明该命令 `writes_order=true`、
  `requires_user_approval=true`，并且只能在 profile、调度、同日买入意向、买入窗口和护栏都
  通过后单独审批执行，不能成为 profile 写入后的自动下一步。
- `profile-activation.after_approval_preview` 必须和 `opportunity` / `agent-context`
  使用同一套只读口径，直接列出当前 `paper auto-readiness` 里除 profile 以外仍会阻断
  模拟承接的原因，例如“当前不在模拟买入窗口”“下个买入窗口前需要重新刷新候选评分”
  和“没有新鲜买入意向”。这个字段只做预判，不写 `.env`、不查账户、不提交模拟委托；
  如果出现 `candidate_refresh_required_before_next_window`，审批页也要让用户知道下个窗口前
  仍需依赖盘中候选刷新和同日买入意向重新生成。
- `auto_trade` 买入侧只承接当前交易日、且不晚于 `auto_trade.buy_window.end` 产生的
  买入意向。收盘后或错过买入窗口后生成的买入意向只作为复核证据，不应跨日自动提交
  MX 模拟盘委托；排查时看 `atrade diagnose strategy --json` 里的“可用买入意向”和
  `atrade paper auto-readiness --json` 的买入侧状态。
- `paper auto-readiness` 如果没有新鲜买入意向，但 24 小时内存在 `BUY` 决策，应通过
  `recent_unusable_buy_signal` 说明它为什么不能承接，例如非交易日、非当前交易日或晚于买入窗口；
  该字段要用 `source_score_event_id` 回补入场信号、策略路线和技术证据，避免把历史买入意向误读成
  “系统没看到入场信号”。顶层 `summary` 也要直接写出近期买入意向不可承接的原因，
  不要只显示“没有新鲜买入意向”。
- `digest`、`opportunity`、`opportunity-watch`、`diagnose flow` 和 `llm-context --mode close` 也必须透传
  `recent_unusable_buy_signal`。这些入口是人和 agent 日常先看的摘要层，不能只显示
  “过期待复核”或“没有新鲜买入意向”；如果近期 `BUY` 因非交易日、跨日或晚于买入窗口不可承接，
  顶层摘要要直接写出数量、最高分候选和不可承接原因。
- `paper auto-readiness` 在没有同日新鲜买入意向时，也要把当前 `core` 候选里已经触发
  入场信号的票展示在 `buy_side.current_entry_signals`，并用 `signal_gap` 指向只读的
  `atrade stock analyze CODE --json` 单票复核。这里的入场信号只是当前候选证据，
  不能等同于 `BUY` 买入意向，也不能绕过同日决策事件、profile 审批、买入窗口和风控门禁。
- `risk trial-guard` 是试运行前的只读风险护栏入口。除展示首轮试运行仓位上限外，
  还应带出当前候选池摘要、当前入场信号、运行 profile 阻断和下一步只读命令；它不提交
  模拟盘订单，也不记录真实成交。agent-context 的 follow-up 应包含
  `atrade risk trial-guard --json`，让 operator 在看到入场信号后自然检查试运行上限。
- `risk trial-guard` 的顶层 `status` 不能在存在阻断项时仍显示 `ok`。如果运行 profile
  仍需人工确认，应返回 `profile_review_required`，顶层 `summary` 要同时写出阻断原因和
  候选池摘要；`candidate_summary` 与 `current_entry_signals` 也应在顶层保留，避免 agent
  只读取顶层字段时误判试运行护栏已通过。
- 周末等非交易日没有买入窗口；人工确认单应进入“过期待复核”，`paper auto-readiness`
  和 `diagnose strategy` 也不能把同一自然日凌晨的旧 `BUY` 事件算成新鲜买入意向。
- `auto_trade` 执行层不能只依赖 `paper auto-readiness` 的外部预检；买入前置诊断也要检查
  `execution_profile.safe_to_auto_apply`。若 profile 仍需人工确认，应写入
  `auto_trade.diagnostic` / `auto_trade.summary`，并禁止模拟买入。
- `diagnose flow` 是 agent 排查“强势行情下为什么没有可观察、可模拟候选流”的首选只读入口；
  它汇总候选池、评分/买入意向、`opportunity`、`paper auto-readiness`、Hermes 调度、
  最新 `auto_trade.summary` 和影子试运行状态。不要用它绕过 `core` / `BUY` /
  人工确认边界，也不要在该命令之外重新拼一套不同口径的候选流判断。
- `diagnose flow --json` 顶层必须直接输出 `candidate_summary` 和
  `current_entry_signals`，并与 `strategy.candidate_flow` 内部字段一致；agent 排查时
  不应只靠自然语言 `summary` 或嵌套列表猜测当前候选流。`current_entry_signals`
  要带 `technical_detail` 和 `data_quality`，让复核能看到入场信号证据而不只看到路线名。
- `screener explain` 保留近期 `score.calculated` / `decision.suggested` 漏斗用于诊断，
  但跟进清单必须合并当前 `projection_candidate_pool`。当当前候选池分数、层级或入场状态
  与历史评分事件不一致时，`follow_up`、`top_scores` 和 `near_misses` 应优先展示当前候选池
  口径，并用 `score_source=current_candidate_pool`、`pool_tier_label` 标出来源；否则 agent
  会被旧评分带偏，误判为候选流没有形成。
- `screener explain` 中不在当前候选池里的历史评分事件必须标出 `score_source=score_event`、
  `score_event_id` 和 `scored_at`。如果历史事件曾有 `entry_signal=true`，只能给出
  “历史入场信号需重新评分入池”的 `recall_hint`，下一步指向 `atrade screener refresh --json`；
  不要把它写成当前可模拟候选，也不要用旧入场信号绕过候选池刷新。
- `diagnose strategy` / `diagnose flow` 判断“当前是否有入场信号”时必须以当前候选池为准；
  历史评分里的旧 `entry_signal=true` 只能作为 `latest_scores_*` 诊断统计，不能把它描述成
  当前可承接入场信号或新鲜买入意向。
- `diagnose strategy` 顶层必须提供中文 `summary`，把候选池数量、核心/观察/强势观察分布
  和 `actionable_state.summary` 汇总在一起；当 `execution_profile.status=review_required`
  时，摘要还必须直接写出 profile 人工确认阻断。不要让 agent 只能从嵌套字段里拼状态结论。
- 当 `diagnose flow` 返回 `approval_gate.required=true` 时，它只是在只读 JSON 中暴露
  人工审批门槛。`approval_gate.apply_command` 需要用户明确批准后才能执行；agent 不得
  因为字段里出现 `--apply-env --yes` 就自行写 `.env`。
- 同一 payload 里的 `after_approval_preview` 只是审批后的只读预判：它会给出
  `ASTOCK_CONFIG_PROFILE=trend_swing atrade paper auto-readiness --skip-account --json`
  这类不写环境、不查账户的预览命令，并把当前 `paper auto-readiness` 中非 profile
  的剩余阻断列出来。它不能替代人工审批，也不能作为提交模拟委托的依据；审批后仍要重新跑
  `atrade paper auto-readiness --json` 和 `atrade diagnose schedule --json` 验证。
- `after_approval_preview` 不能只写“没有新鲜买入意向”。如果当前核心候选已有入场信号，
  或近期买入意向因非交易日、跨日、过窗不可承接，预演 payload 必须透传
  `current_entry_signals`、`signal_gap` 和 `recent_unusable_buy_signal`，并在摘要里说明
  profile 批准后仍需下个交易窗口重新形成同日买入意向。
- `diagnose flow` 的 `next_window_plan` 用于解释错过买入窗口后的下一次承接路径。
  它必须明确展示下个 `auto_trade.buy_window`、当前买入意向是否能承接到下个窗口、
  以及 13:40 / 14:00 / 14:12 / 14:24 等盘中刷新和模拟承接任务。今日买入意向默认不跨日
  自动提交；下个交易日必须重新形成同日 `BUY` 意向后，`auto_trade` 才能在窗口内承接。
  若这些盘中任务尚未首跑，`next_window_plan.first_run_verification` 必须列出待核查任务、
  是否影响模拟承接，以及只读核查命令 `atrade diagnose schedule --json`。
- `diagnose schedule` 是 profile 批准后的调度核查入口，不能只返回大列表。
  顶层必须有 `intraday_simulation`，直接汇总下个窗口 13:40 / 14:00 / 14:12 /
  14:24 等候选刷新和模拟承接步骤、关键任务数量、首次运行核查、profile 是否就绪、
  以及只读下一步命令；该入口只读，不补跑任务、不写环境、不提交模拟盘订单。
- `diagnose schedule --json` 还必须输出 `runtime_contract`，只读检查下个窗口模拟承接脚本
  是否存在、是否通过 `atrade` 入口执行、是否没有禁用 `.env` 加载。该检查只覆盖
  `next_window_simulation_scripts`，不要把 LLM 报告脚本等非模拟承接脚本误判为买入链路阻断。
- 如果 `diagnose strategy` 显示已有买入意向但错过买入窗口，且 `actionable_state`
  指向 `atrade diagnose schedule --json`，应先核查 Hermes `trading` profile 中
  13:40 / 14:12 / 14:24 / 13:45,15:45 等盘中候选、模拟承接和影子试运行任务是否
  当日实际运行；该命令还会通过 `runtime_profile` 只读暴露下次 Hermes/atrade 执行会
  使用的 `ASTOCK_CONFIG_PROFILE`。如果已经记录 `trend_swing` 激活计划但运行环境仍是
  `default`，应先让用户人工确认并设置 profile；agent 不能自行修改 `.env` 或调度。
- `opportunity` 是人读入口，必须跟上面的运行 profile 诊断一致：已有核心候选/买入意向
  但运行环境仍是 `default` 时，状态应为 `profile_review_required`，下一步指向
  `atrade strategy profile-activation --target trend_swing --json`，而不是继续让用户绕回
  `paper auto-readiness`。
- `opportunity` 和 Discord 机会卡也必须暴露同一套 `approval_gate` 与 `next_window_plan`：
  用户日常看到的卡片要明确“先人工确认 profile”“旧买入意向不会跨日自动提交”“下个窗口前
  依赖盘中刷新和模拟承接任务重新形成同日买入意向”，不能只把这些信息藏在
  `diagnose flow` 里。
- `opportunity.after_approval_preview` 必须和 `agent-context.operator_attention.after_approval_preview`
  使用同一套只读口径：人工批准 profile 后仍剩哪些非 profile 阻断、当前核心入场信号是否
  只是入场证据而非同日买入意向、近期买入意向为什么不可承接，以及后续只读预检/调度核查命令。
  机会卡和 Discord 展示这层信息时不得写 `.env`、不得提交模拟盘订单。
- `llm-context --mode close` 是收盘 LLM/Discord 复盘的取数入口。它的
  `close_review.simulation_flow` 必须复用 `diagnose flow` / `paper auto-readiness`
  口径，展示候选池、profile 审批、买入侧阻断、下个买入窗口和影子试运行复盘，避免收盘报告
  只写“没有交易”而漏掉已经形成的可观察、可模拟候选链路。profile 审批、自动交易摘要和
  影子试运行复盘都要带可进入 `evidence_registry` 的事件编号，保证最终 LLM 摘要能通过
  evidence_id 门禁。
- `llm-context --mode close` 的 `tomorrow_checklist` 不能只按 provider failure 总数排序。
  只有核心源不可用、逐票 L1 覆盖不足或 `data_source_blockers` 会阻断新增交易判断时，
  数据源复核才应排为 high priority；OpenCli 等非关键市场情报空结果要保留为 normal
  复核项，但不能压过 profile 审批、候选流和模拟承接链路。
- `close_review.simulation_flow.automation_schedule` 必须透传 `diagnose schedule`
  的 `runtime_contract` 和 `intraday_simulation` 摘要，包括下个窗口盘中候选刷新/模拟承接
  步骤、关键任务数量、首次运行核查、profile 是否就绪，以及模拟承接脚本是否会通过
  `atrade` 加载运行 `.env`。收盘报告不能只写“明天再看”，要能区分“脚本合约已就绪，
  只差人工确认 profile”与“脚本/调度本身还需要修复”，并指向
  `atrade diagnose schedule --json` 验证下个窗口承接任务。
- `llm-context --mode close` 的 `tomorrow_checklist` 遇到 profile 审批门时，默认
  `command` 只能给只读复核命令；同时必须输出明确的 `requires_user_approval` 标记、
  `safe_to_auto_apply=false`、人工批准后才能执行的
  `apply_command_after_approval`、`verify_command` 和 `writes_environment_after_approval`。
  这样报告能给运营闭环路径，但 agent 仍不能自行写 `.env`。
- `opportunity-watch` 不能只按候选 key 去重。候选池没有新增但 `opportunity.status`、
  `next_action` 或 `approval_gate` 变成需要处理时，应触发 `operator_action_required`
  提醒，并把下一步指向同一条只读复核命令；状态写入后再按同一动作 key 去重，避免重复刷屏。
  嵌套的 `opportunity` 摘要也必须同步携带 `approval_gate` 与 `next_window_plan`，
  让 Discord / Hermes 主动提醒和 `opportunity` 人读入口展示同一条下个窗口路径。
- `opportunity-watch` 的嵌套 `opportunity` 不能丢掉 `after_approval_preview`。主动提醒
  如果是因为 profile 审批或下个窗口路径变化而触发，Discord 卡片必须展示 profile 审批、
  审批后只读预演和下个买入窗口；否则用户只会看到“有动作需要处理”，看不到批准后仍需
  同日买入意向、调度首跑核查和不写环境/不提交订单边界。
- `digest` 是轻量入口，但不能把需要动作的状态压成 `ok`。如果有核心候选对应的过期或过窗
  买入意向，且运行 profile 仍需人工确认，`digest` 必须返回
  `status=profile_review_required` 和 `attention.command`，指向
  `atrade strategy profile-activation --target trend_swing --json`。
- `digest` 的主摘要不能只展示最新一条 `decision.suggested`。如果存在待人工确认或过期待复核
  买入意向，摘要和 `signal_focus` 应优先展示这条当前重点信号；`latest_decision` 只能作为
  原始最新事件证据保留，避免后续低分 `CLEAR` 把已有核心买入意向盖掉。
- `agent-context` 是 agent 的首个自检入口，除了安全入口和命令清单，还应通过
  `operator_attention` 暴露当前运行态的 `current_action`、`approval_gate`、
  `after_approval_preview`、`next_window_plan`、`runtime_contract` 和后续只读命令。
  `runtime_contract` 要来自 `diagnose schedule` 的同一套脚本合约检查，让 agent 能直接区分
  “脚本可读取 `.env`，只差人工确认 profile”与“脚本/调度本身需要修”。`after_approval_preview`
  必须复用 `diagnose flow` 的只读预演口径，默认不读取 MX 账户、不写环境、不提交订单。
  它只能提示下一步，不能因为 payload 中出现 `--apply-env --yes`、`record-buy` 或
  模拟盘命令就自行写环境、记录成交或提交订单。
- `commands` 是机器可读命令契约入口，必须标注参数、选项、`writes_state`、
  `writes_environment`、`writes_order`、`requires_user_approval` 和 `risk_level`。
  `agent-context.operator_attention.current_action` 和 `approval_gate` 会内联对应的
  `command_contract_id` / `*_command_contract`；agent 应优先读取这层契约来判断命令是
  只读复核、写状态、写环境还是需要人工批准。
- `commands` 必须覆盖候选流的真实后续动作：`stock analyze`、`risk trial-guard`、
  `screener refresh`、`paper trial-plan --record`、`paper trial-review --record` 和
  `run-pipeline auto_trade`。其中 `run-pipeline auto_trade` 可能提交 MX 模拟盘委托，
  必须标记 `writes_order=true` 和 `requires_user_approval=true`。
- `stock analyze` 会同时展示即时单股分析和候选池投影。若
  `candidate_pool_consistency.requires_pool_refresh=true`，说明两者已经错位，agent
  应先复核或运行 `atrade screener refresh --json` 重建候选池证据，不要把旧的
  `core` 层级直接当成可模拟买入依据。
- 但如果 `stock analyze` 显示 `candidate_pool_consistency.status=execution_gate_blocked`，
  且当前评分与核心候选一致、入场信号仍在，说明被大盘择时、profile、买入窗口或同日
  新鲜买入意向闸门拦住；这不是候选池过期。下一步应走只读 profile / 候选流 /
  模拟预检复核，不要把它误导成写状态刷新候选池。
- `stock analyze` 的大盘择时必须在实时指数无效时复用最近 `projection_market_state`。
  若 provider 只返回涨跌幅但价格、MA 均为 0，应视为无效指数数据并走投影回退；
  MySQL 查询 `projection_market_state.signal` 时要用反引号，避免保留字导致回退静默失败。
- `stock analyze` 的顶层 JSON 必须直接暴露 `code`、`name`、`score_total`、
  `action_label`、`entry_signal` 和中文 `summary`。不要只把买入意向和入场信号藏在
  嵌套的 `score` / `decision` 里，否则 agent 和 Discord 报告会误判为单票分析没有结论。
- `stock analyze` 的 `BUY` 只是只读即时单股判断，不写入 `decision.suggested`，也不等于
  已有可承接的同日买入意向。若 `paper auto-readiness` 显示
  `entry_signal_without_fresh_buy_intent`，单股摘要必须说明“尚未形成可承接的同日买入意向”，
  并在 `execution_readiness` 里透出买入侧阻断，避免用户把即时分析误读成自动模拟已经可下单。
- `stock analyze` 的 `next_action` 不能在可操作状态下留空：观察候选且入场信号未触发时，
  应指向只读的影子试运行清单复核；只有 `core + BUY + 当日窗口` 对齐后才进入
  `paper auto-readiness` 或后续人工确认链路。
- 主评分器必须把短续接力模型接入 `strategy_routes`：干净的强势延续形态应输出
  `short_continuation` 路线和入场信号，让后续候选晋级、买入意向和模拟承接有真实信号源。
  已明显拉开的票仍应被量能、RSI、乖离率、当日涨幅等条件拦在观察层，不能为了追热点
  直接放宽买入线。
- `stock analyze` 遇到高分但没有入场信号时，必须在 `findings` 中解释具体阻断，
  例如未出现金叉、量能确认不足、RSI 过热、乖离率过高或当日涨幅过大。不要只写
  “入场信号未触发”，否则用户无法区分“系统没工作”和“系统看到了但拒绝追高”。
- 真实场景验证时优先看 `atrade data-sources diagnose --json`、`atrade data-sources status --json`、`atrade diagnose health --json`、`atrade health --json`

## 人工确认边界

`BUY` 决策不等于真实买入。

正确链路是：

1. 策略产生 `decision.suggested`
2. `BUY` 触发 `manual_trade.requested`
3. Discord / 报告展示为“买入意向”或“待人工确认”
4. 人工确认后使用 `atrade record-buy CODE SHARES PRICE --yes --json`
   记录成交；如券商 App 显示含费用成本价，追加 `--cost-price COST_PRICE`
5. 本地写入 `order.*`、`position.*`，再重建投影
6. 如果已补录持仓后才发现券商成本价不同，使用
   `atrade adjust-position-cost CODE --cost-price COST_PRICE --yes --json`
   追加 `position.cost_basis_adjusted` 校准本地总成本，不下单
7. `ExecutionService.audit_manual_trade_consistency(order_id)` 可审计本地记录是否一致

待确认买入意向超过 `manual_confirmation.pending_max_age_hours`，或已经错过
`auto_trade.buy_window`，会被视为“过期待复核”。它仍保留在事件链里，但不再作为
`suggest` / `opportunity` 的新鲜待确认阻断项，避免压住后续观察候选和影子试运行。
查看用 `atrade manual-trades list --status stale --json`；确认结案用
`atrade manual-trades expire-stale --yes --json`，该命令只追加
`manual_trade.expired` 审计事件，不会下单。

不要把 `radar`、`watch`、`core`、`BUY` 混为同一个交易强度。弱信号应表达为“强势观察”“观察”或“等待”，不要包装成看多结论。

## 开发定位规则

常见需求的首读位置：

- 命令面、JSON 输出、MCP：`src/astock_trading/platform/cli/`、`src/astock_trading/platform/mcp_server.py`
- 服务组装：`src/astock_trading/platform/service_factory.py`
- pipeline：`src/astock_trading/platform/pipeline_runner.py`、`src/astock_trading/pipeline/`
- 数据源健康、覆盖率诊断和 provider 路由：`src/astock_trading/market/health.py`、
  `src/astock_trading/platform/data_source_diagnostics.py`、
  `src/astock_trading/market/source_router.py`、`src/astock_trading/platform/pipeline_policy.py`
- 数据源稳定性重构：`docs/architecture/DATA_SOURCE_STABILITY_REFACTOR.md`
- 选股和评分：`src/astock_trading/platform/cli/screener.py`、`src/astock_trading/strategy/`
- P5 参数校准：`src/astock_trading/pipeline/param_calibration.py`、`atrade calibrate --json`
- P6 自适应风控建议：`src/astock_trading/pipeline/adaptive_risk.py`、`atrade risk adaptive --json`
- P6 多策略 profile 对比、激活计划和隔离资金建议：`src/astock_trading/pipeline/strategy_profiles.py`、
  `atrade strategy profiles --json`、`atrade strategy profile-activation --target trend_swing --json`、
  `atrade strategy allocation --json`
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
