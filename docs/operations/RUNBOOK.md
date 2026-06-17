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
atrade ops watchdog --json
atrade notify ops-watchdog --dry-run --json
atrade diagnose flow --json
atrade diagnose strategy --json
atrade diagnose schedule --json
atrade calibrate --json
atrade dashboard snapshot --json
atrade risk adaptive --json
atrade strategy allocation --json
atrade strategy health --json
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

`atrade diagnose flow --json` 是每天盘中/盘后排查候选流的第一入口。它把候选池、
评分与买入意向、`opportunity`、`paper auto-readiness`、Hermes 调度、
最新 `auto_trade.summary` 和影子试运行状态放在同一份只读 JSON 里，用于判断当前卡在
“没有候选”“有候选但没入场信号”“已有买入意向但错过窗口”“profile 待人工确认”还是
“模拟承接预检通过”。该命令不运行 pipeline、不提交 MX 模拟盘委托，也不触发真实交易。
如果返回 `approval_gate.required=true`，说明当前需要人工批准某个运行环境动作；
`approval_gate.apply_command` 只作为审批后的执行命令展示，不得由 agent 自行执行。
`after_approval_preview` 是审批后的只读预判，只会给出带
`ASTOCK_CONFIG_PROFILE=...` 的 `paper auto-readiness --skip-account` 预览命令，
并列出当前预检里扣除 profile 后还剩的窗口、账户、候选等阻断；它不写 `.env`、
不查模拟账户、不提交委托。
`next_window_plan` 用于错过买入窗口后的承接排查：它会列出下个买入窗口、当前买入意向
是否能带到下个窗口、以及下次窗口前会运行的候选刷新和模拟承接任务。若显示
`next_window_requires_fresh_buy_signal=true`，表示今天的买入意向只保留为复核证据，
不会在下个交易日自动提交模拟买入。

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

历史 SQLite 迁移入口已移除；运行、运维和测试都应使用 `ASTOCK_DATABASE_URL=mysql+pymysql://...` 指向 MySQL。

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

如果券商 App 显示的持仓成本价已经摊入费用，买入补录时追加
`--cost-price`，持仓收益会按券商总成本口径计算：

```bash
atrade record-buy 002156 300 72.080 --cost-price 72.097 --yes --json
```

如果买入已补录完成，后续才发现本地成本价和券商不一致，使用成本校准命令
追加修正事件；该命令只改本地持仓成本，不提交任何订单：

```bash
atrade adjust-position-cost 002156 --cost-price 72.097 --reason "同步券商成本价" --yes --json
```

`record-buy` / `record-sell` 会额外写入 `trade.hypothesis.recorded` 和
`trade.outcome.recorded`，用于把交易前假设、成交后结果和后续复盘证据串起来。

待人工确认如果超过 `manual_confirmation.pending_max_age_hours`，或已经错过
`auto_trade.buy_window`，会转成“过期待复核”状态，不再压住新的候选观察和影子试运行提示。
先只读查看，再显式结案：

```bash
atrade manual-trades list --status stale --json
atrade manual-trades expire-stale --yes --json
```

`expire-stale` 只追加 `manual_trade.expired` 审计事件，不会记录真实成交，也不会提交模拟盘订单。

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
atrade strategy profile-activation --target trend_swing --json
atrade strategy profile-activation --target trend_swing --record --json
atrade strategy allocation --json
atrade strategy allocation --record --json
```

该命令读取 `trend_swing`、`short_continuation`、`defensive_watch` 等配置 profile，
对比买入阈值、数据质量门禁、仓位上限、短线续涨参数，并匹配 `config_versions` /
`run_log` / `decision.suggested` / `trade.review.recorded` 判断每个 profile 是否已有
运行与复盘证据。样本不足时返回 `needs_shadow_validation`；`--record` 会追加
`strategy.profile_comparison.proposed` 和 Markdown artifact。执行前仍必须显式设置并确认
`ASTOCK_CONFIG_PROFILE`，不要让 agent 自动切换。

`strategy profile-activation` 生成可人工确认的 profile 激活计划，默认只读；`--record`
只追加 `strategy.profile_activation.requested` 和 Markdown artifact，不会修改 `.env`、
Hermes profile 或当前 shell 环境。它会给出 `export ASTOCK_CONFIG_PROFILE=...`、
`paper auto-readiness` 复核命令和 `auto_trade` 执行命令，供人工确认后使用。
JSON 输出中的 `summary` 会说明当前 profile 与目标 profile，`next_action` 会给出唯一的
写入命令，但 `safe_to_auto_apply=false`，agent 不能自行执行。
如果已经人工确认要让 Hermes/atrade 后续稳定使用目标 profile，可以显式写入运行
`.env`：

```bash
atrade strategy profile-activation --target trend_swing --apply-env --yes --json
```

该命令只写 `ASTOCK_CONFIG_PROFILE`，会先备份目标 `.env`，并追加
`strategy.profile_activation.applied` 审计事件；它不改 Hermes 调度、不提交真实或模拟订单。
没有 `--yes` 时只返回 `confirmation_required`，不会写文件。需要指定运行环境文件时加
`--env-file PATH`。

`strategy allocation` 在 profile 对比基础上生成隔离资金桶和弱策略复核建议：正收益且
样本达标的 profile 进入启用候选，负收益或胜率不足的 profile 进入暂停候选，证据不足的
profile 只做影子验证。`--record` 会追加 `strategy.capital_allocation.proposed` 和
Markdown artifact。该命令不改真实账户、不分配真实资金、不停用任何 profile。

P6-3 策略体检从闭合交易复盘做深度归因：

```bash
atrade strategy health --json
atrade strategy health --record --json
```

该命令读取 `trade.review.recorded`、交易前假设和来源评分证据，按行业、市值、持仓天数、
入场信号类型、入场星期和月份统计收益均值、胜率、MFE/MAE，并输出“能力圈”强项/弱项。
样本不足时返回 `insufficient_data`；`--record` 会追加 `strategy.health_report.proposed`
和 Markdown artifact。缺少行业、市值或入场信号证据时只标记证据缺口，不让 AI 补写。

P6-4 仪表盘先使用稳定只读数据契约：

```bash
atrade dashboard snapshot --json
```

该命令汇总 `projection_balances`、`projection_positions`、`projection_candidate_pool`、
`projection_market_state`、`run_log`、`report_artifacts` 和待人工确认事件，输出 Web /
手机仪表盘可消费的首屏 JSON。它只读数据库，不提供下单、撤单、确认交易或改配置能力。

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
atrade ops watchdog --json
atrade notify ops-watchdog --json
atrade opportunity --json
atrade opportunity-watch --json
atrade review manual-followup --json
atrade notify manual-followup --dry-run --json
atrade paper trial-plan --json
atrade paper trial-plan --record --json
atrade paper trial-review --json
atrade paper trial-review --record --json
atrade suggest --json
atrade explain 002138 --json
```

这些命令面向 Hermes / OpenClaw / launchd 的轻量读取与提醒：`digest` 给一句话状态，
`ops watchdog` 聚合调度、数据源、候选池和模拟承接断点，`notify ops-watchdog`
只在运维状态变化时推送 Discord，并把去重快照写到
`~/.local/state/a-stock-trading/ops_watchdog/state.json`；它不运行 pipeline、不下单。
`opportunity` 生成主动机会卡，`opportunity-watch` 记录今日候选池基线并检测
“候选池从 0 变为 >0 / 新强势观察候选 / 新观察候选 / 新核心候选”，`suggest` 给下一步建议，
`review manual-followup` 把机会卡、影子复盘和模拟承接预检合成一份人工复核清单，
`notify manual-followup` 把同一份清单做成 Discord 卡片，
`paper trial-plan` 把观察候选转成只读的模拟盘影子试运行清单，`paper trial-review`
复盘影子候选的起始价、当前价和表现状态，`explain` 解释单只股票最近评分和决策。
它们不会下单，也不会自动调低买入门槛。

候选池现在分三层：`radar` 展示为“强势观察”，用于保留接近观察线或热股召回的股票；
`watch` 是观察池；`core` 才是进入模拟买入前置检查的核心池。`radar` 只提醒和跟踪，
不能被当作买入候选。

`atrade screener refresh --json` 是调度型深度刷新，默认使用
`strategy.screening.refresh_scan_limit` 控制逐票评分预算；`market_scan_limit`
保留给手工 `screener run` 的粗筛广度。需要临时扩大刷新时显式传
`--limit N`，不要在 Hermes 默认任务里直接全量评分 300 只。

刷新链路有两个耗时护栏：`strategy.screening.snapshot_timeout_seconds` 控制单票
完整快照总超时，`strategy.screening.sector_context_timeout_seconds` 控制行业/概念
上下文总超时。超时只会让对应候选降级为缺少部分证据，不能拖住整批候选池刷新。
Hermes 盘中轻量刷新任务在 `13:40` 运行，默认 `ASTOCK_INTRADAY_REFRESH_LIMIT=10`，
用于在 `14:00` 模拟盘自动交易前补当天候选证据；收盘后 `15:10` 仍执行常规刷新。
强势行情下如果候选/买入意向在 14 点后才出现，`14:12` 的盘中候选-模拟闭环会再做
一次 `ASTOCK_INTRADAY_EXECUTION_REFRESH_LIMIT=20` 的刷新，并立即执行
`atrade run-pipeline auto_trade --json`。这条链路只承接已有安全 gate，不自动调低阈值；
当 `auto_trade.dry_run=false` 时会提交 MX 模拟盘委托，仍然不会触碰实盘券商接口。运行前可用
`atrade paper auto-readiness --json` 只读检查当前执行模式、MX 模拟盘账户状态、买入窗口、
核心候选池、新鲜买入意向和异常保护；如果只想检查本地配置和事件证据，可加 `--skip-account`。
真实 Hermes profile 还在 `14:24` 增加一次 `auto_trade` 兜底，只承接当前交易日且未晚于
买入窗口结束产生的买入意向；脚本带 `auto_trade.lock`，避免和 `14:12` 闭环重叠。
如果出现“已有买入意向但自动模拟错过买入窗口”，先运行
`atrade diagnose schedule --json` 检查 Hermes `trading` profile 的 13:40、14:12、
14:24 和影子试运行任务是否当日实际执行。该诊断还会展示 `runtime_profile`，只读确认
`atrade` 运行环境是否已显式设置 `ASTOCK_CONFIG_PROFILE=trend_swing`；如果已经记录
profile 激活计划但 `.env` / 当前进程仍会使用 `default`，诊断会返回 `warning`，并指向
`atrade strategy profile-activation --target trend_swing --json` 复核人工确认计划。该诊断
只读，不补跑、不修改调度、不写 `.env`。

当 `opportunity` / `suggest` 发现已有观察候选但没有买入意向时，下一步会指向
`atrade paper trial-plan --json`。这条链路只生成影子观察计划和复核命令，不调用
`paper buy`，也不写入模拟盘成交。需要把本次候选作为后续复盘证据时，可以执行
`atrade paper trial-plan --record --json`；它只写入幂等的 `paper.trial.recorded`
影子试运行事件，仍然不会提交模拟盘订单。
后续复盘执行 `atrade paper trial-review --json`；它只读取影子事件和市场观测，
输出收益、状态和下一步复核命令，不自动晋级 `core`，也不提交模拟盘订单。
复盘结果会同时带出当前候选池状态；如果影子候选已经掉出候选池，会显示
“已移出候选池”和候选变化。这类正收益只能作为策略复核线索，不能直接晋级或提交模拟盘。
机会卡会把仍在当前候选池内的影子正收益放入主行动；已经移出候选池的影子正收益只保留在
证据和阻断说明里，不应压过当前核心候选、观察候选和新的影子试运行计划。
人工复核自动汇总使用 `atrade review manual-followup --json`。它只读聚合
`atrade opportunity --json`、`atrade paper trial-review --json`、
`atrade paper auto-readiness --json` 和 `atrade risk trial-guard --json` 的判断面，
会把影子正收益候选分成“继续观察 / 复核核心 / 等待自动承接 / 需要你确认”等人读状态。
如果模拟承接预检已经到可提交 MX 模拟盘委托的状态，汇总只会把
`atrade run-pipeline auto_trade --json` 放进 `manual_actions`，并标明
`writes_order=true` / `requires_user_approval=true`；agent 和 Hermes 不能自动执行。
Discord 推送用 `atrade notify manual-followup --json`，调试时先加 `--dry-run`。
机会卡摘要和 Discord 区块标题必须同时展示核心候选与观察候选数量，例如
“核心候选 1 只，观察候选 4 只”。不要把含有核心候选的候选池只写成“观察候选”，
否则运营上会误以为系统仍然没有形成核心候选流。
如果已有核心候选和买入意向，但 `diagnose schedule` 的 `runtime_profile` 显示已记录
`trend_swing` 激活计划而运行环境仍会使用 `default`，机会卡应返回
`status=profile_review_required`，下一步指向
`atrade strategy profile-activation --target trend_swing --json`。这表示模拟承接前还缺
人工确认 profile，不表示候选池或模拟盘账户坏掉。
机会卡 JSON 与 Discord embed 也会带出 `approval_gate` 和 `next_window_plan`，用于给人读
通知显示同一套动作：先人工复核运行 profile；若当前买入意向已经过期或错过窗口，它不会
跨日自动提交，下个交易日必须重新形成同日买入意向后才可能被 `auto_trade` 承接。
`atrade digest --json` 也会把这类状态提升为 `status=profile_review_required`，并在
`attention` 里给出复核命令；不要只看摘要里的“待人工确认 0”就判断系统无事可做。
`atrade agent-context --json` 会把同一套当前动作放进 `operator_attention`，供 agent
第一步就看到该复核哪个命令、是否需要人工批准、旧买入意向是否能跨到下个买入窗口。这个
区块仍是只读提示；带 `--apply-env --yes` 的 profile 写入、人工成交记录和模拟盘提交都
不能由 agent 自动执行。
`atrade commands --json` 是配套的机器可读命令契约目录，列出关键命令的参数、选项、
风险等级、是否写状态、是否写环境、是否需要人工批准。agent 拿到 `operator_attention`
的下一步命令后，应优先读取内联的 `command_contract_id` / `command_contract`；审批门
里的 `review_command_contract` 和 `apply_command_contract` 会明确区分只读复核命令与写
运行 `.env` 的人工批准命令。
候选流相关的后续动作也必须先查这个目录：`stock analyze` 与 `risk trial-guard` 是只读，
`screener refresh` 和 `paper trial-review --record` 会写本地证据，`run-pipeline auto_trade`
可能提交 MX 模拟盘委托，必须看到 `writes_order=true` / `requires_user_approval=true` 后
再等待人工批准。
如果过期买入意向对应的股票仍在当前核心候选池，机会卡会先指向
`atrade paper auto-readiness --json`，用于确认模拟盘承接是被时间窗口、账户、
异常保护还是配置模式挡住；这比回到普通影子试运行计划更贴近“已有核心候选 + 买入证据”
的真实状态。
单股复核用 `atrade stock analyze CODE_OR_NAME --json`。如果返回
`candidate_pool_consistency.requires_pool_refresh=true`，表示即时评分/决策和候选池投影
已经错位，应先刷新候选池证据或回到 `atrade diagnose flow --json`，不要把旧候选池层级
直接当作模拟承接依据。
`paper auto-readiness` 的顶层状态会跟买入侧预检保持一致：例如已有新鲜买入意向但
已过买入窗口时返回 `status=waiting_window`，摘要会直接说明本轮不会提交模拟买入，
而不是只因为配置处于 MX 模拟盘委托模式就显示 `ready`。
预检还会展示 `execution_profile`。如果当前仍是 `default` 混合配置，且仓库里已有
`trend_swing` / `short_continuation` / `defensive_watch` profile，系统会增加
`profile_review_required` 阻断项；需要人工确认后再设置
`ASTOCK_CONFIG_PROFILE=trend_swing` 运行自动模拟，agent 不应自行切换。
当 `diagnose flow` 同时返回 `after_approval_preview` 时，只能把它当作
“批准 profile 后还要检查什么”的提示；如果它列出 `buy_window_closed`，表示候选和买入意向
已经形成，但当前时间不在模拟买入窗口，本轮仍不会提交模拟买入。
同时查看 `next_window_plan`：如果 `current_signal.carries_to_next_window=false`，
下个交易日必须先由候选刷新、评分和决策重新形成同日买入意向，不能把今天的过窗买入意向
跨日自动提交。
`auto_trade` 执行层也必须保留同一条 gate：即使 Hermes 直接调用
`atrade run-pipeline auto_trade --json`，买入窗口内发现运行 profile 仍需人工确认时，也应写入
`auto_trade.diagnostic` / `auto_trade.summary` 的 `profile_review_required`，并禁止提交 MX
模拟盘买入。
如果同日或短周期收益超过价格异常护栏，复盘状态会显示为“价格异常”，下一步会指向
`atrade stock analyze CODE --json` 核查行情证据；这种异常不能被当作正收益候选。
`--record` 只追加 append-only 影子复盘事件。若新护栏把旧复盘从“表现为正”修正为
“价格异常”，系统会追加一条带 `review_corrected` 的更正事件，不改写旧事件。
行情采集在写入 snapshot 前会用同票日 K 最新收盘价过滤明显串价的 quote；影子复盘
读取历史价格时也会跳过最近稳定价格之外的最新离群点。这样坏 provider 返回不会直接
污染候选池、模拟盘影子收益和后续 agent 判断。

Hermes `trading` profile 已增加影子试运行周期任务，调度为 `45 13,15 * * 1-5`：
`13:45` 把盘中轻量刷新后的候选写入影子试运行事件；`15:45` 在收盘候选刷新、
核心池评分和机会卡之后补录候选并写入当日影子复盘。脚本只调用
`atrade paper trial-plan --record --json`、`atrade paper trial-review --record --json`
和 `atrade notify manual-followup --skip-account --json`，不调用 `paper buy` / `paper sell`。
15:45 的人工复核卡片是收盘后的复盘清单，只把候选和待确认动作推送给人；
不会读取 MX 账户，也不会把当日影子结果自动带到下一交易日买入。涉及
`run-pipeline auto_trade`、`record-buy` / `record-sell` 或 profile 写入的动作仍必须人工确认。

`atrade opportunity-watch --json` 会写入
`~/.local/state/a-stock-trading/opportunity_watch/state.json` 用于去重；dry-run 或只读检查
可加 `--no-write`。Discord 推送使用 `atrade notify opportunity-watch --json`，无变化时
返回 `status=silent` 并保持静默，有变化时才发送“机会变化提醒”。
去重不只看新增候选，也看当前动作。如果候选池没变，但机会状态转为
`profile_review_required`、模拟预检、人工确认或数据健康复核等需要处理的状态，会返回
`change_types` 包含 `operator_action_required`，并在 Discord 卡片的下一步里展示对应
只读复核命令。状态文件记录同一动作 key 后，后续相同状态会继续静默，避免重复刷屏。

`atrade market-intel watchlist-sync --source candidate-pool --preserve-holdings --dry-run --json`
用于预演 MX 自选同步计划；加 `--yes` 后才会写 MX 自选。目标自选由最新核心池、
观察池和强势观察组成，当前 MX 模拟盘持仓和本地 `record-buy` / `record-sell`
重建出的 `projection_positions` 正持仓会被保留，不会因候选池变化被删除。候选池刷新
Hermes 脚本已在推送机会变化前自动执行该同步；可用 `ASTOCK_WATCHLIST_SYNC_DISABLE=1`
临时关闭，或用 `ASTOCK_WATCHLIST_SYNC_DRY_RUN=1` 只记录计划不写 MX。

MCP 客户端可用只读工具 `trade_opportunity_card` 获取同一份机会卡；实现仍复用
`atrade opportunity --json` 的 payload 构建逻辑，不能绕过人工确认边界。

实盘试运行护栏审计：

```bash
atrade risk trial-guard --json
atrade risk trial-guard --capital 500000 --amount 60000 --json
```

该命令只读配置和运行库，不执行交易。默认试运行单票上限为正式单票上限的一半
（`risk.position.single_max * trial_single_max_ratio`），同时明确真实交易必须人工确认，
系统没有券商实盘下单接口。若运行库可用，输出还会带出当前候选池摘要、当前入场信号、
运行 profile 阻断和下一步只读命令；若运行 profile 仍是 `default`，应先处理
`atrade strategy profile-activation --target trend_swing --json`，不能直接进入模拟承接。

## launchd 安装

模板在 `config/launchd/`。当前推荐至少安装 `ops-watchdog`，作为独立于 Hermes
调度图的系统化巡检和告警层。它每 10 分钟执行
`~/.local/state/a-stock-trading/bin/a_stock_ops_watchdog_supervise.py`；外部监督脚本只用 Python 标准库启动
工具环境里的轻量 worker 子进程，worker 只导入 watchdog 必需模块，不走全量 Typer CLI
装载路径，并按 wall-clock timeout 杀掉卡住的子进程，避免 Discord 网络或原生依赖卡住。
watchdog 只在状态变化时推送；
没有变化会静默并更新/保留状态文件。

```bash
mkdir -p ~/.local/state/a-stock-trading/logs/launchd
mkdir -p ~/.local/state/a-stock-trading/bin
mkdir -p ~/.local/state/a-stock-trading/config
rsync -aL ~/.config/a-stock-trading/ ~/.local/state/a-stock-trading/config/
chmod 700 ~/.local/state/a-stock-trading/config ~/.local/state/a-stock-trading/config/profiles
find ~/.local/state/a-stock-trading/config -type f -exec chmod 600 {} \;
cp config/scripts/a_stock_ops_watchdog_supervise.py \
  ~/.local/state/a-stock-trading/bin/a_stock_ops_watchdog_supervise.py
chmod +x ~/.local/state/a-stock-trading/bin/a_stock_ops_watchdog_supervise.py
sed "s#/Users/USER#$HOME#g" \
  config/launchd/com.astock.trade.ops-watchdog.plist \
  > ~/Library/LaunchAgents/com.astock.trade.ops-watchdog.plist
launchctl bootstrap "gui/$(id -u)" ~/Library/LaunchAgents/com.astock.trade.ops-watchdog.plist
launchctl enable "gui/$(id -u)/com.astock_trading.trade.ops_watchdog"
```

launchd 后台进程不要直接读取指向 checkout 的配置 symlink。本机的
`~/.config/a-stock-trading/` 可能为了开发方便链接到 `/Users/.../Documents/...`，
而 LaunchAgent 未必拥有 Documents 访问权限；因此 watchdog plist 默认用
`ASTOCK_ENV_FILE=~/.local/state/a-stock-trading/config/.env` 和
`ASTOCK_CONFIG_DIR=~/.local/state/a-stock-trading/config` 读取实文件副本。配置变更后，
需要重新执行上面的 `rsync -aL` 并重启 LaunchAgent。

手工检查：

```bash
launchctl print "gui/$(id -u)/com.astock_trading.trade.ops_watchdog"
tail -n 50 ~/.local/state/a-stock-trading/logs/launchd/ops-watchdog.log
tail -n 50 ~/.local/state/a-stock-trading/logs/launchd/ops-watchdog.err.log
```

盘前/收盘模板仍只是示例；生产确定性业务节奏当前由 Hermes 管，launchd 不替代
Hermes 的业务任务。launchd watchdog 的职责是发现 Hermes 任务失败、候选池新鲜度过期、
核心池为空、数据源阻断和模拟承接阻塞，并给出恢复命令。

## Hermes LLM 摘要

Hermes 定时任务分为两层：原有 `no_agent: true` 任务继续跑确定性流水，LLM 摘要任务只通过 `atrade llm-context --mode ...` 读取上下文后生成中文总结。

安装和任务创建步骤见 `docs/operations/HERMES_LLM_SUMMARIES.md`。Hermes 不应进入交易系统 checkout 或直接运行仓库脚本；不要用 LLM 摘要任务替代盘中风控、止损/止盈、人工确认、pipeline 失败和核心数据源严重异常告警。

`atrade llm-context --mode morning|close|weekly` 会输出“证据编号清单”。Hermes LLM 最终摘要每个判断段落必须写 `evidence_id: ...`；`atrade notify llm-summary-card` 默认会拒绝缺少 `evidence_id` 的摘要。

完整调度节奏和精简目标见 `docs/operations/HERMES_SCHEDULE.md`。

## 何时考虑服务化

当前推荐保持 CLI + MCP + MySQL，并用 Hermes 承担业务调度、launchd
`ops-watchdog` 承担独立巡检和告警。只有出现以下情况时再引入 HTTP 服务：

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
