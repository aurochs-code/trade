# Hermes 调度节奏

本文整理 A 股交易系统在 Hermes 中的当前调度节奏。原则是：

- Hermes 只做调度、消息投递和 LLM 摘要，不进入交易系统 checkout。
- 交易系统能力统一通过 `atrade ...` 暴露，Hermes wrapper 只调用稳定 CLI。
- 确定性生产任务用 `no_agent: true`，成功时尽量静默。
- LLM 任务只做盘前、收盘、周复盘三类人读总结。
- 盘中风控、止损/止盈、人工确认、pipeline 失败和核心数据源严重异常直接告警，不等待 LLM 汇总。
- Hermes 不是唯一监控面；独立 launchd `ops-watchdog` 每 10 分钟运行外部监督脚本
  `a_stock_ops_watchdog_supervise.py`，由它启动轻量 watchdog worker。该链路用于发现
  Hermes 调度失败、候选池新鲜度过期、核心池为空、数据源阻断和模拟承接阻塞。

## 当前快照

截至 2026-06-16 本机快照，Hermes `trading` profile active job 共 `23` 个，其中 A 股相关任务包括：

- A 股确定性执行：`morning`、`noon`、`evening`、`scoring`、`weekly`、`auto_trade`、盘中轻量 `screener refresh`、收盘后 `screener refresh`、影子试运行记录复盘、`propose-plan`、`daily inspection`
- A 股健康探针：盘前 1 个 Hermes 探针；系统化异常感知由 launchd watchdog 独立承担
- A 股盘中风控：合并为 1 个 Hermes cron，通过 `a_stock_intraday_monitor_window.sh` 判断交易时间窗
- A 股舆情监控：合并为 1 个 Hermes cron，通过 `a_stock_sentiment_window.sh` 判断交易时间窗
- A 股 LLM 摘要：盘前、收盘、周复盘 3 个任务
- 当前 `trading` profile 只保留 A 股任务；非 A 股任务不在该 profile 内统计

已暂停但未删除的旧任务：

- `A股盘中风控轮巡(上午)` 2 个
- `A股盘中风控轮巡(午盘)` 1 个
- `A股盘中风控轮巡(下午)` 1 个
- `A股盘中风控轮巡(尾盘)` 1 个
- `A股舆情监控(半点)` 1 个
- `A股舆情监控(整点)` 1 个
- `A股健康诊断(收盘后)` 1 个

## 当前启用节奏

### 交易日盘前

| 时间 | 任务 | 类型 | 目标 |
| --- | --- | --- | --- |
| `09:00` | 盘前健康探针 | `no_agent` / local | 检查 DB、数据源、运行状态 |
| `09:15` | 盘前 pipeline | `no_agent` / local | 生成盘前基础数据、持仓、市场状态 |
| `09:20` | LLM 盘前摘要 | 脚本内 LLM / Discord Rich Embed | 输出系统与数据质量、今日动作、热点、候选池、持仓风险和今日纪律 |

### 交易日盘中

| 时间 | 任务 | 类型 | 目标 |
| --- | --- | --- | --- |
| `09:30-15:00` 每 30 分钟 | 舆情监控 | `no_agent` / local 或必要时告警 | 监控持仓、核心池和观察池舆情 |
| `09:35-11:30` 每 5 分钟 | 盘中风控轮巡 | `no_agent` / origin | 检查持仓风险、止损/止盈和异常告警 |
| `11:55` | 午间检查 | `no_agent` / local | 生成午盘状态、市场变化和风险提示 |
| `13:00-14:55` 每 5 分钟 | 盘中风控轮巡 | `no_agent` / origin | 下午继续风险监控 |
| `13:40` | 盘中候选池轻量刷新 | `no_agent` / local | 小限额刷新当日候选证据，为 14:00 模拟盘自动交易提供当天候选池 |
| `13:45` | 影子试运行记录 | `no_agent` / local | 把盘中候选写入 `paper.trial.recorded`，只做影子观察 |
| `14:00` | 模拟盘自动交易 | `no_agent` / local | 只跑模拟盘，不真实下单；可重复运行，重复信号会去重 |
| `14:12` | 盘中候选-模拟闭环 | `no_agent` / local | 再做一次更靠近买入窗口尾段的候选刷新，并立即跑模拟盘自动交易 |
| `14:24` | 模拟盘买入兜底 | `no_agent` / local | 买入窗口结束前再承接一次当前交易日可用买入意向 |

盘中风控当前由 1 个 Hermes cron 承担：`*/5 9-14 * * 1-5`。wrapper 会在 `09:35-11:30`、`13:00-14:55` 之外静默退出。

舆情监控当前由 1 个 Hermes cron 承担：`*/30 9-15 * * 1-5`。wrapper 会在 `09:30-15:00` 之外静默退出。

### 交易日收盘后

| 时间 | 任务 | 类型 | 目标 |
| --- | --- | --- | --- |
| `15:10` | 候选池刷新 | `no_agent` / local | 刷新强势观察、观察池和核心候选池 |
| `15:25` | 核心池评分 | `no_agent` / local | 对候选池和核心池重新评分 |
| `15:30` | 交易计划生成 | `no_agent` / local | 生成只读交易计划，不执行 |
| `15:35` | 收盘 pipeline | `no_agent` / local | 生成收盘报告、投影和基础复盘数据 |
| `15:45` | 影子试运行记录复盘 | `no_agent` / local | 补录收盘候选、记录当日影子复盘并推送人工复核清单 |
| `15:50` | 每日巡检报告 | `no_agent` / local | 汇总系统健康、运行记录、人工确认和交易计划 |
| `15:55` | LLM 收盘复盘 | 脚本内 LLM / Discord Rich Embed | 输出系统与数据质量、今日闭环、收盘热点、盘前与收盘对比、候选池变化和明日清单 |

收盘后健康诊断与每日巡检、LLM 收盘复盘重叠，当前已暂停。

候选池刷新脚本成功后会先执行
`atrade market-intel watchlist-sync --source candidate-pool --preserve-holdings --yes --json`：
清理 MX 自选里的非持仓旧票，并加入最新核心池、观察池和强势观察。同步只改 MX 自选，
不提交模拟盘订单；如需临时关闭，可设置 `ASTOCK_WATCHLIST_SYNC_DISABLE=1`，测试时可设置
`ASTOCK_WATCHLIST_SYNC_DRY_RUN=1`。
逐票评分默认按 `strategy.screening.screener_scoring_chunk_size` 分块隔离执行；单个分块失败
会写入 `scoring_chunks.failed` 遥测，成功分块仍可刷新候选池，不放宽买入线和入场路线门禁。

建议在 `15:10` 候选池刷新后、`15:25` 核心池评分前补充一条 `15:18`
机会变化提醒任务，执行 `atrade notify opportunity-watch --json`。它只对新强势观察、
新观察、新核心候选做去重提醒，不下单，也不改变候选池。

盘中 `13:40` 的轻量刷新使用 `a_stock_screener_refresh_intraday_silent.sh`，默认
`ASTOCK_INTRADAY_REFRESH_LIMIT=10`。它复用候选池刷新脚本的锁，避免和手工刷新或
收盘后刷新重叠；刷新完成后会触发机会变化提醒。这个任务用于修正“14:00 自动模拟
交易早于 15:10 收盘候选池刷新”的时序错位。

`14:12` 的盘中候选-模拟闭环使用 `a_stock_intraday_execution_cycle_silent.sh`，默认
`ASTOCK_INTRADAY_EXECUTION_REFRESH_LIMIT=20`。它按顺序执行盘中候选刷新和
`atrade run-pipeline auto_trade --json`，用于承接 14 点后才出现的强势候选/买入意向，
避免信号生成在买入窗口后段、自动模拟只在 14:00 跑一次导致错过。当前仍遵守
`auto_trade.dry_run` 配置；`dry_run=false` 时会提交 MX 模拟盘委托，实盘交易仍需要人工确认。
排查前先看 `atrade paper auto-readiness --json`，确认当前是影子记录模式还是 MX 模拟盘委托模式。

`14:24` 的模拟盘买入兜底使用 `a_stock_pipeline_auto_trade_silent.sh`，只再次执行
`atrade run-pipeline auto_trade --json`，不刷新候选池。它用于承接 `14:12` 刷新后、
买入窗口结束前才形成的当前交易日买入意向。脚本持有 `auto_trade.lock`，如果另一轮
`auto_trade` 正在运行会静默跳过；买入侧本身还会按信号和股票去重，避免重复模拟买入。
如果买入意向已经错过买入窗口，`atrade opportunity --json` 会把它放入过期待复核，
不再压住新的观察候选或影子复盘；需要结案时手工运行
`atrade manual-trades expire-stale --yes --json`。

影子试运行记录复盘由 `a_stock_paper_trial_cycle_silent.sh` 执行，调度为
`45 13,15 * * 1-5`。它先执行 `atrade paper trial-plan --record --json`，把观察候选、
强势观察候选和核心候选写入 `paper.trial.recorded`；15:40 后再执行
`atrade paper trial-review --min-age-days 0 --record --json` 记录当日影子复盘，并调用
`atrade notify manual-followup --skip-account --json` 推送收盘后的人工复核清单。该任务只写
`paper.trial.*` 影子事件和 Discord 通知，不读取 MX 账户、不调用 `paper buy` / `paper sell`，
也不自动晋级候选或把信号带到下一交易日买入。

人工复核自动汇总用 `atrade review manual-followup --json`，手工补发 Discord 卡片用
`atrade notify manual-followup --json`。它只读聚合机会卡、影子复盘和模拟承接预检，
把“继续观察、等待自动承接、需要你确认”的动作直接列给 Hermes/人读；若下一步可能提交
MX 模拟盘委托，只会展示待确认命令，不会由 Hermes 自动执行。

### 每周

| 时间 | 任务 | 类型 | 目标 |
| --- | --- | --- | --- |
| 周日 `20:00` | 周报 pipeline | `no_agent` / local | 生成确定性周报和 Obsidian 周复盘 |
| 周日 `20:10` | LLM 周复盘补充 | LLM / Discord | 总结系统运行质量、交易/持仓质量、信号质量和下周重点 |

### 非交易系统任务

| 时间 | 任务 | 处理 |
| --- | --- | --- |
| 每天 `08:00` | 英语早读 BBC | 保留，属于独立任务 |
| 每天 `21:00` | 英语晚读 ESL Pod | 保留，属于独立任务 |
| 周一 `09:00` | 每周包更新检查 | 保留，属于独立任务 |

## 本次整理

历史整理曾把重复的 Hermes 配置收敛；当前应以 `atrade diagnose schedule --json`
和 Hermes `jobs.json` 为准，不再把本文件当作唯一机器事实来源：

| 动作 | 整理前 | 整理后 | 说明 |
| --- | --- | --- | --- |
| 暂停收盘后健康诊断 | 1 个 | 0 个 | 由每日巡检和 LLM 收盘复盘覆盖 |
| 合并盘中风控 cron | 5 个 | 1 个 | wrapper 内判断交易时间段 |
| 合并舆情 cron | 2 个 | 1 个 | wrapper 内判断半点/整点和是否有内容 |
| 保留 LLM 摘要 | 3 个 | 3 个 | 盘前、收盘、周复盘 |

当前运维边界：

- Hermes `trading` profile active job 当前为 `23` 个。
- 关键业务告警仍由 Hermes wrapper 直接投递。
- 调度失败、候选池过期、核心池为空和模拟承接阻塞由独立 launchd
  `ops-watchdog` 发现并推送，不等待下一次 Hermes 盘前或收盘摘要。

## 观察和回滚

1. 先观察一次 LLM 周复盘和下一个交易日盘前/收盘摘要。
2. 观察下一个交易日盘中风控、舆情监控是否按窗口正常运行。
3. 若发现漏报，优先恢复被暂停的旧分段任务，再排查合并 wrapper。
4. 稳定运行一到两个交易日后，再考虑删除 paused 旧任务。

不建议立即删除旧任务；当前采用 paused 状态，方便回滚。
