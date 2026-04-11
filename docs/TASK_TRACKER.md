# 交易系统任务跟踪

更新时间：2026-04-11

说明：

- `[x]` 已完成
- `[ ]` 未完成
- 本文档只跟踪可执行任务，不重复写背景说明
- 后续每完成一项，直接在这里打勾并补一行结果备注

---

## 已完成

### 状态层 / 账本

- [x] 结构化 ledger 落地，统一状态服务可读 `portfolio / market / pool / activity`
- [x] 三账户 scope 建模：`cn_a_system / hk_legacy / paper_mx`
- [x] `state bootstrap / audit / sync / reconcile` 命令接入 CLI
- [x] `paper_mx` drift 审计和 reconcile 流程落地
- [x] `status today` 统一读结构化状态，不再直接解析 Markdown
- [x] `paper_mx` 完全收口为标准 portfolio snapshot
备注：`load_portfolio_snapshot("paper_mx")` 固定经 broker 刷新并回写 ledger，失败时保留缓存快照。
- [x] `paper_mx` 的 balance / exposure 契约与主仓一致
备注：paper snapshot summary 已统一输出 `holding_count / current_exposure / cash_value / total_capital`。
- [x] `status today` / `doctor` / `state audit` 完全共享同一份 paper snapshot
备注：`status today` 暴露 `paper_mx_portfolio`，`state audit/doctor` 共享 `paper_portfolio_snapshot` 检查。

### 信号 / 风控 / 执行

- [x] `reason_code` 主干收敛，`status today` 有统一 `signal_bus`
- [x] 大盘信号统一成单一来源，修正指数口径问题
- [x] 周买入次数、交易事件、周报统计改读结构化事件
- [x] advisory 风控输出：`time_stop / drawdown_take_profit`
- [x] 组合级风控第一版接入 `today_decision`
- [x] 订单状态第一版：`orders` 表、状态快照、提醒、确认
- [x] pool snapshot / action history / projection 双写打通
- [x] pool 引擎事务化与 projection 对账强化
备注：`screener` 已收敛为只走统一 `save_pool_snapshot` 投影；`audit_state` 增加 missing/extra/score/bucket drift 明细。
- [x] 订单生命周期第二阶段：`partial_fill / cancel_replace / review_queue`
- [x] broker 回报更细状态接入订单状态表
备注：broker sync 已接入 `cancel_requested / cancel_replace_pending / review_required / partially_filled`，部分成交按增量写交易事件并保留 raw broker status。
- [x] 组合风控扩展到板块集中度 / 相关性 / 事件风险
备注：`check_portfolio_risk` 已输出板块集中度、相关性分组集中度、事件风险日 warning reason code 与 metrics。

### 回测 / 复盘 / 观测

- [x] `backtest run / sweep / walk-forward` 基础框架
- [x] backtest 历史样本、JSON 结果、Markdown 报表落盘
- [x] `backtest history / compare` 历史索引与横向对比
- [x] MFE / MAE 代理估算落地
- [x] 有历史日线时优先重建真实 `MFE / MAE`
- [x] 历史日线可用时，单笔止损/止盈规则重放
- [x] 评分权重 sweep 接入 backtest
- [x] 组合级回放第一版：逐日时间线、峰值占用、并发持仓
- [x] 组合级回放加入总仓位 / 单票上限 / 资金占用约束
- [x] 告警中心第一版
- [x] 结构化复盘归因第一版
- [x] CLI contract tests / fixture / core e2e smoke 主干建立
- [x] 把组合级回放从"闭合交易回放"推进到"真实逐日组合模拟"
备注：已落地日级事件主循环，逐日推进 cash / entry / exit / exposure；当前仍基于闭合交易样本，完整信号驱动策略引擎继续放在 P3。
- [x] 同日多信号竞争资金的排序规则
备注：已按 `entry_score desc -> realized_pnl desc -> desired_capital asc` 固定排序，并输出 accepted/rejected allocation 明细。
- [x] walk-forward 多 fold 报表强化
备注：已补 fold comparison / aggregate comparison，输出 selected parameters 以及 train vs eval 的 pnl / win_rate / sample_count 对照。
- [x] 真正的逐日策略回放引擎
备注：`scripts/backtest/strategy_replay.py` 实现信号驱动逐日模拟，每日评估大盘信号→候选评分→veto过滤→资金分配→持仓管理（止损/止盈/时间止损/大盘清仓），CLI 通过 `backtest strategy-replay` 接入。
- [x] 回测参数扩展到 veto / 评分更多维度
备注：`_parameter_grid` 已扩展 `watch_threshold / reject_threshold / time_stop_days / veto_presets`，`_apply_parameter_set` 支持 veto 规则过滤和 reject_threshold 过滤，三个入口（run/sweep/walk-forward）均已接入。
- [x] 组合层连续亏损冷却纳入回测
备注：portfolio replay 已按 `consecutive_loss_days_limit / cooldown_days` 拒绝冷却期新开仓，并输出 cooldown rejected 明细。
- [x] 组合层资金占用与持仓上限纳入 walk-forward 评估
备注：walk-forward 每个 fold now 输出 evaluation portfolio replay summary，并在 comparison rows 中展示峰值仓位/并发/冷却拒绝数。
- [x] 回测结果对比视图继续强化
备注：`backtest compare` now 输出风险状态分布、总/均值指标与 leaderboard 排名。
- [x] 历史数据批量回测引擎（`historical_pipeline.py` + `run_multi_stock_system_backtest`）
备注：支持乖离率过滤 / 收阳线确认 / 两市成交额过滤；CLI `backtest batch` 统一入口。
- [x] 策略参数 C 版校准（乖离率<10% + 收阳线 + trailing10% + time_stop15天）
备注：经 80 只股回测对比，选定保守验证C写入 `strategy.yaml` 主配置。
- [x] `backtest batch` CLI 入口
备注：`trade backtest batch --codes --start --end --preset --params-json`，统一外部调用。

### 复盘归因深化

- [x] entry factor 结构化沉淀
备注：闭合交易 now 输出 `entry_factors`，来源覆盖 reason code 与 reason text。
- [x] exit factor 结构化沉淀
备注：闭合交易 now 输出 `exit_factors`，覆盖风控、池子、对账、人工干预等出场来源。
- [x] 收益归因拆解
备注：闭合交易 now 输出 `pnl_attribution`，包含 outcome / pnl_pct / MFE 捕获率 / exit_style。
- [x] 规则偏离解释
备注：闭合交易 now 输出 `rule_deviation`，解释 compliant / reconcile / manual_override / drift。
- [x] 组合级归因摘要
备注：`load_trade_review` now 输出 `portfolio_attribution_summary`，周报同步展示出场风格盈亏与规则偏离分布。

### 告警中心深化

- [x] 财报告警接入
备注：pool entry `earnings_bomb` now 生成 `FINANCIAL_EARNINGS_WARNING`。
- [x] 异动 / 放量破位 / 涨停回落告警接入
备注：pool entry `limit_up_today / volume_break` now 生成涨停回落观察与放量破位告警。
- [x] 池子失分告警接入
备注：pool entry `score_delta <= -1.0` now 生成 `POOL_SCORE_LOSS`。
- [x] 告警去重、节流、处理状态
备注：告警 now 带 `alert_key / handling_status / throttled`，同 key 去重并统计 suppressed duplicate count，保存时保留历史 ack 状态。

### MX 能力层

- [x] `scripts.mx.cli_tools` 能力注册表 / dispatcher 落地
- [x] `bin/trade mx list / groups / run / health` 接入
- [x] `stock_screener` 改走 MX capability layer
- [x] `shadow_trade` 改走 MX capability layer
- [x] `status today` 暴露 `mx_health`
- [x] `morning` 资讯搜索完全切到 MX capability layer
备注：盘前资讯已统一经 `dispatch_mx_command("news", ...)` 走 capability layer，并补了可用/降级测试。

### 自动化闭环

- [x] 舆情独立监控定时任务
备注：`pipeline/sentiment_monitor.py` 实现核心池+持仓舆情扫描，关键词匹配（高/中两级），告警去重（24h），Discord 推送。crontab 每30分钟执行。
- [x] 港股遗留仓位自动监控
备注：`pipeline/hk_monitor.py` 自动拉取港股价格（MX → akshare），检查绝对止损（-15%）和反弹止损上调规则，触发时 Discord 告警，更新结构化账本。crontab 16:30 执行。
- [x] 订单管理 CLI（Hermes-Agent 专用）
备注：`trade order` 子命令组，支持 `pending / confirm / place / cancel / modify / remind / overdue-check / list`，Hermes-Agent 通过 CLI 直接管理条件单，无需 Discord Bot。
- [x] 超时未确认提醒
备注：`trade order overdue-check` 实现 T+1 再提醒 / T+2 异常标记，crontab 每日 9:15 自动执行。

### P4 实盘磨合

- [x] P4 roadmap 优化为当前状态 / 剩余风险 / 下一步优先级
备注：已补充实盘试运行护栏、逐笔对账口径、数据源降级阻断待办和 P4/P5/P6 成功标准。
- [x] 非交易日 skipped 状态落盘
备注：`hermes_cron.sh` 对 A 股日内 pipeline 写入 `skipped_reason=non_trading_day`，周报/月报/港股检查不受 A 股交易日历限制。
- [x] 评分数据质量传播到 snapshot 和 Discord
备注：核心池 snapshot 保留 `data_quality / data_missing_fields`，盘前/收盘核心池评分展示数据降级提示。
- [x] doctor 数据源健康度检查
备注：`doctor` now 输出 `data_source_health`，聚合最近 pipeline 运行、缓存新鲜度、字段缺失率、评分数据质量和最后成功时间。
- [x] 数据质量买入门禁
备注：`data_quality=degraded/error` 的新增买入建议降级为人工复核/blocked；池子晋级、筛选建议和影子交易自动买入不再把降级数据当作自动买入依据。
- [x] 历史信号镜像任务拆解入文档
备注：把 `market snapshot / pool snapshot / scored candidates / today_decision` 的历史归档能力拆成状态层、流水线接入、回测接入、CLI/报告四段任务。
- [x] 历史快照状态层建模
备注：ledger 新增历史快照表，支持按 `snapshot_date / history_group_id` 读取 market / pool / candidates / decision，并可组合成单日信号 bundle。
- [x] 历史快照首条生产链路接入
备注：`stock_screener` 运行时按统一 `history_group_id` 落盘 market / pool / scored candidates / today_decision；`core_pool_scoring` 同步落盘评分历史。
- [ ] 历史信号镜像回放引擎
备注：`historical_pipeline.py` 优先读取每日历史快照，缺失时才回退到当前 proxy replay。
- [ ] 历史信号镜像 CLI / 诊断
备注：增加按日查看某次 `history_group_id` 的 market / pool / candidates / decision，支持单股命中/漏判解释。
- [ ] 盘前/午间/收盘市场快照归档补齐
备注：`morning / noon / evening` 统一按日写入 market snapshot history，保留时点差异。

---

## 完成定义

- "完成"指代码已落地、测试已覆盖、CLI 或流程已有可验证输出
