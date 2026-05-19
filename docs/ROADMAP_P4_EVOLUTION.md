# 交易系统进化路线图（P4+）

> 起始日期：2026-04-11
> 更新日期：2026-05-19
> 前置：P1-P3 全部收口，自动化闭环已打通
> 定位：从"能跑起来"进化到"跑得好、跑得稳、跑得聪明"

---

## 当前系统画像

一句话：**决策全自动 + 模拟盘验证 + 实盘手动执行**。

已经有的：
- 四维评分 + 一票否决 + 大盘择时 → 综合决策
- 影子交易自动验证策略信号
- 完整回测框架（sweep / walk-forward / strategy-replay）
- 结构化账本 + 告警中心 + 复盘归因
- 9 个定时 pipeline + Discord 推送 + Hermes CLI 闭环
- 月度复盘自动生成、A 股交易日历、数据质量标记

还没验证的：
- 实盘还没真正跑过完整的买卖周期
- 评分权重和风控参数已有 P5 校准入口，仍需足够闭合交易样本验证后才能采纳建议
- 模拟盘刚跑了 1 天（04-10），样本量不足
- 模拟盘信号和实盘手动操作还没有逐笔对账

---

## 进化方向总览

| 阶段 | 主题 | 核心目标 | 预计周期 | 启动条件 |
|------|------|---------|---------|---------|
| P4 | 实盘磨合 | 跑通第一轮完整买卖周期，积累真实数据 | 2-4 周 | P1-P3 已收口 |
| P5 | 参数校准 | 用实盘 + 模拟盘数据校准策略参数 | 持续 | 20+ 笔实盘闭合交易 |
| P6 | 智能进化 | 自适应调参 + 多策略 + 更深归因 | 长期 | 连续稳定运行 3 个月 |

---

## 本轮已处理（2026-04-10）

- [x] 优化 P4 roadmap 结构：补充实盘护栏、下一步优先级、成功标准。
- [x] `hermes_cron.sh` 非交易日跳过时写入 daily state：`status=skipped` + `skipped_reason=non_trading_day`。
- [x] 交易日历 gating 收窄到 A 股日内 pipeline：`morning / noon / evening / scoring / sentiment`；`weekly / monthly / hk_monitor` 不再被 A 股交易日历误跳过。
- [x] 核心池 snapshot 保留 `data_quality / data_missing_fields`，供下游报告和状态读取。
- [x] 盘前/收盘 Discord 核心池评分增加数据质量提示：`⚠️ 数据降级（缺失:...）`。
- [x] `doctor` 增加数据源健康度检查：聚合最近 pipeline 运行、缓存新鲜度、评分数据质量。
- [x] 数据质量门禁：`data_quality != ok` 的新增买入信号降级为人工复核/blocked，影子交易不自动查价下单。

---

## P4：实盘磨合期（2-4 周）

> 目标：让系统在真实市场环境中跑起来，暴露问题，积累数据；不急于自动化下单。

### P4-0 实盘试运行护栏

**问题**：系统已经能给交易建议，但实盘尚未跑过完整闭合买卖周期；如果直接放大仓位，早期参数误差会被放大。

**方案**：
- [x] 首轮实盘只做人工确认执行，系统不直连券商下单
- [x] 首轮实盘单笔仓位采用试运行上限（建议不超过正式单票上限的一半，具体金额人工确认）
  - 2026-05-19：新增 `risk trial-guard --json` 审计命令，明确无实盘券商接口、真实交易必须人工确认，并按正式单票上限的一半计算试运行金额上限。
- [x] `data_quality != ok` 时，新增买入建议必须降级为"人工复核"，不得作为自动买入依据
- [x] 单日异常保护：连续失败 pipeline、关键数据源 error、组合风控 block 时停止新增交易
  - 2026-05-19：关键数据源失败继续由 pipeline_runner 阻断；auto_trade 买入前新增 `new_trade_guard`，近期失败 pipeline 或组合风控 breach 会记录诊断并停止新增买入。
- [x] 每笔实盘交易记录 `decision_id / signal_id / manual_reason`，方便和模拟盘逐笔对账
  - 2026-05-19：`record-buy` / `record-sell` 支持 `--decision-id`、`--signal-id`、`--manual-reason`，底层写入交易假设证据链。

### P4-1 交易日历感知

**问题**：cron 不区分节假日会空跑并产生无效数据；但周报、月报、港股检查不应该被 A 股交易日历误跳过。

**方案**：
- [x] 新增 `utils/trading_calendar.py`，维护 A 股交易日历（节假日 + 调休）
- [x] `hermes_cron.sh` 启动时先检查是否交易日，非交易日直接跳过 A 股日内 pipeline
- [x] pipeline 运行状态记录 `skipped_reason: non_trading_day`
- [x] 数据源：akshare `tool_trade_date_hist_sina()` + 本地缓存（7 天刷新）
- [x] 周报、月报、港股检查不受 A 股交易日历限制

### P4-2 模拟盘 vs 实盘对账

**问题**：模拟盘和实盘是两套独立仓位，当前月报只做汇总对比，不能解释逐笔偏离。

**方案**：
- [x] 月报增加"模拟盘 vs 实盘"对比区块（买入信号数 / 执行数 / 偏离 / 盈亏差异）
- [x] 收盘报告增加"模拟盘 vs 实盘"对比区块（待实盘有数据后启用）
  - 2026-05-19：收盘报告复用 `review shadow` 对账结果，展示模拟盘/实盘数量、匹配数、偏离数和前 5 条偏离明细。
- [x] 周报增加模拟盘独立 P&L 统计（待积累更多数据）
  - 2026-05-19：模拟盘卖出事件写入 `realized_pnl_cents`；周报/月报 `paper_stats` 从模拟盘事件独立汇总，缺少 realized_pnl 时明确显示暂无可用数据。
- [x] 建立逐笔 join 口径：`signal_id / code / side / event_date / order_id`
- [x] 偏离类型结构化：`not_executed / extra_real_trade / partial_fill / price_slippage / manual_override`
- [x] 当模拟盘信号与实盘操作不一致时，记录 `rule_deviation: shadow_divergence`
  - 2026-05-19：新增 `review shadow --json`，支持预览/写入模拟盘与实盘逐笔偏离；模拟盘买入事件保留来源 decision/score id。

### P4-3 月度复盘自动生成

**问题**：月度复盘需要自动汇总，避免月底靠手工补数。

**方案**：
- [x] 新增 `pipeline/monthly_review.py`
- [x] 从结构化账本自动统计：月度 P&L、胜率、盈亏比、最大回撤
- [x] 从周报聚合：周度汇总表
- [x] 从交易事件提取：亏损最大 3 笔 + 原因分析
- [x] 从 pool action history 提取：核心池变化汇总
- [x] 模拟盘 vs 实盘对比
- [x] 系统参数检查表
- [x] crontab：每月 28 日 20:30 自动执行
- [x] CLI：`bin/trade run monthly --json` / `--month 2026-04`

### P4-4 数据源稳定性加固

**问题**：04-10 实测发现东财数据不稳定，导致评分偏低（6.8/6.9 < 阈值 7）。

**方案**：
- [x] 评分引擎增加 `data_quality` 标记：`ok` / `degraded` / `error`
- [x] 基本面评分 detail 中标注缺失字段（`⚠️缺失:营收,现金流`）
- [x] 核心池 snapshot 保留 `data_quality / data_missing_fields`
- [x] 盘前/收盘 Discord 推送增加数据质量标记
- [x] `doctor` 增加数据源健康度检查：最近 N 次运行可用率、缓存新鲜度、字段缺失率、最后成功时间
- [x] 评分引擎遇到 `data_quality: degraded` 时，显示最近一次有效评分作为参考值，不直接替代当前分数
  - 2026-05-19：`score.calculated` 降级事件追加 `previous_valid_score`，Discord 评分区展示“上次有效评分”，仅作参考。
- [x] `data_quality: error` 时新增买入建议降级为 blocked 或 manual_review

### P4-5 Hermes 交互增强

**问题**：Hermes-Agent 目前通过 workflow/CLI 被动调用，缺少轻量查询命令。

**方案**：
- [x] 新增 `trade digest` 命令：一句话总结今日状态（给 Hermes 生成摘要用）
- [x] `trade suggest` 命令：基于当前状态输出下一步建议（买什么 / 卖什么 / 等待）
- [x] `trade explain <code>` 命令：解释某只股票的评分明细和决策逻辑
- [x] 这三个命令的输出格式对 Hermes 友好（结构化 JSON + 人类可读摘要）
  - 2026-05-19：新增 `digest`、`suggest`、`explain CODE` 只读命令；输出包含中文摘要、结构化 next_action 和人工确认护栏。

### P4-6 历史信号镜像

**问题**：当前历史回测仍以价格重建 + 规则代理为主，缺少“当天系统实际看到了什么”的按日留痕，无法精确回答某只股票为什么没进池、没过分数、被 veto，或因为组合决策而未执行。

**方案**：
- [x] ledger 增加历史快照表：`market snapshot / pool snapshot / scored candidates / today_decision`
- [x] 每次 `screener run/refresh` 运行生成统一 `history_group_id`，把四类对象作为同一次信号运行归档
- [x] `core_pool_scoring` 同步归档评分历史，供核心池单独复核
- 2026-05-19：新增 `signal_history_snapshots` schema v4、`platform/history_mirror.py`、
  `atrade history signal --date YYYY-MM-DD --code CODE --json`；`screener run/refresh`
  和 `scoring` 会把 market / pool / candidates / decision 归入同一 `history_group_id`。
- [x] `morning / noon / evening` 补齐 market snapshot history，保留时点差异
- [x] `historical_pipeline.py` 优先读取历史信号镜像，缺失时才回退到 proxy replay
- [x] 新增历史镜像诊断入口：按 `snapshot_date / history_group_id` 查看 market / pool / candidates / decision
- [x] 单股验证报告优先引用历史镜像中的真实 miss reason，而不是仅靠事后代理分类
- 2026-05-19：`morning / noon / evening` 已按各自 phase 归档 market snapshot；
  `backtest` 默认启用 `--history-mirror`，找不到镜像时回退代理回放；`stock analyze`
  输出 `history_signal` 并在 findings 中展示历史镜像 miss reason。

---

## P5：参数校准期（持续）

> 目标：用真实交易数据反哺策略参数，形成"实盘 → 回测 → 调参 → 实盘"闭环。

> **2026-04-11 进展**：通过 80 只股批量回测对比，选定「保守验证C」参数写入 `strategy.yaml` 主配置。
> 实盘验证启动后，将积累真实 MFE/MAE 数据推进 P5-1 的自动校准工作。

### P5-1 回测参数自动校准

**前置**：需要至少 20-30 笔闭合交易（约 2-3 个月实盘数据）。

**方案**：
- [x] 新增 `pipeline/param_calibration.py`
- [x] 从实盘闭合交易提取真实 MFE/MAE 分布
- [x] 基于 MFE 分布校准止盈参数（t1_pct / t1_drawdown / t2_drawdown）
- [x] 基于 MAE 分布校准止损参数（stop_loss / absolute_stop）
- [x] 基于持仓天数分布校准时间止损（time_stop_days）
- [x] 输出校准建议报告，不自动修改 `strategy.yaml`
- [x] 每次参数建议记录版本、样本窗口、walk-forward 验证结果
- [x] CLI：`bin/trade calibrate --json`
- 2026-05-19：`atrade calibrate --json` 已接入；默认至少 20 笔闭合复盘才输出可执行参数建议，`--record` 追加 `strategy.calibration.proposed` 和 Markdown artifact。

### P5-2 评分权重优化

**方案**：
- [x] 收集每笔交易的入场评分 vs 最终盈亏
- [x] 分析四个维度（技术/基本面/资金/舆情）哪个对盈亏的预测力最强
- [x] 用 walk-forward 的 sweep 结果交叉验证
- [x] 保留独立验证窗口，避免用同一批样本反复调参
- [x] 输出权重调整建议
- 2026-05-19：校准报告按来源评分事件关联 `trade.review.recorded`，输出四维相关性和 increase/decrease/hold 建议；样本不足时只报缺口。

### P5-3 选股条件优化

**方案**：
- [x] 统计核心池股票的"入池后 N 日表现"
- [x] 分析哪些 veto 规则最有效（拦截了多少真正的亏损）
- [x] 分析哪些 veto 规则过于严格（误杀了多少盈利机会）
- [x] 基于数据调整 `screening.mx_query` 和 `scoring.veto` 列表
- 2026-05-19：校准报告统计 candidate 入池后 5 日表现、veto 触发频率和选股条件复核建议；只输出建议，不自动改 `screening.mx_query` 或 `scoring.veto`。

---

## P6：智能进化（长期）

> 目标：让系统具备自适应能力，减少人工干预；P4/P5 数据不足前不启动。

### P6-1 自适应风控

**方案**：
- [x] 根据近期市场波动率生成止损幅度调整建议（高波动放宽、低波动收紧）
- [x] 根据账户净值曲线生成仓位上限调整建议（回撤期建议降仓）
- [x] 根据连续盈亏状态生成买入阈值调整建议（连亏后提高门槛）
- 2026-05-19：`atrade risk adaptive --json` 已接入；默认只输出建议和证据缺口，`--record` 追加 `risk.adaptive_suggestion.proposed` 和 Markdown artifact，不自动改配置、不自动下单。

### P6-2 多策略框架

**方案**：
- [x] 支持多个独立策略 profile 对比（不同的评分权重 / 选股条件 / 风控参数）
- [x] 每个策略按已有 profile 运行证据统计决策分布、复盘均值和胜率
- [x] 策略间隔离资金桶建议，互不影响（建议层，不自动改真实账户）
- [x] 定期对比策略表现，输出弱策略暂停候选
- 2026-05-19：`atrade strategy profiles --json` 已接入；匹配 `config_versions`、`run_log`、`decision.suggested`、`trade.review.recorded`，`--record` 追加 `strategy.profile_comparison.proposed` 和 Markdown artifact；只输出建议，不自动切换 `ASTOCK_CONFIG_PROFILE`。
- 2026-05-19：`atrade strategy allocation --json` 已接入；生成 profile 隔离资金桶、影子验证、暂停候选和人工复核建议，`--record` 追加 `strategy.capital_allocation.proposed` 和 Markdown artifact；不自动分配资金、不停用 profile。

### P6-3 深度归因分析

**方案**：
- [x] 按行业 / 市值 / 持仓天数 / 入场信号类型分组统计盈亏
- [x] 识别系统的"能力圈"：哪类股票赚钱、哪类亏钱
- [x] 时间维度分析：哪个时间段（周几 / 月初月末 / 财报季）表现好
- [x] 输出"策略体检报告"
- 2026-05-19：`atrade strategy health --json` 已接入；读取 `trade.review.recorded`、交易前假设和来源评分证据，输出分组归因、能力圈和证据缺口，`--record` 追加 `strategy.health_report.proposed` 和 Markdown artifact。

### P6-4 Web 仪表盘

**方案**：
- [x] 仪表盘只读数据契约，读取结构化账本
- [ ] 轻量 Web 服务（FastAPI / Streamlit），消费 `atrade dashboard snapshot --json`
- [ ] 手机可访问：持仓概览 / 今日决策 / 告警列表 / 模拟盘对比
- [ ] 历史净值曲线 / 回撤图 / 胜率趋势
- [ ] 不做交易操作，只做展示
- 2026-05-19：`atrade dashboard snapshot --json` 已接入；汇总持仓、资金、候选池、人工确认、市场状态、运行记录和报告 artifact；只读，不提供交易操作。

### P6-5 券商 API 接入（可选）

**方案**：
- [ ] 评估券商 API 可行性（华泰 / 东财 / 同花顺）
- [ ] 先做"半自动"：系统生成订单 → 推送到券商 APP 的条件单
- [ ] 再做"全自动"：系统直接下单（需要严格的风控保护）
- [ ] 全自动模式必须有"熔断开关"：单日亏损超限自动停止

---

## 下一步优先级

### P0：马上做

1. **逐笔模拟盘 vs 实盘对账** — 先定义 `signal_id/order_id` 和偏离类型，否则月报只能看总数。
2. **单日异常保护** — 连续失败 pipeline、关键数据源 error、组合风控 block 时停止新增交易。

### P1：本月内做

3. **最近有效评分参考** — `data_quality=degraded` 时展示最近一次有效评分，只作为参考不替代当前评分。
4. **收盘/周报增加模拟盘独立统计** — 等模拟盘至少跑满一周后接入。
5. **Hermes 轻量交互命令** — `digest / suggest / explain`，基于现有 `status today` 和评分明细实现。

### P2：延后

6. **参数校准** — 需要 20+ 笔实盘闭合交易。
7. **Web 仪表盘 / 多策略 / 券商 API** — 等系统稳定运行 3 个月后再评估。

---

## 成功标准

| 阶段 | 标准 |
|------|------|
| P4 完成 | 系统连续运行 4 周无重大故障；至少 1 个完整实盘买卖周期；模拟盘有 10+ 笔闭合交易；模拟盘/实盘偏离可逐笔解释 |
| P5 启动 | 实盘有 20+ 笔闭合交易；参数校准报告可输出；校准结果有独立验证窗口 |
| P6 启动 | 系统稳定运行 3 个月；月度复盘数据完整；风控和对账无未解释重大偏离 |
