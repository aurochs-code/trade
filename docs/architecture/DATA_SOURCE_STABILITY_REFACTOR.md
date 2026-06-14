# 数据源稳定性重构方案

> 目标：让交易系统稳定运行，并让 agent 能明确区分“运行/核心数据源故障”“逐票增强源降级”“筛选后确实无合格候选”。

## 2026-05-20 实测结论

本次使用真实 MySQL 运行面排查，核心命令和结论如下：

- `atrade suggest --json` 会把近期运行失败、核心源故障和最近筛选 L1 覆盖不足都归入 `needs_health_check`，
  且 `execution_allowed=false`，建议文案为“先修运行/数据问题，暂停新增交易判断。”
- `atrade db check --json` 返回 `ok`，MySQL 运行库和 schema 没有损坏。
- `atrade data-sources status --json` 的核心门禁源可用：`hot_stocks`、`northbound_realtime`、`baidu_fund_flow` 均为 `healthy`。
- `atrade diagnose health --json` 显示当前是 `candidate_pool_freshness` / `core_pool` 降级或为空，不是核心市场数据断供。
- `atrade screener refresh --json` 实测筛出 75 只，最高分约 3.9，`added_to_watch=[]`，说明当前候选池为空主要是“筛选后无合格候选”，不是应该强行产生交易建议。
- 刷新过程中大量出现 `BaiduFundFlow` 非 JSON 响应，以及行业对比源东财/Sina fallback 失败，说明逐票增强源抖动明显。

## 当前根因

问题不是单一“没数据”，而是三类状态被混在一起：

1. **核心门禁源**：热榜、北向、资金流最近一次健康即可让 pipeline 继续。
2. **逐票增强源**：每只股票的资金流、行业对比、研报、公告可能失败；这些失败会降低单股证据质量，但不一定使全局门禁失败。
3. **策略结果**：候选池为空可能是候选确实低分、被否决、缺少入场信号，也可能是增强源缺失拖低评分。

因此只看 `data-sources status` 会低估逐票数据问题；只看候选池为空又容易误判为核心源故障。

## 立即执行原则

- `atrade suggest --json` 遇到近期运行/数据问题时继续返回“先修运行/数据问题，暂停新增交易判断。”
- `data_quality=ok` 不应与 `flow_detail=数据缺失` 同时出现。资金流缺失应让评分结果降级，并进入 `data_missing_fields`。
- 核心源健康但候选池为空时，报告应说“无合格候选”，不能说成“行情没数据”。
- 热榜只能用于召回和市场背景，不能替代实时行情、资金流和财务证据。

## 重构目标架构

### 1. 数据源分层

把源按运行语义拆成三层：

- **L0 运行门禁源**：`hot_stocks`、`northbound_realtime`、`baidu_fund_flow`。失败时阻断关键 pipeline，必须写入 run artifact。
- **L1 评分必需源**：单股行情、K 线技术指标、财务核心字段、逐票资金流。失败时不阻断全局 pipeline，但单股 `data_quality` 降级，并阻断买入意向。
- **L2 增强解释源**：公告、新闻、研报、行业/概念上下文、热点搜索。失败时只影响解释和置信度，不阻断筛选主链路。

### 2. 统一源状态模型

新增统一的 provider 结果结构，至少包含：

- `source`
- `kind`
- `symbol`
- `status`: `ok` / `empty` / `stale` / `timeout` / `parse_error` / `provider_error`
- `latency_ms`
- `payload_count`
- `error_type`
- `error_message`
- `observed_at`
- `fallback_from`

这些字段写入 `market_observations.payload_json`，并在 `run_log.artifacts_json` 汇总。不要只在 stderr 打日志。

### 3. Provider 路由和降级

为每类数据定义显式 provider 链：

- 行情/K 线：Tencent / AkShare / BaoStock / Mootdx 按可用性轮转。
- 财务：Tencent 估值字段 + AkShare 财报字段按字段合并。
- 资金流：优先稳定日级资金流；Baidu 返回非 JSON 时标记 `parse_error`，再走 AkShare/东财/Tencent tick 降级。
- 行业对比：Eastmoney -> Sina -> THS，THS 仅作最后兜底。

Provider 不应直接“失败返回空数组”而丢失原因；空结果和异常必须可区分。

### 4. 健康检查从“最近一次”升级为“覆盖率”

`evaluate_data_source_health()` 保留当前全局门禁，但新增逐票覆盖率：

- 本次评分 `quote_coverage`
- `technical_coverage`
- `financial_coverage`
- `flow_coverage`
- `sector_coverage`
- `provider_error_counts`
- `parse_error_counts`

例如本次刷新中全局资金流健康，但多只股票逐票资金流失败；这应表现为 `flow_coverage` 低，而不是全局 `baidu_fund_flow=healthy` 掩盖。

### 5. CLI/MCP 入口

CLI 先行，MCP 只做薄适配：

- 已新增 `atrade data-sources diagnose --json`：输出全局健康、最近筛选逐票覆盖率、
  provider 错误分布和最近失败样本。
- 扩展 `atrade screener refresh --json`：返回 `source_quality` 摘要，不只返回评分列表。
- 扩展 `atrade screener explain --json`：把“分数低”“硬否决”“数据缺失”“入场信号缺失”拆开。
- 后续 MCP 工具只调用这些 CLI/服务层能力，不另写一套判断。

## 分阶段实施

### P0：已处理

- `CLEAR` 在 Hermes digest/suggest/explain 面向用户输出中转义为“观望”。
- 评分器把资金流缺失纳入 `data_quality=degraded` 和 `data_missing_fields=["资金流"]`，避免“资金流缺失但数据质量正常”的误导。

### P1：可观测性

- 已给 `screener run` / `screener refresh` / `screener score` 的 JSON 输出增加
  `source_quality` 覆盖率摘要，统计本次逐票 `行情`、`技术指标`、`基本面`、
  `资金流`、`舆情`、`行业上下文` 覆盖率，并汇总评分 `data_quality` 和缺失字段。
- 已给 provider 失败增加 `kind=provider_failure` 结构化观测，不计入目标源成功样本；
  `data-sources status --json` / `evaluate_data_source_health()` 会返回最近失败样本、
  按 provider 和目标数据类型汇总的失败次数。
- provider 失败已拆分为 `resolved_recent` / `unresolved_recent`：同一股票 /
  数据类型里有后续成功观测时，失败样本标记 `resolved_by_fallback=true`，避免把
  “主源失败但 fallback 或后续重跑已成功”误读成当前仍缺数据。
- 已新增 `data-sources diagnose --json` 作为 CLI 一站式入口，汇总全局门禁、
  `provider_failures.unresolved` 和最近筛选 `source_quality` 覆盖率。
- `BaiduFundFlow` 非 JSON、资金流 provider 异常、行业/信号类 fallback 失败已写入结构化观测；
  AkShare 内部东财资金流 -> 腾讯 tick 降级也已拆出子源失败明细。
- 市场观测写库已对 numpy / pandas 标量做 JSON 安全转换，避免 AkShare fallback
  返回 `int64` 等类型时因序列化失败把有效资金流误判成缺失。

### P2：Provider 路由

- 已抽出 `src/astock_trading/market/source_router.py` 的 `SourceRouter`，按 provider
  链顺序处理 `ok` / `empty` / `timeout` / `provider_error` / `circuit_open`。
- `fund_flow` 和 signal 类增强源已接入 `SourceRouter`：支持单 provider 超时、
  transient error 重试、连续失败熔断冷却，并把每次失败 attempt 写成结构化
  `provider_failure` 观测。
- `fund_flow` 默认设置为 `timeout_seconds=15`、不重试、`max_failures=3`、
  `cooldown_seconds=300`；signal 类增强源默认 `timeout_seconds=10`、不重试、
  连续 3 次失败熔断 5 分钟。
- AkShare 腾讯 tick fallback 已屏蔽内部 “正在下载数据，请稍等” warning，避免
  `screener refresh --json` 的自动化输出被第三方库进度提示污染。
- BaiduFundFlow parse error 和行业对比内部 fallback 失败已降为 debug 日志；
  可观测性以 `provider_failure` 结构化观测和 `data-sources diagnose --json` 为准。
- 最近失败样本在 `data-sources diagnose --json` 中按 `provider_failures.recent`
  / `provider_failures.unresolved` 采样展示，避免 CLI 主输出被逐票失败刷屏。
- AkShareFlowAdapter 内部的东财资金流 -> 腾讯 tick 降级已拆出子源失败明细：
  两个子源都失败时，`provider_failure.details.provider_diagnostic.subsource_errors`
  会包含 `em_fund_flow_failed` / `tx_tick_failed` 等状态，并透传到
  `data-sources diagnose --json` 的 provider failure 样本里。

### P3：运行策略（已处理）

- 核心源失败：关键 pipeline 会先尝试刷新核心源；刷新后仍失败则返回
  `data_source_health_failed`，并把 `data_sources` / `data_source_refresh`
  写入 `run_log.artifacts_json`。
- 逐票 L1 覆盖低：pipeline 可完成，但 `atrade suggest --json` 和
  `atrade propose-plan --json` 会返回 `data_source_blockers`，下一步指向
  `atrade data-sources diagnose --json`，暂停新增交易判断，直到覆盖率恢复或人工确认降级运行。
- 候选池为空且核心源健康：`atrade suggest --json` 返回
  `wait_no_qualified_candidates`，表述为“核心数据源可用，候选池为空；继续观察，不降低买入线”，
  不把它误报成行情断供。

### P4：来源补强

- 已把付费稳定源调整为主源策略：MX 负责实时/日线级行情和选股搜索主入口；
  Tushare SDK 负责日线/复权 K 线、指数日线、每日指标、财务指标、个股资金流、
  股票基础信息、龙虎榜、限售解禁和沪深股通持股等常规积分接口。
- `ASTOCK_TUSHARE_TOKEN` 存在时，provider 顺序为：
  - 行情/K 线：`MXMarketAdapter -> TushareMarketAdapter -> AStockSignal/OpenCli/Mootdx/AkShare/BaoStock`
  - 财务：`TushareFinancialAdapter -> TencentFinancialAdapter -> AkShare`
  - 资金流：`TushareFlowAdapter -> BaiduFundFlowAdapter -> AkShareFlowAdapter`
- Tushare token 不是启动硬依赖；缺失时自动禁用 Tushare provider，并在
  `atrade data-sources diagnose --json` 的 `optional_providers.tushare` 中展示
  `enabled=false`，不会泄露 token 明文。
- 6000 积分档按常规积分接口使用；分钟、新闻、公告和 10000+ 特色数据仍按官方独立权限
  或更高积分要求处理，不在代码里假设已开通。
- 保留当前 `CLI + MCP + MySQL + 人工确认` 边界，不引入 MongoDB/Redis 式外部运行核心。
- 对外部源只借鉴 provider 优先级、fallback、缓存和健康可视化，不复制整套架构。

## 验证命令

```bash
atrade suggest --json
atrade db check --json
atrade data-sources diagnose --json
atrade data-sources status --json
atrade diagnose health --json
atrade screener refresh --json
atrade screener explain --json
```

稳定运行的判定不是“每天都有买入候选”，而是：

- 运行库健康；
- 核心源状态可解释；
- 逐票源失败可计数、可降级、可追溯；
- 候选池为空时能明确说明是“无合格候选”还是“证据链不足”；
- `suggest` 在数据/运行问题未修复前明确暂停新增交易判断。
