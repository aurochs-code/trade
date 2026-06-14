# A-Stock Trading 系统深度评估报告

> 评估日期：2026-06-13
> 评估范围：全系统 6 维度
> 代码版本：commit `3f3ae9f`，branch `codex/strategy-recommendation-optimization`

---

## 总评

**整体架构设计质量很高。** 事件溯源 + 纯函数策略引擎 + 显式依赖组装的组合，在个人量化交易系统中属于第一梯队。核心链路（数据采集→评分→决策→风控→执行）闭环完整，测试覆盖 749 条。

**但系统正在从一个"个人工具"向"半自动交易系统"演进，在这个临界点上暴露出一些结构性债务。** 主要集中在：硬编码阈值分散、`BacktestEngine.run()` 主循环缺端到端测试、日内风控执行缺口、候选池缺少显式容量与过期淘汰策略。

---

## 一、架构 & Pipeline

### 优势
- **严格的事件溯源基础**：`event_log` 是单一事实来源，所有写入通过追加完成，`projection_*` 表可完全重建
- **纯函数核心**：`Scorer`、`Decider`、`RiskRules`、`PositionSizing` 均无 IO，可在回测和实盘间共享
- **配置冻结**：每次 pipeline 运行锁定不可变 `ConfigSnapshot` + SHA256 哈希，无热重载竞态
- **显式依赖组装**：`service_factory.py` 手工构造函数注入，无魔法 DI 容器
- **幂等性守卫**：`should_skip_pipeline` 防止同日重复运行
- **数据源熔断 + 回退链**：`SourceRouter` 提供 provider 级熔断、超时、重试，多级 fallback

### 问题

| 严重度 | 问题 | 位置 |
|--------|------|------|
| **高** | `auto_trade.py` 1800 行单体，混合了买卖、诊断、报告三大职责 | `pipeline/auto_trade.py` |
| **高** | `asyncio.run()` 在同步函数中嵌套调用，循环中可能触发事件循环冲突 | `pipeline/scoring.py` 等多处 |
| **中** | `ctx: Any` 类型擦除，全 pipeline 代码失去静态检查 | 所有 pipeline 文件 |
| **中** | 适配器优先级硬编码在 `service_factory.py`，不可运行时配置 | `platform/service_factory.py` |
| **中** | `platform/db.py` SQLite/MySQL 双路径分支增加维护成本，WAL 模式不一致 | `platform/db.py` |
| **低** | 配置版本号基于本地时间，时钟跳跃可能导致冲突 | `platform/config.py` |
| **低** | `_deep_merge` 使用递归，理论上存在栈溢出风险（当前深度安全） | `platform/config.py` |

---

## 二、策略 & 评分

### 优势
- **四维评分体系完整**：技术面(3.0) + 基本面(3.0) + 资金流(2.0) + 舆情(3.0)，权重 4:2.5:3:0.5
- **否决机制清晰**：硬否决强制归零，软否决降级警告，`consecutive_outflow` 区分趋势上下行
- **8 条策略路线**：覆盖趋势、突破、回踩、金叉、龙头、延续等多种形态
- **路线识别已优化**：新增 `status`/`route_score`/`matched_conditions`/`missing_conditions`，支持三档路线状态
- **延续策略评分使用连续归一化**：`_scale()`/`_inverse_scale()` 避免阈值边缘跳跃
- **风格自适应**：slow_bull ↔ momentum 切换，含保险丝机制

### 问题

| 严重度 | 问题 | 位置 |
|--------|------|------|
| **高** | **几十个硬编码幻数**：动量阈值(5/2/0)、ROE 阈值(15/10/5)、主力流入阈值(10亿/5亿)、路线条件中的乖离率、量比、RSI 范围等，均不在 YAML 配置中 | `scorer.py`、`continuation_scorer.py` |
| **高** | **评分未校准**：无 Z-score 归一化、无百分位排名、无跨市场环境标准化。同一只股票跨天分数跳跃可能仅因量比 1.49→1.51 | `scorer.py` |
| **中** | **维度不独立**：技术面与资金流高度相关，高分票同时获得金叉+动量+流入+放量突破分数，同一市场行为被重复计算 | `scorer.py` |
| **中** | **配置漂移**：`red_market` 放在 scorer veto 配置但实际由 `decider.py` 市场门控承接；`earnings_bomb` 配置存在但 scorer 未实现 | `scorer.py:254-278`、`decider.py:89` |
| **中** | **基本面阈值无行业归一化**：银行 ROE 15% 和科技 ROE 15% 含义完全不同，但评分相同 | `scorer.py:193` |
| **中** | **资金流对小盘股不友好**：主力净流入 5 亿门槛对中小盘股偏高，虽然 `_flow_strength_confirmed` 已用占比补充 | `scorer.py:220-227` |
| **低** | **路线置信度硬编码**：0.92/0.86/0.84/0.78/0.62 等不是从回测中推导出来的 | `scorer.py` |
| **低** | **舆情权重 0.5**：数据缺失时默认 1.5/3.0，对总分影响微乎其微 | `scorer.py:244` |

---

## 三、风控

### 优势
- **多层风控体系**：单票止损/移动止盈/时间止损/MA 退出 + 组合日亏损/连续亏损/集中度/冷却
- **大盘择时**：GREEN/YELLOW/RED/CLEAR 四级信号，影响仓位乘数
- **自适应风控建议**：基于波动率和回撤的止损/仓位调整建议（人工确认后生效）
- **试买机制**：TRIAL_BUY 不执行真实交易，作为低置信信号记录
- **入场门控链完整**：分数 → 入场信号 → 数据质量 → 缺失字段 → 关键字段 → 仓位空间 → 周限额 → 市场信号
- **风格切换保险丝**：单日暴涨 >7% 或 RSI>75 持续 3 天自动切换风控规则

### 问题

| 严重度 | 问题 | 位置 |
|--------|------|------|
| **高** | **日内无自动止损执行**：`intraday_monitor.py` 只发 Discord 告警，不执行卖出。止损只在主 auto_trade 周期（日终/定时）执行 | `pipeline/intraday_monitor.py` |
| **高** | **相关性敞口检查已配置但未实现**：`max_correlation_group_exposure_warn_pct: 0.50` 在 YAML 中存在，`rules.py` 中完全未引用 | `risk/rules.py`、`config/strategy.yaml` |
| **中** | **移动止盈已实现，但缺少保本止损/实时峰值级追踪**：`trailing_stop` 基于历史 K 线最高价触发，止损价不随价格上涨上移（无保本止损） | `risk/rules.py:73-85` |
| **中** | **无最大回撤硬止损**：组合回撤仅触发自适应建议，不自动减仓 | `pipeline/adaptive_risk.py` |
| **中** | **日亏损限额仅告警**：3% 日亏损触发违规记录但不强制平仓 | `risk/rules.py:138-144` |
| **低** | **无分批建仓/金字塔加仓**：每个代码一次买入，不支持递增部署 | `execution/positions.py` |
| **低** | **无凯利公式/最优 f 仓位**：仓位基于固定比例，非基于胜率/赔率 | `risk/sizing.py` |

---

## 四、数据源 & 质量

### 优势
- **多源冗余**：13+ 数据源（MX、AkShare、Tushare、Baostock、百度、腾讯、同花顺、东财、mootdx、OpenCLI、新浪 等）
- **分层回退链**：行情(MX→Tushare→OpenCli→Mootdx→AkShare→Baostock)，财务(Tushare→腾讯→AkShare HK→AkShare CN)，资金(Tushare→百度→AkShare)
- **Provider 级熔断**：连续失败 N 次后冷却，防止雪崩
- **报价-K线交叉验证**：实时价与 K 线收盘价偏差 >40% 拒绝
- **缓存模式完整性检查**：`get_cached` 验证缓存包含 `StockQuote` 所有字段
- **K 线成交量有效性检查**：区分真实成交量与回退零值
- **非交易日智能处理**：非交易日必要源过期不阻止新交易
- **财务字段级合并**：多 provider 逐字段回退，不覆盖已有好数据

### 问题

| 严重度 | 问题 | 位置 |
|--------|------|------|
| **中** | **MX 舆情无回退**：`MXSentimentAdapter` 是舆情唯一 provider，无 fallback。故障时只能默认为 1.5 | `data_source_diagnostics.py` |
| **中** | **akshare 是隐式 SPOF**：东财/新浪/腾讯/同花顺数据全走 akshare 库，akshare 上游变更影响多个"不同"数据源 | `market/akshare_adapters.py` |
| **低** | **无 API 调用配额跟踪**：快速轮询可能触发外部 provider 限流，无预警 | `market/service.py` |
| **低** | **Baostock 实时数据是伪实时**：回退到最新日 K 线收盘价 | `market/baostock_adapters.py` |

---

## 五、测试 & 验证

### 优势
- **61 个测试文件，749 个 pytest 用例**
- **策略纯函数测试覆盖极高**：Scorer(20 tests)、Decider(22 tests)、路线检测全部覆盖
- **数据源回退链测试全面**：Mock 各类失败 provider，验证 fallback 行为
- **风控规则良好覆盖**：止损/移动止盈/时间止损/MA 退出/Market Timer
- **Pipeline 集成测试**：含真实 SQLite + EventStore
- **CLI 测试投入大**：`test_cli.py` 6981 行、`test_agent_diagnostics_cli.py` 3315 行

### 问题

| 严重度 | 问题 | 位置 |
|--------|------|------|
| **高** | **`BacktestEngine.run()` 主循环缺端到端测试**：日级评分→买卖→风控→权益跟踪的主循环无直接测试（backtest 目录有 history mirror 和 continuation backtest 测试，但主循环未覆盖） | `backtest/engine.py` |
| **高** | **回测 `_compute_indicators()` 独立于生产代码**：与 `market/service.py` 中的指标计算重复实现，存在分化风险 | `backtest/engine.py` |
| **中** | **无参数化测试**：没有 `@pytest.mark.parametrize`，大量边界值测试靠手写重复代码 | 全测试套件 |
| **中** | **无 CI 配置**：没有 GitHub Actions / Jenkins / CI 配置文件 | 项目根目录 |
| **中** | **无回归对比机制**：修改策略参数后无法自动对比回测结果 | - |
| **低** | **无属性测试/Fuzz 测试** | - |
| **低** | **无性能基准测试** | - |

---

## 六、CLI & 候选池

### 优势
- **CLI 命令丰富**：25+ 子命令覆盖筛选/评分/分析/交易/风控/诊断/报告
- **MCP 接口完整**：40+ MCP tools 暴露核心功能给 AI agent
- **候选池三级管理**：core/watch/radar + 连续晋级天数 + 入场信号加速
- **选股器多源召回**：MX 搜索 + 同花顺热门 + 近期信号召回 + 现有池保留
- **来源预算分配**：按优先级分配候选名额

### 问题

| 严重度 | 问题 | 位置 |
|--------|------|------|
| **高** | **候选池缺少显式容量与过期淘汰策略**：无 `max_pool_size` / TTL / 全局淘汰上限 | `platform/cli/screener.py` |
| **高** | **无退市/更名处理**：退市股票记录永久残留，直到分数低于拒绝线（可能永远不会） | `platform/cli/screener.py` |
| **中** | **并发写入竞态**：screener 和 scoring pipeline 独立写 `projection_candidate_pool`，读-决定-写窗口内无事务锁 | `platform/cli/screener.py:1454-1621` |
| **中** | **池新鲜度依赖外部 cron**：morning/evening pipeline 不执行筛选，只读现有池；如果 screener refresh cron 停运，池会停滞 | `pipeline/morning.py` |
| **中** | **投影重建时并发读可能看到空表**：`rebuild_all()` 先 `DELETE FROM` 再重建，无表锁 | `reporting/projectors.py` |
| **低** | **`explain` 回望期依赖 event_store**：如果长时间未运行，event_store 可能包含过时数据 | `platform/hermes_commands.py` |

---

## 七、交叉关注点

### 7.1 异步/同步混合
`MarketService` 是全异步的，但多个 pipeline 通过 `asyncio.run()` 在同步函数中调用。在循环和重复调用场景下存在事件循环冲突风险。建议统一为 async pipeline 或使用明确的同步门面。

### 7.2 错误处理不一致
IO 回调（Discord 通知、Obsidian 写入）普遍采用「吞异常」模式——`try/except Exception: log`。基础设施故障不会向上传播，依赖人工监控发现。建议区分可恢复错误和需告警错误。

### 7.3 配置漂移
- `veto: red_market` — 在 `strategy.yaml` 的 `scoring.veto` 中配置，但 `scorer._check_veto()` 未检查。`red_market` 的实际门控在 `decider.py:89` 的市场信号检查中实现，属于 decision gate 而非 scorer veto。建议清理 `scoring.veto` 配置：将 `red_market` 从 veto 列表移除（它已是 decision gate），或显式注明其门控位置。
- `veto: earnings_bomb` — 同上，在配置中存在但 scorer 未实现。建议要么实现，要么从配置中移除。
- `max_correlation_group_exposure_warn_pct: 0.50` — YAML 中已定义，`rules.py` 未消费。

---

## 八、优先级行动项

### P0（立即修复）
1. **配置漂移清理**：`scoring.veto` 中的 `red_market` 移到 decision gate 或从 veto 列表移除（它已在 `decider.py` 实现）；`earnings_bomb` 要么实现要么移除；`max_correlation_group_exposure_warn_pct` 要么实现要么移除
2. **候选池容量与过期治理**：加 `max_pool_size` + TTL 淘汰策略 + 退市/更名清理

### P1（本迭代）
3. **`BacktestEngine.run()` 测试**：至少一条端到端回测（单股、固定日期范围、验证收益计算）
4. **回测 `_compute_indicators()` 去重**：复用 `market/service.py` 的指标计算，或提取共享模块
5. **硬编码幻数外迁**：将 scorer 和 continuation_scorer 中的核心阈值提取到 `strategy.yaml`
6. **相关性敞口检查实现**：在 `rules.py` 中实现 `max_correlation_group_exposure`

### P2（下迭代）
7. **`auto_trade.py` 拆分**：分离买卖执行、诊断、报告为独立模块
8. **日内止损执行**：`intraday_monitor` 增加可选自动止损（需 dry-run 模式）
9. **保本止损**：止损价随价格上涨上移，锁定利润
10. **评分校准**：引入跨截面百分位排名或 Z-score 归一化

### P3（中长期）
11. **`asyncio.run()` 嵌套消除**：统一为 async pipeline
12. **参数化测试**：用 `pytest.mark.parametrize` 替换重复边界值测试
13. **CI/CD pipeline**：GitHub Actions 跑测试 + ruff + 回测回归
14. **属性测试**：对纯函数（Scorer/Decider）引入 Hypothesis
15. **`ctx: Any` 类型化**：替换为 `PipelineContext` 类型注解
