# 证据链深化 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把交易系统从“保留新增证据”推进到“历史可回填、LLM 摘要必须带证据编号、持仓到期自动复盘 MFE/MAE 与假设验证”。

**Architecture:** 继续遵守 append-only event_log：历史旧事件不改写，只追加 `evidence.backfilled` 或缺失的交易证据事件。LLM 摘要在上下文阶段给证据清单，在通知阶段做发送前校验。交易复盘以 `trade.hypothesis.recorded` 为入口，从 `market_bars` 计算到期 MFE/MAE 后追加 `trade.review.recorded`。

**Tech Stack:** Python、Typer CLI、SQLite/MySQL 兼容 SQL、pytest、ruff。

---

### Task 1: 旧事件证据回填

**Files:**
- Create: `src/astock_trading/platform/evidence.py`
- Modify: `src/astock_trading/platform/domain_events.py`
- Modify: `src/astock_trading/platform/cli/events.py`
- Test: `tests/astock_trading/platform/test_events_cli.py`

- [ ] 写失败测试：旧 `score.calculated`、`decision.suggested` 会生成 `evidence.backfilled`；旧 `order.filled` 会补 `trade.hypothesis.recorded` / `trade.outcome.recorded`，二次执行不重复。
- [ ] 运行：`.venv/bin/pytest tests/astock_trading/platform/test_events_cli.py -q`，预期失败。
- [ ] 实现最小回填服务和 `atrade events backfill-evidence --apply --json`。
- [ ] 重跑同一测试，预期通过。

### Task 2: LLM 摘要 evidence_id 强约束

**Files:**
- Modify: `src/astock_trading/platform/llm_context.py`
- Modify: `src/astock_trading/platform/cli/notifications.py`
- Test: `tests/astock_trading/platform/test_cli.py`

- [ ] 写失败测试：LLM context Markdown 出现证据编号清单；`notify llm-summary-card` 遇到缺少 `evidence_id` 的摘要返回失败。
- [ ] 运行对应测试，预期失败。
- [ ] 在上下文里生成 `evidence_registry`，在通知 CLI 里按章节校验 `evidence_id`。
- [ ] 重跑对应测试，预期通过。

### Task 3: 持仓到期 MFE/MAE 与假设验证复盘

**Files:**
- Create: `src/astock_trading/execution/review.py`
- Create: `src/astock_trading/platform/cli/review.py`
- Modify: `src/astock_trading/platform/cli/__init__.py`
- Modify: `src/astock_trading/platform/domain_events.py`
- Test: `tests/astock_trading/execution/test_trade_review.py`
- Test: `tests/astock_trading/platform/test_cli.py`

- [ ] 写失败测试：到达 `review_after_days` 后，根据 `market_bars` 计算 MFE/MAE 并追加 `trade.review.recorded`。
- [ ] 写失败测试：`bin/trade review trades --json` 存在，`--record` 会返回写入的复盘证据。
- [ ] 实现复盘服务和 CLI。
- [ ] 重跑复盘测试和 CLI 测试，预期通过。

### Task 4: 文档与验证

**Files:**
- Modify: `docs/architecture/DATA_MODEL.md`
- Modify: `docs/operations/RUNBOOK.md`

- [ ] 更新事件契约、运行命令和 LLM evidence_id 纪律。
- [ ] 运行聚焦 pytest 与 ruff。
- [ ] 汇报已验证命令、未覆盖风险和未提交状态。
