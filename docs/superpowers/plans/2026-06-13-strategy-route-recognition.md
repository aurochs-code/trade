# Strategy Route Recognition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让高分股票在已有资金确认、趋势延续、回踩转强等证据时，输出结构化路线状态、缺口诊断，并能在严格买入门槛之外进入观察/试买意向链路。

**Architecture:** 评分器继续作为纯函数输出 `ScoreResult`，新增路线状态和路线诊断，不放宽正式 `BUY` / core 晋级边界。决策器只把 `status="watch"` 且 `route_score >= 0.6` 的软路线作为 `TRIAL_BUY` 参考，仍受评分、数据质量、仓位和入场阻断约束。`stock analyze` 复用评分 payload 输出中文路线缺口，避免只显示“入场信号未触发”。

**Tech Stack:** Python dataclass, pytest, ruff, existing `bin/trade ... --json` CLI contract.

---

### Task 1: 结构化路线证据字段

**Files:**
- Modify: `src/astock_trading/strategy/models.py`
- Modify: `src/astock_trading/strategy/scorer.py`
- Test: `tests/astock_trading/strategy/test_scorer.py`

- [x] **Step 1: Write the failing test**

Add assertions that `trend_watch` keeps `entry_signal=False` but exposes:

```python
assert route.status == "watch"
assert route.route_score >= 0.6
assert "volume_ratio" in route.missing_conditions
assert "above_ma20" in route.matched_conditions
assert result.to_dict()["strategy_routes"][0]["status"] == "watch"
assert result.to_dict()["route_diagnostics"][0]["route"] == "trend_watch"
```

- [x] **Step 2: Run test to verify it fails**

Run:

```bash
.venv/bin/pytest tests/astock_trading/strategy/test_scorer.py::test_detects_trend_watch_route_when_volume_ratio_is_missing -q
```

Expected: FAIL with missing `status` / `route_score` fields.

- [x] **Step 3: Write minimal implementation**

Add `status`, `route_score`, `matched_conditions`, and `missing_conditions` to `StrategyRouteEvidence`. Add `StrategyRouteDiagnostic` and `ScoreResult.route_diagnostics`. In `Scorer.score()`, return both matched routes and diagnostics from route evaluation.

- [x] **Step 4: Run test to verify it passes**

Run:

```bash
.venv/bin/pytest tests/astock_trading/strategy/test_scorer.py::test_detects_trend_watch_route_when_volume_ratio_is_missing -q
```

Expected: PASS.

### Task 2: 资金趋势和板块路线放宽为可诊断条件组

**Files:**
- Modify: `src/astock_trading/strategy/scorer.py`
- Test: `tests/astock_trading/strategy/test_scorer.py`

- [x] **Step 1: Write the failing tests**

Add tests for:

```python
assert result.primary_strategy_route == "flow_confirmed_trend"
assert route.entry_signal is True
assert route.status == "entry"
assert "flow_strength" in route.matched_conditions

assert dragon.entry_signal is False
assert dragon.status == "watch"
assert dragon.confidence == 0.6
assert "sector_strength" in dragon.missing_conditions
```

- [x] **Step 2: Run tests to verify they fail**

Run:

```bash
.venv/bin/pytest tests/astock_trading/strategy/test_scorer.py::test_detects_flow_confirmed_trend_when_relative_volume_is_low tests/astock_trading/strategy/test_scorer.py::test_dragon_head_route_without_sector_is_watch_not_entry -q
```

Expected: FAIL with missing route condition fields and absent dragon-head watch behavior.

- [x] **Step 3: Write minimal implementation**

Use condition helpers to populate matched/missing conditions. Change `flow_confirmed_trend`资金条件 from absolute `flow_net >= 5e8` to `flow_net >= 3e8 or (flow_net >= 1e8 and flow_net / amount >= 0.05)`. Keep final formal entry route strict enough on trend and risk controls. For `dragon_head`, confirmed sector keeps entry; missing sector becomes watch at confidence `0.6`; explicit weak sector stays blocked/diagnostic only.

- [x] **Step 4: Run tests to verify they pass**

Run:

```bash
.venv/bin/pytest tests/astock_trading/strategy/test_scorer.py::test_detects_flow_confirmed_trend_when_relative_volume_is_low tests/astock_trading/strategy/test_scorer.py::test_dragon_head_route_without_sector_is_watch_not_entry -q
```

Expected: PASS.

### Task 3: 软路线打通 TRIAL_BUY

**Files:**
- Modify: `src/astock_trading/strategy/decider.py`
- Test: `tests/astock_trading/strategy/test_decider.py`

- [x] **Step 1: Write the failing tests**

Add tests that a score above `trial_buy_entry_signal_threshold` with a `watch` route and no entry signal can become `TRIAL_BUY`, while lower score or bad data quality remains `WATCH`:

```python
score = _make_score(5.6, entry_signal=False, strategy_routes=[watch_route])
assert decider.decide(score, market).action == Action.TRIAL_BUY
```

- [x] **Step 2: Run tests to verify they fail**

Run:

```bash
.venv/bin/pytest tests/astock_trading/strategy/test_decider.py::test_watch_route_near_buy_line_becomes_trial_buy tests/astock_trading/strategy/test_decider.py::test_low_score_watch_route_stays_watch -q
```

Expected: FAIL because `_trial_buy_allowed()` currently only accepts total trial line or true entry signal.

- [x] **Step 3: Write minimal implementation**

Add `_has_watch_route(score)` and allow `watch_route_reaches_trial_line` when `route.status == "watch"`, `route.route_score >= 0.6`, and `score.total >= trial_buy_entry_signal_threshold`. Keep data-quality and missing-field gates unchanged.

- [x] **Step 4: Run tests to verify they pass**

Run:

```bash
.venv/bin/pytest tests/astock_trading/strategy/test_decider.py::test_watch_route_near_buy_line_becomes_trial_buy tests/astock_trading/strategy/test_decider.py::test_low_score_watch_route_stays_watch -q
```

Expected: PASS.

### Task 4: 单股分析输出路线缺口

**Files:**
- Modify: `src/astock_trading/platform/stock_analysis.py`
- Test: `tests/astock_trading/platform/test_stock_analysis.py`

- [x] **Step 1: Write the failing test**

Build a `ScoreResult` with a watch route and route diagnostics, then assert:

```python
assert payload["score"]["route_diagnostics"][0]["missing_conditions"]
assert any("路线缺口：" in item for item in payload["findings"])
```

- [x] **Step 2: Run test to verify it fails**

Run:

```bash
.venv/bin/pytest tests/astock_trading/platform/test_stock_analysis.py::test_build_stock_analysis_payload_explains_route_missing_conditions -q
```

Expected: FAIL because findings do not yet include route-level missing conditions.

- [x] **Step 3: Write minimal implementation**

Include `route_diagnostics` in `ScoreResult.to_dict()`. Extend `_findings()` to append one concise Chinese route gap line using primary watch route or top diagnostic missing conditions.

- [x] **Step 4: Run test to verify it passes**

Run:

```bash
.venv/bin/pytest tests/astock_trading/platform/test_stock_analysis.py::test_build_stock_analysis_payload_explains_route_missing_conditions -q
```

Expected: PASS.

### Task 5: 验证与 CLI 合约

**Files:**
- No production file changes beyond previous tasks.

- [x] **Step 1: Run focused strategy tests**

Run:

```bash
.venv/bin/pytest tests/astock_trading/strategy/test_scorer.py tests/astock_trading/strategy/test_decider.py tests/astock_trading/platform/test_stock_analysis.py -q
```

Expected: PASS.

- [x] **Step 2: Run linter**

Run:

```bash
.venv/bin/ruff check src tests
```

Expected: PASS.

- [x] **Step 3: Run read-only CLI smoke checks**

Run:

```bash
bin/trade stock analyze 002384 --json
bin/trade screener explain --json
```

Expected: exit 0 and valid JSON with `strategy_routes` / `route_diagnostics` fields when route evidence exists.
