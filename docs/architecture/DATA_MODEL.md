# 数据模型

Runtime 数据库是 MySQL，schema 由 SQLAlchemy Core 定义并创建。金额存分（_cents 整数），JSON 放 *_json 字段，MySQL 使用 JSON 类型；SQLite 只作为测试替身和历史迁移源。

## 核心表

```sql
-- 业务事实 (append-only)
event_log (event_id PK, stream, stream_type, stream_version, event_type, payload_json, metadata_json, occurred_at)
  UNIQUE(stream, stream_version)

-- 规则版本
config_versions (config_version PK, config_hash UNIQUE, config_json, created_at, activated_at)

-- 运行记录
run_log (run_id PK, run_type, scope, config_version, status, started_at, finished_at, error_message, artifacts_json)

-- 历史信号镜像
signal_history_snapshots (snapshot_id PK, snapshot_date, history_group_id, run_id, phase, snapshot_type, payload_json, created_at)
```

## 市场观察表

```sql
market_observations (observation_id PK, source, kind, symbol, observed_at, run_id, payload_json)
market_bars (symbol+bar_date+period PK, open/high/low/close_cents, volume, amount_cents, source, fetched_at)
```

## 投影表（全部可删可重建）

```sql
projection_positions (code PK, name, style, shares, avg_cost_cents, cost_basis_cents, entry_date, ...)
projection_orders (order_id PK, code, side, shares, price_cents, status, broker, ...)
projection_balances (scope PK, cash_cents, total_asset_cents, weekly_buy_count, ...)
projection_candidate_pool (code+pool_tier PK, name, score, added_at, streak_days, note)
projection_market_state (index_symbol PK, name, signal, price_cents, change_pct, ...)
report_artifacts (artifact_id PK, run_id, report_type, format, content, delivered_to, created_at)
```

## 事件类型

### strategy
| event_type | 说明 |
|-----------|------|
| score.calculated | 四维评分完成；payload 保留各维度分数、说明和 raw_data 证据 |
| decision.suggested | 交易意图生成；payload 关联 source_score_event_id，并保存决策输入、市场状态和当时规则 |
| pool.promoted / demoted / removed | 池子变动 |

### risk
| event_type | 说明 |
|-----------|------|
| risk.stop_loss_triggered | 止损触发 |
| risk.trailing_stop_triggered | 移动止盈触发 |
| risk.time_stop_triggered | 时间止损 |
| risk.ma_exit_triggered | MA 跌破离场 |
| risk.portfolio_breach | 组合风控触发 |
| risk.position_sized | 仓位计算结果 |

### execution
| event_type | 说明 |
|-----------|------|
| order.created / filled / cancelled | 订单生命周期 |
| position.opened / closed | 持仓生命周期 |
| trade.hypothesis.recorded | 交易前假设，记录人工理由、验证点、失效条件、来源评分/决策事件 |
| trade.outcome.recorded | 交易后结果，记录成交状态、费用、成交后持仓和已实现盈亏 |
| trade.review.recorded | 到期交易复盘，记录 MFE/MAE、复盘日收益、假设验证状态和原始 K 线证据 |
| evidence.backfilled | 历史旧事件证据回填；只追加 legacy payload 和缺失字段说明，不改写原始事件 |

### platform
| event_type | 说明 |
|-----------|------|
| run.started / completed / failed | 运行生命周期 |

## 证据链约束

- 新事件尽量在原始 payload 中保存原始行情、原始分析、原始决策、交易前假设、交易后结果和复盘证据。
- 历史旧事件不允许 UPDATE 成“新格式”。回填只能追加 `evidence.backfilled` 或缺失的 `trade.*` 证据事件，并标记 `evidence_status=legacy_partial`。
- `trade.review.recorded` 的 MFE/MAE 来源是 `market_bars`，payload 中必须保留 `review_evidence.bars`，用于后续人工核对。
- LLM 摘要不是事实源。最终摘要引用任何判断时必须写 `evidence_id: ...`，证据编号来自 `event_log.event_id`、`market_observations.observation_id` 或其他明确事实编号。
