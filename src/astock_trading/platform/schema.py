"""SQLAlchemy Core schema for runtime databases."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Column,
    Float,
    Index,
    Integer,
    JSON,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
)

metadata = MetaData()

event_log = Table(
    "event_log",
    metadata,
    Column("event_id", String(64), primary_key=True),
    Column("stream", String(255), nullable=False),
    Column("stream_type", String(64), nullable=False),
    Column("stream_version", Integer, nullable=False),
    Column("event_type", String(128), nullable=False),
    Column("payload_json", JSON, nullable=False),
    Column("metadata_json", JSON, nullable=False),
    Column("occurred_at", String(64), nullable=False),
    UniqueConstraint("stream", "stream_version", name="uq_event_log_stream_version"),
    Index("idx_event_log_type", "event_type"),
    Index("idx_event_log_stream", "stream"),
    Index("idx_event_log_occurred", "occurred_at"),
)

event_streams = Table(
    "event_streams",
    metadata,
    Column("stream", String(255), primary_key=True),
    Column("stream_type", String(64), nullable=False),
    Column("next_version", Integer, nullable=False),
    Column("updated_at", String(64), nullable=False),
    Index("idx_event_streams_type", "stream_type"),
)

config_versions = Table(
    "config_versions",
    metadata,
    Column("config_version", String(128), primary_key=True),
    Column("config_hash", String(64), nullable=False, unique=True),
    Column("config_json", JSON, nullable=False),
    Column("created_at", String(64), nullable=False),
    Column("activated_at", String(64)),
)

run_log = Table(
    "run_log",
    metadata,
    Column("run_id", String(128), primary_key=True),
    Column("run_type", String(64), nullable=False),
    Column("scope", String(64), nullable=False, default="cn_a"),
    Column("config_version", String(128), nullable=False),
    Column("data_cutoff", String(64)),
    Column("status", String(32), nullable=False, default="running"),
    Column("started_at", String(64), nullable=False),
    Column("finished_at", String(64)),
    Column("error_message", Text),
    Column("artifacts_json", JSON),
    Index("idx_run_log_type_date", "run_type", "started_at"),
)

market_observations = Table(
    "market_observations",
    metadata,
    Column("observation_id", String(64), primary_key=True),
    Column("source", String(128), nullable=False),
    Column("kind", String(128), nullable=False),
    Column("symbol", String(64), nullable=False),
    Column("observed_at", String(64), nullable=False),
    Column("run_id", String(128)),
    Column("payload_json", JSON, nullable=False),
    UniqueConstraint("source", "kind", "symbol", "observed_at", name="uq_market_obs"),
    Index("idx_market_obs_symbol", "symbol", "kind", "observed_at"),
    Index("idx_market_obs_kind_observed", "kind", "observed_at"),
)

market_bars = Table(
    "market_bars",
    metadata,
    Column("symbol", String(64), primary_key=True),
    Column("bar_date", String(32), primary_key=True),
    Column("period", String(32), primary_key=True, default="daily"),
    Column("open_cents", Integer, nullable=False),
    Column("high_cents", Integer, nullable=False),
    Column("low_cents", Integer, nullable=False),
    Column("close_cents", Integer, nullable=False),
    Column("volume", BigInteger, nullable=False),
    Column("amount_cents", BigInteger, nullable=False),
    Column("source", String(128), nullable=False),
    Column("fetched_at", String(64), nullable=False),
)

market_price_bars = Table(
    "market_price_bars",
    metadata,
    Column("symbol", String(64), primary_key=True),
    Column("bar_date", String(32), primary_key=True),
    Column("period", String(32), primary_key=True, default="daily"),
    Column("adjustflag", String(8), primary_key=True, default="2"),
    Column("source", String(128), primary_key=True),
    Column("open_cents", Integer, nullable=False),
    Column("high_cents", Integer, nullable=False),
    Column("low_cents", Integer, nullable=False),
    Column("close_cents", Integer, nullable=False),
    Column("volume", BigInteger, nullable=False),
    Column("amount_cents", BigInteger, nullable=False),
    Column("change_pct", Float),
    Column("fetched_at", String(64), nullable=False),
    Column("raw_json", JSON),
    Index("idx_market_price_symbol_date", "symbol", "bar_date"),
    Index("idx_market_price_date", "bar_date"),
)

market_financials = Table(
    "market_financials",
    metadata,
    Column("symbol", String(64), primary_key=True),
    Column("report_year", Integer, primary_key=True),
    Column("report_quarter", Integer, primary_key=True),
    Column("source", String(128), primary_key=True),
    Column("report_date", String(32), nullable=False),
    Column("available_date", String(32), nullable=False),
    Column("roe", Float),
    Column("roe_3y_ago", Float),
    Column("revenue_growth", Float),
    Column("net_profit_growth", Float),
    Column("operating_cash_flow", Float),
    Column("pe_ttm", Float),
    Column("pb", Float),
    Column("debt_ratio", Float),
    Column("fetched_at", String(64), nullable=False),
    Column("raw_json", JSON),
    Index("idx_market_financials_available", "symbol", "available_date"),
    Index("idx_market_financials_report", "symbol", "report_year", "report_quarter"),
)

market_fund_flows = Table(
    "market_fund_flows",
    metadata,
    Column("symbol", String(64), primary_key=True),
    Column("trade_date", String(32), primary_key=True),
    Column("source", String(128), primary_key=True),
    Column("net_inflow_1d", Float),
    Column("net_inflow_5d", Float),
    Column("main_force_ratio", Float),
    Column("northbound_net", Float),
    Column("consecutive_outflow_days", Integer),
    Column("fetched_at", String(64), nullable=False),
    Column("raw_json", JSON),
    Index("idx_market_fund_flows_symbol_date", "symbol", "trade_date"),
)

market_data_coverage = Table(
    "market_data_coverage",
    metadata,
    Column("coverage_key", String(128), primary_key=True),
    Column("domain", String(64), nullable=False),
    Column("symbol", String(64), nullable=False),
    Column("start_date", String(32)),
    Column("end_date", String(32)),
    Column("period", String(32)),
    Column("adjustflag", String(8)),
    Column("source", String(128), nullable=False),
    Column("row_count", Integer, nullable=False, default=0),
    Column("status", String(32), nullable=False),
    Column("fetched_at", String(64), nullable=False),
    Column("error_json", JSON),
    Index("idx_market_data_coverage_lookup", "domain", "symbol", "source"),
)

projection_positions = Table(
    "projection_positions",
    metadata,
    Column("code", String(64), primary_key=True),
    Column("name", String(255), nullable=False),
    Column("style", String(64), nullable=False),
    Column("shares", Integer, nullable=False),
    Column("avg_cost_cents", Integer, nullable=False),
    Column("cost_basis_cents", Integer),
    Column("entry_date", String(32), nullable=False),
    Column("entry_day_low_cents", Integer),
    Column("stop_loss_cents", Integer),
    Column("take_profit_cents", Integer),
    Column("highest_since_entry_cents", Integer),
    Column("current_price_cents", Integer),
    Column("unrealized_pnl_cents", Integer),
    Column("currency", String(16), nullable=False, default="CNY"),
    Column("updated_at", String(64), nullable=False),
)

projection_orders = Table(
    "projection_orders",
    metadata,
    Column("order_id", String(64), primary_key=True),
    Column("code", String(64), nullable=False),
    Column("side", String(16), nullable=False),
    Column("shares", Integer, nullable=False),
    Column("price_cents", Integer, nullable=False),
    Column("status", String(32), nullable=False),
    Column("broker", String(64)),
    Column("created_at", String(64), nullable=False),
    Column("filled_at", String(64)),
    Column("updated_at", String(64), nullable=False),
)

projection_balances = Table(
    "projection_balances",
    metadata,
    Column("scope", String(64), primary_key=True),
    Column("cash_cents", BigInteger, nullable=False),
    Column("total_asset_cents", BigInteger),
    Column("weekly_buy_count", Integer, nullable=False, default=0),
    Column("daily_pnl_cents", BigInteger, nullable=False, default=0),
    Column("consecutive_loss_days", Integer, nullable=False, default=0),
    Column("updated_at", String(64), nullable=False),
)

projection_candidate_pool = Table(
    "projection_candidate_pool",
    metadata,
    Column("code", String(64), primary_key=True),
    Column("pool_tier", String(32), primary_key=True),
    Column("name", String(255)),
    Column("score", Float),
    Column("added_at", String(64), nullable=False),
    Column("last_scored_at", String(64)),
    Column("streak_days", Integer, default=0),
    Column("note", Text),
)

projection_market_state = Table(
    "projection_market_state",
    metadata,
    Column("index_symbol", String(64), primary_key=True),
    Column("name", String(255), nullable=False),
    Column("signal", String(32)),
    Column("price_cents", Integer),
    Column("change_pct", Float),
    Column("ma20_pct", Float),
    Column("ma60_pct", Float),
    Column("updated_at", String(64), nullable=False),
)

report_artifacts = Table(
    "report_artifacts",
    metadata,
    Column("artifact_id", String(64), primary_key=True),
    Column("run_id", String(128), nullable=False),
    Column("report_type", String(64), nullable=False),
    Column("format", String(32), nullable=False),
    Column("content", Text, nullable=False),
    Column("delivered_to", String(255)),
    Column("created_at", String(64), nullable=False),
)

backtest_runs = Table(
    "backtest_runs",
    metadata,
    Column("run_id", String(64), primary_key=True),
    Column("preset", String(128), nullable=False),
    Column("codes_json", JSON, nullable=False),
    Column("start_date", String(32), nullable=False),
    Column("end_date", String(32), nullable=False),
    Column("initial_cash", Float, nullable=False),
    Column("final_value", Float),
    Column("metrics_json", JSON, nullable=False),
    Column("request_json", JSON, nullable=False),
    Column("created_at", String(64), nullable=False),
    Index("idx_backtest_runs_created", "created_at"),
    Index("idx_backtest_runs_period", "start_date", "end_date"),
)

backtest_trades = Table(
    "backtest_trades",
    metadata,
    Column("run_id", String(64), primary_key=True),
    Column("trade_index", Integer, primary_key=True),
    Column("trade_date", String(32), nullable=False),
    Column("code", String(64), nullable=False),
    Column("name", String(255)),
    Column("side", String(16), nullable=False),
    Column("price", Float),
    Column("shares", Integer),
    Column("pnl", Float),
    Column("return_pct", Float),
    Column("payload_json", JSON, nullable=False),
    Index("idx_backtest_trades_code_date", "code", "trade_date"),
)

backtest_equity_curve = Table(
    "backtest_equity_curve",
    metadata,
    Column("run_id", String(64), primary_key=True),
    Column("curve_index", Integer, primary_key=True),
    Column("trade_date", String(32), nullable=False),
    Column("equity", Float, nullable=False),
    Column("cash", Float),
    Column("positions", Integer),
    Column("payload_json", JSON, nullable=False),
    Index("idx_backtest_equity_run_date", "run_id", "trade_date"),
)

signal_history_snapshots = Table(
    "signal_history_snapshots",
    metadata,
    Column("snapshot_id", String(64), primary_key=True),
    Column("snapshot_date", String(32), nullable=False),
    Column("history_group_id", String(128), nullable=False),
    Column("run_id", String(128), nullable=False),
    Column("phase", String(32), nullable=False),
    Column("snapshot_type", String(32), nullable=False),
    Column("payload_json", JSON, nullable=False),
    Column("created_at", String(64), nullable=False),
    UniqueConstraint("history_group_id", "snapshot_type", name="uq_signal_history_group_type"),
    Index("idx_signal_history_date", "snapshot_date", "created_at"),
    Index("idx_signal_history_group", "history_group_id"),
)

schema_version = Table(
    "_schema_version",
    metadata,
    Column("version", Integer, primary_key=True),
    Column("applied_at", String(64), nullable=False),
)
