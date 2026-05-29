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
