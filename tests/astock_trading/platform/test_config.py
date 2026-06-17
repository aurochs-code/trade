"""Tests for platform/config.py — ConfigRegistry"""

from pathlib import Path
from types import SimpleNamespace

import pytest

from astock_trading.platform.config import ConfigRegistry, ConfigRepository


@pytest.fixture
def conn(mysql_conn):
    yield mysql_conn


def test_freeze_creates_version(conn):
    """freeze() should create a config_versions row and return a snapshot."""
    registry = ConfigRegistry()  # uses real config/ dir
    snapshot = registry.freeze(conn)

    assert snapshot.version.startswith("v")
    assert len(snapshot.hash) == 16
    assert isinstance(snapshot.data, dict)
    assert "strategy" in snapshot.data  # strategy.yaml should be loaded


def test_freeze_idempotent(conn):
    """Freezing the same config twice should return the same version."""
    registry = ConfigRegistry()
    s1 = registry.freeze(conn)
    s2 = registry.freeze(conn)

    assert s1.version == s2.version
    assert s1.hash == s2.hash


def test_freeze_recovers_when_same_hash_is_inserted_concurrently(conn, monkeypatch):
    """并发 freeze 已写入同一 hash 时，应复用已有版本而不是失败。"""
    original_insert = ConfigRepository.insert_version

    def racing_insert(self, version, config_hash, config_json):
        original_insert(self, "v_race_existing", config_hash, config_json)
        raise RuntimeError("Duplicate entry for key config_hash")

    monkeypatch.setattr(ConfigRepository, "insert_version", racing_insert)

    snapshot = ConfigRegistry().freeze(conn)

    assert snapshot.version == "v_race_existing"


def test_freeze_retries_when_generated_version_collides(conn, monkeypatch):
    """并发 freeze 生成同秒版本号冲突时，应换一个版本号重试。"""
    original_insert = ConfigRepository.insert_version
    calls = []

    def racing_insert(self, version, config_hash, config_json):
        calls.append(version)
        if len(calls) == 1:
            raise RuntimeError("Duplicate entry for key config_version")
        original_insert(self, version, config_hash, config_json)

    monkeypatch.setattr(ConfigRepository, "insert_version", racing_insert)

    snapshot = ConfigRegistry().freeze(conn)

    assert len(calls) == 2
    assert snapshot.version == calls[1]


def test_freeze_rolls_back_failed_insert_before_rechecking_existing_hash(monkeypatch):
    """MySQL duplicate-key failures can require rollback before the existing hash is visible."""
    calls = {"find": 0, "rollback": 0}

    def find_after_rollback(self, config_hash):  # noqa: ARG001
        calls["find"] += 1
        return "v_after_rollback" if calls["find"] >= 3 else None

    def duplicate_insert(self, version, config_hash, config_json):  # noqa: ARG001
        raise RuntimeError("Duplicate entry for key config_hash")

    def rollback():
        calls["rollback"] += 1

    monkeypatch.setattr(ConfigRepository, "find_version_by_hash", find_after_rollback)
    monkeypatch.setattr(ConfigRepository, "insert_version", duplicate_insert)

    snapshot = ConfigRegistry().freeze(SimpleNamespace(rollback=rollback))

    assert snapshot.version == "v_after_rollback"
    assert calls["rollback"] == 1


def test_get_version_roundtrip(conn):
    """freeze → get_version should return the same data."""
    registry = ConfigRegistry()
    snapshot = registry.freeze(conn)

    loaded = registry.get_version(conn, snapshot.version)
    assert loaded is not None
    assert loaded == snapshot.data


def test_get_nonexistent_version(conn):
    registry = ConfigRegistry()
    assert registry.get_version(conn, "v_nonexistent") is None


def test_snapshot_get_nested(conn):
    registry = ConfigRegistry()
    snapshot = registry.freeze(conn)

    # strategy.yaml has scoring.weights
    weights = snapshot.get("strategy", "scoring", "weights")
    assert weights is not None or snapshot.get("strategy", "scoring") is not None

    # nonexistent key returns default
    assert snapshot.get("nonexistent", "key", default=42) == 42


def test_default_strategy_config_uses_recovery_ma120_runtime_policy(monkeypatch):
    """默认运行配置应和最终可承接的恢复相位策略一致。"""
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)

    data, errors = ConfigRegistry().load_and_validate()

    assert errors == []
    strategy = data["strategy"]
    overlays = strategy["scoring"]["market_regime_overlays"]
    route_policy = strategy["scoring"]["route_execution_policy"]
    auto_trade = strategy["auto_trade"]

    assert overlays["YELLOW"]["buy_threshold"] == 9.9
    yellow_overheat = route_policy["YELLOW:relative_strength_overheat"]
    assert yellow_overheat["require_above_ma120"] is True
    assert yellow_overheat["min_index_ma120_slope_20d_pct"] == 0.0
    assert yellow_overheat["min_data_quality_for_buy"] == "degraded"
    assert yellow_overheat["max_missing_fields_for_buy"] == 6
    assert route_policy["YELLOW:pullback_to_ma20"]["actions"] == []
    assert strategy["risk"]["momentum"]["trailing_stop"] == 0.16
    assert auto_trade["scale_in"]["markets"] == ["GREEN"]
    assert auto_trade["scale_in"]["step_position_pct"] == 0.04
    assert auto_trade["scale_in"]["aggressive_max_position_pct"] == 0.30


def test_template_strategy_config_uses_same_runtime_policy(monkeypatch):
    """安装模板的默认策略不能落后于源码配置。"""
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    repo_root = Path(__file__).resolve().parents[3]
    template_dir = repo_root / "src" / "astock_trading" / "templates" / "config"

    data, errors = ConfigRegistry(config_dir=template_dir).load_and_validate()

    assert errors == []
    strategy = data["strategy"]
    route_policy = strategy["scoring"]["route_execution_policy"]
    assert strategy["scoring"]["market_regime_overlays"]["YELLOW"]["buy_threshold"] == 9.9
    assert route_policy["YELLOW:relative_strength_overheat"]["require_above_ma120"] is True
    assert route_policy["YELLOW:pullback_to_ma20"]["actions"] == []
    assert strategy["risk"]["momentum"]["trailing_stop"] == 0.16
    assert strategy["auto_trade"]["scale_in"]["markets"] == ["GREEN"]


def test_list_versions(conn):
    registry = ConfigRegistry()
    registry.freeze(conn)

    versions = registry.list_versions(conn)
    assert len(versions) >= 1
    assert "config_version" in versions[0]
    assert "config_hash" in versions[0]


def test_env_config_profile_overlay(tmp_path, monkeypatch):
    (tmp_path / "profiles").mkdir()
    (tmp_path / "strategy.yaml").write_text(
        """
scoring:
  weights:
    technical: 3
    fundamental: 2
    flow: 2
    sentiment: 3
  thresholds:
    buy: 5.5
    watch: 5.0
    reject: 4.0
risk:
  position:
    single_max: 0.2
    total_max: 0.6
""",
        encoding="utf-8",
    )
    (tmp_path / "profiles" / "defensive_watch.yaml").write_text(
        """
strategy:
  scoring:
    thresholds:
      buy: 6.5
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("ASTOCK_CONFIG_PROFILE", "defensive_watch")

    data, errors = ConfigRegistry(config_dir=tmp_path).load_and_validate()

    assert errors == []
    assert data["strategy"]["scoring"]["thresholds"]["buy"] == 6.5
