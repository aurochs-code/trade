"""MySQL 测试辅助层的契约测试。"""

from __future__ import annotations

import pytest

from tests.astock_trading.helpers.mysql import _database_name, _validated_mysql_url


def test_validated_mysql_url_requires_configuration(monkeypatch):
    monkeypatch.delenv("ASTOCK_TEST_DATABASE_URL", raising=False)

    with pytest.raises(pytest.skip.Exception):
        _validated_mysql_url()


def test_validated_mysql_url_rejects_non_mysql_driver(monkeypatch):
    monkeypatch.setenv("ASTOCK_TEST_DATABASE_URL", "postgresql://user:pass@localhost/astock")

    with pytest.raises(AssertionError, match="mysql\\+pymysql"):
        _validated_mysql_url()


def test_validated_mysql_url_requires_database_name(monkeypatch):
    monkeypatch.setenv("ASTOCK_TEST_DATABASE_URL", "mysql+pymysql://user:pass@localhost")

    with pytest.raises(AssertionError, match="database"):
        _validated_mysql_url()


def test_database_name_sanitizes_base_name():
    database = _database_name("a-stock trading")

    assert database.startswith("a_stock_trading_test_")
    assert database.replace("_", "").isalnum()
