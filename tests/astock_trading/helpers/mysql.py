"""MySQL-only 测试数据库 fixture。"""

from __future__ import annotations

from dataclasses import dataclass
import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import SQLAlchemyError

from astock_trading.platform import db as runtime_db


@dataclass(frozen=True)
class MySQLRuntime:
    """单个测试独占的 MySQL runtime。"""

    url: str
    database: str

    def write_env_file(self, path, **extra: str) -> None:
        lines = [f"ASTOCK_DATABASE_URL={self.url}"]
        lines.extend(f"{key}={value}" for key, value in extra.items())
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _validated_mysql_url() -> URL:
    configured = os.getenv("ASTOCK_TEST_DATABASE_URL", "").strip()
    if not configured:
        pytest.skip("ASTOCK_TEST_DATABASE_URL is required for MySQL integration tests")
    url = make_url(configured)
    if url.drivername != "mysql+pymysql":
        raise AssertionError("ASTOCK_TEST_DATABASE_URL must use mysql+pymysql://")
    if not url.database:
        raise AssertionError("ASTOCK_TEST_DATABASE_URL must include a database name")
    return url


def _database_name(base_name: str) -> str:
    normalized = "".join(ch if ch.isalnum() else "_" for ch in base_name).strip("_")
    normalized = normalized or "astock"
    return f"{normalized}_test_{uuid.uuid4().hex[:12]}"


def _quoted_identifier(name: str) -> str:
    if not name.replace("_", "").isalnum():
        raise ValueError(f"unsafe database name: {name}")
    return f"`{name}`"


def _reset_runtime_cache() -> None:
    cached = getattr(runtime_db, "_RUNTIME_DB", None)
    if cached is not None:
        cached.engine.dispose()
    runtime_db._RUNTIME_DB = None
    runtime_db._RUNTIME_DB_URL = None


@pytest.fixture
def mysql_runtime(monkeypatch):
    base_url = _validated_mysql_url()
    database = _database_name(base_url.database or "astock")
    admin_url = base_url.set(database=None)
    runtime_url = base_url.set(database=database).render_as_string(hide_password=False)
    admin_engine = create_engine(admin_url, future=True, pool_pre_ping=True)

    try:
        with admin_engine.begin() as conn:
            conn.execute(
                text(
                    f"CREATE DATABASE {_quoted_identifier(database)} "
                    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
            )
    except SQLAlchemyError as exc:
        admin_engine.dispose()
        raise AssertionError(f"无法创建 MySQL 测试库: {exc}") from exc

    monkeypatch.setenv("ASTOCK_DATABASE_URL", runtime_url)
    _reset_runtime_cache()
    runtime_db.init_db()

    try:
        yield MySQLRuntime(url=runtime_url, database=database)
    finally:
        _reset_runtime_cache()
        with admin_engine.begin() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {_quoted_identifier(database)}"))
        admin_engine.dispose()


@pytest.fixture
def mysql_conn(mysql_runtime):
    del mysql_runtime
    conn = runtime_db.connect()
    try:
        yield conn
    finally:
        conn.close()
