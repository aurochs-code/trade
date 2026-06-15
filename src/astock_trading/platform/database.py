"""Runtime MySQL settings and SQLAlchemy-backed direct-SQL connection."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from sqlalchemy.engine import make_url
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from astock_trading.platform.schema import metadata


class MissingDatabaseUrl(RuntimeError):
    """Raised when runtime code tries to use the DB without ASTOCK_DATABASE_URL."""


@dataclass(frozen=True)
class DatabaseSettings:
    url: str

    @classmethod
    def from_env(cls) -> "DatabaseSettings":
        url = os.getenv("ASTOCK_DATABASE_URL", "").strip()
        if not url:
            raise MissingDatabaseUrl(
                "ASTOCK_DATABASE_URL is required for runtime DB access. "
                "Set it to mysql+pymysql://user:password@host:3306/database."
            )
        parsed = make_url(url)
        if parsed.drivername != "mysql+pymysql":
            raise MissingDatabaseUrl(
                "ASTOCK_DATABASE_URL must use mysql+pymysql://user:password@host:3306/database."
            )
        return cls(url=url)


class CompatRow:
    """Small row wrapper for DBAPI tuples."""

    def __init__(self, keys: list[str], values: tuple[Any, ...]):
        self._keys = keys
        self._values = values
        self._mapping = dict(zip(keys, values))

    def __getitem__(self, key: int | str) -> Any:
        if isinstance(key, int):
            return self._values[key]
        return self._mapping[key]

    def __iter__(self):
        return iter(self._keys)

    def keys(self) -> list[str]:
        return list(self._keys)

    def get(self, key: str, default: Any = None) -> Any:
        return self._mapping.get(key, default)


class CompatResult:
    def __init__(self, keys: list[str], rows: list[tuple[Any, ...]]):
        self._rows = [CompatRow(keys, row) for row in rows]
        self._index = 0

    def fetchone(self) -> Optional[CompatRow]:
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row

    def fetchall(self) -> list[CompatRow]:
        remaining = self._rows[self._index :]
        self._index = len(self._rows)
        return remaining


class Database:
    """SQLAlchemy engine holder."""

    def __init__(self, settings: DatabaseSettings):
        self.settings = settings
        self.engine = create_engine(settings.url, future=True, pool_pre_ping=True)
        self._schema_ready = False

    @classmethod
    def from_env(cls) -> "Database":
        return cls(DatabaseSettings.from_env())

    def create_schema(self) -> None:
        try:
            metadata.create_all(self.engine)
        except OperationalError as exc:
            if "already exists" not in str(exc).lower():
                raise
        self._schema_ready = True

    def connect(self) -> "SQLAlchemyCompatConnection":
        if not self._schema_ready:
            self.create_schema()
        return SQLAlchemyCompatConnection(self.engine)


class SQLAlchemyCompatConnection:
    """DBAPI-like connection backed by a SQLAlchemy engine.

    This lets the existing code keep using conn.execute(sql, params) while the
    runtime connection is managed by SQLAlchemy. It is intentionally temporary:
    repositories should gradually replace direct SQL call sites.
    """

    def __init__(self, engine: Engine):
        self._engine = engine
        self._raw = engine.raw_connection()
        self._dialect = engine.dialect.name
        if not self._dialect.startswith("mysql"):
            raise MissingDatabaseUrl(
                "Runtime database must be MySQL. Set ASTOCK_DATABASE_URL to mysql+pymysql://..."
            )
        self._in_tx = False

    @property
    def dialect(self) -> str:
        return self._dialect

    def execute(self, sql: str, params: Any = None) -> CompatResult:
        statement = sql.strip()
        upper = statement.upper()
        if upper == "BEGIN":
            self._begin()
            return CompatResult([], [])
        if upper == "COMMIT":
            self.commit()
            return CompatResult([], [])
        if upper == "ROLLBACK":
            self.rollback()
            return CompatResult([], [])

        translated = self._translate_sql(statement)
        bound = self._translate_params(params)
        cursor = self._raw.cursor()
        try:
            if bound is None:
                cursor.execute(translated)
            else:
                cursor.execute(translated, bound)
            rows, keys = self._read_rows(cursor)
            if not self._in_tx and cursor.description is None:
                self._raw.commit()
            return CompatResult(keys, rows)
        finally:
            cursor.close()

    def executemany(self, sql: str, seq_of_params: Iterable[Any]) -> CompatResult:
        cursor = self._raw.cursor()
        try:
            cursor.executemany(
                self._translate_sql(sql.strip()),
                [self._translate_params(params) for params in seq_of_params],
            )
            if not self._in_tx:
                self._raw.commit()
            return CompatResult([], [])
        finally:
            cursor.close()

    def executescript(self, script: str) -> None:
        for statement in script.split(";"):
            if statement.strip():
                self.execute(statement)

    def commit(self) -> None:
        self._raw.commit()
        self._in_tx = False

    def rollback(self) -> None:
        self._raw.rollback()
        self._in_tx = False

    def close(self) -> None:
        self._raw.close()

    def _begin(self) -> None:
        cursor = self._raw.cursor()
        try:
            cursor.execute("START TRANSACTION")
            self._in_tx = True
        finally:
            cursor.close()

    def _read_rows(self, cursor) -> tuple[list[tuple[Any, ...]], list[str]]:
        if cursor.description is None:
            return [], []
        keys = [desc[0] for desc in cursor.description]
        return list(cursor.fetchall()), keys

    def _translate_sql(self, sql: str) -> str:
        sql = re.sub(
            r"json_extract\((\w+), '\$\.(\w+)'\)",
            r"JSON_UNQUOTE(JSON_EXTRACT(\1, '$.\2'))",
            sql,
            flags=re.IGNORECASE,
        )
        sql = re.sub(r"\bAUTOINCREMENT\b", "AUTO_INCREMENT", sql, flags=re.IGNORECASE)
        return sql.replace("?", "%s")

    def _translate_params(self, params: Any) -> Any:
        if params is None:
            return None
        if isinstance(params, list):
            return tuple(params)
        return params
