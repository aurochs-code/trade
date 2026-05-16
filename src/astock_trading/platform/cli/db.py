"""Database administration CLI commands."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
from pathlib import Path
from typing import Any

import typer
from sqlalchemy.engine import make_url

from astock_trading.platform.cli.common import (
    json_or_text,
)
from astock_trading.platform.database import DatabaseSettings
from astock_trading.platform.db import connect, get_schema_version, init_db


db_app = typer.Typer(name="db", help="数据库管理")


def _runtime_url():
    url = make_url(DatabaseSettings.from_env().url)
    if not url.drivername.startswith("mysql"):
        raise typer.BadParameter("This command requires ASTOCK_DATABASE_URL=mysql+pymysql://...")
    return url


def _mysql_table_names(conn) -> list[str]:
    rows = conn.execute("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'").fetchall()
    return [row[0] for row in rows]


def _mysql_table_status(conn) -> list[dict[str, Any]]:
    rows = conn.execute("SHOW TABLE STATUS").fetchall()
    return [dict(row) for row in rows]


def _event_payload_hash(rows) -> str:
    def _normalize(value: Any) -> Any:
        if isinstance(value, float):
            return round(value, 12)
        if isinstance(value, list):
            return [_normalize(item) for item in value]
        if isinstance(value, dict):
            return {key: _normalize(item) for key, item in value.items()}
        return value

    def _canonical_json(value: Any) -> str:
        if isinstance(value, str):
            value = json.loads(value)
        value = _normalize(value)
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    hasher = hashlib.sha256()
    for row in rows:
        hasher.update(str(row["event_id"]).encode())
        hasher.update(_canonical_json(row["payload_json"]).encode())
        hasher.update(_canonical_json(row["metadata_json"]).encode())
    return hasher.hexdigest()


@db_app.command("init")
def db_init():
    """初始化数据库（创建所有表）"""
    _runtime_url()
    path = init_db()
    typer.echo(f"数据库已初始化: {path}")


@db_app.command("migrate")
def db_migrate():
    """运行数据库 migration（创建缺失的表，更新 schema 版本）"""
    _runtime_url()
    path = init_db()
    conn = connect()
    try:
        version = get_schema_version(conn)
        typer.echo(f"Migration 完成: schema v{version} @ {path}")
    finally:
        conn.close()


@db_app.command("status")
def db_status(
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查看数据库状态"""
    conn = connect()
    try:
        result = {
            "schema_version": get_schema_version(conn),
            "events": conn.execute("SELECT COUNT(*) FROM event_log").fetchone()[0],
            "runs": conn.execute("SELECT COUNT(*) FROM run_log").fetchone()[0],
            "config_versions": conn.execute("SELECT COUNT(*) FROM config_versions").fetchone()[0],
        }
        if as_json:
            json_or_text(result, True)
        else:
            typer.echo(f"Schema version: {result['schema_version']}")
            typer.echo(f"Events: {result['events']}")
            typer.echo(f"Runs: {result['runs']}")
            typer.echo(f"Config versions: {result['config_versions']}")
    finally:
        conn.close()


@db_app.command("backup")
def db_backup(
    output: Path = typer.Option(..., "--output", "-o", help="输出 .sql 文件路径"),
    yes: bool = typer.Option(False, "--yes", "-y", help="确认执行 mysqldump"),
    docker_container: str = typer.Option(
        "",
        "--docker-container",
        help="宿主机无 mysqldump 时，在指定 MySQL 容器内执行 mysqldump",
    ),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """使用 mysqldump 备份 MySQL runtime 数据库。"""
    url = _runtime_url()
    if not yes:
        raise typer.BadParameter("db backup requires --yes")
    mysqldump = shutil.which("mysqldump")
    docker_container = docker_container or os.environ.get("ASTOCK_MYSQL_CONTAINER", "")
    if not mysqldump and not docker_container:
        raise typer.BadParameter("mysqldump not found in PATH; pass --docker-container or set ASTOCK_MYSQL_CONTAINER")

    output.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    if url.password:
        env["MYSQL_PWD"] = url.password
    if mysqldump:
        command = [
            mysqldump,
            "--single-transaction",
            "--routines",
            "--triggers",
            "--hex-blob",
            "-h",
            url.host or "localhost",
            "-P",
            str(url.port or 3306),
            "-u",
            url.username or "",
            url.database or "",
        ]
        backend = "local"
    else:
        command = [
            "docker",
            "exec",
            "-e",
            f"MYSQL_PWD={url.password or ''}",
            docker_container,
            "mysqldump",
            "--single-transaction",
            "--routines",
            "--triggers",
            "--hex-blob",
            "-u",
            url.username or "",
            url.database or "",
        ]
        backend = f"docker:{docker_container}"

    with output.open("wb") as f:
        completed = subprocess.run(command, stdout=f, stderr=subprocess.PIPE, env=env)
    result = {
        "status": "ok" if completed.returncode == 0 else "failed",
        "output": str(output),
        "backend": backend,
        "returncode": completed.returncode,
        "stderr": completed.stderr.decode(errors="replace")[-4000:],
    }
    json_or_text(result, as_json)
    if completed.returncode != 0:
        raise typer.Exit(completed.returncode)


@db_app.command("tables")
def db_tables(
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查看 MySQL 表大小和行数估算。"""
    _runtime_url()
    conn = connect()
    try:
        tables = _mysql_table_status(conn)
        result = [
            {
                "name": row.get("Name"),
                "engine": row.get("Engine"),
                "rows": row.get("Rows"),
                "data_length": row.get("Data_length"),
                "index_length": row.get("Index_length"),
                "collation": row.get("Collation"),
            }
            for row in tables
        ]
        if as_json:
            json_or_text(result, True)
        else:
            for row in result:
                typer.echo(
                    f"{row['name']} rows={row['rows']} "
                    f"data={row['data_length']} index={row['index_length']} engine={row['engine']}"
                )
    finally:
        conn.close()


@db_app.command("check")
def db_check(
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """执行 MySQL CHECK TABLE。"""
    _runtime_url()
    conn = connect()
    try:
        results = []
        for table in _mysql_table_names(conn):
            rows = conn.execute(f"CHECK TABLE `{table}`").fetchall()
            results.extend(dict(row) for row in rows)
        ok = all(str(row.get("Msg_text", "")).lower() == "ok" for row in results)
        result = {"status": "ok" if ok else "failed", "checks": results}
        if as_json:
            json_or_text(result, True)
        else:
            typer.echo(f"MySQL check: {result['status']}")
            if not ok:
                json_or_text(result, True)
        if not ok:
            raise typer.Exit(1)
    finally:
        conn.close()


@db_app.command("optimize")
def db_optimize(
    yes: bool = typer.Option(False, "--yes", "-y", help="确认执行 OPTIMIZE TABLE"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """执行 MySQL OPTIMIZE TABLE。"""
    _runtime_url()
    if not yes:
        raise typer.BadParameter("db optimize requires --yes")
    conn = connect()
    try:
        results = []
        for table in _mysql_table_names(conn):
            rows = conn.execute(f"OPTIMIZE TABLE `{table}`").fetchall()
            results.extend(dict(row) for row in rows)
        result = {"status": "ok", "results": results}
        if as_json:
            json_or_text(result, True)
        else:
            typer.echo(f"Optimized {len(results)} table results")
    finally:
        conn.close()


@db_app.command("migrate-sqlite-to-mysql")
def db_migrate_sqlite_to_mysql(
    sqlite_path: Path = typer.Option(..., "--sqlite-path", help="外部归档的源 SQLite DB"),
    dry_run: bool = typer.Option(False, "--dry-run", help="只检查源库并输出计划，不写入目标库"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """将历史 SQLite 数据迁移到当前 runtime DB（生产为 MySQL）。"""
    tables = [
        "config_versions",
        "event_log",
        "run_log",
        "market_observations",
        "market_bars",
        "projection_positions",
        "projection_orders",
        "projection_balances",
        "projection_candidate_pool",
        "projection_market_state",
        "report_artifacts",
    ]
    source = sqlite3.connect(str(sqlite_path))
    source.row_factory = sqlite3.Row
    try:
        source_counts = {}
        source_hash = ""
        for table in tables:
            try:
                source_counts[table] = source.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            except sqlite3.OperationalError:
                source_counts[table] = 0
        try:
            payloads = source.execute(
                "SELECT event_id, payload_json, metadata_json FROM event_log ORDER BY occurred_at, stream_version"
            ).fetchall()
            source_hash = _event_payload_hash(payloads)
        except sqlite3.OperationalError:
            source_hash = ""

        if dry_run:
            json_or_text(
                {
                    "dry_run": True,
                    "sqlite_path": str(sqlite_path),
                    "source_counts": source_counts,
                    "event_payload_hash": source_hash,
                    "target": "not_written",
                },
                as_json,
            )
            return

        _runtime_url()
        init_db()
        target = connect()
        try:
            for table in tables:
                try:
                    rows = source.execute(f"SELECT * FROM {table}").fetchall()
                except sqlite3.OperationalError:
                    rows = []
                if not rows:
                    continue
                columns = rows[0].keys()
                placeholders = ", ".join("?" for _ in columns)
                column_sql = ", ".join(f"`{column}`" for column in columns)
                sql = f"INSERT OR REPLACE INTO `{table}` ({column_sql}) VALUES ({placeholders})"
                target.executemany(sql, [tuple(row[col] for col in columns) for row in rows])

            target_counts = {}
            for table in tables:
                try:
                    target_counts[table] = target.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                except Exception:
                    target_counts[table] = 0
            try:
                target_payloads = target.execute(
                    "SELECT event_id, payload_json, metadata_json FROM event_log ORDER BY occurred_at, stream_version"
                ).fetchall()
                target_hash = _event_payload_hash(target_payloads)
            except Exception:
                target_hash = ""
            target.commit()
        finally:
            target.close()

        result = {
            "dry_run": False,
            "sqlite_path": str(sqlite_path),
            "source_counts": source_counts,
            "target_counts": target_counts,
            "counts_match": source_counts == target_counts,
            "source_event_payload_hash": source_hash,
            "target_event_payload_hash": target_hash,
            "event_payload_hash_match": source_hash == target_hash,
        }
        json_or_text(result, as_json)
        if not result["counts_match"] or not result["event_payload_hash_match"]:
            raise typer.Exit(1)
    finally:
        source.close()
