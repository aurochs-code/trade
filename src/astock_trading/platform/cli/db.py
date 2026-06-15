"""Database administration CLI commands."""

from __future__ import annotations

import os
import shutil
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
