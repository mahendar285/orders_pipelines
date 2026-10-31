"""Reconciliation runner.

The SQL lives in sql/reconciliation.sql so a human can paste it into a notebook;
this module only registers the views and executes the statements in file order.
Names come from the leading "-- N. ..." comment of each statement, which keeps
the file readable and the report labelled at the same time.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List

from .schemas import QUARANTINE_SCHEMA

DEFAULT_SQL_PATH = "sql/reconciliation.sql"
_NAME_PATTERN = re.compile(r"^\s*(\d+)\s*\.\s*(.+)$")


def _statement_name(comments: List[str], fallback: str) -> str:
    for comment in comments:
        cleaned = comment.strip()
        if cleaned.startswith("--"):
            cleaned = cleaned[2:].strip()
        match = _NAME_PATTERN.match(cleaned)
        if match:
            return match.group(2).strip().rstrip(".")
    return fallback


def load_statements(path: str = DEFAULT_SQL_PATH) -> List[Dict[str, str]]:
    """Split the SQL file into named statements."""
    text = Path(path).read_text(encoding="utf-8")
    statements: List[Dict[str, str]] = []

    for chunk in text.split(";"):
        lines = [line for line in chunk.splitlines() if line.strip()]
        if not lines:
            continue
        comments = [line for line in lines if line.strip().startswith("--")]
        sql_lines = [line for line in lines if not line.strip().startswith("--")]
        sql = "\n".join(sql_lines).strip()
        if not sql:
            continue
        statements.append({
            "name": _statement_name(comments, "statement " + str(len(statements) + 1)),
            "sql": sql,
        })
    return statements


def run_reconciliation(spark, config, sql_path: str = DEFAULT_SQL_PATH) -> List[Dict[str, Any]]:
    """Register views, run every statement, return named result frames."""
    fact_path = Path(config.fact_path)
    if not fact_path.exists():
        raise FileNotFoundError(
            "no curated table at " + str(fact_path) + " - run the pipeline first"
        )
    spark.read.parquet(str(fact_path)).createOrReplaceTempView("fact_order_line")

    quarantine_path = Path(config.quarantine_dir)
    if quarantine_path.exists():
        spark.read.parquet(str(quarantine_path)).createOrReplaceTempView("quarantine")
    else:
        spark.createDataFrame([], QUARANTINE_SCHEMA).createOrReplaceTempView("quarantine")

    return [
        {"name": statement["name"], "frame": spark.sql(statement["sql"])}
        for statement in load_statements(sql_path)
    ]
