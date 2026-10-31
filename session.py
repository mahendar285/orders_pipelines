"""Spark session construction.

Keeping this in one function means the baseline and the optimized strategy are
measured on the same runtime with an explicitly different set of switches, not
on two differently configured sessions nobody can reproduce.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from .config import Config

BASELINE_CONF: Dict[str, str] = {
    # Deliberately naive: no adaptive query execution, no automatic broadcast,
    # a shuffle partition count borrowed from a cluster this data never sees.
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.shuffle.partitions": "200",
}

OPTIMIZED_CONF: Dict[str, str] = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.autoBroadcastJoinThreshold": "10485760",
}


def get_spark(config: Config, strategy: str = "optimized", app_name: Optional[str] = None) -> SparkSession:
    """Build (or reuse) a session configured for the given strategy."""
    builder = SparkSession.builder.appName(app_name or ("orders-pipeline:" + strategy))

    if config.master:
        builder = builder.master(config.master)

    base: Dict[str, Any] = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.ui.showConsoleProgress": "false",
    }
    base.update(config.spark_conf())

    if strategy == "baseline":
        base.update(BASELINE_CONF)
    else:
        base.update(OPTIMIZED_CONF)

    for key, value in base.items():
        builder = builder.config(key, value)

    session = builder.getOrCreate()
    # getOrCreate returns the existing session when one is alive, and builder
    # config is ignored in that case. Setting the confs explicitly is what makes
    # "run both strategies in one process" actually mean two different runtimes.
    for key, value in base.items():
        try:
            session.conf.set(key, value)
        except Exception:  # noqa: BLE001 - static confs are fixed at start-up
            pass

    session.sparkContext.setLogLevel("WARN")
    return session
