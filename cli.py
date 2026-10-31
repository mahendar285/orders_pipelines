"""Command line entry point: python -m pipeline <command>."""
from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional

from . import reconcile as reconcile_module
from .config import DEFAULT_CONFIG_PATH, Config
from .generator import generate_dataset
from .metrics import new_run_id
from .runner import run_pipeline
from .session import get_spark


def _config(args) -> Config:
    return Config.load(
        path=args.config,
        data_root=args.data_root,
        shuffle_partitions=args.shuffle_partitions,
    )


def _cmd_generate(args) -> int:
    config = _config(args)
    summary = generate_dataset(
        config,
        rows=args.rows,
        seed=args.seed,
        inject_defects=not args.clean,
    )
    print(json.dumps(summary, indent=2))
    return 0


def _cmd_run(args) -> int:
    config = _config(args)
    summary = run_pipeline(
        config,
        strategy=args.strategy,
        run_id=args.run_id,
        force=args.force,
        full_refresh=args.full_refresh,
    )
    print(json.dumps(summary, indent=2, default=str))
    return 0 if summary.get("status") == "SUCCEEDED" else 1


def _cmd_benchmark(args) -> int:
    from .benchmark import run_benchmark

    config = _config(args)
    result = run_benchmark(
        config,
        rows=args.rows,
        repeats=args.repeats,
        seed=args.seed,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0 if result["checksums_match"] else 1


def _cmd_reconcile(args) -> int:
    config = _config(args)
    spark = get_spark(config, strategy="optimized", app_name="orders-pipeline:reconcile")
    report = reconcile_module.run_reconciliation(spark, config)
    for section in report:
        print("")
        print("# " + section["name"])
        section["frame"].show(50, truncate=False)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline", description="Order-line revenue pipeline")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--data-root", default=None)
    parser.add_argument("--shuffle-partitions", type=int, default=None)
    sub = parser.add_subparsers(dest="command", required=True)

    generate = sub.add_parser("generate", help="write the synthetic source extracts")
    generate.add_argument("--rows", type=int, default=2000)
    generate.add_argument("--seed", type=int, default=42)
    generate.add_argument(
        "--clean",
        action="store_true",
        help="generate only valid rows (no injected defects)",
    )
    generate.set_defaults(func=_cmd_generate)

    run = sub.add_parser("run", help="run the pipeline end to end")
    run.add_argument("--strategy", choices=["baseline", "optimized"], default="optimized")
    run.add_argument("--run-id", default=None)
    run.add_argument("--force", action="store_true", help="re-run even if this run id succeeded")
    run.add_argument("--full-refresh", action="store_true", help="delete the curated table first")
    run.set_defaults(func=_cmd_run)

    benchmark = sub.add_parser("benchmark", help="baseline vs optimized on generated data")
    benchmark.add_argument("--rows", type=int, default=None)
    benchmark.add_argument("--repeats", type=int, default=None)
    benchmark.add_argument("--seed", type=int, default=42)
    benchmark.set_defaults(func=_cmd_benchmark)

    reconcile = sub.add_parser("reconcile", help="print the reconciliation report")
    reconcile.set_defaults(func=_cmd_reconcile)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except Exception as error:  # noqa: BLE001 - CLI boundary: print, do not traceback
        print("FAILED: " + type(error).__name__ + ": " + str(error), file=sys.stderr)
        return 1
