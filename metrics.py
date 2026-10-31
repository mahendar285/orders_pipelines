"""Run bookkeeping.

A run is only useful if it is explainable after the fact, so the context object
carries identity, timings, counts, reconciliation and the failure reason, and
writes all of it to disk even when the process is about to raise.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

LOGGER_NAME = "pipeline"


def utc_now() -> datetime:
    """Current UTC time, truncated to seconds so ids stay readable."""
    return datetime.now(timezone.utc).replace(microsecond=0)


def new_run_id(moment: Optional[datetime] = None) -> str:
    """Deterministic-shape, collision-resistant run identifier."""
    stamp = (moment or utc_now()).strftime("%Y%m%dT%H%M%SZ")
    return stamp + "-" + uuid.uuid4().hex[:6]


def get_logger() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%H:%M:%S"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


class RunContext:
    """Mutable record of one pipeline run."""

    def __init__(self, run_id: str, strategy: str, config: Any, as_of: Optional[datetime] = None):
        self.run_id = run_id
        self.strategy = strategy
        self.config = config
        self.as_of = as_of or utc_now()
        self.started_at = utc_now()
        self.finished_at: Optional[datetime] = None
        self.status = "RUNNING"
        self.counts: Dict[str, Dict[str, int]] = {}
        self.rejects: Dict[str, int] = {}
        self.timings: Dict[str, float] = {}
        self.checks: Dict[str, Any] = {}
        self.reconciliation: Dict[str, Any] = {}
        self.plan_shape: Dict[str, int] = {}
        self.failure_reason: Optional[str] = None
        self.notes: List[str] = []
        self.log = get_logger()

    # --- instrumentation --------------------------------------------------
    @contextmanager
    def stage(self, name: str) -> Iterator[None]:
        """Time one stage. Recorded even when the stage raises."""
        started = time.perf_counter()
        try:
            yield
        finally:
            self.timings[name] = round(time.perf_counter() - started, 3)

    def note(self, message: str) -> None:
        self.notes.append(message)
        self.log.warning(message)

    def fail(self, reason: str) -> None:
        self.status = "FAILED"
        self.failure_reason = reason

    # --- persistence ------------------------------------------------------
    @property
    def run_dir(self) -> Path:
        return Path(self.config.runs_dir) / ("run_id=" + self.run_id)

    def summary(self) -> Dict[str, Any]:
        duration = None
        if self.finished_at is not None:
            duration = round((self.finished_at - self.started_at).total_seconds(), 3)
        return {
            "run_id": self.run_id,
            "status": self.status,
            "strategy": self.strategy,
            "as_of": self.as_of.isoformat(),
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_seconds": duration,
            "timings": self.timings,
            "counts": self.counts,
            "rejects": self.rejects,
            "reconciliation": self.reconciliation,
            "plan_shape": self.plan_shape,
            "checks": self.checks,
            "failure_reason": self.failure_reason,
            "notes": self.notes,
        }

    def persist(self) -> Path:
        if self.finished_at is None:
            self.finished_at = utc_now()
        self.run_dir.mkdir(parents=True, exist_ok=True)
        target = self.run_dir / "summary.json"
        target.write_text(json.dumps(self.summary(), indent=2, default=str), encoding="utf-8")
        return target


def read_summary(summary_path: Path) -> Dict[str, Any]:
    """Load a previously written run summary."""
    return json.loads(Path(summary_path).read_text(encoding="utf-8"))
