"""
Structured pipeline logger backed by SQLite.

Writes run-level metadata to `pipeline_runs` and per-event rows to
`pipeline_events`. Used as a context manager so exceptions are captured
as failed runs automatically.

Example:
    with PipelineLogger("rss_pipeline") as log:
        log.event("fetched feeds", stage="fetch")
        log.increment_processed(42)

Environment:
    PIPELINE_LOG_DB — path to the SQLite file.
                      Defaults to /var/log/pfa_bvc/pipeline_logs.db (container).
"""

from __future__ import annotations

import os
import sqlite3
import threading
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

DEFAULT_DB_PATH = "/var/log/pfa_bvc/pipeline_logs.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          TEXT PRIMARY KEY,
    pipeline_name   TEXT NOT NULL,
    dag_run_id      TEXT,
    task_id         TEXT,
    started_at      TEXT NOT NULL,
    ended_at        TEXT,
    status          TEXT NOT NULL,
    error_message   TEXT,
    rows_processed  INTEGER NOT NULL DEFAULT 0,
    rows_failed     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pipeline_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id        TEXT NOT NULL,
    ts            TEXT NOT NULL,
    level         TEXT NOT NULL,
    stage         TEXT,
    message       TEXT NOT NULL,
    item_key      TEXT,
    metric_name   TEXT,
    metric_value  REAL,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_events_run    ON pipeline_events(run_id);
CREATE INDEX IF NOT EXISTS idx_events_ts     ON pipeline_events(ts);
CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON pipeline_runs(pipeline_name, started_at DESC);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _resolve_db_path(db_path: Optional[str]) -> str:
    return db_path or os.getenv("PIPELINE_LOG_DB") or DEFAULT_DB_PATH


class PipelineLogger:
    def __init__(self, pipeline_name: str, db_path: Optional[str] = None):
        self.pipeline_name = pipeline_name
        self.db_path = _resolve_db_path(db_path)
        self.run_id = str(uuid.uuid4())
        self.dag_run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID")
        self.task_id = os.getenv("AIRFLOW_CTX_TASK_ID")
        self._lock = threading.Lock()
        self._conn: Optional[sqlite3.Connection] = None

    # --- lifecycle ---------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript(_SCHEMA)
        return conn

    def __enter__(self) -> "PipelineLogger":
        self._conn = self._connect()
        with self._lock:
            self._conn.execute(
                "INSERT INTO pipeline_runs(run_id, pipeline_name, dag_run_id, task_id, started_at, status) "
                "VALUES (?, ?, ?, ?, ?, 'running')",
                (self.run_id, self.pipeline_name, self.dag_run_id, self.task_id, _now_iso()),
            )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._conn is None:
            return
        status = "success" if exc is None else "failed"
        err_msg: Optional[str] = None
        if exc is not None:
            err_msg = f"{exc_type.__name__}: {exc}\n" + "".join(
                traceback.format_exception(exc_type, exc, tb)
            )
            try:
                self.event(f"pipeline failed: {exc}", level="error")
            except Exception:
                pass
        try:
            with self._lock:
                self._conn.execute(
                    "UPDATE pipeline_runs SET ended_at=?, status=?, error_message=? WHERE run_id=?",
                    (_now_iso(), status, err_msg, self.run_id),
                )
        finally:
            self._conn.close()
            self._conn = None

    # --- writes ------------------------------------------------------------

    def event(
        self,
        message: str,
        *,
        level: str = "info",
        stage: Optional[str] = None,
        item_key: Optional[str] = None,
    ) -> None:
        self._insert_event(level=level, stage=stage, message=message, item_key=item_key)

    def metric(
        self,
        name: str,
        value: float,
        *,
        stage: Optional[str] = None,
        message: Optional[str] = None,
    ) -> None:
        self._insert_event(
            level="info",
            stage=stage,
            message=message or f"{name}={value}",
            metric_name=name,
            metric_value=float(value),
        )

    def increment_processed(self, n: int = 1) -> None:
        self._increment("rows_processed", n)

    def increment_failed(self, n: int = 1) -> None:
        self._increment("rows_failed", n)

    # --- internals ---------------------------------------------------------

    def _insert_event(
        self,
        *,
        level: str,
        stage: Optional[str],
        message: str,
        item_key: Optional[str] = None,
        metric_name: Optional[str] = None,
        metric_value: Optional[float] = None,
    ) -> None:
        if self._conn is None:
            return
        with self._lock:
            self._conn.execute(
                "INSERT INTO pipeline_events(run_id, ts, level, stage, message, item_key, metric_name, metric_value) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (self.run_id, _now_iso(), level, stage, message, item_key, metric_name, metric_value),
            )

    def _increment(self, column: str, n: int) -> None:
        if self._conn is None or n == 0:
            return
        with self._lock:
            self._conn.execute(
                f"UPDATE pipeline_runs SET {column} = {column} + ? WHERE run_id = ?",
                (int(n), self.run_id),
            )
