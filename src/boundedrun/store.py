"""SQLite persistence: runs, steps, audit trail, misfit signals.

One file, WAL, stdlib ``sqlite3``. Every write is small and synchronous; the
async surface is a thin ``asyncio.to_thread`` wrapper so a crash mid-run leaves
a durable record of every step that finished.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import threading
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  run_id          TEXT PRIMARY KEY,
  pipeline        TEXT NOT NULL,
  idempotency_key TEXT,
  status          TEXT NOT NULL,
  started_at      TEXT NOT NULL,
  ended_at        TEXT,
  cost_usd        REAL DEFAULT 0,
  tokens_in       INTEGER DEFAULT 0,
  tokens_out      INTEGER DEFAULT 0,
  input_hash      TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ix_runs_idem ON runs(pipeline, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS run_steps (
  step_run_id  TEXT PRIMARY KEY,
  run_id       TEXT NOT NULL REFERENCES runs(run_id),
  seq          INTEGER NOT NULL,
  step_name    TEXT NOT NULL,
  kind         TEXT NOT NULL,
  attempt      INTEGER NOT NULL DEFAULT 1,
  status       TEXT NOT NULL,
  input_json   TEXT,
  output_json  TEXT,
  prompt_version TEXT,
  model        TEXT,
  tokens_in    INTEGER,
  tokens_out   INTEGER,
  latency_ms   INTEGER,
  error        TEXT,
  started_at   TEXT NOT NULL,
  ended_at     TEXT
);

CREATE TABLE IF NOT EXISTS misfit_signals (
  signal_id   TEXT PRIMARY KEY,
  run_id      TEXT NOT NULL REFERENCES runs(run_id),
  kind        TEXT NOT NULL,
  detail      TEXT,
  recorded_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_steps_run   ON run_steps(run_id, seq);
CREATE INDEX IF NOT EXISTS ix_misfit_kind ON misfit_signals(kind, recorded_at);
"""

SIGNAL_KINDS = (
    "missing_step",
    "manual_correction",
    "needs_review",
    "branch_wrong",
    "step_skipped",
)
TERMINAL_STATUSES = ("done", "failed", "needs_review", "bounds_exceeded")

# Marker written in place of a value that cannot survive a round trip through
# JSON. A run cannot be resumed across such a step; it is re-executed instead.
OPAQUE = "__boundedrun_opaque__"


def utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


def new_id() -> str:
    return uuid.uuid4().hex


def since_ts(delta: timedelta | None) -> str | None:
    if delta is None:
        return None
    return (datetime.now(UTC) - delta).isoformat(timespec="microseconds")


def encode(value: Any) -> str:
    """JSON-encode a step value, degrading to an opaque marker when it cannot."""
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError):
        return json.dumps({OPAQUE: type(value).__name__})


def decode(text: str | None) -> Any:
    if text is None:
        return None
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return None


def is_opaque(value: Any) -> bool:
    return isinstance(value, dict) and OPAQUE in value


def input_hash(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        payload = bytes(value)
    else:
        payload = encode(value).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass
class StepRecord:
    """One attempt at one step. Rows are written twice: on start, then on finish."""

    step_run_id: str
    run_id: str
    seq: int
    step_name: str
    kind: str
    attempt: int
    status: str
    started_at: str
    input_json: str | None = None
    output_json: str | None = None
    prompt_version: str | None = None
    model: str | None = None
    tokens_in: int = 0
    tokens_out: int = 0
    latency_ms: int | None = None
    error: str | None = None
    ended_at: str | None = None


@dataclass
class Store:
    """Thread-safe SQLite handle. Sync methods are also used by ``bounds()``."""

    path: str | Path = ":memory:"
    _conn: sqlite3.Connection | None = field(default=None, init=False, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)

    # ---------------------------------------------------------------- connection

    def connect(self) -> sqlite3.Connection:
        with self._lock:
            if self._conn is None:
                path = str(self.path)
                if path not in (":memory:", ""):
                    Path(path).parent.mkdir(parents=True, exist_ok=True)
                conn = sqlite3.connect(path, check_same_thread=False, timeout=30.0)
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA foreign_keys=ON")
                conn.executescript(SCHEMA)
                conn.commit()
                self._conn = conn
            return self._conn

    def is_available(self) -> bool:
        """True when reading is free of side effects — connecting creates the file."""
        return (
            self._conn is not None
            or str(self.path) in (":memory:", "")
            or Path(str(self.path)).exists()
        )

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    async def aclose(self) -> None:
        await asyncio.to_thread(self.close)

    def _write(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self._lock:
            conn = self.connect()
            cur = conn.execute(sql, params)
            conn.commit()
            return cur

    def _query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return list(self.connect().execute(sql, params).fetchall())

    async def _awrite(self, sql: str, params: tuple = ()) -> None:
        await asyncio.to_thread(self._write, sql, params)

    async def _aquery(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        return await asyncio.to_thread(self._query, sql, params)

    # ---------------------------------------------------------------------- runs

    def claim_run(
        self,
        *,
        run_id: str,
        pipeline: str,
        idempotency_key: str | None,
        input_hash_: str | None,
    ) -> bool:
        """Insert a ``running`` row. False means the key is already claimed."""
        try:
            self._write(
                "INSERT INTO runs (run_id, pipeline, idempotency_key, status, started_at,"
                " input_hash) VALUES (?, ?, ?, 'running', ?, ?)",
                (run_id, pipeline, idempotency_key, utcnow(), input_hash_),
            )
        except sqlite3.IntegrityError:
            return False
        return True

    async def aclaim_run(self, **kw: Any) -> bool:
        return await asyncio.to_thread(lambda: self.claim_run(**kw))

    async def finish_run(
        self,
        run_id: str,
        *,
        status: str,
        cost_usd: float,
        tokens_in: int,
        tokens_out: int,
    ) -> None:
        await self._awrite(
            "UPDATE runs SET status=?, ended_at=?, cost_usd=?, tokens_in=?, tokens_out=?"
            " WHERE run_id=?",
            (status, utcnow(), cost_usd, tokens_in, tokens_out, run_id),
        )

    async def reopen_run(self, run_id: str) -> None:
        """Mark an interrupted run running again, so resume() can continue it."""
        await self._awrite(
            "UPDATE runs SET status='running', ended_at=NULL WHERE run_id=?", (run_id,)
        )

    def get_run(self, run_id: str) -> sqlite3.Row | None:
        rows = self._query("SELECT * FROM runs WHERE run_id=?", (run_id,))
        return rows[0] if rows else None

    async def aget_run(self, run_id: str) -> sqlite3.Row | None:
        return await asyncio.to_thread(self.get_run, run_id)

    async def find_by_idempotency(self, pipeline: str, key: str) -> sqlite3.Row | None:
        rows = await self._aquery(
            "SELECT * FROM runs WHERE pipeline=? AND idempotency_key=?", (pipeline, key)
        )
        return rows[0] if rows else None

    def list_runs(
        self,
        *,
        pipeline: str | None = None,
        status: str | None = None,
        since: str | None = None,
        limit: int = 50,
    ) -> list[sqlite3.Row]:
        sql = "SELECT * FROM runs WHERE 1=1"
        params: list[Any] = []
        if pipeline:
            sql += " AND pipeline=?"
            params.append(pipeline)
        if status:
            sql += " AND status=?"
            params.append(status)
        if since:
            sql += " AND started_at>=?"
            params.append(since)
        sql += " ORDER BY started_at DESC LIMIT ?"
        params.append(limit)
        return self._query(sql, tuple(params))

    # --------------------------------------------------------------------- steps

    async def start_step(self, rec: StepRecord) -> None:
        await self._awrite(
            "INSERT INTO run_steps (step_run_id, run_id, seq, step_name, kind, attempt,"
            " status, input_json, prompt_version, model, started_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                rec.step_run_id,
                rec.run_id,
                rec.seq,
                rec.step_name,
                rec.kind,
                rec.attempt,
                rec.status,
                rec.input_json,
                rec.prompt_version,
                rec.model,
                rec.started_at,
            ),
        )

    async def finish_step(self, rec: StepRecord) -> None:
        await self._awrite(
            "UPDATE run_steps SET status=?, output_json=?, tokens_in=?, tokens_out=?,"
            " latency_ms=?, error=?, ended_at=?, model=?, prompt_version=? WHERE step_run_id=?",
            (
                rec.status,
                rec.output_json,
                rec.tokens_in,
                rec.tokens_out,
                rec.latency_ms,
                rec.error,
                rec.ended_at or utcnow(),
                rec.model,
                rec.prompt_version,
                rec.step_run_id,
            ),
        )

    async def mark_interrupted(self, run_id: str) -> None:
        """Close out attempts left open by a process that died mid-step."""
        await self._awrite(
            "UPDATE run_steps SET status='failed', error='process interrupted', ended_at=?"
            " WHERE run_id=? AND status='running'",
            (utcnow(), run_id),
        )

    def steps_of(self, run_id: str) -> list[sqlite3.Row]:
        return self._query(
            "SELECT * FROM run_steps WHERE run_id=? ORDER BY seq, attempt", (run_id,)
        )

    async def asteps_of(self, run_id: str) -> list[sqlite3.Row]:
        return await asyncio.to_thread(self.steps_of, run_id)

    async def completed_steps(self, run_id: str) -> dict[int, sqlite3.Row]:
        """Last successful attempt per sequence number — the resume checkpoint."""
        rows = await self.asteps_of(run_id)
        return {r["seq"]: r for r in rows if r["status"] == "ok"}

    # ------------------------------------------------------------------- signals

    def record_signal(self, run_id: str, kind: str, detail: str | None = None) -> str:
        """Record a misfit signal, once.

        The same observation about the same run is one signal however many code
        paths notice it — the report counts runs that went wrong, not how loudly.
        """
        if kind not in SIGNAL_KINDS:
            raise ValueError(f"unknown misfit signal {kind!r}; expected one of {SIGNAL_KINDS}")
        existing = self._query(
            "SELECT signal_id FROM misfit_signals WHERE run_id=? AND kind=?"
            " AND IFNULL(detail,'')=IFNULL(?,'')",
            (run_id, kind, detail),
        )
        if existing:
            return str(existing[0]["signal_id"])
        signal_id = new_id()
        self._write(
            "INSERT INTO misfit_signals (signal_id, run_id, kind, detail, recorded_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (signal_id, run_id, kind, detail, utcnow()),
        )
        return signal_id

    async def arecord_signal(self, run_id: str, kind: str, detail: str | None = None) -> str:
        return await asyncio.to_thread(self.record_signal, run_id, kind, detail)

    def signals(
        self, *, pipeline: str | None = None, since: str | None = None
    ) -> list[sqlite3.Row]:
        sql = "SELECT s.* FROM misfit_signals s JOIN runs r ON r.run_id = s.run_id WHERE 1=1"
        params: list[Any] = []
        if pipeline:
            sql += " AND r.pipeline=?"
            params.append(pipeline)
        if since:
            sql += " AND s.recorded_at>=?"
            params.append(since)
        return self._query(sql + " ORDER BY s.recorded_at DESC", tuple(params))

    def count_runs(self, *, pipeline: str | None = None, since: str | None = None) -> int:
        sql = "SELECT COUNT(*) c FROM runs WHERE 1=1"
        params: list[Any] = []
        if pipeline:
            sql += " AND pipeline=?"
            params.append(pipeline)
        if since:
            sql += " AND started_at>=?"
            params.append(since)
        return int(self._query(sql, tuple(params))[0]["c"])

    # ---------------------------------------------------- measured latency (§13)

    def measured_latency(self, pipeline: str) -> tuple[int, dict[str, float]]:
        """(number of finished runs, mean latency in ms per step name).

        ``bounds()`` swaps its static per-model estimate for these once there is
        enough history to be worth quoting — and says which one it used.
        """
        n = int(
            self._query(
                "SELECT COUNT(*) c FROM runs WHERE pipeline=? AND status IN"
                " ('done','needs_review')",
                (pipeline,),
            )[0]["c"]
        )
        rows = self._query(
            "SELECT step_name, AVG(latency_ms) avg_ms FROM run_steps s"
            " JOIN runs r ON r.run_id = s.run_id"
            " WHERE r.pipeline=? AND s.latency_ms IS NOT NULL GROUP BY step_name",
            (pipeline,),
        )
        return n, {r["step_name"]: float(r["avg_ms"]) for r in rows}
