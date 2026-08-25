"""Declaration and execution of a fixed sequence with judgment steps in it.

The sequence is decided when you write it down. Branching is allowed on values
already in hand; it is never a free choice made by a model. That line is the
whole project: cross it and you are on rung 4, where the worst case stops being
knowable and this library stops being the right tool.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path as FsPath
from typing import Any

from . import bounds as bounds_mod
from .bounds import Bounds, Estimator, Meter
from .errors import (
    BoundsExceeded,
    BoundsUndeclared,
    NeedsReview,
    RunInProgress,
    SchemaViolation,
    Skip,
    StepFailed,
)
from .steps import (
    DETERMINISTIC,
    JUDGMENT,
    Branch,
    ModelSpec,
    Node,
    Parallel,
    Step,
    walk,
)
from .store import (
    TERMINAL_STATUSES,
    StepRecord,
    Store,
    decode,
    encode,
    input_hash,
    is_opaque,
    new_id,
    utcnow,
)

log = logging.getLogger("boundedrun")

_MISSING = object()
CHARS_PER_TOKEN = 4  # only used when a model wrapper reports no usage of its own


@dataclass
class ModelResult:
    """What a model call returned, and what it cost.

    Return one of these from your model callable to get exact accounting. A bare
    ``str`` or ``dict`` is accepted too; then token counts are estimated at
    ~4 characters per token and should be treated as such.
    """

    output: Any
    tokens_in: int = 0
    tokens_out: int = 0
    model: str | None = None
    estimated: bool = False

    @classmethod
    def coerce(cls, value: Any, prompt: str | None) -> ModelResult:
        if isinstance(value, cls):
            return value
        text_in = prompt or ""
        text_out = value if isinstance(value, str) else encode(value)
        return cls(
            output=value,
            tokens_in=len(text_in) // CHARS_PER_TOKEN,
            tokens_out=len(text_out) // CHARS_PER_TOKEN,
            estimated=True,
        )


@dataclass
class RunResult:
    """The outcome of one run. ``status`` is the whole story."""

    run_id: str
    pipeline: str
    status: str
    output: Any = None
    error: str | None = None
    cost_usd: float = 0.0
    tokens_in: int = 0
    tokens_out: int = 0
    calls: int = 0
    reused: bool = False
    signals: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status == "done"

    @property
    def needs_review(self) -> bool:
        return self.status == "needs_review"


class Context:
    """Handed to every step. The only way a step reaches the outside world."""

    NeedsReview = NeedsReview
    Skip = Skip

    def __init__(self, pipeline: Pipeline, run_id: str) -> None:
        self.pipeline = pipeline
        self.run_id = run_id
        self.state: dict[str, Any] = {}
        self.log = log
        self.step: str | None = None
        self.attempt: int = 0
        self._record: StepRecord | None = None
        self._spec: ModelSpec = pipeline.model_spec

    @property
    def store(self) -> Store:
        return self.pipeline.store

    async def model(self, prompt: str | None = None, **kwargs: Any) -> Any:
        """Call the model, accounting for every token against the run's bound."""
        if self.pipeline.model is None:
            raise RuntimeError(
                f"step {self.step!r} called ctx.model() but the pipeline was built without model="
            )
        if self._record is not None and self._record.kind != JUDGMENT:
            raise RuntimeError(
                f"step {self.step!r} is a @step but called ctx.model(); a step that calls "
                "a model is a @judgment — that is what makes its cost countable"
            )
        started = time.perf_counter()
        raw = (
            await self.pipeline.model(prompt=prompt, **kwargs)
            if prompt is not None
            else await self.pipeline.model(**kwargs)
        )
        result = ModelResult.coerce(raw, prompt)
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        spec = self._spec
        self.pipeline._meters[self.run_id].add_call(result.tokens_in, result.tokens_out, spec)
        if self._record is not None:
            self._record.tokens_in += result.tokens_in
            self._record.tokens_out += result.tokens_out
            self._record.model = result.model or spec.name
            self._record.latency_ms = (self._record.latency_ms or 0) + elapsed_ms
        return result.output

    async def signal(self, kind: str, detail: str | None = None) -> None:
        """Record a misfit signal (§9): evidence about where the fixed flow is wrong."""
        await self.store.arecord_signal(self.run_id, kind, detail)

    async def needs_step(self, name: str, detail: str | None = None) -> None:
        """'I would need a step called X here' — X not being in the pipeline is
        the strongest single signal that the sequence has outgrown itself."""
        await self.signal("missing_step", detail or name)


class Pipeline:
    """A fixed sequence of steps, some of which are judgment calls."""

    def __init__(
        self,
        name: str,
        steps: Sequence[Node],
        *,
        store: Store | str | FsPath | None = None,
        model: Callable[..., Any] | None = None,
        model_spec: ModelSpec | None = None,
        model_specs: dict[str, ModelSpec] | None = None,
        enforce_bounds: bool = False,
        idempotency_wait_s: float = 30.0,
        measured_after: int = 30,
    ) -> None:
        if not steps:
            raise ValueError("a pipeline needs at least one step")
        self.name = name
        self.nodes: list[Node] = list(steps)
        self.model = model
        self.model_spec = model_spec or ModelSpec.undeclared()
        self.model_specs = dict(model_specs or {})
        self.enforce_bounds = enforce_bounds
        self.idempotency_wait_s = idempotency_wait_s
        self.measured_after = measured_after
        self.store = store if isinstance(store, Store) else Store(store or ":memory:")
        self._meters: dict[str, Meter] = {}

        self._check_declaration()

    # ------------------------------------------------------------- declaration

    def _check_declaration(self) -> None:
        seen: set[str] = set()
        for step in walk(self.nodes):
            if step.name in seen:
                raise ValueError(
                    f"duplicate step name {step.name!r}: names are the audit trail's "
                    "primary key, so they must be unique across the declaration"
                )
            seen.add(step.name)
        if self.enforce_bounds:
            missing = bounds_mod.undeclared_judgments(self.nodes)
            if missing:
                raise BoundsUndeclared(
                    "enforce_bounds=True but these judgment steps declare no ceiling "
                    f"(max_tokens / max_input_tokens): {', '.join(missing)}"
                )

    @property
    def step_names(self) -> list[str]:
        return [s.name for s in walk(self.nodes)]

    def estimator(self) -> Estimator:
        est = Estimator(
            default_spec=self.model_spec,
            specs=self.model_specs,
            measured_threshold=self.measured_after,
        )
        # Reading history must never be the thing that creates the store: printing
        # bounds in CI should leave no files behind.
        if self.store.is_available():
            with contextlib.suppress(Exception):
                runs, measured = self.store.measured_latency(self.name)
                est.measured_runs, est.measured_ms = runs, measured
        return est

    def bounds(self) -> Bounds:
        """The worst case, computed without running anything (§5)."""
        return bounds_mod.compute(self.nodes, self.estimator(), name=self.name)

    def _spec_for(self, step: Step) -> ModelSpec:
        if step.model:
            return self.model_specs.get(step.model, ModelSpec(name=step.model, priced=False))
        return self.model_spec

    # -------------------------------------------------------------- public API

    async def run(self, value: Any, *, idempotency_key: str | None = None) -> RunResult:
        """Execute the sequence once."""
        if idempotency_key is not None:
            existing, run_id = await self._claim_or_wait(idempotency_key, value)
            if existing is not None:
                return existing
        else:
            run_id = new_id()
            await self.store.aclaim_run(
                run_id=run_id,
                pipeline=self.name,
                idempotency_key=None,
                input_hash_=input_hash(value),
            )
        return await self._execute(run_id, value, resume=False)

    async def resume(
        self,
        run_id: str | None = None,
        *,
        idempotency_key: str | None = None,
        value: Any = _MISSING,
    ) -> RunResult:
        """Continue an interrupted or failed run from its last successful step.

        Steps whose output is on disk are not re-executed. ``value`` is only
        needed when the interruption happened before the first checkpoint, or
        when the last checkpoint holds something JSON could not carry. Resuming
        a run that already finished returns its recorded result untouched.
        """
        if run_id is None:
            if idempotency_key is None:
                raise ValueError("resume() needs run_id or idempotency_key")
            row = await self.store.find_by_idempotency(self.name, idempotency_key)
            if row is None:
                raise LookupError(f"no run with idempotency_key {idempotency_key!r}")
            run_id = row["run_id"]
        row = await self.store.aget_run(run_id)
        if row is None:
            raise LookupError(f"no run {run_id!r}")
        if row["status"] in ("done", "needs_review"):
            return await self._result_from_store(run_id, reused=True)
        await self.store.mark_interrupted(run_id)
        await self.store.reopen_run(run_id)
        return await self._execute(run_id, value, resume=True)

    async def record_correction(self, run_id: str, detail: str) -> None:
        """A human changed the outcome after the fact (§9)."""
        await self.store.arecord_signal(run_id, "manual_correction", detail)

    async def record_branch_wrong(self, run_id: str, detail: str) -> None:
        """The branch taken was the wrong one for this input (§9)."""
        await self.store.arecord_signal(run_id, "branch_wrong", detail)

    async def aclose(self) -> None:
        await self.store.aclose()

    async def __aenter__(self) -> Pipeline:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    # ------------------------------------------------------------ idempotency

    async def _claim_or_wait(self, key: str, value: Any) -> tuple[RunResult | None, str]:
        run_id = new_id()
        claimed = await self.store.aclaim_run(
            run_id=run_id,
            pipeline=self.name,
            idempotency_key=key,
            input_hash_=input_hash(value),
        )
        if claimed:
            return None, run_id

        row = await self.store.find_by_idempotency(self.name, key)
        assert row is not None
        deadline = time.monotonic() + self.idempotency_wait_s
        while row["status"] not in TERMINAL_STATUSES:
            if time.monotonic() > deadline:
                raise RunInProgress(
                    f"run {row['run_id']} still holds idempotency key {key!r}; "
                    f"resume() it if the process that started it is gone"
                )
            await asyncio.sleep(0.05)
            row = await self.store.find_by_idempotency(self.name, key)
            assert row is not None
        return await self._result_from_store(row["run_id"], reused=True), row["run_id"]

    async def _result_from_store(self, run_id: str, *, reused: bool) -> RunResult:
        row = await self.store.aget_run(run_id)
        assert row is not None
        steps = await self.store.asteps_of(run_id)
        ok = [s for s in steps if s["status"] in ("ok", "skipped")]
        output = decode(ok[-1]["output_json"]) if ok else None
        error = next((s["error"] for s in reversed(steps) if s["error"]), None)
        return RunResult(
            run_id=run_id,
            pipeline=self.name,
            status=row["status"],
            output=output,
            error=error,
            cost_usd=row["cost_usd"] or 0.0,
            tokens_in=row["tokens_in"] or 0,
            tokens_out=row["tokens_out"] or 0,
            calls=sum(1 for s in steps if s["kind"] == JUDGMENT),
            reused=reused,
        )

    # -------------------------------------------------------------- execution

    async def _execute(self, run_id: str, value: Any, *, resume: bool) -> RunResult:
        meter = Meter(bounds=self.bounds(), enforce=self.enforce_bounds)
        self._meters[run_id] = meter
        ctx = Context(self, run_id)
        done = await self.store.completed_steps(run_id) if resume else {}
        state = _Walk(seq=0, value=value, done=done)
        status, error = "done", None
        try:
            await self._run_nodes(self.nodes, ctx, state)
        except NeedsReview as exc:
            status, error = "needs_review", str(exc)
            await self.store.arecord_signal(run_id, "needs_review", str(exc))
        except BoundsExceeded as exc:
            status, error = "bounds_exceeded", str(exc)
        except StepFailed as exc:
            status, error = "failed", str(exc)
        except Exception as exc:
            status, error = "failed", repr(exc)
        finally:
            await self.store.finish_run(
                run_id,
                status=status,
                cost_usd=meter.cost_usd,
                tokens_in=meter.tokens_in,
                tokens_out=meter.tokens_out,
            )
            self._meters.pop(run_id, None)
        output = state.value if status in ("done", "needs_review") else None
        if state.value is _MISSING:
            output = None
        return RunResult(
            run_id=run_id,
            pipeline=self.name,
            status=status,
            output=output,
            error=error,
            cost_usd=meter.cost_usd,
            tokens_in=meter.tokens_in,
            tokens_out=meter.tokens_out,
            calls=meter.calls,
        )

    async def _run_nodes(self, nodes: Sequence[Node], ctx: Context, state: _Walk) -> None:
        for node in nodes:
            if isinstance(node, Step):
                state.value = await self._run_step(node, ctx, state)
            elif isinstance(node, Parallel):
                await self._run_parallel(node, ctx, state)
            elif isinstance(node, Branch):
                await self._run_branch(node, ctx, state)
            else:
                raise TypeError(f"not a pipeline node: {node!r}")

    async def _run_branch(self, node: Branch, ctx: Context, state: _Walk) -> None:
        seq = state.take()
        recorded = state.done.get(seq)
        if state.value is _MISSING and recorded is None:
            raise RuntimeError(f"cannot resume into {node.name}: no recorded branch choice")
        if state.value is _MISSING:
            key = decode(recorded["output_json"])
            arm = list(node.arms.get(key) or node.default or [])
        else:
            key, arm = node.select(state.value)
        if recorded is None:
            rec = StepRecord(
                step_run_id=new_id(),
                run_id=ctx.run_id,
                seq=seq,
                step_name=f"{node.name}={key}",
                kind=DETERMINISTIC,
                attempt=1,
                status="ok",
                started_at=utcnow(),
                input_json=encode({"on": node.on}),
                output_json=encode(key),
                latency_ms=0,
            )
            await self.store.start_step(rec)
            await self.store.finish_step(rec)
        await self._run_nodes(arm, ctx, state)

    async def _run_parallel(self, node: Parallel, ctx: Context, state: _Walk) -> None:
        incoming = state.value
        seqs = {member.name: state.take() for member in node.nodes}
        outputs: dict[str, Any] = {}

        async def one(member: Step) -> None:
            member_ctx = Context(self, ctx.run_id)
            member_ctx.state = ctx.state
            sub = _Walk(seq=seqs[member.name], value=incoming, done=state.done)
            outputs[member.name] = await self._run_step(
                member, member_ctx, sub, seq=seqs[member.name]
            )

        async with asyncio.TaskGroup() as tg:
            for member in node.nodes:
                tg.create_task(one(member))
        state.value = node.combine(outputs)

    async def _run_step(
        self, step: Step, ctx: Context, state: _Walk, *, seq: int | None = None
    ) -> Any:
        seq = state.take() if seq is None else seq
        recorded = state.done.get(seq)
        if recorded is not None and recorded["step_name"] == step.name:
            output = decode(recorded["output_json"])
            if not is_opaque(output):
                log.debug("resume: reusing %s (seq %d)", step.name, seq)
                return output
        if state.value is _MISSING:
            raise RuntimeError(
                f"cannot resume at step {step.name!r}: nothing was checkpointed before it. "
                "Pass value= to resume() to re-run from the beginning."
            )

        value = state.value
        spec = self._spec_for(step)
        last_error: BaseException | None = None

        for attempt in range(1, step.attempts + 1):
            delay = step.policy.delay_before(attempt)
            if delay:
                await asyncio.sleep(delay)
            rec = StepRecord(
                step_run_id=new_id(),
                run_id=ctx.run_id,
                seq=seq,
                step_name=step.name,
                kind=step.kind,
                attempt=attempt,
                status="running",
                started_at=utcnow(),
                input_json=encode(value),
                prompt_version=step.prompt_version,
                model=spec.name if step.is_judgment else None,
            )
            await self.store.start_step(rec)
            ctx.step, ctx.attempt, ctx._record, ctx._spec = step.name, attempt, rec, spec
            started = time.perf_counter()
            try:
                output = await step.fn(ctx, value)
                step.validate_output(output)
            except Skip as exc:
                await self._settle(rec, "skipped", output=value, started=started, error=str(exc))
                await self.store.arecord_signal(ctx.run_id, "step_skipped", f"{step.name}: {exc}")
                return value
            except NeedsReview as exc:
                await self._settle(
                    rec,
                    "ok",
                    output={"needs_review": str(exc), "context": decode(encode(value))},
                    started=started,
                    error=str(exc),
                )
                raise
            except BoundsExceeded as exc:
                await self._settle(rec, "failed", output=None, started=started, error=str(exc))
                raise
            except Exception as exc:
                last_error = exc
                final = attempt == step.attempts
                await self._settle(
                    rec,
                    "failed" if final else "retried",
                    output=None,
                    started=started,
                    error=f"{type(exc).__name__}: {exc}",
                )
                if final:
                    raise StepFailed(step.name, exc) from exc
                log.warning(
                    "step %s attempt %d/%d failed (%s), retrying",
                    step.name,
                    attempt,
                    step.attempts,
                    exc,
                )
                continue
            await self._settle(rec, "ok", output=output, started=started)
            await self._auto_signals(step, output, ctx)
            return output

        raise StepFailed(step.name, last_error or RuntimeError("no attempts made"))

    async def _settle(
        self,
        rec: StepRecord,
        status: str,
        *,
        output: Any,
        started: float,
        error: str | None = None,
    ) -> None:
        rec.status = status
        rec.output_json = encode(output) if status in ("ok", "skipped") else None
        rec.error = error
        rec.ended_at = utcnow()
        measured = int((time.perf_counter() - started) * 1000)
        rec.latency_ms = max(rec.latency_ms or 0, measured)
        await self.store.finish_step(rec)

    async def _auto_signals(self, step: Step, output: Any, ctx: Context) -> None:
        """A judgment that asks for a step the pipeline does not have (§9)."""
        if not step.is_judgment or not isinstance(output, dict):
            return
        wanted = output.get("needs_step") or output.get("requested_step")
        if isinstance(wanted, str) and wanted not in self.step_names:
            await ctx.signal("missing_step", wanted)


@dataclass
class _Walk:
    """Cursor through one execution: sequence number, current value, checkpoints."""

    seq: int
    value: Any
    done: dict[int, Any] = field(default_factory=dict)

    def take(self) -> int:
        current = self.seq
        self.seq += 1
        return current


__all__ = [
    "Context",
    "ModelResult",
    "Pipeline",
    "RunResult",
    "SchemaViolation",
]
