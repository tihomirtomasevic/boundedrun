"""Replay recorded runs against the current code.

This is the evaluation set the article asks for, except nobody assembled it: it
accumulated on its own, one real run at a time. Deterministic steps are not
re-executed — their recorded output already is the answer, and calling a model
for them would be paying for arithmetic. Nothing here writes to the store, so a
replay can never disturb the runs it learns from.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from .pipeline import Context, ModelResult
from .steps import Branch, Step, walk
from .store import Store, decode, since_ts

if TYPE_CHECKING:
    from .pipeline import Pipeline


@dataclass
class StepDiff:
    run_id: str
    step_name: str
    recorded_version: str | None
    before: Any
    after: Any

    @property
    def changed(self) -> bool:
        return self.before != self.after

    def transition(self, key: str | None = None) -> str | None:
        """A readable 'invoice -> receipt', when one scalar field explains it."""
        if not self.changed:
            return None
        before, after = self.before, self.after
        if isinstance(before, dict) and isinstance(after, dict):
            keys = [key] if key else sorted(set(before) | set(after))
            for k in keys:
                a, b = before.get(k), after.get(k)
                if a != b and _scalar(a) and _scalar(b):
                    return f"{a} -> {b}"
            return "output changed"
        if _scalar(before) and _scalar(after):
            return f"{before} -> {after}"
        return "output changed"


def _scalar(value: Any) -> bool:
    return isinstance(value, (str, int, float, bool))


@dataclass
class ReplayReport:
    pipeline: str
    runs: int = 0
    identical: int = 0
    changed: int = 0
    transitions: Counter = field(default_factory=Counter)
    branch_changes: int = 0
    cost_before: float = 0.0
    cost_after: float = 0.0
    calls: int = 0
    errors: list[str] = field(default_factory=list)
    diffs: list[StepDiff] = field(default_factory=list)
    prompt_version: str | None = None
    deterministic_skipped: int = 0

    @property
    def changed_run_ids(self) -> list[str]:
        return sorted({d.run_id for d in self.diffs if d.changed})

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "runs": self.runs,
            "identical": self.identical,
            "changed": self.changed,
            "transitions": dict(self.transitions),
            "branch_changes": self.branch_changes,
            "cost_before_per_run": round(self.cost_before, 6),
            "cost_after_per_run": round(self.cost_after, 6),
            "deterministic_steps_skipped": self.deterministic_skipped,
            "errors": self.errors,
        }

    def __str__(self) -> str:
        if not self.runs:
            return "boundedrun replay: no recorded runs matched."

        def pct(n: int) -> str:
            return f"({n / self.runs:.0%})"

        lines = [f"{self.runs:,} runs replayed"]
        if self.prompt_version:
            lines[0] += f" against {self.prompt_version}"
        lines += [
            f"  outcome identical  {self.identical:>5}  {pct(self.identical)}",
            f"  outcome changed    {self.changed:>5}  {pct(self.changed)}",
        ]
        for label, count in self.transitions.most_common():
            lines.append(f"      {label:<22} {count:>5}")
        if self.branch_changes:
            lines.append(f"  runs that would take a different branch: {self.branch_changes}")
        lines.append(f"  cost: ${self.cost_before:.2f} -> ${self.cost_after:.2f} per run")
        lines.append(
            f"  {self.deterministic_skipped:,} deterministic steps skipped (not re-executed)"
        )
        for err in self.errors[:5]:
            lines.append(f"  ! {err}")
        return "\n".join(lines)


class _ReplayContext(Context):
    """A context with the store disconnected: replays observe, they never record."""

    def __init__(self, pipeline: Pipeline, run_id: str, model: Callable[..., Any]) -> None:
        super().__init__(pipeline, run_id)
        self._model = model
        self.tokens_in = 0
        self.tokens_out = 0
        self.calls = 0

    async def model(self, prompt: str | None = None, **kwargs: Any) -> Any:
        raw = (
            await self._model(prompt=prompt, **kwargs)
            if prompt is not None
            else await self._model(**kwargs)
        )
        result = ModelResult.coerce(raw, prompt)
        self.calls += 1
        self.tokens_in += result.tokens_in
        self.tokens_out += result.tokens_out
        return result.output

    async def signal(self, kind: str, detail: str | None = None) -> None:
        return None


async def replay(
    pipeline: Pipeline,
    *,
    since: timedelta | None = None,
    prompt_version: str | None = None,
    limit: int = 500,
    model: Callable[..., Any] | None = None,
    store: Store | None = None,
    transition_key: str | None = None,
) -> ReplayReport:
    """Re-run the judgment steps of recorded runs against the code as it is now.

    A run's outcome is the tuple of its judgment outputs: those are the only
    places where a different prompt or model can change anything.
    """
    store = store or pipeline.store
    model = model or pipeline.model
    if model is None:
        raise ValueError("replay needs a model — pass model= or build the pipeline with one")

    declared = {s.name: s for s in walk(pipeline.nodes) if s.is_judgment}
    if prompt_version is not None:
        versions = {s.prompt_version for s in declared.values()}
        if prompt_version not in versions:
            raise ValueError(
                f"no judgment step currently declares prompt_version={prompt_version!r}; "
                f"the declaration has {sorted(v for v in versions if v)}. "
                "Replay compares recorded runs against the code you have now."
            )
        declared = {n: s for n, s in declared.items() if s.prompt_version == prompt_version}

    report = ReplayReport(pipeline=pipeline.name, prompt_version=prompt_version)
    rows = store.list_runs(pipeline=pipeline.name, since=since_ts(since), limit=limit)
    rows = [r for r in rows if r["status"] in ("done", "needs_review")]
    branches = [n for n in pipeline.nodes if isinstance(n, Branch)]

    for run in rows:
        steps = store.steps_of(run["run_id"])
        judgments = [
            s
            for s in steps
            if s["kind"] == "judgment" and s["status"] == "ok" and s["step_name"] in declared
        ]
        if not judgments:
            continue
        report.runs += 1
        report.deterministic_skipped += sum(
            1 for s in steps if s["kind"] == "deterministic" and s["status"] == "ok"
        )
        report.cost_before += run["cost_usd"] or 0.0

        ctx = _ReplayContext(pipeline, run["run_id"], model)
        run_changed = False
        for row in judgments:
            step: Step = declared[row["step_name"]]
            recorded_in = decode(row["input_json"])
            recorded_out = decode(row["output_json"])
            ctx.step, ctx.attempt = step.name, 1
            try:
                fresh = await step.fn(ctx, recorded_in)
                step.validate_output(fresh)
            except Exception as exc:
                report.errors.append(
                    f"{run['run_id'][:8]} {step.name}: {type(exc).__name__}: {exc}"
                )
                continue
            diff = StepDiff(run["run_id"], step.name, row["prompt_version"], recorded_out, fresh)
            report.diffs.append(diff)
            if diff.changed:
                run_changed = True
                label = diff.transition(transition_key)
                if label:
                    report.transitions[label] += 1
                for br in branches:
                    if br.key_of(recorded_out) != br.key_of(fresh):
                        report.branch_changes += 1
                        break
        report.changed += int(run_changed)
        report.identical += int(not run_changed)
        report.calls += ctx.calls
        report.cost_after += pipeline.model_spec.cost(ctx.tokens_in, ctx.tokens_out)

    if report.runs:
        report.cost_before /= report.runs
        report.cost_after /= report.runs
    return report
