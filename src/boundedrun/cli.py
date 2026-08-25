"""``boundedrun`` — bounds, runs, show, replay, graduation, resume, correct.

``bounds`` is the one that belongs in CI: it prints the worst case without
running anything, and fails the build when a new step quietly makes the flow
more expensive than you told everyone it was.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import re
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import typer

from .graduation import DEFAULT_THRESHOLD
from .graduation import graduation as graduation_report
from .pipeline import Pipeline
from .replay import replay as replay_runs
from .store import Store, decode

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="A fixed pipeline with a language model in the middle.",
)

DURATION = re.compile(r"^(\d+)([smhdw])$")
_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}


def parse_since(value: str | None) -> timedelta | None:
    """'30d', '12h', '90m' — the window every report takes."""
    if not value:
        return None
    match = DURATION.match(value.strip())
    if not match:
        raise typer.BadParameter(f"expected something like 30d or 12h, got {value!r}")
    amount, unit = int(match.group(1)), match.group(2)
    return timedelta(**{_UNITS[unit]: amount})


def load_pipeline(target: str) -> Pipeline:
    """Import ``package.module:attribute`` and return the Pipeline it names."""
    if ":" not in target:
        raise typer.BadParameter(
            f"expected module:attribute (e.g. mypkg.flows:classify), got {target!r}"
        )
    module_name, attr = target.split(":", 1)
    if str(Path.cwd()) not in sys.path:
        sys.path.insert(0, str(Path.cwd()))
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise typer.BadParameter(f"cannot import {module_name!r}: {exc}") from exc
    try:
        obj = getattr(module, attr)
    except AttributeError as exc:
        raise typer.BadParameter(f"{module_name!r} has no attribute {attr!r}") from exc
    if not isinstance(obj, Pipeline):
        raise typer.BadParameter(f"{target} is a {type(obj).__name__}, not a Pipeline")
    return obj


def open_store(path: str) -> Store:
    if path != ":memory:" and not Path(path).exists():
        raise typer.BadParameter(f"no store at {path!r}")
    return Store(path)


def emit(payload: dict[str, Any] | Any, as_json: bool) -> None:
    typer.echo(json.dumps(payload.to_dict(), indent=2) if as_json else str(payload))


@app.command()
def bounds(
    target: str = typer.Argument(..., help="module:attribute naming a Pipeline"),
    store: str = typer.Option("", help="read measured latency from this store"),
    max_cost: float = typer.Option(None, help="fail if worst-case cost exceeds this"),
    max_latency: float = typer.Option(None, help="fail if worst-case latency (s) exceeds this"),
    max_calls: int = typer.Option(None, help="fail if worst-case model calls exceed this"),
    max_tokens: int = typer.Option(None, help="fail if worst-case total tokens exceed this"),
    as_json: bool = typer.Option(False, "--json", help="machine-readable output"),
) -> None:
    """Print the worst case of a pipeline without running it."""
    pipeline = load_pipeline(target)
    if store:
        pipeline.store = open_store(store)
    result = pipeline.bounds()
    emit(result, as_json)
    violations = result.check(
        max_cost=max_cost,
        max_latency_s=max_latency,
        max_calls=max_calls,
        max_tokens=max_tokens,
    )
    if violations:
        typer.echo("", err=True)
        for line in violations:
            typer.echo(f"bounds exceeded: {line}", err=True)
        raise typer.Exit(1)


@app.command()
def runs(
    store: str = typer.Option("./runs.db", help="path to the run store"),
    pipeline: str = typer.Option("", help="only this pipeline"),
    status: str = typer.Option("", help="only this status"),
    since: str = typer.Option("", help="window, e.g. 30d"),
    limit: int = typer.Option(20),
) -> None:
    """List recorded runs, most recent first."""
    from .store import since_ts

    db = open_store(store)
    rows = db.list_runs(
        pipeline=pipeline or None,
        status=status or None,
        since=since_ts(parse_since(since or None)),
        limit=limit,
    )
    if not rows:
        typer.echo("no runs matched")
        return
    typer.echo(f"{'run_id':<34}{'pipeline':<18}{'status':<16}{'cost':>8}  started")
    for row in rows:
        typer.echo(
            f"{row['run_id']:<34}{row['pipeline']:<18}{row['status']:<16}"
            f"{row['cost_usd'] or 0:>8.4f}  {row['started_at'][:19]}"
        )


@app.command()
def show(
    run_id: str = typer.Argument(..., help="run to inspect"),
    store: str = typer.Option("./runs.db", help="path to the run store"),
) -> None:
    """Print one run's audit trail: every step, every attempt."""
    db = open_store(store)
    row = db.get_run(run_id)
    if row is None:
        typer.echo(f"no run {run_id}", err=True)
        raise typer.Exit(1)
    typer.echo(
        f"run {row['run_id']}  {row['pipeline']}  {row['status']}\n"
        f"  started {row['started_at']}  ended {row['ended_at'] or '-'}\n"
        f"  cost ${row['cost_usd'] or 0:.4f}  tokens {row['tokens_in']}/{row['tokens_out']}"
    )
    for step in db.steps_of(run_id):
        head = (
            f"  [{step['seq']:>2}] {step['step_name']:<24} {step['kind']:<14}"
            f"attempt {step['attempt']}  {step['status']}"
        )
        typer.echo(head)
        if step["kind"] == "judgment":
            typer.echo(
                f"       prompt={step['prompt_version']} model={step['model']} "
                f"tokens={step['tokens_in']}/{step['tokens_out']} "
                f"latency={step['latency_ms']}ms"
            )
        if step["error"]:
            typer.echo(f"       error: {step['error']}")
        if step["output_json"]:
            typer.echo(f"       out: {_clip(step['output_json'])}")
    signals = [s for s in db.signals() if s["run_id"] == run_id]
    for sig in signals:
        typer.echo(f"  signal: {sig['kind']}  {sig['detail'] or ''}")


@app.command()
def replay(
    target: str = typer.Argument(..., help="module:attribute naming a Pipeline"),
    store: str = typer.Option("", help="override the pipeline's store"),
    since: str = typer.Option("", help="window, e.g. 30d"),
    prompt: str = typer.Option("", help="prompt version to replay against, e.g. classify@v8"),
    limit: int = typer.Option(500),
    as_json: bool = typer.Option(False, "--json"),
) -> None:
    """Re-run recorded judgment steps against the current code and diff outcomes."""
    pipeline = load_pipeline(target)
    if store:
        pipeline.store = open_store(store)
    report = asyncio.run(
        replay_runs(
            pipeline,
            since=parse_since(since or None),
            prompt_version=prompt or None,
            limit=limit,
        )
    )
    emit(report, as_json)


@app.command()
def graduation(
    store: str = typer.Option("./runs.db", help="path to the run store"),
    pipeline: str = typer.Option("", help="only this pipeline"),
    since: str = typer.Option("", help="window, e.g. 90d"),
    threshold: float = typer.Option(DEFAULT_THRESHOLD, help="misfit rate that justifies rung 4"),
    as_json: bool = typer.Option(False, "--json"),
) -> None:
    """Report how often the fixed flow was the wrong shape — and what to do about it."""
    db = open_store(store)
    report = asyncio.run(
        graduation_report(
            db,
            pipeline=pipeline or None,
            since=parse_since(since or None),
            threshold=threshold,
        )
    )
    emit(report, as_json)


@app.command()
def resume(
    target: str = typer.Argument(..., help="module:attribute naming a Pipeline"),
    run_id: str = typer.Argument(..., help="the interrupted run"),
    store: str = typer.Option("", help="override the pipeline's store"),
) -> None:
    """Continue an interrupted run from its last successful step."""
    pipeline = load_pipeline(target)
    if store:
        pipeline.store = open_store(store)
    result = asyncio.run(pipeline.resume(run_id))
    typer.echo(f"{result.run_id}  {result.status}  ${result.cost_usd:.4f}")
    if result.error:
        typer.echo(f"  {result.error}")


@app.command()
def correct(
    run_id: str = typer.Argument(..., help="the run a human corrected"),
    detail: str = typer.Option(..., help="what was wrong, e.g. 'category invoice -> receipt'"),
    store: str = typer.Option("./runs.db", help="path to the run store"),
    kind: str = typer.Option("manual_correction", help="misfit signal kind"),
) -> None:
    """Record that a human changed a run's outcome after the fact (§9)."""
    db = open_store(store)
    if db.get_run(run_id) is None:
        typer.echo(f"no run {run_id}", err=True)
        raise typer.Exit(1)
    db.record_signal(run_id, kind, detail)
    typer.echo(f"recorded {kind} on {run_id}")


def _clip(text: str, width: int = 96) -> str:
    value = decode(text)
    rendered = json.dumps(value, ensure_ascii=False) if value is not None else text
    return rendered if len(rendered) <= width else rendered[: width - 1] + "…"


def main() -> None:
    app()


if __name__ == "__main__":
    main()
