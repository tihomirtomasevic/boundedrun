"""The load-bearing test.

Everything this library claims rests on one property: the number printed before
the run is not smaller than the bill after it. If that is ever false, the thesis
is false, so this file runs the genuinely worst case — every retry exhausted —
and compares reality against the published bound.
"""

from __future__ import annotations

import pytest

from boundedrun import Pipeline, judgment, step

from .conftest import TEST_MODEL, FakeModel

pytestmark = pytest.mark.anyio

TOKENS_IN, TOKENS_OUT = 1_000, 100


def flaky_pipeline(store, model, **kwargs) -> Pipeline:
    """Two judgments, each retried twice, each failing every attempt."""

    @judgment(prompt_version="a@v1", max_tokens=TOKENS_OUT, max_input_tokens=TOKENS_IN, retries=2)
    async def first(ctx, value):
        out = await ctx.model(prompt="first")
        raise RuntimeError(f"always fails after calling the model, got {out!r}")

    @judgment(prompt_version="b@v1", max_tokens=TOKENS_OUT, max_input_tokens=TOKENS_IN, retries=2)
    async def second(ctx, value):
        return await ctx.model(prompt="second")

    @step
    async def done(ctx, value):
        return value

    return Pipeline(
        name="worst-case",
        steps=[first, second, done],
        store=store,
        model=model,
        model_spec=TEST_MODEL,
        **kwargs,
    )


async def test_worst_case_run_never_exceeds_the_published_bound(store):
    model = FakeModel(reply={"ok": True}, tokens_in=TOKENS_IN, tokens_out=TOKENS_OUT)
    pipeline = flaky_pipeline(store, model)
    published = pipeline.bounds()

    result = await pipeline.run("input")

    assert result.status == "failed"
    assert model.call_count <= published.worst.calls
    assert result.tokens_in <= published.worst.tokens_in
    assert result.tokens_out <= published.worst.tokens_out
    assert result.cost_usd <= published.worst.cost_usd + 1e-9


async def test_the_bound_is_tight_not_merely_safe(store):
    """A bound nobody can reach is useless. The first judgment burns all three
    attempts, so the published call count is actually attained."""
    model = FakeModel(reply={"ok": True}, tokens_in=TOKENS_IN, tokens_out=TOKENS_OUT)
    pipeline = flaky_pipeline(store, model)
    published = pipeline.bounds()

    await pipeline.run("input")

    assert published.worst.calls == 6  # 2 judgments x 3 attempts
    assert model.call_count == 3  # the run stops when the first one gives up
    assert model.call_count == published.worst.calls // 2


async def test_enforce_bounds_raises_instead_of_quietly_costing_more(store):
    """A model that returns more tokens than declared trips the meter."""

    @judgment(max_tokens=10, max_input_tokens=10)
    async def greedy(ctx, value):
        return await ctx.model(prompt="x")

    model = FakeModel(reply={"ok": True}, tokens_in=5_000, tokens_out=5_000)
    pipeline = Pipeline(
        name="greedy",
        steps=[greedy],
        store=store,
        model=model,
        model_spec=TEST_MODEL,
        enforce_bounds=True,
    )

    result = await pipeline.run("input")

    assert result.status == "bounds_exceeded"
    row = store.get_run(result.run_id)
    assert row["status"] == "bounds_exceeded"


async def test_without_enforcement_the_same_run_merely_costs_more(store):
    @judgment(max_tokens=10, max_input_tokens=10)
    async def greedy(ctx, value):
        return await ctx.model(prompt="x")

    model = FakeModel(reply={"ok": True}, tokens_in=5_000, tokens_out=5_000)
    pipeline = Pipeline(
        name="greedy", steps=[greedy], store=store, model=model, model_spec=TEST_MODEL
    )

    result = await pipeline.run("input")

    assert result.status == "done"
    assert result.cost_usd > pipeline.bounds().worst.cost_usd


async def test_bounds_are_computed_without_running_anything(store, model):
    @judgment(max_tokens=100, max_input_tokens=1_000)
    async def never_called(ctx, value):
        return await ctx.model(prompt="x")

    pipeline = Pipeline(
        name="untouched",
        steps=[never_called],
        store=store,
        model=model,
        model_spec=TEST_MODEL,
    )

    published = pipeline.bounds()

    assert published.worst.calls == 1
    assert model.call_count == 0
    assert store.list_runs() == []


async def test_failed_step_is_recorded_with_every_attempt(store):
    model = FakeModel(reply={"ok": True}, tokens_in=TOKENS_IN, tokens_out=TOKENS_OUT)
    pipeline = flaky_pipeline(store, model)

    result = await pipeline.run("input")
    rows = store.steps_of(result.run_id)

    attempts = [r for r in rows if r["step_name"] == "first"]
    assert [r["attempt"] for r in attempts] == [1, 2, 3]
    assert [r["status"] for r in attempts] == ["retried", "retried", "failed"]


async def test_step_failure_surfaces_as_step_failed_not_a_bare_exception(store):
    @step
    async def boom(ctx, value):
        raise ValueError("nope")

    pipeline = Pipeline(name="boom", steps=[boom], store=store, model=None)
    result = await pipeline.run("input")

    assert result.status == "failed"
    assert "boom" in result.error and "nope" in result.error


def test_bounds_check_reports_every_violated_budget(store, model):
    @judgment(max_tokens=1_000, max_input_tokens=10_000, retries=2)
    async def expensive(ctx, value):
        return await ctx.model(prompt="x")

    pipeline = Pipeline(
        name="expensive", steps=[expensive], store=store, model=model, model_spec=TEST_MODEL
    )
    violations = pipeline.bounds().check(max_cost=0.001, max_calls=1, max_tokens=100)

    assert len(violations) == 3
    assert any("cost" in v for v in violations)
    assert any("model calls" in v for v in violations)
    assert any("tokens" in v for v in violations)
