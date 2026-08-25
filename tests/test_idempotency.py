"""One key, one execution — including when two callers arrive at once."""

from __future__ import annotations

import asyncio

import pytest

from boundedrun import Pipeline, RunInProgress, judgment, step

from .conftest import TEST_MODEL, FakeModel

pytestmark = pytest.mark.anyio


async def test_the_same_key_returns_the_first_result_without_redoing_the_work(store):
    runs = {"count": 0}

    @step
    async def work(ctx, value):
        runs["count"] += 1
        return {"answer": 42}

    pipeline = Pipeline(name="idem", steps=[work], store=store)

    first = await pipeline.run("x", idempotency_key="doc-1")
    second = await pipeline.run("x", idempotency_key="doc-1")

    assert runs["count"] == 1
    assert second.reused and not first.reused
    assert second.output == first.output == {"answer": 42}
    assert second.run_id == first.run_id


async def test_two_concurrent_runs_with_one_key_execute_once(store):
    runs = {"count": 0}

    @step
    async def work(ctx, value):
        runs["count"] += 1
        await asyncio.sleep(0.05)
        return {"answer": runs["count"]}

    pipeline = Pipeline(name="race", steps=[work], store=store)

    a, b = await asyncio.gather(
        pipeline.run("x", idempotency_key="doc-1"),
        pipeline.run("x", idempotency_key="doc-1"),
    )

    assert runs["count"] == 1
    assert a.output == b.output == {"answer": 1}
    assert a.run_id == b.run_id
    assert [a.reused, b.reused].count(True) == 1


async def test_a_reused_result_carries_the_recorded_cost(store):
    @judgment(max_tokens=100, max_input_tokens=1_000)
    async def classify(ctx, value):
        return await ctx.model(prompt="x")

    model = FakeModel(reply={"category": "invoice"})
    pipeline = Pipeline(
        name="cost", steps=[classify], store=store, model=model, model_spec=TEST_MODEL
    )

    first = await pipeline.run("x", idempotency_key="k")
    second = await pipeline.run("x", idempotency_key="k")

    assert model.call_count == 1
    assert second.cost_usd == pytest.approx(first.cost_usd)
    assert second.cost_usd > 0


async def test_a_failed_run_is_not_silently_retried_under_the_same_key(store):
    @step
    async def boom(ctx, value):
        raise RuntimeError("nope")

    pipeline = Pipeline(name="failed", steps=[boom], store=store)

    first = await pipeline.run("x", idempotency_key="k")
    second = await pipeline.run("x", idempotency_key="k")

    assert first.status == "failed"
    assert second.status == "failed" and second.reused


async def test_a_key_held_by_a_run_that_never_finished_reports_rather_than_hangs(store):
    from boundedrun.store import new_id

    store.claim_run(run_id=new_id(), pipeline="stuck", idempotency_key="k", input_hash_=None)

    @step
    async def work(ctx, value):
        return value

    pipeline = Pipeline(name="stuck", steps=[work], store=store, idempotency_wait_s=0.1)

    with pytest.raises(RunInProgress, match="resume"):
        await pipeline.run("x", idempotency_key="k")
