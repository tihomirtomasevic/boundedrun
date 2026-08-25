"""Concurrency, only where it was declared."""

from __future__ import annotations

import asyncio

import pytest

from boundedrun import Pipeline, judgment, parallel, step

from .conftest import TEST_MODEL, FakeModel

pytestmark = pytest.mark.anyio


async def test_declared_parallel_steps_run_concurrently(store):
    @step
    async def slow_a(ctx, value):
        await asyncio.sleep(0.1)
        return "a"

    @step
    async def slow_b(ctx, value):
        await asyncio.sleep(0.1)
        return "b"

    pipeline = Pipeline(name="par", steps=[parallel(slow_a, slow_b)], store=store)

    started = asyncio.get_running_loop().time()
    result = await pipeline.run("x")
    elapsed = asyncio.get_running_loop().time() - started

    assert result.output == {"slow_a": "a", "slow_b": "b"}
    assert elapsed < 0.18  # sequential would be at least 0.2


async def test_a_merge_function_shapes_the_combined_output(store):
    @step
    async def left(ctx, value):
        return {"l": 1}

    @step
    async def right(ctx, value):
        return {"r": 2}

    pipeline = Pipeline(
        name="merged",
        steps=[parallel(left, right, merge=lambda out: {**out["left"], **out["right"]})],
        store=store,
    )
    result = await pipeline.run("x")

    assert result.output == {"l": 1, "r": 2}


async def test_every_parallel_member_gets_its_own_audit_row(store):
    @judgment(max_tokens=10, max_input_tokens=10)
    async def one(ctx, value):
        return await ctx.model(prompt="one")

    @judgment(max_tokens=10, max_input_tokens=10)
    async def two(ctx, value):
        return await ctx.model(prompt="two")

    model = FakeModel(reply={"ok": True})
    pipeline = Pipeline(
        name="par-audit",
        steps=[parallel(one, two)],
        store=store,
        model=model,
        model_spec=TEST_MODEL,
    )
    result = await pipeline.run("x")

    rows = {r["step_name"]: r for r in store.steps_of(result.run_id)}
    assert set(rows) == {"one", "two"}
    assert all(r["kind"] == "judgment" and r["status"] == "ok" for r in rows.values())
    assert result.calls == 2


async def test_a_failure_in_one_member_fails_the_run(store):
    @step
    async def fine(ctx, value):
        return "fine"

    @step
    async def broken(ctx, value):
        raise RuntimeError("nope")

    pipeline = Pipeline(name="par-fail", steps=[parallel(fine, broken)], store=store)
    result = await pipeline.run("x")

    assert result.status == "failed"
