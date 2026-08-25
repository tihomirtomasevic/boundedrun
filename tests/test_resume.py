"""Resume, in every state a run can be found in.

The SIGKILL case lives in test_crash_resume.py; this file covers the rest of
the surface, including the two cases where resume must refuse rather than guess.
"""

from __future__ import annotations

import pytest

from boundedrun import Pipeline, step
from boundedrun.store import new_id

pytestmark = pytest.mark.anyio


def flaky_pipeline(store, fail: dict, seen: list):
    @step
    async def first(ctx, value):
        seen.append("first")
        return {"stage": 1}

    @step
    async def second(ctx, value):
        seen.append("second")
        if fail["yes"]:
            raise RuntimeError("transient")
        return {**value, "stage": 2}

    return Pipeline(name="resumable", steps=[first, second], store=store)


async def test_a_failed_run_resumes_without_repeating_the_work_that_succeeded(store):
    seen, fail = [], {"yes": True}
    pipeline = flaky_pipeline(store, fail, seen)

    failed = await pipeline.run("x", idempotency_key="doc-1")
    assert failed.status == "failed"
    assert seen == ["first", "second"]

    fail["yes"] = False
    resumed = await pipeline.resume(failed.run_id)

    assert resumed.status == "done"
    assert resumed.output == {"stage": 2}
    assert seen == ["first", "second", "second"]  # 'first' was not run again


async def test_a_run_can_be_resumed_by_its_idempotency_key(store):
    seen, fail = [], {"yes": True}
    pipeline = flaky_pipeline(store, fail, seen)
    failed = await pipeline.run("x", idempotency_key="doc-1")

    fail["yes"] = False
    resumed = await pipeline.resume(idempotency_key="doc-1")

    assert resumed.run_id == failed.run_id
    assert resumed.status == "done"


async def test_resuming_a_finished_run_returns_its_result_untouched(store):
    seen, fail = [], {"yes": False}
    pipeline = flaky_pipeline(store, fail, seen)
    done = await pipeline.run("x", idempotency_key="doc-1")

    again = await pipeline.resume(done.run_id)

    assert again.reused and again.status == "done"
    assert again.output == done.output
    assert seen == ["first", "second"]


async def test_resume_needs_to_be_told_which_run(store):
    pipeline = flaky_pipeline(store, {"yes": False}, [])
    with pytest.raises(ValueError, match="run_id or idempotency_key"):
        await pipeline.resume()


async def test_resuming_something_that_does_not_exist_says_so(store):
    pipeline = flaky_pipeline(store, {"yes": False}, [])

    with pytest.raises(LookupError, match="no run"):
        await pipeline.resume("nope")
    with pytest.raises(LookupError, match="idempotency_key"):
        await pipeline.resume(idempotency_key="never-used")


async def test_resume_refuses_to_guess_when_nothing_was_checkpointed(store):
    """A run interrupted before its first step has no value to continue from."""
    pipeline = flaky_pipeline(store, {"yes": False}, [])
    run_id = new_id()
    store.claim_run(run_id=run_id, pipeline="resumable", idempotency_key="orphan", input_hash_=None)

    result = await pipeline.resume(run_id)

    assert result.status == "failed"
    assert "nothing was checkpointed" in result.error


async def test_passing_the_original_input_restarts_such_a_run(store):
    seen = []
    pipeline = flaky_pipeline(store, {"yes": False}, seen)
    run_id = new_id()
    store.claim_run(run_id=run_id, pipeline="resumable", idempotency_key="orphan", input_hash_=None)

    result = await pipeline.resume(run_id, value="x")

    assert result.status == "done"
    assert seen == ["first", "second"]


async def test_a_checkpoint_json_cannot_carry_is_re_executed_not_faked(store):
    seen, fail = [], {"yes": True}

    @step
    async def produce_bytes(ctx, value):
        seen.append("produce_bytes")
        return b"\x00 raw pdf bytes"

    @step
    async def consume(ctx, value):
        seen.append("consume")
        if fail["yes"]:
            raise RuntimeError("transient")
        return {"length": len(value)}

    pipeline = Pipeline(name="opaque", steps=[produce_bytes, consume], store=store)
    failed = await pipeline.run("x")
    assert failed.status == "failed"

    fail["yes"] = False
    resumed = await pipeline.resume(failed.run_id, value="x")

    assert resumed.status == "done"
    assert resumed.output == {"length": 15}
    # the opaque step ran again rather than resuming from a marker
    assert seen == ["produce_bytes", "consume", "produce_bytes", "consume"]


async def test_a_resumed_run_follows_the_branch_it_took_the_first_time(store):
    from boundedrun import branch

    seen, fail = [], {"yes": True}

    @step
    async def classify(ctx, value):
        seen.append("classify")
        return {"category": "invoice"}

    @step
    async def invoice_arm(ctx, value):
        seen.append("invoice_arm")
        if fail["yes"]:
            raise RuntimeError("transient")
        return {**value, "totals": 1}

    @step
    async def contract_arm(ctx, value):
        seen.append("contract_arm")
        return value

    pipeline = Pipeline(
        name="branchy-resume",
        steps=[
            classify,
            branch("category", {"invoice": [invoice_arm], "contract": [contract_arm]}),
        ],
        store=store,
    )
    failed = await pipeline.run("x")
    fail["yes"] = False

    resumed = await pipeline.resume(failed.run_id)

    assert resumed.status == "done"
    assert resumed.output == {"category": "invoice", "totals": 1}
    assert seen.count("classify") == 1
    assert "contract_arm" not in seen
