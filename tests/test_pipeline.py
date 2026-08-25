"""Execution semantics: order, context, retries, and the two deterministic exits."""

from __future__ import annotations

import pytest

from boundedrun import Pipeline, judgment, step

from .conftest import TEST_MODEL, FakeModel

pytestmark = pytest.mark.anyio


async def test_steps_run_in_declaration_order_and_chain_their_values(build):
    @step
    async def one(ctx, value):
        return value + "-one"

    @step
    async def two(ctx, value):
        return value + "-two"

    result = await build(one, two).run("start")

    assert result.status == "done"
    assert result.output == "start-one-two"


async def test_context_state_is_shared_across_steps_of_one_run(build):
    @step
    async def stash(ctx, value):
        ctx.state["seen"] = value
        return value

    @step
    async def read(ctx, value):
        return ctx.state["seen"]

    result = await build(stash, read).run("payload")
    assert result.output == "payload"


async def test_needs_review_is_an_outcome_not_a_failure(build, store):
    @step
    async def decide(ctx, value):
        raise ctx.NeedsReview("confidence 0.55 below 0.7")

    result = await build(decide).run({"confidence": 0.55})

    assert result.status == "needs_review"
    assert result.needs_review and not result.ok
    assert "0.55" in result.error
    assert store.get_run(result.run_id)["status"] == "needs_review"


async def test_needs_review_preserves_the_context_that_produced_it(build, store):
    @step
    async def enrich(ctx, value):
        return {**value, "enriched": True}

    @step
    async def decide(ctx, value):
        raise ctx.NeedsReview("too uncertain")

    result = await build(enrich, decide).run({"id": 7})

    assert result.output == {"id": 7, "enriched": True}
    rows = store.steps_of(result.run_id)
    recorded = next(r for r in rows if r["step_name"] == "decide")
    assert "too uncertain" in recorded["output_json"]
    assert "enriched" in recorded["output_json"]


async def test_needs_review_records_a_misfit_signal(build, store):
    @step
    async def decide(ctx, value):
        raise ctx.NeedsReview("low confidence")

    result = await build(decide).run("x")
    signals = [s for s in store.signals() if s["run_id"] == result.run_id]

    assert [s["kind"] for s in signals] == ["needs_review"]


async def test_skip_passes_the_value_through_and_records_the_signal(build, store):
    @step
    async def maybe(ctx, value):
        raise ctx.Skip("already done upstream")

    result = await build(maybe).run("untouched")

    assert result.status == "done"
    assert result.output == "untouched"
    signals = [s for s in store.signals() if s["run_id"] == result.run_id]
    assert signals[0]["kind"] == "step_skipped"
    assert store.steps_of(result.run_id)[0]["status"] == "skipped"


async def test_a_schema_violation_is_retried_and_then_fails(store):
    @judgment(
        output_schema={"type": "object", "required": ["category"]},
        max_tokens=10,
        max_input_tokens=10,
        retries=2,
    )
    async def classify(ctx, value):
        return await ctx.model(prompt="x")

    model = FakeModel(reply={"wrong_field": 1})
    pipeline = Pipeline(
        name="schema", steps=[classify], store=store, model=model, model_spec=TEST_MODEL
    )
    result = await pipeline.run("x")

    assert result.status == "failed"
    assert model.call_count == 3
    rows = store.steps_of(result.run_id)
    assert [r["status"] for r in rows] == ["retried", "retried", "failed"]
    assert "category" in rows[-1]["error"]


async def test_a_transient_failure_inside_the_retry_budget_succeeds(store):
    @judgment(max_tokens=10, max_input_tokens=10, retries=2)
    async def flaky(ctx, value):
        return await ctx.model(prompt="x")

    model = FakeModel(reply={"ok": True}, fail_times=2)
    pipeline = Pipeline(
        name="flaky", steps=[flaky], store=store, model=model, model_spec=TEST_MODEL
    )
    result = await pipeline.run("x")

    assert result.status == "done"
    assert result.output == {"ok": True}
    assert model.call_count == 3


async def test_retry_budgets_are_per_step_not_global(store):
    calls = {"patient": 0, "impatient": 0}

    @step(retries=3)
    async def patient(ctx, value):
        calls["patient"] += 1
        if calls["patient"] < 4:
            raise RuntimeError("not yet")
        return value

    @step(retries=0)
    async def impatient(ctx, value):
        calls["impatient"] += 1
        raise RuntimeError("never")

    result = await Pipeline(name="mixed", steps=[patient, impatient], store=store).run("x")

    assert result.status == "failed"
    assert calls == {"patient": 4, "impatient": 1}


async def test_the_audit_trail_of_a_judgment_step_is_complete(store):
    @judgment(prompt_version="classify@v7", max_tokens=100, max_input_tokens=1_000)
    async def classify(ctx, value):
        return await ctx.model(prompt=f"classify {value}")

    model = FakeModel(reply={"category": "invoice"}, tokens_in=1_234, tokens_out=56)
    pipeline = Pipeline(
        name="audit", steps=[classify], store=store, model=model, model_spec=TEST_MODEL
    )
    result = await pipeline.run("doc-1")
    row = store.steps_of(result.run_id)[0]

    assert row["kind"] == "judgment"
    assert row["prompt_version"] == "classify@v7"
    assert row["model"] == "test-model"
    assert (row["tokens_in"], row["tokens_out"]) == (1_234, 56)
    assert row["input_json"] == '"doc-1"'
    assert "invoice" in row["output_json"]
    assert row["started_at"] and row["ended_at"]


async def test_a_deterministic_step_may_not_call_the_model(build):
    @step
    async def sneaky(ctx, value):
        return await ctx.model(prompt="x")

    result = await build(sneaky).run("x")

    assert result.status == "failed"
    assert "that is what makes its cost countable" in result.error


async def test_calling_the_model_without_one_configured_is_a_clear_error(store):
    @judgment(max_tokens=10, max_input_tokens=10)
    async def classify(ctx, value):
        return await ctx.model(prompt="x")

    result = await Pipeline(name="nomodel", steps=[classify], store=store).run("x")

    assert result.status == "failed"
    assert "without model=" in result.error


async def test_a_judgment_asking_for_a_step_that_does_not_exist_is_recorded(store):
    @judgment(max_tokens=10, max_input_tokens=10)
    async def classify(ctx, value):
        return await ctx.model(prompt="x")

    model = FakeModel(reply={"category": "memo", "needs_step": "fetch_previous_contract"})
    pipeline = Pipeline(
        name="missing", steps=[classify], store=store, model=model, model_spec=TEST_MODEL
    )
    result = await pipeline.run("x")
    signals = [s for s in store.signals() if s["run_id"] == result.run_id]

    assert [(s["kind"], s["detail"]) for s in signals] == [
        ("missing_step", "fetch_previous_contract")
    ]


async def test_asking_for_a_step_that_does_exist_is_not_a_misfit(store):
    @judgment(max_tokens=10, max_input_tokens=10)
    async def classify(ctx, value):
        return await ctx.model(prompt="x")

    @step
    async def persist(ctx, value):
        return value

    model = FakeModel(reply={"needs_step": "persist"})
    pipeline = Pipeline(
        name="present",
        steps=[classify, persist],
        store=store,
        model=model,
        model_spec=TEST_MODEL,
    )
    result = await pipeline.run("x")

    assert [s for s in store.signals() if s["run_id"] == result.run_id] == []


async def test_an_empty_pipeline_is_rejected():
    with pytest.raises(ValueError, match="at least one step"):
        Pipeline(name="empty", steps=[])


async def test_manual_corrections_are_recorded_against_the_run(build, store):
    @step
    async def noop(ctx, value):
        return value

    pipeline = build(noop)
    result = await pipeline.run("x")
    await pipeline.record_correction(result.run_id, "category invoice -> receipt")
    await pipeline.record_branch_wrong(result.run_id, "invoice arm, should have been contract")

    kinds = sorted(s["kind"] for s in store.signals() if s["run_id"] == result.run_id)
    assert kinds == ["branch_wrong", "manual_correction"]
