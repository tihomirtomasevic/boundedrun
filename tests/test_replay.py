"""Replay: compare recorded runs against the code as it is now."""

from __future__ import annotations

import pytest

from boundedrun import Pipeline, judgment, replay, step

from .conftest import TEST_MODEL, FakeModel

pytestmark = pytest.mark.anyio


@step
async def prepare(ctx, value):
    return f"doc {value}"


@judgment(prompt_version="classify@v7", max_tokens=100, max_input_tokens=1_000)
async def classify(ctx, text):
    return await ctx.model(prompt=text)


@step
async def persist(ctx, value):
    return {**value, "stored": True}


def build(store, model, name="replayable") -> Pipeline:
    return Pipeline(
        name=name,
        steps=[prepare, classify, persist],
        store=store,
        model=model,
        model_spec=TEST_MODEL,
    )


def doc(i: int) -> str:
    """Zero-padded so that no document id is a substring of another."""
    return f"{i:03d}"


async def record_runs(store, categories: list[str]) -> FakeModel:
    model = FakeModel(replies={doc(i): {"category": c} for i, c in enumerate(categories)})
    pipeline = build(store, model)
    for i in range(len(categories)):
        await pipeline.run(doc(i), idempotency_key=f"doc-{i}")
    return model


async def test_an_unchanged_prompt_reproduces_every_outcome(store):
    model = await record_runs(store, ["invoice", "invoice", "contract"])
    report = await replay(build(store, model))

    assert report.runs == 3
    assert (report.identical, report.changed) == (3, 0)


async def test_a_changed_outcome_is_reported_as_a_transition(store):
    await record_runs(store, ["invoice"] * 7 + ["contract"] * 4)
    new_model = FakeModel(
        replies={doc(i): {"category": "receipt"} for i in range(7)},
        reply={"category": "invoice"},
    )
    report = await replay(build(store, new_model))

    assert report.runs == 11
    assert report.changed == 11
    assert report.transitions["invoice -> receipt"] == 7
    assert report.transitions["contract -> invoice"] == 4


async def test_replay_does_not_call_the_model_for_deterministic_steps(store):
    await record_runs(store, ["invoice", "contract"])
    fresh = FakeModel(reply={"category": "invoice"})

    report = await replay(build(store, fresh), model=fresh)

    assert fresh.call_count == 2  # one judgment per run, not three steps per run
    assert report.deterministic_skipped == 4  # prepare + persist, twice


async def test_replay_never_touches_the_recorded_runs(store):
    await record_runs(store, ["invoice", "contract"])
    before = [dict(r) for r in store.list_runs()]
    before_steps = [dict(s) for r in before for s in store.steps_of(r["run_id"])]

    await replay(build(store, FakeModel(reply={"category": "receipt"})))

    after = [dict(r) for r in store.list_runs()]
    after_steps = [dict(s) for r in after for s in store.steps_of(r["run_id"])]
    assert before == after
    assert before_steps == after_steps
    assert store.signals() == []


async def test_replay_reports_the_cost_of_the_new_prompt(store):
    await record_runs(store, ["invoice", "invoice"])
    cheaper = FakeModel(reply={"category": "invoice"}, tokens_in=100, tokens_out=10)

    report = await replay(build(store, cheaper))

    assert report.cost_before > report.cost_after > 0


async def test_replaying_against_a_prompt_version_nobody_declares_is_refused(store):
    model = await record_runs(store, ["invoice"])
    with pytest.raises(ValueError, match="classify@v7"):
        await replay(build(store, model), prompt_version="classify@v99")


async def test_a_declared_prompt_version_selects_the_steps_to_replay(store):
    model = await record_runs(store, ["invoice", "contract"])
    report = await replay(build(store, model), prompt_version="classify@v7")

    assert report.runs == 2
    assert report.prompt_version == "classify@v7"
    assert "against classify@v7" in str(report)


async def test_a_changed_outcome_that_reroutes_a_branch_is_flagged(store):
    from boundedrun import branch

    @step
    async def invoice_arm(ctx, value):
        return value

    @step
    async def other_arm(ctx, value):
        return value

    model = FakeModel(reply={"category": "invoice"})
    recorded = Pipeline(
        name="branchy",
        steps=[classify, branch("category", {"invoice": [invoice_arm]}, default=[other_arm])],
        store=store,
        model=model,
        model_spec=TEST_MODEL,
    )
    await recorded.run("doc", idempotency_key="d1")

    rerouting = Pipeline(
        name="branchy",
        steps=[classify, branch("category", {"invoice": [invoice_arm]}, default=[other_arm])],
        store=store,
        model=FakeModel(reply={"category": "receipt"}),
        model_spec=TEST_MODEL,
    )
    report = await replay(rerouting)

    assert report.branch_changes == 1


async def test_a_replay_failure_is_a_finding_not_a_crash(store):
    await record_runs(store, ["invoice"])

    class Broken(FakeModel):
        async def __call__(self, prompt=None, **kw):
            raise RuntimeError("model down")

    report = await replay(build(store, Broken()))

    assert report.errors and "model down" in report.errors[0]
    assert report.runs == 1


async def test_replay_without_any_model_is_refused(store):
    await record_runs(store, ["invoice"])
    pipeline = Pipeline(
        name="replayable", steps=[prepare, classify, persist], store=store, model=None
    )
    with pytest.raises(ValueError, match="model"):
        await replay(pipeline)


async def test_an_empty_window_reports_nothing_rather_than_dividing_by_zero(store):
    model = FakeModel(reply={"category": "invoice"})
    report = await replay(build(store, model))

    assert report.runs == 0
    assert "no recorded runs" in str(report)
