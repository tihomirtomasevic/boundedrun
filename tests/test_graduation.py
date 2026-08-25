"""The graduation report: all five signals, and a recommendation that flips."""

from __future__ import annotations

from datetime import timedelta

import pytest

from boundedrun import Pipeline, graduation, judgment, step
from boundedrun.store import SIGNAL_KINDS

from .conftest import TEST_MODEL, FakeModel

pytestmark = pytest.mark.anyio


@step
async def passthrough(ctx, value):
    return value


async def clean_runs(store, n: int) -> Pipeline:
    pipeline = Pipeline(name="grad", steps=[passthrough], store=store)
    for i in range(n):
        await pipeline.run("x", idempotency_key=f"clean-{i}")
    return pipeline


async def test_all_five_misfit_signals_are_recorded(store):
    @judgment(max_tokens=10, max_input_tokens=10)
    async def classify(ctx, value):
        return await ctx.model(prompt="x")

    @step
    async def skipper(ctx, value):
        raise ctx.Skip("nothing to do")

    @step
    async def decline(ctx, value):
        raise ctx.NeedsReview("not confident")

    model = FakeModel(reply={"category": "memo", "needs_step": "fetch_previous_contract"})
    pipeline = Pipeline(
        name="all-signals",
        steps=[classify, skipper, decline],
        store=store,
        model=model,
        model_spec=TEST_MODEL,
    )
    result = await pipeline.run("x")
    await pipeline.record_correction(result.run_id, "category memo -> letter")
    await pipeline.record_branch_wrong(result.run_id, "default arm was wrong")

    recorded = {s["kind"] for s in store.signals()}
    assert recorded == set(SIGNAL_KINDS)


async def test_a_quiet_pipeline_is_told_to_stay_where_it_is(store):
    await clean_runs(store, 20)
    report = await graduation(store, pipeline="grad")

    assert report.runs == 20
    assert report.signal_runs == 0
    assert report.should_climb is False
    assert "Stay on rung 3" in report.recommendation
    assert "below the 15% threshold" in str(report)


async def test_crossing_the_threshold_flips_the_recommendation(store):
    pipeline = await clean_runs(store, 17)

    @step
    async def decline(ctx, value):
        raise ctx.NeedsReview("no arm for this")

    noisy = Pipeline(name="grad", steps=[decline], store=store)
    for i in range(3):
        await noisy.run("x", idempotency_key=f"noisy-{i}")

    report = await graduation(store, pipeline="grad")

    assert (report.runs, report.signal_runs) == (20, 3)
    assert report.rate == pytest.approx(0.15)
    assert report.should_climb is True
    assert "evidence to climb" in report.recommendation
    assert len(report.evaluation_set) == 3
    assert pipeline.name == "grad"


async def test_the_threshold_is_configurable(store):
    pipeline = await clean_runs(store, 9)

    @step
    async def decline(ctx, value):
        raise ctx.NeedsReview("nope")

    await Pipeline(name="grad", steps=[decline], store=store).run("x", idempotency_key="n")

    lenient = await graduation(store, pipeline="grad", threshold=0.5)
    strict = await graduation(store, pipeline="grad", threshold=0.05)

    assert lenient.should_climb is False
    assert strict.should_climb is True
    assert pipeline.name == "grad"


async def test_the_report_names_the_most_common_detail(store):
    @judgment(max_tokens=10, max_input_tokens=10)
    async def classify(ctx, value):
        return await ctx.model(prompt="x")

    model = FakeModel(reply={"needs_step": "fetch_previous_contract"})
    pipeline = Pipeline(
        name="detail", steps=[classify], store=store, model=model, model_spec=TEST_MODEL
    )
    for i in range(4):
        await pipeline.run("x", idempotency_key=f"d-{i}")

    report = await graduation(store, pipeline="detail")

    assert report.tallies[0].kind == "missing_step"
    assert report.tallies[0].count == 4
    assert 'mostly "fetch_previous_contract"' in str(report)


async def test_an_empty_window_recommends_nothing(store):
    await clean_runs(store, 3)
    report = await graduation(store, pipeline="grad", since=timedelta(seconds=0))

    assert report.runs == 0
    assert "Nothing to recommend yet" in report.recommendation


async def test_the_window_label_appears_in_the_report(store):
    await clean_runs(store, 1)
    assert "last 90d" in str(await graduation(store, since=timedelta(days=90)))
    assert "all time" in str(await graduation(store))


async def test_the_report_serialises_for_a_dashboard(store):
    await clean_runs(store, 4)
    payload = (await graduation(store, pipeline="grad")).to_dict()

    assert payload["runs"] == 4
    assert payload["should_climb"] is False
    assert "recommendation" in payload
    assert payload["evaluation_set"] == []


async def test_signals_from_another_pipeline_do_not_count(store):
    await clean_runs(store, 5)

    @step
    async def decline(ctx, value):
        raise ctx.NeedsReview("nope")

    await Pipeline(name="other", steps=[decline], store=store).run("x")

    report = await graduation(store, pipeline="grad")
    assert report.signal_runs == 0
    assert (await graduation(store, pipeline="other")).signal_runs == 1
