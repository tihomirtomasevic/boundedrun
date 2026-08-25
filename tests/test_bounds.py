"""Static computation of the worst case, including branches, parallelism and
the switch from declared latency to measured latency (§13)."""

from __future__ import annotations

import pytest

from boundedrun import ModelSpec, Pipeline, branch, judgment, parallel, step
from boundedrun.bounds import Estimator, compute

from .conftest import TEST_MODEL

pytestmark = pytest.mark.anyio


@step(latency_ms=50)
async def det(ctx, value):
    return value


@step(latency_ms=50)
async def det2(ctx, value):
    return value


def a_judgment(name: str, *, tokens_in=1_000, tokens_out=100, retries=0, latency_ms=None):
    @judgment(
        name=name,
        prompt_version=f"{name}@v1",
        max_tokens=tokens_out,
        max_input_tokens=tokens_in,
        retries=retries,
        latency_ms=latency_ms,
    )
    async def fn(ctx, value):
        return await ctx.model(prompt="x")

    return fn


def bounds_of(*nodes, **kwargs):
    return Pipeline(name="b", steps=list(nodes), model_spec=TEST_MODEL, **kwargs).bounds()


def test_counts_deterministic_and_judgment_steps_separately():
    b = bounds_of(det, a_judgment("j1"), a_judgment("j2"))
    assert (b.steps, b.deterministic, b.judgments) == (3, 1, 2)


def test_retries_multiply_calls_tokens_and_cost():
    b = bounds_of(a_judgment("j", retries=2))
    assert b.worst.calls == 3
    assert b.worst.tokens_in == 3_000
    assert b.no_retry.calls == 1
    assert b.worst.cost_usd == pytest.approx(3 * (1.0 * 0.001 + 0.1 * 0.002))


def test_deterministic_latency_counts_toward_the_quote():
    b = bounds_of(det, det2, a_judgment("j", latency_ms=800))
    assert b.worst.latency_ms == pytest.approx(50 + 50 + 800)


def test_retry_backoff_is_included_in_worst_case_latency():
    @judgment(max_tokens=10, max_input_tokens=10, retries=2, backoff_s=1.0, latency_ms=100)
    async def slow(ctx, value):
        return await ctx.model(prompt="x")

    b = bounds_of(slow)
    # three attempts of 100ms, plus 1s then 2s of backoff
    assert b.worst.latency_ms == pytest.approx(300 + 3_000)


def test_branch_reports_the_worst_arm_and_the_range():
    cheap = branch(
        "category",
        {"invoice": [det], "contract": [a_judgment("parties", retries=1)]},
    )
    b = bounds_of(a_judgment("classify"), cheap)

    assert b.worst.calls == 3  # classify + two attempts of the expensive arm
    assert b.branch_low is not None
    assert b.branch_low.calls == 1
    assert b.worst_branch == ("category=contract",)
    assert "branch range" in str(b)


def test_parallel_sums_cost_but_maxes_latency():
    fast = a_judgment("fast", latency_ms=100)
    slow = a_judgment("slow", latency_ms=900)
    b = bounds_of(parallel(fast, slow))

    assert b.worst.calls == 2
    assert b.worst.tokens_in == 2_000
    assert b.worst.latency_ms == pytest.approx(900)


def test_a_pipeline_with_no_pricing_says_so_instead_of_quoting_zero():
    p = Pipeline(name="unpriced", steps=[a_judgment("j")], model_spec=ModelSpec.undeclared())
    rendered = str(p.bounds())
    assert "no pricing declared" in rendered
    assert p.bounds().priced is False


def test_undeclared_token_ceilings_are_reported_as_undeclared():
    @judgment(prompt_version="v1")
    async def loose(ctx, value):
        return await ctx.model(prompt="x")

    b = bounds_of(loose)
    assert b.tokens_declared is False
    assert "(undeclared)" in str(b)


def test_rendered_output_matches_the_documented_shape():
    b = bounds_of(det, a_judgment("j", retries=2))
    text = str(b)
    for fragment in (
        "boundedrun: 2 steps (1 deterministic, 1 judgment)",
        "worst case, with retries (max 3 attempts per step)",
        "model calls",
        "input tokens",
        "output tokens",
        "cost",
        "latency",
        "without retries:",
        "latency estimate: static, as declared",
    ):
        assert fragment in text


def test_measured_latency_replaces_the_static_estimate_once_there_is_history():
    est = Estimator(default_spec=TEST_MODEL, measured_runs=30, measured_ms={"j": 2_000.0})
    b = compute([a_judgment("j", latency_ms=100)], est, name="p")

    assert b.worst.latency_ms == pytest.approx(2_000)
    assert "measured, from 30 runs" in str(b)


def test_too_little_history_keeps_the_static_estimate_and_says_which_it_used():
    est = Estimator(default_spec=TEST_MODEL, measured_runs=29, measured_ms={"j": 2_000.0})
    b = compute([a_judgment("j", latency_ms=100)], est, name="p")

    assert b.worst.latency_ms == pytest.approx(100)
    assert "static, as declared" in str(b)


async def test_measured_latency_is_read_from_the_store_after_enough_runs(store, model):
    j = a_judgment("j", latency_ms=1)
    pipeline = Pipeline(
        name="measured",
        steps=[j],
        store=store,
        model=model,
        model_spec=TEST_MODEL,
        measured_after=3,
    )
    assert "static" in pipeline.bounds().latency_source

    for i in range(3):
        await pipeline.run("x", idempotency_key=f"k{i}")

    assert "measured, from 3 runs" in pipeline.bounds().latency_source


def test_bounds_serialise_to_json_for_ci():
    payload = bounds_of(det, a_judgment("j", retries=1)).to_dict()
    assert payload["judgments"] == 1
    assert payload["worst_case"]["calls"] == 2
    assert payload["without_retries"]["calls"] == 1
    assert "latency_source" in payload


def test_per_step_model_override_uses_that_models_price():
    big = ModelSpec(name="big", usd_per_1k_input=1.0, usd_per_1k_output=1.0)

    @judgment(model="big", max_tokens=1_000, max_input_tokens=1_000)
    async def on_big(ctx, value):
        return await ctx.model(prompt="x")

    p = Pipeline(
        name="two-models",
        steps=[on_big],
        model_spec=TEST_MODEL,
        model_specs={"big": big},
    )
    assert p.bounds().worst.cost_usd == pytest.approx(2.0)
