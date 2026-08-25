"""Deterministic branching — chosen by a value in hand, never by the model."""

from __future__ import annotations

import pytest

from boundedrun import Pipeline, branch, judgment, step

from .conftest import TEST_MODEL, FakeModel

pytestmark = pytest.mark.anyio


@step
async def invoice_arm(ctx, value):
    return {**value, "took": "invoice"}


@step
async def contract_arm(ctx, value):
    return {**value, "took": "contract"}


@step
async def fallback_arm(ctx, value):
    return {**value, "took": "default"}


@judgment(prompt_version="classify@v7", max_tokens=100, max_input_tokens=1_000)
async def classify(ctx, value):
    return await ctx.model(prompt=value)


def routed(store, model, *, with_default: bool = True) -> Pipeline:
    return Pipeline(
        name="routed",
        steps=[
            classify,
            branch(
                "category",
                {"invoice": [invoice_arm], "contract": [contract_arm]},
                default=[fallback_arm] if with_default else None,
            ),
        ],
        store=store,
        model=model,
        model_spec=TEST_MODEL,
    )


@pytest.mark.parametrize(
    ("category", "expected"),
    [("invoice", "invoice"), ("contract", "contract"), ("memo", "default")],
)
async def test_each_arm_is_taken_for_its_own_category(store, category, expected):
    model = FakeModel(reply={"category": category})
    result = await routed(store, model).run("doc")

    assert result.status == "done"
    assert result.output["took"] == expected


async def test_the_branch_choice_is_written_to_the_audit_trail(store):
    model = FakeModel(reply={"category": "contract"})
    result = await routed(store, model).run("doc")

    names = [r["step_name"] for r in store.steps_of(result.run_id)]
    assert names == ["classify", "branch:category=contract", "contract_arm"]


async def test_an_unroutable_value_without_a_default_fails_loudly(store):
    model = FakeModel(reply={"category": "memo"})
    result = await routed(store, model, with_default=False).run("doc")

    assert result.status == "failed"
    assert "no arm for" in result.error


async def test_only_the_steps_of_the_chosen_arm_are_executed(store):
    seen = []

    @step
    async def left(ctx, value):
        seen.append("left")
        return value

    @step
    async def right(ctx, value):
        seen.append("right")
        return value

    pipeline = Pipeline(
        name="one-arm",
        steps=[branch("k", {"a": [left], "b": [right]})],
        store=store,
    )
    await pipeline.run({"k": "a"})

    assert seen == ["left"]


async def test_bounds_report_the_worst_arm_of_a_real_pipeline(store):
    model = FakeModel(reply={"category": "invoice"})
    published = routed(store, model).bounds()

    assert published.branch_low is not None
    assert published.worst.calls >= published.branch_low.calls
    assert "branch range" in str(published)
