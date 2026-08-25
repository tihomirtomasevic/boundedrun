"""Declaration-time behaviour: what a step carries and what it refuses."""

from __future__ import annotations

import pytest

from boundedrun import (
    ModelSpec,
    Pipeline,
    RetryPolicy,
    SchemaViolation,
    branch,
    judgment,
    parallel,
    step,
)
from boundedrun.steps import DETERMINISTIC, JUDGMENT, walk


def test_step_decorator_works_bare_and_called():
    @step
    async def bare(ctx, value):
        return value

    @step(retries=2, latency_ms=40)
    async def configured(ctx, value):
        return value

    assert bare.kind == DETERMINISTIC and bare.attempts == 1
    assert configured.attempts == 3 and configured.latency_ms == 40


def test_judgment_carries_everything_bounds_needs():
    @judgment(
        prompt_version="classify@v7",
        output_schema={"type": "object"},
        max_tokens=1_200,
        max_input_tokens=3_000,
        retries=2,
    )
    async def classify(ctx, value):
        return {}

    assert classify.kind == JUDGMENT
    assert classify.prompt_version == "classify@v7"
    assert (classify.max_tokens, classify.max_input_tokens) == (1_200, 3_000)
    assert classify.attempts == 3


def test_a_synchronous_step_is_rejected_at_declaration_time():
    with pytest.raises(TypeError, match="async"):

        @step
        def sync_step(ctx, value):
            return value


def test_a_broken_output_schema_is_rejected_at_declaration_time():
    with pytest.raises(Exception, match=r"(?i)schema|valid"):

        @judgment(output_schema={"type": "not-a-type"})
        async def bad(ctx, value):
            return {}


def test_schema_violation_names_the_step_and_the_field():
    @judgment(output_schema={"type": "object", "required": ["category"]})
    async def classify(ctx, value):
        return {}

    with pytest.raises(SchemaViolation, match="classify"):
        classify.validate_output({})


def test_retry_policy_backoff_is_per_step_and_bounded():
    policy = RetryPolicy(attempts=4, backoff_s=1.0, multiplier=2.0, max_backoff_s=3.0)

    assert policy.delay_before(1) == 0.0
    assert policy.delay_before(2) == 1.0
    assert policy.delay_before(3) == 2.0
    assert policy.delay_before(4) == 3.0  # clamped
    assert policy.worst_case_backoff_s == 6.0


def test_retry_policy_rejects_zero_attempts():
    with pytest.raises(ValueError, match="attempts"):
        RetryPolicy(attempts=0)


def test_walk_finds_steps_inside_branches_and_parallels():
    @step
    async def a(ctx, v):
        return v

    @step
    async def b(ctx, v):
        return v

    @step
    async def c(ctx, v):
        return v

    @step
    async def d(ctx, v):
        return v

    nodes = [a, branch("k", {"x": [b]}, default=[c]), parallel(d)]
    assert sorted(s.name for s in walk(nodes)) == ["a", "b", "c", "d"]


def test_branch_selects_the_declared_arm_and_falls_back_to_default():
    @step
    async def a(ctx, v):
        return v

    @step
    async def b(ctx, v):
        return v

    node = branch("category", {"invoice": [a]}, default=[b])

    assert node.select({"category": "invoice"})[0] == "invoice"
    assert node.select({"category": "other"})[0] == "default"


def test_branch_without_a_matching_arm_or_default_is_an_error():
    @step
    async def a(ctx, v):
        return v

    node = branch("category", {"invoice": [a]})
    with pytest.raises(KeyError, match="no arm"):
        node.select({"category": "contract"})


def test_parallel_rejects_nested_control_flow():
    @step
    async def a(ctx, v):
        return v

    with pytest.raises(TypeError, match="plain steps"):
        parallel(branch("k", {"x": [a]}))


def test_duplicate_step_names_are_rejected_because_names_are_the_audit_key():
    @step(name="same")
    async def a(ctx, v):
        return v

    @step(name="same")
    async def b(ctx, v):
        return v

    with pytest.raises(ValueError, match="duplicate step name"):
        Pipeline(name="dupes", steps=[a, b])


def test_enforce_bounds_refuses_a_judgment_that_declared_no_ceiling():
    @judgment(prompt_version="v1")
    async def unbounded(ctx, v):
        return {}

    with pytest.raises(Exception, match="max_tokens"):
        Pipeline(name="p", steps=[unbounded], enforce_bounds=True, model_spec=ModelSpec())


def test_model_spec_prices_what_it_is_told_and_nothing_more():
    spec = ModelSpec(name="m", usd_per_1k_input=0.003, usd_per_1k_output=0.015)
    assert spec.cost(1_000, 1_000) == pytest.approx(0.018)
    assert ModelSpec.undeclared().priced is False
