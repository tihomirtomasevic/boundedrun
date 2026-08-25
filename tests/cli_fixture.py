"""A tiny importable pipeline for the CLI tests to point at."""

from __future__ import annotations

from boundedrun import Pipeline, Store, judgment, step

from .conftest import TEST_MODEL, FakeModel

NOT_A_PIPELINE = "a string, not a pipeline"

model = FakeModel(
    replies={
        "INVOICE": {"category": "invoice", "confidence": 0.95},
        "VAGUE": {"category": "unknown", "confidence": 0.4},
    },
    reply={"category": "other", "confidence": 0.9},
)


@step
async def extract(ctx, pdf: bytes) -> str:
    return pdf.decode()


@judgment(
    prompt_version="classify@v7",
    output_schema={"type": "object", "required": ["category", "confidence"]},
    max_tokens=1_200,
    max_input_tokens=3_000,
    retries=2,
)
async def classify(ctx, text: str) -> dict:
    return await ctx.model(prompt=text)


@step
async def validate(ctx, result: dict) -> dict:
    if result["confidence"] < 0.7:
        raise ctx.NeedsReview("low confidence")
    return result


def fresh(db: str) -> Pipeline:
    return Pipeline(
        name="cli-fixture",
        steps=[extract, classify, validate],
        store=Store(db),
        model=model,
        model_spec=TEST_MODEL,
    )


pipeline = fresh(":memory:")


async def seed(target: Pipeline) -> None:
    for i, blob in enumerate([b"INVOICE 1", b"INVOICE 2", b"VAGUE 3"]):
        await target.run(blob, idempotency_key=f"seed-{i}")
    await target.aclose()
