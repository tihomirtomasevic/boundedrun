"""The sixty-second example: a document classifier on rung 3.

Four steps, one of which is a judgment call. Run it with no API key:

    python examples/01_classify.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from boundedrun import Pipeline, judgment, step
from fakes import DEMO_MODEL, FakeModel, banner

CLASSIFY = "Classify this document. Reply with category and confidence.\n\n{text}"

model = FakeModel(
    replies={
        "INVOICE": {"category": "invoice", "confidence": 0.94},
        "CONTRACT": {"category": "contract", "confidence": 0.55},
        "MEMO": {
            "category": "memo",
            "confidence": 0.91,
            # The model is telling you the fixed flow is missing something.
            # boundedrun records that as a misfit signal — see example 03.
            "needs_step": "fetch_previous_contract",
        },
    },
    default={"category": "unknown", "confidence": 0.2},
)


@step
async def extract(ctx, pdf: bytes) -> str:
    """Deterministic: bytes in, text out. No model, no judgment."""
    return pdf.decode("utf-8")


@judgment(
    prompt_version="classify@v7",
    output_schema={
        "type": "object",
        "required": ["category", "confidence"],
        "properties": {
            "category": {"type": "string"},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        },
    },
    max_tokens=1_200,
    max_input_tokens=3_000,
    retries=2,
)
async def classify(ctx, text: str) -> dict:
    """The one place a model decides anything."""
    return await ctx.model(prompt=CLASSIFY.format(text=text))


@step
async def validate(ctx, result: dict) -> dict:
    """Deterministic exit. Low confidence is not a failure — it is a decision
    the pipeline declines to make."""
    if result["confidence"] < 0.7:
        raise ctx.NeedsReview(f"confidence {result['confidence']} below 0.7")
    return result


@step
async def persist(ctx, result: dict) -> dict:
    return {**result, "stored": True}


pipeline = Pipeline(
    name="doc-classify",
    steps=[extract, classify, validate, persist],
    store="./runs.db",
    model=model,
    model_spec=DEMO_MODEL,
)


async def main() -> None:
    banner("What this costs, before running anything")
    print(pipeline.bounds())

    documents = {
        "doc-4471": b"INVOICE 4471 - total 1,200 EUR",
        "doc-4472": b"CONTRACT between two parties",
        "doc-4473": b"MEMO about the previous contract",
    }

    banner("Three runs")
    for key, blob in documents.items():
        result = await pipeline.run(blob, idempotency_key=key)
        print(f"{key}  {result.status:<13} ${result.cost_usd:.4f}  {result.output}")
        if result.needs_review:
            print(f"          reason: {result.error}")

    banner("Idempotency")
    again = await pipeline.run(documents["doc-4471"], idempotency_key="doc-4471")
    print(f"same key, re-run: reused={again.reused}  status={again.status}  {again.output}")
    print(f"model calls made in total: {len(model.calls)} (the re-run made none)")

    banner("Next")
    print("  boundedrun runs --store ./runs.db")
    print("  boundedrun show <run_id> --store ./runs.db")
    print("  boundedrun graduation --store ./runs.db")
    await pipeline.aclose()


if __name__ == "__main__":
    asyncio.run(main())
