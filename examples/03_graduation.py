"""A pipeline that outgrows itself, and the report that proves it.

Month one: invoices and contracts, both handled by a branch that was declared
up front. Month six: a new kind of document arrives that the fixed sequence has
no arm for. Nothing crashes — the misfit signals just start piling up, and the
recommendation flips on its own.

    python examples/03_graduation.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from boundedrun import Pipeline, Store, branch, graduation, judgment, step
from fakes import DEMO_MODEL, FakeModel, banner

STORE = Path(__file__).parent / "graduation-demo.db"

model = FakeModel(
    replies={
        "INVOICE": {"category": "invoice", "confidence": 0.95},
        "CONTRACT": {"category": "contract", "confidence": 0.88},
        # The amendment nobody planned for. The model knows what it would need;
        # the pipeline has no such step, and says so.
        "parties": ["acme", "globex"],
        "AMENDMENT": {
            "category": "amendment",
            "confidence": 0.62,
            "needs_step": "fetch_previous_contract",
        },
    },
    default={"category": "unknown", "confidence": 0.3},
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
async def extract_totals(ctx, doc: dict) -> dict:
    return {**doc, "total_eur": 1200}


@step
async def validate_totals(ctx, doc: dict) -> dict:
    return {**doc, "validated": True}


@judgment(
    prompt_version="parties@v2",
    max_tokens=600,
    max_input_tokens=4_000,
    retries=1,
)
async def extract_parties(ctx, doc: dict) -> dict:
    """Only the contract arm pays for this one — so the branches do not cost the
    same, and bounds() reports the range as well as the worst arm."""
    parties = await ctx.model(prompt=f"Who are the parties? {doc}")
    return {**doc, "parties": parties}


@step
async def unhandled(ctx, doc: dict) -> dict:
    """The default arm: no branch fits, so the flow declines to decide.

    Nothing here reports the missing step — the judgment's own output already
    asked for one, and boundedrun noticed that on its own.
    """
    raise ctx.NeedsReview(f"no arm for category {doc.get('category')!r}")


pipeline = Pipeline(
    name="doc-route",
    steps=[
        extract,
        classify,
        branch(
            "category",
            {
                "invoice": [extract_totals, validate_totals],
                "contract": [extract_parties],
            },
            default=[unhandled],
        ),
    ],
    store=Store(str(STORE)),
    model=model,
    model_spec=DEMO_MODEL,
)


async def feed(docs: list[bytes], prefix: str) -> None:
    for i, blob in enumerate(docs):
        await pipeline.run(blob, idempotency_key=f"{prefix}-{i}")


async def main() -> None:
    STORE.unlink(missing_ok=True)

    banner("The declared worst case, branches and all")
    print(pipeline.bounds())

    banner("Month one: 60 documents the flow was designed for")
    await feed([b"INVOICE %d" % i for i in range(45)], "m1-inv")
    await feed([b"CONTRACT %d" % i for i in range(15)], "m1-con")
    report = await graduation(pipeline.store, pipeline="doc-route", since=None)
    print(report)

    banner("Month six: a new document type nobody declared an arm for")
    await feed([b"AMENDMENT %d" % i for i in range(38)], "m6-amd")
    # Two of the invoices turned out to be receipts; a human fixed them by hand.
    for i in range(2):
        row = await pipeline.store.find_by_idempotency("doc-route", f"m1-inv-{i}")
        await pipeline.record_correction(row["run_id"], "category invoice -> receipt")

    report = await graduation(pipeline.store, pipeline="doc-route", since=None)
    print(report)

    banner("The evaluation set is already collected")
    print(f"  {len(report.evaluation_set)} runs with a recorded misfit, ready to replay")
    print(f"  boundedrun graduation --store {STORE.name} --threshold 0.15")
    await pipeline.aclose()


if __name__ == "__main__":
    asyncio.run(main())
