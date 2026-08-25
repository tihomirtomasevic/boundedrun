"""Adding a step changes the number you publish. That is the whole point.

python examples/02_bounds.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from boundedrun import Pipeline, judgment, step
from fakes import DEMO_MODEL, FakeModel, banner

model = FakeModel(replies={}, default={"category": "invoice", "confidence": 0.9})


@step
async def extract(ctx, pdf: bytes) -> str:
    return pdf.decode()


@judgment(
    prompt_version="classify@v7",
    max_tokens=1_200,
    max_input_tokens=3_000,
    retries=2,
)
async def classify(ctx, text: str) -> dict:
    return await ctx.model(prompt=text)


@step
async def persist(ctx, result: dict) -> dict:
    return result


@judgment(
    prompt_version="summarize@v1",
    max_tokens=800,
    max_input_tokens=6_000,
    retries=1,
)
async def summarize(ctx, result: dict) -> dict:
    """The innocuous-looking addition. It is not innocuous — see below."""
    return await ctx.model(prompt=str(result))


def build(*steps) -> Pipeline:
    return Pipeline(
        name="doc-classify",
        steps=list(steps),
        model=model,
        model_spec=DEMO_MODEL,
    )


before = build(extract, classify, persist)
after = build(extract, classify, summarize, persist)

if __name__ == "__main__":
    banner("Three steps")
    print(before.bounds())

    banner("Someone adds one summarisation step")
    print(after.bounds())

    b, a = before.bounds(), after.bounds()
    banner("What changed")
    print(f"  model calls   {b.worst.calls}  ->  {a.worst.calls}")
    print(
        f"  worst cost    ${b.worst.cost_usd:.2f}  ->  ${a.worst.cost_usd:.2f}"
        f"   (+{(a.worst.cost_usd / b.worst.cost_usd - 1):.0%})"
    )
    print(f"  worst latency {b.worst.latency_ms / 1000:.1f}s  ->  {a.worst.latency_ms / 1000:.1f}s")

    banner("So put it in CI")
    print("  boundedrun bounds examples.02_bounds:after --max-cost 0.10")
    violations = a.check(max_cost=0.10)
    for line in violations:
        print(f"  -> exit 1: bounds exceeded: {line}")
    print("\n  An agent cannot fail this check: its step count is not known until it runs.")
