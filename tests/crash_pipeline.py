"""A pipeline that kills its own process mid-run. Driven by test_crash_resume.

python tests/crash_pipeline.py <db> <marker> crash
python tests/crash_pipeline.py <db> <marker> resume <run_id>
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys

from boundedrun import Pipeline, Store, step

DB, MARKER, MODE = sys.argv[1], sys.argv[2], sys.argv[3]


def note(line: str) -> None:
    with open(MARKER, "a") as handle:
        handle.write(line + "\n")


@step
async def first(ctx, value):
    note("first")
    return {"stage": 1, "value": value}


@step
async def second(ctx, value):
    if MODE == "crash":
        # Hardest possible interruption: no unwinding, no flush, no atexit.
        os.kill(os.getpid(), signal.SIGKILL)
    note("second")
    return {**value, "stage": 2}


@step
async def third(ctx, value):
    note("third")
    return {**value, "stage": 3}


pipeline = Pipeline(name="crashy", steps=[first, second, third], store=Store(DB))


async def main() -> None:
    if MODE == "crash":
        result = await pipeline.run("payload", idempotency_key="doc-1")
    else:
        result = await pipeline.resume(sys.argv[4])
    print(f"{result.run_id} {result.status} {result.output}")
    await pipeline.aclose()


if __name__ == "__main__":
    asyncio.run(main())
