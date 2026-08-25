"""A deterministic stand-in for a real model, so every example runs offline.

It reports its own token counts, which is what a real wrapper should do too:
estimated tokens make an estimated bill.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field

from boundedrun import ModelResult, ModelSpec

# Prices are made up, and declared here rather than shipped inside the library:
# a price the library cannot verify is a number nobody should quote.
DEMO_MODEL = ModelSpec(
    name="demo-small",
    usd_per_1k_input=0.003,
    usd_per_1k_output=0.015,
    latency_ms=800,
)


@dataclass
class FakeModel:
    """Routes on a substring of the prompt; counts calls; can be made to fail."""

    replies: dict[str, object]
    default: object = None
    tokens_in: int = 2_500
    tokens_out: int = 400
    latency_s: float = 0.0
    fail_times: int = 0
    calls: list[str] = field(default_factory=list)

    async def __call__(self, prompt: str | None = None, **_: object) -> ModelResult:
        self.calls.append(prompt or "")
        if self.latency_s:
            await asyncio.sleep(self.latency_s)
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("model unavailable")
        payload = self.default
        for needle, reply in self.replies.items():
            if needle in (prompt or ""):
                payload = reply
                break
        return ModelResult(
            output=payload,
            tokens_in=self.tokens_in,
            tokens_out=self.tokens_out,
            model="demo-small",
        )


def banner(title: str) -> None:
    print(f"\n\033[1m{title}\033[0m\n" + "─" * len(title))


__all__ = ["DEMO_MODEL", "Callable", "FakeModel", "banner"]
