"""Shared fixtures. Nothing here reaches the network; there is no API key anywhere."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from boundedrun import ModelResult, ModelSpec, Pipeline, Store

TEST_MODEL = ModelSpec(
    name="test-model",
    usd_per_1k_input=0.001,
    usd_per_1k_output=0.002,
    latency_ms=100,
)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@dataclass
class FakeModel:
    """A model whose every answer, token count and failure is scripted."""

    reply: object = None
    replies: dict[str, object] = field(default_factory=dict)
    tokens_in: int = 1_000
    tokens_out: int = 100
    fail_times: int = 0
    delay_s: float = 0.0
    calls: list[str] = field(default_factory=list)

    async def __call__(self, prompt: str | None = None, **_: object) -> ModelResult:
        self.calls.append(prompt or "")
        if self.delay_s:
            await asyncio.sleep(self.delay_s)
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("model unavailable")
        payload = self.reply
        for needle, value in self.replies.items():
            if needle in (prompt or ""):
                payload = value
                break
        return ModelResult(
            output=payload,
            tokens_in=self.tokens_in,
            tokens_out=self.tokens_out,
            model="test-model",
        )

    @property
    def call_count(self) -> int:
        return len(self.calls)


@pytest.fixture
def model() -> FakeModel:
    return FakeModel(reply={"category": "invoice", "confidence": 0.9})


@pytest.fixture
def store(tmp_path: Path) -> Store:
    db = Store(str(tmp_path / "runs.db"))
    db.connect()
    yield db
    db.close()


@pytest.fixture
def build(store: Store, model: FakeModel) -> Callable[..., Pipeline]:
    """A pipeline factory wired to the temp store and the fake model."""

    def make(*steps, name: str = "test", **kwargs) -> Pipeline:
        kwargs.setdefault("model", model)
        kwargs.setdefault("model_spec", TEST_MODEL)
        return Pipeline(name=name, steps=list(steps), store=store, **kwargs)

    return make
