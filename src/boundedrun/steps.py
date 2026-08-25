"""Step declarations: ``@step``, ``@judgment``, and the two deterministic
control-flow nodes a pipeline may contain.

Everything ``bounds()`` needs is declared here, at decoration time. That is the
whole trick: the worst case is a property of the declaration, not of the run.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import jsonschema

DETERMINISTIC = "deterministic"
JUDGMENT = "judgment"


@dataclass(frozen=True)
class ModelSpec:
    """What a model call costs and how long it takes.

    Prices are yours to declare — this library ships none, because a price it
    cannot verify is a number nobody should quote.
    """

    name: str = "unpriced"
    usd_per_1k_input: float = 0.0
    usd_per_1k_output: float = 0.0
    latency_ms: int = 0
    priced: bool = True

    @classmethod
    def undeclared(cls) -> ModelSpec:
        return cls(name="undeclared", priced=False)

    def cost(self, tokens_in: int, tokens_out: int) -> float:
        return (tokens_in / 1000) * self.usd_per_1k_input + (
            tokens_out / 1000
        ) * self.usd_per_1k_output


@dataclass(frozen=True)
class RetryPolicy:
    """Declared per step, never globally — a flaky OCR call and a flaky model
    call do not deserve the same policy."""

    attempts: int = 1
    backoff_s: float = 0.0
    multiplier: float = 2.0
    max_backoff_s: float = 30.0

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("attempts must be >= 1")

    def delay_before(self, attempt: int) -> float:
        """Seconds to wait before ``attempt`` (1-based; nothing before the first)."""
        if attempt <= 1 or self.backoff_s <= 0:
            return 0.0
        return min(self.backoff_s * (self.multiplier ** (attempt - 2)), self.max_backoff_s)

    @property
    def worst_case_backoff_s(self) -> float:
        return sum(self.delay_before(a) for a in range(1, self.attempts + 1))


@dataclass(frozen=True)
class Step:
    """A single node in the sequence. Callable, so it can be unit-tested alone."""

    name: str
    kind: str
    fn: Callable[..., Any]
    policy: RetryPolicy = field(default_factory=RetryPolicy)
    prompt_version: str | None = None
    output_schema: Mapping[str, Any] | None = None
    max_tokens: int | None = None
    max_input_tokens: int | None = None
    model: str | None = None
    latency_ms: int | None = None
    doc: str | None = None

    def __call__(self, ctx: Any, value: Any) -> Any:
        return self.fn(ctx, value)

    @property
    def attempts(self) -> int:
        return self.policy.attempts

    @property
    def is_judgment(self) -> bool:
        return self.kind == JUDGMENT

    def validate_output(self, value: Any) -> None:
        if self.output_schema is None:
            return
        try:
            jsonschema.validate(value, dict(self.output_schema))
        except jsonschema.ValidationError as exc:
            from .errors import SchemaViolation

            raise SchemaViolation(f"{self.name}: {exc.message}") from exc


@dataclass(frozen=True)
class Branch:
    """Deterministic branching: the arm is chosen by a value already in hand.

    ``on`` names a key of the incoming mapping (or an attribute of the incoming
    object). It is never a free choice made by a model — that is rung 4, and it
    is out of scope by design.
    """

    on: str
    arms: Mapping[str, Sequence[Any]]
    default: Sequence[Any] | None = None
    name: str = "branch"

    def select(self, value: Any) -> tuple[str, Sequence[Any]]:
        key = self.key_of(value)
        if key in self.arms:
            return str(key), list(self.arms[key])
        if self.default is not None:
            return "default", list(self.default)
        raise KeyError(
            f"branch on {self.on!r}: no arm for {key!r} and no default "
            f"(declared arms: {sorted(self.arms)})"
        )

    def key_of(self, value: Any) -> Any:
        if isinstance(value, Mapping):
            return value.get(self.on)
        return getattr(value, self.on, None)


@dataclass(frozen=True)
class Parallel:
    """Independent steps run concurrently, by explicit declaration only.

    Concurrency is opt-in because it is the one thing that makes quoted latency
    stop meaning what it says: the bound becomes the slowest member, not the sum.
    """

    nodes: tuple[Step, ...]
    merge: Callable[[dict[str, Any]], Any] | None = None
    name: str = "parallel"

    def combine(self, outputs: dict[str, Any]) -> Any:
        return self.merge(outputs) if self.merge else outputs


Node = Step | Branch | Parallel


# --------------------------------------------------------------------- decorators


def _require_async(fn: Callable[..., Any]) -> None:
    if not inspect.iscoroutinefunction(fn):
        raise TypeError(
            f"{getattr(fn, '__name__', fn)!r} must be async — boundedrun is async-first "
            "because these steps are I/O bound"
        )


def step(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    retries: int = 0,
    backoff_s: float = 0.0,
    latency_ms: int | None = None,
) -> Any:
    """Declare a deterministic step. Usable bare (``@step``) or called."""

    def wrap(func: Callable[..., Any]) -> Step:
        _require_async(func)
        return Step(
            name=name or func.__name__,
            kind=DETERMINISTIC,
            fn=func,
            policy=RetryPolicy(attempts=retries + 1, backoff_s=backoff_s),
            latency_ms=latency_ms,
            doc=inspect.getdoc(func),
        )

    return wrap(fn) if fn is not None else wrap


def judgment(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    prompt_version: str | None = None,
    output_schema: Mapping[str, Any] | None = None,
    max_tokens: int | None = None,
    max_input_tokens: int | None = None,
    retries: int = 0,
    backoff_s: float = 0.0,
    model: str | None = None,
    latency_ms: int | None = None,
) -> Any:
    """Declare the one place where a model decides anything.

    ``max_tokens`` and ``max_input_tokens`` are the ceiling per call; multiplied
    by the attempt count they are the whole worst case of this step.
    """

    def wrap(func: Callable[..., Any]) -> Step:
        _require_async(func)
        if output_schema is not None:
            jsonschema.Draft202012Validator.check_schema(dict(output_schema))
        return Step(
            name=name or func.__name__,
            kind=JUDGMENT,
            fn=func,
            policy=RetryPolicy(attempts=retries + 1, backoff_s=backoff_s),
            prompt_version=prompt_version,
            output_schema=output_schema,
            max_tokens=max_tokens,
            max_input_tokens=max_input_tokens,
            model=model,
            latency_ms=latency_ms,
            doc=inspect.getdoc(func),
        )

    return wrap(fn) if fn is not None else wrap


def branch(
    on: str,
    arms: Mapping[str, Sequence[Node]] | None = None,
    *,
    default: Sequence[Node] | None = None,
    name: str | None = None,
) -> Branch:
    """``branch("category", {"invoice": [...], "contract": [...]})``"""
    return Branch(on=on, arms=dict(arms or {}), default=default, name=name or f"branch:{on}")


def parallel(
    *nodes: Step,
    merge: Callable[[dict[str, Any]], Any] | None = None,
    name: str | None = None,
) -> Parallel:
    """``parallel(fetch_a, fetch_b, merge=lambda out: {**out["a"], **out["b"]})``"""
    if not nodes:
        raise ValueError("parallel() needs at least one step")
    for node in nodes:
        if not isinstance(node, Step):
            raise TypeError("parallel() takes plain steps only, not branches or nested groups")
    return Parallel(nodes=tuple(nodes), merge=merge, name=name or "parallel")


def walk(nodes: Sequence[Node]) -> list[Step]:
    """Every step reachable in a declaration, branches included."""
    found: list[Step] = []
    for node in nodes:
        if isinstance(node, Step):
            found.append(node)
        elif isinstance(node, Parallel):
            found.extend(node.nodes)
        elif isinstance(node, Branch):
            for arm in node.arms.values():
                found.extend(walk(list(arm)))
            if node.default:
                found.extend(walk(list(node.default)))
        else:
            raise TypeError(f"not a pipeline node: {node!r}")
    return found
