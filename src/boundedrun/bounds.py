"""The worst case, computed from the declaration alone.

An agent cannot produce this file's output: its step count is not known until
runtime. A fixed sequence's is known at import time, so the number of model
calls, the tokens, the cost and the latency can all be printed before anything
runs — and enforced while it does.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import Any

from .errors import BoundsExceeded
from .steps import Branch, ModelSpec, Node, Parallel, Step, walk


@dataclass(frozen=True)
class Usage:
    """A quantity of work. Adds along a sequence; maxes across a branch."""

    calls: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    cost_usd: float = 0.0
    latency_ms: float = 0.0

    def __add__(self, other: Usage) -> Usage:
        return Usage(
            calls=self.calls + other.calls,
            tokens_in=self.tokens_in + other.tokens_in,
            tokens_out=self.tokens_out + other.tokens_out,
            cost_usd=self.cost_usd + other.cost_usd,
            latency_ms=self.latency_ms + other.latency_ms,
        )

    def merged_parallel(self, other: Usage) -> Usage:
        """Concurrent work: everything sums except elapsed time, which maxes."""
        total = self + other
        return replace(total, latency_ms=max(self.latency_ms, other.latency_ms))

    @property
    def weight(self) -> tuple[float, float, int]:
        """Ordering key for 'which branch is worst'."""
        return (self.cost_usd, self.latency_ms, self.tokens_in + self.tokens_out)


@dataclass(frozen=True)
class Path:
    """A usage total plus the branch choices that produced it."""

    usage: Usage = field(default_factory=Usage)
    labels: tuple[str, ...] = ()

    def then(self, other: Path) -> Path:
        return Path(self.usage + other.usage, self.labels + other.labels)


@dataclass
class Estimator:
    """Resolves per-step price and latency, static or measured (§13)."""

    default_spec: ModelSpec
    specs: dict[str, ModelSpec] = field(default_factory=dict)
    measured_ms: dict[str, float] = field(default_factory=dict)
    measured_runs: int = 0
    measured_threshold: int = 30

    @property
    def uses_measured(self) -> bool:
        return self.measured_runs >= self.measured_threshold and bool(self.measured_ms)

    def spec_for(self, step: Step) -> ModelSpec:
        if step.model and step.model in self.specs:
            return self.specs[step.model]
        if step.model:
            return ModelSpec(name=step.model, priced=False)
        return self.default_spec

    def latency_ms(self, step: Step) -> float:
        if self.uses_measured and step.name in self.measured_ms:
            return self.measured_ms[step.name]
        if step.latency_ms is not None:
            return float(step.latency_ms)
        if step.is_judgment:
            return float(self.spec_for(step).latency_ms)
        return 0.0

    @property
    def latency_source(self) -> str:
        if self.uses_measured:
            return f"measured, from {self.measured_runs} runs"
        return "static, as declared"


@dataclass(frozen=True)
class Bounds:
    """What ``pipeline.bounds()`` returns. Printable, comparable, checkable."""

    pipeline: str
    steps: int
    deterministic: int
    judgments: int
    max_attempts: int
    worst: Usage
    no_retry: Usage
    latency_source: str
    priced: bool = True
    tokens_declared: bool = True
    branch_low: Usage | None = None
    worst_branch: tuple[str, ...] = ()
    cheapest_branch: tuple[str, ...] = ()

    # ------------------------------------------------------------------ checking

    def check(
        self,
        *,
        max_cost: float | None = None,
        max_latency_s: float | None = None,
        max_calls: int | None = None,
        max_tokens: int | None = None,
    ) -> list[str]:
        """Violations of a CI budget, worst case against declared ceilings."""
        bad: list[str] = []
        if max_cost is not None and self.worst.cost_usd > max_cost:
            bad.append(f"cost ${self.worst.cost_usd:.4f} exceeds --max-cost ${max_cost:.4f}")
        if max_latency_s is not None and self.worst.latency_ms / 1000 > max_latency_s:
            bad.append(
                f"latency {self.worst.latency_ms / 1000:.1f}s exceeds "
                f"--max-latency {max_latency_s:.1f}s"
            )
        if max_calls is not None and self.worst.calls > max_calls:
            bad.append(f"{self.worst.calls} model calls exceed --max-calls {max_calls}")
        total_tokens = self.worst.tokens_in + self.worst.tokens_out
        if max_tokens is not None and total_tokens > max_tokens:
            bad.append(f"{total_tokens:,} tokens exceed --max-tokens {max_tokens:,}")
        return bad

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "steps": self.steps,
            "deterministic": self.deterministic,
            "judgments": self.judgments,
            "max_attempts_per_step": self.max_attempts,
            "latency_source": self.latency_source,
            "priced": self.priced,
            "tokens_declared": self.tokens_declared,
            "worst_case": _usage_dict(self.worst),
            "without_retries": _usage_dict(self.no_retry),
            "worst_branch": list(self.worst_branch),
            "cheapest_branch": list(self.cheapest_branch),
            "branch_low": _usage_dict(self.branch_low) if self.branch_low else None,
        }

    # ------------------------------------------------------------------ printing

    def __str__(self) -> str:
        money = f"${self.worst.cost_usd:,.2f}" if self.priced else "(no pricing declared)"
        cheap = f"${self.no_retry.cost_usd:,.2f}" if self.priced else "n/a"
        tokens_in = f"{self.worst.tokens_in:,}" if self.tokens_declared else "(undeclared)"
        tokens_out = f"{self.worst.tokens_out:,}" if self.tokens_declared else "(undeclared)"
        lines = [
            f"boundedrun: {self.steps} steps "
            f"({self.deterministic} deterministic, {self.judgments} judgment)",
            f"  worst case, with retries (max {self.max_attempts} attempts per step):",
            f"    model calls   {self.worst.calls:>10,}",
            f"    input tokens  {tokens_in:>10}",
            f"    output tokens {tokens_out:>10}",
            f"    cost          {money:>10}",
            f"    latency       {'~' + _secs(self.worst.latency_ms):>10}",
            f"  without retries:  {_plural(self.no_retry.calls, 'call')}, {cheap}, "
            f"~{_secs(self.no_retry.latency_ms)}",
        ]
        if self.branch_low is not None:
            low = f"${self.branch_low.cost_usd:,.2f}" if self.priced else "n/a"
            lines.append(
                f"  branch range:  {self.branch_low.calls}-{self.worst.calls} calls, "
                f"{low}-{money} "
                f"(worst branch: {' > '.join(self.worst_branch) or 'none'})"
            )
        lines.append(f"  latency estimate: {self.latency_source}")
        return "\n".join(lines)


def _usage_dict(u: Usage) -> dict[str, Any]:
    return {
        "calls": u.calls,
        "tokens_in": u.tokens_in,
        "tokens_out": u.tokens_out,
        "cost_usd": round(u.cost_usd, 6),
        "latency_ms": round(u.latency_ms, 1),
    }


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def _secs(ms: float) -> str:
    return f"{ms / 1000:.1f} s"


# ----------------------------------------------------------------- computation


def step_usage(step: Step, est: Estimator, *, with_retries: bool) -> Usage:
    attempts = step.attempts if with_retries else 1
    per_call_ms = est.latency_ms(step)
    backoff_ms = step.policy.worst_case_backoff_s * 1000 if with_retries else 0.0
    if not step.is_judgment:
        return Usage(latency_ms=per_call_ms * attempts + backoff_ms)
    spec = est.spec_for(step)
    tokens_in = (step.max_input_tokens or 0) * attempts
    tokens_out = (step.max_tokens or 0) * attempts
    return Usage(
        calls=attempts,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        cost_usd=spec.cost(tokens_in, tokens_out) if spec.priced else 0.0,
        latency_ms=per_call_ms * attempts + backoff_ms,
    )


def _paths(nodes: Sequence[Node], est: Estimator, *, with_retries: bool) -> tuple[Path, Path]:
    """(most expensive path, cheapest path) through a declaration."""
    high, low = Path(), Path()
    for node in nodes:
        if isinstance(node, Step):
            usage = step_usage(node, est, with_retries=with_retries)
            high, low = high.then(Path(usage)), low.then(Path(usage))
        elif isinstance(node, Parallel):
            total = Usage()
            for member in node.nodes:
                total = total.merged_parallel(step_usage(member, est, with_retries=with_retries))
            high, low = high.then(Path(total)), low.then(Path(total))
        elif isinstance(node, Branch):
            arms = dict(node.arms)
            if node.default is not None:
                arms["default"] = node.default
            if not arms:
                continue
            options = {
                key: _paths(list(arm), est, with_retries=with_retries) for key, arm in arms.items()
            }
            worst_key = max(options, key=lambda k: options[k][0].usage.weight)
            best_key = min(options, key=lambda k: options[k][1].usage.weight)
            worst_arm = options[worst_key][0]
            best_arm = options[best_key][1]
            label = f"{node.on}={worst_key}"
            best_label = f"{node.on}={best_key}"
            high = high.then(Path(worst_arm.usage, (label, *worst_arm.labels)))
            low = low.then(Path(best_arm.usage, (best_label, *best_arm.labels)))
        else:
            raise TypeError(f"not a pipeline node: {node!r}")
    return high, low


def compute(nodes: Sequence[Node], est: Estimator, *, name: str = "pipeline") -> Bounds:
    """The whole of §5, from the declaration and nothing else."""
    all_steps = walk(nodes)
    judgments = [s for s in all_steps if s.is_judgment]
    has_branch = any(isinstance(n, Branch) for n in nodes)

    worst_path, cheap_path = _paths(nodes, est, with_retries=True)
    plain_path, _ = _paths(nodes, est, with_retries=False)

    priced = all(est.spec_for(s).priced for s in judgments) and bool(judgments)
    tokens_declared = all(
        s.max_tokens is not None and s.max_input_tokens is not None for s in judgments
    )
    return Bounds(
        pipeline=name,
        steps=len(all_steps),
        deterministic=len(all_steps) - len(judgments),
        judgments=len(judgments),
        max_attempts=max((s.attempts for s in all_steps), default=1),
        worst=worst_path.usage,
        no_retry=plain_path.usage,
        latency_source=est.latency_source,
        priced=priced if judgments else True,
        tokens_declared=tokens_declared,
        branch_low=cheap_path.usage if has_branch else None,
        worst_branch=worst_path.labels,
        cheapest_branch=cheap_path.labels,
    )


def undeclared_judgments(nodes: Sequence[Node]) -> list[str]:
    """Judgment steps that cannot be bounded — needed by ``enforce_bounds=True``."""
    return [
        s.name
        for s in walk(nodes)
        if s.is_judgment and (s.max_tokens is None or s.max_input_tokens is None)
    ]


@dataclass
class Meter:
    """Running total for one run, checked against the published bound.

    ``enforce_bounds=True`` turns the quote into a promise: crossing it raises
    instead of quietly costing more.
    """

    bounds: Bounds
    enforce: bool = False
    calls: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    cost_usd: float = 0.0

    def add_call(self, tokens_in: int, tokens_out: int, spec: ModelSpec) -> None:
        self.calls += 1
        self.tokens_in += tokens_in
        self.tokens_out += tokens_out
        if spec.priced:
            self.cost_usd += spec.cost(tokens_in, tokens_out)
        if self.enforce:
            self._check()

    def _check(self) -> None:
        worst = self.bounds.worst
        if self.calls > worst.calls:
            raise BoundsExceeded(
                f"{self.calls} model calls exceeds the declared worst case of {worst.calls}"
            )
        if self.bounds.tokens_declared:
            if self.tokens_in > worst.tokens_in:
                raise BoundsExceeded(
                    f"{self.tokens_in:,} input tokens exceeds the declared worst case of "
                    f"{worst.tokens_in:,}"
                )
            if self.tokens_out > worst.tokens_out:
                raise BoundsExceeded(
                    f"{self.tokens_out:,} output tokens exceeds the declared worst case of "
                    f"{worst.tokens_out:,}"
                )
        if self.bounds.priced and self.cost_usd > worst.cost_usd + 1e-9:
            raise BoundsExceeded(
                f"${self.cost_usd:.4f} exceeds the declared worst case of ${worst.cost_usd:.4f}"
            )
