"""The graduation report: evidence, not impression, for climbing to rung 4.

The article's advice is to analyse *the specific decisions the rigid flow got
wrong* before reaching for an agent. Nobody has a tool for that, so the decision
gets made on vibes. Every signal counted here was recorded deterministically by
a real run.

The threshold is deliberately high. This tool is biased toward staying simple;
that bias is the point.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from .store import Store, since_ts

LABELS = {
    "missing_step": "requested a step that does not exist",
    "manual_correction": "manual correction",
    "needs_review": "needs_review",
    "branch_wrong": "wrong branch taken",
    "step_skipped": "step skipped as unnecessary",
}

DEFAULT_THRESHOLD = 0.15


@dataclass
class KindTally:
    kind: str
    count: int
    top_detail: str | None = None
    top_detail_count: int = 0

    @property
    def label(self) -> str:
        return LABELS.get(self.kind, self.kind)


@dataclass
class GraduationReport:
    pipeline: str | None
    window: str
    runs: int
    signal_runs: int
    threshold: float
    tallies: list[KindTally] = field(default_factory=list)
    evaluation_set: list[str] = field(default_factory=list)

    @property
    def rate(self) -> float:
        return self.signal_runs / self.runs if self.runs else 0.0

    @property
    def should_climb(self) -> bool:
        return self.runs > 0 and self.rate >= self.threshold

    @property
    def recommendation(self) -> str:
        if not self.runs:
            return "No runs in this window. Nothing to recommend yet."
        pct, thr = f"{self.rate:.1%}", f"{self.threshold:.0%}"
        if self.should_climb:
            return (
                f"{pct} is at or above the {thr} threshold. You have the evidence to climb "
                f"to rung 4 — and an evaluation set of {len(self.evaluation_set)} runs, "
                "already collected, to take with you."
            )
        return f"{pct} is below the {thr} threshold. Stay on rung 3."

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "window": self.window,
            "runs": self.runs,
            "signal_runs": self.signal_runs,
            "rate": round(self.rate, 4),
            "threshold": self.threshold,
            "should_climb": self.should_climb,
            "signals": {t.kind: t.count for t in self.tallies},
            "recommendation": self.recommendation,
            "evaluation_set": self.evaluation_set,
        }

    def __str__(self) -> str:
        head = f"boundedrun graduation{f' — {self.pipeline}' if self.pipeline else ''}"
        lines = [f"{head}  ({self.window})", "", f"{self.runs:,} runs"]
        if not self.runs:
            lines.append("  no runs recorded in this window")
            lines += ["", f"  Recommendation: {self.recommendation}"]
            return "\n".join(lines)
        lines.append(f"  misfit signals in {self.signal_runs} runs ({self.rate:.1%})")
        width = max((len(t.label) for t in self.tallies), default=0)
        for tally in self.tallies:
            detail = ""
            if tally.top_detail and tally.top_detail_count > 1:
                detail = f'   mostly "{tally.top_detail}"'
            lines.append(f"     {tally.label:<{width}} {tally.count:>5}{detail}")
        lines += ["", f"  Recommendation: {self.recommendation}"]
        if not self.should_climb:
            lines.append(
                f"  If it crosses {self.threshold:.0%}, you have your evidence to climb — "
                f"and an evaluation set of {len(self.evaluation_set)} runs to start from."
            )
        return "\n".join(lines)


async def graduation(
    store: Store,
    *,
    pipeline: str | None = None,
    since: timedelta | None = None,
    threshold: float = DEFAULT_THRESHOLD,
) -> GraduationReport:
    """Count misfit signals over a window and say whether they justify an agent."""
    ts = since_ts(since)
    runs = store.count_runs(pipeline=pipeline, since=ts)
    signals = store.signals(pipeline=pipeline, since=ts)

    by_kind: Counter[str] = Counter()
    details: dict[str, Counter[str]] = defaultdict(Counter)
    run_ids: set[str] = set()
    for row in signals:
        by_kind[row["kind"]] += 1
        run_ids.add(row["run_id"])
        if row["detail"]:
            details[row["kind"]][row["detail"]] += 1

    tallies = []
    for kind, count in by_kind.most_common():
        top = details[kind].most_common(1)
        tallies.append(
            KindTally(
                kind=kind,
                count=count,
                top_detail=top[0][0] if top else None,
                top_detail_count=top[0][1] if top else 0,
            )
        )
    return GraduationReport(
        pipeline=pipeline,
        window=_window_label(since),
        runs=runs,
        signal_runs=len(run_ids),
        threshold=threshold,
        tallies=tallies,
        evaluation_set=sorted(run_ids),
    )


def _window_label(since: timedelta | None) -> str:
    if since is None:
        return "all time"
    days = since.days
    return f"last {days}d" if days else f"last {int(since.total_seconds() // 3600)}h"
