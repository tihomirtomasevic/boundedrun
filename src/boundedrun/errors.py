"""Every way a run can end other than plainly succeeding."""

from __future__ import annotations


class BoundedrunError(Exception):
    """Base class for everything this library raises."""


class NeedsReview(BoundedrunError):
    """A deterministic exit: the pipeline declines to decide.

    Raised from a step (``raise ctx.NeedsReview(...)``). It is a first-class
    outcome, not a failure: the run ends in ``needs_review`` with its context
    intact, and a misfit signal is recorded.
    """


class Skip(BoundedrunError):
    """Raised by a step that finds it has nothing to do.

    The incoming value passes through untouched, the step is recorded as
    ``skipped``, and a ``step_skipped`` misfit signal is recorded: a fixed
    sequence doing unnecessary work is evidence about the shape of the flow.
    """


class SchemaViolation(BoundedrunError):
    """A judgment step returned something its declared schema rejects."""


class StepFailed(BoundedrunError):
    """A step exhausted its retries."""

    def __init__(self, step_name: str, cause: BaseException) -> None:
        super().__init__(f"step {step_name!r} failed: {cause!r}")
        self.step_name = step_name
        self.cause = cause


class BoundsExceeded(BoundedrunError):
    """A run passed the worst case its declaration published."""


class BoundsUndeclared(BoundedrunError):
    """A judgment step did not declare what its worst case costs."""


class RunInProgress(BoundedrunError):
    """Another run holds this idempotency key and has not finished yet."""
