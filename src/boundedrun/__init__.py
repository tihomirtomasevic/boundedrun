"""boundedrun — a fixed pipeline with a language model in the middle.

Rungs 2 and 3 of the ladder in *Before You Build an Agent, Try a Cron Job*:
one model call in a fixed pipeline, and a fixed multi-step flow with several
judgment points. The sequence is fixed, so the worst case is knowable before
the run — see ``Pipeline.bounds()``.

Rung 4, where the model decides what happens next, is deliberately out of scope.
"""

from .bounds import Bounds, Estimator, Meter, Usage
from .errors import (
    BoundedrunError,
    BoundsExceeded,
    BoundsUndeclared,
    NeedsReview,
    RunInProgress,
    SchemaViolation,
    Skip,
    StepFailed,
)
from .graduation import GraduationReport, graduation
from .pipeline import Context, ModelResult, Pipeline, RunResult
from .replay import ReplayReport, replay
from .steps import (
    Branch,
    ModelSpec,
    Parallel,
    RetryPolicy,
    Step,
    branch,
    judgment,
    parallel,
    step,
)
from .store import Store

__version__ = "0.1.0"

__all__ = [
    "BoundedrunError",
    "Bounds",
    "BoundsExceeded",
    "BoundsUndeclared",
    "Branch",
    "Context",
    "Estimator",
    "GraduationReport",
    "Meter",
    "ModelResult",
    "ModelSpec",
    "NeedsReview",
    "Parallel",
    "Pipeline",
    "ReplayReport",
    "RetryPolicy",
    "RunInProgress",
    "RunResult",
    "SchemaViolation",
    "Skip",
    "Step",
    "StepFailed",
    "Store",
    "Usage",
    "__version__",
    "branch",
    "graduation",
    "judgment",
    "parallel",
    "replay",
    "step",
]
