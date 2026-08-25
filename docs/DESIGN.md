# Design notes

Why this library is shaped the way it is, and what I deliberately did not build.

---

## 1. The ladder

From [*Before You Build an Agent, Try a Cron
Job*](https://t2software.ai/writing/before-you-build-an-agent.html). One
question decides everything:

> *does the sequence of steps need to vary per input?*

If the answer is no, an agent is not required. The ladder, by rising complexity:

1. **Ordinary code.** You control the flow as you write it.
2. **One model call in a fixed pipeline.** The model fills in values.
3. **A fixed multi-step flow with several model steps.** More points of judgment.
4. **An agent.** The model controls the flow at runtime, and the worst case is
   unbounded.

The boundary is *who decides what happens next*. Rungs 1–3: you, in readable
code. Rung 4: a probabilistic decision taken at runtime.

Staying on the lower rungs buys four things the article names, and this library
turns each into something you can point at:

| the article says | where it lives here |
|---|---|
| *a bounded worst case* | `pipeline.bounds()`, `enforce_bounds=True` |
| failures traceable to a line of code | the per-step audit trail in `store.py` |
| one evaluation surface per model call | `@judgment` with `prompt_version` + `output_schema` |
| *latency you can quote* | the latency line of `bounds()`, static or measured |

And the reason to care, also from the article: ambitious architectures that
demo beautifully and then *get quietly switched off eight months later*.

> *Agentic is a control-flow decision, not a product decision.*

**Scope of this repository:** rungs 2 and 3, plus the instrumentation that tells
you when you have outgrown them. Rung 4 is `fivegates`; the graduation report
(§9 of the spec, `graduation.py`) is the bridge between the two.

## 2. The one rule

The sequence is fixed and declared. Branching may depend on deterministic
conditions; it may never depend on a free choice by the model.

This is enforced structurally, not by convention: `Branch.select()` reads a key
from a value already in hand, and there is no API through which a model's reply
can name the next node. The moment you need that, you are on rung 4 and this is
the wrong library — which is a supported outcome, not a failure.

## 3. Decisions, and what they cost

### Bounds are declaration-time, so steps must carry their own ceilings

`bounds.py` was written second, before the executor, precisely because it
dictates what metadata a step must declare. A `@judgment` carries
`max_tokens`, `max_input_tokens`, its retry budget and (optionally) its own
model. Multiply by the attempt count, sum along the sequence, and the worst case
falls out.

The cost: a judgment that declares no ceiling cannot be bounded. Rather than
inventing a number, `bounds()` prints `(undeclared)`, and `enforce_bounds=True`
refuses to construct the pipeline at all. **You cannot enforce what you never
declared.**

### Prices ship with your code, not with this library

`ModelSpec` is yours to fill in. Hard-coding vendor prices would produce numbers
that go stale silently, and the whole selling point here is a quote you can
trust. An unpriced pipeline prints `cost (no pricing declared)` rather than
`$0.00`.

### Branches report the worst arm and the range

Arms can differ wildly. `bounds()` takes the most expensive arm as *the* worst
case — the only safe reading — and also prints the range, so a reviewer can see
how lopsided the flow is. Ties are broken by cost, then latency, then tokens.

### Latency: static by default, measured once there is history

A static per-model estimate is available immediately and is approximate. A
measured one is credible but needs history. So: static by default, automatically
replaced by the mean of recorded runs once there are at least 30 (configurable
via `measured_after`), and **the output always says which one it used**.

`bounds()` stays synchronous, which means it reads that history with a blocking
SQLite query. That is deliberate: it is a declaration-time and CI-time call, not
something on the hot path of a run.

### Concurrency is opt-in

`parallel(...)` runs independent steps together via `asyncio.TaskGroup`. It is
explicit because it is the one construct that changes what a quoted latency
means: the bound becomes the slowest member instead of the sum. Silent
parallelism would make the headline number a lie.

### Tokens: reported if you report them, estimated if you do not

Return a `ModelResult` from your model callable and the accounting is exact.
Return a bare `str` or `dict` and tokens are estimated at ~4 characters per
token — a documented heuristic, not a measurement. Estimated tokens make an
estimated bill; wrap your client properly for a real one.

### SQLite, one file, WAL

Stdlib `sqlite3` through `asyncio.to_thread`. Every step commits as it
completes, which is what makes resume-after-`kill -9` work — the test kills a
real process with `SIGKILL` and continues the run from another one.

Values are stored as JSON. Anything that will not serialise (raw PDF bytes, an
open handle) is recorded as an opaque marker rather than silently dropped, and a
resume that lands on such a step re-executes it instead of pretending it has the
value. Resume tells you plainly when it cannot proceed without the original
input.

### Idempotency is a database constraint, not a lock

A partial unique index on `(pipeline, idempotency_key)` decides the winner. Two
concurrent callers with one key: the first INSERT wins and executes, the loser
polls until the run reaches a terminal status and returns that result. If the
holder never finishes — its process died — the loser raises `RunInProgress`
naming `resume()`, rather than hanging or silently duplicating the work.

I chose an explicit `resume()` over automatic takeover deliberately. Deciding
that another process is dead is a distributed-systems problem, and guessing
wrong means running someone else's work twice. A supervisor or an operator
should make that call; `boundedrun resume <run_id>` is the command.

### A misfit signal is a fact about a run, not an event count

The same observation about the same run is recorded once, however many code
paths notice it. Otherwise a judgment whose reply asks for a missing step *and*
a default arm that reports the same thing would double the number, and the
graduation percentage — the number the whole recommendation rests on — would
drift away from "share of runs that went wrong".

### Replay compares judgment outputs only

A run's outcome, for replay purposes, is the tuple of its judgment outputs.
Those are the only places a different prompt or model can change anything.

The alternative — re-running the whole flow with recorded values spliced in —
would have to invent downstream state once a judgment's answer changes, and
inventing state is exactly what an honest replay must not do. Where a changed
answer would have taken a different branch, replay says so as a separate count
instead of guessing what that branch would have produced.

### Step names are unique

Names are the primary key of the audit trail, of resume matching and of the
`missing_step` signal. Duplicates are rejected at construction time with a
message that says why.

## 4. Departures from the specification

- **`errors.py`** is not in the file list of §10. Exceptions are used by every
  module; putting them in any one of them would have created either an import
  cycle or a misfiling. Thirteen lines of code, no dependencies.
- **Core size.** §0 targets roughly 900 lines of core. The engine —
  `pipeline.py`, `steps.py`, `bounds.py`, `errors.py` — is about 950 lines of
  code excluding blanks, comments and docstrings; with `store.py` it is about
  1,280, and the whole package including CLI, replay and graduation is under
  1,900. Every module carries the M0–M3 scope in full. That is still an order of
  magnitude below any agent framework, which is the comparison that matters, but
  it is above the number in the spec and I would rather say so than trim
  docstrings to hit a figure.
- **`@judgment` gained `max_input_tokens`.** §6 declares only `max_tokens`.
  Without an input ceiling there is no token or cost bound at all — only an
  output-token count — so the headline feature would have been unquotable.

## 5. Deliberately not built

- Model-driven branching (rung 4, out of scope by definition).
- Distributed execution, scheduling, backfills — that is Temporal's job.
- Quality scoring. Replay compares versions; it does not judge them.
- Prompt management, vendor clients, retries at the HTTP layer.
- Streaming judgment output. It would make token accounting partial, and a
  partial bound is not a bound.

## 6. Testing strategy

See `tests/`. The load-bearing file is `test_bounds_vs_actual.py`: it runs the
genuine worst case — every retry exhausted — and asserts that the real cost,
token count and call count never exceed the published bound, and separately that
the bound is *attainable* rather than merely safe. If that pair of tests ever
fails, the thesis of this repository is false.

Everything runs offline against a scripted fake model. There is no API key in
this repository, and no test reaches the network.
