# boundedrun — specification

The design brief this library was built against, written before any code. The
acceptance criteria in §16 are the ones the test suite checks; where the
implementation departed from this document, [docs/DESIGN.md](docs/DESIGN.md)
records what changed and why.

---

## 1. Name

`boundedrun` — a *bounded worst case* is the concrete benefit of staying on
rungs 2–3 of the ladder, and the headline feature: the worst case is knowable
**before** the run (§4).

## 2. Thesis

Companion essay: [*Before You Build an Agent, Try a Cron
Job*](https://t2software.ai/writing/before-you-build-an-agent.html). Its
argument, compressed:

1. **One question decides it:** does the sequence of steps need to vary per
   input? If not, you do not need an agent.
2. **A ladder of four rungs:** plain code; one model call in a fixed pipeline;
   a fixed workflow with several model steps; an agent.
3. **The line is *who decides what happens next*.** On rungs 1–3 it is you, in
   readable code. On rung 4 it is a probabilistic decision taken at runtime.
4. **What the lower rungs buy you:** a bounded worst case, failures traceable
   to a line of code, one evaluation surface per model call, latency you can
   quote, and code any engineer can maintain.
5. **Climb only with evidence:** build the deterministic version, instrument
   it, run it against real traffic, and analyse the specific decisions the
   rigid flow got wrong.

Most teams jump to rung 4 because rungs 2 and 3 have no tooling. There are a
dozen agent frameworks and almost nothing for *a pipeline with a language model
in the middle* — so the agent is the path of least resistance, even when it is
the wrong answer.

`boundedrun` is tooling for that level, and adds two things an agent cannot
have by definition:

1. **A statically bounded worst case** — model calls, tokens, cost and latency
   known before the run, because the sequence is fixed.
2. **A graduation report** (§8) — measuring how often the rigid flow was wrong,
   turning the decision to climb into evidence rather than impression.

## 3. What this is not

- **Not a workflow engine.** Airflow, Prefect, Temporal and Dagster orchestrate
  arbitrary DAGs across a distributed fleet. `boundedrun` is a small in-process
  library for one linear pipeline with judgment steps in it. If you need
  scheduling, backfills and horizontal scale, use Temporal.
- **Not an agent framework.** Deliberately. Rung 4 is a different project.
- **Not an LLM client.** The model call is a function you supply.
- **Not an eval framework.** Replay (§7) compares prompt versions; it does not
  score quality.
- **No model-driven branching.** The moment the model decides what happens
  next, you are on rung 4 and this is the wrong tool. Out of scope on purpose.

## 4. Principles

**The sequence is fixed and declared.** Branching may depend on deterministic
conditions, never on a free choice by the model. This is the boundary that
defines the project.

**Non-determinism in the smallest possible box.** A judgment step takes typed
input and returns output matching a schema. Everything around it — queueing,
retries, persistence, idempotency, the audit trail — stays conventional and
testable.

**Async-first.** Steps are I/O-bound; the API is async from the first line.

**Every run is reproducible.** Recorded inputs and prompt versions are enough
to repeat a run byte for byte wherever it is deterministic.

## 5. The bounded worst case — headline feature

Because the sequence is known at declaration time, the bounds are computed
statically:

```python
bounds = pipeline.bounds()
print(bounds)
```

```
boundedrun: 6 steps (4 deterministic, 2 judgment)
  worst case, with retries (max 3 per step):
    model calls          6
    input tokens    18,000
    output tokens    3,000
    cost             $0.14
    latency           ~5.2 s
  without retries:  2 calls, $0.05, ~1.7 s
```

This is *latency you can quote*, turned into a command. An agent cannot give
you these numbers, because it does not know its own step count until it runs.

Bounds can also be **enforced**: `Pipeline(enforce_bounds=True)` raises rather
than quietly costing more.

The CLI prints them without running the pipeline, so they fit in CI as a guard
against unnoticed growth:

```console
boundedrun bounds mypkg.flows:classify --max-cost 0.20
```

## 6. Declaring a pipeline

```python
from boundedrun import Pipeline, step, judgment


@step
async def extract(ctx, pdf: bytes) -> str:
    return await pdf_to_text(pdf)  # deterministic


@judgment(
    prompt_version="classify@v7",
    output_schema={"type": "object", "required": ["category", "confidence"]},
    max_tokens=1_200,
    retries=2,
)
async def classify(ctx, text: str) -> dict:
    return await ctx.model(prompt=CLASSIFY.format(text=text))


@step
async def validate(ctx, result: dict) -> dict:
    if result["confidence"] < 0.7:
        raise ctx.NeedsReview("low confidence")  # deterministic exit
    return result


pipeline = Pipeline(
    name="doc-classify",
    steps=[extract, classify, validate, persist],
    store="./runs.db",
    model=my_async_model,
)

result = await pipeline.run(pdf_bytes, idempotency_key="doc-4471")
```

Deterministic branching is allowed, declared up front:

```python
steps=[extract, classify, branch(on="category", {
    "invoice":  [extract_totals, validate_totals],
    "contract": [extract_parties],
})]
```

**Branching on a free choice by the model is not supported and will not be.**
If you need it, you have climbed to rung 4.

## 7. Durability, audit trail, and replay

Every step records its input, output, duration and outcome. A pipeline resumes
from the last successful step after a process failure.

- **Idempotency**: an `idempotency_key` per run; a repeat call with the same
  key returns the first result instead of redoing the work.
- **Retries per step**, with the policy declared on the step rather than
  globally — a flaky OCR call and a flaky model call do not deserve the same
  policy.
- **An audit record for every judgment step**: input, prompt version, model,
  output, token counts. This is the difference between a system allowed to
  touch real money and one that is not.
- **`NeedsReview` is a first-class outcome**, not an exception that gets
  swallowed: the run ends in `needs_review` with its context preserved.

Recorded runs can be replayed against a different prompt version or model, and
the differences reported:

```console
boundedrun replay --since 30d --prompt classify@v8

142 runs replayed
  outcome unchanged   131  (92%)
  outcome changed      11   (8%)
      invoice -> receipt     7
      contract -> invoice    4
  cost: $0.11 -> $0.09 per run
```

This is an evaluation set drawn from real failures that assembled itself.
Replay skips deterministic steps.

## 8. The graduation report

The essay says to climb only after analysing *the specific decisions the rigid
flow got wrong*. Nobody has tooling for that, so the decision gets made on
impression. This supplies it.

The pipeline collects **misfit signals**, each recorded deterministically:

| signal | what it means |
|---|---|
| `needs_review` rate | the rigid flow gives up too often |
| manual correction | a human changed the outcome after the fact |
| repeated branch path | one arm is always wrong for some class of input |
| step skipped as unnecessary | the fixed sequence is doing surplus work |
| **a step was requested that does not exist** | a judgment says "I would need X", and X is not in the pipeline |

```console
boundedrun graduation --since 90d

2,431 runs
  misfit signals in 104 runs (4.3%)
     requested a step that does not exist   61   mostly "fetch_previous_contract"
     manual correction                      29
     needs_review                           14

  Recommendation: 4.3% is below the 15% threshold. Stay on rung 3.
  If it crosses 15%, you have your evidence to climb — and an evaluation set of
  104 runs, already collected, to take with you.
```

The threshold is configurable and **deliberately high**. The tool is biased
toward simplicity; that is the point of the essay.

## 9. Repository layout

```
boundedrun/
  README.md            thesis, the ladder, bounds output, link to the essay
  pyproject.toml
  src/boundedrun/
    pipeline.py        declaration, execution, deterministic branching
    steps.py           @step and @judgment, schemas, retry policies
    bounds.py          static worst-case computation
    store.py           SQLite: runs, steps, audit trail, misfit signals
    replay.py          §7
    graduation.py      §8
    cli.py             bounds / runs / show / replay / graduation
  examples/
    01_classify.py     a pipeline with a fake model, offline
    02_bounds.py       adding a step moves the published worst case
    03_graduation.py   a pipeline that outgrows itself
  tests/
  docs/DESIGN.md       the reasoning behind each trade-off
```

## 10. Milestones

| | scope | result |
|---|---|---|
| **M0** | declaration, execution, store, retries, idempotency | a pipeline runs and survives a crash |
| **M1** | `bounds()` and CLI, `NeedsReview`, examples 01–02, README | **the repository is showable** |
| **M2** | audit trail, replay (§7) | prompt-version comparison works |
| **M3** | graduation report (§8), deterministic branching, example 03 | the differentiator is complete |

`bounds()` lands in M1 rather than later: it is the most convincing part and it
carries the README.

## 11. Open questions

- **Latency estimation in `bounds()`**: static per model, or measured from
  history? Static is available immediately but inaccurate; measured is credible
  but needs prior runs. Proposal: static by default, replaced by measured once
  there are ≥30 runs, and **always state which is in use**.
- **Deterministic branching and `bounds()`**: worst cases can differ sharply
  between arms. Proposal: report the worst arm, plus the range.
- **Parallel steps**: may independent steps run concurrently? Yes, but only in
  M3 and only when marked explicitly — otherwise latency stops being
  predictable in a way you can quote.

## 12. Technical decisions

| decision | choice | why |
|---|---|---|
| language | Python 3.11+ | `TaskGroup` and `asyncio.timeout()` |
| execution model | **async-first**, `asyncio` | steps are I/O-bound |
| storage | **SQLite**, stdlib `sqlite3`, WAL, via `asyncio.to_thread` | one file, no service, survives a crash |
| schema validation | `jsonschema` | small, standard |
| CLI | `typer` | |
| tests | `pytest` + `anyio` | |
| licence | MIT | |
| formatting | `ruff` | |

Runtime dependencies are `typer` and `jsonschema`. **No pydantic**, no
orchestration library, no agent framework — any of those would contradict the
argument.

## 13. Storage schema

```sql
CREATE TABLE runs (
  run_id          TEXT PRIMARY KEY,
  pipeline        TEXT NOT NULL,
  idempotency_key TEXT,
  status          TEXT NOT NULL,   -- running|done|failed|needs_review|bounds_exceeded
  started_at      TEXT NOT NULL,
  ended_at        TEXT,
  cost_usd        REAL DEFAULT 0,
  tokens_in       INTEGER DEFAULT 0,
  tokens_out      INTEGER DEFAULT 0,
  input_hash      TEXT
);
CREATE UNIQUE INDEX ix_runs_idem ON runs(pipeline, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE TABLE run_steps (
  step_run_id  TEXT PRIMARY KEY,
  run_id       TEXT NOT NULL REFERENCES runs(run_id),
  seq          INTEGER NOT NULL,
  step_name    TEXT NOT NULL,
  kind         TEXT NOT NULL,      -- deterministic | judgment
  attempt      INTEGER NOT NULL DEFAULT 1,
  status       TEXT NOT NULL,      -- ok | retried | failed | skipped
  input_json   TEXT,
  output_json  TEXT,
  prompt_version TEXT,             -- judgment only
  model        TEXT,
  tokens_in    INTEGER,
  tokens_out   INTEGER,
  latency_ms   INTEGER,
  error        TEXT,
  started_at   TEXT NOT NULL,
  ended_at     TEXT
);

CREATE TABLE misfit_signals (            -- §8
  signal_id  TEXT PRIMARY KEY,
  run_id     TEXT NOT NULL REFERENCES runs(run_id),
  kind       TEXT NOT NULL,        -- missing_step | manual_correction |
                                   -- needs_review | branch_wrong | step_skipped
  detail     TEXT,
  recorded_at TEXT NOT NULL
);

CREATE INDEX ix_steps_run   ON run_steps(run_id, seq);
CREATE INDEX ix_misfit_kind ON misfit_signals(kind, recorded_at);
```

## 14. Acceptance criteria

A milestone is done when these pass, not when it looks done.

**M0**
- [ ] the pipeline runs its steps in order with a fake model and no API key
- [ ] `kill -9` mid-run → resumes from the last successful step
- [ ] the same `idempotency_key` returns the first result without redoing work
- [ ] retry policy is per step, not global
- [ ] a `@judgment` output that violates its schema retries, then fails

**M1**
- [ ] `pipeline.bounds()` produces the output in §5, computed **without running**
- [ ] `enforce_bounds=True` raises on breach; the run ends `bounds_exceeded`
- [ ] `NeedsReview` ends the run in `needs_review` with context preserved
- [ ] `examples/02_bounds.py` shows an added step moving the published number
- [ ] README written

**M2**
- [ ] the audit record for every judgment step is complete: input, prompt
      version, model, output, token counts
- [ ] `boundedrun replay` produces the output in §7 and **does not call the
      model for deterministic steps**
- [ ] replay does not mutate the original runs

**M3**
- [ ] all five misfit signals are recorded
- [ ] `boundedrun graduation` produces the output in §8 with a recommendation
- [ ] deterministic branching works; `bounds()` reports the worst arm
- [ ] `examples/03_graduation.py` crosses the threshold and the recommendation
      changes

## 15. Test strategy

- **No real API calls.** A fake async model with canned outputs and token
  counts. The whole suite runs offline.
- **`bounds()` is tested against actual execution**: run the pipeline in its
  worst case, with every retry exhausted, and assert the real cost and call
  count **do not exceed** the published bound. This is the most important test
  in the repository — if the bound is not true, the whole thesis fails.
- **Crash recovery is tested for real**, with a subprocess killed by `SIGKILL`.
- **Idempotency under concurrency**: two parallel runs with the same key — one
  executes, the other receives the same result.
