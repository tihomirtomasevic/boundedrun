# Test plan

The claim this repository makes is narrow and checkable: *the worst case printed
before a run is never smaller than the bill after it*. Everything else is
supporting evidence. The suite is built around that.

## Principles

1. **Offline by construction.** No API key exists in this repository, no test
   touches the network. Judgment steps are driven by a scripted fake model whose
   replies, token counts and failures are set per test (`tests/conftest.py`).
   Anyone can clone this and run the whole suite in three seconds.
2. **Test the property, not the implementation.** The bound is checked against a
   real execution, not against a second copy of the same arithmetic.
3. **The hard cases are exercised for real.** Process death is a real `SIGKILL`
   of a real subprocess. Concurrent idempotency is two genuinely concurrent
   coroutines racing for one key, not a mocked lock.
4. **Every test names a behaviour.** Test names read as sentences, so a failure
   report says what broke rather than which function did.

## Layers

| file | tests | what it pins down |
|---|---|---|
| `test_bounds_vs_actual.py` | 8 | **load-bearing** — published bound vs. a real worst-case run |
| `test_bounds.py` | 14 | static computation: retries, branches, parallelism, measured latency |
| `test_steps.py` | 14 | declaration-time contracts: async-only, schema validity, unique names |
| `test_pipeline.py` | 16 | execution: order, context, retries, `NeedsReview`, `Skip`, audit trail |
| `test_store.py` | 12 | schema, WAL, idempotency index, signal dedup, JSON limits |
| `test_idempotency.py` | 5 | one key, one execution — including under concurrency |
| `test_crash_resume.py` | 6 | `SIGKILL` mid-run, then continue in another process |
| `test_resume.py` | 9 | every other state a run can be resumed from, and the two refusals |
| `test_branching.py` | 7 | deterministic arms, recorded branch choice, unroutable input |
| `test_parallel.py` | 4 | declared concurrency, merge, per-member audit rows |
| `test_replay.py` | 11 | outcome diffs, no model calls for deterministic steps, originals untouched |
| `test_graduation.py` | 9 | all five signals, threshold, the recommendation flipping |
| `test_cli.py` | 21 | every command, plus the non-zero exit CI depends on |
| **total** | **136** | 95% statement coverage |

## The load-bearing test

`test_bounds_vs_actual.py` runs a pipeline whose first judgment fails on every
attempt, so all three attempts are spent, and then asserts:

- actual model calls, tokens and cost never exceed the published worst case;
- the published call count is **attainable**, not merely safe — a bound nobody
  can reach is a useless bound;
- `enforce_bounds=True` ends the run in `bounds_exceeded` when a model returns
  more tokens than declared;
- without enforcement, the same run merely costs more — proving the flag is what
  does the work;
- computing bounds executes nothing and records nothing.

If these fail, the thesis of the project is false, not just the code.

## How to run

```console
make install      # editable install with dev extras
make test         # pytest -q, ~3 seconds
make lint         # ruff check + ruff format --check
make examples     # the three examples, end to end
make bounds       # the CI budget guard, locally
```

Coverage:

```console
pytest -q --cov=boundedrun --cov-report=term-missing
```

CI (`.github/workflows/ci.yml`) runs lint, format check and the suite on Python
3.11, 3.12 and 3.13, and separately runs `boundedrun bounds` with a budget — so
the worst case of the example pipeline is part of the build rather than a README
claim nobody rechecks.

## What is deliberately not tested, and the residual risk

- **Real model providers.** Out of scope: the model call is a function you pass
  in. The risk this leaves is in *your* wrapper's token reporting, which is why
  `ModelResult` exists and why estimated tokens are labelled as estimates.
- **Multi-process idempotency.** Tested across coroutines against one SQLite
  file. Two separate processes racing for the same key rely on the same partial
  unique index, but the polling loser path is not exercised across a process
  boundary. Low risk, non-zero.
- **Long-running or high-volume behaviour.** No load test. SQLite with WAL and a
  single connection is the documented design, not a scaling claim.
- **Clock skew and timezone edges** in `--since` windows. Timestamps are UTC ISO
  strings throughout; comparisons are lexical, which is correct for that format.

## What I would add next

1. **Property-based tests over `bounds()`** (Hypothesis): generate arbitrary
   declarations and assert the invariant `worst >= no_retry` and
   `worst >= any executed run` across shapes I did not think of.
2. **A two-process idempotency test**, closing the gap named above.
3. **A soak test** for resume: kill a long pipeline at every step index in turn
   and assert the completed prefix is never re-executed.
4. **A contract test for model wrappers**, shipped as a helper, so users can
   check their own adapter reports tokens the way the bound assumes.
