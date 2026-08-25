# boundedrun

**Most systems that call themselves agents are pipelines with a model in the
middle. This is the tooling for that.**

A fixed sequence of steps, one or two of which are judgment calls handed to a
language model. Because the sequence is fixed, the worst case is knowable
*before* the run: how many model calls, how many tokens, how much money, how
long. An agent cannot tell you any of that, because it does not know its own
step count until it runs.

Companion to [*Before You Build an Agent, Try a Cron
Job*](https://t2software.ai/writing/before-you-build-an-agent.html).

---

## The ladder

| rung | who decides what happens next | what it is | tooling |
|---|---|---|---|
| 1 | you, when you write it | ordinary code | your language |
| 2 | you | **one model call in a fixed pipeline** | **boundedrun** |
| 3 | you | **a fixed multi-step flow with several judgment points** | **boundedrun** |
| 4 | the model, at runtime | an agent | an agent framework — `fivegates` |

The line is *who decides what happens next*. On rungs 1–3 it is you, in code a
reviewer can read. On rung 4 it is a probabilistic decision taken at runtime,
and the worst case stops being a number you can quote.

Most teams jump to rung 4 because rungs 2 and 3 have no tooling. There are a
dozen agent frameworks and almost nothing for *a pipeline with a language model
in the middle*, so the agent is the path of least resistance — and it is
usually the wrong one. This library is that missing tooling.

## The headline feature: a worst case you can quote

```python
print(pipeline.bounds())
```

```
boundedrun: 4 steps (3 deterministic, 1 judgment)
  worst case, with retries (max 3 attempts per step):
    model calls            3
    input tokens       9,000
    output tokens      3,600
    cost               $0.08
    latency           ~2.4 s
  without retries:  1 call, $0.03, ~0.8 s
  latency estimate: static, as declared
```

Nothing ran to produce that. It is computed from the declaration: the steps are
known, the retry budget per step is known, the token ceiling per judgment is
declared, so the arithmetic is available at import time.

Which means it belongs in CI, guarding against a flow that quietly gets more
expensive:

```console
$ boundedrun bounds mypkg.flows:classify --max-cost 0.10
...
bounds exceeded: cost $0.1410 exceeds --max-cost $0.1000
$ echo $?
1
```

Add one summarisation step and the published number moves — `examples/02_bounds.py`
shows exactly that: 3 model calls to 5, `$0.08` to `$0.14`, +74%. **An agent
cannot fail this check**, because its step count is not knowable until it runs.

Bounds can also be enforced at runtime. `Pipeline(enforce_bounds=True)` raises
and ends the run in `bounds_exceeded` rather than quietly costing more than you
told everyone it would.

## Sixty seconds

```python
from boundedrun import Pipeline, judgment, step


@step
async def extract(ctx, pdf: bytes) -> str:
    return await pdf_to_text(pdf)  # deterministic


@judgment(
    prompt_version="classify@v7",
    output_schema={"type": "object", "required": ["category", "confidence"]},
    max_tokens=1_200,
    max_input_tokens=3_000,
    retries=2,
)
async def classify(ctx, text: str) -> dict:
    return await ctx.model(prompt=CLASSIFY.format(text=text))


@step
async def validate(ctx, result: dict) -> dict:
    if result["confidence"] < 0.7:
        raise ctx.NeedsReview("low confidence")  # a deterministic exit
    return result


pipeline = Pipeline(
    name="doc-classify",
    steps=[extract, classify, validate, persist],
    store="./runs.db",
    model=my_async_model,
)

result = await pipeline.run(pdf_bytes, idempotency_key="doc-4471")
```

What you get around that model call is deliberately boring, and therefore
testable:

- **an audit trail** — every step's input, output, duration and outcome; for
  judgment steps also the prompt version, the model, and the token counts
- **idempotency** — the same key returns the first result instead of redoing
  the work, including when two callers arrive at the same moment
- **resume** — `kill -9` in the middle of a run, then continue from the last
  successful step in another process
- **retry budgets per step**, not one global policy
- **`NeedsReview` as a first-class outcome**, not an exception someone swallows
- **schema-checked judgment output** — a reply that does not fit the declared
  schema is retried, then failed

Branching is allowed, as long as it is deterministic and declared up front:

```python
steps = [
    extract,
    classify,
    branch(
        "category",
        {
            "invoice": [extract_totals, validate_totals],
            "contract": [extract_parties],
        },
        default=[needs_human],
    ),
]
```

**Branching on a free choice made by the model is not supported and will not
be.** That is rung 4 by definition.

## When to climb: the graduation report

The article's advice is to build the deterministic version, run it on real
traffic, and analyse *the specific decisions the rigid flow got wrong* before
reaching for an agent. Nobody has a tool for that, so the decision gets made on
impression. This is the tool.

Every run records misfit signals — deterministic evidence that the fixed shape
was the wrong shape:

| signal | what it means |
|---|---|
| `needs_review` | the fixed flow gives up too often |
| `manual_correction` | a human changed the outcome afterwards |
| `branch_wrong` | one arm is always wrong for some class of input |
| `step_skipped` | the fixed sequence does unnecessary work |
| `missing_step` | a judgment asked for a step that does not exist — the strongest signal of all |

```console
$ boundedrun graduation --since 90d

boundedrun graduation — doc-route  (last 90d)

98 runs
  misfit signals in 40 runs (40.8%)
     needs_review                            38   mostly "no arm for category 'amendment'"
     requested a step that does not exist    38   mostly "fetch_previous_contract"
     manual correction                        2   mostly "category invoice -> receipt"

  Recommendation: 40.8% is at or above the 15% threshold. You have the evidence
  to climb to rung 4 — and an evaluation set of 40 runs, already collected, to
  take with you.
```

Below the threshold, it tells you to stay where you are. The threshold is
configurable and deliberately high: this tool is biased toward simplicity, which
is the whole point of the article it comes from.

`examples/03_graduation.py` runs a pipeline that outgrows itself and watches the
recommendation flip on its own.

## Replay

Recorded runs can be re-run against the code as it is now — a new prompt
version, a different model — and the differences printed:

```console
$ boundedrun replay mypkg.flows:classify --since 30d --prompt classify@v8

142 runs replayed against classify@v8
  outcome identical    131  (92%)
  outcome changed       11   (8%)
      invoice -> receipt         7
      contract -> invoice        4
  cost: $0.11 -> $0.09 per run
  568 deterministic steps skipped (not re-executed)
```

This is the evaluation set the article asks for, except nobody assembled it — it
accumulated one real run at a time. Deterministic steps are skipped, because
paying a model to redo arithmetic proves nothing. Replay never writes to the
store, so it cannot disturb the runs it learns from.

## What this is not

- **Not a workflow engine.** Airflow, Prefect, Temporal and Dagster orchestrate
  arbitrary DAGs across a distributed system. This is a small in-process library
  for one linear pipeline with judgment steps in it. If you need scheduling,
  backfills and horizontal scale, use Temporal.
- **Not an agent framework.** Deliberately. Rung 4 is `fivegates`.
- **Not an LLM client.** The model call is a function you pass in. This library
  ships no prompts, no vendor SDKs, and no prices — a price it cannot verify is
  a number nobody should quote.
- **Not an eval framework.** Replay compares prompt versions; it does not score
  quality.
- **No model-driven branching.** Explicitly out of scope, forever.

## Install

Not on PyPI yet — install from source:

```console
git clone https://github.com/tihomirtomasevic/boundedrun
cd boundedrun
pip install -e ".[dev]"
make test          # 136 tests, offline, no API key needed
```

Python 3.11+. Two dependencies: `typer` and `jsonschema`. No pydantic, no
orchestration library, no agent framework — any of those would contradict the
argument.

## Status

v0.1.0. The engine — declaration, execution, bounds — is 1,053 lines; the whole
package including the CLI, replay and the graduation report is 2,037 (non-blank,
non-comment, `src/boundedrun/*.py`). Counted rather than estimated, which felt
like the minimum for a project whose argument is that the number should be
knowable.

The test suite is 136 tests at 95% coverage and runs entirely offline — there is
no API key anywhere in this repository, and crash recovery is tested by actually
killing a process with `SIGKILL`.

```console
python examples/01_classify.py      # a run, offline, with a fake model
python examples/02_bounds.py        # adding a step moves the published number
python examples/03_graduation.py    # a pipeline that outgrows itself
```

Design notes and the reasoning behind each trade-off:
[docs/DESIGN.md](docs/DESIGN.md). The test plan: [docs/TESTING.md](docs/TESTING.md).

## Provenance

Designed from a written specification ([SPEC.md](SPEC.md)), implemented with AI
assistance, and verified against the acceptance criteria in that spec: 136
offline tests, 95% coverage, no network access in the suite.

The specification is the argument; the implementation is execution against it.

## Licence

MIT.
