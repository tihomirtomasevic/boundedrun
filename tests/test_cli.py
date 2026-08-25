"""The command line, exercised the way CI would use it."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest
from typer.testing import CliRunner

from boundedrun.cli import app, parse_since

runner = CliRunner()

pytestmark = pytest.mark.anyio

TARGET = "tests.cli_fixture:pipeline"


@pytest.fixture
def seeded(tmp_path: Path, monkeypatch) -> Path:
    """A store with three recorded runs, produced by the fixture pipeline."""
    import asyncio

    monkeypatch.setenv("BOUNDEDRUN_TEST_DB", str(tmp_path / "runs.db"))
    from tests import cli_fixture

    pipeline = cli_fixture.fresh(str(tmp_path / "runs.db"))
    asyncio.run(cli_fixture.seed(pipeline))
    return tmp_path / "runs.db"


def test_bounds_prints_the_worst_case_without_running_anything():
    result = runner.invoke(app, ["bounds", TARGET])

    assert result.exit_code == 0
    assert "boundedrun: 3 steps (2 deterministic, 1 judgment)" in result.stdout
    assert "worst case, with retries" in result.stdout


def test_bounds_exits_nonzero_when_it_busts_the_budget():
    result = runner.invoke(app, ["bounds", TARGET, "--max-cost", "0.0001"])

    assert result.exit_code == 1
    assert "bounds exceeded" in result.output


def test_bounds_passes_a_budget_it_fits_in():
    result = runner.invoke(app, ["bounds", TARGET, "--max-cost", "100", "--max-calls", "99"])
    assert result.exit_code == 0


def test_bounds_speaks_json_for_machines():
    result = runner.invoke(app, ["bounds", TARGET, "--json"])
    payload = json.loads(result.stdout)

    assert payload["judgments"] == 1
    assert payload["worst_case"]["calls"] == 3


def test_an_unimportable_target_is_a_usage_error_not_a_traceback():
    result = runner.invoke(app, ["bounds", "no.such.module:thing"])
    assert result.exit_code != 0
    assert "cannot import" in result.output


def test_a_target_that_is_not_a_pipeline_is_rejected():
    result = runner.invoke(app, ["bounds", "tests.cli_fixture:NOT_A_PIPELINE"])
    assert result.exit_code != 0
    assert "not a Pipeline" in result.output


def test_a_target_without_a_colon_is_rejected():
    result = runner.invoke(app, ["bounds", "tests.cli_fixture"])
    assert result.exit_code != 0
    assert "module:attribute" in result.output


def test_runs_lists_what_was_recorded(seeded):
    result = runner.invoke(app, ["runs", "--store", str(seeded)])

    assert result.exit_code == 0
    assert result.stdout.count("cli-fixture") == 3


def test_runs_filters_by_status(seeded):
    result = runner.invoke(app, ["runs", "--store", str(seeded), "--status", "needs_review"])

    assert result.exit_code == 0
    assert result.stdout.count("needs_review") == 1


def test_show_prints_the_audit_trail_of_one_run(seeded):
    from boundedrun import Store

    run_id = Store(str(seeded)).list_runs(status="done", limit=1)[0]["run_id"]
    result = runner.invoke(app, ["show", run_id, "--store", str(seeded)])

    assert result.exit_code == 0
    assert "classify" in result.stdout
    assert "prompt=classify@v7" in result.stdout
    assert "model=test-model" in result.stdout


def test_show_on_an_unknown_run_fails_cleanly(seeded):
    result = runner.invoke(app, ["show", "nope", "--store", str(seeded)])
    assert result.exit_code == 1
    assert "no run" in result.output


def test_graduation_reports_and_recommends(seeded):
    result = runner.invoke(app, ["graduation", "--store", str(seeded), "--threshold", "0.9"])

    assert result.exit_code == 0
    assert "Recommendation:" in result.stdout
    assert "Stay on rung 3" in result.stdout


def test_graduation_speaks_json(seeded):
    result = runner.invoke(app, ["graduation", "--store", str(seeded), "--json"])
    payload = json.loads(result.stdout)

    assert payload["runs"] == 3
    assert "recommendation" in payload


def test_correct_records_a_manual_correction(seeded):
    from boundedrun import Store

    run_id = Store(str(seeded)).list_runs(status="done", limit=1)[0]["run_id"]
    result = runner.invoke(
        app, ["correct", run_id, "--detail", "invoice -> receipt", "--store", str(seeded)]
    )

    assert result.exit_code == 0
    assert "recorded manual_correction" in result.stdout
    assert Store(str(seeded)).signals()[0]["detail"] == "invoice -> receipt"


def test_replay_runs_against_the_recorded_store(seeded):
    result = runner.invoke(app, ["replay", TARGET, "--store", str(seeded)])

    assert result.exit_code == 0
    assert "runs replayed" in result.stdout


def test_a_missing_store_is_a_usage_error(tmp_path):
    result = runner.invoke(app, ["runs", "--store", str(tmp_path / "absent.db")])
    assert result.exit_code != 0
    assert "no store at" in result.output


@pytest.mark.parametrize(
    ("text", "expected"),
    [("30d", timedelta(days=30)), ("12h", timedelta(hours=12)), ("90m", timedelta(minutes=90))],
)
def test_since_windows_parse(text, expected):
    assert parse_since(text) == expected


def test_a_nonsense_window_is_rejected():
    with pytest.raises(Exception, match="30d"):
        parse_since("last tuesday")
