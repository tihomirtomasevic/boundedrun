"""Process death, for real.

The claim is that a run survives its process. The only honest way to test that
is to kill one with SIGKILL — no cleanup, no unwinding — and then continue it
from another process.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from boundedrun import Store

SCRIPT = Path(__file__).parent / "crash_pipeline.py"

pytestmark = pytest.mark.anyio


def run_script(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        text=True,
        timeout=60,
    )


@pytest.fixture
def crashed(tmp_path: Path):
    db, marker = tmp_path / "runs.db", tmp_path / "marker.txt"
    killed = run_script(str(db), str(marker), "crash")
    assert killed.returncode == -9, killed.stderr
    return db, marker


def test_the_process_really_died_mid_run(crashed):
    _db, marker = crashed
    assert marker.read_text().split() == ["first"]


def test_the_completed_step_is_on_disk_despite_the_kill(crashed):
    db, _marker = crashed
    store = Store(str(db))
    run = store.list_runs()[0]
    steps = store.steps_of(run["run_id"])

    assert run["status"] == "running"
    assert steps[0]["step_name"] == "first" and steps[0]["status"] == "ok"
    assert steps[1]["step_name"] == "second" and steps[1]["status"] == "running"
    store.close()


def test_resume_continues_from_the_last_successful_step(crashed):
    db, marker = crashed
    store = Store(str(db))
    run_id = store.list_runs()[0]["run_id"]
    store.close()

    resumed = run_script(str(db), str(marker), "resume", run_id)

    assert resumed.returncode == 0, resumed.stderr
    assert "done" in resumed.stdout
    assert "'stage': 3" in resumed.stdout


def test_the_finished_steps_are_not_executed_a_second_time(crashed):
    db, marker = crashed
    store = Store(str(db))
    run_id = store.list_runs()[0]["run_id"]
    store.close()

    run_script(str(db), str(marker), "resume", run_id)

    assert marker.read_text().split() == ["first", "second", "third"]


def test_the_interrupted_attempt_is_recorded_as_interrupted(crashed):
    db, marker = crashed
    store = Store(str(db))
    run_id = store.list_runs()[0]["run_id"]
    store.close()

    run_script(str(db), str(marker), "resume", run_id)

    store = Store(str(db))
    steps = store.steps_of(run_id)
    interrupted = [s for s in steps if s["error"] == "process interrupted"]
    assert len(interrupted) == 1
    assert interrupted[0]["step_name"] == "second"
    assert store.get_run(run_id)["status"] == "done"
    store.close()


def test_resuming_a_finished_run_returns_its_result_without_redoing_it(crashed):
    db, marker = crashed
    store = Store(str(db))
    run_id = store.list_runs()[0]["run_id"]
    store.close()

    run_script(str(db), str(marker), "resume", run_id)
    again = run_script(str(db), str(marker), "resume", run_id)

    assert again.returncode == 0, again.stderr
    assert marker.read_text().split() == ["first", "second", "third"]
