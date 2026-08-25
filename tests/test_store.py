"""Persistence: the schema, the idempotency index, and what survives JSON."""

from __future__ import annotations

import sqlite3

import pytest

from boundedrun.store import (
    Store,
    decode,
    encode,
    input_hash,
    is_opaque,
    new_id,
    since_ts,
)

pytestmark = pytest.mark.anyio


def test_schema_creates_the_three_tables_and_runs_in_wal(store):
    conn = store.connect()
    tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"runs", "run_steps", "misfit_signals"} <= tables
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


def test_the_idempotency_index_admits_exactly_one_claim(store):
    first = store.claim_run(
        run_id=new_id(), pipeline="p", idempotency_key="doc-1", input_hash_=None
    )
    second = store.claim_run(
        run_id=new_id(), pipeline="p", idempotency_key="doc-1", input_hash_=None
    )
    assert (first, second) == (True, False)


def test_runs_without_a_key_are_never_blocked_by_each_other(store):
    assert store.claim_run(run_id=new_id(), pipeline="p", idempotency_key=None, input_hash_=None)
    assert store.claim_run(run_id=new_id(), pipeline="p", idempotency_key=None, input_hash_=None)


def test_the_same_key_in_two_pipelines_is_two_different_claims(store):
    assert store.claim_run(run_id=new_id(), pipeline="a", idempotency_key="k", input_hash_=None)
    assert store.claim_run(run_id=new_id(), pipeline="b", idempotency_key="k", input_hash_=None)


def test_an_unknown_signal_kind_is_rejected(store):
    run_id = new_id()
    store.claim_run(run_id=run_id, pipeline="p", idempotency_key=None, input_hash_=None)
    with pytest.raises(ValueError, match="unknown misfit signal"):
        store.record_signal(run_id, "vibes", "hunch")


def test_the_same_observation_about_one_run_is_recorded_once(store):
    run_id = new_id()
    store.claim_run(run_id=run_id, pipeline="p", idempotency_key=None, input_hash_=None)
    first = store.record_signal(run_id, "missing_step", "fetch_contract")
    second = store.record_signal(run_id, "missing_step", "fetch_contract")
    other = store.record_signal(run_id, "missing_step", "fetch_invoice")

    assert first == second
    assert other != first
    assert len(store.signals()) == 2


def test_values_that_cannot_survive_json_are_marked_not_silently_lost():
    encoded = encode({"blob": object()})
    assert is_opaque(decode(encoded))
    assert not is_opaque(decode(encode({"fine": 1})))


def test_input_hash_is_stable_for_bytes_and_for_structures():
    assert input_hash(b"abc") == input_hash(b"abc")
    assert input_hash({"a": 1}) == input_hash({"a": 1})
    assert input_hash(b"abc") != input_hash(b"abd")


def test_since_ts_is_none_for_an_open_window():
    assert since_ts(None) is None


def test_foreign_keys_stop_a_signal_from_dangling(store):
    with pytest.raises(sqlite3.IntegrityError):
        store.record_signal("no-such-run", "needs_review", None)


async def test_completed_steps_is_the_resume_checkpoint(store, build):
    from boundedrun import step

    @step
    async def one(ctx, value):
        return value + 1

    @step
    async def two(ctx, value):
        return value + 1

    result = await build(one, two).run(0)
    checkpoint = await store.completed_steps(result.run_id)

    assert sorted(checkpoint) == [0, 1]
    assert decode(checkpoint[1]["output_json"]) == 2


def test_a_missing_store_file_is_created_with_its_parent_directory(tmp_path):
    nested = tmp_path / "a" / "b" / "runs.db"
    db = Store(str(nested))
    db.connect()
    assert nested.exists()
    db.close()
