"""Durable SQLite backend: restart persistence, multi-process sharing,
atomic idempotency/key mapping, seal freezing, reset counts and explicit
storage failures."""

from __future__ import annotations

import multiprocessing
import os

import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.config import Settings
from app.engine import build_seal_evidence, evaluate_batch, hash_seal_evidence
from app.storage import (
    IdempotencyConflict,
    SqliteEventStore,
    StoredEvent,
    StorageError,
)


def _event(rev, parent, start, end, value=None, withdrawn=False, key=None,
           batch="B1", sensor="S1", unit="mm"):
    return {
        "batch_id": batch,
        "sensor_id": sensor,
        "revision_id": rev,
        "parent_id": parent,
        "idempotency_key": key or f"k-{rev}",
        "start_ns": start,
        "end_ns": end,
        "value": value,
        "unit": unit,
        "withdrawn": withdrawn,
    }


def _stored(payload: dict) -> StoredEvent:
    return StoredEvent(
        batch_id=payload["batch_id"],
        sensor_id=payload["sensor_id"],
        revision_id=payload["revision_id"],
        parent_id=payload["parent_id"],
        idempotency_key=payload["idempotency_key"],
        start_ns=payload["start_ns"],
        end_ns=payload["end_ns"],
        value=None if payload["withdrawn"] else float(payload["value"]),
        unit=payload["unit"],
        withdrawn=payload["withdrawn"],
    )


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "data" / "calibration.db")


@pytest.fixture()
def sqlite_client(db_path):
    db = SqliteEventStore(db_path)
    old_store = main_module.store
    main_module.store = db
    try:
        with TestClient(main_module.app) as c:
            yield c, db, db_path
    finally:
        main_module.store = old_store


# -- restart persistence -----------------------------------------------------


def test_events_survive_store_recreation(sqlite_client):
    client, db, path = sqlite_client
    client.post("/api/v1/batches/B1/events", json=_event("v1", "ROOT", 0, 100, 1.0))
    client.post("/api/v1/batches/B1/events", json=_event("v2", "v1", 10, 20, 2.0))
    expected = client.get("/api/v1/batches/B1/result").json()

    # Simulate a service restart: a brand-new store process opens the file.
    restarted = SqliteEventStore(path)
    batch = restarted.get_batch("B1")
    assert batch is not None and len(batch.events) == 2
    assert set(batch.idempotency) == {"k-v1", "k-v2"}
    result = evaluate_batch("B1", batch)
    assert result["timeline_hash"] == expected["timeline_hash"]


def test_unknown_batch_is_not_fabricated_after_restart(sqlite_client):
    _, db, path = sqlite_client
    restarted = SqliteEventStore(path)
    assert restarted.get_batch("never-seen") is None
    assert restarted.get_seal("never-seen", "s") is None


def test_seal_evidence_survives_restart_and_stays_frozen(sqlite_client):
    client, db, path = sqlite_client
    client.post("/api/v1/batches/B1/events", json=_event("v1", "ROOT", 0, 100, 1.0))
    client.post("/api/v1/batches/B1/events", json=_event("v2", "v1", 10, 20, 2.0))
    h = client.get("/api/v1/batches/B1/result").json()["timeline_hash"]
    sealed = client.post(
        "/api/v1/batches/B1/seals",
        json={"batch_id": "B1", "seal_id": "S-1", "timeline_hash": h},
    ).json()

    # A later revision moves the live batch on...
    client.post(
        "/api/v1/batches/B1/events", json=_event("v3", "v2", 10, 20, 9.0, key="k-v3")
    )

    # ...but the frozen evidence survives a restart unchanged.
    restarted = SqliteEventStore(path)
    record = restarted.get_seal("B1", "S-1")
    assert record is not None
    assert record.evidence_digest == sealed["evidence_digest"]
    assert len(record.evidence["revisions"]) == 2
    assert record.timeline_hash == h
    recall = client.get("/api/v1/batches/B1/seals/S-1").json()
    assert recall["consistent"] is False
    assert recall["evidence_digest"] == sealed["evidence_digest"]


# -- API semantics on sqlite -------------------------------------------------


def test_accepted_duplicate_conflict_unchanged(sqlite_client):
    client, _, _ = sqlite_client
    assert client.post(
        "/api/v1/batches/B1/events", json=_event("v1", "ROOT", 0, 10, 1.0)
    ).json()["status"] == "accepted"
    assert client.post(
        "/api/v1/batches/B1/events", json=_event("v1", "ROOT", 0, 10, 1.0)
    ).json()["status"] == "duplicate"
    # Same revision, same content, new key: duplicate and the new key sticks.
    assert client.post(
        "/api/v1/batches/B1/events",
        json=_event("v1", "ROOT", 0, 10, 1.0, key="k-other"),
    ).json()["status"] == "duplicate"
    # Original key still deduplicates; changed payload under it conflicts.
    r = client.post(
        "/api/v1/batches/B1/events", json=_event("v1", "ROOT", 0, 10, 7.0)
    )
    assert r.status_code == 409
    # Same revision with different content always conflicts.
    r = client.post(
        "/api/v1/batches/B1/events",
        json=_event("v1", "ROOT", 0, 10, 8.0, key="k-brand-new"),
    )
    assert r.status_code == 409


def test_preview_does_not_persist_or_occupy_key(sqlite_client):
    client, db, path = sqlite_client
    preview = client.post(
        "/api/v1/batches/B1/events:preview",
        json=_event("v1", "ROOT", 0, 10, 1.0),
    ).json()
    assert preview["status"] == "submittable"

    # The key is free for a real submission afterwards, in any process.
    other = SqliteEventStore(path)
    candidate = _stored(_event("v1", "ROOT", 0, 10, 1.0))
    assert other.classify_submission(candidate) == "submittable"
    assert client.post(
        "/api/v1/batches/B1/events", json=_event("v1", "ROOT", 0, 10, 1.0)
    ).json()["status"] == "accepted"
    assert "k-v1" in other.get_batch("B1").idempotency


def test_seal_replay_duplicate_diff_param_conflict(sqlite_client):
    client, _, _ = sqlite_client
    client.post("/api/v1/batches/B1/events", json=_event("v1", "ROOT", 0, 10, 1.0))
    h = client.get("/api/v1/batches/B1/result").json()["timeline_hash"]
    body = {"batch_id": "B1", "seal_id": "S-1", "timeline_hash": h}
    first = client.post("/api/v1/batches/B1/seals", json=body).json()
    assert first["status"] == "sealed"
    again = client.post("/api/v1/batches/B1/seals", json=body).json()
    assert again["status"] == "duplicate"
    assert again["evidence_digest"] == first["evidence_digest"]

    # Same seal id, different parameters -> 409, original evidence untouched.
    other_hash = "a" * 64
    r = client.post(
        "/api/v1/batches/B1/seals",
        json={"batch_id": "B1", "seal_id": "S-1", "timeline_hash": other_hash},
    )
    assert r.status_code == 409
    rec = client.get("/api/v1/batches/B1/seals/S-1").json()
    assert rec["evidence_digest"] == first["evidence_digest"]


def test_seal_stale_hash_and_incomplete_rejected(sqlite_client):
    client, _, _ = sqlite_client
    client.post("/api/v1/batches/B1/events", json=_event("v1", "ROOT", 0, 10, 1.0))
    h = client.get("/api/v1/batches/B1/result").json()["timeline_hash"]
    # Child arrives after the client cached the hash -> stale hash, 409.
    client.post("/api/v1/batches/B1/events", json=_event("v2", "v1", 0, 10, 2.0))
    r = client.post(
        "/api/v1/batches/B1/seals",
        json={"batch_id": "B1", "seal_id": "S-1", "timeline_hash": h},
    )
    assert r.status_code == 409
    assert client.get("/api/v1/batches/B1/seals/S-1").status_code == 404

    # Blocked (incomplete) batch cannot be sealed.
    client.post(
        "/api/v1/batches/B9/events",
        json=_event("c", "missing", 0, 10, 1.0, key="k-c", batch="B9"),
    )
    h9 = client.get("/api/v1/batches/B9/result").json()["timeline_hash"]
    r = client.post(
        "/api/v1/batches/B9/seals",
        json={"batch_id": "B9", "seal_id": "S-9", "timeline_hash": h9},
    )
    assert r.status_code == 409


def test_reset_returns_accurate_counts(sqlite_client):
    client, _, _ = sqlite_client
    client.post("/api/v1/batches/B1/events", json=_event("v1", "ROOT", 0, 10, 1.0))
    client.post(
        "/api/v1/batches/B1/events",
        json=_event("v2", "v1", 10, 20, 2.0, batch="B1"),
    )
    client.post(
        "/api/v1/batches/B2/events",
        json=_event("r", "ROOT", 0, 5, 4.0, batch="B2", key="k-r"),
    )
    h = client.get("/api/v1/batches/B2/result").json()["timeline_hash"]
    client.post(
        "/api/v1/batches/B2/seals",
        json={"batch_id": "B2", "seal_id": "S", "timeline_hash": h},
    )
    body = client.post("/admin/reset").json()
    assert body == {
        "status": "reset",
        "batches_removed": 2,
        "events_removed": 3,
        "seals_removed": 1,
    }
    assert client.get("/api/v1/batches/B1/result").status_code == 404
    assert client.get("/api/v1/batches/B2/seals/S").status_code == 404
    # Resetting empty storage reports zeros, not an error.
    assert client.post("/admin/reset").json() == {
        "status": "reset",
        "batches_removed": 0,
        "events_removed": 0,
        "seals_removed": 0,
    }


def test_health_reports_sqlite(sqlite_client, monkeypatch):
    client, _, _ = sqlite_client
    monkeypatch.setattr(
        main_module, "settings", Settings(storage_backend="sqlite", sqlite_path="/x.db")
    )
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert body["storage"] == "sqlite"
    assert "version" in body


# -- two stores sharing one file (multi-process equivalent) ------------------


def test_two_store_instances_share_and_serialize(db_path):
    first = SqliteEventStore(db_path)
    first.add(_stored(_event("v1", "ROOT", 0, 10, 1.0)))
    second = SqliteEventStore(db_path)  # another service process
    assert second.add(_stored(_event("v1", "ROOT", 0, 10, 1.0))) == "duplicate"
    assert second.add(_stored(_event("v2", "v1", 10, 20, 2.0))) == "accepted"
    # The first process immediately observes the second process' commit.
    batch = first.get_batch("B1")
    assert set(batch.events) == {"v1", "v2"}
    assert set(batch.idempotency) == {"k-v1", "k-v2"}


def _concurrent_worker(args):
    path, payload = args
    store = SqliteEventStore(path)
    try:
        return store.add(_stored(payload))
    except IdempotencyConflict:
        return "conflict"
    except StorageError:
        return "storage-error"


def test_concurrent_same_key_submission_serializes(db_path):
    payload = _event("v1", "ROOT", 0, 10, 1.0)
    # Pre-create the schema so the workers only contend on the write itself.
    SqliteEventStore(db_path)
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(6) as pool:
        results = pool.map(_concurrent_worker, [(db_path, payload)] * 6)
    assert results.count("accepted") == 1
    assert results.count("duplicate") == 5
    assert "conflict" not in results
    assert "storage-error" not in results

    winner = SqliteEventStore(db_path)
    batch = winner.get_batch("B1")
    assert len(batch.events) == 1
    assert list(batch.idempotency) == ["k-v1"]


def test_concurrent_same_revision_different_content_conflicts(db_path):
    accepted_payload = _event("v1", "ROOT", 0, 10, 1.0, key="k-a")
    changed_payload = _event("v1", "ROOT", 0, 10, 2.0, key="k-b")
    SqliteEventStore(db_path)
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(4) as pool:
        results = pool.map(
            _concurrent_worker,
            [(db_path, accepted_payload), (db_path, changed_payload)] * 2,
        )
    # Whatever the interleaving: one value wins exactly once; the other key's
    # matching replays duplicate and its divergent attempts conflict.
    assert results.count("accepted") == 1
    assert "storage-error" not in results
    assert results.count("conflict") >= 1
    store = SqliteEventStore(db_path)
    batch = store.get_batch("B1")
    assert set(batch.events) == {"v1"}


# -- storage failures are explicit -------------------------------------------


def test_unopenable_storage_raises_storage_error():
    bad_path = os.path.join("/dev/null", "cannot", "be", "a", "dir", "cal.db")
    with pytest.raises(StorageError):
        SqliteEventStore(bad_path)


def test_storage_failure_is_503_not_404(sqlite_client, monkeypatch):
    client, db, _ = sqlite_client

    def boom(batch_id):
        raise StorageError("simulated disk failure")

    monkeypatch.setattr(db, "get_batch", boom)
    r = client.get("/api/v1/batches/B1/result")
    assert r.status_code == 503
    assert "storage" in r.json()["detail"]


def test_evidence_digest_roundtrips_identically(sqlite_client):
    client, db, path = sqlite_client
    client.post(
        "/api/v1/batches/B1/events",
        json=_event("v1", "ROOT", 0, 100, 1.5, unit="℃"),
    )
    h = client.get("/api/v1/batches/B1/result").json()["timeline_hash"]
    sealed = client.post(
        "/api/v1/batches/B1/seals",
        json={"batch_id": "B1", "seal_id": "S", "timeline_hash": h},
    ).json()

    restarted = SqliteEventStore(path)
    record = restarted.get_seal("B1", "S")
    batch = restarted.get_batch("B1")
    assert hash_seal_evidence(record.evidence) == record.evidence_digest
    assert hash_seal_evidence(build_seal_evidence("B1", batch)) == (
        record.evidence_digest
    )
    assert sealed["evidence_digest"] == record.evidence_digest
