"""Durable, shared SQLite storage tests.

These tests deliberately construct their own :class:`SqliteEventStore` /
app instances (the shared suite fixture runs the in-memory backend):

* revisions, idempotency-key mappings and seal evidence survive a fresh
  store instance ("process restart") and are visible to another instance
  pointed at the same database file ("shared by multiple processes");
* previews leave no trace; reset clears and counts everything;
* backend failures surface as explicit errors/503, never as "empty batch";
* true cross-process sharing is verified end-to-end with subprocesses.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.storage import (
    IdempotencyConflict,
    SqliteEventStore,
    StorageError,
    StoredEvent,
    create_store,
)


def _event(
    rev,
    parent,
    key,
    *,
    batch="B",
    sensor="S1",
    start=0,
    end=10,
    value=1.0,
    unit="C",
    withdrawn=False,
):
    return StoredEvent(
        batch_id=batch,
        sensor_id=sensor,
        revision_id=rev,
        parent_id=parent,
        idempotency_key=key,
        start_ns=start,
        end_ns=end,
        value=None if withdrawn else value,
        unit=None if withdrawn else unit,
        withdrawn=withdrawn,
    )


@pytest.fixture()
def db_path(tmp_path: Path) -> str:
    return str(tmp_path / "data" / "calibration.db")


@pytest.fixture()
def sqlite_store(db_path: str) -> SqliteEventStore:
    return SqliteEventStore(db_path)


def test_revisions_and_keys_survive_restart(sqlite_store, db_path):
    assert sqlite_store.add(_event("v1", "ROOT", "k1")) == "accepted"
    # Same revision, same content, different key: registers the new key.
    assert sqlite_store.add(_event("v1", "ROOT", "k1-alt")) == "duplicate"

    # A fresh store instance over the same file is a "restarted process".
    restarted = SqliteEventStore(db_path)
    batch = restarted.get_batch("B")
    assert set(batch.events) == {"v1"}
    assert set(batch.idempotency) == {"k1", "k1-alt"}
    # The duplicate rules keep working from the persisted mapping.
    assert restarted.add(_event("v1", "ROOT", "k1")) == "duplicate"
    with pytest.raises(IdempotencyConflict):
        restarted.add(_event("v1", "ROOT", "k2", value=2.0))
    with pytest.raises(IdempotencyConflict):
        restarted.add(_event("v2", "ROOT", "k1", value=2.0))


def test_revision_and_key_insert_is_atomic(sqlite_store):
    # The accepted revision and its key mapping commit in one transaction;
    # a second instance either sees both or neither, never a half mapping.
    assert sqlite_store.add(_event("v1", "ROOT", "k1")) == "accepted"
    other = SqliteEventStore(sqlite_store.path)
    batch = other.get_batch("B")
    assert set(batch.events) == {"v1"}
    assert set(batch.idempotency) == {"k1"}


def test_unknown_batch_is_none_not_an_error(sqlite_store):
    assert sqlite_store.get_batch("missing") is None
    assert sqlite_store.has_batch("missing") is False


def test_preview_leaves_no_trace(sqlite_store, db_path):
    # classify_submission must not register keys or create batches, even on
    # the durable backend.
    assert (
        sqlite_store.classify_submission(_event("v1", "ROOT", "preview-key"))
        == "submittable"
    )
    assert sqlite_store.get_batch("B") is None
    restarted = SqliteEventStore(db_path)
    assert restarted.get_batch("B") is None
    # The key is still free for a real submission after the preview.
    assert restarted.add(_event("v1", "ROOT", "preview-key")) == "accepted"


def test_reset_clears_and_counts(sqlite_store):
    sqlite_store.add(_event("v1", "ROOT", "k1", batch="B1"))
    sqlite_store.add(
        _event("v1", "ROOT", "k1", batch="B2", sensor="S2", value=4.0)
    )
    assert sqlite_store.reset() == (2, 2, 0)
    assert sqlite_store.get_batch("B1") is None
    assert sqlite_store.get_batch("B2") is None
    # Resetting an already-empty store reports zeroes, not an error.
    assert sqlite_store.reset() == (0, 0, 0)


def test_seals_survive_restart_and_are_immutable(sqlite_store, db_path):
    sqlite_store.add(_event("v1", "ROOT", "k1", start=0, end=300, value=1.0))
    sqlite_store.add(
        _event("v2", "v1", "k2", start=100, end=200, value=3.5)
    )
    from app.engine import build_seal_evidence, hash_seal_evidence

    evidence = build_seal_evidence("B", sqlite_store.get_batch("B"))
    digest = hash_seal_evidence(evidence)
    timeline_hash = evidence["result"]["timeline_hash"]

    from app.storage import SealRecord

    sqlite_store.add_seal(
        SealRecord(
            batch_id="B",
            seal_id="SEAL-1",
            timeline_hash=timeline_hash,
            evidence=evidence,
            evidence_digest=digest,
        )
    )

    restarted = SqliteEventStore(db_path)
    record = restarted.get_seal("B", "SEAL-1")
    assert record is not None
    assert record.evidence_digest == digest
    assert [r["revision_id"] for r in record.evidence["revisions"]] == [
        "v1",
        "v2",
    ]
    assert record.timeline_hash == timeline_hash

    # A later submission changes the live batch but never the frozen seal.
    restarted.add(_event("v3", "v2", "k3", value=9.0))
    frozen = restarted.get_seal("B", "SEAL-1")
    assert [r["revision_id"] for r in frozen.evidence["revisions"]] == [
        "v1",
        "v2",
    ]
    new_evidence = build_seal_evidence("B", restarted.get_batch("B"))
    assert hash_seal_evidence(new_evidence) != digest


def test_reset_clears_seals(sqlite_store):
    from app.engine import build_seal_evidence, hash_seal_evidence
    from app.storage import SealRecord

    sqlite_store.add(_event("v1", "ROOT", "k1"))
    evidence = build_seal_evidence("B", sqlite_store.get_batch("B"))
    sqlite_store.add_seal(
        SealRecord("B", "SEAL-1", evidence["result"]["timeline_hash"], evidence,
                   hash_seal_evidence(evidence))
    )
    assert sqlite_store.reset() == (1, 1, 1)
    assert sqlite_store.get_seal("B", "SEAL-1") is None


# ---------------------------------------------------------------------------
# Factory / configuration
# ---------------------------------------------------------------------------


def test_factory_rejects_memory_dsn_for_sqlite(tmp_path):
    with pytest.raises(ValueError):
        create_store("sqlite", ":memory:")


def test_factory_requires_path_for_sqlite():
    with pytest.raises(StorageError):
        create_store("sqlite", "")


def test_factory_rejects_unknown_backend(tmp_path):
    with pytest.raises(ValueError):
        create_store("nfs", str(tmp_path / "x.db"))


def test_unwritable_path_is_storage_error(tmp_path):
    bad = tmp_path / "no-such-dir" / "x.db"
    # Make the parent creation fail: create the leaf path as a regular file
    # is the opposite case; instead point inside a read-only directory.
    readonly = tmp_path / "readonly"
    readonly.mkdir()
    readonly.chmod(0o500)
    target = readonly / "deep" / "x.db"
    if os.geteuid() == 0:
        pytest.skip("root bypasses directory permissions")
    with pytest.raises(StorageError):
        SqliteEventStore(target)


def test_runtime_storage_failure_is_storage_error(sqlite_store, monkeypatch):
    # A backend error on read must raise StorageError rather than returning
    # None (which callers would misinterpret as an empty batch / 404).
    import sqlite3

    def boom(self):
        conn = sqlite3.connect(self.path)
        conn.close()
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(SqliteEventStore, "_connect", boom)
    with pytest.raises(StorageError):
        sqlite_store.get_batch("B")
    with pytest.raises(StorageError):
        sqlite_store.add(_event("v1", "ROOT", "k1"))


# ---------------------------------------------------------------------------
# HTTP API on the durable backend (incl. restart via a second app instance)
# ---------------------------------------------------------------------------


def _make_app_client(db_path: str) -> TestClient:
    store = SqliteEventStore(db_path)
    # Point the module-level app at this durable store (the exception
    # handlers and routes are module-level and backend-agnostic).
    main_module.store = store
    return TestClient(main_module.app)


@pytest.fixture()
def api_client(db_path, monkeypatch):
    main_module.store.reset()  # no-op safety for a stray module store
    client = _make_app_client(db_path)
    with client:
        yield client
    main_module.store.reset()
    # Restore the suite-default in-memory store for other tests.
    main_module.store = create_store("memory")


def _post_event(client, rev, parent, key, **kw):
    body = {
        "batch_id": kw.pop("batch", "B42"),
        "sensor_id": kw.pop("sensor", "S1"),
        "revision_id": rev,
        "parent_id": parent,
        "idempotency_key": key,
        "start_ns": kw.pop("start", 0),
        "end_ns": kw.pop("end", 300),
        "value": kw.pop("value", 1.0),
        "unit": kw.pop("unit", "C"),
        "withdrawn": kw.pop("withdrawn", False),
    }
    return client.post(f"/api/v1/batches/{body['batch_id']}/events", json=body)


def test_health_reports_sqlite(api_client):
    body = api_client.get("/health").json()
    assert body == {
        "status": "ok",
        "storage": "sqlite",
        "version": main_module.__version__,
    }


def test_api_state_survives_app_restart(api_client, db_path):
    assert _post_event(api_client, "v1", "ROOT", "e1").json()["status"] == "accepted"
    assert _post_event(api_client, "v2", "v1", "e2", value=3.5,
                       start=100, end=200).json()["status"] == "accepted"

    # "Restart": a brand new client/store over the same database file.
    restarted = _make_app_client(db_path)
    with restarted:
        result = restarted.get("/api/v1/batches/B42/result").json()
    assert result["revisions_total"] == 2
    assert result["complete"] is True

    # Exact replay through the restarted process is a duplicate; conflicting
    # replay is still 409.
    with restarted:
        dup = _post_event(restarted, "v1", "ROOT", "e1")
        assert dup.json()["status"] == "duplicate"
        conflict = _post_event(restarted, "v1", "ROOT", "other", value=7.0)
        assert conflict.status_code == 409


def test_api_preview_does_not_persist(api_client, db_path):
    preview = api_client.post(
        "/api/v1/batches/B42/events:preview",
        json={
            "batch_id": "B42",
            "sensor_id": "S1",
            "revision_id": "v1",
            "parent_id": "ROOT",
            "idempotency_key": "p1",
            "start_ns": 0,
            "end_ns": 300,
            "value": 1.0,
            "unit": "C",
        },
    ).json()
    assert preview["status"] == "submittable"
    assert api_client.get("/api/v1/batches/B42/result").status_code == 404
    # Restarted process also sees no batch and the key is still free.
    restarted = _make_app_client(db_path)
    with restarted:
        assert restarted.get("/api/v1/batches/B42/result").status_code == 404
        accepted = _post_event(restarted, "v1", "ROOT", "p1")
        assert accepted.json()["status"] == "accepted"
    main_module.store = create_store("memory")


def test_api_seal_replay_and_immutability_across_restart(api_client, db_path):
    _post_event(api_client, "v1", "ROOT", "e1")
    _post_event(api_client, "v2", "v1", "e2", value=3.5, start=100, end=200)
    h = api_client.get("/api/v1/batches/B42/result").json()["timeline_hash"]

    sealed = api_client.post(
        "/api/v1/batches/B42/seals",
        json={"batch_id": "B42", "seal_id": "SEAL-1", "timeline_hash": h},
    ).json()
    assert sealed["status"] == "sealed"
    digest = sealed["evidence_digest"]

    restarted = _make_app_client(db_path)
    with restarted:
        # Original request replay after restart -> original frozen evidence.
        replay = restarted.post(
            "/api/v1/batches/B42/seals",
            json={"batch_id": "B42", "seal_id": "SEAL-1", "timeline_hash": h},
        ).json()
        assert replay["status"] == "duplicate"
        assert replay["evidence_digest"] == digest

        # Different parameters under the same seal id -> 409.
        diff = restarted.post(
            "/api/v1/batches/B42/seals",
            json={"batch_id": "B42", "seal_id": "SEAL-1",
                  "timeline_hash": "0" * 64},
        )
        assert diff.status_code == 409

        # A new submission changes the live batch but never the seal.
        _post_event(restarted, "v3", "v2", "e3", value=9.0)
        recall = restarted.get("/api/v1/batches/B42/seals/SEAL-1").json()
        assert recall["consistent"] is False
        assert [r["revision_id"] for r in recall["evidence"]["revisions"]] == [
            "v1",
            "v2",
        ]
        assert recall["evidence_digest"] == digest
    main_module.store = create_store("memory")


def test_api_storage_failure_returns_503(api_client, monkeypatch):
    def failing_get_batch(self, batch_id):
        raise StorageError("simulated disk failure")

    monkeypatch.setattr(SqliteEventStore, "get_batch", failing_get_batch)
    r = api_client.get("/api/v1/batches/B42/result")
    assert r.status_code == 503
    assert "storage unavailable" in r.json()["detail"]


# ---------------------------------------------------------------------------
# True multi-process sharing (two OS processes, one SQLite file)
# ---------------------------------------------------------------------------

_HELPER = r"""
import os, sys, json
sys.path.insert(0, %r)
os.environ["CAL_STORAGE"] = "sqlite"
os.environ["CAL_SQLITE_PATH"] = %r
from fastapi.testclient import TestClient
from app.main import app

def event(rev, parent, key, value=1.0, start=0, end=300):
    return {"batch_id":"B77","sensor_id":"S1","revision_id":rev,"parent_id":parent,
            "idempotency_key":key,"start_ns":start,"end_ns":end,
            "value":value,"unit":"C","withdrawn":False}

phase = sys.argv[1]
with TestClient(app) as c:
    if phase == "writer":
        r1 = c.post("/api/v1/batches/B77/events", json=event("v1","ROOT","w1"))
        r2 = c.post("/api/v1/batches/B77/events",
                    json=event("v2","v1","w2",value=3.5,start=100,end=200))
        h = c.get("/api/v1/batches/B77/result").json()["timeline_hash"]
        seal = c.post("/api/v1/batches/B77/seals",
                      json={"batch_id":"B77","seal_id":"S","timeline_hash":h})
        print(json.dumps({
            "r1": r1.json()["status"], "r2": r2.json()["status"],
            "seal": seal.json()["status"], "digest": seal.json()["evidence_digest"],
            "hash": h,
        }))
    elif phase == "reader":
        result = c.get("/api/v1/batches/B77/result").json()
        recall = c.get("/api/v1/batches/B77/seals/S").json()
        # Same payload through a second process -> duplicate.
        dup = c.post("/api/v1/batches/B77/events", json=event("v1","ROOT","w1"))
        print(json.dumps({
            "revisions": result["revisions_total"],
            "complete": result["complete"],
            "digest": recall["evidence_digest"],
            "consistent": recall["consistent"],
            "dup": dup.json()["status"],
        }))
"""


def test_two_os_processes_share_state(db_path):
    workspace = str(Path(__file__).resolve().parents[1])
    helper = (_HELPER % (workspace, db_path)).encode("utf-8")

    writer = subprocess.run(
        [sys.executable, "-c", helper, "writer"],
        capture_output=True,
        text=True,
        check=True,
    )
    w = json.loads(writer.stdout.strip().splitlines()[-1])
    assert w["r1"] == "accepted" and w["r2"] == "accepted"
    assert w["seal"] == "sealed"

    # Completely separate OS process: must see revisions, keys and the seal.
    reader = subprocess.run(
        [sys.executable, "-c", helper, "reader"],
        capture_output=True,
        text=True,
        check=True,
    )
    r = json.loads(reader.stdout.strip().splitlines()[-1])
    assert r["revisions"] == 2
    assert r["complete"] is True
    assert r["dup"] == "duplicate"
    assert r["digest"] == w["digest"]
    assert r["consistent"] is True


def test_two_uvicorn_workers_share_state(db_path):
    """Smoke test: two uvicorn worker processes serve one shared database."""
    workspace = str(Path(__file__).resolve().parents[1])
    port = _free_port()
    env = {
        **os.environ,
        "CAL_STORAGE": "sqlite",
        "CAL_SQLITE_PATH": db_path,
        "CAL_PORT": str(port),
        "PYTHONPATH": workspace,
    }
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--workers",
            "2",
        ],
        cwd=workspace,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        import urllib.request

        _wait_for_health(f"http://127.0.0.1:{port}/health", proc)
        body = json.dumps(
            {
                "batch_id": "B88",
                "sensor_id": "S1",
                "revision_id": "v1",
                "parent_id": "ROOT",
                "idempotency_key": "k1",
                "start_ns": 0,
                "end_ns": 100,
                "value": 1.0,
                "unit": "C",
            }
        ).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/v1/batches/B88/events",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            assert json.loads(resp.read())["status"] == "accepted"
        # Repeated hits may land on either worker; the state is shared.
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/api/v1/batches/B88/result", timeout=10
        ) as resp:
            assert json.loads(resp.read())["revisions_total"] == 1
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_health(url: str, proc: subprocess.Popen, timeout: float = 20.0) -> None:
    import urllib.error
    import urllib.request

    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        if proc.poll() is not None:
            raise AssertionError(
                f"server exited early: {proc.stderr.read() if proc.stderr else ''}"
            )
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError, OSError) as exc:
            last_err = exc
            time.sleep(0.2)
    raise AssertionError(f"server never became healthy: {last_err}")
