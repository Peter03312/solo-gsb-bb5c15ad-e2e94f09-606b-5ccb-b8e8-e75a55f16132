"""End-to-end API behavior for batch sealing (immutable archival evidence)."""


def _event(rev, parent, start, end, value=None, withdrawn=False, key=None,
           batch="B1", sensor="S1", unit="mm"):
    return {
        "batch_id": batch,
        "sensor_id": sensor,
        "revision_id": rev,
        "parent_id": parent,
        "idempotency_key": key or f"k-{sensor}-{rev}",
        "start_ns": start,
        "end_ns": end,
        "value": value,
        "unit": unit,
        "withdrawn": withdrawn,
    }


def _post(client, event, batch="B1"):
    return client.post(f"/api/v1/batches/{batch}/events", json=event)


def _hash(client, batch="B1"):
    return client.get(f"/api/v1/batches/{batch}/result").json()["timeline_hash"]


def _seal(client, seal_id="SEAL-1", timeline_hash=None, batch="B1"):
    body = {
        "batch_id": batch,
        "seal_id": seal_id,
        "timeline_hash": timeline_hash if timeline_hash is not None else _hash(client, batch),
    }
    return client.post(f"/api/v1/batches/{batch}/seals", json=body)


def _recall(client, seal_id="SEAL-1", batch="B1"):
    return client.get(f"/api/v1/batches/{batch}/seals/{seal_id}")


def _complete_batch(client, batch="B1"):
    """A grounded two-revision batch: [0,100)=1.0 with [10,20)=2.0 on top."""
    _post(client, _event("v1", "ROOT", 0, 100, value=1.0, batch=batch), batch=batch)
    _post(client, _event("v2", "v1", 10, 20, value=2.0, batch=batch), batch=batch)


def test_seal_complete_batch_returns_evidence_and_digest(client):
    _complete_batch(client)
    r = _seal(client)
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "sealed"
    assert body["batch_id"] == "B1" and body["seal_id"] == "SEAL-1"
    assert body["timeline_hash"] == _hash(client)
    assert len(body["evidence_digest"]) == 64

    evidence = body["evidence"]
    assert evidence["batch_id"] == "B1"
    # Every revision stored at seal time, sorted by (sensor_id, revision_id).
    assert [(rev["sensor_id"], rev["revision_id"]) for rev in evidence["revisions"]] == [
        ("S1", "v1"),
        ("S1", "v2"),
    ]
    v1 = evidence["revisions"][0]
    assert v1["parent_id"] == "ROOT" and v1["value"] == 1.0
    assert v1["unit"] == "mm" and v1["withdrawn"] is False
    assert v1["idempotency_key"] == "k-S1-v1"
    # The folded result frozen at seal time matches the live result.
    assert evidence["result"] == client.get("/api/v1/batches/B1/result").json()
    assert evidence["result"]["complete"] is True


def test_seal_replay_returns_original_snapshot_even_after_drift(client):
    _complete_batch(client)
    original_hash = _hash(client)
    first = _seal(client, timeline_hash=original_hash).json()

    # The live batch moves on after sealing.
    _post(client, _event("v3", "v2", 30, 40, value=3.0))

    # Replaying the original request (same seal id + same hash) returns the
    # frozen snapshot even though the live batch hash has changed.
    replay = _seal(client, timeline_hash=original_hash)
    assert replay.status_code == 200
    body = replay.json()
    assert body["status"] == "duplicate"
    # The original snapshot is returned unchanged.
    assert body["evidence"] == first["evidence"]
    assert body["evidence_digest"] == first["evidence_digest"]
    assert body["timeline_hash"] == first["timeline_hash"]
    assert len(body["evidence"]["revisions"]) == 2


def test_seal_same_id_different_hash_conflicts(client):
    _complete_batch(client)
    assert _seal(client).status_code == 200
    _post(client, _event("v3", "v2", 30, 40, value=3.0))
    # Same seal id but a different request hash: conflict, original kept.
    r = _seal(client)
    assert r.status_code == 409
    recalled = _recall(client).json()
    assert len(recalled["evidence"]["revisions"]) == 2


def test_seal_unknown_batch_404(client):
    r = _seal(client, timeline_hash="0" * 64)
    assert r.status_code == 404
    assert "'B1'" in r.json()["detail"]


def test_seal_stale_hash_409_and_leaves_no_record(client):
    _complete_batch(client)
    stale = _hash(client)
    _post(client, _event("v3", "v2", 30, 40, value=3.0))
    r = _seal(client, timeline_hash=stale)
    assert r.status_code == 409
    assert "does not match" in r.json()["detail"]
    # A rejected seal leaves no record behind.
    assert _recall(client).status_code == 404


def test_seal_incomplete_batch_with_blocked_revisions_409(client):
    _post(client, _event("v1", "ROOT", 0, 100, value=1.0))
    _post(client, _event("orphan", "ghost", 10, 20, value=9.0, key="ko"))
    r = _seal(client)
    assert r.status_code == 409
    assert "not complete" in r.json()["detail"]
    assert _recall(client).status_code == 404


def test_seal_batch_with_invalid_cycle_409(client):
    _post(client, _event("c1", "c2", 0, 10, value=1.0, key="kc1"))
    _post(client, _event("c2", "c1", 0, 10, value=2.0, key="kc2"))
    r = _seal(client)
    assert r.status_code == 409
    assert _recall(client).status_code == 404


def test_seal_path_body_batch_mismatch_422(client):
    _complete_batch(client)
    body = {"batch_id": "OTHER", "seal_id": "SEAL-1", "timeline_hash": _hash(client)}
    r = client.post("/api/v1/batches/B1/seals", json=body)
    assert r.status_code == 422


def test_seal_malformed_hash_422(client):
    _complete_batch(client)
    for bad in ("abc", "0" * 63, "0" * 65, "A" * 64, "g" * 64):
        assert _seal(client, timeline_hash=bad).status_code == 422


def test_seal_invalid_params_422(client):
    _complete_batch(client)
    # Missing fields.
    assert client.post("/api/v1/batches/B1/seals", json={"batch_id": "B1"}).status_code == 422
    # Empty seal id.
    assert _seal(client, seal_id="").status_code == 422
    # Unknown extra field is rejected.
    body = {
        "batch_id": "B1",
        "seal_id": "SEAL-1",
        "timeline_hash": _hash(client),
        "note": "x",
    }
    assert client.post("/api/v1/batches/B1/seals", json=body).status_code == 422


def test_seal_does_not_change_live_batch_result(client):
    _complete_batch(client)
    before = client.get("/api/v1/batches/B1/result").json()
    _seal(client)
    assert client.get("/api/v1/batches/B1/result").json() == before


def test_recall_seal_returns_evidence_and_consistency(client):
    _complete_batch(client)
    sealed = _seal(client).json()
    r = _recall(client)
    assert r.status_code == 200
    body = r.json()
    assert body["batch_id"] == "B1" and body["seal_id"] == "SEAL-1"
    assert body["evidence"] == sealed["evidence"]
    assert body["evidence_digest"] == sealed["evidence_digest"]
    assert body["current_evidence_digest"] == sealed["evidence_digest"]
    assert body["consistent"] is True


def test_recall_after_later_revisions_shows_drift_but_seal_unchanged(client):
    _complete_batch(client)
    sealed = _seal(client).json()

    # Later revisions only change the live result, never the stored seal.
    _post(client, _event("v3", "v2", 30, 40, value=3.0))
    _post(client, _event("w", "v3", 0, 5, withdrawn=True, value=None, key="kw"))

    body = _recall(client).json()
    assert body["consistent"] is False
    assert body["current_evidence_digest"] != body["evidence_digest"]
    assert body["evidence"] == sealed["evidence"]
    assert body["evidence_digest"] == sealed["evidence_digest"]
    # The live batch result reflects the new revisions.
    live = client.get("/api/v1/batches/B1/result").json()
    assert live["revisions_total"] == 4
    assert live["timeline_hash"] != sealed["timeline_hash"]


def test_recall_unknown_seal_404(client):
    assert _recall(client).status_code == 404
    _complete_batch(client)
    assert _recall(client).status_code == 404
    assert "'SEAL-1'" in _recall(client).json()["detail"]
    # Sealing another id does not make this one known.
    _seal(client, seal_id="SEAL-2")
    assert _recall(client).status_code == 404
    assert _recall(client, seal_id="SEAL-2").status_code == 200


def test_second_seal_after_drift_uses_new_hash(client):
    _complete_batch(client)
    first = _seal(client, seal_id="S-old").json()
    _post(client, _event("v3", "v2", 30, 40, value=3.0))
    second = _seal(client, seal_id="S-new").json()
    assert second["status"] == "sealed"
    assert second["evidence_digest"] != first["evidence_digest"]
    assert len(second["evidence"]["revisions"]) == 3
    # The old seal now shows drift; the new one is consistent.
    assert _recall(client, seal_id="S-old").json()["consistent"] is False
    assert _recall(client, seal_id="S-new").json()["consistent"] is True


def test_seals_are_scoped_per_batch(client):
    _complete_batch(client, batch="B1")
    _complete_batch(client, batch="B2")
    r1 = _seal(client, batch="B1")
    r2 = _seal(client, batch="B2")
    assert r1.status_code == r2.status_code == 200
    # Same seal id in two batches is fine; digests differ (batch-bound).
    assert r1.json()["evidence_digest"] != r2.json()["evidence_digest"]
    assert _recall(client, batch="B1").json()["consistent"] is True
    assert _recall(client, batch="B2").json()["consistent"] is True


def test_reset_clears_seals(client):
    _complete_batch(client)
    _seal(client)
    r = client.post("/admin/reset")
    assert r.json()["seals_removed"] == 1
    assert _recall(client).status_code == 404
    assert client.get("/api/v1/batches/B1/result").status_code == 404


def test_seal_digest_deterministic_across_arrival_order(client):
    events = [
        _event("v1", "ROOT", 0, 100, value=1.0, key="k1"),
        _event("v2", "v1", 10, 20, value=2.0, key="k2"),
        _event("w", "v2", 30, 40, withdrawn=True, value=None, key="k3"),
    ]
    digests = []
    for order in (events, list(reversed(events)), [events[1], events[2], events[0]]):
        client.post("/admin/reset")
        for e in order:
            assert _post(client, e).status_code == 200
        r = _seal(client)
        assert r.status_code == 200
        digests.append(r.json()["evidence_digest"])
    assert digests[0] == digests[1] == digests[2]


def test_openapi_lists_seal_endpoints(client):
    spec = client.get("/openapi.json").json()
    post = spec["paths"]["/api/v1/batches/{batch_id}/seals"]["post"]
    ref = post["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
    assert ref.endswith("/SealResponse")
    get = spec["paths"]["/api/v1/batches/{batch_id}/seals/{seal_id}"]["get"]
    ref = get["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
    assert ref.endswith("/SealRecallResponse")
    props = spec["components"]["schemas"]["SealRequest"]["properties"]
    assert {"batch_id", "seal_id", "timeline_hash"} <= set(props)
