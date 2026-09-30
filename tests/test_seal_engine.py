"""Engine unit tests: seal evidence capture and digest determinism."""

from app.engine import build_seal_evidence, hash_seal_evidence, revision_to_dict
from app.storage import BatchStore, StoredEvent


def _ev(rev, parent, start=0, end=10, value=1.0, withdrawn=False,
        batch="B", sensor="S", key=None):
    return StoredEvent(
        batch_id=batch,
        sensor_id=sensor,
        revision_id=rev,
        parent_id=parent,
        idempotency_key=key or f"k-{rev}",
        start_ns=start,
        end_ns=end,
        value=None if withdrawn else float(value),
        unit=None if withdrawn else "u",
        withdrawn=withdrawn,
    )


def _store(events):
    store = BatchStore()
    for e in events:
        store.events[e.revision_id] = e
        store.revision_sensor[e.revision_id] = e.sensor_id
        store.idempotency[e.idempotency_key] = "x"
    return store


def test_revision_to_dict_captures_full_record():
    event = _ev("v1", "ROOT", 5, 15, value=2.5, sensor="S1", key="k1")
    assert revision_to_dict(event) == {
        "sensor_id": "S1",
        "revision_id": "v1",
        "parent_id": "ROOT",
        "idempotency_key": "k1",
        "start_ns": 5,
        "end_ns": 15,
        "value": 2.5,
        "unit": "u",
        "withdrawn": False,
    }
    tombstone = revision_to_dict(_ev("w", "v1", withdrawn=True, key="kw"))
    assert tombstone["value"] is None
    assert tombstone["unit"] is None
    assert tombstone["withdrawn"] is True


def test_seal_evidence_sorted_and_independent_of_arrival_order():
    events = [
        _ev("v2", "v1", 10, 20, value=2.0, sensor="S1"),
        _ev("r1", "ROOT", 0, 100, value=1.0, sensor="S2"),
        _ev("v1", "ROOT", 0, 100, value=1.0, sensor="S1"),
    ]
    forward = build_seal_evidence("B", _store(events))
    shuffled = build_seal_evidence("B", _store([events[2], events[0], events[1]]))
    assert forward == shuffled
    assert [(r["sensor_id"], r["revision_id"]) for r in forward["revisions"]] == [
        ("S1", "v1"),
        ("S1", "v2"),
        ("S2", "r1"),
    ]
    assert forward["batch_id"] == "B"
    assert forward["result"]["complete"] is True
    assert hash_seal_evidence(forward) == hash_seal_evidence(shuffled)


def test_seal_digest_covers_revisions_and_folded_result():
    base = [_ev("v1", "ROOT", 0, 100, value=1.0)]
    digest = hash_seal_evidence(build_seal_evidence("B", _store(base)))
    assert len(digest) == 64

    # A changed revision value changes both the fold and the digest.
    changed_value = build_seal_evidence("B", _store([_ev("v1", "ROOT", 0, 100, value=9.0)]))
    assert hash_seal_evidence(changed_value) != digest

    # A revision that does not change the folded result (blocked orphan)
    # still changes the digest: the evidence covers all stored revisions.
    with_orphan = build_seal_evidence(
        "B", _store(base + [_ev("orphan", "ghost", 0, 10, value=5.0)])
    )
    base_result_hash = build_seal_evidence("B", _store(base))["result"]["timeline_hash"]
    assert with_orphan["result"]["timeline_hash"] == base_result_hash
    assert hash_seal_evidence(with_orphan) != digest

    # The digest is bound to the batch id.
    other_batch = build_seal_evidence("B2", _store([_ev("v1", "ROOT", 0, 100, value=1.0, batch="B2")]))
    assert hash_seal_evidence(other_batch) != digest


def test_seal_evidence_is_decoupled_from_live_store():
    store = _store([_ev("v1", "ROOT", 0, 100, value=1.0)])
    evidence = build_seal_evidence("B", store)
    digest = hash_seal_evidence(evidence)
    # Mutating the store afterwards must not leak into the captured evidence.
    store.events["v2"] = _ev("v2", "v1", 10, 20, value=2.0)
    assert len(evidence["revisions"]) == 1
    assert hash_seal_evidence(evidence) == digest
