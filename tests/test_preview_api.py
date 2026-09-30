"""End-to-end behavior of the non-mutating submission preview endpoint."""


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


def _post(client, event, batch="B1"):
    return client.post(f"/api/v1/batches/{batch}/events", json=event)


def _preview(client, event, batch="B1"):
    return client.post(f"/api/v1/batches/{batch}/events:preview", json=event)


# ---------------------------------------------------------------------------
# Submittability, non-mutation and empty batches
# ---------------------------------------------------------------------------


def test_preview_nonexistent_batch_is_previewed_as_empty(client):
    event = _event("v1", "ROOT", 0, 100, value=1.0, unit="C", batch="NEW")
    r = _preview(client, event, batch="NEW")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "submittable"
    # Before snapshot is the canonical empty-batch result.
    assert body["before"]["sensors"] == []
    assert body["before"]["gaps"] == []
    assert body["before"]["revisions_total"] == 0
    assert body["before"]["complete"] is False
    assert body["before_timeline_hash"] != body["after_timeline_hash"]
    [interval] = body["changed_intervals"]
    assert (interval["start_ns"], interval["end_ns"], interval["kind"]) == (
        0,
        100,
        "added",
    )
    assert interval["before"] is None
    assert interval["after"] == {"state": "calibrated", "value": 1.0, "unit": "C"}
    assert body["gaps_added"] == [] and body["gaps_removed"] == []
    # Nothing was written: the batch still does not exist and the idempotency
    # key was not occupied.
    assert client.get("/api/v1/batches/NEW/result").status_code == 404
    assert _preview(client, event, batch="NEW").json()["status"] == "submittable"


def test_preview_does_not_create_batch_or_consume_idempotency_key(client):
    event = _event("v2", "v1", 10, 20, value=2.0)
    assert _preview(client, event).status_code == 200
    # Repeated previews stay submittable; the key becomes used only on submit.
    assert _preview(client, event).json()["status"] == "submittable"
    submitted = _post(client, event)
    assert submitted.json()["status"] == "accepted"


def test_preview_after_matches_real_submission_result(client):
    _post(client, _event("r", "ROOT", 0, 100, value=5.0))
    before = client.get("/api/v1/batches/B1/result").json()
    candidate = _event("c", "r", 20, 80, value=6.0, key="kc")
    body = _preview(client, candidate).json()
    actual = _post(client, candidate).json()["result"]
    assert body["before"] == before
    assert body["after"] == actual


def test_preview_exact_duplicate_reports_duplicate_and_unchanged_hashes(client):
    event = _event("v1", "ROOT", 0, 10, value=1.0)
    _post(client, event)
    body = _preview(client, event).json()
    assert body["status"] == "duplicate"
    assert body["before_timeline_hash"] == body["after_timeline_hash"]
    assert body["before"] == body["after"]
    assert body["changed_intervals"] == []
    assert body["gaps_added"] == [] and body["gaps_removed"] == []


def test_preview_same_revision_same_content_new_key_is_duplicate(client):
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, key="k1"))
    body = _preview(client, _event("v1", "ROOT", 0, 10, value=1.0, key="k2")).json()
    assert body["status"] == "duplicate"


# ---------------------------------------------------------------------------
# 409 / 422 semantics, identical to ingest
# ---------------------------------------------------------------------------


def test_preview_same_key_different_payload_conflicts_409(client):
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, key="same"))
    r = _preview(client, _event("v2", "ROOT", 0, 10, value=2.0, key="same"))
    assert r.status_code == 409
    assert "same" in r.json()["detail"]


def test_preview_same_revision_different_content_conflicts_409(client):
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, key="k1"))
    r = _preview(client, _event("v1", "ROOT", 0, 10, value=9.0, key="k2"))
    assert r.status_code == 409


def test_preview_conflicting_preview_does_not_occupy_key(client):
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, key="shared"))
    conflict = _event("v2", "ROOT", 0, 10, value=2.0, key="shared")
    assert _preview(client, conflict).status_code == 409
    # The original key mapping is intact and an exact replay is still duplicate.
    assert _preview(
        client, _event("v1", "ROOT", 0, 10, value=1.0, key="shared")
    ).json()["status"] == "duplicate"
    # And the original event still submits as duplicate (no state corruption).
    assert _post(
        client, _event("v1", "ROOT", 0, 10, value=1.0, key="shared")
    ).json()["status"] == "duplicate"


def test_preview_invalid_payload_422(client):
    bad_interval = _event("r", "ROOT", 50, 40, value=1.0)
    assert _preview(client, bad_interval).status_code == 422
    missing_value = _event("r2", "ROOT", 0, 10)
    assert _preview(client, missing_value).status_code == 422
    reserved = _event("ROOT", "ROOT", 0, 10, value=1.0)
    assert _preview(client, reserved).status_code == 422
    extra = _event("r3", "ROOT", 0, 10, value=1.0)
    extra["unexpected"] = 1
    assert _preview(client, extra).status_code == 422


def test_preview_path_body_batch_mismatch_422(client):
    event = _event("r", "ROOT", 0, 10, value=1.0, batch="OTHER")
    r = client.post("/api/v1/batches/B1/events:preview", json=event)
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Changed intervals: effective state/value/unit only
# ---------------------------------------------------------------------------


def test_preview_changed_interval_for_value_overwrite(client):
    _post(client, _event("r", "ROOT", 0, 100, value=5.0))
    body = _preview(client, _event("c", "r", 40, 60, value=9.0, key="kc")).json()
    [interval] = body["changed_intervals"]
    assert interval["start_ns"] == 40 and interval["end_ns"] == 60
    assert interval["kind"] == "changed"
    assert interval["before"] == {"state": "calibrated", "value": 5.0, "unit": "mm"}
    assert interval["after"] == {"state": "calibrated", "value": 9.0, "unit": "mm"}


def test_preview_unit_change_is_a_value_change_interval(client):
    _post(client, _event("r", "ROOT", 0, 100, value=5.0, unit="mm"))
    body = _preview(
        client, _event("c", "r", 40, 60, value=5.0, unit="C", key="kc")
    ).json()
    [interval] = body["changed_intervals"]
    assert interval["kind"] == "changed"
    assert interval["before"]["unit"] == "mm" and interval["after"]["unit"] == "C"


def test_preview_provenance_only_change_is_not_reported(client):
    _post(client, _event("r", "ROOT", 0, 100, value=5.0))
    # Descendant rewrites the whole span with the identical effective payload;
    # only the source revision changes, which is not an effective change.
    body = _preview(client, _event("c", "r", 0, 100, value=5.0, key="kc")).json()
    assert body["status"] == "submittable"
    assert body["changed_intervals"] == []


def test_preview_withdrawn_tombstone_is_state_change(client):
    _post(client, _event("r", "ROOT", 0, 100, value=5.0))
    body = _preview(
        client, _event("w", "r", 30, 70, withdrawn=True, value=None, key="kw")
    ).json()
    [interval] = body["changed_intervals"]
    assert (interval["start_ns"], interval["end_ns"], interval["kind"]) == (
        30,
        70,
        "changed",
    )
    assert interval["before"] == {"state": "calibrated", "value": 5.0, "unit": "mm"}
    assert interval["after"] == {"state": "withdrawn", "value": None, "unit": None}


def test_preview_descendant_writeback_after_withdrawal(client):
    _post(client, _event("r", "ROOT", 0, 100, value=5.0))
    _post(client, _event("w", "r", 0, 100, withdrawn=True, value=None))
    body = _preview(client, _event("fix", "w", 10, 90, value=7.0, key="kfix")).json()
    [interval] = body["changed_intervals"]
    assert (interval["start_ns"], interval["end_ns"]) == (10, 90)
    assert interval["kind"] == "changed"
    assert interval["before"] == {"state": "withdrawn", "value": None, "unit": None}
    assert interval["after"] == {"state": "calibrated", "value": 7.0, "unit": "mm"}


def test_preview_added_coverage_outside_existing_span(client):
    _post(client, _event("r", "ROOT", 0, 10, value=5.0))
    body = _preview(client, _event("c", "r", 10, 20, value=6.0, key="kc")).json()
    [interval] = body["changed_intervals"]
    assert (interval["start_ns"], interval["end_ns"], interval["kind"]) == (
        10,
        20,
        "added",
    )
    assert interval["before"] is None


# ---------------------------------------------------------------------------
# Gaps: blocked candidates and backfills activating descendants
# ---------------------------------------------------------------------------


def test_preview_blocked_candidate_reports_gap_even_without_timeline_change(client):
    _post(client, _event("r", "ROOT", 0, 100, value=5.0))
    body = _preview(
        client, _event("orphan", "ghost", 40, 60, value=9.0, key="ko")
    ).json()
    # The blocked revision contributes nothing to the folded timeline...
    assert body["changed_intervals"] == []
    assert body["before_timeline_hash"] == body["after_timeline_hash"]
    # ...but the new backfillable gap must still be surfaced.
    [gap] = body["gaps_added"]
    assert gap["missing_revision_id"] == "ghost"
    assert gap["awaited_by"] == ["orphan"]
    assert gap["referenced_from"] == "orphan"
    assert body["after"]["revisions_blocked"] == 1
    assert body["gaps_removed"] == []


def test_preview_blocked_candidate_on_empty_batch(client):
    body = _preview(client, _event("v2", "v1", 100, 200, value=3.5, key="e2")).json()
    assert body["status"] == "submittable"
    assert body["changed_intervals"] == []
    [gap] = body["gaps_added"]
    assert gap["missing_revision_id"] == "v1"
    assert gap["awaited_by"] == ["v2"]
    assert body["after"]["sensors"][0]["segments"] == []


def test_preview_backfill_removes_gap_and_activates_descendants(client):
    _post(client, _event("v2", "v1", 100, 200, value=3.5, key="e2"))
    body = _preview(
        client,
        _event("v1", "ROOT", 0, 300, value=1.0, unit="C", key="e1"),
    ).json()
    [removed] = body["gaps_removed"]
    assert removed["missing_revision_id"] == "v1"
    assert removed["awaited_by"] == ["v2"]
    assert body["gaps_added"] == []
    # The previously blocked descendant now participates in the fold.
    kinds = [(i["start_ns"], i["end_ns"], i["kind"]) for i in body["changed_intervals"]]
    assert kinds == [(0, 100, "added"), (100, 200, "added"), (200, 300, "added")]
    middle = body["changed_intervals"][1]
    assert middle["after"] == {"state": "calibrated", "value": 3.5, "unit": "mm"}
    assert body["after"]["complete"] is True
    assert body["after"]["revisions_grounded"] == 2
    # Still only a preview: the real batch remains blocked.
    real = client.get("/api/v1/batches/B1/result").json()
    assert real["revisions_blocked"] == 1


def test_preview_partial_backfill_moves_gap_up_the_chain(client):
    # v3 -> v2 (missing); uploading v2 removes gap "v2" but exposes gap "v1".
    _post(client, _event("v3", "v2", 0, 10, value=3.0, key="k3"))
    body = _preview(client, _event("v2", "v1", 0, 20, value=2.0, key="k2")).json()
    assert [g["missing_revision_id"] for g in body["gaps_removed"]] == ["v2"]
    [added] = body["gaps_added"]
    assert added["missing_revision_id"] == "v1"
    assert added["awaited_by"] == ["v2"]
    # Nothing is grounded yet, so the timeline does not change.
    assert body["changed_intervals"] == []


def test_preview_full_chain_backfill_activates_all_descendants(client):
    _post(client, _event("v3", "v2", 0, 10, value=3.0, key="k3"))
    _post(client, _event("v2", "v1", 0, 20, value=2.0, key="k2"))
    body = _preview(client, _event("v1", "ROOT", 0, 30, value=1.0, key="k1")).json()
    assert [g["missing_revision_id"] for g in body["gaps_removed"]] == ["v1"]
    assert body["gaps_added"] == []
    assert body["after"]["revisions_grounded"] == 3
    assert body["after"]["complete"] is True
    assert [(i["start_ns"], i["end_ns"], i["after"]["value"]) for i in body["changed_intervals"]] == [
        (0, 10, 3.0),
        (10, 20, 2.0),
        (20, 30, 1.0),
    ]


def test_preview_gaps_are_scoped_to_the_candidate_sensor(client):
    _post(client, _event("sy", "ghost", 0, 10, value=2.0, sensor="SY", key="ksy"))
    body = _preview(
        client, _event("sx", "ROOT", 0, 10, value=1.0, sensor="SX", key="ksx")
    ).json()
    # SX's own root adds coverage; SY's pre-existing gap is reported in neither
    # delta list.
    assert [i["kind"] for i in body["changed_intervals"]] == ["added"]
    assert body["gaps_added"] == [] and body["gaps_removed"] == []


def test_preview_cycle_creation_removes_gap_without_timeline_change(client):
    # a -> missing b; submitting b -> a forms a cycle: both become invalid and
    # the "b" gap disappears.
    _post(client, _event("a", "b", 0, 10, value=1.0, key="ka"))
    body = _preview(client, _event("b", "a", 0, 10, value=2.0, key="kb")).json()
    assert [g["missing_revision_id"] for g in body["gaps_removed"]] == ["b"]
    assert body["gaps_added"] == []
    assert body["after"]["revisions_invalid"] == 2
    assert body["changed_intervals"] == []


# ---------------------------------------------------------------------------
# Hashes, snapshots and OpenAPI
# ---------------------------------------------------------------------------


def test_preview_hashes_match_full_batch_result_hashes(client):
    first = _post(client, _event("r", "ROOT", 0, 100, value=5.0)).json()["result"]
    body = _preview(client, _event("c", "r", 20, 80, value=6.0, key="kc")).json()
    assert body["before_timeline_hash"] == first["timeline_hash"]
    # The "after" hash equals the hash the real subsequent submission returns.
    actual = _post(client, _event("c", "r", 20, 80, value=6.0, key="kc")).json()
    assert body["after_timeline_hash"] == actual["result"]["timeline_hash"]


def test_preview_empty_batch_before_hash_is_canonical(client):
    import hashlib
    import json

    expected = hashlib.sha256(
        json.dumps([], separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    body = _preview(
        client, _event("v1", "ROOT", 0, 10, value=1.0, batch="GONE"), batch="GONE"
    ).json()
    assert body["before_timeline_hash"] == expected


def test_preview_based_on_consistent_snapshot_under_concurrency(client):
    import threading

    _post(client, _event("r", "ROOT", 0, 1000, value=5.0))
    errors: list[Exception] = []

    def hammer() -> None:
        for i in range(40):
            try:
                _preview(client, _event(f"p{i}", "r", i, i + 1, value=float(i), key=f"kp{i}"))
                _post(client, _event(f"a{i}", "r", i, i + 1, value=float(i), key=f"ka{i}"))
            except Exception as exc:  # pragma: no cover - failure recorder
                errors.append(exc)

    threads = [threading.Thread(target=hammer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    # Every real submission landed exactly once; previews landed zero times.
    result = client.get("/api/v1/batches/B1/result").json()
    assert result["revisions_total"] == 1 + 40
    assert result["revisions_grounded"] == 1 + 40


def test_openapi_lists_preview_endpoint(client):
    spec = client.get("/openapi.json").json()
    path = spec["paths"]["/api/v1/batches/{batch_id}/events:preview"]["post"]
    ref = path["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
    assert ref.endswith("/PreviewResponse")
    props = spec["components"]["schemas"]["PreviewResponse"]["properties"]
    assert {
        "status",
        "before_timeline_hash",
        "after_timeline_hash",
        "before",
        "after",
        "changed_intervals",
        "gaps_added",
        "gaps_removed",
    } <= set(props)
    interval_props = spec["components"]["schemas"]["PreviewChangeInterval"]["properties"]
    assert {"start_ns", "end_ns", "kind", "before", "after"} <= set(interval_props)
