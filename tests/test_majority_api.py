"""End-to-end API behavior for the trusted-majority quorum endpoint."""


def _event(rev, parent, start=0, end=100, value=None, unit="C", withdrawn=False,
           key=None, batch="B1", sensor="S1"):
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


def _majority(client, sensors=("S1", "S2", "S3"), batch="B1", *,
              unit="C", q=2, diff=0.5, start=0, end=100):
    body = {
        "window_start_ns": start,
        "window_end_ns": end,
        "sensors": [{"sensor_id": s} for s in sensors],
        "expected_unit": unit,
        "q": q,
        "max_value_difference": diff,
    }
    return client.post(f"/api/v1/batches/{batch}/majority", json=body)


def test_majority_full_window_qualified(client):
    _post(client, _event("r1", "ROOT", value=10.0, sensor="S1", key="k1"))
    _post(client, _event("r2", "ROOT", value=10.1, sensor="S2", key="k2"))
    _post(client, _event("r3", "ROOT", value=10.2, sensor="S3", key="k3"))
    r = _majority(client)
    assert r.status_code == 200
    body = r.json()
    assert body["batch_id"] == "B1"
    assert body["sensors"] == ["S1", "S2", "S3"]
    assert (body["window_start_ns"], body["window_end_ns"]) == (0, 100)
    assert (body["q"], body["max_value_difference"], body["expected_unit"]) == (
        2, 0.5, "C"
    )
    assert body["status"] == "qualified"
    (span,) = body["intervals"]
    assert (span["start_ns"], span["end_ns"], span["duration_ns"], span["verdict"]) == (
        0, 100, 100, "qualified"
    )
    assert [m["sensor_id"] for m in span["members"]] == ["S1", "S2", "S3"]
    assert span["non_voting_sensors"] == []
    assert span["missing_ancestors"] == []
    assert len(body["input_hash"]) == 64


def test_majority_pending_backfill_lists_missing_ancestor(client):
    _post(client, _event("r1", "ROOT", value=10.0, sensor="S1", key="k1"))
    _post(client, _event("r2", "ROOT", value=10.1, sensor="S2", key="k2"))
    _post(client, _event("r3", "ROOT", value=10.2, sensor="S3", key="k3"))
    _post(client, _event("b", "ghost", start=40, end=60, value=9.0,
                         sensor="S3", key="kb"))
    body = _majority(client, q=3).json()
    assert body["status"] == "qualified"
    assert [(i["start_ns"], i["end_ns"], i["verdict"]) for i in body["intervals"]] == [
        (0, 40, "qualified"),
        (40, 60, "pending_backfill"),
        (60, 100, "qualified"),
    ]
    pending = body["intervals"][1]
    assert [m["sensor_id"] for m in pending["members"]] == ["S1", "S2"]
    (reason,) = pending["non_voting_sensors"]
    assert reason["sensor_id"] == "S3" and reason["reason"] == "blocked"
    assert reason["missing_ancestors"] == ["ghost"]
    assert pending["missing_ancestors"] == ["ghost"]


def test_majority_divergent_voter_is_unqualified_with_reason(client):
    _post(client, _event("r1", "ROOT", value=10.0, sensor="S1", key="k1"))
    _post(client, _event("r2", "ROOT", value=30.0, sensor="S2", key="k2"))
    _post(client, _event("r3", "ROOT", value=50.0, sensor="S3", key="k3"))
    body = _majority(client).json()
    assert body["status"] == "unqualified"
    (span,) = body["intervals"]
    assert span["verdict"] == "unqualified"
    assert [m["sensor_id"] for m in span["members"]] == ["S1"]
    assert sorted(
        (r["sensor_id"], r["reason"]) for r in span["non_voting_sensors"]
    ) == [("S2", "value_divergent"), ("S3", "value_divergent")]


def test_majority_unit_mismatch_withdrawn_uncovered_do_not_vote(client):
    _post(client, _event("r1", "ROOT", value=10.0, sensor="S1", key="k1"))
    _post(client, _event("r2", "ROOT", value=10.0, unit="F", sensor="S2", key="k2"))
    _post(client, _event("w", "ROOT", withdrawn=True, value=None,
                         sensor="S3", key="kw"))
    _post(client, _event("r4", "ROOT", start=200, end=300, value=10.0,
                         sensor="S4", key="k4"))
    body = _majority(client, sensors=("S1", "S2", "S3", "S4"), q=3).json()
    (span,) = body["intervals"]
    assert span["verdict"] == "unqualified"
    reasons = {r["sensor_id"]: r["reason"] for r in span["non_voting_sensors"]}
    assert reasons == {"S2": "unit_mismatch", "S3": "withdrawn", "S4": "uncovered"}


def test_majority_unknown_batch_or_sensor_404(client):
    r = _majority(client)
    assert r.status_code == 404 and "B1" in r.json()["detail"]
    for rev, sensor in (("r1", "S1"), ("r2", "S2")):
        _post(client, _event(rev, "ROOT", value=10.0, sensor=sensor, key=f"k{sensor}"))
    r = _majority(client)
    assert r.status_code == 404
    assert "S3" in r.json()["detail"] and "B1" in r.json()["detail"]


def test_majority_quorum_out_of_range_422(client):
    _post(client, _event("r1", "ROOT", value=10.0, sensor="S1", key="k1"))
    _post(client, _event("r2", "ROOT", value=10.0, sensor="S2", key="k2"))
    _post(client, _event("r3", "ROOT", value=10.0, sensor="S3", key="k3"))

    def body(q):
        return {
            "window_start_ns": 0,
            "window_end_ns": 100,
            "sensors": [{"sensor_id": "S1"}, {"sensor_id": "S2"}, {"sensor_id": "S3"}],
            "expected_unit": "C",
            "q": q,
            "max_value_difference": 0.5,
        }

    for bad_q in (0, 1, 4, -1):  # strict majority of 3 is 2; max is 3
        r = client.post("/api/v1/batches/B1/majority", json=body(bad_q))
        assert r.status_code == 422, bad_q
    # Boundary values are accepted.
    assert client.post("/api/v1/batches/B1/majority", json=body(2)).status_code == 200
    assert client.post("/api/v1/batches/B1/majority", json=body(3)).status_code == 200
    # Two sensors: strict majority is exactly 2.
    two = {
        "window_start_ns": 0,
        "window_end_ns": 100,
        "sensors": [{"sensor_id": "S1"}, {"sensor_id": "S2"}],
        "expected_unit": "C",
        "q": 1,
        "max_value_difference": 0.5,
    }
    assert client.post("/api/v1/batches/B1/majority", json=two).status_code == 422


def test_majority_invalid_params_422(client):
    _post(client, _event("r1", "ROOT", value=10.0, sensor="S1", key="k1"))
    _post(client, _event("r2", "ROOT", value=10.0, sensor="S2", key="k2"))
    good_sensors = [{"sensor_id": "S1"}, {"sensor_id": "S2"}]

    def payload(**overrides):
        base = {
            "window_start_ns": 0,
            "window_end_ns": 100,
            "sensors": good_sensors,
            "expected_unit": "C",
            "q": 2,
            "max_value_difference": 0.5,
        }
        base.update(overrides)
        return base

    # Fewer than two sensors, duplicates and an empty list.
    assert client.post(
        "/api/v1/batches/B1/majority", json=payload(sensors=[{"sensor_id": "S1"}])
    ).status_code == 422
    assert client.post(
        "/api/v1/batches/B1/majority", json=payload(sensors=[])
    ).status_code == 422
    assert client.post(
        "/api/v1/batches/B1/majority",
        json=payload(sensors=[{"sensor_id": "S1"}, {"sensor_id": "S1"}]),
    ).status_code == 422
    # Empty / inverted window and negative start.
    assert client.post(
        "/api/v1/batches/B1/majority", json=payload(window_start_ns=100, window_end_ns=100)
    ).status_code == 422
    assert client.post(
        "/api/v1/batches/B1/majority", json=payload(window_start_ns=90, window_end_ns=10)
    ).status_code == 422
    assert client.post(
        "/api/v1/batches/B1/majority", json=payload(window_start_ns=-1)
    ).status_code == 422
    # Negative / non-finite spread.
    assert client.post(
        "/api/v1/batches/B1/majority", json=payload(max_value_difference=-0.1)
    ).status_code == 422
    for raw in ("NaN", "Infinity", "-Infinity"):
        raw_body = (
            '{"window_start_ns":0,"window_end_ns":100,'
            '"sensors":[{"sensor_id":"S1"},{"sensor_id":"S2"}],'
            f'"expected_unit":"C","q":2,"max_value_difference":{raw}}}'
        )
        r = client.post(
            "/api/v1/batches/B1/majority",
            content=raw_body,
            headers={"content-type": "application/json"},
        )
        assert r.status_code == 422, raw
    # Missing required fields, unknown extra fields and a blank unit.
    assert client.post(
        "/api/v1/batches/B1/majority", json={"window_start_ns": 0}
    ).status_code == 422
    assert client.post(
        "/api/v1/batches/B1/majority", json=payload(extra=1)
    ).status_code == 422
    assert client.post(
        "/api/v1/batches/B1/majority", json=payload(expected_unit="")
    ).status_code == 422


def test_majority_is_read_only(client):
    _post(client, _event("r1", "ROOT", value=10.0, sensor="S1", key="k1"))
    _post(client, _event("r2", "ROOT", value=10.1, sensor="S2", key="k2"))
    before = client.get("/api/v1/batches/B1/result").json()
    assert _majority(client, sensors=("S1", "S2")).status_code == 200
    after = client.get("/api/v1/batches/B1/result").json()
    assert before == after


def test_majority_deterministic_across_arrival_order(client):
    events = [
        _event("r1", "ROOT", value=10.0, sensor="S1", key="k1"),
        _event("r2", "ROOT", value=10.2, sensor="S2", key="k2"),
        _event("r3", "ROOT", value=30.0, sensor="S3", key="k3"),
        _event("b", "ghost", start=40, end=60, value=9.0, sensor="S2", key="kb"),
    ]

    def run(order):
        client.post("/admin/reset")
        for event in order:
            assert _post(client, event).status_code == 200
        return _majority(client).json()

    first = run(events)
    assert run(list(reversed(events))) == first
    assert run([events[2], events[0], events[3], events[1]]) == first


def test_majority_existing_endpoints_remain_compatible(client):
    # Submission, preview, result, sample, compare, quality-gate and release
    # all keep working alongside the new endpoint.
    _post(client, _event("r1", "ROOT", 0, 100, value=5.0, sensor="S1", key="k1"))
    assert client.get("/api/v1/batches/B1/result").status_code == 200
    assert client.get(
        "/api/v1/batches/B1/sensors/S1/sample?timestamp_ns=50"
    ).status_code == 200
    assert client.post(
        "/api/v1/batches/B1/quality-gate",
        json={"sensor_id": "S1", "min_value": 0.0, "max_value": 10.0,
              "min_duration_ns": 10, "missing_policy": "ignore"},
    ).status_code == 200
    _post(client, _event("r2", "ROOT", 0, 100, value=5.0, sensor="S2", key="k2"))
    assert client.post(
        "/api/v1/batches/B1/release",
        json={
            "window_start_ns": 0, "window_end_ns": 100, "min_duration_ns": 10,
            "sensors": [
                {"sensor_id": "S1", "min_value": 0.0, "max_value": 10.0,
                 "expected_unit": "C"},
                {"sensor_id": "S2", "min_value": 0.0, "max_value": 10.0,
                 "expected_unit": "C"},
            ],
        },
    ).status_code == 200
    assert _majority(client, sensors=("S1", "S2")).status_code == 200


def test_openapi_lists_majority_endpoint(client):
    spec = client.get("/openapi.json").json()
    path = spec["paths"]["/api/v1/batches/{batch_id}/majority"]["post"]
    ref = path["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
    assert ref.endswith("/MajorityResponse")
    props = spec["components"]["schemas"]["MajorityRequest"]["properties"]
    assert {
        "window_start_ns", "window_end_ns", "sensors",
        "expected_unit", "q", "max_value_difference",
    } <= set(props)
