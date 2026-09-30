"""End-to-end API behavior for the device-release endpoint."""


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


def _release(client, sensors, batch="B1", **overrides):
    body = {
        "window_start_ns": 0,
        "window_end_ns": 100,
        "min_duration_ns": 10,
        "sensors": sensors,
    }
    body.update(overrides)
    return client.post(f"/api/v1/batches/{batch}/release", json=body)


def _req(sensor="S1", lo=0.0, hi=10.0, unit="mm"):
    return {"sensor_id": sensor, "min_value": lo, "max_value": hi, "expected_unit": unit}


def test_release_full_window_released(client):
    _post(client, _event("r1", "ROOT", 0, 100, value=5.0, sensor="S1", key="k1"))
    _post(client, _event("r2", "ROOT", 0, 100, value=2.0, sensor="S2", key="k2"))
    r = _release(client, [_req("S1", 0, 10), _req("S2", 0, 10)])
    assert r.status_code == 200
    body = r.json()
    assert body["batch_id"] == "B1"
    assert (body["window_start_ns"], body["window_end_ns"], body["min_duration_ns"]) == (0, 100, 10)
    assert body["status"] == "released"
    assert body["released_intervals"] == [
        {"start_ns": 0, "end_ns": 100, "duration_ns": 100}
    ]
    assert body["unreleased_intervals"] == []
    assert len(body["input_hash"]) == 64


def test_release_blocked_span_is_unreleased_with_backfill_hint(client):
    _post(client, _event("r1", "ROOT", 0, 100, value=5.0, sensor="S1", key="k1"))
    _post(client, _event("r2", "ROOT", 0, 100, value=5.0, sensor="S2", key="k2"))
    _post(client, _event("orphan", "ghost", 40, 60, value=99.0, sensor="S1", key="ko"))
    body = _release(client, [_req("S1"), _req("S2")], min_duration_ns=20).json()
    assert body["status"] == "released"
    assert [(i["start_ns"], i["end_ns"]) for i in body["released_intervals"]] == [
        (0, 40), (60, 100)
    ]
    (span,) = body["unreleased_intervals"]
    assert span["qualified"] is False
    reason = span["sensors"][0]
    assert reason["sensor_id"] == "S1" and reason["reason"] == "blocked"
    assert reason["missing_ancestors"] == ["ghost"]


def test_release_reason_kinds_are_listed_per_sensor(client):
    _post(client, _event("root", "ROOT", 0, 80, value=5.0, sensor="S1", key="kr"))
    _post(client, _event("o", "root", 20, 40, value=99.0, sensor="S1", key="ko"))
    _post(client, _event("w", "o", 40, 60, withdrawn=True, value=None, sensor="S1", key="kw"))
    _post(client, _event("f", "w", 60, 80, value=5.0, unit="C", sensor="S1", key="kf"))
    _post(client, _event("r2", "ROOT", 0, 100, value=5.0, sensor="S2", key="k2"))
    body = _release(client, [_req("S1"), _req("S2")]).json()
    assert [(i["start_ns"], i["end_ns"]) for i in body["released_intervals"]] == [(0, 20)]
    assert [
        (i["start_ns"], i["end_ns"], i["sensors"][0]["reason"])
        for i in body["unreleased_intervals"]
    ] == [
        (20, 40, "out_of_bounds"),
        (40, 60, "withdrawn"),
        (60, 80, "unit_mismatch"),
        (80, 100, "uncovered"),
    ]
    unit_span = body["unreleased_intervals"][2]["sensors"][0]
    assert unit_span["observed_units"] == ["C"] and unit_span["unit"] == "C"
    oob_span = body["unreleased_intervals"][0]["sensors"][0]
    assert oob_span["observed_min_value"] == 99.0 == oob_span["observed_max_value"]


def test_release_qualifying_run_too_short_is_marked(client):
    _post(client, _event("r1", "ROOT", 0, 100, value=99.0, sensor="S1", key="k1"))
    _post(client, _event("ok", "r1", 40, 60, value=5.0, sensor="S1", key="kok"))
    _post(client, _event("r2", "ROOT", 0, 100, value=5.0, sensor="S2", key="k2"))
    body = _release(client, [_req("S1"), _req("S2")], min_duration_ns=30).json()
    assert body["status"] == "not_released"
    assert body["released_intervals"] == []
    short = next(i for i in body["unreleased_intervals"] if i["start_ns"] == 40)
    assert short["qualified"] is True and short["too_short"] is True
    assert short["duration_ns"] == 20 and short["sensors"] == []


def test_release_half_open_boundary_and_window_clipping(client):
    _post(client, _event("r1", "ROOT", 0, 100, value=5.0, sensor="S1", key="k1"))
    _post(client, _event("r2", "ROOT", 0, 100, value=5.0, sensor="S2", key="k2"))
    # End exactly on window_end is excluded; here the whole window qualifies.
    body = _release(
        client, [_req("S1"), _req("S2")],
        window_start_ns=10, window_end_ns=90, min_duration_ns=80,
    ).json()
    assert body["released_intervals"] == [
        {"start_ns": 10, "end_ns": 90, "duration_ns": 80}
    ]


def test_release_unknown_batch_or_sensor_404(client):
    r = _release(client, [_req("S1"), _req("S2")])
    assert r.status_code == 404 and "B1" in r.json()["detail"]
    _post(client, _event("r1", "ROOT", 0, 100, value=5.0, sensor="S1", key="k1"))
    r = _release(client, [_req("S1"), _req("S2")])
    assert r.status_code == 404
    assert "S2" in r.json()["detail"] and "B1" in r.json()["detail"]


def test_release_invalid_params_422(client):
    _post(client, _event("r1", "ROOT", 0, 100, value=5.0, sensor="S1", key="k1"))
    _post(client, _event("r2", "ROOT", 0, 100, value=5.0, sensor="S2", key="k2"))
    good = [_req("S1"), _req("S2")]

    # Inverted / empty query window.
    assert _release(client, good, window_start_ns=100, window_end_ns=100).status_code == 422
    assert _release(client, good, window_start_ns=90, window_end_ns=10).status_code == 422
    # Non-positive minimum duration.
    assert _release(client, good, min_duration_ns=0).status_code == 422
    # Empty sensor list and duplicate sensor ids.
    assert _release(client, []).status_code == 422
    assert _release(client, [_req("S1"), _req("S1")]).status_code == 422
    # Inverted / non-finite bounds per sensor.
    assert _release(client, [_req("S1", lo=10.0, hi=0.0), _req("S2")]).status_code == 422
    for raw in ("NaN", "Infinity", "-Infinity"):
        payload = (
            '{"window_start_ns":0,"window_end_ns":100,"min_duration_ns":10,'
            f'"sensors":[{{"sensor_id":"S1","min_value":{raw},"max_value":10.0,'
            '"expected_unit":"mm"},'
            '{"sensor_id":"S2","min_value":0.0,"max_value":10.0,"expected_unit":"mm"}]}'
        )
        r = client.post(
            "/api/v1/batches/B1/release",
            content=payload,
            headers={"content-type": "application/json"},
        )
        assert r.status_code == 422, raw
    # Missing required fields, unknown extra fields and unknown unit issues.
    r = client.post("/api/v1/batches/B1/release", json={"window_start_ns": 0})
    assert r.status_code == 422
    body = {
        "window_start_ns": 0, "window_end_ns": 100, "min_duration_ns": 10,
        "sensors": good, "extra": 1,
    }
    assert client.post("/api/v1/batches/B1/release", json=body).status_code == 422


def test_release_is_read_only(client):
    _post(client, _event("r1", "ROOT", 0, 100, value=5.0, sensor="S1", key="k1"))
    before = client.get("/api/v1/batches/B1/result").json()
    _release(client, [_req("S1")])
    after = client.get("/api/v1/batches/B1/result").json()
    assert before == after


def test_release_deterministic_across_arrival_order(client):
    events = [
        _event("r1", "ROOT", 0, 100, value=5.0, sensor="S1", key="k1"),
        _event("orphan", "ghost", 40, 60, value=99.0, sensor="S1", key="ko"),
        _event("r2", "ROOT", 0, 100, value=5.0, sensor="S2", key="k2"),
    ]

    def run(order):
        client.post("/admin/reset")
        for event in order:
            assert _post(client, event).status_code == 200
        return _release(client, [_req("S1"), _req("S2")]).json()

    first = run(events)
    assert run(list(reversed(events))) == first
    assert run([events[1], events[2], events[0]]) == first


def test_openapi_lists_release_endpoint(client):
    spec = client.get("/openapi.json").json()
    path = spec["paths"]["/api/v1/batches/{batch_id}/release"]["post"]
    ref = path["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
    assert ref.endswith("/ReleaseResponse")
    props = spec["components"]["schemas"]["ReleaseRequest"]["properties"]
    assert {
        "window_start_ns", "window_end_ns", "sensors", "min_duration_ns"
    } <= set(props)
