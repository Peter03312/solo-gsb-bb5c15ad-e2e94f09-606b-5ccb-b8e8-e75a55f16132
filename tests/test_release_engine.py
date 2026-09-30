"""Engine unit tests: multi-sensor device-release sweep."""

from app.engine import ReleaseRequirement, evaluate_device_release
from app.storage import BatchStore, StoredEvent


def _ev(rev, sensor, parent, start=0, end=10, value=1.0, unit="u",
        withdrawn=False, batch="B", key=None):
    return StoredEvent(
        batch_id=batch,
        sensor_id=sensor,
        revision_id=rev,
        parent_id=parent,
        idempotency_key=key or f"k-{sensor}-{rev}",
        start_ns=start,
        end_ns=end,
        value=None if withdrawn else float(value),
        unit=None if withdrawn else unit,
        withdrawn=withdrawn,
    )


def _store(events):
    store = BatchStore()
    for e in events:
        store.events[e.revision_id] = e
        store.idempotency[e.idempotency_key] = "x"
    return store


def _reqs(*specs):
    # specs: (sensor_id, min_value, max_value, unit)
    return [ReleaseRequirement(s, lo, hi, unit) for s, lo, hi, unit in specs]


def _partition(doc):
    parts = [("rel", i["start_ns"], i["end_ns"]) for i in doc["released_intervals"]]
    parts += [
        ("unrel", i["start_ns"], i["end_ns"]) for i in doc["unreleased_intervals"]
    ]
    return sorted(parts, key=lambda p: p[1])


def test_release_all_sensors_qualifying_releases_full_window():
    store = _store([
        _ev("r1", "S1", "ROOT", 0, 100, value=2.0),
        _ev("r2", "S2", "ROOT", 0, 100, value=5.0),
    ])
    doc = evaluate_device_release("B", 0, 100, _reqs(("S1", 0, 3, "u"), ("S2", 4, 6, "u")), 10, store)
    assert doc["status"] == "released"
    assert doc["released_intervals"] == [
        {"start_ns": 0, "end_ns": 100, "duration_ns": 100}
    ]
    assert doc["unreleased_intervals"] == []


def test_release_one_failing_sensor_blocks_release_with_reason():
    store = _store([
        _ev("r1", "S1", "ROOT", 0, 100, value=9.0),  # out of bounds
        _ev("r2", "S2", "ROOT", 0, 100, value=5.0),
    ])
    doc = evaluate_device_release("B", 0, 100, _reqs(("S1", 0, 3, "u"), ("S2", 4, 6, "u")), 1, store)
    assert doc["status"] == "not_released"
    assert doc["released_intervals"] == []
    (span,) = doc["unreleased_intervals"]
    assert (span["start_ns"], span["end_ns"], span["qualified"]) == (0, 100, False)
    (reason,) = span["sensors"]
    assert reason["sensor_id"] == "S1" and reason["reason"] == "out_of_bounds"
    assert reason["observed_min_value"] == 9.0 and reason["observed_max_value"] == 9.0
    assert reason["value"] == 9.0 and reason["unit"] == "u"


def test_release_reason_kinds_withdrawn_uncovered_unit_oob():
    store = _store([
        _ev("root", "S1", "ROOT", 0, 80, value=1.0),
        _ev("o", "S1", "root", 20, 40, value=9.0),
        _ev("w", "S1", "o", 40, 60, withdrawn=True),
        _ev("f", "S1", "w", 60, 80, value=1.0, unit="F"),
        # [80,100): nothing grounded -> uncovered
        _ev("r2", "S2", "ROOT", 0, 100, value=5.0),
    ])
    doc = evaluate_device_release("B", 0, 100, _reqs(("S1", 0, 3, "u"), ("S2", 4, 6, "u")), 1, store)
    assert [(i["start_ns"], i["end_ns"]) for i in doc["released_intervals"]] == [(0, 20)]
    spans = doc["unreleased_intervals"]
    assert [
        (i["start_ns"], i["end_ns"], i["sensors"][0]["reason"]) for i in spans
    ] == [
        (20, 40, "out_of_bounds"),
        (40, 60, "withdrawn"),
        (60, 80, "unit_mismatch"),
        (80, 100, "uncovered"),
    ]
    assert spans[2]["sensors"][0]["observed_units"] == ["F"]
    assert spans[2]["sensors"][0]["unit"] == "F"
    for span in spans:
        assert span["qualified"] is False and span["too_short"] is False


def test_release_blocked_takes_precedence_over_grounded_value():
    store = _store([
        _ev("r1", "S1", "ROOT", 0, 100, value=2.0),
        _ev("orphan", "S1", "ghost", 40, 60, value=99.0),
        _ev("r2", "S2", "ROOT", 0, 100, value=5.0),
    ])
    doc = evaluate_device_release("B", 0, 100, _reqs(("S1", 0, 3, "u"), ("S2", 4, 6, "u")), 10, store)
    assert [(i["start_ns"], i["end_ns"]) for i in doc["released_intervals"]] == [
        (0, 40), (60, 100)
    ]
    (gap,) = doc["unreleased_intervals"]
    reason = gap["sensors"][0]
    assert reason["reason"] == "blocked"
    assert reason["missing_ancestors"] == ["ghost"]
    assert reason["value"] is None and reason["unit"] is None


def test_release_cycle_revisions_do_not_participate():
    store = _store([
        _ev("c1", "S1", "c2", 0, 100, value=1.0),
        _ev("c2", "S1", "c1", 0, 100, value=1.0),
        _ev("r2", "S2", "ROOT", 0, 100, value=5.0),
    ])
    doc = evaluate_device_release("B", 0, 100, _reqs(("S1", 0, 3, "u"), ("S2", 4, 6, "u")), 1, store)
    assert doc["released_intervals"] == []
    (span,) = doc["unreleased_intervals"]
    assert span["sensors"][0]["reason"] == "uncovered"


def test_release_qualifying_run_too_short_is_marked_not_released():
    store = _store([
        _ev("r1", "S1", "ROOT", 0, 100, value=9.0),
        _ev("ok", "S1", "r1", 40, 60, value=2.0),
        _ev("r2", "S2", "ROOT", 0, 100, value=5.0),
    ])
    # Qualifying run [40,60) is 20ns: releasable at 20, too short at 21.
    reqs = _reqs(("S1", 0, 3, "u"), ("S2", 4, 6, "u"))
    ok = evaluate_device_release("B", 0, 100, reqs, 20, store)
    assert ok["status"] == "released"
    assert [(i["start_ns"], i["end_ns"], i["duration_ns"])
            for i in ok["released_intervals"]] == [(40, 60, 20)]
    short = evaluate_device_release("B", 0, 100, reqs, 21, store)
    assert short["status"] == "not_released"
    assert short["released_intervals"] == []
    spans = short["unreleased_intervals"]
    assert [
        (i["start_ns"], i["end_ns"], i["qualified"], i["too_short"], i["sensors"])
        for i in spans
    ] == [
        (0, 40, False, False, spans[0]["sensors"]),
        (40, 60, True, True, []),
        (60, 100, False, False, spans[2]["sensors"]),
    ]
    assert spans[0]["sensors"][0]["reason"] == "out_of_bounds"
    assert spans[2]["sensors"][0]["reason"] == "out_of_bounds"


def test_release_adjacent_same_reason_spans_merge_with_extremes():
    store = _store([
        _ev("root", "S1", "ROOT", 0, 100, value=1.0),
        _ev("a", "S1", "root", 10, 20, value=9.0),
        _ev("b", "S1", "a", 20, 30, value=8.0),
        _ev("r2", "S2", "ROOT", 0, 100, value=5.0),
    ])
    doc = evaluate_device_release("B", 0, 100, _reqs(("S1", 0, 3, "u"), ("S2", 4, 6, "u")), 1, store)
    (bad,) = doc["unreleased_intervals"]
    assert (bad["start_ns"], bad["end_ns"], bad["duration_ns"]) == (10, 30, 20)
    assert bad["sensors"][0]["observed_min_value"] == 8.0
    assert bad["sensors"][0]["observed_max_value"] == 9.0


def test_release_changing_reason_set_splits_non_qualifying_spans():
    store = _store([
        _ev("root", "S1", "ROOT", 0, 100, value=9.0),  # always out of bounds
        _ev("r2", "S2", "ROOT", 0, 20, value=5.0),
        _ev("c", "S2", "r2", 20, 30, value=99.0),
        _ev("d", "S2", "c", 30, 100, value=5.0),
    ])
    reqs = _reqs(("S1", 0, 3, "u"), ("S2", 4, 6, "u"))
    doc = evaluate_device_release("B", 0, 100, reqs, 1, store)
    assert doc["released_intervals"] == []
    assert [
        (i["start_ns"], i["end_ns"], sorted(s["sensor_id"] for s in i["sensors"]))
        for i in doc["unreleased_intervals"]
    ] == [
        (0, 20, ["S1"]),
        (20, 30, ["S1", "S2"]),
        (30, 100, ["S1"]),
    ]


def test_release_window_is_clipped_half_open():
    store = _store([
        _ev("root", "S1", "ROOT", 0, 100, value=1.0),
        _ev("a", "S1", "root", 10, 30, value=9.0),
        _ev("r2", "S2", "ROOT", 0, 100, value=5.0),
    ])
    reqs = _reqs(("S1", 0, 3, "u"), ("S2", 4, 6, "u"))
    doc = evaluate_device_release("B", 15, 85, reqs, 1, store)
    assert _partition(doc) == [("unrel", 15, 30), ("rel", 30, 85)]
    # Empty window entirely outside coverage: every sensor is uncovered.
    outside = evaluate_device_release("B", 200, 300, reqs, 1, store)
    (span,) = outside["unreleased_intervals"]
    assert (span["start_ns"], span["end_ns"]) == (200, 300)
    assert sorted(s["sensor_id"] for s in span["sensors"]) == ["S1", "S2"]
    assert all(s["reason"] == "uncovered" for s in span["sensors"])


def test_release_partition_covers_whole_window_in_order():
    store = _store([
        _ev("r1", "S1", "ROOT", 0, 100, value=2.0),
        _ev("orphan", "S1", "ghost", 40, 60, value=9.0),
        _ev("r2", "S2", "ROOT", 0, 100, value=5.0),
    ])
    reqs = _reqs(("S1", 0, 3, "u"), ("S2", 4, 6, "u"))
    doc = evaluate_device_release("B", 5, 95, reqs, 1, store)
    assert _partition(doc) == [("rel", 5, 40), ("unrel", 40, 60), ("rel", 60, 95)]
    covered = sum(
        i["end_ns"] - i["start_ns"]
        for i in doc["released_intervals"] + doc["unreleased_intervals"]
    )
    assert covered == 90


def test_release_hash_stable_and_independent_of_query_params():
    store = _store([
        _ev("r1", "S1", "ROOT", 0, 100, value=2.0),
        _ev("r2", "S2", "ROOT", 0, 100, value=5.0),
    ])
    reqs = _reqs(("S1", 0, 3, "u"), ("S2", 4, 6, "u"))
    h1 = evaluate_device_release("B", 0, 100, reqs, 10, store)["input_hash"]
    h2 = evaluate_device_release(
        "B", 5, 90, _reqs(("S1", -9, 99, "X"), ("S2", -1, 1, "Z")), 99, store
    )["input_hash"]
    assert h1 == h2 and len(h1) == 64
    # A blocked revision changes the scan input even where a value exists.
    store.events["orphan"] = _ev("orphan", "S1", "ghost", 0, 5, value=9.0)
    h3 = evaluate_device_release("B", 0, 100, reqs, 10, store)["input_hash"]
    assert h3 != h1


def test_release_unknown_sensor_returns_none():
    store = _store([_ev("r1", "S1", "ROOT", 0, 10, value=1.0)])
    reqs = _reqs(("S1", 0, 3, "u"), ("MISSING", 0, 3, "u"))
    assert evaluate_device_release("B", 0, 10, reqs, 1, store) is None


def test_release_deterministic_across_dict_insertion_order():
    reqs = _reqs(("S1", 0, 3, "u"), ("S2", 4, 6, "u"))

    def build(reverse=False):
        store = BatchStore()
        events = [
            _ev("r1", "S1", "ROOT", 0, 100, value=2.0),
            _ev("orphan", "S1", "ghost", 40, 60, value=9.0),
            _ev("r2", "S2", "ROOT", 0, 100, value=5.0),
        ]
        for e in reversed(events) if reverse else events:
            store.events[e.revision_id] = e
        doc = evaluate_device_release("B", 0, 100, reqs, 10, store)
        doc.pop("input_hash")
        return doc

    assert build() == build(reverse=True)


def test_release_result_independent_of_requirement_order():
    store = _store([
        _ev("r1", "S1", "ROOT", 0, 50, value=9.0),
        _ev("r2", "S2", "ROOT", 0, 50, value=9.0),
    ])
    reqs = _reqs(("S1", 0, 3, "u"), ("S2", 0, 3, "u"))
    forward = evaluate_device_release("B", 0, 50, reqs, 1, store)
    reverse = evaluate_device_release("B", 0, 50, list(reversed(reqs)), 1, store)
    assert forward == reverse
    assert [s["sensor_id"] for s in forward["unreleased_intervals"][0]["sensors"]] == [
        "S1", "S2"
    ]


def test_release_adjacent_blocked_spans_merge_and_union_ancestors():
    store = _store([
        _ev("root", "S1", "ROOT", 0, 100, value=1.0),
        _ev("b1", "S1", "mz", 0, 20, value=99.0),
        _ev("b2", "S1", "ma", 20, 40, value=99.0),
        _ev("r2", "S2", "ROOT", 0, 100, value=5.0),
    ])
    doc = evaluate_device_release(
        "B", 0, 100, _reqs(("S1", 0, 3, "u"), ("S2", 4, 6, "u")), 1, store
    )
    bad = doc["unreleased_intervals"][0]
    assert (bad["start_ns"], bad["end_ns"]) == (0, 40)
    assert bad["sensors"][0]["reason"] == "blocked"
    assert bad["sensors"][0]["missing_ancestors"] == ["ma", "mz"]

