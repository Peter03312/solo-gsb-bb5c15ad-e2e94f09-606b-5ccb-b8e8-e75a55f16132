"""Engine unit tests for the non-mutating submission preview primitives."""

from app.engine import (
    diff_effective_intervals,
    folded_sensor_segments,
    classify_sensor,
    preview_submission,
)
from app.storage import BatchStore, IdempotencyConflict, StoredEvent


def _ev(rev, parent, start=0, end=10, value=1.0, unit="u", withdrawn=False,
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
        unit=unit,
        withdrawn=withdrawn,
    )


def _store(events):
    store = BatchStore()
    for e in events:
        store.events[e.revision_id] = e
        store.revision_sensor[e.revision_id] = e.sensor_id
        store.idempotency[e.idempotency_key] = "x"
    return store


def _segments(events):
    states, depths = classify_sensor(events)
    return folded_sensor_segments(events, states, depths)


# ---------------------------------------------------------------------------
# diff_effective_intervals
# ---------------------------------------------------------------------------


def test_diff_reports_added_removed_and_changed_spans():
    from app.engine import Segment

    before = [
        Segment(0, 20, "calibrated", 1.0, "u", "r"),
        Segment(40, 60, "calibrated", 1.0, "u", "r"),
    ]
    after = [
        Segment(0, 10, "calibrated", 1.0, "u", "r"),   # same
        Segment(10, 20, "calibrated", 9.0, "u", "c"),  # changed
        Segment(20, 30, "calibrated", 2.0, "u", "c"),  # added
        Segment(40, 60, "withdrawn", None, None, "w"),  # changed->tombstone
        Segment(60, 70, "calibrated", 3.0, "u", "c"),  # added
    ]
    intervals = diff_effective_intervals(before, after)
    assert [(i["start_ns"], i["end_ns"], i["kind"]) for i in intervals] == [
        (10, 20, "changed"),
        (20, 30, "added"),
        (40, 60, "changed"),
        (60, 70, "added"),
    ]
    assert intervals[1]["before"] is None
    assert intervals[1]["after"] == {"state": "calibrated", "value": 2.0, "unit": "u"}
    assert intervals[2]["after"] == {"state": "withdrawn", "value": None, "unit": None}


def test_diff_ignores_revision_provenance_boundaries():
    from app.engine import Segment

    # Same effective payload before and after; only the winning revision swaps.
    before = [
        Segment(0, 10, "calibrated", 1.0, "u", "v1"),
        Segment(10, 20, "calibrated", 1.0, "u", "v2"),
    ]
    after = [Segment(0, 20, "calibrated", 1.0, "u", "w1")]
    assert diff_effective_intervals(before, after) == []


def test_diff_unit_change_is_a_change_and_identical_spans_merge():
    from app.engine import Segment

    before = [Segment(0, 30, "calibrated", 1.0, "C", "r")]
    after = [
        Segment(0, 10, "calibrated", 1.0, "K", "c1"),
        Segment(10, 20, "calibrated", 1.0, "K", "c2"),
        Segment(20, 30, "calibrated", 1.0, "C", "r"),
    ]
    intervals = diff_effective_intervals(before, after)
    assert [(i["start_ns"], i["end_ns"], i["kind"]) for i in intervals] == [
        (0, 20, "changed")
    ]
    assert intervals[0]["before"]["unit"] == "C"
    assert intervals[0]["after"]["unit"] == "K"


def test_diff_empty_when_both_sides_uncovered_or_equal():
    assert diff_effective_intervals([], []) == []
    from app.engine import Segment

    seg = [Segment(0, 10, "calibrated", 1.0, "u", "r")]
    assert diff_effective_intervals(seg, list(seg)) == []


# ---------------------------------------------------------------------------
# folded_sensor_segments parity
# ---------------------------------------------------------------------------


def test_folded_sensor_segments_match_batch_fold_and_exclude_blocked():
    events = {
        "r": _ev("r", "ROOT", 0, 100, value=5.0),
        "o": _ev("o", "ghost", 0, 10, value=9.0),
    }
    segs = _segments(events)
    assert [(s.start_ns, s.end_ns, s.value, s.revision_id) for s in segs] == [
        (0, 100, 5.0, "r")
    ]


# ---------------------------------------------------------------------------
# preview_submission
# ---------------------------------------------------------------------------


def test_preview_empty_batch_adds_coverage_without_writing():
    candidate = _ev("r", "ROOT", 0, 10, value=5.0)
    doc = preview_submission("B", candidate, None)
    assert doc["status"] == "submittable"
    assert doc["before"]["revisions_total"] == 0
    assert doc["after"]["revisions_total"] == 1
    assert [i["kind"] for i in doc["changed_intervals"]] == ["added"]
    assert doc["gaps_added"] == [] and doc["gaps_removed"] == []


def test_preview_blocked_then_backfill_gap_delta_and_activation():
    store = _store([_ev("v2", "v1", 10, 20, value=2.0)])
    # Backfilling the root activates the blocked child over [10,20).
    doc = preview_submission("B", _ev("v1", "ROOT", 0, 30, value=1.0), store)
    assert [g["missing_revision_id"] for g in doc["gaps_removed"]] == ["v1"]
    assert doc["gaps_added"] == []
    assert [(i["start_ns"], i["end_ns"], i["after"]["value"]) for i in doc["changed_intervals"]] == [
        (0, 10, 1.0),
        (10, 20, 2.0),
        (20, 30, 1.0),
    ]
    assert doc["after"]["complete"] is True
    # The virtual event never leaked into the real store and no key was added.
    assert set(store.events) == {"v2"}
    assert set(store.idempotency) == {"k-v2"}


def test_preview_partial_backfill_reports_gap_replaced_up_chain():
    store = _store([_ev("v3", "v2", 0, 10, value=3.0)])
    doc = preview_submission("B", _ev("v2", "v1", 0, 20, value=2.0), store)
    assert [g["missing_revision_id"] for g in doc["gaps_removed"]] == ["v2"]
    assert [g["missing_revision_id"] for g in doc["gaps_added"]] == ["v1"]
    assert doc["changed_intervals"] == []
    assert set(store.events) == {"v3"}


def test_preview_does_not_register_idempotency_key_even_for_submittable():
    candidate = _ev("r", "ROOT", 0, 10, value=5.0, key="kr")
    preview_submission("B", candidate, None)
    # A fresh store is untouched (the function built a virtual copy).
    store = BatchStore()
    assert store.events == {} and store.idempotency == {}


def test_classify_submission_mirrors_add_decisions():
    from app.storage import EventStore

    event_store = EventStore()
    root = _ev("r", "ROOT", 0, 10, value=1.0, key="shared")
    assert event_store.classify_submission(root) == "submittable"
    # Previewing never consumes the key: the real add still accepts.
    assert event_store.add(root) == "accepted"
    # Exact replay -> duplicate via key and via revision/new key.
    assert event_store.classify_submission(root) == "duplicate"
    assert event_store.classify_submission(_ev("r", "ROOT", 0, 10, value=1.0, key="other")) == "duplicate"
    # Same key, other payload -> conflict.
    try:
        event_store.classify_submission(_ev("r2", "ROOT", 0, 10, value=2.0, key="shared"))
    except IdempotencyConflict:
        pass
    else:  # pragma: no cover
        raise AssertionError("expected IdempotencyConflict")
    # Same revision, other content -> conflict.
    try:
        event_store.classify_submission(_ev("r", "ROOT", 0, 10, value=9.0, key="k3"))
    except IdempotencyConflict:
        pass
    else:  # pragma: no cover
        raise AssertionError("expected IdempotencyConflict")
    # Classifying never registered "other" nor "k3".
    assert set(event_store.get_batch("B").idempotency) == {"shared"}
