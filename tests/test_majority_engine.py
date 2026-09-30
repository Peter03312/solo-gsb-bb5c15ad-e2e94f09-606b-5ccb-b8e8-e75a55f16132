"""Engine unit tests: redundant-sensor trusted-majority quorum sweep."""

import pytest

from app.engine import evaluate_sensor_majority
from app.storage import BatchStore, StoredEvent


def _ev(rev, sensor, parent, start=0, end=100, value=1.0, unit="C",
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


def _eval(store, sensors, q=2, diff=0.5, unit="C", start=0, end=100, batch="B"):
    return evaluate_sensor_majority(
        batch, start, end, sensors, unit, q, diff, store
    )


def _partition(doc):
    return [(i["start_ns"], i["end_ns"], i["verdict"]) for i in doc["intervals"]]


def test_majority_three_sensors_in_agreement_qualified_full_window():
    store = _store([
        _ev("r1", "S1", "ROOT", value=10.0),
        _ev("r2", "S2", "ROOT", value=10.1),
        _ev("r3", "S3", "ROOT", value=10.2),
    ])
    doc = _eval(store, ["S1", "S2", "S3"], q=2, diff=0.5)
    assert doc["status"] == "qualified"
    assert _partition(doc) == [(0, 100, "qualified")]
    (span,) = doc["intervals"]
    assert [m["sensor_id"] for m in span["members"]] == ["S1", "S2", "S3"]
    assert span["non_voting_sensors"] == []
    assert span["missing_ancestors"] == []
    assert span["duration_ns"] == 100


def test_majority_two_of_three_within_threshold_picks_largest_set():
    store = _store([
        _ev("r1", "S1", "ROOT", value=10.0),
        _ev("r2", "S2", "ROOT", value=10.2),
        _ev("r3", "S3", "ROOT", value=12.0),
    ])
    doc = _eval(store, ["S1", "S2", "S3"], q=2, diff=0.5)
    (span,) = doc["intervals"]
    assert span["verdict"] == "qualified"
    assert [m["sensor_id"] for m in span["members"]] == ["S1", "S2"]
    (divergent,) = span["non_voting_sensors"]
    assert divergent == {
        "sensor_id": "S3",
        "reason": "value_divergent",
        "value": 12.0,
        "unit": "C",
        "missing_ancestors": [],
    }


def test_majority_boundary_difference_is_inclusive():
    store = _store([
        _ev("r1", "S1", "ROOT", value=10.0),
        _ev("r2", "S2", "ROOT", value=10.5),
    ])
    doc = _eval(store, ["S1", "S2"], q=2, diff=0.5)
    assert doc["intervals"][0]["verdict"] == "qualified"
    tight = _eval(store, ["S1", "S2"], q=2, diff=0.4999)
    assert tight["intervals"][0]["verdict"] == "unqualified"


def test_majority_tie_chooses_lexicographically_smallest_member_set():
    # Two disjoint agreeing pairs ({S1,S2} and {S3,S4}); the smaller ids win.
    store = _store([
        _ev("r1", "S1", "ROOT", value=10.0),
        _ev("r2", "S2", "ROOT", value=10.1),
        _ev("r3", "S3", "ROOT", value=20.0),
        _ev("r4", "S4", "ROOT", value=20.1),
    ])
    doc = _eval(store, ["S1", "S2", "S3", "S4"], q=2, diff=0.5)
    span = doc["intervals"][0]
    assert span["verdict"] == "qualified"
    assert [m["sensor_id"] for m in span["members"]] == ["S1", "S2"]
    assert sorted(r["sensor_id"] for r in span["non_voting_sensors"]) == [
        "S3", "S4"
    ]
    assert all(r["reason"] == "value_divergent" for r in span["non_voting_sensors"])


def test_majority_single_voter_below_quorum_is_unqualified():
    store = _store([
        _ev("r1", "S1", "ROOT", value=10.0),
        _ev("r2", "S2", "ROOT", value=30.0),
        _ev("r3", "S3", "ROOT", value=50.0),
    ])
    doc = _eval(store, ["S1", "S2", "S3"], q=2, diff=0.5)
    span = doc["intervals"][0]
    assert span["verdict"] == "unqualified"
    assert [m["sensor_id"] for m in span["members"]] == ["S1"]
    assert sorted(
        (r["sensor_id"], r["reason"]) for r in span["non_voting_sensors"]
    ) == [("S2", "value_divergent"), ("S3", "value_divergent")]


def test_majority_blocked_masks_landed_value_and_waits_on_backfill():
    store = _store([
        _ev("r1", "S1", "ROOT", value=10.0),
        _ev("r2", "S2", "ROOT", value=10.1),
        # S3 has an agreeing landed value, but a blocked revision masks it on
        # [40, 60): qualified outside, pending_backfill inside the patch.
        _ev("r3", "S3", "ROOT", value=10.2),
        _ev("b", "S3", "ghost", start=40, end=60, value=9.0),
    ])
    doc = _eval(store, ["S1", "S2", "S3"], q=3, diff=1.0)
    assert doc["status"] == "qualified"
    assert _partition(doc) == [
        (0, 40, "qualified"),
        (40, 60, "pending_backfill"),
        (60, 100, "qualified"),
    ]
    pending = doc["intervals"][1]
    assert [m["sensor_id"] for m in pending["members"]] == ["S1", "S2"]
    (reason,) = pending["non_voting_sensors"]
    assert reason["sensor_id"] == "S3" and reason["reason"] == "blocked"
    assert reason["value"] is None and reason["unit"] is None
    assert reason["missing_ancestors"] == ["ghost"]
    assert pending["missing_ancestors"] == ["ghost"]


def test_majority_blocked_without_enough_voters_is_unqualified():
    store = _store([
        _ev("r1", "S1", "ROOT", value=10.0),
        _ev("r2", "S2", "ROOT", value=50.0),  # diverges
        _ev("b", "S3", "ghost", start=0, end=100, value=9.0),
    ])
    # q=2, one voter + one blocked reaches q -> pending.
    pending = _eval(store, ["S1", "S2", "S3"], q=2, diff=0.5)
    assert pending["intervals"][0]["verdict"] == "pending_backfill"
    # q=3, one voter + one blocked = 2 < 3 -> unqualified.
    doc = _eval(store, ["S1", "S2", "S3"], q=3, diff=0.5)
    assert doc["intervals"][0]["verdict"] == "unqualified"
    assert doc["status"] == "unqualified"
    # The blocked ancestor is still evidence on the unqualified span.
    assert doc["intervals"][0]["missing_ancestors"] == ["ghost"]


def test_majority_non_voting_reasons_withdrawn_uncovered_unit_mismatch():
    store = _store([
        _ev("r1", "S1", "ROOT", value=10.0),
        _ev("w", "S2", "ROOT", withdrawn=True),
        _ev("r3", "S3", "ROOT", value=10.1, unit="F"),
        # S4 exists but never covers the queried window -> uncovered.
        _ev("r4", "S4", "ROOT", start=200, end=300, value=10.0),
    ])
    doc = _eval(store, ["S1", "S2", "S3", "S4"], q=2, diff=0.5)
    span = doc["intervals"][0]
    assert span["verdict"] == "unqualified"
    assert [m["sensor_id"] for m in span["members"]] == ["S1"]
    reasons = {r["sensor_id"]: r for r in span["non_voting_sensors"]}
    assert reasons["S2"]["reason"] == "withdrawn"
    assert reasons["S2"]["value"] is None and reasons["S2"]["unit"] is None
    assert reasons["S3"]["reason"] == "unit_mismatch"
    assert reasons["S3"]["value"] == 10.1 and reasons["S3"]["unit"] == "F"
    assert reasons["S4"]["reason"] == "uncovered"


def test_majority_only_calibrated_unit_matching_values_vote():
    # Unit mismatch must never join an agreeing set even within the spread.
    store = _store([
        _ev("r1", "S1", "ROOT", value=10.0, unit="C"),
        _ev("r2", "S2", "ROOT", value=10.0, unit="F"),
    ])
    doc = _eval(store, ["S1", "S2"], q=2, diff=10.0, unit="C")
    span = doc["intervals"][0]
    assert span["verdict"] == "unqualified"
    assert [m["sensor_id"] for m in span["members"]] == ["S1"]
    assert span["non_voting_sensors"][0]["reason"] == "unit_mismatch"


def test_majority_cycle_revisions_do_not_participate():
    store = _store([
        _ev("c1", "S1", "c2", value=10.0),
        _ev("c2", "S1", "c1", value=10.0),
        _ev("r2", "S2", "ROOT", value=10.0),
    ])
    doc = _eval(store, ["S1", "S2"], q=2, diff=1.0)
    span = doc["intervals"][0]
    assert span["verdict"] == "unqualified"
    reasons = {r["sensor_id"]: r["reason"] for r in span["non_voting_sensors"]}
    assert reasons == {"S1": "uncovered"}


def test_majority_window_clipped_half_open():
    store = _store([
        _ev("r1", "S1", "ROOT", value=10.0),
        _ev("r2", "S2", "ROOT", value=10.0),
    ])
    doc = _eval(store, ["S1", "S2"], q=2, diff=1.0, start=10, end=90)
    assert _partition(doc) == [(10, 90, "qualified")]
    covered = sum(i["end_ns"] - i["start_ns"] for i in doc["intervals"])
    assert covered == 80
    # Window outside all coverage: both uncovered -> unqualified.
    outside = _eval(store, ["S1", "S2"], q=2, diff=1.0, start=200, end=300)
    assert _partition(outside) == [(200, 300, "unqualified")]
    assert sorted(r["reason"] for r in outside["intervals"][0]["non_voting_sensors"]) == [
        "uncovered", "uncovered"
    ]


def test_majority_partition_covers_whole_window_in_order():
    store = _store([
        _ev("r1", "S1", "ROOT", value=10.0),
        _ev("r2", "S2", "ROOT", value=10.0),
        _ev("b", "S3", "ghost", start=40, end=60, value=9.0),
        _ev("r3", "S3", "ROOT", value=10.0),
    ])
    doc = _eval(store, ["S1", "S2", "S3"], q=3, diff=1.0)
    assert _partition(doc) == [
        (0, 40, "qualified"),
        (40, 60, "pending_backfill"),
        (60, 100, "qualified"),
    ]
    covered = sum(i["end_ns"] - i["start_ns"] for i in doc["intervals"])
    assert covered == 100


def test_majority_adjacent_spans_merge_only_on_identical_evidence():
    # S1 switches value at 50; S2/S3 stay at 10.0 (q=2, so both halves are
    # qualified but the chosen members differ -> no merge across 50).
    store = _store([
        _ev("a", "S1", "ROOT", start=0, end=50, value=10.0),
        _ev("b", "S1", "ROOT", start=50, end=100, value=20.0),
        _ev("r2", "S2", "ROOT", value=10.0),
        _ev("r3", "S3", "ROOT", value=10.1),
    ])
    doc = _eval(store, ["S1", "S2", "S3"], q=2, diff=0.5)
    assert _partition(doc) == [
        (0, 50, "qualified"),
        (50, 100, "qualified"),
    ]
    assert [m["sensor_id"] for m in doc["intervals"][0]["members"]] == [
        "S1", "S2", "S3"
    ]
    assert [m["sensor_id"] for m in doc["intervals"][1]["members"]] == ["S2", "S3"]
    assert doc["intervals"][1]["non_voting_sensors"][0]["reason"] == "value_divergent"

    # Identical evidence across an internal boundary merges into one span.
    flat = _store([
        _ev("a", "S1", "ROOT", start=0, end=50, value=10.0),
        _ev("b", "S1", "ROOT", start=50, end=100, value=10.0),
        _ev("r2", "S2", "ROOT", value=10.0),
    ])
    doc2 = _eval(flat, ["S1", "S2"], q=2, diff=0.5)
    assert _partition(doc2) == [(0, 100, "qualified")]


def test_majority_verdict_change_splits_adjacent_spans():
    store = _store([
        _ev("r1", "S1", "ROOT", value=10.0),
        _ev("r2", "S2", "ROOT", value=10.0),
        # Blocked patch on S2 splits a qualified run into unqualified/qualified.
        _ev("b", "S2", "ghost", start=30, end=70, value=99.0),
    ])
    doc = _eval(store, ["S1", "S2"], q=2, diff=1.0)
    assert _partition(doc) == [
        (0, 30, "qualified"),
        (30, 70, "pending_backfill"),
        (70, 100, "qualified"),
    ]
    assert doc["status"] == "qualified"


def test_majority_multiple_blocked_sensors_union_ancestors():
    store = _store([
        _ev("r1", "S1", "ROOT", value=10.0),
        _ev("b2", "S2", "mz", value=9.0),
        _ev("b3", "S3", "ma", value=9.0),
        _ev("r4", "S4", "ROOT", value=10.0),
    ])
    doc = _eval(store, ["S1", "S2", "S3", "S4"], q=2, diff=1.0)
    span = doc["intervals"][0]
    assert span["verdict"] == "qualified"  # S1 + S4 already reach q
    blocked = {r["sensor_id"]: r for r in span["non_voting_sensors"]
               if r["reason"] == "blocked"}
    assert blocked["S2"]["missing_ancestors"] == ["mz"]
    assert blocked["S3"]["missing_ancestors"] == ["ma"]
    assert span["missing_ancestors"] == ["ma", "mz"]


def test_majority_blocked_ancestor_chains_list_terminal_ancestor():
    store = _store([
        _ev("r1", "S1", "ROOT", value=10.0),
        _ev("r2", "S2", "ROOT", value=10.0),
        # Chain: mid -> ghost (missing); child -> mid (known but blocked).
        _ev("mid", "S3", "ghost", value=9.0),
        _ev("child", "S3", "mid", value=9.0),
    ])
    doc = _eval(store, ["S1", "S2", "S3"], q=3, diff=1.0)
    span = doc["intervals"][0]
    assert span["verdict"] == "pending_backfill"
    assert span["missing_ancestors"] == ["ghost"]


def test_majority_hash_stable_and_independent_of_query_params():
    store = _store([
        _ev("r1", "S1", "ROOT", value=10.0),
        _ev("r2", "S2", "ROOT", value=10.0),
    ])
    h1 = _eval(store, ["S1", "S2"], q=2, diff=0.5)["input_hash"]
    h2 = _eval(store, ["S1", "S2"], q=2, diff=9.0, start=10, end=90)["input_hash"]
    assert h1 == h2 and len(h1) == 64
    # A blocked revision changes the scan input even where a value exists.
    store.events["b"] = _ev("b", "S1", "ghost", start=0, end=10, value=9.0)
    h3 = _eval(store, ["S1", "S2"], q=2, diff=0.5)["input_hash"]
    assert h3 != h1


def test_majority_unknown_sensor_returns_none():
    store = _store([_ev("r1", "S1", "ROOT", value=10.0)])
    assert _eval(store, ["S1", "NOPE"], q=2, diff=1.0) is None


def test_majority_deterministic_across_insertion_order():
    events = [
        _ev("r1", "S1", "ROOT", value=10.0),
        _ev("r2", "S2", "ROOT", value=10.2),
        _ev("r3", "S3", "ROOT", value=20.0),
        _ev("b", "S3", "ghost", start=40, end=60, value=9.0),
    ]

    def build(order):
        store = BatchStore()
        for e in order:
            store.events[e.revision_id] = e
        doc = evaluate_sensor_majority(
            "B", 0, 100, ["S1", "S2", "S3"], "C", 2, 0.5, store
        )
        doc.pop("input_hash")
        return doc

    first = build(events)
    assert build(list(reversed(events))) == first
    assert build([events[2], events[0], events[3], events[1]]) == first


def test_majority_independent_of_sensor_request_order():
    store = _store([
        _ev("r1", "S1", "ROOT", value=10.0),
        _ev("r2", "S2", "ROOT", value=10.1),
        _ev("r3", "S3", "ROOT", value=30.0),
    ])
    forward = _eval(store, ["S1", "S2", "S3"], q=2, diff=0.5)
    reverse = _eval(store, ["S3", "S2", "S1"], q=2, diff=0.5)
    # Echoed sensor list follows request order; verdict/evidence is the same.
    assert forward["status"] == reverse["status"] == "qualified"
    assert forward["intervals"] == reverse["intervals"]
    assert forward["sensors"] == ["S1", "S2", "S3"]
    assert reverse["sensors"] == ["S3", "S2", "S1"]


@pytest.mark.parametrize(
    "values,diff,expected",
    [
        ([1.0, 1.0, 1.0], 0.0, ["S1", "S2", "S3"]),
        ([1.0, 2.0, 3.0], 1.0, ["S1", "S2"]),  # tie [1,2] vs [2,3] -> smaller ids
        ([3.0, 1.0, 2.0], 1.0, ["S1", "S3"]),  # bands {S2,S3} vs {S1,S3} -> S1<S2
        ([1.0, 5.0, 9.0], 2.0, ["S1"]),
    ],
)
def test_largest_compatible_set_band_selection(values, diff, expected):
    from app.engine import _largest_compatible_set

    voters = {f"S{i + 1}": v for i, v in enumerate(values)}
    assert _largest_compatible_set(voters, diff) == expected
