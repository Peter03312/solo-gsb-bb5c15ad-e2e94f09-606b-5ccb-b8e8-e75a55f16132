"""Deterministic effective-calibration-timeline engine.

The engine is pure: given a :class:`~app.storage.BatchStore` snapshot it
produces per-sensor timelines, backfillable gaps, content hashes,
cross-batch diffs and archival seal evidence, without keeping state of its
own.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

from .models import ROOT_PARENT
from .storage import BatchStore, StoredEvent
# Revision states (reachable from a known root vs. waiting on a missing
# ancestor vs. structurally unusable, e.g. part of a parent cycle).
GROUNDED = "grounded"
BLOCKED = "blocked"
INVALID = "invalid"


@dataclass(frozen=True)
class Segment:
    start_ns: int
    end_ns: int
    state: str  # "calibrated" | "withdrawn"
    value: Optional[float]
    unit: Optional[str]
    revision_id: str


# ---------------------------------------------------------------------------
# Causal classification
# ---------------------------------------------------------------------------


def classify_sensor(
    events: dict[str, StoredEvent],
) -> tuple[dict[str, str], dict[str, int]]:
    """Classify every revision of one sensor and compute its causal depth.

    Depth is the number of parent hops from a ``ROOT`` revision (roots are
    depth 0). Returns ``(states, depths)``.

    States:

    * ``grounded`` - reachable from a known ``ROOT`` revision;
    * ``blocked``  - has an unknown (not yet uploaded) ancestor;
    * ``invalid``  - participates in a parent cycle or descends from one.
    """
    states: dict[str, str] = {}
    depths: dict[str, int] = {}

    def resolve(node: str) -> None:
        """Iterative postorder DFS rooted at ``node``."""
        # Frames: (node, entered). `entered` frames have already pushed their
        # parent frame and just need post-order finalization.
        stack: list[tuple[str, bool]] = [(node, False)]
        on_path: set[str] = set()
        while stack:
            cur, entered = stack.pop()
            if cur in states:
                continue
            parent = events[cur].parent_id

            if not entered:
                if cur in on_path:  # pragma: no cover - pushed once normally
                    continue
                on_path.add(cur)
                if parent == ROOT_PARENT:
                    states[cur] = GROUNDED
                    depths[cur] = 0
                    on_path.discard(cur)
                    continue
                if parent not in events:
                    states[cur] = BLOCKED
                    on_path.discard(cur)
                    continue
                if parent in states:
                    _inherit(cur, parent, states, depths)
                    on_path.discard(cur)
                    continue
                # Resolve parent first, then finalize this node.
                stack.append((cur, True))
                stack.append((parent, False))
            else:
                if parent in on_path and parent not in states:
                    # Back edge: the parent chain currently on the stack is a
                    # cycle. Mark every still-open node invalid; finalization
                    # frames will then propagate INVALID to descendants.
                    for n in on_path:
                        states[n] = INVALID
                elif parent in states:
                    _inherit(cur, parent, states, depths)
                else:  # pragma: no cover - defensive
                    states[cur] = INVALID
                on_path.discard(cur)

    for rev in sorted(events):
        if rev not in states:
            resolve(rev)

    return states, depths


def _inherit(
    node: str,
    parent: str,
    states: dict[str, str],
    depths: dict[str, int],
) -> None:
    pstate = states[parent]
    if pstate == GROUNDED:
        states[node] = GROUNDED
        depths[node] = depths[parent] + 1
    elif pstate == INVALID:
        states[node] = INVALID
    else:
        states[node] = BLOCKED


# ---------------------------------------------------------------------------
# Interval folding
# ---------------------------------------------------------------------------


def _apply(segments: list[Segment], event: StoredEvent) -> None:
    """Overwrite the half-open window ``[start_ns, end_ns)`` in ``segments``.

    Existing segments are split at the window boundaries; the window itself is
    replaced by one segment describing the applying revision.
    """
    state = "withdrawn" if event.withdrawn else "calibrated"
    new_seg = Segment(
        start_ns=event.start_ns,
        end_ns=event.end_ns,
        state=state,
        value=None if event.withdrawn else float(event.value),
        unit=None if event.withdrawn else event.unit,
        revision_id=event.revision_id,
    )

    result: list[Segment] = []
    lo, hi = event.start_ns, event.end_ns
    for seg in segments:
        if seg.end_ns <= lo or seg.start_ns >= hi:
            result.append(seg)
            continue
        # Overlap: keep the non-covered parts (half-open intervals).
        if seg.start_ns < lo:
            result.append(
                Segment(seg.start_ns, lo, seg.state, seg.value, seg.unit, seg.revision_id)
            )
        if seg.end_ns > hi:
            result.append(
                Segment(hi, seg.end_ns, seg.state, seg.value, seg.unit, seg.revision_id)
            )
    # Insert the replacement and re-sort; windows may bridge old segments.
    result.append(new_seg)
    result.sort(key=lambda s: (s.start_ns, s.end_ns))

    # Merge adjacent segments that are identical in every output attribute
    # (different revision provenance keeps a boundary).
    merged: list[Segment] = []
    for seg in result:
        if merged and merged[-1].end_ns == seg.start_ns and _same_payload(
            merged[-1], seg
        ):
            merged[-1] = Segment(
                merged[-1].start_ns,
                seg.end_ns,
                seg.state,
                seg.value,
                seg.unit,
                seg.revision_id,
            )
        else:
            merged.append(seg)
    segments[:] = merged


def _same_payload(a: Segment, b: Segment) -> bool:
    return (
        a.state == b.state
        and a.revision_id == b.revision_id
        and a.value == b.value
        and a.unit == b.unit
    )


def build_timeline(events: list[StoredEvent], depths: dict[str, int]) -> list[Segment]:
    """Fold grounded events into the effective timeline.

    Deterministic application order: causal depth first (descendants overwrite
    ancestors), then ascending ``revision_id`` to break ties between
    incomparable causal branches; the larger id is therefore applied last and
    wins on same-depth overlaps.
    """
    ordered = sorted(events, key=lambda e: (depths[e.revision_id], e.revision_id))
    segments: list[Segment] = []
    for event in ordered:
        _apply(segments, event)
    return segments


# ---------------------------------------------------------------------------
# Gaps
# ---------------------------------------------------------------------------


def collect_gaps(
    batch_id: str,
    sensor_events: dict[str, StoredEvent],
    states: dict[str, str],
) -> list[dict]:
    """One entry per distinct missing parent, listing every awaiting child."""
    gaps = []
    sensor_id = next(iter(sensor_events.values())).sensor_id
    awaited: dict[str, set[str]] = defaultdict(set)
    referenced_from: dict[str, str] = {}
    for event in sensor_events.values():
        if states.get(event.revision_id) != BLOCKED:
            continue
        parent = event.parent_id
        if parent != ROOT_PARENT and parent not in sensor_events:
            awaited[parent].add(event.revision_id)
            # Deterministic "first referrer" = lexicographically smallest child.
            if parent not in referenced_from or event.revision_id < referenced_from[parent]:
                referenced_from[parent] = event.revision_id

    for missing in sorted(awaited):
        gaps.append(
            {
                "batch_id": batch_id,
                "sensor_id": sensor_id,
                "missing_revision_id": missing,
                "awaited_by": sorted(awaited[missing]),
                "referenced_from": referenced_from[missing],
            }
        )
    return gaps


# ---------------------------------------------------------------------------
# Hashing / serialization
# ---------------------------------------------------------------------------


def segment_to_dict(seg: Segment) -> dict:
    return {
        "start_ns": seg.start_ns,
        "end_ns": seg.end_ns,
        "state": seg.state,
        "value": None if seg.value is None else float(seg.value),
        "unit": seg.unit,
        "revision_id": seg.revision_id,
    }


def hash_segments(segments: list[Segment]) -> str:
    """Stable SHA-256 over the canonical segment list."""
    payload = json.dumps(
        [segment_to_dict(s) for s in segments],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def hash_batch(sensor_payloads: list[dict]) -> str:
    payload = json.dumps(
        sensor_payloads, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def hash_gate_input(segments: list[Segment], blocked_spans: list[dict]) -> str:
    """Stable SHA-256 over one sensor's quality-gate scan input.

    Covers the folded grounded segments plus the blocked revision spans, so the
    hash changes exactly when the gate evaluation input changes.
    """
    payload = json.dumps(
        {
            "segments": [segment_to_dict(s) for s in segments],
            "blocked": blocked_spans,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Batch-level orchestration
# ---------------------------------------------------------------------------


def group_by_sensor(store: BatchStore) -> dict[str, dict[str, StoredEvent]]:
    """Group every stored revision of a batch by its owning sensor."""
    by_sensor: dict[str, dict[str, StoredEvent]] = {}
    for event in store.events.values():
        by_sensor.setdefault(event.sensor_id, {})[event.revision_id] = event
    return by_sensor


def _parent_chain(revision_id: str, events: dict[str, StoredEvent]) -> list[str]:
    """Ordered chain from ``revision_id`` to ``ROOT`` inclusive.

    The revision itself is first and the literal ``"ROOT"`` is last. Only
    called for grounded revisions, so every parent hop is known.
    """
    chain = [revision_id]
    parent = events[revision_id].parent_id
    while parent != ROOT_PARENT:
        chain.append(parent)
        parent = events[parent].parent_id
    chain.append(ROOT_PARENT)
    return chain


def _terminal_missing_ancestors(
    revision_ids: list[str], sensor_events: dict[str, StoredEvent]
) -> list[str]:
    """Distinct unknown terminal ancestors of the given revisions (sorted).

    Walks each revision's parent chain until a known revision or ``ROOT``;
    the chain's terminal parent is missing iff it is neither.
    """
    missing: set[str] = set()
    for rev in revision_ids:
        parent = sensor_events[rev].parent_id
        while parent != ROOT_PARENT and parent in sensor_events:
            parent = sensor_events[parent].parent_id
        if parent != ROOT_PARENT:
            missing.add(parent)
    return sorted(missing)


def query_sample(
    sensor_id: str,
    timestamp_ns: int,
    sensor_events: dict[str, StoredEvent],
    states: dict[str, str],
    depths: dict[str, int],
) -> dict:
    """Resolve one sample instant of one sensor.

    Blocking takes precedence: if any known-but-not-grounded revision covers
    the instant, backfilling its missing ancestors could change which revision
    wins the deterministic fold at that point, so the point is reported as
    ``blocked`` with the distinct missing ancestors that must be uploaded.
    Otherwise the instant is looked up in the folded effective timeline,
    reusing the exact same fold order as the full batch result.
    """
    covering_blocked = sorted(
        rev
        for rev, event in sensor_events.items()
        if states[rev] == BLOCKED
        and event.start_ns <= timestamp_ns < event.end_ns
    )
    if covering_blocked:
        return {
            "sensor_id": sensor_id,
            "timestamp_ns": timestamp_ns,
            "state": "blocked",
            "value": None,
            "unit": None,
            "revision_id": None,
            "parent_chain": [],
            "missing_ancestors": _terminal_missing_ancestors(
                covering_blocked, sensor_events
            ),
        }

    grounded_events = [
        e for e in sensor_events.values() if states[e.revision_id] == GROUNDED
    ]
    segments = build_timeline(grounded_events, depths)
    for seg in segments:
        if seg.start_ns <= timestamp_ns < seg.end_ns:
            return {
                "sensor_id": sensor_id,
                "timestamp_ns": timestamp_ns,
                "state": seg.state,
                "value": seg.value,
                "unit": seg.unit,
                "revision_id": seg.revision_id,
                "parent_chain": _parent_chain(seg.revision_id, sensor_events),
                "missing_ancestors": [],
            }

    return {
        "sensor_id": sensor_id,
        "timestamp_ns": timestamp_ns,
        "state": "uncovered",
        "value": None,
        "unit": None,
        "revision_id": None,
        "parent_chain": [],
        "missing_ancestors": [],
    }


def evaluate_sample(
    batch_id: str, sensor_id: str, timestamp_ns: int, store: BatchStore
) -> Optional[dict]:
    """Compute the sample-point document; ``None`` when the sensor is unknown."""
    by_sensor = group_by_sensor(store)
    sensor_events = by_sensor.get(sensor_id)
    if sensor_events is None:
        return None
    states, depths = classify_sensor(sensor_events)
    doc = query_sample(sensor_id, timestamp_ns, sensor_events, states, depths)
    doc = {"batch_id": batch_id, **doc}
    return doc


def evaluate_batch(batch_id: str, store: BatchStore) -> dict:
    """Compute the complete, deterministic result document for one batch."""
    by_sensor = group_by_sensor(store)

    sensors_out: list[dict] = []
    gaps_out: list[dict] = []
    totals = {GROUNDED: 0, BLOCKED: 0, INVALID: 0}

    for sensor_id in sorted(by_sensor):
        sensor_events = by_sensor[sensor_id]
        states, depths = classify_sensor(sensor_events)
        gaps_out.extend(collect_gaps(batch_id, sensor_events, states))

        for state in states.values():
            totals[state] += 1

        grounded_events = [
            e for e in sensor_events.values() if states[e.revision_id] == GROUNDED
        ]
        segments = build_timeline(grounded_events, depths)
        seg_dicts = [segment_to_dict(s) for s in segments]
        sensor_doc = {
            "sensor_id": sensor_id,
            "grounded": not any(
                states[r] != GROUNDED for r in sensor_events
            ),
            "segments": seg_dicts,
            "segments_hash": hash_segments(segments),
            "covered_start_ns": segments[0].start_ns if segments else None,
            "covered_end_ns": segments[-1].end_ns if segments else None,
        }
        sensors_out.append(sensor_doc)

    gaps_out.sort(key=lambda g: (g["sensor_id"], g["missing_revision_id"]))
    timeline_hash = hash_batch(
        [{"sensor_id": s["sensor_id"], "segments": s["segments"]} for s in sensors_out]
    )

    revisions_total = sum(totals.values())
    return {
        "batch_id": batch_id,
        "sensors": sensors_out,
        "gaps": gaps_out,
        "revisions_total": revisions_total,
        "revisions_grounded": totals[GROUNDED],
        "revisions_blocked": totals[BLOCKED],
        "revisions_invalid": totals[INVALID],
        "complete": not gaps_out and totals[INVALID] == 0 and revisions_total > 0,
        "timeline_hash": timeline_hash,
    }


# ---------------------------------------------------------------------------
# Submission preview (non-mutating)
# ---------------------------------------------------------------------------

# Effective-state change kinds emitted by the half-open preview diff.
ADDED = "added"
REMOVED = "removed"
CHANGED = "changed"


def folded_sensor_segments(
    sensor_events: dict[str, StoredEvent],
    states: dict[str, str],
    depths: dict[str, int],
) -> list[Segment]:
    """Fold the grounded revisions of one sensor exactly like the batch result."""
    grounded_events = [
        e for e in sensor_events.values() if states[e.revision_id] == GROUNDED
    ]
    return build_timeline(grounded_events, depths)


def diff_effective_intervals(
    before: list[Segment], after: list[Segment]
) -> list[dict]:
    """Half-open diff of effective (state/value/unit) coverage between folds.

    Mirrors :func:`compare_segments`, except the two sides represent the same
    sensor before and after a hypothetical submission and every returned
    interval carries a change kind:

    * ``added``   - uncovered before, covered after;
    * ``removed`` - covered before, uncovered after;
    * ``changed`` - covered on both sides but state/value/unit differ.

    Spans whose effective payload is unchanged (including spans where only the
    winning revision id changes) are not reported. Adjacent elementary spans
    with identical kind and payloads merge, so the output is a contiguous,
    time-sorted partition of the *changed* part of the union coverage span.
    """
    points = sorted({p for seg in before + after for p in (seg.start_ns, seg.end_ns)})
    intervals: list[dict] = []
    bi = ai = 0
    for lo, hi in zip(points, points[1:]):
        while bi < len(before) and before[bi].end_ns <= lo:
            bi += 1
        while ai < len(after) and after[ai].end_ns <= lo:
            ai += 1
        bseg = before[bi] if bi < len(before) and before[bi].start_ns <= lo else None
        aseg = after[ai] if ai < len(after) and after[ai].start_ns <= lo else None
        before_payload = _side_payload(bseg)
        after_payload = _side_payload(aseg)
        if before_payload == after_payload:
            continue
        if before_payload is None:
            kind = ADDED
        elif after_payload is None:
            kind = REMOVED
        else:
            kind = CHANGED
        if (
            intervals
            and intervals[-1]["end_ns"] == lo
            and intervals[-1]["kind"] == kind
            and intervals[-1]["before"] == before_payload
            and intervals[-1]["after"] == after_payload
        ):
            intervals[-1]["end_ns"] = hi
        else:
            intervals.append(
                {
                    "start_ns": lo,
                    "end_ns": hi,
                    "kind": kind,
                    "before": before_payload,
                    "after": after_payload,
                }
            )
    return intervals


def preview_submission(
    batch_id: str, event: StoredEvent, store: Optional[BatchStore]
) -> dict:
    """Compute the effect a submission would have, without writing anything.

    Uses the exact validation/idempotency/fold rules of a real submission
    (the caller first classifies via :meth:`EventStore.classify_submission`),
    evaluates the batch as it is now and as it would be with the candidate
    appended, and derives the effective-state change intervals plus the
    missing-parent gap deltas for the candidate's sensor. A batch that does
    not exist yet is treated as an empty batch.
    """
    before_doc = (
        evaluate_batch(batch_id, store)
        if store is not None and store.events
        else evaluate_batch(batch_id, BatchStore())
    )

    virtual = BatchStore()
    if store is not None:
        virtual.events = dict(store.events)
        virtual.revision_sensor = dict(store.revision_sensor)
        virtual.idempotency = dict(store.idempotency)
    virtual.events[event.revision_id] = event
    virtual.revision_sensor[event.revision_id] = event.sensor_id
    # Do NOT register the idempotency key: previews must not occupy it.
    after_doc = evaluate_batch(batch_id, virtual)

    sensor_id = event.sensor_id
    before_events = {
        rev: e
        for rev, e in virtual.events.items()
        if e.sensor_id == sensor_id and rev != event.revision_id
    }
    after_events = {
        rev: e for rev, e in virtual.events.items() if e.sensor_id == sensor_id
    }
    before_states, before_depths = classify_sensor(before_events)
    after_states, after_depths = classify_sensor(after_events)
    before_segments = folded_sensor_segments(
        before_events, before_states, before_depths
    )
    after_segments = folded_sensor_segments(after_events, after_states, after_depths)
    changed_intervals = diff_effective_intervals(before_segments, after_segments)

    before_gaps = {
        (g["sensor_id"], g["missing_revision_id"]): g
        for g in before_doc["gaps"]
        if g["sensor_id"] == sensor_id
    }
    after_gaps = {
        (g["sensor_id"], g["missing_revision_id"]): g
        for g in after_doc["gaps"]
        if g["sensor_id"] == sensor_id
    }
    gaps_added = [after_gaps[key] for key in sorted(after_gaps.keys() - before_gaps)]
    gaps_removed = [before_gaps[key] for key in sorted(before_gaps.keys() - after_gaps)]

    return {
        "status": "submittable",
        "idempotency_key": event.idempotency_key,
        "batch_id": batch_id,
        "sensor_id": sensor_id,
        "revision_id": event.revision_id,
        "before": before_doc,
        "after": after_doc,
        "before_timeline_hash": before_doc["timeline_hash"],
        "after_timeline_hash": after_doc["timeline_hash"],
        "changed_intervals": changed_intervals,
        "gaps_added": gaps_added,
        "gaps_removed": gaps_removed,
    }


# ---------------------------------------------------------------------------
# Cross-batch comparison
# ---------------------------------------------------------------------------

# Compare interval kinds emitted by the half-open diff scan.
EQUAL = "equal"
LEFT_ONLY = "left_only"
RIGHT_ONLY = "right_only"
VALUE_MISMATCH = "value_mismatch"
STATE_MISMATCH = "state_mismatch"


def find_sensor_events(
    store: BatchStore, sensor_id: str
) -> Optional[dict[str, StoredEvent]]:
    """All stored revisions of one sensor keyed by revision id.

    Returns ``None`` when the sensor is unknown in this batch.
    """
    events = {
        event.revision_id: event
        for event in store.events.values()
        if event.sensor_id == sensor_id
    }
    return events or None


def _side_payload(seg: Optional[Segment]) -> Optional[dict]:
    """Effective state one side contributes on a diff span (``None``=uncovered).

    Shared by the cross-batch compare and the before/after submission preview
    scans. Provenance (revision id) is deliberately excluded: both scans
    compare effective calibration state only.
    """
    if seg is None:
        return None
    return {
        "state": seg.state,
        "value": None if seg.value is None else float(seg.value),
        "unit": seg.unit,
    }


def _classify_pair(left: Optional[Segment], right: Optional[Segment]) -> str:
    if left is None:
        return EQUAL if right is None else RIGHT_ONLY
    if right is None:
        return LEFT_ONLY
    if left.state != right.state:
        return STATE_MISMATCH
    if left.state == "calibrated" and (
        left.value != right.value or left.unit != right.unit
    ):
        return VALUE_MISMATCH
    return EQUAL


def compare_segments(left: list[Segment], right: list[Segment]) -> list[dict]:
    """Half-open diff scan over two folded timelines.

    Sweeps the union of both timelines' boundaries and classifies every
    elementary interval. Adjacent intervals whose kind and side payloads
    all match are merged, so the output is a contiguous, time-sorted
    partition of the union coverage span (both sides uncovered counts as
    ``equal``).
    """
    points = sorted({p for seg in left + right for p in (seg.start_ns, seg.end_ns)})
    intervals: list[dict] = []
    li = ri = 0
    for lo, hi in zip(points, points[1:]):
        while li < len(left) and left[li].end_ns <= lo:
            li += 1
        while ri < len(right) and right[ri].end_ns <= lo:
            ri += 1
        lseg = left[li] if li < len(left) and left[li].start_ns <= lo else None
        rseg = right[ri] if ri < len(right) and right[ri].start_ns <= lo else None
        kind = _classify_pair(lseg, rseg)
        left_payload = _side_payload(lseg)
        right_payload = _side_payload(rseg)
        if (
            intervals
            and intervals[-1]["kind"] == kind
            and intervals[-1]["left"] == left_payload
            and intervals[-1]["right"] == right_payload
        ):
            intervals[-1]["end_ns"] = hi
        else:
            intervals.append(
                {
                    "start_ns": lo,
                    "end_ns": hi,
                    "kind": kind,
                    "left": left_payload,
                    "right": right_payload,
                }
            )
    return intervals


def _incomplete_reasons(
    side: str,
    batch_id: str,
    sensor_events: dict[str, StoredEvent],
    states: dict[str, str],
) -> list[dict]:
    """Reasons why one side's folded timeline may still change."""
    reasons: list[dict] = []
    blocked = sorted(rev for rev, state in states.items() if state == BLOCKED)
    if blocked:
        missing = sorted(
            gap["missing_revision_id"]
            for gap in collect_gaps(batch_id, sensor_events, states)
        )
        reasons.append(
            {
                "side": side,
                "batch_id": batch_id,
                "reason": "gaps",
                "detail": (
                    f"{len(blocked)} revision(s) wait on missing ancestors; "
                    "backfilling them may change the folded timeline"
                ),
                "affected_revisions": blocked,
                "missing_revision_ids": missing,
            }
        )
    invalid = sorted(rev for rev, state in states.items() if state == INVALID)
    if invalid:
        reasons.append(
            {
                "side": side,
                "batch_id": batch_id,
                "reason": "invalid_revisions",
                "detail": (
                    f"{len(invalid)} revision(s) form or descend from a parent "
                    "cycle and are excluded from the folded timeline"
                ),
                "affected_revisions": invalid,
                "missing_revision_ids": [],
            }
        )
    return reasons


def compare_sensor(
    left_batch_id: str,
    right_batch_id: str,
    sensor_id: str,
    left_events: dict[str, StoredEvent],
    right_events: dict[str, StoredEvent],
) -> dict:
    """Build the cross-batch diff document for one sensor.

    Both sides are folded with the exact deterministic rules of the batch
    result, so the diff reflects effective-state differences only, never
    arrival order.
    """
    left_states, left_depths = classify_sensor(left_events)
    right_states, right_depths = classify_sensor(right_events)
    left_segments = build_timeline(
        [e for e in left_events.values() if left_states[e.revision_id] == GROUNDED],
        left_depths,
    )
    right_segments = build_timeline(
        [e for e in right_events.values() if right_states[e.revision_id] == GROUNDED],
        right_depths,
    )
    reasons = _incomplete_reasons("left", left_batch_id, left_events, left_states)
    reasons += _incomplete_reasons("right", right_batch_id, right_events, right_states)
    return {
        "sensor_id": sensor_id,
        "left_batch_id": left_batch_id,
        "right_batch_id": right_batch_id,
        "left_segments_hash": hash_segments(left_segments),
        "right_segments_hash": hash_segments(right_segments),
        "incomplete": bool(reasons),
        "incomplete_reasons": reasons,
        "intervals": compare_segments(left_segments, right_segments),
    }


# ---------------------------------------------------------------------------
# Quality gate
# ---------------------------------------------------------------------------

# Missing-data policies: gaps are always recorded; "fail" also fails the gate.
MISSING_IGNORE = "ignore"
MISSING_FAIL = "fail"


def evaluate_quality_gate(
    batch_id: str,
    sensor_id: str,
    min_value: float,
    max_value: float,
    min_duration_ns: int,
    missing_policy: str,
    store: BatchStore,
) -> Optional[dict]:
    """Scan one sensor's folded timeline against allowed-value bounds.

    Deterministic sweep over the union span of the sensor's grounded and
    blocked revisions (invalid cycle members are excluded, exactly as in the
    sample endpoint). Out-of-bounds ``calibrated`` spans are candidate
    violations: adjacent candidates merge into runs by nanosecond adjacency,
    and only runs whose duration reaches ``min_duration_ns`` are reported.
    ``withdrawn``/``uncovered``/``blocked`` spans are recorded as gaps
    (adjacent same-state spans merge) and never join a violation run.
    ``blocked`` takes precedence over grounded coverage, since backfilling
    could still change the fold winner there.

    Returns ``None`` when the sensor is unknown in this batch.
    """
    sensor_events = find_sensor_events(store, sensor_id)
    if sensor_events is None:
        return None
    states, depths = classify_sensor(sensor_events)
    segments = build_timeline(
        [e for e in sensor_events.values() if states[e.revision_id] == GROUNDED],
        depths,
    )
    blocked = sorted(
        (e for e in sensor_events.values() if states[e.revision_id] == BLOCKED),
        key=lambda e: (e.start_ns, e.end_ns, e.revision_id),
    )

    points = sorted(
        {p for seg in segments for p in (seg.start_ns, seg.end_ns)}
        | {p for e in blocked for p in (e.start_ns, e.end_ns)}
    )

    violations: list[dict] = []
    gaps: list[dict] = []
    vrun: Optional[dict] = None  # open out-of-bounds run
    grun: Optional[dict] = None  # open gap run

    def close_violation_run() -> None:
        nonlocal vrun
        if vrun is not None:
            duration = vrun["end_ns"] - vrun["start_ns"]
            if duration >= min_duration_ns:
                violations.append({**vrun, "duration_ns": duration})
            vrun = None

    def close_gap_run() -> None:
        nonlocal grun
        if grun is not None:
            gaps.append(
                {
                    "start_ns": grun["start_ns"],
                    "end_ns": grun["end_ns"],
                    "duration_ns": grun["end_ns"] - grun["start_ns"],
                    "state": grun["state"],
                    "missing_ancestors": sorted(grun["missing_ancestors"]),
                }
            )
            grun = None

    si = 0  # segments are sorted; advance monotonically like compare_segments
    for lo, hi in zip(points, points[1:]):
        covering = [e for e in blocked if e.start_ns <= lo < e.end_ns]
        if covering:
            kind = "blocked"
            value = None
            missing = _terminal_missing_ancestors(
                [e.revision_id for e in covering], sensor_events
            )
            seg = None
        else:
            missing = []
            while si < len(segments) and segments[si].end_ns <= lo:
                si += 1
            seg = (
                segments[si]
                if si < len(segments) and segments[si].start_ns <= lo
                else None
            )
            if seg is None:
                kind, value = "uncovered", None
            elif seg.state == "withdrawn":
                kind, value = "withdrawn", None
            elif min_value <= seg.value <= max_value:
                kind, value = "ok", None
            else:
                kind, value = "violation", seg.value

        if kind == "ok":
            close_violation_run()
            close_gap_run()
        elif kind == "violation":
            close_gap_run()
            if vrun is None:
                vrun = {
                    "start_ns": lo,
                    "end_ns": hi,
                    "min_value": value,
                    "max_value": value,
                }
            else:
                vrun["end_ns"] = hi
                vrun["min_value"] = min(vrun["min_value"], value)
                vrun["max_value"] = max(vrun["max_value"], value)
        else:  # gap: withdrawn | uncovered | blocked
            close_violation_run()
            if grun is not None and grun["state"] == kind:
                grun["end_ns"] = hi
                grun["missing_ancestors"].update(missing)
            else:
                close_gap_run()
                grun = {
                    "start_ns": lo,
                    "end_ns": hi,
                    "state": kind,
                    "missing_ancestors": set(missing),
                }

    close_violation_run()
    close_gap_run()

    blocked_spans = [
        {"start_ns": e.start_ns, "end_ns": e.end_ns, "revision_id": e.revision_id}
        for e in blocked
    ]
    failed = bool(violations) or (missing_policy == MISSING_FAIL and bool(gaps))
    return {
        "batch_id": batch_id,
        "sensor_id": sensor_id,
        "status": "fail" if failed else "pass",
        "scanned_start_ns": points[0] if points else None,
        "scanned_end_ns": points[-1] if points else None,
        "violations": violations,
        "gaps": gaps,
        "timeline_hash": hash_gate_input(segments, blocked_spans),
    }


# ---------------------------------------------------------------------------
# Device release (multi-sensor simultaneous qualification)
# ---------------------------------------------------------------------------

# Per-sensor non-qualification reasons emitted by the release scan.
RELEASE_BLOCKED = "blocked"
RELEASE_UNCOVERED = "uncovered"
RELEASE_WITHDRAWN = "withdrawn"
RELEASE_UNIT_MISMATCH = "unit_mismatch"
RELEASE_OUT_OF_BOUNDS = "out_of_bounds"


@dataclass(frozen=True)
class ReleaseRequirement:
    sensor_id: str
    min_value: float
    max_value: float
    expected_unit: str


def hash_release_input(views: list[dict]) -> str:
    """Stable SHA-256 over a release query's per-sensor scan input.

    Covers every required sensor's folded grounded segments plus its blocked
    revision spans, so the hash changes exactly when the evaluation input
    changes (window, bounds and threshold do not affect it).
    """
    payload = json.dumps(
        [
            {
                "sensor_id": view["req"].sensor_id,
                "segments": [segment_to_dict(s) for s in view["segments"]],
                "blocked": [
                    {
                        "start_ns": e.start_ns,
                        "end_ns": e.end_ns,
                        "revision_id": e.revision_id,
                    }
                    for e in view["blocked"]
                ],
            }
            for view in sorted(views, key=lambda v: v["req"].sensor_id)
        ],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def evaluate_device_release(
    batch_id: str,
    window_start_ns: int,
    window_end_ns: int,
    requirements: list[ReleaseRequirement],
    min_duration_ns: int,
    store: BatchStore,
) -> Optional[dict]:
    """Find when all required sensors simultaneously qualify for release.

    Deterministic sweep over the half-open query window. On every elementary
    interval each sensor is classified against its own requirement; blocked
    coverage takes precedence (backfilling may change the fold winner) and
    invalid (cycle) revisions never participate, exactly as in the sample
    endpoint and quality gate. An interval qualifies only when *every*
    sensor is calibrated, carries the expected unit and lies within its
    inclusive bounds.

    Adjacent elementary intervals merge into runs: qualifying runs merge
    unconditionally; non-qualifying runs merge only while the per-sensor
    reason set is identical (same sensor ids and reasons). Qualifying runs
    reaching ``min_duration_ns`` are released; shorter ones are reported as
    too short. The two output lists together form a contiguous, time-sorted
    partition of the query window.

    Returns ``None`` when any required sensor is unknown in this batch.
    """
    by_sensor = group_by_sensor(store)
    if any(req.sensor_id not in by_sensor for req in requirements):
        return None

    views: list[dict] = []
    points: set[int] = {window_start_ns, window_end_ns}
    for req in requirements:
        sensor_events = by_sensor[req.sensor_id]
        states, depths = classify_sensor(sensor_events)
        segments = build_timeline(
            [e for e in sensor_events.values() if states[e.revision_id] == GROUNDED],
            depths,
        )
        blocked = sorted(
            (e for e in sensor_events.values() if states[e.revision_id] == BLOCKED),
            key=lambda e: (e.start_ns, e.end_ns, e.revision_id),
        )
        for seg in segments:
            points.update((seg.start_ns, seg.end_ns))
        for event in blocked:
            points.update((event.start_ns, event.end_ns))
        views.append(
            {
                "req": req,
                "events": sensor_events,
                "segments": segments,
                "blocked": blocked,
                "seg_index": 0,
            }
        )

    sweep = sorted(p for p in points if window_start_ns < p < window_end_ns)
    boundaries = [window_start_ns, *sweep, window_end_ns]

    released: list[dict] = []
    unreleased: list[dict] = []
    # Open qualifying run: [start, end). Open non-qualifying run carries the
    # aggregated per-sensor reason state keyed by (sensor_id, reason).
    qrun: Optional[list[int]] = None
    nrun: Optional[dict] = None

    def close_qualifying_run(end_ns: int) -> None:
        nonlocal qrun
        if qrun is None:
            return
        duration = end_ns - qrun[0]
        if duration >= min_duration_ns:
            released.append(
                {"start_ns": qrun[0], "end_ns": end_ns, "duration_ns": duration}
            )
        else:
            unreleased.append(
                {
                    "start_ns": qrun[0],
                    "end_ns": end_ns,
                    "duration_ns": duration,
                    "qualified": True,
                    "too_short": True,
                    "sensors": [],
                }
            )
        qrun = None

    def close_non_qualifying_run(end_ns: int) -> None:
        nonlocal nrun
        if nrun is None:
            return
        sensors = [
            {
                "sensor_id": state["sensor_id"],
                "reason": state["reason"],
                "min_value": state["min_value"],
                "max_value": state["max_value"],
                "expected_unit": state["expected_unit"],
                "value": state["value"],
                "unit": state["unit"],
                "observed_units": sorted(state["observed_units"]),
                "observed_min_value": state["observed_min_value"],
                "observed_max_value": state["observed_max_value"],
                "missing_ancestors": sorted(state["missing_ancestors"]),
            }
            for state in nrun["states"].values()
        ]
        sensors.sort(key=lambda s: s["sensor_id"])
        unreleased.append(
            {
                "start_ns": nrun["start_ns"],
                "end_ns": end_ns,
                "duration_ns": end_ns - nrun["start_ns"],
                "qualified": False,
                "too_short": False,
                "sensors": sensors,
            }
        )
        nrun = None

    for lo, hi in zip(boundaries, boundaries[1:]):
        reasons: dict[tuple[str, str], dict] = {}
        for view in views:
            req: ReleaseRequirement = view["req"]
            covering = [
                e for e in view["blocked"] if e.start_ns <= lo < e.end_ns
            ]
            if covering:
                reasons[(req.sensor_id, RELEASE_BLOCKED)] = {
                    "sensor_id": req.sensor_id,
                    "reason": RELEASE_BLOCKED,
                    "min_value": req.min_value,
                    "max_value": req.max_value,
                    "expected_unit": req.expected_unit,
                    "value": None,
                    "unit": None,
                    "observed_units": set(),
                    "observed_min_value": None,
                    "observed_max_value": None,
                    "missing_ancestors": set(
                        _terminal_missing_ancestors(
                            [e.revision_id for e in covering], view["events"]
                        )
                    ),
                }
                continue

            segments = view["segments"]
            idx = view["seg_index"]
            while idx < len(segments) and segments[idx].end_ns <= lo:
                idx += 1
            view["seg_index"] = idx
            if idx < len(segments) and segments[idx].start_ns <= lo:
                seg = segments[idx]
            else:
                seg = None
            if seg is None:
                reason_kind = RELEASE_UNCOVERED
                value = unit = None
            elif seg.state == "withdrawn":
                reason_kind = RELEASE_WITHDRAWN
                value = unit = None
            elif seg.unit != req.expected_unit:
                reason_kind = RELEASE_UNIT_MISMATCH
                value, unit = seg.value, seg.unit
            elif not (req.min_value <= seg.value <= req.max_value):
                reason_kind = RELEASE_OUT_OF_BOUNDS
                value, unit = seg.value, seg.unit
            else:
                continue  # this sensor qualifies on the elementary interval

            reasons[(req.sensor_id, reason_kind)] = {
                "sensor_id": req.sensor_id,
                "reason": reason_kind,
                "min_value": req.min_value,
                "max_value": req.max_value,
                "expected_unit": req.expected_unit,
                "value": value,
                "unit": unit,
                "observed_units": (
                    {unit} if reason_kind == RELEASE_UNIT_MISMATCH else set()
                ),
                "observed_min_value": (
                    value if reason_kind == RELEASE_OUT_OF_BOUNDS else None
                ),
                "observed_max_value": (
                    value if reason_kind == RELEASE_OUT_OF_BOUNDS else None
                ),
                "missing_ancestors": set(),
            }

        if not reasons:
            # Every sensor qualifies on [lo, hi): extend/open a qualifying run.
            close_non_qualifying_run(lo)
            if qrun is None:
                qrun = [lo]
        else:
            close_qualifying_run(lo)
            reason_keys = set(reasons)
            if nrun is not None and nrun["keys"] == reason_keys:
                for key, state in reasons.items():
                    acc = nrun["states"][key]
                    acc["missing_ancestors"].update(state["missing_ancestors"])
                    if state["reason"] == RELEASE_UNIT_MISMATCH:
                        acc["observed_units"].update(state["observed_units"])
                    if state["reason"] == RELEASE_OUT_OF_BOUNDS:
                        acc["observed_min_value"] = min(
                            acc["observed_min_value"], state["observed_min_value"]
                        )
                        acc["observed_max_value"] = max(
                            acc["observed_max_value"], state["observed_max_value"]
                        )
                    # value/unit: keep those of the first elementary span
            else:
                close_non_qualifying_run(lo)
                nrun = {
                    "start_ns": lo,
                    "keys": reason_keys,
                    "states": reasons,
                }

    close_qualifying_run(window_end_ns)
    close_non_qualifying_run(window_end_ns)

    return {
        "batch_id": batch_id,
        "window_start_ns": window_start_ns,
        "window_end_ns": window_end_ns,
        "min_duration_ns": min_duration_ns,
        "status": "released" if released else "not_released",
        "released_intervals": released,
        "unreleased_intervals": unreleased,
        "input_hash": hash_release_input(views),
    }


# ---------------------------------------------------------------------------
# Redundant-sensor trusted-majority quorum
# ---------------------------------------------------------------------------

# Non-voting reasons emitted by the trusted-majority scan. The first four
# mirror the release scan (the sensor cannot cast a calibrated vote); the
# last marks a cast vote excluded from the chosen agreeing set.
MAJORITY_BLOCKED = "blocked"
MAJORITY_UNCOVERED = "uncovered"
MAJORITY_WITHDRAWN = "withdrawn"
MAJORITY_UNIT_MISMATCH = "unit_mismatch"
MAJORITY_VALUE_DIVERGENT = "value_divergent"

MAJORITY_QUALIFIED = "qualified"
MAJORITY_PENDING = "pending_backfill"
MAJORITY_UNQUALIFIED = "unqualified"


def _largest_compatible_set(
    voters: dict[str, float], max_value_difference: float
) -> list[str]:
    """Largest voter subset whose value spread stays within the threshold.

    ``voters`` maps sensor id to its calibrated value. A subset agrees iff
    ``max(values) - min(values) <= max_value_difference``. Every maximum
    compatible subset is the full value band ``{v: lo <= v <= lo + diff}``
    seeded at one distinct voter value ``lo``, so the bands are enumerated by
    ascending seed value and the lexicographically smallest sorted sensor-id
    list wins on cardinality ties (a deterministic, order-independent
    resolution).
    """
    best: list[str] = []
    for lo in sorted(set(voters.values())):
        hi = lo + max_value_difference
        members = sorted(s for s, v in voters.items() if lo <= v <= hi)
        if len(members) > len(best) or (
            len(members) == len(best) and members < best
        ):
            best = members
    return best


def hash_majority_input(views: list[dict]) -> str:
    """Stable SHA-256 over a majority query's per-sensor scan input.

    Covers every queried sensor's folded grounded segments plus its blocked
    revision spans, so the hash changes exactly when the evaluation input
    changes (window, expected unit, quorum and threshold do not affect it).
    """
    payload = json.dumps(
        [
            {
                "sensor_id": sensor_id,
                "segments": [
                    segment_to_dict(s) for s in view["segments"]
                ],
                "blocked": [
                    {
                        "start_ns": e.start_ns,
                        "end_ns": e.end_ns,
                        "revision_id": e.revision_id,
                    }
                    for e in view["blocked"]
                ],
            }
            for sensor_id, view in sorted(views.items())
        ],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def evaluate_sensor_majority(
    batch_id: str,
    window_start_ns: int,
    window_end_ns: int,
    sensor_ids: list[str],
    expected_unit: str,
    q: int,
    max_value_difference: float,
    store: BatchStore,
) -> Optional[dict]:
    """Decide when redundant sensors form a trusted majority.

    Deterministic sweep over the half-open query window, reusing the same
    causal-fold snapshot and blocked-coverage precedence as the sample,
    quality-gate and release scans (invalid cycle revisions never
    participate). On each elementary interval a sensor casts a vote only when
    its landed value is *not* masked by blocked coverage, the folded winner
    is ``calibrated`` and its unit equals ``expected_unit``. Votes agreeing
    within ``max_value_difference`` form compatible sets; the most-populous
    one is chosen (ties: lexicographically smallest sorted member ids).

    The interval is ``qualified`` when the chosen set has at least ``q``
    votes. Otherwise, when that set plus the currently blocked sensors could
    still reach ``q`` after backfill, it is ``pending_backfill`` and the
    blocked sensors' terminal missing ancestors are listed; anything left is
    ``unqualified``. Adjacent elementary intervals merge only when the
    verdict and the complete evidence (members and per-sensor reasons) are
    identical, so the output is a contiguous, time-sorted partition of the
    query window.

    Returns ``None`` when any queried sensor is unknown in this batch.
    """
    by_sensor = group_by_sensor(store)
    if any(sensor_id not in by_sensor for sensor_id in sensor_ids):
        return None

    views: dict[str, dict] = {}
    points: set[int] = {window_start_ns, window_end_ns}
    for sensor_id in sensor_ids:
        sensor_events = by_sensor[sensor_id]
        states, depths = classify_sensor(sensor_events)
        segments = build_timeline(
            [e for e in sensor_events.values() if states[e.revision_id] == GROUNDED],
            depths,
        )
        blocked = sorted(
            (e for e in sensor_events.values() if states[e.revision_id] == BLOCKED),
            key=lambda e: (e.start_ns, e.end_ns, e.revision_id),
        )
        for seg in segments:
            points.update((seg.start_ns, seg.end_ns))
        for event in blocked:
            points.update((event.start_ns, event.end_ns))
        views[sensor_id] = {
            "events": sensor_events,
            "segments": segments,
            "blocked": blocked,
            "seg_index": 0,
        }

    sweep = sorted(p for p in points if window_start_ns < p < window_end_ns)
    boundaries = [window_start_ns, *sweep, window_end_ns]

    raw_intervals: list[dict] = []
    for lo, hi in zip(boundaries, boundaries[1:]):
        voters: dict[str, float] = {}
        reasons: dict[str, dict] = {}
        blocked_sensors: set[str] = set()
        blocked_missing: set[str] = set()

        for sensor_id in sensor_ids:
            view = views[sensor_id]
            covering = [
                e for e in view["blocked"] if e.start_ns <= lo < e.end_ns
            ]
            if covering:
                # Blocked coverage masks the landed value: the sensor cannot
                # vote yet, but backfilling may change the folded winner.
                missing = _terminal_missing_ancestors(
                    [e.revision_id for e in covering], view["events"]
                )
                blocked_sensors.add(sensor_id)
                blocked_missing.update(missing)
                reasons[sensor_id] = {
                    "sensor_id": sensor_id,
                    "reason": MAJORITY_BLOCKED,
                    "value": None,
                    "unit": None,
                    "missing_ancestors": missing,
                }
                continue

            segments = view["segments"]
            idx = view["seg_index"]
            while idx < len(segments) and segments[idx].end_ns <= lo:
                idx += 1
            view["seg_index"] = idx
            seg = (
                segments[idx]
                if idx < len(segments) and segments[idx].start_ns <= lo
                else None
            )
            if seg is None:
                reason_kind, value, unit = MAJORITY_UNCOVERED, None, None
            elif seg.state == "withdrawn":
                reason_kind, value, unit = MAJORITY_WITHDRAWN, None, None
            elif seg.unit != expected_unit:
                reason_kind, value, unit = MAJORITY_UNIT_MISMATCH, seg.value, seg.unit
            else:
                voters[sensor_id] = seg.value
                continue
            reasons[sensor_id] = {
                "sensor_id": sensor_id,
                "reason": reason_kind,
                "value": value,
                "unit": unit,
                "missing_ancestors": [],
            }

        chosen = _largest_compatible_set(voters, max_value_difference)
        chosen_set = set(chosen)
        # Every cast vote excluded from the chosen set diverges on value.
        for sensor_id, value in voters.items():
            if sensor_id not in chosen_set:
                reasons[sensor_id] = {
                    "sensor_id": sensor_id,
                    "reason": MAJORITY_VALUE_DIVERGENT,
                    "value": value,
                    "unit": expected_unit,
                    "missing_ancestors": [],
                }

        if len(chosen) >= q:
            verdict = MAJORITY_QUALIFIED
        elif len(chosen) + len(blocked_sensors) >= q:
            verdict = MAJORITY_PENDING
        else:
            verdict = MAJORITY_UNQUALIFIED

        members = [
            {"sensor_id": s, "value": voters[s], "unit": expected_unit}
            for s in chosen
        ]
        non_voting = [reasons[s] for s in sorted(reasons)]
        for reason in non_voting:
            reason["missing_ancestors"] = sorted(reason["missing_ancestors"])
        raw_intervals.append(
            {
                "start_ns": lo,
                "end_ns": hi,
                "verdict": verdict,
                "members": members,
                "non_voting_sensors": non_voting,
                # Interval-level union of blocked ancestors; only blocked
                # sensors contribute, so this is empty without blocked masks.
                "missing_ancestors": sorted(blocked_missing),
            }
        )

    # Merge adjacent spans only when verdict and the full evidence are equal.
    intervals: list[dict] = []
    for span in raw_intervals:
        if (
            intervals
            and intervals[-1]["end_ns"] == span["start_ns"]
            and _majority_signature(intervals[-1]) == _majority_signature(span)
        ):
            intervals[-1]["end_ns"] = span["end_ns"]
        else:
            intervals.append(dict(span))
    for span in intervals:
        span["duration_ns"] = span["end_ns"] - span["start_ns"]

    if any(span["verdict"] == MAJORITY_QUALIFIED for span in intervals):
        status = MAJORITY_QUALIFIED
    elif any(span["verdict"] == MAJORITY_PENDING for span in intervals):
        status = MAJORITY_PENDING
    else:
        status = MAJORITY_UNQUALIFIED

    return {
        "batch_id": batch_id,
        "window_start_ns": window_start_ns,
        "window_end_ns": window_end_ns,
        "expected_unit": expected_unit,
        "q": q,
        "max_value_difference": max_value_difference,
        "sensors": list(sensor_ids),
        "status": status,
        "intervals": intervals,
        "input_hash": hash_majority_input(views),
    }


def _majority_signature(span: dict) -> tuple:
    """Hashable identity of a span's verdict and complete evidence.

    Two adjacent spans merge exactly when their verdicts, member sets
    (sensor/value/unit), per-sensor non-voting reasons and missing ancestors
    all agree.
    """
    return (
        span["verdict"],
        tuple(
            (m["sensor_id"], m["value"], m["unit"]) for m in span["members"]
        ),
        tuple(
            (
                r["sensor_id"],
                r["reason"],
                r["value"],
                r["unit"],
                tuple(r["missing_ancestors"]),
            )
            for r in span["non_voting_sensors"]
        ),
        tuple(span["missing_ancestors"]),
    )


# ---------------------------------------------------------------------------
# Batch sealing (immutable archival evidence)
# ---------------------------------------------------------------------------


def revision_to_dict(event: StoredEvent) -> dict:
    """Plain-data snapshot of one stored revision, for seal evidence."""
    return {
        "sensor_id": event.sensor_id,
        "revision_id": event.revision_id,
        "parent_id": event.parent_id,
        "idempotency_key": event.idempotency_key,
        "start_ns": event.start_ns,
        "end_ns": event.end_ns,
        "value": None if event.value is None else float(event.value),
        "unit": event.unit,
        "withdrawn": event.withdrawn,
    }


def build_seal_evidence(batch_id: str, store: BatchStore) -> dict:
    """Deterministic sealable evidence of one batch.

    Captures every stored revision (sorted by sensor then revision id, so
    the document is independent of arrival order) plus the folded batch
    result evaluated on the same snapshot. The document contains only plain
    data, fully decoupled from the live store.
    """
    revisions = [
        revision_to_dict(event)
        for event in sorted(
            store.events.values(), key=lambda e: (e.sensor_id, e.revision_id)
        )
    ]
    return {
        "batch_id": batch_id,
        "revisions": revisions,
        "result": evaluate_batch(batch_id, store),
    }


def hash_seal_evidence(evidence: dict) -> str:
    """Stable SHA-256 over a seal evidence document.

    Covers both the frozen revisions and the folded batch result, so the
    digest changes exactly when the sealed evidence changes.
    """
    payload = json.dumps(
        evidence, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
