"""Pydantic schemas for the calibration timeline service.

Time coordinates are nanoseconds since an arbitrary batch-local epoch.
Intervals are half-open ``[start_ns, end_ns)`` so that abutting fragments do
not share any sample instant.
"""

from typing import Annotated, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

# A revision root uses this literal as its (missing) parent.
ROOT_PARENT = "ROOT"

VersionId = Annotated[str, Field(min_length=1, max_length=200)]
BatchId = Annotated[str, Field(min_length=1, max_length=200)]
SensorId = Annotated[str, Field(min_length=1, max_length=200)]
IdemKey = Annotated[str, Field(min_length=1, max_length=200)]
UnitName = Annotated[str, Field(min_length=1, max_length=40)]
SealId = Annotated[str, Field(min_length=1, max_length=200)]
# SHA-256 hex digest, exactly as emitted in a batch result's timeline_hash.
TimelineHash = Annotated[str, Field(pattern="^[0-9a-f]{64}$")]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MeasurementEvent(StrictModel):
    """One measurement-fragment revision delivered to the ingest endpoint."""

    batch_id: BatchId
    sensor_id: SensorId
    revision_id: VersionId
    # Parent revision for this sensor; the literal "ROOT" marks a causal root.
    parent_id: VersionId
    # Idempotency key; the same event payload is accepted exactly once in
    # effect and every repeated submission is acknowledged as a duplicate.
    idempotency_key: IdemKey
    start_ns: int = Field(ge=0)
    end_ns: int = Field(ge=1)
    # Calibration value for the fragment; required unless the fragment is
    # withdrawn (withdrawn fragments erase instead of writing a value).
    value: Optional[float] = None
    unit: Optional[str] = Field(default=None, max_length=40)
    withdrawn: bool = False

    @model_validator(mode="after")
    def _check_interval_and_value(self) -> "MeasurementEvent":
        if self.revision_id == ROOT_PARENT:
            raise ValueError(f"revision_id may not be the reserved value {ROOT_PARENT!r}")
        if self.end_ns <= self.start_ns:
            raise ValueError("end_ns must be strictly greater than start_ns")
        if not self.withdrawn and self.value is None:
            raise ValueError("value is required when withdrawn is false")
        return self


class TimelineSegment(StrictModel):
    start_ns: int
    end_ns: int
    state: Literal["calibrated", "withdrawn"]
    # Populated for calibrated segments; null on withdrawn (erased) spans.
    value: Optional[float] = None
    unit: Optional[str] = None
    revision_id: str


class Gap(StrictModel):
    """A missing revision that blocks one or more known children."""

    batch_id: str
    sensor_id: str
    missing_revision_id: str
    awaited_by: list[str]
    # Revision that referenced the missing parent (may repeat across gaps).
    referenced_from: str


class SensorTimeline(StrictModel):
    sensor_id: str
    grounded: bool
    segments: list[TimelineSegment]
    segments_hash: str
    covered_start_ns: Optional[int] = None
    covered_end_ns: Optional[int] = None


class BatchResult(StrictModel):
    batch_id: str
    sensors: list[SensorTimeline]
    gaps: list[Gap]
    revisions_total: int
    revisions_grounded: int
    revisions_blocked: int
    revisions_invalid: int
    # True when every known revision is grounded (no gaps, no cycles).
    complete: bool
    timeline_hash: str


class SamplePointResponse(StrictModel):
    """Effective calibration state at one queried nanosecond of one sensor."""

    batch_id: str
    sensor_id: str
    timestamp_ns: int
    # calibrated: a grounded, value-bearing segment wins the fold at the point;
    # withdrawn:   a tombstone segment wins (value/unit are null);
    # uncovered:   no grounded segment covers the instant;
    # blocked:     a revision covering the instant is waiting on missing
    #              ancestors, so the eventual winner cannot yet be determined.
    state: Literal["calibrated", "withdrawn", "uncovered", "blocked"]
    # Populated only for calibrated points; null otherwise.
    value: Optional[float] = None
    unit: Optional[str] = None
    # Winning revision (last applied in deterministic fold order); null unless
    # the state is calibrated/withdrawn.
    revision_id: Optional[str] = None
    # Ordered parent chain from revision_id back to the causal root, e.g.
    # ["v3", "v2", "v1", "ROOT"]; empty unless the point is resolved.
    parent_chain: list[str] = Field(default_factory=list)
    # Distinct not-yet-uploaded revisions that block every covering revision;
    # populated only for blocked points (sorted, ready for backfill).
    missing_ancestors: list[str] = Field(default_factory=list)


class CompareSide(StrictModel):
    """Effective folded state one side contributes to a compare interval."""

    state: Literal["calibrated", "withdrawn"]
    # Populated for calibrated spans; null on withdrawn (erased) spans.
    value: Optional[float] = None
    unit: Optional[str] = None


class CompareInterval(StrictModel):
    """One half-open diff interval over the union of both folded timelines."""

    start_ns: int
    end_ns: int
    # equal:          both sides agree (both uncovered counts as agreement);
    # left_only:      only the left batch covers the span;
    # right_only:     only the right batch covers the span;
    # value_mismatch: both calibrated but value or unit differ;
    # state_mismatch: one side calibrated, the other withdrawn.
    kind: Literal["equal", "left_only", "right_only", "value_mismatch", "state_mismatch"]
    # Effective payload of each side on this span; null where uncovered.
    left: Optional[CompareSide] = None
    right: Optional[CompareSide] = None


class IncompleteReason(StrictModel):
    """Why one side of a comparison cannot be considered final yet."""

    side: Literal["left", "right"]
    batch_id: str
    # gaps:              revisions blocked by not-yet-uploaded ancestors;
    # invalid_revisions: revisions in or below a parent cycle.
    reason: Literal["gaps", "invalid_revisions"]
    detail: str
    # Blocked or invalid revisions excluded from the fold (sorted).
    affected_revisions: list[str] = Field(default_factory=list)
    # For reason="gaps": distinct missing parents to backfill (sorted).
    missing_revision_ids: list[str] = Field(default_factory=list)


class CompareResponse(StrictModel):
    """Half-open interval diff of one sensor across two batches."""

    sensor_id: str
    left_batch_id: str
    right_batch_id: str
    # Hashes of each side's folded segments (matching the per-sensor
    # segments_hash of the batch result) for cross-checking.
    left_segments_hash: str
    right_segments_hash: str
    # True when either side has gaps or invalid revisions, so backfilling
    # may still change the diff.
    incomplete: bool
    incomplete_reasons: list[IncompleteReason] = Field(default_factory=list)
    # Contiguous, time-sorted partition of the union coverage span;
    # adjacent intervals merge when kind and both side payloads match.
    intervals: list[CompareInterval] = Field(default_factory=list)


class QualityGateRequest(StrictModel):
    """Parameters for one quality-gate evaluation of a folded sensor timeline."""

    sensor_id: SensorId
    # Inclusive allowed-value bounds: a calibrated value v passes iff
    # min_value <= v <= max_value. NaN/inf bounds are rejected.
    min_value: float = Field(
        allow_inf_nan=False,
        description="Inclusive lower bound of the allowed calibrated value.",
    )
    max_value: float = Field(
        allow_inf_nan=False,
        description="Inclusive upper bound of the allowed calibrated value.",
    )
    # A contiguous out-of-bounds run is reported as a violation only when
    # its duration reaches this many nanoseconds.
    min_duration_ns: int = Field(
        ge=1,
        description="Minimum duration (ns) an out-of-bounds run must reach to "
        "be reported as a violation.",
    )
    # How missing-data gaps (withdrawn/uncovered/blocked) affect the verdict:
    # "ignore" only records them; "fail" fails the gate when any gap exists.
    missing_policy: Literal["ignore", "fail"] = Field(
        description="ignore: record gaps without failing; fail: any gap fails the gate.",
    )

    @model_validator(mode="after")
    def _check_bounds(self) -> "QualityGateRequest":
        if self.min_value > self.max_value:
            raise ValueError("min_value must be less than or equal to max_value")
        return self


class QualityGateViolation(StrictModel):
    """One merged out-of-bounds run whose duration reached the threshold."""

    start_ns: int
    end_ns: int
    duration_ns: int
    # Extreme calibrated values observed inside the run.
    min_value: float
    max_value: float


class QualityGateGap(StrictModel):
    """One merged missing-data span; gaps never merge into violation runs."""

    start_ns: int
    end_ns: int
    duration_ns: int
    state: Literal["withdrawn", "uncovered", "blocked"]
    # Distinct not-yet-uploaded terminal ancestors; populated for blocked
    # spans (sorted, ready for backfill), empty otherwise.
    missing_ancestors: list[str] = Field(default_factory=list)


class QualityGateResponse(StrictModel):
    """Deterministic quality-gate verdict for one sensor of one batch."""

    batch_id: str
    sensor_id: str
    status: Literal["pass", "fail"]
    # Half-open scan window: the union span of grounded and blocked
    # revisions; both null when nothing is scannable (e.g. all revisions
    # are invalid cycle members).
    scanned_start_ns: Optional[int] = None
    scanned_end_ns: Optional[int] = None
    violations: list[QualityGateViolation] = Field(default_factory=list)
    gaps: list[QualityGateGap] = Field(default_factory=list)
    # SHA-256 over the evaluated sensor timeline (folded segments plus
    # blocked spans); changes iff the gate input changes.
    timeline_hash: str


class ReleaseSensorRequirement(StrictModel):
    """Per-sensor acceptance condition for a device-release query."""

    sensor_id: SensorId
    # Inclusive calibrated-value bounds; a folded value v qualifies iff
    # min_value <= v <= max_value (NaN/inf bounds are rejected).
    min_value: float = Field(
        allow_inf_nan=False,
        description="Inclusive lower bound of the acceptable calibrated value.",
    )
    max_value: float = Field(
        allow_inf_nan=False,
        description="Inclusive upper bound of the acceptable calibrated value.",
    )
    # Unit the folded calibrated value must carry exactly.
    expected_unit: UnitName

    @model_validator(mode="after")
    def _check_bounds(self) -> "ReleaseSensorRequirement":
        if self.min_value > self.max_value:
            raise ValueError("min_value must be less than or equal to max_value")
        return self


class ReleaseRequest(StrictModel):
    """Parameters of one read-only device-release evaluation of a batch.

    The query window is the half-open nanosecond interval
    ``[window_start_ns, window_end_ns)``. All listed sensors must qualify
    simultaneously throughout a span for it to be releasable; a qualifying
    span reaches release only when its duration reaches
    ``min_duration_ns``.
    """

    window_start_ns: int = Field(ge=0)
    window_end_ns: int = Field(ge=1)
    # Every required sensor of the batch; must be non-empty and duplicate-free.
    sensors: list[ReleaseSensorRequirement] = Field(min_length=1)
    # Minimum contiguous duration (ns, >= 1) a fully qualifying run must
    # reach to be reported as a released interval.
    min_duration_ns: int = Field(ge=1)

    @model_validator(mode="after")
    def _check_window_and_sensors(self) -> "ReleaseRequest":
        if self.window_end_ns <= self.window_start_ns:
            raise ValueError(
                "window_end_ns must be strictly greater than window_start_ns"
            )
        ids = [s.sensor_id for s in self.sensors]
        if len(ids) != len(set(ids)):
            raise ValueError("sensors must not contain duplicate sensor_id entries")
        return self


class ReleaseSensorReason(StrictModel):
    """Why one sensor fails a non-qualifying release span.

    reason:
      blocked   - a not-yet-grounded revision covers the span (backfill may
                  change the folded winner); see missing_ancestors;
      uncovered - no grounded segment covers the span;
      withdrawn - the folded winner is a withdrawal tombstone;
      unit_mismatch - folded unit is not the expected unit;
      out_of_bounds - folded value lies outside the inclusive bounds.
    """

    sensor_id: str
    reason: Literal[
        "blocked", "uncovered", "withdrawn", "unit_mismatch", "out_of_bounds"
    ]
    # Inclusive bounds / expected unit of the queried requirement.
    min_value: float
    max_value: float
    expected_unit: str
    # Folded effective payload on the span; null unless the sensor is
    # calibrated there (blocked/withdrawn/uncovered expose no value).
    value: Optional[float] = None
    unit: Optional[str] = None
    # Distinct folded units observed inside the merged span; populated for
    # unit_mismatch spans (sorted), empty otherwise.
    observed_units: list[str] = Field(default_factory=list)
    # Extreme calibrated values observed inside the merged span; populated
    # for out_of_bounds spans, null otherwise.
    observed_min_value: Optional[float] = None
    observed_max_value: Optional[float] = None
    # Distinct not-yet-uploaded terminal ancestors; populated for blocked
    # spans (sorted, ready for backfill), empty otherwise.
    missing_ancestors: list[str] = Field(default_factory=list)


class ReleasedInterval(StrictModel):
    """One merged half-open span where every required sensor qualifies."""

    start_ns: int
    end_ns: int
    duration_ns: int


class UnreleasedInterval(StrictModel):
    """One merged half-open span that cannot be released.

    Either at least one sensor fails the span (``qualified=false``,
    ``sensors`` lists the failing reasons), or every sensor qualifies but
    the contiguous run is shorter than ``min_duration_ns``
    (``qualified=true``, ``too_short=true``).
    """

    start_ns: int
    end_ns: int
    duration_ns: int
    qualified: bool
    too_short: bool = False
    sensors: list[ReleaseSensorReason] = Field(default_factory=list)


class ReleaseResponse(StrictModel):
    """Deterministic device-release verdict for one batch over one window."""

    batch_id: str
    window_start_ns: int
    window_end_ns: int
    min_duration_ns: int
    # "released": at least one contiguous qualifying run reached the minimum
    # duration; otherwise "not_released".
    status: Literal["released", "not_released"]
    # Contiguous, time-sorted, merged qualifying spans that reached the
    # duration threshold, clipped to the half-open query window.
    released_intervals: list[ReleasedInterval] = Field(default_factory=list)
    # Every other span of the query window, in the same ordering: merged
    # non-qualifying spans (with per-sensor reasons) and qualifying runs
    # that were too short. Together with released_intervals this is a
    # contiguous partition of the whole query window.
    unreleased_intervals: list[UnreleasedInterval] = Field(default_factory=list)
    # SHA-256 over the evaluation input (required sensors' folded segments
    # plus blocked spans); changes iff the snapshot input changes.
    input_hash: str


# ---------------------------------------------------------------------------
# Redundant-sensor trusted-majority quorum
# ---------------------------------------------------------------------------


class MajoritySensorSpec(StrictModel):
    """One redundant sensor participating in a trusted-majority vote."""

    sensor_id: SensorId


class MajorityRequest(StrictModel):
    """Parameters of one read-only trusted-majority evaluation of a batch.

    At least two distinct redundant sensors vote over the half-open window
    ``[window_start_ns, window_end_ns)``. ``q`` is the strict-majority quorum:
    it must be strictly more than half of the sensors and at most their total
    count. A span qualifies only when ``q`` voting values agree within
    ``max_value_difference`` (max - min).
    """

    window_start_ns: int = Field(ge=0)
    window_end_ns: int = Field(ge=1)
    # Redundant sensors; at least two and duplicate-free.
    sensors: list[MajoritySensorSpec] = Field(min_length=2)
    # Unit every voting (calibrated) folded value must carry exactly.
    expected_unit: UnitName
    # Required agreeing votes: floor(n/2) + 1 <= q <= n (strict majority).
    q: int = Field(ge=1)
    # Non-negative finite inclusive spread allowed among agreeing votes:
    # max(value) - min(value) <= max_value_difference.
    max_value_difference: float = Field(
        allow_inf_nan=False,
        ge=0.0,
        description="Inclusive maximum spread (max - min) of agreeing votes.",
    )

    @model_validator(mode="after")
    def _check_window_sensors_and_quorum(self) -> "MajorityRequest":
        if self.window_end_ns <= self.window_start_ns:
            raise ValueError(
                "window_end_ns must be strictly greater than window_start_ns"
            )
        ids = [s.sensor_id for s in self.sensors]
        if len(ids) != len(set(ids)):
            raise ValueError("sensors must not contain duplicate sensor_id entries")
        strict_majority = len(ids) // 2 + 1
        if not (strict_majority <= self.q <= len(ids)):
            raise ValueError(
                f"q must be a strict majority: {strict_majority} <= q <= "
                f"{len(ids)} for {len(ids)} sensors"
            )
        return self


class MajorityMember(StrictModel):
    """One sensor whose calibrated vote belongs to the reported agreeing set."""

    sensor_id: str
    value: float
    unit: str


class MajorityNonVotingReason(StrictModel):
    """Why one sensor is absent from the reported agreeing member set.

    reason:
      blocked        - a not-yet-grounded revision covers the span and masks
                       the sensor's landed value (see missing_ancestors);
      uncovered      - no grounded segment covers the span;
      withdrawn      - the folded winner is a withdrawal tombstone;
      unit_mismatch  - the folded value carries a different unit;
      value_divergent - the sensor voted, but its value is outside the chosen
                       maximum-cardinality agreeing set.
    """

    sensor_id: str
    reason: Literal[
        "blocked", "uncovered", "withdrawn", "unit_mismatch", "value_divergent"
    ]
    # Folded value/unit on the span; null unless the sensor is calibrated there
    # (blocked/withdrawn/uncovered expose no value).
    value: Optional[float] = None
    unit: Optional[str] = None
    # Distinct not-yet-uploaded terminal ancestors; populated for blocked
    # sensors (sorted, ready for backfill), empty otherwise.
    missing_ancestors: list[str] = Field(default_factory=list)


class MajorityInterval(StrictModel):
    """One merged half-open span with one trusted-majority verdict.

    verdict:
      qualified        - the largest agreeing voter set reaches q votes;
      pending_backfill - it does not, but together with the currently blocked
                         sensors the quorum is reachable after backfill;
      unqualified      - the quorum is unreachable even after backfill.
    """

    start_ns: int
    end_ns: int
    duration_ns: int
    verdict: Literal["qualified", "pending_backfill", "unqualified"]
    # qualified: the chosen most-populous agreeing set (>= q members);
    # otherwise: the largest agreeing set of currently voting sensors (< q).
    # Ties resolve to the lexicographically smallest sorted sensor-id list.
    members: list[MajorityMember] = Field(default_factory=list)
    # Every queried sensor not in ``members`` with its reason, sorted by
    # sensor_id: blocked / uncovered / withdrawn / unit_mismatch / divergent.
    non_voting_sensors: list[MajorityNonVotingReason] = Field(default_factory=list)
    # Union of the blocked sensors' distinct terminal missing ancestors
    # (sorted, ready for backfill); non-empty mainly for pending_backfill.
    missing_ancestors: list[str] = Field(default_factory=list)


class MajorityResponse(StrictModel):
    """Deterministic trusted-majority verdict for one batch over one window."""

    batch_id: str
    window_start_ns: int
    window_end_ns: int
    expected_unit: str
    q: int
    max_value_difference: float
    # Queried sensor ids, echoed in request order.
    sensors: list[str]
    # qualified:        at least one span reaches q agreeing votes;
    # pending_backfill: no qualified span but at least one span waits on
    #                   backfill to possibly reach q;
    # unqualified:      every span stays below q even after backfill.
    status: Literal["qualified", "pending_backfill", "unqualified"]
    # Contiguous, time-sorted partition of the whole half-open query window;
    # adjacent spans merge only when verdict and the full evidence match.
    intervals: list[MajorityInterval] = Field(default_factory=list)
    # SHA-256 over the evaluation input (queried sensors' folded segments plus
    # blocked spans); changes iff the snapshot input changes.
    input_hash: str


class IngestResponse(StrictModel):
    status: Literal["accepted", "duplicate", "conflict"]
    idempotency_key: str
    batch_id: str
    sensor_id: str
    revision_id: str
    result: BatchResult


class PreviewChangeInterval(StrictModel):
    """One half-open span whose effective state/value/unit changes if submitted.

    Revision provenance is ignored: a span whose only difference is the
    winning ``revision_id`` is not reported.
    """

    start_ns: int
    end_ns: int
    # added:   uncovered before, covered after;
    # removed: covered before, uncovered after;
    # changed: covered on both sides but state/value/unit differ.
    kind: Literal["added", "removed", "changed"]
    # Effective payload on the span before/after the hypothetical submission;
    # null where that side is uncovered.
    before: Optional[CompareSide] = None
    after: Optional[CompareSide] = None


class PreviewResponse(StrictModel):
    """Non-mutating impact preview of one measurement-fragment revision."""

    # submittable: the event would be newly accepted;
    # duplicate:   the exact event (or its revision content) is already stored.
    status: Literal["submittable", "duplicate"]
    idempotency_key: str
    batch_id: str
    sensor_id: str
    revision_id: str
    # Batch timeline hash before and after the hypothetical submission.
    before_timeline_hash: str
    after_timeline_hash: str
    # Full deterministic batch results for both snapshots, for convenience.
    before: BatchResult
    after: BatchResult
    # Half-open intervals of this sensor whose effective state/value/unit
    # changes (provenance-only changes are excluded); empty for duplicates.
    changed_intervals: list[PreviewChangeInterval] = Field(default_factory=list)
    # Missing-parent gaps that would appear / disappear for this sensor.
    gaps_added: list[Gap] = Field(default_factory=list)
    gaps_removed: list[Gap] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Batch sealing (immutable archival evidence)
# ---------------------------------------------------------------------------


class SealRequest(StrictModel):
    """Parameters of one batch-seal request.

    The caller supplies the ``timeline_hash`` obtained from an earlier batch
    result; the seal is created only when the batch still evaluates to that
    exact hash and is complete (no blocked or invalid revisions), both
    checked against one consistent batch snapshot.
    """

    batch_id: BatchId
    # Caller-chosen seal identifier, unique per batch.
    seal_id: SealId
    # Batch timeline hash the caller verified before sealing.
    timeline_hash: TimelineHash


class SealedRevision(StrictModel):
    """One stored revision frozen into a seal's evidence."""

    sensor_id: str
    revision_id: str
    parent_id: str
    idempotency_key: str
    start_ns: int
    end_ns: int
    # Populated for value-bearing revisions; null on withdrawn tombstones.
    value: Optional[float] = None
    unit: Optional[str] = None
    withdrawn: bool


class SealEvidence(StrictModel):
    """Immutable snapshot frozen by a seal: all revisions + folded result."""

    batch_id: str
    # Every revision stored at seal time, sorted by (sensor_id, revision_id).
    revisions: list[SealedRevision]
    # The deterministic batch result evaluated at seal time.
    result: BatchResult


class SealResponse(StrictModel):
    """Result of a seal request (newly sealed or an exact replay)."""

    # sealed:    the seal was created by this request;
    # duplicate: an identical request was already sealed; the original
    #            snapshot is returned unchanged.
    status: Literal["sealed", "duplicate"]
    batch_id: str
    seal_id: str
    # Batch timeline hash verified at seal time (echo of the request).
    timeline_hash: str
    # SHA-256 over the frozen evidence (all revisions + folded result).
    evidence_digest: str
    evidence: SealEvidence


class SealRecallResponse(StrictModel):
    """A stored seal plus whether the live batch still matches it."""

    batch_id: str
    seal_id: str
    # SHA-256 over the frozen evidence (all revisions + folded result).
    evidence_digest: str
    evidence: SealEvidence
    # Digest recomputed over the batch's current revisions + folded result;
    # null when the batch no longer exists.
    current_evidence_digest: Optional[str] = None
    # True when the live batch evidence is still identical to the sealed one.
    consistent: bool


class ResetResponse(StrictModel):
    status: Literal["reset"]
    batches_removed: int
    events_removed: int
    seals_removed: int


class HealthResponse(StrictModel):
    status: Literal["ok"]
    # Backend actually serving state: "sqlite" (durable, shared) or "memory".
    storage: Literal["memory", "sqlite"]
    version: str
