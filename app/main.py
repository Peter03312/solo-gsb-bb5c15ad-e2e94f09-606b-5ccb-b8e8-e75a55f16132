"""FastAPI application: ingest, preview, query, evaluation, seal and reset endpoints."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from . import __version__
from .config import get_settings
from .engine import (
    ReleaseRequirement,
    build_seal_evidence,
    compare_sensor,
    evaluate_batch,
    evaluate_device_release,
    evaluate_quality_gate,
    evaluate_sample,
    evaluate_sensor_majority,
    find_sensor_events,
    hash_seal_evidence,
    preview_submission,
)
from .models import (
    BatchResult,
    CompareResponse,
    HealthResponse,
    IngestResponse,
    MajorityRequest,
    MajorityResponse,
    MeasurementEvent,
    PreviewResponse,
    QualityGateRequest,
    QualityGateResponse,
    ReleaseRequest,
    ReleaseResponse,
    ResetResponse,
    SamplePointResponse,
    SealEvidence,
    SealRecallResponse,
    SealRequest,
    SealResponse,
)
from .storage import (
    BatchStore,
    IdempotencyConflict,
    SealRecord,
    StorageError,
    StoredEvent,
    create_store,
)

logger = logging.getLogger("calibration")

settings = get_settings()
# The store is created at startup. An unusable durable backend (bad path,
# unwritable directory) raises StorageError here so the process fails fast
# instead of silently serving an empty, non-persistent state.
store = create_store(settings.storage, settings.sqlite_path)


@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    logging.basicConfig(level=settings.log_level.upper())
    logger.info(
        "calibration-timeline-service %s started on %s:%s (storage=%s%s)",
        __version__,
        settings.host,
        settings.port,
        store.backend,
        f", path={settings.sqlite_path}" if store.backend == "sqlite" else "",
    )
    yield
    logger.info("calibration-timeline-service shutting down")


app = FastAPI(
    title="Factory Calibration Timeline Service",
    version=__version__,
    description=(
        "Ingests out-of-order, overlapping and withdrawable measurement-fragment "
        "revisions for a sensor in a calibration batch and produces a deterministic "
        "effective calibration timeline with backfillable gap reports."
    ),
    lifespan=lifespan,
)


@app.exception_handler(IdempotencyConflict)
async def conflict_handler(_: Request, exc: IdempotencyConflict) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": (
                f"idempotency_key {exc.args[0]!r} was already used with a "
                "different event payload"
            )
        },
    )


@app.exception_handler(StorageError)
async def storage_error_handler(_: Request, exc: StorageError) -> JSONResponse:
    # A backend failure must never be reported as an empty batch: surface it
    # as a clear 503 so clients can retry instead of accepting fabricated
    # "not found"/"empty" results.
    logger.error("storage failure: %s", exc)
    return JSONResponse(
        status_code=503,
        content={"detail": f"storage unavailable: {exc}"},
    )


@app.exception_handler(RequestValidationError)
async def validation_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    # The default FastAPI handler echoes the rejected input verbatim; echoing
    # a NaN/Inf float raises during JSON serialization and turns a client
    # parameter error into a 500. Encode exactly like the default handler,
    # then replace non-finite floats with string placeholders so malformed
    # floats are reported as 422 like any other parameter error.
    detail = jsonable_encoder(exc.errors())
    return JSONResponse(
        status_code=422, content={"detail": _sanitize_errors(detail)}
    )


def _sanitize_errors(value: object) -> object:
    if isinstance(value, float):
        if value == value and value not in (float("inf"), float("-inf")):
            return value
        if value != value:
            return "<NaN>"
        return "<Infinity>" if value > 0 else "<-Infinity>"
    if isinstance(value, dict):
        return {key: _sanitize_errors(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_errors(item) for item in value]
    return value


@app.get("/health", response_model=HealthResponse, tags=["ops"])
def health() -> HealthResponse:
    return HealthResponse(status="ok", storage=store.backend, version=__version__)


@app.post(
    "/api/v1/batches/{batch_id}/events",
    response_model=IngestResponse,
    status_code=200,
    tags="calibration",
    summary="Submit one measurement-fragment revision",
)
def submit_event(batch_id: str, event: MeasurementEvent) -> IngestResponse:
    if event.batch_id != batch_id:
        raise HTTPException(
            status_code=422,
            detail="batch_id in path and body must match",
        )
    stored = StoredEvent(
        batch_id=event.batch_id,
        sensor_id=event.sensor_id,
        revision_id=event.revision_id,
        parent_id=event.parent_id,
        idempotency_key=event.idempotency_key,
        start_ns=event.start_ns,
        end_ns=event.end_ns,
        value=None if event.withdrawn else float(event.value),
        unit=event.unit,
        withdrawn=event.withdrawn,
    )
    # One cross-process transaction for the whole decision + evaluation:
    # concurrent submissions serialize, the revision and its idempotency-key
    # mapping commit together, and the returned result is computed on the
    # same committed snapshot.
    with store.transaction(write=True):
        status = store.add(stored)
        batch = store.get_batch(batch_id)
        result = evaluate_batch(batch_id, batch)
    return IngestResponse(
        status=status,
        idempotency_key=event.idempotency_key,
        batch_id=batch_id,
        sensor_id=event.sensor_id,
        revision_id=event.revision_id,
        result=BatchResult(**result),
    )


@app.post(
    "/api/v1/batches/{batch_id}/events:preview",
    response_model=PreviewResponse,
    status_code=200,
    tags=["calibration"],
    summary="Preview one measurement-fragment revision without storing it",
)
def preview_event(batch_id: str, event: MeasurementEvent) -> PreviewResponse:
    if event.batch_id != batch_id:
        raise HTTPException(
            status_code=422,
            detail="batch_id in path and body must match",
        )
    stored = StoredEvent(
        batch_id=event.batch_id,
        sensor_id=event.sensor_id,
        revision_id=event.revision_id,
        parent_id=event.parent_id,
        idempotency_key=event.idempotency_key,
        start_ns=event.start_ns,
        end_ns=event.end_ns,
        value=None if event.withdrawn else float(event.value),
        unit=event.unit,
        withdrawn=event.withdrawn,
    )
    # One lock/transaction for the whole decision + evaluation: the preview is
    # computed against one consistent batch snapshot (held for the duration,
    # including against concurrent processes on the durable backend), the
    # same guarantee a concurrent real submission gets.
    with store.transaction(write=False):
        status = store.classify_submission(stored)
        batch = store.get_batch(batch_id)
        if status == "duplicate":
            # An exact replay changes nothing: before and after are the same
            # snapshot and no key is occupied by the preview itself. A batch
            # that does not exist yet is evaluated as an empty batch.
            snapshot = batch if batch is not None and batch.events else BatchStore()
            result = evaluate_batch(batch_id, snapshot)
            return PreviewResponse(
                status="duplicate",
                idempotency_key=event.idempotency_key,
                batch_id=batch_id,
                sensor_id=event.sensor_id,
                revision_id=event.revision_id,
                before_timeline_hash=result["timeline_hash"],
                after_timeline_hash=result["timeline_hash"],
                before=BatchResult(**result),
                after=BatchResult(**result),
            )
        doc = preview_submission(batch_id, stored, batch)
    return PreviewResponse(**doc)


@app.get(
    "/api/v1/batches/{batch_id}/result",
    response_model=BatchResult,
    tags=["calibration"],
    summary="Query the effective calibration timeline for a batch",
)
def get_result(batch_id: str) -> BatchResult:
    with store.transaction(write=False):
        batch = store.get_batch(batch_id)
        if batch is None or not batch.events:
            raise HTTPException(
                status_code=404, detail=f"batch {batch_id!r} not found"
            )
        result = evaluate_batch(batch_id, batch)
    return BatchResult(**result)


@app.get(
    "/api/v1/batches/{batch_id}/sensors/{sensor_id}/sample",
    response_model=SamplePointResponse,
    tags=["calibration"],
    summary="Query the effective calibration state at one nanosecond",
    responses={404: {"description": "Unknown batch or sensor"}},
)
def get_sample(
    batch_id: str,
    sensor_id: str,
    timestamp_ns: int = Query(
        ...,
        ge=0,
        description="Batch-local nanosecond instant to resolve (half-open intervals).",
    ),
) -> SamplePointResponse:
    with store.transaction(write=False):
        batch = store.get_batch(batch_id)
        if batch is None or not batch.events:
            raise HTTPException(status_code=404, detail=f"batch {batch_id!r} not found")
        doc = evaluate_sample(batch_id, sensor_id, timestamp_ns, batch)
    if doc is None:
        raise HTTPException(
            status_code=404,
            detail=f"sensor {sensor_id!r} not found in batch {batch_id!r}",
        )
    return SamplePointResponse(**doc)


@app.get(
    "/api/v1/batches/compare",
    response_model=CompareResponse,
    tags=["calibration"],
    summary="Compare one sensor's effective timeline across two batches",
    responses={404: {"description": "Unknown batch or sensor"}},
)
def compare_batches(
    left_batch_id: str = Query(..., min_length=1, max_length=200),
    right_batch_id: str = Query(..., min_length=1, max_length=200),
    sensor_id: str = Query(..., min_length=1, max_length=200),
) -> CompareResponse:
    with store.transaction(write=False):
        left_batch = store.get_batch(left_batch_id)
        if left_batch is None or not left_batch.events:
            raise HTTPException(
                status_code=404, detail=f"batch {left_batch_id!r} not found"
            )
        right_batch = store.get_batch(right_batch_id)
        if right_batch is None or not right_batch.events:
            raise HTTPException(
                status_code=404, detail=f"batch {right_batch_id!r} not found"
            )
        left_events = find_sensor_events(left_batch, sensor_id)
        if left_events is None:
            raise HTTPException(
                status_code=404,
                detail=f"sensor {sensor_id!r} not found in batch {left_batch_id!r}",
            )
        right_events = find_sensor_events(right_batch, sensor_id)
        if right_events is None:
            raise HTTPException(
                status_code=404,
                detail=f"sensor {sensor_id!r} not found in batch {right_batch_id!r}",
            )
        doc = compare_sensor(
            left_batch_id, right_batch_id, sensor_id, left_events, right_events
        )
    return CompareResponse(**doc)


@app.post(
    "/api/v1/batches/{batch_id}/quality-gate",
    response_model=QualityGateResponse,
    tags=["calibration"],
    summary="Evaluate a quality gate over one sensor's folded timeline",
    responses={404: {"description": "Unknown batch or sensor"}},
)
def quality_gate(batch_id: str, gate: QualityGateRequest) -> QualityGateResponse:
    with store.transaction(write=False):
        batch = store.get_batch(batch_id)
        if batch is None or not batch.events:
            raise HTTPException(
                status_code=404, detail=f"batch {batch_id!r} not found"
            )
        doc = evaluate_quality_gate(
            batch_id,
            gate.sensor_id,
            gate.min_value,
            gate.max_value,
            gate.min_duration_ns,
            gate.missing_policy,
            batch,
        )
    if doc is None:
        raise HTTPException(
            status_code=404,
            detail=f"sensor {gate.sensor_id!r} not found in batch {batch_id!r}",
        )
    return QualityGateResponse(**doc)


@app.post(
    "/api/v1/batches/{batch_id}/release",
    response_model=ReleaseResponse,
    tags=["calibration"],
    summary="Evaluate device release over required sensors and a time window",
    responses={404: {"description": "Unknown batch or sensor"}},
)
def device_release(batch_id: str, query: ReleaseRequest) -> ReleaseResponse:
    requirements = [
        ReleaseRequirement(
            sensor_id=s.sensor_id,
            min_value=s.min_value,
            max_value=s.max_value,
            expected_unit=s.expected_unit,
        )
        for s in query.sensors
    ]
    # Read-only evaluation against one consistent batch snapshot: the batch
    # load, sensor-existence check and the sweep run inside a single
    # transaction (the cross-process snapshot guarantee on the durable store).
    with store.transaction(write=False):
        batch = store.get_batch(batch_id)
        if batch is None or not batch.events:
            raise HTTPException(
                status_code=404, detail=f"batch {batch_id!r} not found"
            )
        doc = evaluate_device_release(
            batch_id,
            query.window_start_ns,
            query.window_end_ns,
            requirements,
            query.min_duration_ns,
            batch,
        )
        known_sensors = {event.sensor_id for event in batch.events.values()}
    if doc is None:
        missing = sorted(
            {req.sensor_id for req in requirements if req.sensor_id not in known_sensors}
        )
        raise HTTPException(
            status_code=404,
            detail=(
                f"sensor(s) {missing} not found in batch {batch_id!r}"
                if len(missing) > 1
                else f"sensor {missing[0]!r} not found in batch {batch_id!r}"
            ),
        )
    return ReleaseResponse(**doc)


@app.post(
    "/api/v1/batches/{batch_id}/majority",
    response_model=MajorityResponse,
    tags=["calibration"],
    summary="Evaluate a trusted majority over redundant sensors and a window",
    responses={404: {"description": "Unknown batch or sensor"}},
)
def sensor_majority(batch_id: str, query: MajorityRequest) -> MajorityResponse:
    sensor_ids = [spec.sensor_id for spec in query.sensors]
    # Read-only evaluation against one consistent batch snapshot: the batch
    # load, sensor-existence check and the sweep run inside a single
    # transaction (the cross-process snapshot guarantee on the durable store).
    with store.transaction(write=False):
        batch = store.get_batch(batch_id)
        if batch is None or not batch.events:
            raise HTTPException(
                status_code=404, detail=f"batch {batch_id!r} not found"
            )
        doc = evaluate_sensor_majority(
            batch_id,
            query.window_start_ns,
            query.window_end_ns,
            sensor_ids,
            query.expected_unit,
            query.q,
            query.max_value_difference,
            batch,
        )
        known_sensors = {event.sensor_id for event in batch.events.values()}
    if doc is None:
        missing = sorted(
            sensor_id for sensor_id in sensor_ids if sensor_id not in known_sensors
        )
        raise HTTPException(
            status_code=404,
            detail=(
                f"sensor(s) {missing} not found in batch {batch_id!r}"
                if len(missing) > 1
                else f"sensor {missing[0]!r} not found in batch {batch_id!r}"
            ),
        )
    return MajorityResponse(**doc)


def _seal_response(status: str, record: SealRecord) -> SealResponse:
    return SealResponse(
        status=status,
        batch_id=record.batch_id,
        seal_id=record.seal_id,
        timeline_hash=record.timeline_hash,
        evidence_digest=record.evidence_digest,
        evidence=SealEvidence(**record.evidence),
    )


@app.post(
    "/api/v1/batches/{batch_id}/seals",
    response_model=SealResponse,
    status_code=200,
    tags=["calibration"],
    summary="Seal a complete batch into immutable archival evidence",
    responses={
        404: {"description": "Unknown batch"},
        409: {"description": "Seal id reused, stale hash, or incomplete batch"},
    },
)
def seal_batch(batch_id: str, request: SealRequest) -> SealResponse:
    if request.batch_id != batch_id:
        raise HTTPException(
            status_code=422,
            detail="batch_id in path and body must match",
        )
    # One transaction for the whole decision + snapshot: on the durable
    # backend BEGIN IMMEDIATE is the cross-process lock, so the replay check,
    # hash/completeness validation, evidence capture and seal insert commit
    # atomically — no concurrent submission can interleave, and a rejected
    # seal leaves no record.
    with store.transaction(write=True):
        existing = store.get_seal(batch_id, request.seal_id)
        if existing is not None:
            if existing.timeline_hash == request.timeline_hash:
                # Exact replay of the original request: return the frozen
                # snapshot, even if the live batch has moved on since.
                return _seal_response("duplicate", existing)
            raise HTTPException(
                status_code=409,
                detail=(
                    f"seal {request.seal_id!r} already exists for batch "
                    f"{batch_id!r} with a different request"
                ),
            )
        batch = store.get_batch(batch_id)
        if batch is None or not batch.events:
            raise HTTPException(
                status_code=404, detail=f"batch {batch_id!r} not found"
            )
        evidence = build_seal_evidence(batch_id, batch)
        result = evidence["result"]
        if result["timeline_hash"] != request.timeline_hash:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"timeline_hash {request.timeline_hash!r} does not match "
                    f"the current batch hash {result['timeline_hash']!r}; "
                    "re-read the batch result and retry"
                ),
            )
        if not result["complete"]:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"batch {batch_id!r} is not complete: "
                    f"{result['revisions_blocked']} blocked and "
                    f"{result['revisions_invalid']} invalid revision(s) remain"
                ),
            )
        record = SealRecord(
            batch_id=batch_id,
            seal_id=request.seal_id,
            timeline_hash=request.timeline_hash,
            evidence=evidence,
            evidence_digest=hash_seal_evidence(evidence),
        )
        store.add_seal(record)
    return _seal_response("sealed", record)


@app.get(
    "/api/v1/batches/{batch_id}/seals/{seal_id}",
    response_model=SealRecallResponse,
    tags=["calibration"],
    summary="Recall a sealed batch snapshot and check it against the live batch",
    responses={404: {"description": "Unknown batch or seal"}},
)
def recall_seal(batch_id: str, seal_id: str) -> SealRecallResponse:
    with store.transaction(write=False):
        record = store.get_seal(batch_id, seal_id)
        if record is None:
            raise HTTPException(
                status_code=404,
                detail=f"seal {seal_id!r} not found for batch {batch_id!r}",
            )
        batch = store.get_batch(batch_id)
        current_digest = (
            hash_seal_evidence(build_seal_evidence(batch_id, batch))
            if batch is not None and batch.events
            else None
        )
    return SealRecallResponse(
        batch_id=record.batch_id,
        seal_id=record.seal_id,
        evidence_digest=record.evidence_digest,
        evidence=SealEvidence(**record.evidence),
        current_evidence_digest=current_digest,
        consistent=current_digest == record.evidence_digest,
    )


@app.post(
    "/admin/reset",
    response_model=ResetResponse,
    tags=["ops"],
    summary="Clear all batches, events and seals",
)
def reset_state() -> ResetResponse:
    batches_removed, events_removed, seals_removed = store.reset()
    logger.warning(
        "state reset: removed %d events across %d batches and %d seals",
        events_removed,
        batches_removed,
        seals_removed,
    )
    return ResetResponse(
        status="reset",
        batches_removed=batches_removed,
        events_removed=events_removed,
        seals_removed=seals_removed,
    )
