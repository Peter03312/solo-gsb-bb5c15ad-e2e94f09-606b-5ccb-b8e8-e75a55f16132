"""Storage backends and deterministic timeline engine support.

Rules (contract implemented in this module)
-------------------------------------------

* Events form a per-(batch, sensor) causal DAG. Each revision has exactly one
  parent; ``"ROOT"`` denotes a causal root.
* Events may arrive in any order, overlap in time, and be re-sent. A revision
  whose parent has not been seen is **blocked**: it (and everything descending
  from it) is excluded from the effective timeline, while every missing parent
  is reported as a backfillable gap.
* Withdrawn revisions are tombstones: applied in the same causal order they
  erase every calibrated value under their interval.
* Application order for timeline folding is strictly deterministic:
  causal depth ascending, then ``revision_id`` ascending lexicographically
  (the later/larger id wins on same-depth overlaps).
  Later applications overwrite earlier ones on overlapping time spans.
* Half-open ``[start_ns, end_ns)`` intervals guarantee abutting fragments
  share no sample instant.
* Idempotency: an event with a known idempotency key is either an exact
  duplicate (accepted, no state change) or a conflicting replay (rejected).

Two interchangeable backends implement exactly the same contract:

* :class:`EventStore` - process-local in-memory state (resets with the
  process);
* :class:`SqliteEventStore` - durable SQLite state (WAL journal) shared by
  every service process pointed at the same database file. Every decision +
  write + snapshot happens inside one database transaction, so concurrent
  processes get the same accepted/duplicate/conflict outcomes as threads
  sharing one in-memory store.
"""

from __future__ import annotations

import contextlib
import json
import os
import sqlite3
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Optional

from .models import ROOT_PARENT  # noqa: F401  (re-exported for the engine)


class IdempotencyConflict(Exception):
    """An idempotency key was reused with a different payload."""


class StorageError(Exception):
    """The persistent backend is unavailable or rejected an operation.

    Storage failures must never masquerade as an empty/unknown batch: callers
    surface this as an explicit ``503`` rather than treating the unreadable
    state as "no data".
    """


@dataclass(frozen=True)
class StoredEvent:
    batch_id: str
    sensor_id: str
    revision_id: str
    parent_id: str
    idempotency_key: str
    start_ns: int
    end_ns: int
    value: Optional[float]
    unit: Optional[str]
    withdrawn: bool


@dataclass
class BatchStore:
    events: dict[str, StoredEvent] = field(default_factory=dict)
    # revision_id -> owning sensor, for stable parent lookups
    revision_sensor: dict[str, str] = field(default_factory=dict)
    # idempotency_key -> canonical payload string
    idempotency: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class SealRecord:
    """One immutable batch seal: frozen evidence plus its deterministic digest.

    The evidence dict is a plain-data snapshot (all revisions at seal time
    plus the folded batch result) fully decoupled from the live store, so
    later submissions can never rewrite a stored seal.
    """

    batch_id: str
    seal_id: str
    # timeline_hash supplied by the original sealing request.
    timeline_hash: str
    evidence: dict
    evidence_digest: str


# Outcomes of a seal attempt that do not produce a new seal.
SEALED = "sealed"
SEAL_DUPLICATE = "duplicate"
SEAL_CONFLICT = "seal_conflict"


def _copy_batch(batch: Optional[BatchStore]) -> Optional[BatchStore]:
    """Detached snapshot copy (the stored events themselves are immutable)."""
    if batch is None:
        return None
    return BatchStore(
        events=dict(batch.events),
        revision_sensor=dict(batch.revision_sensor),
        idempotency=dict(batch.idempotency),
    )


class EventStore:
    """Thread-safe, process-local, resettable in-memory event store."""

    backend = "memory"

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._batches: dict[str, BatchStore] = defaultdict(BatchStore)
        # batch_id -> seal_id -> frozen archival evidence
        self._seals: dict[str, dict[str, SealRecord]] = defaultdict(dict)

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    # -- mutation -----------------------------------------------------------

    def add(self, event: StoredEvent) -> str:
        """Insert an event.

        Returns ``"accepted"`` or ``"duplicate"``. Raises
        :class:`IdempotencyConflict` on a key reused with other content.
        """
        with self._lock:
            batch = self._batches[event.batch_id]
            canonical = canonical_event(event)

            previous = batch.idempotency.get(event.idempotency_key)
            if previous is not None:
                if previous == canonical:
                    return "duplicate"
                raise IdempotencyConflict(event.idempotency_key)

            existing = batch.events.get(event.revision_id)
            if existing is not None:
                # Same revision_id submitted under a new idempotency key.
                if canonical_event(existing) != canonical:
                    raise IdempotencyConflict(event.idempotency_key)
                # Identical content but different key: still an exact replay;
                # register the new key so both forms deduplicate going forward.
                batch.idempotency[event.idempotency_key] = canonical
                return "duplicate"

            batch.events[event.revision_id] = event
            batch.revision_sensor[event.revision_id] = event.sensor_id
            batch.idempotency[event.idempotency_key] = canonical
            return "accepted"

    def add_view(
        self, event: StoredEvent
    ) -> tuple[str, Optional[BatchStore]]:
        """Atomically decide+insert and snapshot the resulting batch.

        Same decision rules as :meth:`add`; the returned :class:`BatchStore`
        is a detached point-in-time snapshot, safe to evaluate after the fact.
        """
        with self._lock:
            status = self.add(event)
            return status, _copy_batch(self._batches.get(event.batch_id))

    def classify_submission(self, event: StoredEvent) -> str:
        """Decide what ``add`` would decide, without changing any state.

        Returns ``"submittable"`` when the event would be newly accepted or
        ``"duplicate"`` when the exact event (same idempotency key and payload,
        or the same revision with identical content under another key) is
        already stored. Raises :class:`IdempotencyConflict` exactly where
        :meth:`add` would. No idempotency key is registered and no batch entry
        is created, so previews leave no trace.
        """
        with self._lock:
            batch = self._batches.get(event.batch_id)
            canonical = canonical_event(event)

            if batch is not None:
                previous = batch.idempotency.get(event.idempotency_key)
                if previous is not None:
                    if previous == canonical:
                        return "duplicate"
                    raise IdempotencyConflict(event.idempotency_key)

                existing = batch.events.get(event.revision_id)
                if existing is not None:
                    if canonical_event(existing) != canonical:
                        raise IdempotencyConflict(event.idempotency_key)
                    return "duplicate"

            return "submittable"

    def preview_view(
        self, event: StoredEvent
    ) -> tuple[str, Optional[BatchStore]]:
        """Classify a submission and snapshot the batch in one critical section.

        The preview is evaluated against one consistent point-in-time batch
        snapshot; nothing is registered or written.
        """
        with self._lock:
            status = self.classify_submission(event)
            batch = self._batches.get(event.batch_id)
            return status, _copy_batch(batch)

    def reset(self) -> tuple[int, int, int]:
        with self._lock:
            batches_removed = len(
                set(self._batches) | set(self._seals)
            )
            events_removed = sum(len(b.events) for b in self._batches.values())
            seals_removed = sum(len(s) for s in self._seals.values())
            self._batches.clear()
            self._seals.clear()
            return batches_removed, events_removed, seals_removed

    # -- seals --------------------------------------------------------------

    def get_seal(self, batch_id: str, seal_id: str) -> Optional[SealRecord]:
        with self._lock:
            return self._seals.get(batch_id, {}).get(seal_id)

    def create_seal(
        self,
        record: SealRecord,
        prepare: Callable[[Optional[BatchStore]], Optional[SealRecord]],
    ) -> tuple[str, SealRecord]:
        """Atomically replay-check, validate against one snapshot and freeze.

        ``prepare`` receives the live batch snapshot (``None`` when the batch
        is unknown/empty) and returns the finalized :class:`SealRecord` to
        store, or raises (e.g. an HTTP 404/409) when the request must not
        seal. The prepare callback performs no storage writes, so an abort
        leaves the store untouched. Returns one of the ``SEAL_*`` outcomes
        alongside the existing or newly stored record.
        """
        with self._lock:
            existing = self._seals.get(record.batch_id, {}).get(record.seal_id)
            if existing is not None:
                outcome = (
                    SEAL_DUPLICATE
                    if existing.timeline_hash == record.timeline_hash
                    else SEAL_CONFLICT
                )
                return outcome, existing
            prepared = prepare(_copy_batch(self._batches.get(record.batch_id)))
            self._seals[prepared.batch_id][prepared.seal_id] = prepared
            return SEALED, prepared

    # -- reads --------------------------------------------------------------

    def has_batch(self, batch_id: str) -> bool:
        with self._lock:
            return batch_id in self._batches and bool(self._batches[batch_id].events)

    def get_batch(self, batch_id: str) -> Optional[BatchStore]:
        with self._lock:
            return _copy_batch(self._batches.get(batch_id))

    def stats(self) -> tuple[int, int]:
        with self._lock:
            return len(self._batches), sum(
                len(b.events) for b in self._batches.values()
            )


# ---------------------------------------------------------------------------
# SQLite backend (durable, multi-process)
# ---------------------------------------------------------------------------


_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    batch_id       TEXT NOT NULL,
    sensor_id      TEXT NOT NULL,
    revision_id    TEXT NOT NULL,
    parent_id      TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    start_ns       INTEGER NOT NULL,
    end_ns         INTEGER NOT NULL,
    value          REAL,
    unit           TEXT,
    withdrawn      INTEGER NOT NULL,
    canonical      TEXT NOT NULL,
    PRIMARY KEY (batch_id, revision_id)
);

CREATE TABLE IF NOT EXISTS idempotency (
    batch_id       TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    canonical      TEXT NOT NULL,
    PRIMARY KEY (batch_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS seals (
    batch_id        TEXT NOT NULL,
    seal_id         TEXT NOT NULL,
    timeline_hash   TEXT NOT NULL,
    evidence        TEXT NOT NULL,
    evidence_digest TEXT NOT NULL,
    PRIMARY KEY (batch_id, seal_id)
);
"""


class SqliteEventStore:
    """Durable, multi-process event store backed by one SQLite database.

    Concurrency model: every decision + write + snapshot runs in one
    ``BEGIN IMMEDIATE`` transaction, so SQLite serializes competing processes
    at the storage boundary exactly like the in-process lock serializes
    threads. Read paths take one read transaction so a preview or validation
    observes a single point-in-time snapshot. The WAL journal lets readers
    and a writer operate concurrently.
    """

    backend = "sqlite"

    def __init__(
        self,
        path: str,
        *,
        busy_timeout_ms: int = 5000,
    ) -> None:
        self._path = path
        self._busy_timeout = busy_timeout_ms
        # In-process serialization mirrors the memory backend; cross-process
        # serialization is provided by SQLite write transactions.
        self._lock = threading.RLock()
        if path != ":memory:":
            parent = os.path.dirname(os.path.abspath(path))
            try:
                os.makedirs(parent, exist_ok=True)
            except OSError as exc:  # unreadable storage location
                raise StorageError(f"cannot open storage directory {parent!r}: {exc}")
        try:
            with self._connect() as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.executescript(_SCHEMA)
        except sqlite3.Error as exc:
            raise StorageError(f"cannot initialize sqlite storage at {path!r}: {exc}")

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    @contextlib.contextmanager
    def _connect(self):
        try:
            conn = sqlite3.connect(
                self._path,
                timeout=self._busy_timeout / 1000.0,
                isolation_level=None,  # explicit BEGIN/COMMIT
            )
        except sqlite3.Error as exc:
            raise StorageError(f"cannot open sqlite storage {self._path!r}: {exc}")
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(f"PRAGMA busy_timeout={int(self._busy_timeout)}")
            yield conn
        except sqlite3.Error as exc:
            raise StorageError(f"sqlite storage failure: {exc}") from exc
        finally:
            conn.close()

    # -- row mapping --------------------------------------------------------

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> StoredEvent:
        return StoredEvent(
            batch_id=row["batch_id"],
            sensor_id=row["sensor_id"],
            revision_id=row["revision_id"],
            parent_id=row["parent_id"],
            idempotency_key=row["idempotency_key"],
            start_ns=row["start_ns"],
            end_ns=row["end_ns"],
            value=row["value"],
            unit=row["unit"],
            withdrawn=bool(row["withdrawn"]),
        )

    @staticmethod
    def _load_batch(conn: sqlite3.Connection, batch_id: str) -> BatchStore:
        batch = BatchStore()
        rows = conn.execute(
            "SELECT * FROM events WHERE batch_id = ? ORDER BY revision_id",
            (batch_id,),
        ).fetchall()
        for row in rows:
            event = SqliteEventStore._row_to_event(row)
            batch.events[event.revision_id] = event
            batch.revision_sensor[event.revision_id] = event.sensor_id
        for row in conn.execute(
            "SELECT idempotency_key, canonical FROM idempotency WHERE batch_id = ?",
            (batch_id,),
        ):
            batch.idempotency[row["idempotency_key"]] = row["canonical"]
        return batch

    @staticmethod
    def _row_to_seal(row: sqlite3.Row) -> SealRecord:
        return SealRecord(
            batch_id=row["batch_id"],
            seal_id=row["seal_id"],
            timeline_hash=row["timeline_hash"],
            evidence=json.loads(row["evidence"]),
            evidence_digest=row["evidence_digest"],
        )

    # -- mutation -----------------------------------------------------------

    def add(self, event: StoredEvent) -> str:
        """Insert an event with the same decision rules as the memory store.

        The idempotency lookup, revision lookup, revision insert and new-key
        registration all share one ``BEGIN IMMEDIATE`` transaction, so a
        revision and its key mapping are saved atomically even across
        processes.
        """
        status, _ = self.add_view(event)
        return status

    def add_view(
        self, event: StoredEvent
    ) -> tuple[str, Optional[BatchStore]]:
        canonical = canonical_event(event)
        with self._lock, self._connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                previous = conn.execute(
                    "SELECT canonical FROM idempotency "
                    "WHERE batch_id = ? AND idempotency_key = ?",
                    (event.batch_id, event.idempotency_key),
                ).fetchone()
                if previous is not None:
                    if previous["canonical"] != canonical:
                        conn.execute("ROLLBACK")
                        raise IdempotencyConflict(event.idempotency_key)
                    # Exact replay: snapshot inside the same transaction and
                    # release the write lock without changing anything.
                    batch = self._load_batch(conn, event.batch_id)
                    conn.execute("ROLLBACK")
                    return "duplicate", batch

                existing = conn.execute(
                    "SELECT canonical FROM events "
                    "WHERE batch_id = ? AND revision_id = ?",
                    (event.batch_id, event.revision_id),
                ).fetchone()
                if existing is not None:
                    if existing["canonical"] != canonical:
                        conn.execute("ROLLBACK")
                        raise IdempotencyConflict(event.idempotency_key)
                    # Same revision, identical content, new key: register the
                    # new key in the same atomic decision.
                    conn.execute(
                        "INSERT INTO idempotency(batch_id, idempotency_key, canonical) "
                        "VALUES (?, ?, ?)",
                        (event.batch_id, event.idempotency_key, canonical),
                    )
                    batch = self._load_batch(conn, event.batch_id)
                    conn.execute("COMMIT")
                    return "duplicate", batch

                conn.execute(
                    "INSERT INTO events(batch_id, sensor_id, revision_id, parent_id, "
                    "idempotency_key, start_ns, end_ns, value, unit, withdrawn, canonical) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        event.batch_id,
                        event.sensor_id,
                        event.revision_id,
                        event.parent_id,
                        event.idempotency_key,
                        event.start_ns,
                        event.end_ns,
                        event.value,
                        event.unit,
                        1 if event.withdrawn else 0,
                        canonical,
                    ),
                )
                conn.execute(
                    "INSERT INTO idempotency(batch_id, idempotency_key, canonical) "
                    "VALUES (?, ?, ?)",
                    (event.batch_id, event.idempotency_key, canonical),
                )
                # Capture the post-write snapshot while still in the atomic
                # transaction, then commit.
                batch = self._load_batch(conn, event.batch_id)
                conn.execute("COMMIT")
                return "accepted", batch
            except IdempotencyConflict:
                with contextlib.suppress(sqlite3.Error):
                    conn.execute("ROLLBACK")
                raise

    def classify_submission(self, event: StoredEvent) -> str:
        status, _ = self.preview_view(event)
        return status

    def preview_view(
        self, event: StoredEvent
    ) -> tuple[str, Optional[BatchStore]]:
        """Decide and snapshot the batch in one read transaction."""
        canonical = canonical_event(event)
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN")
            batch = self._load_batch(conn, event.batch_id)
            previous = conn.execute(
                "SELECT canonical FROM idempotency "
                "WHERE batch_id = ? AND idempotency_key = ?",
                (event.batch_id, event.idempotency_key),
            ).fetchone()
            if previous is not None:
                conn.execute("COMMIT")
                if previous["canonical"] != canonical:
                    raise IdempotencyConflict(event.idempotency_key)
                return "duplicate", batch if batch.events else None
            existing = conn.execute(
                "SELECT canonical FROM events WHERE batch_id = ? AND revision_id = ?",
                (event.batch_id, event.revision_id),
            ).fetchone()
            conn.execute("COMMIT")
            if existing is not None:
                if existing["canonical"] != canonical:
                    raise IdempotencyConflict(event.idempotency_key)
                return "duplicate", batch if batch.events else None
            return "submittable", batch if batch.events else None

    def _snapshot(self, batch_id: str) -> Optional[BatchStore]:
        """Read one detached batch snapshot (``None`` when it has no events)."""
        with self._connect() as conn:
            batch = self._load_batch(conn, batch_id)
        return batch if batch.events else None

    def reset(self) -> tuple[int, int, int]:
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT COUNT(DISTINCT batch_id) AS n FROM ("
                "SELECT batch_id FROM events UNION ALL "
                "SELECT batch_id FROM seals)"
            ).fetchone()
            batches_removed = row["n"]
            events_removed = conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()[
                "n"
            ]
            seals_removed = conn.execute("SELECT COUNT(*) AS n FROM seals").fetchone()[
                "n"
            ]
            conn.execute("DELETE FROM events")
            conn.execute("DELETE FROM idempotency")
            conn.execute("DELETE FROM seals")
            conn.execute("COMMIT")
            return batches_removed, events_removed, seals_removed

    # -- seals --------------------------------------------------------------

    def get_seal(self, batch_id: str, seal_id: str) -> Optional[SealRecord]:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM seals WHERE batch_id = ? AND seal_id = ?",
                (batch_id, seal_id),
            ).fetchone()
            return self._row_to_seal(row) if row is not None else None

    def create_seal(
        self,
        record: SealRecord,
        prepare: Callable[[Optional[BatchStore]], Optional[SealRecord]],
    ) -> tuple[str, SealRecord]:
        """Replay-check, validate and freeze inside one write transaction.

        The ``prepare`` validation observes the same locked snapshot the row
        is inserted against, so the current ``timeline_hash`` and completeness
        are checked atomically with freezing all revisions and results.
        """
        with self._lock, self._connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                existing = conn.execute(
                    "SELECT * FROM seals WHERE batch_id = ? AND seal_id = ?",
                    (record.batch_id, record.seal_id),
                ).fetchone()
                if existing is not None:
                    conn.execute("ROLLBACK")
                    existing_record = self._row_to_seal(existing)
                    outcome = (
                        SEAL_DUPLICATE
                        if existing_record.timeline_hash == record.timeline_hash
                        else SEAL_CONFLICT
                    )
                    return outcome, existing_record

                batch = self._load_batch(conn, record.batch_id)
                prepared = prepare(batch if batch.events else None)

                conn.execute(
                    "INSERT INTO seals(batch_id, seal_id, timeline_hash, evidence, "
                    "evidence_digest) VALUES (?, ?, ?, ?, ?)",
                    (
                        prepared.batch_id,
                        prepared.seal_id,
                        prepared.timeline_hash,
                        json.dumps(
                            prepared.evidence,
                            sort_keys=True,
                            ensure_ascii=False,
                        ),
                        prepared.evidence_digest,
                    ),
                )
                conn.execute("COMMIT")
                return SEALED, prepared
            except Exception:
                with contextlib.suppress(sqlite3.Error):
                    conn.execute("ROLLBACK")
                raise

    # -- reads --------------------------------------------------------------

    def has_batch(self, batch_id: str) -> bool:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM events WHERE batch_id = ? LIMIT 1", (batch_id,)
            ).fetchone()
            return row is not None

    def get_batch(self, batch_id: str) -> Optional[BatchStore]:
        with self._lock:
            return self._snapshot(batch_id)

    def stats(self) -> tuple[int, int]:
        with self._lock, self._connect() as conn:
            batches = conn.execute(
                "SELECT COUNT(DISTINCT batch_id) AS n FROM events"
            ).fetchone()["n"]
            events = conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"]
            return batches, events


def create_store(
    backend: str = "memory", *, path: Optional[str] = None
):
    """Build a store by backend name (``"memory"`` or ``"sqlite"``)."""
    if backend == "memory":
        return EventStore()
    if backend == "sqlite":
        if not path:
            raise StorageError("sqlite backend requires CAL_SQLITE_PATH")
        return SqliteEventStore(path)
    raise StorageError(f"unknown storage backend {backend!r}")


# ---------------------------------------------------------------------------
# Deterministic serialization helpers
# ---------------------------------------------------------------------------


def canonical_event(event: StoredEvent) -> str:
    """Canonical representation used for idempotent payload comparison."""
    value = "null" if event.value is None else repr(float(event.value))
    unit = "null" if event.unit is None else event.unit
    return "|".join(
        [
            event.batch_id,
            event.sensor_id,
            event.revision_id,
            event.parent_id,
            str(event.start_ns),
            str(event.end_ns),
            value,
            unit,
            "1" if event.withdrawn else "0",
        ]
    )
