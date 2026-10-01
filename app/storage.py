"""Storage backends and deterministic timeline support structures.

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

Two backends implement exactly the same contract:

* :class:`EventStore` — process-local, resettable in-memory storage;
* :class:`SqliteEventStore` — durable SQLite storage (WAL). Revisions,
  idempotency-key mappings and frozen seal evidence survive process restarts
  and are shared by several service processes pointed at the same database
  file. Every mutating operation (including the revision/key pair insert and
  the validate-then-freeze seal capture) runs in one ``BEGIN IMMEDIATE``
  transaction, which is also the cross-process critical section.

Both backends expose :meth:`BaseEventStore.transaction`, a reentrant
in-process critical section that, for SQLite, holds one cross-process write
transaction: a decision and its follow-up reads/evaluations observe one
consistent point-in-time snapshot.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

from .models import ROOT_PARENT

__all__ = [
    "BatchStore",
    "SealRecord",
    "StoredEvent",
    "IdempotencyConflict",
    "StorageError",
    "EventStore",
    "SqliteEventStore",
    "create_store",
]


class IdempotencyConflict(Exception):
    """An idempotency key was reused with a different payload."""


class StorageError(Exception):
    """The storage backend failed.

    Never raised for an empty database: a missing batch stays a normal
    ``None`` return (so callers keep mapping it to 404). This error means the
    backend itself could not be read or written and must be surfaced (HTTP
    503), never silently treated as an empty store.
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


# ---------------------------------------------------------------------------
# Common interface
# ---------------------------------------------------------------------------


class BaseEventStore:
    """Interface shared by the memory and SQLite event stores."""

    #: value reported by ``GET /health`` for this backend
    backend: str = "memory"

    @property
    def lock(self) -> Any:  # pragma: no cover - overridden
        raise NotImplementedError

    @contextmanager
    def transaction(self, write: bool = True) -> Iterator[None]:
        """Reentrant in-process guard + (for the outer block) one SQLite txn.

        Standalone method calls open their own transaction; this method is
        the hook handlers use to wrap a multi-step decision/evaluation so it
        commits (or rolls back) atomically against other processes.
        """
        raise NotImplementedError

    # -- mutations ----------------------------------------------------------

    def add(self, event: StoredEvent) -> str:
        """Insert an event.

        Returns ``"accepted"`` or ``"duplicate"``. Raises
        :class:`IdempotencyConflict` on a key reused with other content.
        """
        raise NotImplementedError

    def classify_submission(self, event: StoredEvent) -> str:
        """Decide what :meth:`add` would decide, without changing any state.

        Returns ``"submittable"`` or ``"duplicate"``; raises
        :class:`IdempotencyConflict` exactly where :meth:`add` would. No
        idempotency key is registered and no batch entry is created, so
        previews leave no trace.
        """
        raise NotImplementedError

    def reset(self) -> tuple[int, int, int]:
        """Clear every batch, event and seal; return their counts."""
        raise NotImplementedError

    # -- seals --------------------------------------------------------------

    def get_seal(self, batch_id: str, seal_id: str) -> Optional[SealRecord]:
        raise NotImplementedError

    def add_seal(self, record: SealRecord) -> None:
        raise NotImplementedError

    # -- reads --------------------------------------------------------------

    def has_batch(self, batch_id: str) -> bool:
        batch = self.get_batch(batch_id)
        return batch is not None and bool(batch.events)

    def get_batch(self, batch_id: str) -> Optional[BatchStore]:
        raise NotImplementedError

    def stats(self) -> tuple[int, int]:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# In-memory backend
# ---------------------------------------------------------------------------


class EventStore(BaseEventStore):
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

    @contextmanager
    def transaction(self, write: bool = True) -> Iterator[None]:
        with self._lock:
            yield

    # -- mutation -----------------------------------------------------------

    def add(self, event: StoredEvent) -> str:
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
            # The revision and its idempotency-key mapping are stored together
            # atomically so a partial mapping can never be observed.
            batch.idempotency[event.idempotency_key] = canonical
            return "accepted"

    def classify_submission(self, event: StoredEvent) -> str:
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

    def reset(self) -> tuple[int, int, int]:
        with self._lock:
            batches_removed = len(self._batches)
            events_removed = sum(len(b.events) for b in self._batches.values())
            seals_removed = sum(len(s) for s in self._seals.values())
            self._batches.clear()
            self._seals.clear()
            return batches_removed, events_removed, seals_removed

    # -- seals --------------------------------------------------------------

    def get_seal(self, batch_id: str, seal_id: str) -> Optional[SealRecord]:
        with self._lock:
            return self._seals.get(batch_id, {}).get(seal_id)

    def add_seal(self, record: SealRecord) -> None:
        with self._lock:
            self._seals[record.batch_id][record.seal_id] = record

    # -- reads --------------------------------------------------------------

    def get_batch(self, batch_id: str) -> Optional[BatchStore]:
        with self._lock:
            return self._batches.get(batch_id)

    def stats(self) -> tuple[int, int]:
        with self._lock:
            return len(self._batches), sum(
                len(b.events) for b in self._batches.values()
            )


# ---------------------------------------------------------------------------
# SQLite backend (durable, shared across processes)
# ---------------------------------------------------------------------------


_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    batch_id        TEXT NOT NULL,
    revision_id     TEXT NOT NULL,
    sensor_id       TEXT NOT NULL,
    parent_id       TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    start_ns        INTEGER NOT NULL,
    end_ns          INTEGER NOT NULL,
    value           REAL,
    unit            TEXT,
    withdrawn       INTEGER NOT NULL,
    PRIMARY KEY (batch_id, revision_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS idempotency_keys (
    batch_id        TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    revision_id     TEXT NOT NULL,
    canonical       TEXT NOT NULL,
    PRIMARY KEY (batch_id, idempotency_key)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS seals (
    batch_id        TEXT NOT NULL,
    seal_id         TEXT NOT NULL,
    timeline_hash   TEXT NOT NULL,
    evidence        TEXT NOT NULL,
    evidence_digest TEXT NOT NULL,
    PRIMARY KEY (batch_id, seal_id)
) WITHOUT ROWID;
"""


_SCHEMA_STATEMENTS = [
    s.strip()
    for s in _SCHEMA.split(";")
    if s.strip() and not s.strip().startswith("--")
]


class _ReentrantLockGuard:
    """Reentrant lock whose owner depth is exposed for ambient txn handling."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._local = threading.local()

    @property
    def depth(self) -> int:
        return getattr(self._local, "depth", 0)

    def acquire(self) -> None:
        self._lock.acquire()
        self._local.depth = self.depth + 1

    def release(self) -> None:
        self._local.depth = self.depth - 1
        self._lock.release()

    @contextmanager
    def held(self) -> Iterator[None]:
        self.acquire()
        try:
            yield
        finally:
            self.release()


class SqliteEventStore(BaseEventStore):
    """Durable SQLite-backed store shared by every process using the file.

    Concurrency model: each thread owns one connection (check_same_thread is
    off, but the guard serializes access in-process). WAL allows one writer
    plus concurrent readers across processes; mutating critical sections use
    ``BEGIN IMMEDIATE`` so the SQLite write lock is the cross-process mutual
    exclusion primitive. A guarded operation that hits a locked database
    waits up to ``busy_timeout`` instead of failing spuriously.
    """

    backend = "sqlite"

    def __init__(self, path: str | Path, busy_timeout_ms: int = 10_000) -> None:
        path = str(path)
        if path == ":memory:":
            raise ValueError(
                "sqlite ':memory:' is not a shared durable store; use the "
                "'memory' backend for ephemeral storage or a file path"
            )
        self.path = path
        self.busy_timeout_ms = busy_timeout_ms
        self._guard = _ReentrantLockGuard()
        self._local = threading.local()
        try:
            parent = Path(path).parent
            if parent and str(parent) != "":
                parent.mkdir(parents=True, exist_ok=True)
            self._init_schema()
        except (OSError, sqlite3.Error) as exc:
            raise StorageError(
                f"cannot initialize sqlite storage at {path!r}: {exc}"
            ) from exc

    # -- plumbing -----------------------------------------------------------

    @property
    def lock(self) -> _ReentrantLockGuard:
        return self._guard

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1000.0,
            isolation_level=None,  # explicit transaction control
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        # WAL gives cross-process readers + a single writer; NORMAL is the
        # recommended durable journal sync level under WAL.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(f"PRAGMA busy_timeout={int(self.busy_timeout_ms)}")
        return conn

    @property
    def _conn(self) -> Optional[sqlite3.Connection]:
        return getattr(self._local, "conn", None)

    @contextmanager
    def transaction(self, write: bool = True) -> Iterator[None]:
        """Reentrant in-process guard + (for the outer block) one SQLite txn."""
        outer = self._conn is None
        self._guard.acquire()
        conn: Optional[sqlite3.Connection] = None
        try:
            if outer:
                try:
                    conn = self._connect()
                except sqlite3.Error as exc:
                    raise StorageError(
                        f"sqlite storage at {self.path!r} is unavailable: {exc}"
                    ) from exc
                self._local.conn = conn
                try:
                    # BEGIN IMMEDIATE reserves the cross-process write lock up
                    # front, so the whole decision+write block is atomic
                    # against other processes; read-only blocks use a plain
                    # deferred transaction and only see committed data.
                    conn.execute(
                        "BEGIN IMMEDIATE" if write else "BEGIN"
                    )
                except sqlite3.Error as exc:
                    raise StorageError(
                        f"sqlite storage at {self.path!r} is unavailable: {exc}"
                    ) from exc
                try:
                    yield
                    conn.execute("COMMIT")
                except BaseException:
                    conn.execute("ROLLBACK")
                    raise
            else:
                # Nested block: join the ambient transaction; no commit here.
                yield
        finally:
            if outer and conn is not None:
                self._local.conn = None
                conn.close()
            self._guard.release()

    def _init_schema(self) -> None:
        with self._guard.held():
            conn = self._connect()
            self._local.conn = conn
            try:
                # executescript() issues an implicit COMMIT, which would close
                # our BEGIN IMMEDIATE; run the DDL statements separately so
                # the schema creation stays one atomic transaction.
                conn.execute("BEGIN IMMEDIATE")
                for statement in _SCHEMA_STATEMENTS:
                    conn.execute(statement)
                conn.execute("COMMIT")
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            finally:
                self._local.conn = None
                conn.close()

    # -- mutations ----------------------------------------------------------

    def add(self, event: StoredEvent) -> str:
        with self.transaction(write=True):
            return self._add_locked(event)

    def _add_locked(self, event: StoredEvent) -> str:
        cur = self._conn
        assert cur is not None
        canonical = canonical_event(event)
        try:
            row = cur.execute(
                "SELECT canonical FROM idempotency_keys "
                "WHERE batch_id = ? AND idempotency_key = ?",
                (event.batch_id, event.idempotency_key),
            ).fetchone()
            if row is not None:
                if row["canonical"] == canonical:
                    return "duplicate"
                raise IdempotencyConflict(event.idempotency_key)

            existing = cur.execute(
                "SELECT * FROM events WHERE batch_id = ? AND revision_id = ?",
                (event.batch_id, event.revision_id),
            ).fetchone()
            if existing is not None:
                stored = _row_to_event(existing)
                if canonical_event(stored) != canonical:
                    raise IdempotencyConflict(event.idempotency_key)
                # Identical revision replayed under a new key: register the new
                # key atomically so the revision<->key mapping is complete.
                cur.execute(
                    "INSERT OR REPLACE INTO idempotency_keys "
                    "(batch_id, idempotency_key, revision_id, canonical) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        event.batch_id,
                        event.idempotency_key,
                        event.revision_id,
                        canonical,
                    ),
                )
                return "duplicate"

            cur.execute(
                "INSERT INTO events "
                "(batch_id, revision_id, sensor_id, parent_id, idempotency_key,"
                " start_ns, end_ns, value, unit, withdrawn) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    event.batch_id,
                    event.revision_id,
                    event.sensor_id,
                    event.parent_id,
                    event.idempotency_key,
                    event.start_ns,
                    event.end_ns,
                    event.value,
                    event.unit,
                    1 if event.withdrawn else 0,
                ),
            )
            cur.execute(
                "INSERT INTO idempotency_keys "
                "(batch_id, idempotency_key, revision_id, canonical) "
                "VALUES (?, ?, ?, ?)",
                (
                    event.batch_id,
                    event.idempotency_key,
                    event.revision_id,
                    canonical,
                ),
            )
            return "accepted"
        except sqlite3.Error as exc:
            raise StorageError(
                f"failed to write event {event.revision_id!r} in batch "
                f"{event.batch_id!r} to sqlite storage: {exc}"
            ) from exc

    def classify_submission(self, event: StoredEvent) -> str:
        with self.transaction(write=False):
            cur = self._conn
            assert cur is not None
            try:
                canonical = canonical_event(event)
                row = cur.execute(
                    "SELECT canonical FROM idempotency_keys "
                    "WHERE batch_id = ? AND idempotency_key = ?",
                    (event.batch_id, event.idempotency_key),
                ).fetchone()
                if row is not None:
                    if row["canonical"] == canonical:
                        return "duplicate"
                    raise IdempotencyConflict(event.idempotency_key)

                existing = cur.execute(
                    "SELECT * FROM events WHERE batch_id = ? AND revision_id = ?",
                    (event.batch_id, event.revision_id),
                ).fetchone()
                if existing is not None:
                    if canonical_event(_row_to_event(existing)) != canonical:
                        raise IdempotencyConflict(event.idempotency_key)
                    return "duplicate"
                return "submittable"
            except sqlite3.Error as exc:
                raise StorageError(
                    f"failed to classify submission in batch "
                    f"{event.batch_id!r}: {exc}"
                ) from exc

    def reset(self) -> tuple[int, int, int]:
        with self.transaction(write=True):
            cur = self._conn
            assert cur is not None
            try:
                event_batches = {
                    r[0]
                    for r in cur.execute(
                        "SELECT DISTINCT batch_id FROM events"
                    ).fetchall()
                }
                seal_batches = {
                    r[0]
                    for r in cur.execute(
                        "SELECT DISTINCT batch_id FROM seals"
                    ).fetchall()
                }
                events_removed = cur.execute(
                    "SELECT COUNT(*) FROM events"
                ).fetchone()[0]
                seals_removed = cur.execute(
                    "SELECT COUNT(*) FROM seals"
                ).fetchone()[0]
                cur.execute("DELETE FROM events")
                cur.execute("DELETE FROM idempotency_keys")
                cur.execute("DELETE FROM seals")
                return (
                    len(event_batches | seal_batches),
                    int(events_removed),
                    int(seals_removed),
                )
            except sqlite3.Error as exc:
                raise StorageError(f"failed to reset sqlite storage: {exc}") from exc

    # -- seals --------------------------------------------------------------

    def get_seal(self, batch_id: str, seal_id: str) -> Optional[SealRecord]:
        with self.transaction(write=False):
            cur = self._conn
            assert cur is not None
            try:
                row = cur.execute(
                    "SELECT * FROM seals WHERE batch_id = ? AND seal_id = ?",
                    (batch_id, seal_id),
                ).fetchone()
            except sqlite3.Error as exc:
                raise StorageError(
                    f"failed to read seal {seal_id!r} of batch "
                    f"{batch_id!r}: {exc}"
                ) from exc
            return None if row is None else _row_to_seal(row)

    def add_seal(self, record: SealRecord) -> None:
        with self.transaction(write=True):
            cur = self._conn
            assert cur is not None
            try:
                cur.execute(
                    "INSERT INTO seals "
                    "(batch_id, seal_id, timeline_hash, evidence, "
                    "evidence_digest) VALUES (?, ?, ?, ?, ?)",
                    (
                        record.batch_id,
                        record.seal_id,
                        record.timeline_hash,
                        json.dumps(
                            record.evidence,
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=False,
                        ),
                        record.evidence_digest,
                    ),
                )
            except sqlite3.Error as exc:
                raise StorageError(
                    f"failed to persist seal {record.seal_id!r} of batch "
                    f"{record.batch_id!r}: {exc}"
                ) from exc

    # -- reads --------------------------------------------------------------

    def get_batch(self, batch_id: str) -> Optional[BatchStore]:
        with self.transaction(write=False):
            return self._load_batch_locked(batch_id)

    def _load_batch_locked(self, batch_id: str) -> Optional[BatchStore]:
        """Materialize one batch snapshot; returns None when the batch is
        unknown (no rows) — a storage error is never confused with empty."""
        cur = self._conn
        assert cur is not None
        try:
            rows = cur.execute(
                "SELECT * FROM events WHERE batch_id = ?",
                (batch_id,),
            ).fetchall()
            if not rows:
                return None
            batch = BatchStore()
            for row in rows:
                event = _row_to_event(row)
                batch.events[event.revision_id] = event
                batch.revision_sensor[event.revision_id] = event.sensor_id
            for row in cur.execute(
                "SELECT idempotency_key, canonical FROM idempotency_keys "
                "WHERE batch_id = ?",
                (batch_id,),
            ).fetchall():
                batch.idempotency[row["idempotency_key"]] = row["canonical"]
            return batch
        except sqlite3.Error as exc:
            raise StorageError(
                f"failed to read batch {batch_id!r} from sqlite storage: {exc}"
            ) from exc

    def stats(self) -> tuple[int, int]:
        with self.transaction(write=False):
            cur = self._conn
            assert cur is not None
            try:
                batches = cur.execute(
                    "SELECT COUNT(DISTINCT batch_id) FROM events"
                ).fetchone()[0]
                events = cur.execute("SELECT COUNT(*) FROM events").fetchone()[0]
                return int(batches), int(events)
            except sqlite3.Error as exc:
                raise StorageError(f"failed to read sqlite stats: {exc}") from exc


def _row_to_event(row: sqlite3.Row) -> StoredEvent:
    return StoredEvent(
        batch_id=row["batch_id"],
        sensor_id=row["sensor_id"],
        revision_id=row["revision_id"],
        parent_id=row["parent_id"],
        idempotency_key=row["idempotency_key"],
        start_ns=int(row["start_ns"]),
        end_ns=int(row["end_ns"]),
        value=None if row["value"] is None else float(row["value"]),
        unit=row["unit"],
        withdrawn=bool(row["withdrawn"]),
    )


def _row_to_seal(row: sqlite3.Row) -> SealRecord:
    return SealRecord(
        batch_id=row["batch_id"],
        seal_id=row["seal_id"],
        timeline_hash=row["timeline_hash"],
        evidence=json.loads(row["evidence"]),
        evidence_digest=row["evidence_digest"],
    )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_store(backend: str, sqlite_path: str | None = None) -> BaseEventStore:
    """Build the configured storage backend.

    ``backend`` is ``"memory"`` (ephemeral, process-local) or ``"sqlite"``
    (durable file shared across processes). An unknown backend or an
    unusable SQLite path raises :class:`StorageError` (or ValueError) so the
    service fails fast at startup instead of silently dropping data.
    """
    if backend == "memory":
        return EventStore()
    if backend == "sqlite":
        if not sqlite_path:
            raise StorageError(
                "storage backend 'sqlite' requires CAL_SQLITE_PATH to be set"
            )
        return SqliteEventStore(sqlite_path)
    raise ValueError(f"unknown storage backend {backend!r}")
