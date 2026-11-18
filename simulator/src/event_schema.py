"""
Canonical event schema and event-ID/sequence management for PMS Module 1.

Every telemetry message published to Kafka or MQTT carries a full metadata
envelope defined here.  Consumers downstream can rely on these fields being
present without parsing the data columns.

Schema version history
----------------------
1.0.0  Initial schema with event_id, run_id, sequence, source_type.
"""

from __future__ import annotations

import hashlib
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

SCHEMA_VERSION = "1.0.0"

# source_type values
SOURCE_CMAPSS = "cmapss"
SOURCE_IMS = "ims"
SOURCE_SYNTHETIC = "synthetic"


# ---------------------------------------------------------------------------
# Deterministic event ID
# ---------------------------------------------------------------------------

def make_event_id(run_id: str, asset_id: str, sequence: int) -> str:
    """Return a deterministic UUID-like hex ID for a given (run, asset, seq).

    Calling this twice with the same arguments produces the same ID, so
    re-publishing a message on retry does not create a new event identity.
    """
    key = f"{run_id}:{asset_id}:{sequence}"
    digest = hashlib.sha256(key.encode()).hexdigest()
    # Format as UUID v5-style string for readability
    return f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"


def new_run_id() -> str:
    """Generate a fresh run ID for a new replay pass."""
    return uuid.uuid4().hex


# ---------------------------------------------------------------------------
# Envelope dataclass
# ---------------------------------------------------------------------------

@dataclass
class TelemetryEnvelope:
    """Metadata envelope attached to every telemetry event.

    Fields
    ------
    schema_version : version of this envelope format
    event_id       : deterministic ID — same across delivery retries
    run_id         : unique per simulator replay pass
    dataset_id     : e.g. 'FD001', 'IMS_2nd'
    asset_id       : string engine/bearing identifier, e.g. 'engine_1'
    timestamp      : UTC ISO-8601 wall-clock time of publication
    sequence       : monotonically increasing per (asset_id, run_id)
    source_type    : 'cmapss' | 'ims' | 'synthetic'
    source_split   : 'train' | 'test' | 'live'
    """

    schema_version: str
    event_id: str
    run_id: str
    dataset_id: str
    asset_id: str
    timestamp: str
    sequence: int
    source_type: str
    source_split: str = "train"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "run_id": self.run_id,
            "dataset_id": self.dataset_id,
            "asset_id": self.asset_id,
            "timestamp": self.timestamp,
            "sequence": self.sequence,
            "source_type": self.source_type,
            "source_split": self.source_split,
        }


def build_envelope(
    run_id: str,
    dataset_id: str,
    asset_id: str,
    sequence: int,
    source_type: str,
    source_split: str = "train",
) -> TelemetryEnvelope:
    """Build an envelope with a deterministic event_id and current UTC time."""
    return TelemetryEnvelope(
        schema_version=SCHEMA_VERSION,
        event_id=make_event_id(run_id, asset_id, sequence),
        run_id=run_id,
        dataset_id=dataset_id,
        asset_id=asset_id,
        timestamp=datetime.now(timezone.utc).isoformat(),
        sequence=sequence,
        source_type=source_type,
        source_split=source_split,
    )


# ---------------------------------------------------------------------------
# Sequence tracker
# ---------------------------------------------------------------------------

class SequenceTracker:
    """Per-(run_id, asset_id) sequence counter with event-ID deduplication.

    Tracks both the highest seen sequence number AND a bounded set of
    previously seen event_ids per (run_id, asset_id) pair.  This allows
    correct classification of:

    - Same event_id re-delivered  → 'duplicate'   (regardless of current last)
    - Different event_id, same seq → 'sequence_conflict' (publisher bug)
    - Sequence below last, unseen event_id → 'out_of_order:N'
    - Forward skip → 'gap:N'
    - Expected next → 'ok'

    Parameters
    ----------
    max_event_ids_per_key : int
        Maximum number of event_ids to remember per (run_id, asset_id) pair.
        Oldest entries are evicted when the limit is reached.  Default 2 048
        covers a full FD001 single-replay pass (20 631 rows) with minimal RAM
        (each event_id is a 36-char string ≈ 36 bytes → 2 048 × 36 ≈ 72 KB
        per asset).  Set to 0 to disable event-ID tracking entirely.

    Restart limitation
    ------------------
    This tracker is in-memory.  After a service restart all state is lost —
    previously seen event_ids and sequence numbers are forgotten.  Any event
    that was already delivered before the restart will be re-accepted.  For
    durable deduplication across restarts, persist the event_id seen-set to
    an external store (e.g. Redis SISMEMBER with TTL).
    """

    def __init__(self, max_event_ids_per_key: int = 2048) -> None:
        # {(run_id, asset_id): last_sequence}
        self._counters: dict[tuple[str, str], int] = {}
        # {(run_id, asset_id): OrderedDict[event_id -> None]}  (insertion-ordered LRU)
        self._seen_eids: dict[tuple[str, str], OrderedDict[str, None]] = {}
        self._max_eids = max_event_ids_per_key

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _register_event_id(self, key: tuple[str, str], event_id: str) -> None:
        """Add event_id to the bounded seen-set, evicting oldest if full."""
        if self._max_eids <= 0:
            return
        seen = self._seen_eids.setdefault(key, OrderedDict())
        if event_id not in seen:
            seen[event_id] = None
            if len(seen) > self._max_eids:
                seen.popitem(last=False)   # evict oldest

    def _already_seen(self, key: tuple[str, str], event_id: str) -> bool:
        """Return True if event_id is in the current seen-set."""
        if self._max_eids <= 0 or event_id is None:
            return False
        return event_id in self._seen_eids.get(key, {})

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def next(self, run_id: str, asset_id: str) -> int:
        """Return the next sequence number for (run_id, asset_id)."""
        key = (run_id, asset_id)
        current = self._counters.get(key, 0)
        nxt = current + 1
        self._counters[key] = nxt
        return nxt

    def check(
        self,
        run_id: str,
        asset_id: str,
        received_seq: int,
        event_id: str | None = None,
    ) -> tuple[bool, str]:
        """Check received sequence (and optional event_id); return (ok, reason).

        Parameters
        ----------
        run_id       : replay run identifier
        asset_id     : engine or bearing identifier
        received_seq : sequence number carried in the envelope
        event_id     : deterministic event identifier (optional).
                       When provided, enables event-ID-level deduplication.

        Returns
        -------
        (True,  'ok')                 — sequence is exactly as expected
        (False, 'duplicate')          — same event_id already seen in this run
                                        (delivery retry, storage must be skipped)
        (False, 'sequence_conflict')  — different event_id at an already-used
                                        sequence position (publisher bug)
        (False, 'out_of_order:N')     — event not seen before, arrived N behind
                                        the current last sequence
        (False, 'gap:N')              — N sequences were skipped above last seen
        """
        key = (run_id, asset_id)

        # ── 1. Event-ID deduplication (checked before sequence logic) ──────
        if event_id is not None and self._already_seen(key, event_id):
            return False, "duplicate"

        last = self._counters.get(key, 0)

        # ── 2. Classify by sequence position ───────────────────────────────
        if received_seq < last:
            behind = last - received_seq
            # Register the event_id even for late arrivals so callers can
            # process valid late events exactly once.
            if event_id is not None:
                self._register_event_id(key, event_id)
            return False, f"out_of_order:{behind}"

        if received_seq == last:
            # Same sequence as last — could be a different event at the same
            # sequence position (publisher bug) or an exact sequence repeat.
            if event_id is not None and self._max_eids > 0:
                # Different event_id at an already-used sequence → conflict.
                # (Only meaningful when event_id tracking is enabled.)
                return False, "sequence_conflict"
            return False, "duplicate"

        # received_seq > last
        if received_seq == last + 1:
            self._counters[key] = received_seq
            if event_id is not None:
                self._register_event_id(key, event_id)
            return True, "ok"

        # received_seq > last + 1 → gap
        gap = received_seq - last - 1
        self._counters[key] = received_seq
        if event_id is not None:
            self._register_event_id(key, event_id)
        return False, f"gap:{gap}"

    def reset(self, run_id: str, asset_id: str) -> None:
        """Reset sequence tracking and event-ID history for a given (run_id, asset_id)."""
        key = (run_id, asset_id)
        self._counters.pop(key, None)
        self._seen_eids.pop(key, None)
