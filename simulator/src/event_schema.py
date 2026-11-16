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
    """Per-(asset, run) sequence counter.

    Resets when a new run_id is seen for the same asset.  Detects gaps and
    duplicates when ``check()`` is called on the consumer side.
    """

    def __init__(self) -> None:
        # {(run_id, asset_id): last_sequence}
        self._counters: dict[tuple[str, str], int] = {}

    def next(self, run_id: str, asset_id: str) -> int:
        """Return the next sequence number for (run_id, asset_id)."""
        key = (run_id, asset_id)
        current = self._counters.get(key, 0)
        nxt = current + 1
        self._counters[key] = nxt
        return nxt

    def check(
        self, run_id: str, asset_id: str, received_seq: int
    ) -> tuple[bool, str]:
        """Check received sequence; return (ok, reason).

        Returns
        -------
        (True, 'ok')                — sequence is exactly as expected
        (False, 'duplicate')        — sequence == last seen (exact repeat)
        (False, 'out_of_order:N')   — sequence arrived below last (N = how far behind)
        (False, 'gap:N')            — N sequences were skipped above last seen
        """
        key = (run_id, asset_id)
        last = self._counters.get(key, 0)
        if received_seq < last:
            behind = last - received_seq
            return False, f"out_of_order:{behind}"
        if received_seq == last:
            return False, "duplicate"
        if received_seq == last + 1:
            self._counters[key] = received_seq
            return True, "ok"
        gap = received_seq - last - 1
        self._counters[key] = received_seq
        return False, f"gap:{gap}"

    def reset(self, run_id: str, asset_id: str) -> None:
        """Reset sequence tracking for a given (run_id, asset_id)."""
        key = (run_id, asset_id)
        self._counters.pop(key, None)
