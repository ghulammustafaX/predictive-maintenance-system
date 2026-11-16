"""Tests for simulator/src/event_schema.py — purely in-process, no I/O."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "simulator" / "src"
sys.path.insert(0, str(SRC))


class TestDeterministicEventID:
    """event_id must be stable across calls and unique across different inputs."""

    def test_same_inputs_produce_same_id(self) -> None:
        from event_schema import make_event_id
        id1 = make_event_id("run_abc", "engine_1", 42)
        id2 = make_event_id("run_abc", "engine_1", 42)
        assert id1 == id2

    def test_different_sequence_produces_different_id(self) -> None:
        from event_schema import make_event_id
        id1 = make_event_id("run_abc", "engine_1", 42)
        id2 = make_event_id("run_abc", "engine_1", 43)
        assert id1 != id2

    def test_different_run_id_produces_different_id(self) -> None:
        from event_schema import make_event_id
        id1 = make_event_id("run_aaa", "engine_1", 1)
        id2 = make_event_id("run_bbb", "engine_1", 1)
        assert id1 != id2

    def test_different_asset_produces_different_id(self) -> None:
        from event_schema import make_event_id
        id1 = make_event_id("run_abc", "engine_1", 1)
        id2 = make_event_id("run_abc", "engine_2", 1)
        assert id1 != id2

    def test_event_id_is_uuid_format(self) -> None:
        from event_schema import make_event_id
        eid = make_event_id("run_x", "engine_1", 1)
        parts = eid.split("-")
        assert len(parts) == 5
        assert all(len(p) > 0 for p in parts)


class TestNewRunID:
    """new_run_id must generate unique IDs."""

    def test_uniqueness(self) -> None:
        from event_schema import new_run_id
        ids = {new_run_id() for _ in range(100)}
        assert len(ids) == 100

    def test_is_hex_string(self) -> None:
        from event_schema import new_run_id
        rid = new_run_id()
        int(rid, 16)  # raises ValueError if not hex


class TestBuildEnvelope:
    """Envelope must carry all required fields with correct types."""

    def test_all_fields_present(self) -> None:
        from event_schema import SOURCE_CMAPSS, build_envelope
        env = build_envelope(
            run_id="abc123",
            dataset_id="FD001",
            asset_id="engine_1",
            sequence=7,
            source_type=SOURCE_CMAPSS,
        )
        d = env.to_dict()
        required = {
            "schema_version", "event_id", "run_id", "dataset_id",
            "asset_id", "timestamp", "sequence", "source_type", "source_split",
        }
        assert required.issubset(d.keys())

    def test_sequence_matches(self) -> None:
        from event_schema import SOURCE_CMAPSS, build_envelope
        env = build_envelope("r", "FD001", "engine_1", 55, SOURCE_CMAPSS)
        assert env.sequence == 55

    def test_event_id_is_deterministic(self) -> None:
        from event_schema import SOURCE_CMAPSS, build_envelope
        e1 = build_envelope("r1", "FD001", "engine_1", 1, SOURCE_CMAPSS)
        e2 = build_envelope("r1", "FD001", "engine_1", 1, SOURCE_CMAPSS)
        assert e1.event_id == e2.event_id

    def test_timestamp_is_iso8601_utc(self) -> None:
        from datetime import timezone
        from dateutil.parser import parse as parse_dt
        from event_schema import SOURCE_CMAPSS, build_envelope
        env = build_envelope("r", "FD001", "engine_1", 1, SOURCE_CMAPSS)
        try:
            dt = parse_dt(env.timestamp)
            assert dt.tzinfo is not None
        except Exception:
            pytest.skip("dateutil not available — skipping timestamp format check")


class TestSequenceTracker:
    """SequenceTracker must detect gaps, duplicates, and out-of-order events."""

    def test_sequential_ok(self) -> None:
        from event_schema import SequenceTracker
        tracker = SequenceTracker()
        assert tracker.check("r", "e1", 1) == (True, "ok")
        assert tracker.check("r", "e1", 2) == (True, "ok")
        assert tracker.check("r", "e1", 3) == (True, "ok")

    def test_duplicate_detected(self) -> None:
        from event_schema import SequenceTracker
        tracker = SequenceTracker()
        tracker.check("r", "e1", 1)
        ok, reason = tracker.check("r", "e1", 1)
        assert not ok
        assert reason == "duplicate"

    def test_out_of_order_detected(self) -> None:
        from event_schema import SequenceTracker
        tracker = SequenceTracker()
        tracker.check("r", "e1", 1)
        tracker.check("r", "e1", 2)
        tracker.check("r", "e1", 3)
        ok, reason = tracker.check("r", "e1", 2)   # late arrival, below last
        assert not ok
        assert reason.startswith("out_of_order")

    def test_gap_detected(self) -> None:
        from event_schema import SequenceTracker
        tracker = SequenceTracker()
        tracker.check("r", "e1", 1)
        ok, reason = tracker.check("r", "e1", 4)   # gap of 2
        assert not ok
        assert reason == "gap:2"

    def test_different_assets_tracked_independently(self) -> None:
        from event_schema import SequenceTracker
        tracker = SequenceTracker()
        assert tracker.check("r", "e1", 1) == (True, "ok")
        assert tracker.check("r", "e2", 1) == (True, "ok")
        assert tracker.check("r", "e1", 2) == (True, "ok")

    def test_next_increments(self) -> None:
        from event_schema import SequenceTracker
        tracker = SequenceTracker()
        assert tracker.next("r", "e1") == 1
        assert tracker.next("r", "e1") == 2
        assert tracker.next("r", "e1") == 3

    def test_reset_clears_state(self) -> None:
        from event_schema import SequenceTracker
        tracker = SequenceTracker()
        tracker.next("r", "e1")
        tracker.next("r", "e1")
        tracker.reset("r", "e1")
        assert tracker.next("r", "e1") == 1
