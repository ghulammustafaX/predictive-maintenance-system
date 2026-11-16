"""Tests for duplicate detection, gap detection, and out-of-order handling."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "simulator" / "src"
sys.path.insert(0, str(SRC))


class TestSequenceTrackerEdgeCases:
    """Extended edge-case tests for the SequenceTracker."""

    def test_first_event_accepted(self) -> None:
        from event_schema import SequenceTracker
        t = SequenceTracker()
        ok, reason = t.check("r", "e1", 1)
        assert ok
        assert reason == "ok"

    def test_zero_sequence_treated_as_duplicate(self) -> None:
        """Sequence counter starts at 0 — seq 0 is an exact repeat of initial state."""
        from event_schema import SequenceTracker
        t = SequenceTracker()
        ok, reason = t.check("r", "e1", 0)
        assert not ok
        assert reason == "duplicate"

    def test_out_of_order_distinguished_from_duplicate(self) -> None:
        """seq < last_seen must return out_of_order, not duplicate."""
        from event_schema import SequenceTracker
        t = SequenceTracker()
        t.check("r", "e1", 1); t.check("r", "e1", 2); t.check("r", "e1", 3)
        # exact repeat of last
        ok_dup, reason_dup = t.check("r", "e1", 3)
        assert not ok_dup and reason_dup == "duplicate"
        # older seq (below last) — out-of-order, not duplicate
        ok_ooo, reason_ooo = t.check("r", "e1", 1)
        assert not ok_ooo and reason_ooo.startswith("out_of_order")

    def test_large_gap_reported(self) -> None:
        from event_schema import SequenceTracker
        t = SequenceTracker()
        t.check("r", "e1", 1)
        ok, reason = t.check("r", "e1", 100)   # gap of 98
        assert not ok
        assert reason == "gap:98"

    def test_multiple_assets_do_not_interfere(self) -> None:
        from event_schema import SequenceTracker
        t = SequenceTracker()
        for seq in range(1, 11):
            ok, _ = t.check("r", "engine_1", seq)
            assert ok
        for seq in range(1, 11):
            ok, _ = t.check("r", "engine_2", seq)
            assert ok

    def test_different_runs_do_not_interfere(self) -> None:
        from event_schema import SequenceTracker
        t = SequenceTracker()
        t.check("run_A", "e1", 1)
        t.check("run_A", "e1", 2)
        # run_B starts fresh
        ok, reason = t.check("run_B", "e1", 1)
        assert ok

    def test_reset_then_reuse(self) -> None:
        from event_schema import SequenceTracker
        t = SequenceTracker()
        t.check("r", "e1", 1)
        t.check("r", "e1", 2)
        t.reset("r", "e1")
        ok, reason = t.check("r", "e1", 1)
        assert ok

    def test_state_persists_across_calls(self) -> None:
        from event_schema import SequenceTracker
        t = SequenceTracker()
        for i in range(1, 101):
            ok, reason = t.check("r", "e1", i)
            assert ok, f"Unexpected failure at seq={i}: {reason}"


class TestEventIDIdempotency:
    """Event ID must not change when the same (run, asset, seq) is re-sent."""

    def test_stable_on_retry(self) -> None:
        from event_schema import make_event_id
        for _ in range(10):
            eid = make_event_id("run_1", "engine_42", 999)
            assert eid == make_event_id("run_1", "engine_42", 999)

    def test_all_different_for_sequential_events(self) -> None:
        from event_schema import make_event_id
        ids = [make_event_id("r", "e1", i) for i in range(1, 1001)]
        assert len(set(ids)) == 1000, "Event IDs should all be unique across sequences"


class TestKafkaKeyUniqueness:
    """Kafka keys must route same-engine events to the same partition."""

    def test_same_engine_same_run_same_key(self) -> None:
        from cmapss_loader import iter_rows
        import pandas as pd

        # Minimal synthetic df
        df = pd.DataFrame({
            "unit_id": [1, 1, 1],
            "time_cycles": [1, 2, 3],
            "operational_setting_1": [0.0, 0.0, 0.0],
            "operational_setting_2": [0.0, 0.0, 0.0],
            "operational_setting_3": [100.0, 100.0, 100.0],
            **{f"sensor_{i}": [float(i)] * 3 for i in range(1, 22)},
        })
        df["unit_id"] = df["unit_id"].astype(int)
        df["time_cycles"] = df["time_cycles"].astype(int)

        keys = [k for _, k, _ in iter_rows(df, "my_run", "FD001", "train")]
        # All keys for unit_id=1 in my_run should be identical
        assert len(set(keys)) == 1
        assert keys[0] == "FD001:1:my_run"

    def test_different_engine_different_key(self) -> None:
        from cmapss_loader import iter_rows
        import pandas as pd

        df = pd.DataFrame({
            "unit_id": [1, 2],
            "time_cycles": [1, 1],
            "operational_setting_1": [0.0, 0.0],
            "operational_setting_2": [0.0, 0.0],
            "operational_setting_3": [100.0, 100.0],
            **{f"sensor_{i}": [float(i), float(i)] for i in range(1, 22)},
        })
        df["unit_id"] = df["unit_id"].astype(int)
        df["time_cycles"] = df["time_cycles"].astype(int)

        keys = [k for _, k, _ in iter_rows(df, "r", "FD001", "train")]
        assert keys[0] != keys[1]
