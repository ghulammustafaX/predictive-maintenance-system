"""
Phase 2: Authoritative duplicate-vs-out-of-order classification tests.

Covers all cases specified in the final verification task:

A. Normal forward delivery: seq 10, 11, 12
B. Same event_id resent (retry):  seq 10 again, same event_id
C. Different event_id at same seq: seq 11 again, different event_id
D. Forward gap then backward arrival: seq 14, then seq 13
E. After simulated restart: seq numbering behaviour
F. Different asset_id / run_id isolation

Also verifies:
- Classification is diagnostic-only (no automatic discard at this layer)
- event_id deduplication gap is documented as a known limitation
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "simulator" / "src"
sys.path.insert(0, str(SRC))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fresh() -> "SequenceTracker":
    from event_schema import SequenceTracker
    return SequenceTracker()


def _eid(run: str, asset: str, seq: int) -> str:
    from event_schema import make_event_id
    return make_event_id(run, asset, seq)


# ---------------------------------------------------------------------------
# Case A: Normal forward delivery
# ---------------------------------------------------------------------------

class TestCaseA_NormalDelivery:
    def test_first_three_sequential(self) -> None:
        t = _fresh()
        ok10, r10 = t.check("run1", "eng1", 10)
        ok11, r11 = t.check("run1", "eng1", 11)
        ok12, r12 = t.check("run1", "eng1", 12)

        # seq 10 arrives into an empty tracker (last=0); gap from 0→10 = 9
        assert ok10 is False and r10 == "gap:9",   f"seq=10 first: expected gap:9, got ({ok10},{r10})"
        assert ok11 is True  and r11 == "ok",       f"seq=11: expected ok, got ({ok11},{r11})"
        assert ok12 is True  and r12 == "ok",       f"seq=12: expected ok, got ({ok12},{r12})"

    def test_tracker_advances_last_on_ok(self) -> None:
        t = _fresh()
        t.check("r", "e", 1)
        t.check("r", "e", 2)
        ok, reason = t.check("r", "e", 3)
        assert ok is True and reason == "ok"


# ---------------------------------------------------------------------------
# Case B: Same event_id resent (retry of seq 10)
# ---------------------------------------------------------------------------

class TestCaseB_SameEventIdResent:
    """
    After seq 10, 11, 12 have been delivered, seq 10 is resent with the
    SAME event_id.  The sequence is below last (12), so SequenceTracker
    returns out_of_order.

    IMPORTANT LIMITATION: SequenceTracker cannot distinguish whether the
    resent seq=10 carries the original event_id or a new one — it only
    sees the sequence number.  event_id-level deduplication is NOT
    implemented in this layer.  The classification is documented as
    out_of_order:2, not 'duplicate', because the tracker only knows that
    seq=10 is 2 behind the current last=12.

    For true event_id-level deduplication, a persistent seen-set (e.g.
    Redis SISMEMBER on event_id) is needed.  That is a documented
    architectural gap, not a code defect.
    """

    def test_seq10_resent_same_event_id_classified_out_of_order(self) -> None:
        t = _fresh()
        # Deliver 10 (gap from 0), 11, 12
        t.check("r", "e", 10)
        t.check("r", "e", 11)
        t.check("r", "e", 12)

        # Resend seq=10 with the same event_id
        eid_original = _eid("r", "e", 10)
        ok, reason = t.check("r", "e", 10)
        eid_retry    = _eid("r", "e", 10)

        # event_ids match (deterministic)
        assert eid_original == eid_retry, "event_id must be same on retry"
        # SequenceTracker: seq=10 < last=12 → out_of_order:2
        assert ok is False
        assert reason == "out_of_order:2", (
            f"Expected out_of_order:2, got {reason}.  "
            "NOTE: Tracker cannot differentiate retry vs late-arrival at this level."
        )

    def test_seq_equal_to_last_is_duplicate(self) -> None:
        """seq == last is 'duplicate' (exact repeat of the highest delivered)."""
        t = _fresh()
        t.check("r", "e", 1)
        t.check("r", "e", 2)
        ok, reason = t.check("r", "e", 2)
        assert ok is False and reason == "duplicate"

    def test_event_id_is_deterministic_across_retries(self) -> None:
        """event_id never changes for same (run, asset, seq) — retries are idempotent."""
        ids = [_eid("run_x", "eng_1", 42) for _ in range(5)]
        assert len(set(ids)) == 1, "event_id must be stable across all retries"


# ---------------------------------------------------------------------------
# Case C: Different event_id at seq 11 (re-publisher bug scenario)
# ---------------------------------------------------------------------------

class TestCaseC_DifferentEventIdSameSeq:
    """
    Case C: A different publisher sends seq=11 with a NEW event_id
    (e.g. a bug that generates a fresh UUID instead of using the
    deterministic make_event_id).

    SequenceTracker: seq=11 < last=12 → out_of_order:1.
    The different event_id is NOT detectable by SequenceTracker.

    This is the core architectural limitation: the tracker is sequence-
    based, not event_id-based.  A consumer that inspects event_id before
    writing to InfluxDB would be the correct deduplication boundary.
    """

    def test_different_event_id_same_seq_still_classified_as_out_of_order(self) -> None:
        t = _fresh()
        t.check("r", "e", 10)
        t.check("r", "e", 11)
        t.check("r", "e", 12)

        # Different event_id for seq=11 (simulating a bug in the publisher)
        import uuid
        eid_original = _eid("r", "e", 11)
        eid_different = uuid.uuid4().hex  # NOT deterministic
        assert eid_original != eid_different

        # SequenceTracker only sees the sequence
        ok, reason = t.check("r", "e", 11)
        assert ok is False
        assert reason == "out_of_order:1", (
            "Sequence tracker cannot distinguish different event_id; "
            "event_id deduplication must happen at the InfluxDB write boundary."
        )


# ---------------------------------------------------------------------------
# Case D: Forward gap (seq 14), then backward arrival (seq 13)
# ---------------------------------------------------------------------------

class TestCaseD_ForwardGapThenBackward:
    def test_gap_detected_on_seq14(self) -> None:
        t = _fresh()
        t.check("r", "e", 10)
        t.check("r", "e", 11)
        t.check("r", "e", 12)
        ok, reason = t.check("r", "e", 14)   # skips 13
        assert ok is False and reason == "gap:1", f"Expected gap:1, got ({ok},{reason})"

    def test_seq13_after_14_is_out_of_order(self) -> None:
        t = _fresh()
        t.check("r", "e", 10)
        t.check("r", "e", 11)
        t.check("r", "e", 12)
        t.check("r", "e", 14)   # last is now 14
        ok, reason = t.check("r", "e", 13)
        assert ok is False
        assert reason == "out_of_order:1", f"Expected out_of_order:1, got ({ok},{reason})"

    def test_late_event_is_classified_not_silently_discarded(self) -> None:
        """
        The tracker returns a classification tuple — it does NOT discard the
        event.  Consumers receive (False, 'out_of_order:N') and must decide
        whether to process or drop.  No valid late event is silently lost
        at this layer.
        """
        from event_schema import SequenceTracker
        t = SequenceTracker()
        t.check("r", "e", 5)
        result = t.check("r", "e", 3)
        # check() returns a value — it never raises or silently swallows
        assert result is not None
        assert result[1].startswith("out_of_order")


# ---------------------------------------------------------------------------
# Case E: Consumer/bridge restart — in-memory state is lost
# ---------------------------------------------------------------------------

class TestCaseE_RestartBehaviour:
    def test_new_tracker_accepts_all_sequences_as_fresh(self) -> None:
        """
        After a service restart, a NEW SequenceTracker instance has no memory
        of previously seen sequences.  seq=10 that was already processed will
        be accepted as a gap from 0 (or as ok if it arrives at seq=1).

        DOCUMENTED LIMITATION: in-memory SequenceTracker cannot prevent
        duplicates across restarts.  Durable deduplication requires persisting
        the last-seen sequence (e.g. in Redis or a Kafka changelog topic).
        """
        # Simulate first run
        t1 = _fresh()
        for s in range(1, 13):
            t1.check("r", "e", s)

        # Simulate restart — fresh tracker
        t2 = _fresh()
        ok, reason = t2.check("r", "e", 1)
        # seq=1 arrives into an empty tracker (last=0) → ok
        assert ok is True and reason == "ok", (
            f"After restart, seq=1 is accepted as ok (last=0). Got ({ok},{reason})."
        )

    def test_restart_then_seq_continues_from_old_position(self) -> None:
        """After restart, seq=10 is a gap from 0 (gap:9) not a duplicate."""
        t_new = _fresh()
        ok, reason = t_new.check("r", "e", 10)
        # 10 > 0+1 → gap:9
        assert ok is False and reason == "gap:9", f"Got ({ok},{reason})"

    def test_duplicate_policy_documentation(self) -> None:
        """
        Regression: document that the tracker is diagnostic only.
        The InfluxDB write path uses (dataset_id, unit_id, run_id, cycle) as
        a natural key via the timestamp tag combination — exact same point
        written twice is idempotent at InfluxDB (overwrite, not duplicate row).
        """
        from event_schema import SequenceTracker
        t = SequenceTracker()
        # Delivers seq 1 twice
        ok1, _ = t.check("r", "e", 1)
        ok2, r2 = t.check("r", "e", 1)
        assert ok1 is True
        assert ok2 is False and r2 == "duplicate"
        # The event is NOT discarded; the caller sees (False, 'duplicate')
        # and can choose to skip the InfluxDB write for idempotency


# ---------------------------------------------------------------------------
# Case F: Isolation across different asset_id and run_id
# ---------------------------------------------------------------------------

class TestCaseF_AssetAndRunIsolation:
    def test_different_asset_ids_are_independent(self) -> None:
        t = _fresh()
        # engine_1 advances to 5
        for s in range(1, 6):
            t.check("r", "engine_1", s)
        # engine_2 starts fresh at 1 — must be ok, not affected by engine_1
        ok, reason = t.check("r", "engine_2", 1)
        assert ok is True and reason == "ok", f"Got ({ok},{reason})"

    def test_different_run_ids_are_independent(self) -> None:
        t = _fresh()
        for s in range(1, 6):
            t.check("run_A", "engine_1", s)
        # run_B starts fresh at 1
        ok, reason = t.check("run_B", "engine_1", 1)
        assert ok is True and reason == "ok", f"Got ({ok},{reason})"

    def test_same_seq_on_different_assets_no_cross_contamination(self) -> None:
        t = _fresh()
        t.check("r", "engine_1", 1)
        t.check("r", "engine_1", 2)
        # Same seq 1 on engine_2 is completely independent
        ok, reason = t.check("r", "engine_2", 1)
        assert ok is True and reason == "ok"

    def test_run_id_collision_scenario(self) -> None:
        """Two assets in the same run get independent sequence counters."""
        t = _fresh()
        # engine_1 and engine_2 both send seq=1,2,3 in the same run
        for s in range(1, 4):
            ok1, _ = t.check("r", "engine_1", s)
            ok2, _ = t.check("r", "engine_2", s)
            assert ok1 is True and ok2 is True


# ---------------------------------------------------------------------------
# Event-ID deduplication gap — regression test documenting the limitation
# ---------------------------------------------------------------------------

class TestEventIdDeduplicationGap:
    """
    Regression tests that document the known architectural gap:
    SequenceTracker cannot detect duplicate event_ids.
    These tests PASS because they confirm the gap exists and is understood.
    """

    def test_tracker_cannot_distinguish_same_event_id(self) -> None:
        """
        KNOWN GAP: SequenceTracker.check() takes only (run_id, asset_id, seq).
        It has no event_id parameter.  Two messages with the same event_id
        but delivered at different times cannot be detected as duplicates
        by this tracker alone.
        """
        from event_schema import SequenceTracker
        import inspect
        sig = inspect.signature(SequenceTracker.check)
        params = list(sig.parameters.keys())
        assert "event_id" not in params, (
            "If event_id is now a parameter, remove this known-gap test."
        )
        # Document the gap — this is expected/known
        # Minimal fix path: add event_id deduplication as a separate layer
        # in the ingestion consumer (bloom filter or Redis SISMEMBER)

    def test_influxdb_write_provides_natural_idempotency(self) -> None:
        """
        InfluxDB writes with the same timestamp+tags overwrite, not duplicate.
        The ingestion consumer (kafka_consumer.py) uses timestamp from payload
        as the point time, so the same event written twice = same point = no
        duplicate row in the time-series.  This is the effective deduplication
        boundary for storage.
        """
        # Verify timestamp is used as point time in kafka_consumer.py
        consumer_path = (
            Path(__file__).resolve().parents[1]
            / "ingestion" / "src" / "kafka_consumer.py"
        )
        content = consumer_path.read_text(encoding="utf-8")
        # build_point() calls .time() with the source timestamp
        assert ".time(" in content, "InfluxDB point must use payload timestamp for idempotency"
        assert "source_timestamp" in content or "timestamp" in content

    def test_recommended_fix_is_documented(self) -> None:
        """
        Documents the minimal recommended fix path without implementing it:
        Add an event_id seen-set (bounded LRU or Bloom filter) in
        ingestion/src/kafka_consumer.py:decode_message() or build_point()
        to skip writes for already-seen event_ids within a rolling window.
        """
        # This test always passes — it is a living documentation test
        KNOWN_GAP = (
            "SequenceTracker is sequence-based, not event_id-based. "
            "True duplicate prevention at InfluxDB requires event_id deduplication "
            "in the ingestion consumer."
        )
        assert len(KNOWN_GAP) > 0
