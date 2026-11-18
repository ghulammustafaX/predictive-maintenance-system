"""
Phase 2: Authoritative duplicate-vs-out-of-order classification tests.

Covers all cases specified in the final verification task with the
upgraded SequenceTracker that accepts an optional event_id parameter:

A. Normal forward delivery: seq 10, 11, 12
B. Same event_id resent (retry) — MUST return 'duplicate'
C. Different event_id at same seq — MUST return 'sequence_conflict'
D. Forward gap (seq 14), then backward arrival (seq 13)
E. Consumer/bridge restart behaviour
F. Different asset_id / run_id isolation
G. Bounded LRU eviction behaviour
H. Backward-compatible mode (no event_id supplied)
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "simulator" / "src"
sys.path.insert(0, str(SRC))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fresh(max_eids: int = 2048) -> "SequenceTracker":
    from event_schema import SequenceTracker
    return SequenceTracker(max_event_ids_per_key=max_eids)


def _eid(run: str, asset: str, seq: int) -> str:
    from event_schema import make_event_id
    return make_event_id(run, asset, seq)


def _new_eid() -> str:
    """Generate a random event_id (simulates a buggy publisher)."""
    return uuid.uuid4().hex


# ---------------------------------------------------------------------------
# Case A: Normal forward delivery
# ---------------------------------------------------------------------------

class TestCaseA_NormalDelivery:
    def test_first_three_sequential_no_eid(self) -> None:
        """Backward-compatible mode: no event_id supplied."""
        t = _fresh()
        ok10, r10 = t.check("run1", "eng1", 10)
        ok11, r11 = t.check("run1", "eng1", 11)
        ok12, r12 = t.check("run1", "eng1", 12)
        assert ok10 is False and r10 == "gap:9"
        assert ok11 is True  and r11 == "ok"
        assert ok12 is True  and r12 == "ok"

    def test_first_three_sequential_with_eid(self) -> None:
        """With event_id supplied: same classification, event_ids registered."""
        t = _fresh()
        ok10, r10 = t.check("r", "e", 10, event_id=_eid("r","e",10))
        ok11, r11 = t.check("r", "e", 11, event_id=_eid("r","e",11))
        ok12, r12 = t.check("r", "e", 12, event_id=_eid("r","e",12))
        assert ok10 is False and r10 == "gap:9"
        assert ok11 is True  and r11 == "ok"
        assert ok12 is True  and r12 == "ok"

    def test_tracker_advances_last_on_ok(self) -> None:
        t = _fresh()
        t.check("r", "e", 1, event_id=_eid("r","e",1))
        t.check("r", "e", 2, event_id=_eid("r","e",2))
        ok, reason = t.check("r", "e", 3, event_id=_eid("r","e",3))
        assert ok is True and reason == "ok"


# ---------------------------------------------------------------------------
# Case B: Same event_id resent — MUST return 'duplicate'
# ---------------------------------------------------------------------------

class TestCaseB_SameEventIdResent:
    """
    After seq 10, 11, 12 have been delivered and their event_ids registered,
    resending seq=10 with the SAME event_id MUST return 'duplicate'
    regardless of the fact that seq 10 < last=12.
    """

    def test_resent_seq10_same_event_id_is_duplicate(self) -> None:
        t = _fresh()
        eid_10 = _eid("r", "e", 10)
        eid_11 = _eid("r", "e", 11)
        eid_12 = _eid("r", "e", 12)

        t.check("r", "e", 10, event_id=eid_10)
        t.check("r", "e", 11, event_id=eid_11)
        t.check("r", "e", 12, event_id=eid_12)

        # Resend seq=10 with its original event_id
        ok, reason = t.check("r", "e", 10, event_id=eid_10)
        assert ok is False
        assert reason == "duplicate", (
            f"Expected 'duplicate' for same event_id resent, got {reason!r}"
        )

    def test_resent_last_seq_same_event_id_is_duplicate(self) -> None:
        """Even the most-recently-delivered seq is duplicate if same event_id."""
        t = _fresh()
        eid_1 = _eid("r", "e", 1)
        t.check("r", "e", 1, event_id=eid_1)
        ok, reason = t.check("r", "e", 1, event_id=eid_1)
        assert ok is False and reason == "duplicate"

    def test_resent_many_cycles_later_still_duplicate(self) -> None:
        """Duplicate detection works even after many subsequent events."""
        t = _fresh()
        eid_1 = _eid("r", "e", 1)
        t.check("r", "e", 1, event_id=eid_1)
        for s in range(2, 102):
            t.check("r", "e", s, event_id=_eid("r","e",s))
        # Resend seq=1 100 steps later
        ok, reason = t.check("r", "e", 1, event_id=eid_1)
        assert ok is False and reason == "duplicate", (
            f"Expected duplicate 100 steps later, got {reason!r}"
        )

    def test_event_id_deterministic_across_retries(self) -> None:
        """event_id never changes for same (run, asset, seq)."""
        ids = [_eid("run_x", "eng_1", 42) for _ in range(5)]
        assert len(set(ids)) == 1

    def test_seq_equal_to_last_no_eid_is_duplicate(self) -> None:
        """Backward-compatible: seq==last without event_id is 'duplicate'."""
        t = _fresh()
        t.check("r", "e", 1)
        t.check("r", "e", 2)
        ok, reason = t.check("r", "e", 2)
        assert ok is False and reason == "duplicate"


# ---------------------------------------------------------------------------
# Case C: Different event_id at an already-used sequence
# ---------------------------------------------------------------------------

class TestCaseC_DifferentEventIdSameSeq:
    """
    Case C: A different publisher sends seq=12 with a NEW event_id while
    seq=12 was already delivered with the correct deterministic event_id.
    This is a publisher bug (non-deterministic UUID).  The tracker must
    return 'sequence_conflict', NOT 'duplicate', because the event_id is
    different — it is a genuinely different event at a conflicting position.
    """

    def test_different_event_id_at_used_seq_is_sequence_conflict(self) -> None:
        t = _fresh()
        eid_12 = _eid("r", "e", 12)
        t.check("r", "e", 10, event_id=_eid("r","e",10))
        t.check("r", "e", 11, event_id=_eid("r","e",11))
        t.check("r", "e", 12, event_id=eid_12)

        # Different event_id at seq=12 (last) — publisher bug
        rogue_eid = _new_eid()
        assert rogue_eid != eid_12

        ok, reason = t.check("r", "e", 12, event_id=rogue_eid)
        assert ok is False
        assert reason == "sequence_conflict", (
            f"Expected 'sequence_conflict' for different event_id at used seq, got {reason!r}"
        )

    def test_different_event_id_at_old_seq_is_out_of_order(self) -> None:
        """Different event_id at seq < last (not at last) → out_of_order:N."""
        t = _fresh()
        t.check("r", "e", 10, event_id=_eid("r","e",10))
        t.check("r", "e", 11, event_id=_eid("r","e",11))
        t.check("r", "e", 12, event_id=_eid("r","e",12))

        rogue_eid = _new_eid()
        ok, reason = t.check("r", "e", 10, event_id=rogue_eid)
        assert ok is False
        assert reason == "out_of_order:2", (
            f"Different event_id at seq=10, last=12 → out_of_order:2, got {reason!r}"
        )

    def test_sequence_conflict_does_not_advance_last(self) -> None:
        """A sequence_conflict must not advance the last-seen counter."""
        t = _fresh()
        t.check("r", "e", 1, event_id=_eid("r","e",1))
        # Conflict at seq=1 with different event_id
        t.check("r", "e", 1, event_id=_new_eid())
        # Next expected sequence must still be 2
        ok, reason = t.check("r", "e", 2, event_id=_eid("r","e",2))
        assert ok is True and reason == "ok"


# ---------------------------------------------------------------------------
# Case D: Forward gap (seq 14), then backward arrival (seq 13)
# ---------------------------------------------------------------------------

class TestCaseD_ForwardGapThenBackward:
    def test_gap_detected_on_seq14(self) -> None:
        t = _fresh()
        for s in [10, 11, 12]:
            t.check("r", "e", s, event_id=_eid("r","e",s))
        ok, reason = t.check("r", "e", 14, event_id=_eid("r","e",14))
        assert ok is False and reason == "gap:1"

    def test_seq13_after_14_is_out_of_order(self) -> None:
        t = _fresh()
        for s in [10, 11, 12, 14]:
            t.check("r", "e", s, event_id=_eid("r","e",s))
        ok, reason = t.check("r", "e", 13, event_id=_eid("r","e",13))
        assert ok is False and reason == "out_of_order:1"

    def test_seq13_valid_late_event_registered_not_discarded(self) -> None:
        """Late events are classified, not silently dropped."""
        t = _fresh()
        eid_13 = _eid("r", "e", 13)
        for s in [10, 11, 12, 14]:
            t.check("r", "e", s, event_id=_eid("r","e",s))
        result = t.check("r", "e", 13, event_id=eid_13)
        assert result is not None
        assert result[1].startswith("out_of_order")
        # Resending the same late event must now be 'duplicate'
        ok2, r2 = t.check("r", "e", 13, event_id=eid_13)
        assert ok2 is False and r2 == "duplicate", (
            f"Late event re-sent must be duplicate, got {r2!r}"
        )

    def test_gap_then_fill_sequence(self) -> None:
        """After a gap is reported, the gap entry can still be filled."""
        t = _fresh()
        t.check("r", "e", 1, event_id=_eid("r","e",1))
        t.check("r", "e", 3, event_id=_eid("r","e",3))   # gap:1
        ok, reason = t.check("r", "e", 2, event_id=_eid("r","e",2))
        # seq=2, last=3 → out_of_order:1
        assert ok is False and reason == "out_of_order:1"


# ---------------------------------------------------------------------------
# Case E: Consumer / bridge restart
# ---------------------------------------------------------------------------

class TestCaseE_RestartBehaviour:
    def test_new_tracker_accepts_seq1_as_ok(self) -> None:
        """After restart, fresh tracker: seq=1 from last=0 is ok."""
        t1 = _fresh()
        for s in range(1, 13):
            t1.check("r", "e", s, event_id=_eid("r","e",s))

        t2 = _fresh()
        ok, reason = t2.check("r", "e", 1, event_id=_eid("r","e",1))
        assert ok is True and reason == "ok"

    def test_restart_forgets_seen_event_ids(self) -> None:
        """
        DOCUMENTED LIMITATION: event_id seen-set is in-memory.
        After restart the same event_id is re-accepted.
        """
        t1 = _fresh()
        eid_1 = _eid("r", "e", 1)
        t1.check("r", "e", 1, event_id=eid_1)
        ok1, r1 = t1.check("r", "e", 1, event_id=eid_1)
        assert ok1 is False and r1 == "duplicate"   # correctly detected

        # After restart — same event_id re-accepted (not ideal, documented)
        t2 = _fresh()
        ok2, r2 = t2.check("r", "e", 1, event_id=eid_1)
        assert ok2 is True, (
            "KNOWN LIMITATION: after restart, event_id deduplication history is lost. "
            f"Got ({ok2},{r2})"
        )

    def test_reset_clears_both_counter_and_seen_ids(self) -> None:
        t = _fresh()
        eid_1 = _eid("r", "e", 1)
        t.check("r", "e", 1, event_id=eid_1)
        t.check("r", "e", 2, event_id=_eid("r","e",2))
        t.reset("r", "e")
        ok, reason = t.check("r", "e", 1, event_id=eid_1)
        assert ok is True and reason == "ok", (
            f"After reset, seq=1 must be ok, got ({ok},{reason})"
        )

    def test_duplicate_policy_no_eid_backward_compat(self) -> None:
        """seq==last without event_id is still 'duplicate' (backward compat)."""
        t = _fresh()
        t.check("r", "e", 1)
        ok2, r2 = t.check("r", "e", 1)
        assert ok2 is False and r2 == "duplicate"

    def test_influxdb_idempotency_still_applies(self) -> None:
        """InfluxDB write path uses timestamp as point time → same event = overwrite."""
        consumer_path = (
            Path(__file__).resolve().parents[1]
            / "ingestion" / "src" / "kafka_consumer.py"
        )
        content = consumer_path.read_text(encoding="utf-8")
        assert ".time(" in content


# ---------------------------------------------------------------------------
# Case F: Isolation across different asset_id and run_id
# ---------------------------------------------------------------------------

class TestCaseF_AssetAndRunIsolation:
    def test_different_assets_independent_counters(self) -> None:
        t = _fresh()
        for s in range(1, 6):
            t.check("r", "engine_1", s, event_id=_eid("r","engine_1",s))
        ok, reason = t.check("r", "engine_2", 1, event_id=_eid("r","engine_2",1))
        assert ok is True and reason == "ok"

    def test_different_run_ids_independent(self) -> None:
        t = _fresh()
        for s in range(1, 6):
            t.check("run_A", "engine_1", s, event_id=_eid("run_A","engine_1",s))
        ok, reason = t.check("run_B", "engine_1", 1, event_id=_eid("run_B","engine_1",1))
        assert ok is True and reason == "ok"

    def test_seen_eid_isolated_by_run(self) -> None:
        """event_id from run_A must not block the same event_id in run_B."""
        t = _fresh()
        # Deliver with run_A
        eid = _eid("run_A", "engine_1", 1)
        t.check("run_A", "engine_1", 1, event_id=eid)

        # Using the same string as event_id in run_B is a separate namespace
        # (run_id is part of the key) — it must be accepted
        eid_b = _eid("run_B", "engine_1", 1)   # different hash, different run
        ok, reason = t.check("run_B", "engine_1", 1, event_id=eid_b)
        assert ok is True and reason == "ok"

    def test_seen_eid_isolated_by_asset(self) -> None:
        """event_id from engine_1 must not block engine_2."""
        t = _fresh()
        eid_e1 = _eid("r", "engine_1", 1)
        t.check("r", "engine_1", 1, event_id=eid_e1)

        eid_e2 = _eid("r", "engine_2", 1)
        ok, reason = t.check("r", "engine_2", 1, event_id=eid_e2)
        assert ok is True and reason == "ok"


# ---------------------------------------------------------------------------
# Case G: Bounded LRU eviction
# ---------------------------------------------------------------------------

class TestCaseG_BoundedEviction:
    def test_oldest_event_id_evicted_when_full(self) -> None:
        """When the seen-set is full, the oldest event_id is evicted."""
        t = _fresh(max_eids=3)
        eids = [_eid("r","e",s) for s in range(1,5)]
        for s in range(1, 5):
            t.check("r", "e", s, event_id=eids[s-1])
        # seq=1 event_id was evicted (window size=3, now holds seq 2,3,4)
        ok, reason = t.check("r", "e", 1, event_id=eids[0])
        # seq=1 < last=4 → out_of_order (not duplicate, because eid was evicted)
        assert reason == "out_of_order:3", (
            f"Evicted eid should not be detected as duplicate, got {reason!r}"
        )

    def test_non_evicted_event_id_still_detected(self) -> None:
        """event_ids within the window must still be detected as duplicates."""
        t = _fresh(max_eids=5)
        for s in range(1, 6):
            t.check("r", "e", s, event_id=_eid("r","e",s))
        # seq=3 is within window (eids 1–5 kept)
        ok, reason = t.check("r", "e", 3, event_id=_eid("r","e",3))
        assert ok is False and reason == "duplicate"

    def test_disabled_tracking_zero_max(self) -> None:
        """max_event_ids_per_key=0 disables event-ID tracking."""
        t = _fresh(max_eids=0)
        eid = _eid("r", "e", 1)
        t.check("r", "e", 1, event_id=eid)
        # With tracking disabled, re-delivery is classified by sequence only
        ok, reason = t.check("r", "e", 1, event_id=eid)
        assert ok is False and reason == "duplicate"   # seq==last still works

    def test_large_window_covers_full_fd001_replay(self) -> None:
        """Default window of 2048 covers the C-MAPSS FD001 engine with most cycles."""
        t = _fresh()   # default max_eids=2048
        # FD001 engine with most cycles has 362 cycles — well within window
        for s in range(1, 363):
            t.check("r", "e", s, event_id=_eid("r","e",s))
        # Resend seq=1 (first event, still in window)
        ok, reason = t.check("r", "e", 1, event_id=_eid("r","e",1))
        assert ok is False and reason == "duplicate"


# ---------------------------------------------------------------------------
# Case H: Backward-compatible mode (no event_id supplied)
# ---------------------------------------------------------------------------

class TestCaseH_BackwardCompatibility:
    def test_check_without_event_id_works_as_before(self) -> None:
        """check() without event_id behaves exactly as the original tracker."""
        t = _fresh()
        t.check("r", "e", 1)
        t.check("r", "e", 2)
        ok3, r3 = t.check("r", "e", 3)
        ok1, r1 = t.check("r", "e", 1)   # out_of_order
        ok2, r2 = t.check("r", "e", 2)   # out_of_order
        ok3r, r3r = t.check("r", "e", 3)  # duplicate
        assert ok3 is True  and r3  == "ok"
        assert ok1 is False and r1  == "out_of_order:2"
        assert ok2 is False and r2  == "out_of_order:1"
        assert ok3r is False and r3r == "duplicate"

    def test_next_still_works(self) -> None:
        t = _fresh()
        assert t.next("r", "e") == 1
        assert t.next("r", "e") == 2
        assert t.next("r", "e") == 3

    def test_mixed_calls_with_and_without_eid(self) -> None:
        """Mixing calls with and without event_id on the same key."""
        t = _fresh()
        t.check("r", "e", 1)                        # no eid
        t.check("r", "e", 2, event_id=_eid("r","e",2))  # with eid
        t.check("r", "e", 3)                        # no eid again
        ok, reason = t.check("r", "e", 2, event_id=_eid("r","e",2))
        # seq=2 eid was registered; re-delivery → duplicate
        assert ok is False and reason == "duplicate"
