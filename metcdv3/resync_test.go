package metcdv3

import (
	"testing"
	"time"
)

func TestResyncBackoffExponentialAndCap(t *testing.T) {
	var b resyncBackoff

	// Rapid resyncs (watch never ran long) should grow exponentially from the
	// minimum and never exceed the maximum.
	prev := time.Duration(0)
	for i := 0; i < 20; i++ {
		delay, _ := b.next(0)
		if delay < resyncBackoffMin {
			t.Fatalf("attempt %d: delay %s below min %s", i, delay, resyncBackoffMin)
		}
		if delay > resyncBackoffMax {
			t.Fatalf("attempt %d: delay %s above max %s", i, delay, resyncBackoffMax)
		}
		if i > 0 && delay < prev {
			t.Fatalf("attempt %d: delay %s decreased from %s without a reset", i, delay, prev)
		}
		prev = delay
	}
	if prev != resyncBackoffMax {
		t.Fatalf("expected backoff to reach cap %s, got %s", resyncBackoffMax, prev)
	}
}

func TestResyncBackoffEscalates(t *testing.T) {
	var b resyncBackoff
	for i := 1; i < resyncEscalateAfter; i++ {
		if _, escalated := b.next(0); escalated {
			t.Fatalf("escalated early at consecutive=%d", b.consecutive)
		}
	}
	if _, escalated := b.next(0); !escalated {
		t.Fatalf("expected escalation at consecutive=%d (threshold %d)", b.consecutive, resyncEscalateAfter)
	}
}

func TestResyncBackoffResetsAfterHealthyWatch(t *testing.T) {
	var b resyncBackoff

	// Drive the backoff up and into escalation.
	for i := 0; i < resyncEscalateAfter+3; i++ {
		b.next(0)
	}
	if b.cur <= resyncBackoffMin {
		t.Fatalf("precondition: expected elevated backoff, got %s", b.cur)
	}

	// A watch that ran longer than the healthy threshold is an isolated
	// compaction: backoff resets to the minimum and escalation clears.
	delay, escalated := b.next(resyncHealthyThreshold)
	if delay != resyncBackoffMin {
		t.Fatalf("expected reset to min %s, got %s", resyncBackoffMin, delay)
	}
	if escalated {
		t.Fatal("expected escalation to clear after a healthy watch")
	}
	if b.consecutive != 1 {
		t.Fatalf("expected consecutive=1 after reset, got %d", b.consecutive)
	}
}
