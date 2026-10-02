package peer

import (
	"sync/atomic"
	"testing"
	"time"
)

// Keep the elapsed-time fixture for inherited authority checks. V14 no longer
// installs mailbox/cache headroom intervals or retains refused wire records.
func recoveryV12Owner(t *testing.T) (*protocolController, *session, *session, func(time.Duration), time.Time) {
	t.Helper()
	p, source, alternate, base := controllerTestOwner(t)
	var clock atomic.Int64
	clock.Store(base.UnixNano())
	p.elapsedNow = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	p.wallNow = p.elapsedNow
	return p, source, alternate, func(offset time.Duration) { clock.Store(base.Add(offset).UnixNano()) }, base
}

func TestPC92V14RecoveryCacheChronology(t *testing.T) {
	base := time.Unix(2000000000, 0)
	c := newBoundedDedupe(600*time.Second, 1, 64)
	if c.admitAt("first", base, base.Add(time.Second)) != dedupeAccepted {
		t.Fatal("setup")
	}
	if c.admitAt("first", base.Add(599*time.Second), base.Add(599*time.Second)) != dedupeDuplicate {
		t.Fatal("duplicate changed original admission")
	}
	if admitted, ok := c.firstAdmission("first", base.Add(600*time.Second)); !ok || !admitted.Equal(base) {
		t.Fatal("duplicate renewed age or exact 600 seconds expired")
	}
	c.prune(base.Add(600*time.Second + time.Nanosecond))
	if count, bytes, _ := c.occupancy(); count != 0 || bytes != 0 {
		t.Fatal("strictly expired entry retained key backing")
	}
	refill := base.Add(601 * time.Second)
	if c.admitAt("second", refill, refill) != dedupeAccepted {
		t.Fatal("expired capacity did not admit replacement")
	}
	if c.admitAt("third", refill, refill) != dedupeFull {
		t.Fatal("unexpired entry evicted under contention")
	}
	if admitted, ok := c.firstAdmission("second", refill.Add(599*time.Second)); !ok || !admitted.Equal(refill) {
		t.Fatal("capacity refusal changed original age")
	}
}
