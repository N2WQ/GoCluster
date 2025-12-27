package peer

import "time"

type backoff struct {
	cur time.Duration
	max time.Duration
}

// Purpose: Construct an exponential backoff timer.
// Key aspects: Normalizes base/max and starts at base delay.
// Upstream: Peer session reconnection logic.
// Downstream: backoff.Next/Reset.
func newBackoff(base, max time.Duration) *backoff {
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = base
	}
	return &backoff{cur: base, max: max}
}

// Purpose: Return the next backoff delay and advance the window.
// Key aspects: Doubles up to the max cap.
// Upstream: Peer reconnect loops.
// Downstream: None.
func (b *backoff) Next() time.Duration {
	if b.cur >= b.max {
		return b.max
	}
	d := b.cur
	b.cur *= 2
	if b.cur > b.max {
		b.cur = b.max
	}
	return d
}

// Purpose: Reset backoff to its initial state.
// Key aspects: Sets cur to zero so Next restarts at base.
// Upstream: Successful reconnect paths.
// Downstream: None.
func (b *backoff) Reset() {
	b.cur = 0
}
