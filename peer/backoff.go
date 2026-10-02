package peer

import "time"

type backoff struct {
	base time.Duration
	cur  time.Duration
	max  time.Duration
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
	return &backoff{base: base, cur: base, max: max}
}

// Next returns the next backoff delay and advances the window.
// Key aspects: Doubles up to the max cap.
// Upstream: Peer reconnect loops.
// Downstream: None.
func (b *backoff) Next() time.Duration {
	if b.cur >= b.max {
		return b.max
	}
	d := b.cur
	if b.cur > b.max-b.cur {
		b.cur = b.max
	} else {
		b.cur *= 2
	}
	return d
}

// Reset resets backoff to its initial state.
// Key aspects: Restores the normalized base; a successful connection never
// turns subsequent failures into a zero-delay reconnect loop.
// Upstream: Successful reconnect paths.
// Downstream: None.
func (b *backoff) Reset() {
	b.cur = b.base
}
