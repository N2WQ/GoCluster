package peer

import "time"

type backoff struct {
	cur time.Duration
	max time.Duration
}

func newBackoff(base, max time.Duration) *backoff {
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = base
	}
	return &backoff{cur: base, max: max}
}

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

func (b *backoff) Reset() {
	b.cur = 0
}
