package peer

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrTimestampRate means all 100 values in this UTC second were issued.
	// The owner must coalesce or retry after the next second, never invent time.
	ErrTimestampRate = errors.New("PC9x timestamp rate exhausted")
	// ErrTimestampClock gates publication until UTC advances past retained state.
	ErrTimestampClock = errors.New("PC9x clock is unsafe")
)

// TimestampGenerator owns one origin's sequence across all its sessions. The
// manager must serialize allocation with enqueueing: this mutex alone cannot
// order writes to multiple recipient queues. State is constant-size and retained
// through disconnect/reconnect; full UTC seconds distinguish a legitimate daily
// wrap from a backwards clock change.
type TimestampGenerator struct {
	mu          sync.Mutex
	lastUnix    int64
	seq         int
	initialized bool
	unsafe      bool
}

func NewTimestampGenerator() *TimestampGenerator { return &TimestampGenerator{} }

// Next allocates without sleeping. Callers must handle rate exhaustion and
// unsafe time explicitly rather than publish an empty or fabricated timestamp.
func (g *TimestampGenerator) Next() (string, error) { return g.NextAt(time.Now()) }

// NextAt emits the integer second, then .01 through .99. In particular the 101st
// allocation never emits .100, whose numerical value would regress to .10.
func (g *TimestampGenerator) NextAt(now time.Time) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.clockSafe(now); err != nil {
		return "", err
	}
	second := now.Unix()
	if !g.initialized || second != g.lastUnix {
		g.lastUnix, g.seq, g.initialized = second, 0, true
	} else {
		if g.seq >= 99 {
			return "", ErrTimestampRate
		}
		g.seq++
	}
	utc := now.UTC()
	daySecond := utc.Hour()*3600 + utc.Minute()*60 + utc.Second()
	if g.seq == 0 {
		return fmt.Sprintf("%d", daySecond), nil
	}
	return fmt.Sprintf("%d.%02d", daySecond, g.seq), nil
}

// ClockSafe checks the recovery condition without consuming a wire value.
// Following a regression, the old second remains blocked even after catching
// up: publication resumes only when UTC advances beyond it. The manager owns
// the additional one-second stable-health interval and peer recovery sequence.
func (g *TimestampGenerator) ClockSafe(now time.Time) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.clockSafe(now)
}

// RemainingAt does not issue a value. The sole publication owner uses it to
// reserve a complete ordered pair and keep timestamp capacity for membership.
// No other production caller may allocate between the check and its enqueue.
func (g *TimestampGenerator) RemainingAt(now time.Time) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.clockSafe(now); err != nil {
		return 0, err
	}
	if !g.initialized || now.Unix() != g.lastUnix {
		return 100, nil
	}
	return 99 - g.seq, nil
}

func (g *TimestampGenerator) clockSafe(now time.Time) error {
	if now.IsZero() || now.Year() < 1970 || now.Year() > 9999 ||
		(g.initialized && now.Unix() < g.lastUnix) {
		g.unsafe = true
		return ErrTimestampClock
	}
	if g.unsafe && g.initialized && now.Unix() <= g.lastUnix {
		return ErrTimestampClock
	}
	g.unsafe = false
	return nil
}
