package peer

import (
	"fmt"
	"sync"
	"time"
)

// TimestampGenerator emits second-based timestamps with an in-second sequence
// suffix used by PC92 frames.
type TimestampGenerator struct {
	lastSec int
	seq     int
	mu      sync.Mutex
}

// timestampGenerator is kept as an alias for existing in-package tests/usages.
type timestampGenerator = TimestampGenerator

// NewTimestampGenerator returns a ready-to-use PC92 timestamp generator.
func NewTimestampGenerator() *TimestampGenerator {
	return &TimestampGenerator{}
}

// Next returns either "<secondsSinceMidnight>" or "<secondsSinceMidnight>.<nn>"
// for additional calls within the same second.
func (g *TimestampGenerator) Next() string {
	now := time.Now().UTC()
	sec := now.Hour()*3600 + now.Minute()*60 + now.Second()
	g.mu.Lock()
	defer g.mu.Unlock()
	if sec != g.lastSec {
		g.lastSec = sec
		g.seq = 0
		return fmt.Sprintf("%d", sec)
	}
	g.seq++
	return fmt.Sprintf("%d.%02d", sec, g.seq)
}
