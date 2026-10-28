// File role: Paces completed command attempts within one Telnet session.
// The read goroutine owns the bucket; no queued work or goroutine is retained.
package telnet

import (
	"fmt"
	"time"
)

const (
	maxCommandPacingValue = 1_000_000_000
	commandCreditPerToken = int64(time.Second)
)

// commandPacer represents fractional tokens as integer nanocommands. Values
// are bounded so capacity and every unsaturated refill fit in int64.
type commandPacer struct {
	rate     int64
	capacity int64
	credit   int64
	last     time.Time
}

func validateCommandPacingOptions(rate, burst int) error {
	if rate < 0 || rate > maxCommandPacingValue {
		return fmt.Errorf("command rate must be between 0 and %d", maxCommandPacingValue)
	}
	if burst < 0 || burst > maxCommandPacingValue || (rate > 0 && burst == 0) {
		return fmt.Errorf("command burst must be between 0 and %d, and positive when pacing is enabled", maxCommandPacingValue)
	}
	return nil
}

func newCommandPacer(rate, burst int, now time.Time) *commandPacer {
	if rate == 0 {
		return nil
	}
	capacity := int64(burst) * commandCreditPerToken
	return &commandPacer{rate: int64(rate), capacity: capacity, credit: capacity, last: now}
}

// take consumes one credit only when available. Otherwise it returns a
// conservatively rounded delay; callers recheck actual elapsed time on wake.
func (p *commandPacer) take(now time.Time) time.Duration {
	if now.After(p.last) {
		elapsed := int64(now.Sub(p.last))
		missing := p.capacity - p.credit
		if missing > 0 {
			// Saturate before multiplying so even long idle periods cannot overflow.
			untilFull := (missing + p.rate - 1) / p.rate
			if elapsed >= untilFull {
				p.credit = p.capacity
			} else {
				p.credit += elapsed * p.rate
			}
		}
		// Discard idle time beyond capacity rather than banking phantom credit.
		p.last = now
	}
	if p.credit >= commandCreditPerToken {
		p.credit -= commandCreditPerToken
		return 0
	}
	deficit := commandCreditPerToken - p.credit
	return time.Duration((deficit + p.rate - 1) / p.rate)
}

func (s *Server) commandPacingNow() time.Time {
	if s.testCommandPacingNow != nil {
		return s.testCommandPacingNow()
	}
	return time.Now()
}

// awaitCommandAllowance is owned by the session reader. It keeps only one
// completed line awaiting dispatch and one on-demand limiter timer.
func (s *Server) awaitCommandAllowance(c *Client, p *commandPacer) bool {
	if p == nil {
		return true
	}
	if commandSessionClosed(c, s) {
		return false
	}
	delay := p.take(s.commandPacingNow())
	if delay == 0 {
		return !commandSessionClosed(c, s)
	}
	s.metrics.commandPacingWaits.Add(1)
	if s.testCommandPacingWait != nil {
		s.testCommandPacingWait(c)
	}
	if commandSessionClosed(c, s) {
		return false
	}
	// The observer is test-only, but rechecking also avoids counting time
	// between the first calculation and timer creation as additional delay.
	delay = p.take(s.commandPacingNow())
	if delay == 0 {
		return !commandSessionClosed(c, s)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	if s.testCommandPacingTimerStarted != nil {
		s.testCommandPacingTimerStarted(c)
	}
	for {
		select {
		case <-c.done:
			return false
		case <-s.shutdown:
			return false
		case <-timer.C:
		}
		if commandSessionClosed(c, s) {
			return false
		}
		delay = p.take(s.commandPacingNow())
		if delay == 0 {
			return !commandSessionClosed(c, s)
		}
		timer.Reset(delay)
	}
}

func commandSessionClosed(c *Client, s *Server) bool {
	select {
	case <-c.done:
		return true
	case <-s.shutdown:
		return true
	default:
		return false
	}
}
