// File role: Owns the shared pause authority for manual controls, generic
// automatic pauses, and human readbacks that finish on the writer goroutine.
// Only fixed state lives on Client. Spot admission remains an atomic read path;
// pause mutation never holds a client, registry, or writer lock or performs I/O.
package telnet

import "time"

// readbackCompletion travels with one complete control message. The writer
// applies it only after that message's batch has been written and flushed.
// Epochs let later commands supersede a completion without retaining callbacks.
type readbackCompletion struct {
	epoch    uint64
	duration time.Duration
}

// beginHumanReadback suppresses spots before capture or output preparation.
// A pending hold has no invented finite deadline: delivery can take longer than
// the reading interval, and its finite cutoff must not discard future traffic.
func (c *Client) beginHumanReadback(now time.Time, duration time.Duration) readbackCompletion {
	if c == nil {
		return readbackCompletion{}
	}
	if duration <= 0 {
		duration = defaultReadPauseDuration
	}
	c.readPauseMu.Lock()
	defer c.readPauseMu.Unlock()
	if c.readPauseClosed {
		return readbackCompletion{}
	}
	if !c.readPausePending.Load() && c.readPauseUntilUnixNano.Load() <= now.UnixNano() {
		c.readPauseSuppressed.Store(0)
	}
	c.readPauseEpoch++
	c.readPausePending.Store(true)
	return readbackCompletion{epoch: c.readPauseEpoch, duration: duration}
}

// completeHumanReadback starts the full interval from successful delivery.
// Publish the finite deadline/cutoff before releasing the pending hold, so the
// atomic spot path cannot see an unpaused gap. Pending suppression counts carry
// forward even if a previous finite deadline expired during delivery.
func (c *Client) completeHumanReadback(completion readbackCompletion, now time.Time) {
	if c == nil || completion.epoch == 0 || completion.duration <= 0 {
		return
	}
	c.readPauseMu.Lock()
	defer c.readPauseMu.Unlock()
	if c.readPauseClosed || !c.readPausePending.Load() || completion.epoch != c.readPauseEpoch {
		return
	}
	until := max(c.readPauseUntilUnixNano.Load(), now.Add(completion.duration).UnixNano())
	c.readPauseUntilUnixNano.Store(until)
	c.readPauseDiscardBefore.Store(until)
	c.readPausePending.Store(false)
}

// invalidateHumanReadback retires completion authority on close/replacement.
// Existing counters and finite pause state remain available for diagnostics;
// a delayed successful flush cannot reopen the retired client's pending hold.
func (c *Client) invalidateHumanReadback() {
	if c == nil {
		return
	}
	c.readPauseMu.Lock()
	defer c.readPauseMu.Unlock()
	if c.readPauseClosed {
		return
	}
	c.readPauseClosed = true
	c.readPauseEpoch++
	c.readPausePending.Store(false)
}

// startReadPause is a valid manual PAUSE. It replaces the finite deadline and
// supersedes pending human completions, including when the requested interval
// is shorter. Invalid command arguments never reach this method.
func (c *Client) startReadPause(now time.Time, duration time.Duration) {
	if c == nil || duration <= 0 {
		return
	}
	c.readPauseMu.Lock()
	defer c.readPauseMu.Unlock()
	if c.readPauseClosed {
		return
	}
	if !c.readPausePending.Load() && c.readPauseUntilUnixNano.Load() <= now.UnixNano() {
		c.readPauseSuppressed.Store(0)
	}
	c.readPauseEpoch++
	until := now.Add(duration).UnixNano()
	c.readPauseUntilUnixNano.Store(until)
	c.readPauseDiscardBefore.Store(until)
	c.readPausePending.Store(false)
}

// extendReadPause is the generic automatic trigger. Its max calculation shares
// mutation authority with writer completion and manual controls; it neither
// cancels a pending hold nor lets a concurrent update shorten the finite pause.
// The server retains the generic trigger's configured zero-disable policy.
func (c *Client) extendReadPause(now time.Time, duration time.Duration) time.Duration {
	if c == nil || duration <= 0 {
		return 0
	}
	c.readPauseMu.Lock()
	defer c.readPauseMu.Unlock()
	if c.readPauseClosed {
		return 0
	}
	nowNanos := now.UnixNano()
	previous := c.readPauseUntilUnixNano.Load()
	if !c.readPausePending.Load() && previous <= nowNanos {
		c.readPauseSuppressed.Store(0)
	}
	until := max(previous, now.Add(duration).UnixNano())
	c.readPauseUntilUnixNano.Store(until)
	c.readPauseDiscardBefore.Store(until)
	return time.Duration(until - nowNanos)
}

// A pending response is active even when it has no finite remaining interval.
// SHOW HOLD and readback status use this coherent snapshot; spot delivery uses
// the separate atomic check below and never waits for the mutation mutex.
func (c *Client) readPauseStatus(now time.Time) (active bool, remaining time.Duration, suppressed uint64) {
	if c == nil {
		return false, 0, 0
	}
	c.readPauseMu.Lock()
	defer c.readPauseMu.Unlock()
	until := c.readPauseUntilUnixNano.Load()
	if until > now.UnixNano() {
		remaining = time.Duration(until - now.UnixNano())
	}
	return c.readPausePending.Load() || remaining > 0, remaining, c.readPauseSuppressed.Load()
}

// RESUME wins over any earlier queued completion. Setting the cutoff before
// clearing active state permits fresh traffic while discarding stale envelopes.
func (c *Client) resumeReadPause(now time.Time) (bool, uint64) {
	if c == nil {
		return false, 0
	}
	c.readPauseMu.Lock()
	defer c.readPauseMu.Unlock()
	if c.readPauseClosed {
		return false, c.readPauseSuppressed.Load()
	}
	nowNanos := now.UnixNano()
	active := c.readPausePending.Load() || c.readPauseUntilUnixNano.Load() > nowNanos
	c.readPauseEpoch++
	c.readPauseDiscardBefore.Store(nowNanos)
	c.readPauseUntilUnixNano.Store(0)
	c.readPausePending.Store(false)
	return active, c.readPauseSuppressed.Swap(0)
}

// Spot suppression stays allocation-free and never takes the mutation mutex.
// These checks cannot recall a spot already prepared for a socket write, as
// specified by ADR-0243. Suppressed envelopes are counted, never replayed, and
// remain separate from slow-client drops.
func (c *Client) suppressSpotForReadPause(env *spotEnvelope, now time.Time) bool {
	if c == nil {
		return false
	}
	nowNanos := now.UnixNano()
	if c.readPausePending.Load() || c.readPauseUntilUnixNano.Load() > nowNanos {
		c.readPauseSuppressed.Add(1)
		return true
	}
	cutoff := c.readPauseDiscardBefore.Load()
	if cutoff <= 0 {
		return false
	}
	enqueueNanos := nowNanos
	if env != nil && !env.enqueueAt.IsZero() {
		enqueueNanos = env.enqueueAt.UnixNano()
	}
	if enqueueNanos <= cutoff {
		c.readPauseSuppressed.Add(1)
		return true
	}
	return false
}
