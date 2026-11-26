// File role: Owns the shared pause authority for manual controls, generic
// automatic pauses, and human readbacks that finish on the writer goroutine.
// Only fixed state lives on Client. Spot admission remains an atomic read path;
// replacement invalidation is ordered with registry ownership and performs no I/O.
package telnet

import "time"

// readbackCompletion travels with one complete control message. The writer
// applies it only after that message's batch has been written and flushed.
// Epochs let later commands supersede a completion without retaining callbacks.
type readbackCompletion struct {
	epoch    uint64
	duration time.Duration
}

// A JSON release advances the server's generation at the command boundary.
// On coarse clocks a spot can share the release's timestamp even after the
// client received RESUME. Every fanout copy carries its original generation.
type jsonPauseFence struct {
	wallNS     int64
	generation uint64
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
	if c.readPauseClosed || c.jsonRequested.Load() {
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
	c.invalidateHumanReadbackLocked()
}

// Caller holds readPauseMu; registration takes this lock before deciding
// whether the replacement still has time to become the owner.
func (c *Client) invalidateHumanReadbackLocked() {
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
	c.manualPauseUntil = until
	c.jsonPauseCutoff.Store(nil) // Natural expiry has only its time boundary.
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
	if c.readPauseClosed || c.jsonRequested.Load() {
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

// Entering JSON cancels every automatic hold, including an unflushed readback.
// Keep the cutoff from a past pause. For a canceled active hold, queued spots
// from that hold remain stale but the old automatic deadline cannot discard
// fresh arrivals. The manual deadline survives any subsequent automatic extension.
func (c *Client) enterJSONPausePolicy(now time.Time) {
	c.readPauseMu.Lock()
	defer c.readPauseMu.Unlock()
	nowNS := now.UnixNano()
	until := c.manualPauseUntil
	if until <= nowNS {
		until = 0
	}
	cutoff := c.readPauseDiscardBefore.Load()
	c.jsonPauseCutoff.Store(nil)
	if c.readPausePending.Load() || (cutoff > 0 && cutoff >= nowNS) {
		cutoff = max(nowNS, until)
		if until == 0 {
			c.setJSONPauseFence(nowNS)
		}
	}
	c.readPauseEpoch++
	c.readPauseDiscardBefore.Store(cutoff)
	c.readPauseUntilUnixNano.Store(until)
	c.readPausePending.Store(false)
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
	c.manualPauseUntil = 0
	if c.jsonRequested.Load() {
		c.setJSONPauseFence(nowNanos)
	}
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
	enqueued := now
	if env != nil && !env.enqueueAt.IsZero() {
		enqueued = env.enqueueAt
	}
	ns := enqueued.UnixNano()
	if ns < cutoff || (ns == cutoff && !c.afterJSONPauseFence(env, cutoff)) {
		c.readPauseSuppressed.Add(1)
		return true
	}
	return false
}

func (c *Client) setJSONPauseFence(wallNS int64) {
	if c.server != nil {
		c.jsonPauseCutoff.Store(&jsonPauseFence{wallNS: wallNS, generation: c.server.jsonPauseGeneration.Add(1)})
	}
}

// Called only on equal wall timestamps. A worker that processes an old queue
// entry after a release cannot stamp it as fresh. Legacy ties stay suppressed.
func (c *Client) afterJSONPauseFence(env *spotEnvelope, cutoff int64) bool {
	if env == nil || !c.jsonRequested.Load() {
		return false
	}
	fence := c.jsonPauseCutoff.Load()
	var generation uint64
	if env.publication != nil {
		generation = env.publication.pauseGeneration
	}
	return fence != nil && fence.wallNS == cutoff && generation >= fence.generation
}
