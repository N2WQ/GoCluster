package dedup

import (
	"time"

	"dxcluster/spot"
)

// Shadow cohorts are fixed runtime populations, not a map of incoming labels.
const (
	ShadowHuman = iota
	ShadowCWRTTY
	ShadowFT
	ShadowOther
	ShadowClassCount
)

// ShadowCounters counts paired admission decisions, not sink delivery. Every
// added forward has exactly one direct admission reason. FrequencyAdmissions
// includes all step-4 admissions, including those the baseline also forwards.
type ShadowCounters struct {
	BaselineForwarded    uint64
	BaselineSuppressed   uint64
	AddedNewEntry        uint64
	AddedReportException uint64
	AddedFrequencyChange uint64
	LostForwards         uint64
	FrequencyAdmissions  uint64
	LimitBlocked         uint64
}

// ShadowSnapshot owns its values. Each shard contributes one internally
// coherent transaction; collection never calls a logger while holding a lock.
type ShadowSnapshot struct {
	StartedAt time.Time
	// RunID is immutable and nonzero for an enabled snapshot. It distinguishes
	// engines with equal StartedAt values within this process, not across boots.
	RunID              uint64
	Cohorts            [ShadowClassCount]ShadowCounters
	BaselineCacheSize  int
	CandidateCacheSize int
}

// ShadowClassName names one fixed cohort without retaining incoming labels.
func ShadowClassName(class int) string {
	switch class {
	case ShadowHuman:
		return "human"
	case ShadowCWRTTY:
		return "skimmer-cw-rtty"
	case ShadowFT:
		return "skimmer-ft"
	case ShadowOther:
		return "skimmer-other"
	default:
		return "unknown"
	}
}

func frequencyClass(s *spot.Spot, mode string) int {
	if !spot.IsSkimmerSource(s.SourceType) {
		return ShadowHuman
	}
	if mode == "" {
		_, mode = spot.SecondaryModeIdentity(s.ModeNorm)
	}
	switch mode {
	case "CW", "RTTY":
		return ShadowCWRTTY
	case "FT2", "FT4", "FT8":
		return ShadowFT
	default:
		return ShadowOther
	}
}

func (c *ShadowCounters) observe(base, candidate bool, reason frequencyReason) {
	if base {
		c.BaselineForwarded++
	} else {
		c.BaselineSuppressed++
	}
	switch reason {
	case frequencyAdmission:
		c.FrequencyAdmissions++
	case frequencyLimitBlocked:
		c.LimitBlocked++
	}
	if base && !candidate {
		c.LostForwards++
	} else if !base && candidate {
		switch reason {
		case frequencyNewEntry:
			c.AddedNewEntry++
		case frequencyReportException:
			c.AddedReportException++
		case frequencyAdmission:
			c.AddedFrequencyChange++
		}
	}
}

func (c *ShadowCounters) add(other ShadowCounters) {
	c.BaselineForwarded += other.BaselineForwarded
	c.BaselineSuppressed += other.BaselineSuppressed
	c.AddedNewEntry += other.AddedNewEntry
	c.AddedReportException += other.AddedReportException
	c.AddedFrequencyChange += other.AddedFrequencyChange
	c.LostForwards += other.LostForwards
	c.FrequencyAdmissions += other.FrequencyAdmissions
	c.LimitBlocked += other.LimitBlocked
}

// FrequencyChangeState returns off for the historical facade, including nil.
func (d *SecondaryDeduper) FrequencyChangeState() string {
	if d == nil || d.changes == nil {
		return "off"
	}
	return d.changes.options.State
}

// ShadowStats reports only a shadow comparison. Baseline GetStats retains its
// historical output-driving meaning; these additional cardinalities expose the
// second cache rather than hiding its retained state in that old statistic.
func (d *SecondaryDeduper) ShadowStats() (ShadowSnapshot, bool) {
	if d == nil || d.changes == nil || d.changes.options.State != "shadow" {
		return ShadowSnapshot{}, false
	}
	result := ShadowSnapshot{StartedAt: d.changes.startedAt, RunID: d.changes.runID}
	for i := range d.shards {
		shard := &d.shards[i]
		shard.mu.Lock()
		cs := &d.changes.shards[i]
		result.BaselineCacheSize += len(shard.cache)
		result.CandidateCacheSize += len(cs.cache)
		for class := range result.Cohorts {
			result.Cohorts[class].add(cs.cohorts[class])
		}
		shard.mu.Unlock()
	}
	return result, true
}

func (d *SecondaryDeduper) changeStats() (processed, duplicates uint64, size int) {
	for i := range d.shards {
		shard := &d.shards[i]
		shard.mu.Lock()
		cs := &d.changes.shards[i]
		processed += cs.processed
		duplicates += cs.duplicates
		size += len(cs.cache)
		shard.mu.Unlock()
	}
	return processed, duplicates, size
}

func (d *SecondaryDeduper) startChanges() {
	f := d.changes
	f.lifeMu.Lock()
	defer f.lifeMu.Unlock()
	if f.started || f.stopped {
		return
	}
	f.started = true
	go func() {
		defer close(f.done)
		d.cleanupLoop()
	}()
}

func (d *SecondaryDeduper) stopChanges() {
	f := d.changes
	f.lifeMu.Lock()
	if !f.stopped {
		f.stopped = true
		close(d.shutdown)
		if !f.started {
			close(f.done)
		}
	}
	f.lifeMu.Unlock()
	<-f.done
}

func (d *SecondaryDeduper) cleanupChangesAt(now time.Time) {
	if d.window <= 0 {
		return
	}
	for i := range d.shards {
		select {
		case <-d.shutdown:
			return
		default:
		}
		shard := &d.shards[i]
		shard.mu.Lock()
		removed := false
		for key, entry := range shard.cache {
			if now.Sub(entry.when) > d.window {
				delete(shard.cache, key)
				removed = true
			}
		}
		if removed {
			d.maybeCompactShardLocked(shard)
		}
		cs := &d.changes.shards[i]
		removed = false
		for key, entry := range cs.cache {
			if now.Sub(entry.when) > d.window {
				delete(cs.cache, key)
				removed = true
			}
		}
		if removed {
			cs.compact()
		}
		shard.mu.Unlock()
	}
}

func (s *frequencyShard) updatePeak() {
	if len(s.cache) > s.peak {
		s.peak = len(s.cache)
	}
}

func (s *frequencyShard) compact() {
	if s.peak < secondaryCompactMinPeak || len(s.cache) >= int(float64(s.peak)*secondaryCompactShrinkRatio) {
		return
	}
	next := make(map[[32]byte]frequencyEntry, len(s.cache))
	for key, entry := range s.cache {
		next[key] = entry
	}
	s.cache = next
	s.peak = len(next)
}
