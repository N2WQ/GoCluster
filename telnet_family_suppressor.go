package main

import (
	"math"
	"sync"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
	"dxcluster/strutil"
)

type telnetFamilyBucket struct {
	mode    string
	freqBin int
}

type telnetFamilyEntry struct {
	bucket  telnetFamilyBucket
	key     string
	freqKHz float64
	seenAt  time.Time
	prev    *telnetFamilyEntry
	next    *telnetFamilyEntry
}

// telnetFamilySuppressor tracks recently emitted calls in small mode/frequency
// buckets so less-specific family variants can be suppressed for telnet output.
// This is output-only: archive/peer behavior is unchanged.
type telnetFamilySuppressor struct {
	window     time.Duration
	maxEntries int
	family     spot.CorrectionFamilyPolicy
	fallbackHz float64

	mu           sync.Mutex
	buckets      map[telnetFamilyBucket]map[string]*telnetFamilyEntry
	head         *telnetFamilyEntry
	tail         *telnetFamilyEntry
	totalEntries int
	lastNow      time.Time
}

func newTelnetFamilySuppressor(window time.Duration, maxEntries int, familyPolicy spot.CorrectionFamilyPolicy, fallbackHz float64) *telnetFamilySuppressor {
	if maxEntries <= 0 {
		maxEntries = 1
	}
	if window <= 0 {
		window = time.Second
	}
	return &telnetFamilySuppressor{
		window:     window,
		maxEntries: maxEntries,
		family:     familyPolicy,
		fallbackHz: fallbackHz,
		buckets:    make(map[telnetFamilyBucket]map[string]*telnetFamilyEntry),
	}
}

// ShouldSuppress returns true when the spot call is less specific than a recent
// call in the same family bucket and should be hidden from telnet output.
func (s *telnetFamilySuppressor) ShouldSuppress(sp *spot.Spot, cfg config.CallCorrectionConfig, now time.Time) bool {
	if s == nil || sp == nil {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	bucket, key, toleranceKHz, ok := telnetFamilyBucketForSpot(sp, cfg, s.fallbackHz)
	if !ok {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now = s.monotonicNowLocked(now)
	s.pruneExpiredLocked(now)
	if calls := s.buckets[bucket]; calls != nil {
		if entry, exists := calls[key]; exists {
			s.touchEntryLocked(entry, now)
			return false
		}
	}

	suppress := false
	minBin := bucket.freqBin - 1
	maxBin := bucket.freqBin + 1
	for bin := minBin; bin <= maxBin; bin++ {
		binBucket := telnetFamilyBucket{mode: bucket.mode, freqBin: bin}
		calls := s.buckets[binBucket]
		if calls == nil {
			continue
		}
		for existingKey, existingEntry := range calls {
			if math.Abs(existingEntry.freqKHz-sp.Frequency) > toleranceKHz {
				continue
			}
			relation, related := spot.DetectCorrectionFamilyWithPolicy(existingKey, key, s.family)
			if !related {
				continue
			}
			if relation.MoreSpecific == existingKey && relation.LessSpecific == key {
				suppress = true
				break
			}
			if relation.MoreSpecific == key && relation.LessSpecific == existingKey {
				s.removeEntryLocked(existingEntry)
			}
		}
		if suppress {
			break
		}
	}
	if suppress {
		return true
	}

	s.addEntryLocked(bucket, key, sp.Frequency, now)
	for s.totalEntries > s.maxEntries {
		if !s.evictHeadLocked() {
			break
		}
	}
	return false
}

func (s *telnetFamilySuppressor) monotonicNowLocked(now time.Time) time.Time {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if !s.lastNow.IsZero() && now.Before(s.lastNow) {
		return s.lastNow
	}
	s.lastNow = now
	return now
}

func (s *telnetFamilySuppressor) touchEntryLocked(entry *telnetFamilyEntry, now time.Time) {
	if entry == nil {
		return
	}
	entry.seenAt = now
	s.moveToTailLocked(entry)
}

func (s *telnetFamilySuppressor) addEntryLocked(bucket telnetFamilyBucket, key string, freqKHz float64, now time.Time) {
	calls := s.buckets[bucket]
	if calls == nil {
		calls = make(map[string]*telnetFamilyEntry, 4)
		s.buckets[bucket] = calls
	}
	entry := &telnetFamilyEntry{
		bucket:  bucket,
		key:     key,
		freqKHz: freqKHz,
		seenAt:  now,
	}
	calls[key] = entry
	s.appendTailLocked(entry)
	s.totalEntries++
}

func (s *telnetFamilySuppressor) pruneExpiredLocked(now time.Time) {
	cutoff := now.Add(-s.window)
	for s.head != nil && s.head.seenAt.Before(cutoff) {
		s.removeEntryLocked(s.head)
	}
}

func (s *telnetFamilySuppressor) evictHeadLocked() bool {
	if s.head == nil {
		return false
	}
	s.removeEntryLocked(s.head)
	return true
}

func (s *telnetFamilySuppressor) removeEntryLocked(entry *telnetFamilyEntry) {
	if entry == nil {
		return
	}
	if calls := s.buckets[entry.bucket]; calls != nil {
		if current, exists := calls[entry.key]; exists && current == entry {
			delete(calls, entry.key)
			if s.totalEntries > 0 {
				s.totalEntries--
			}
			if len(calls) == 0 {
				delete(s.buckets, entry.bucket)
			}
		}
	}
	s.detachLocked(entry)
}

func (s *telnetFamilySuppressor) appendTailLocked(entry *telnetFamilyEntry) {
	if entry == nil {
		return
	}
	if s.tail == nil {
		s.head = entry
		s.tail = entry
		return
	}
	entry.prev = s.tail
	entry.next = nil
	s.tail.next = entry
	s.tail = entry
}

func (s *telnetFamilySuppressor) moveToTailLocked(entry *telnetFamilyEntry) {
	if entry == nil || s.tail == entry {
		return
	}
	s.detachLocked(entry)
	s.appendTailLocked(entry)
}

func (s *telnetFamilySuppressor) detachLocked(entry *telnetFamilyEntry) {
	if entry == nil {
		return
	}
	if entry.prev == nil && entry.next == nil && s.head != entry && s.tail != entry {
		return
	}
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		s.head = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		s.tail = entry.prev
	}
	entry.prev = nil
	entry.next = nil
}

func telnetFamilyBucketForSpot(sp *spot.Spot, cfg config.CallCorrectionConfig, fallbackHz float64) (telnetFamilyBucket, string, float64, bool) {
	if sp == nil {
		return telnetFamilyBucket{}, "", 0, false
	}
	mode := sp.ModeNorm
	if mode == "" {
		mode = strutil.NormalizeUpper(sp.Mode)
	}
	if !spot.IsCallCorrectionCandidate(mode) {
		return telnetFamilyBucket{}, "", 0, false
	}
	call := sp.DXCallNorm
	if call == "" {
		call = sp.DXCall
	}
	key := spot.CorrectionVoteKey(call)
	if key == "" {
		return telnetFamilyBucket{}, "", 0, false
	}
	toleranceHz := cfg.FrequencyToleranceHz
	if mode == "USB" || mode == "LSB" {
		toleranceHz = cfg.VoiceFrequencyToleranceHz
	}
	if toleranceHz <= 0 {
		toleranceHz = fallbackHz
	}
	widthKHz := toleranceHz / 1000.0
	if widthKHz <= 0 {
		return telnetFamilyBucket{}, "", 0, false
	}
	freqBin := int(math.Floor(sp.Frequency/widthKHz + 0.5))
	return telnetFamilyBucket{mode: mode, freqBin: freqBin}, key, widthKHz, true
}
