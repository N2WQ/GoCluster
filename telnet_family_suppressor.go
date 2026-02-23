package main

import (
	"math"
	"strings"
	"sync"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

type telnetFamilyBucket struct {
	mode    string
	freqBin int
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
	buckets      map[telnetFamilyBucket]map[string]time.Time
	totalEntries int
}

func newTelnetFamilySuppressor(window time.Duration, maxEntries int, familyPolicy spot.CorrectionFamilyPolicy, fallbackHz float64) *telnetFamilySuppressor {
	return &telnetFamilySuppressor{
		window:     window,
		maxEntries: maxEntries,
		family:     familyPolicy,
		fallbackHz: fallbackHz,
		buckets:    make(map[telnetFamilyBucket]map[string]time.Time),
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
	bucket, key, ok := telnetFamilyBucketForSpot(sp, cfg, s.fallbackHz)
	if !ok {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.pruneBucketLocked(bucket, now)
	calls := s.buckets[bucket]
	if calls == nil {
		calls = make(map[string]time.Time, 4)
		s.buckets[bucket] = calls
	}
	if _, exists := calls[key]; exists {
		calls[key] = now
		return false
	}

	suppress := false
	toDelete := make([]string, 0, 2)
	for existing := range calls {
		relation, related := spot.DetectCorrectionFamilyWithPolicy(existing, key, s.family)
		if !related {
			continue
		}
		if relation.MoreSpecific == existing && relation.LessSpecific == key {
			suppress = true
			break
		}
		if relation.MoreSpecific == key && relation.LessSpecific == existing {
			toDelete = append(toDelete, existing)
		}
	}
	if suppress {
		return true
	}

	for _, existing := range toDelete {
		if _, exists := calls[existing]; exists {
			delete(calls, existing)
			if s.totalEntries > 0 {
				s.totalEntries--
			}
		}
	}
	calls[key] = now
	s.totalEntries++
	for s.totalEntries > s.maxEntries {
		if !s.evictOldestLocked() {
			break
		}
	}
	return false
}

func (s *telnetFamilySuppressor) pruneBucketLocked(bucket telnetFamilyBucket, now time.Time) {
	calls := s.buckets[bucket]
	if len(calls) == 0 {
		delete(s.buckets, bucket)
		return
	}
	cutoff := now.Add(-s.window)
	for key, seenAt := range calls {
		if seenAt.Before(cutoff) {
			delete(calls, key)
			if s.totalEntries > 0 {
				s.totalEntries--
			}
		}
	}
	if len(calls) == 0 {
		delete(s.buckets, bucket)
	}
}

func (s *telnetFamilySuppressor) evictOldestLocked() bool {
	var (
		oldestBucket telnetFamilyBucket
		oldestKey    string
		oldestTime   time.Time
		found        bool
	)
	for bucket, calls := range s.buckets {
		for key, seenAt := range calls {
			if !found || seenAt.Before(oldestTime) {
				oldestBucket = bucket
				oldestKey = key
				oldestTime = seenAt
				found = true
			}
		}
	}
	if !found {
		return false
	}
	calls := s.buckets[oldestBucket]
	if _, exists := calls[oldestKey]; exists {
		delete(calls, oldestKey)
		if s.totalEntries > 0 {
			s.totalEntries--
		}
	}
	if len(calls) == 0 {
		delete(s.buckets, oldestBucket)
	}
	return true
}

func telnetFamilyBucketForSpot(sp *spot.Spot, cfg config.CallCorrectionConfig, fallbackHz float64) (telnetFamilyBucket, string, bool) {
	if sp == nil {
		return telnetFamilyBucket{}, "", false
	}
	mode := sp.ModeNorm
	if mode == "" {
		mode = strings.ToUpper(strings.TrimSpace(sp.Mode))
	}
	if !spot.IsCallCorrectionCandidate(mode) {
		return telnetFamilyBucket{}, "", false
	}
	call := sp.DXCallNorm
	if call == "" {
		call = sp.DXCall
	}
	key := spot.CorrectionVoteKey(call)
	if key == "" {
		return telnetFamilyBucket{}, "", false
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
		return telnetFamilyBucket{}, "", false
	}
	freqBin := int(math.Floor(sp.Frequency/widthKHz + 0.5))
	return telnetFamilyBucket{mode: mode, freqBin: freqBin}, key, true
}
