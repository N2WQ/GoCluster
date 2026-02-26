package main

import (
	"math"
	"strings"
	"sync"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/strutil"
)

type telnetFamilyBucket struct {
	mode    string
	freqBin int
}

type telnetFamilyEntry struct {
	bucket    telnetFamilyBucket
	key       string
	freqKHz   float64
	seenAt    time.Time
	support   int
	contested bool
	prev      *telnetFamilyEntry
	next      *telnetFamilyEntry
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
	return s.ShouldSuppressWithResolver(sp, cfg, now, spot.ResolverSnapshot{}, false)
}

// ShouldSuppressWithResolver returns true when the spot should be hidden from
// telnet output due to family precedence or resolver-contested edit-neighbor
// suppression policy.
func (s *telnetFamilySuppressor) ShouldSuppressWithResolver(sp *spot.Spot, cfg config.CallCorrectionConfig, now time.Time, resolverSnapshot spot.ResolverSnapshot, resolverSnapshotOK bool) bool {
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
	incomingSupport := 0
	incomingContested := false
	if resolverSnapshotOK {
		incomingSupport = correctionflow.ResolverSupportForCall(resolverSnapshot, key)
		incomingContested = resolverSnapshot.State == spot.ResolverStateSplit || resolverSnapshot.State == spot.ResolverStateUncertain
		if !incomingContested && incomingSupport > 0 {
			incomingContested = hasComparableEditNeighborCandidate(resolverSnapshot, key, bucket.mode, cfg.DistanceModelCW, cfg.DistanceModelRTTY)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now = s.monotonicNowLocked(now)
	s.pruneExpiredLocked(now)
	if calls := s.buckets[bucket]; calls != nil {
		if entry, exists := calls[key]; exists {
			entry.support = incomingSupport
			entry.contested = incomingContested
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
			if related {
				if relation.MoreSpecific == existingKey && relation.LessSpecific == key {
					suppress = true
					break
				}
				if relation.MoreSpecific == key && relation.LessSpecific == existingKey {
					s.removeEntryLocked(existingEntry)
				}
			}
			if cfg.FamilyPolicy.TelnetSuppression.EditNeighborEnabled &&
				(incomingContested || existingEntry.contested) &&
				isEditNeighborPair(existingKey, key, bucket.mode, cfg.DistanceModelCW, cfg.DistanceModelRTTY) {
				if existingEntry.support > incomingSupport {
					suppress = true
					break
				}
				if existingEntry.support < incomingSupport {
					s.removeEntryLocked(existingEntry)
					continue
				}
				// Deterministic tie-break: earlier emitted spot wins.
				suppress = true
				break
			}
		}
		if suppress {
			break
		}
	}
	if suppress {
		return true
	}

	s.addEntryLocked(bucket, key, sp.Frequency, incomingSupport, incomingContested, now)
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

func (s *telnetFamilySuppressor) addEntryLocked(bucket telnetFamilyBucket, key string, freqKHz float64, support int, contested bool, now time.Time) {
	calls := s.buckets[bucket]
	if calls == nil {
		calls = make(map[string]*telnetFamilyEntry, 4)
		s.buckets[bucket] = calls
	}
	entry := &telnetFamilyEntry{
		bucket:    bucket,
		key:       key,
		freqKHz:   freqKHz,
		seenAt:    now,
		support:   support,
		contested: contested,
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

func hasComparableEditNeighborCandidate(snapshot spot.ResolverSnapshot, call string, mode, cwModel, rttyModel string) bool {
	if strings.TrimSpace(call) == "" || len(snapshot.CandidateRanks) == 0 {
		return false
	}
	callSupport := correctionflow.ResolverSupportForCall(snapshot, call)
	for _, candidate := range snapshot.CandidateRanks {
		candidateCall := spot.CorrectionVoteKey(candidate.Call)
		if candidateCall == "" || strings.EqualFold(candidateCall, call) {
			continue
		}
		if candidate.Support < callSupport {
			continue
		}
		if isEditNeighborPair(call, candidateCall, mode, cwModel, rttyModel) {
			return true
		}
	}
	return false
}

func isEditNeighborPair(left, right, mode, cwModel, rttyModel string) bool {
	left = spot.CorrectionVoteKey(left)
	right = spot.CorrectionVoteKey(right)
	if left == "" || right == "" || strings.EqualFold(left, right) {
		return false
	}
	if strings.Contains(left, "/") || strings.Contains(right, "/") {
		return false
	}
	return spot.CallDistance(left, right, mode, cwModel, rttyModel) == 1
}
