// File role: Owns bounded telnet-only family offer history. Fresh shared
// evidence decides hiding; nonblocking queue admission commits per-policy
// witnesses under the same mutex. No spot or resolver population is retained.
package cluster

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

type familyLanes struct{ fast, med, slow bool }

func (l familyLanes) any() bool { return l.fast || l.med || l.slow }

type telnetFamilyWitness struct {
	call    string // Owned normalized literal; canonical aliases do not pool votes.
	freqKHz float64
	at      time.Time
}

// telnetFamilyEntry retains at most three compact successful ordinary offers.
// The global cap couples entries, bucket maps and LRU nodes; seenAt is the
// newest lane offer, while each lane expires independently and inclusively.
type telnetFamilyEntry struct {
	bucket telnetFamilyBucket
	key    string
	seenAt time.Time
	lanes  [3]telnetFamilyWitness
	prev   *telnetFamilyEntry
	next   *telnetFamilyEntry
}

// telnetFamilySuppressor tracks admitted ordinary lanes in mode/frequency
// buckets. Evidence and publishing callbacks are request-local, never retained.
// Archive, ring and peer gates are independent of its returned telnet mask.
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

// handoff serializes fresh arbitration, nonblocking publication and commitment
// across main and stabilizer consumers. publish must not block in production.
// Its returned mask records attempts even on queue refusal, preventing a SELF
// fallback; only a successful admission teaches ordinary offer history.
func (s *telnetFamilySuppressor) handoff(sp *spot.Spot, cfg config.CallCorrectionConfig, now time.Time, allowed familyLanes,
	selectEvidence func(time.Time) correctionflow.ResolverPrimarySelection, publish func(familyLanes) bool) familyLanes {
	if sp == nil || !allowed.any() {
		return allowed
	}
	bucket, key, toleranceKHz, ok := telnetFamilyBucketForSpot(sp, cfg, s.fallbackHz)
	literal := normalizedDXCall(sp)
	if !ok || literal == "" {
		// Unsupported modes retain the ordinary path without acquiring family
		// history ownership or extending this mutex to FT broadcast traffic.
		if publish != nil {
			publish(allowed)
		}
		return allowed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Resolve a zero production clock after acquiring ownership, so waiting
	// consumers cannot compare evidence at an obsolete pre-lock time.
	now = s.monotonicNowLocked(now)
	s.pruneExpiredLocked(now)
	var selection correctionflow.ResolverPrimarySelection
	if selectEvidence != nil {
		selection = selectEvidence(now)
	}
	offered := allowed
	if selection.SnapshotOK && !selection.NeighborhoodSplit && !selection.Snapshot.EvidenceLoss &&
		(selection.Snapshot.State == spot.ResolverStateConfident || selection.Snapshot.State == spot.ResolverStateProbable) {
		if percent, usable := familyPercent(selection.Snapshot, literal); usable {
			offered = s.compareLanesLocked(sp, cfg, bucket, key, toleranceKHz, now, offered, selection.Snapshot, percent)
		}
	}
	if offered.any() && publish != nil && publish(offered) {
		s.recordOfferLocked(bucket, key, literal, sp.Frequency, now, offered)
	}
	return offered
}

// compareLanesLocked changes only the request-local mask. A stronger independent
// rival can protect a lane even if another comparison is tied/unavailable;
// dominance never deletes an earlier admitted witness.
func (s *telnetFamilySuppressor) compareLanesLocked(sp *spot.Spot, cfg config.CallCorrectionConfig, bucket telnetFamilyBucket,
	key string, toleranceKHz float64, now time.Time, offered familyLanes, snapshot spot.ResolverSnapshot, incomingPercent int) familyLanes {
	lanes := [3]bool{offered.fast, offered.med, offered.slow}
	cutoff := now.Add(-s.window)
	// Only identities represented in this bounded fresh publication can hide.
	// Lookup their canonical entries directly rather than scanning up to 20,000
	// unrelated historical calls. Winner/runner fallback remains authoritative
	// only when familyPercent accepts it; ranked zero support still overrides it.
	for index := 0; index < len(snapshot.CandidateRanks)+2; index++ {
		literal := snapshot.Winner
		switch {
		case index < len(snapshot.CandidateRanks):
			literal = snapshot.CandidateRanks[index].Call
		case index == len(snapshot.CandidateRanks)+1:
			literal = snapshot.RunnerUp
		}
		percent, usable := familyPercent(snapshot, literal)
		if !usable || percent < incomingPercent {
			continue
		}
		existingKey := spot.CorrectionVoteKey(literal)
		if existingKey == "" || existingKey == key {
			continue
		}
		var relation spot.CorrectionFamilyRelation
		related, relationChecked := false, false
		for bin := bucket.freqBin - 1; bin <= bucket.freqBin+1; bin++ {
			entry := s.buckets[telnetFamilyBucket{mode: bucket.mode, freqBin: bin}][existingKey]
			if entry == nil {
				continue
			}
			for lane, witness := range entry.lanes {
				if !lanes[lane] || witness.call != literal || witness.at.IsZero() || witness.at.Before(cutoff) ||
					math.Abs(witness.freqKHz-sp.Frequency) > toleranceKHz {
					continue
				}
				if !relationChecked {
					// Canonical relationship is shared across the three literal
					// lane witnesses; classify once after fresh support qualifies.
					relation, related = spot.DetectCorrectionFamilyWithPolicy(existingKey, key, s.family)
					if !related && cfg.FamilyPolicy.TelnetSuppression.EditNeighborEnabled {
						related = isEditNeighborPair(existingKey, key, bucket.mode, cfg.DistanceModelCW, cfg.DistanceModelRTTY)
					}
					relationChecked = true
				}
				if related && (percent > incomingPercent ||
					(relation.Kind == spot.CorrectionFamilySlash && relation.MoreSpecific == existingKey)) {
					lanes[lane] = false
				}
			}
		}
	}
	return familyLanes{lanes[0], lanes[1], lanes[2]}
}

// familyPercent requires positive usable support before comparing displayed
// shares. A known zero percentage alone supplies no hiding authority. Direct
// literal lookup is bounded by published candidates and avoids
// normalizing thousands of absent retained history calls on dense scans.
func familyPercent(snapshot spot.ResolverSnapshot, literal string) (int, bool) {
	if literal == "" || snapshot.TotalReporters <= 0 {
		return 0, false
	}
	raw, weighted, found := 0, 0, false
	for _, candidate := range snapshot.CandidateRanks {
		if candidate.Call == literal {
			raw, weighted, found = candidate.Support, candidate.WeightedSupportMilli, true
			break
		}
	}
	if !found {
		switch literal {
		case snapshot.Winner:
			raw, weighted = snapshot.WinnerSupport, snapshot.WinnerWeightedSupportMilli
		case snapshot.RunnerUp:
			raw, weighted = snapshot.RunnerSupport, snapshot.RunnerWeightedSupportMilli
		}
	}
	if raw <= 0 {
		return 0, false
	}
	switch snapshot.EffectiveSupportBasis() {
	case spot.ResolverSupportBasisWeighted:
		if weighted <= 0 {
			return 0, false
		}
	case spot.ResolverSupportBasisReceivers:
	default:
		return 0, false
	}
	return correctionflow.ResolverCallConfidencePercent(snapshot, literal)
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

// recordOfferLocked owns small strings only after admission. Copying prevents
// uppercase substring identities from retaining a full received frame. Repeats
// reuse owned literals; the fixed three witnesses add no unbounded history.
func (s *telnetFamilySuppressor) recordOfferLocked(bucket telnetFamilyBucket, key, literal string, freqKHz float64, now time.Time, offered familyLanes) {
	calls := s.buckets[bucket]
	entry := calls[key]
	if entry == nil {
		bucket.mode, key = strings.Clone(bucket.mode), strings.Clone(key)
		if calls == nil {
			calls = make(map[string]*telnetFamilyEntry, 4)
			s.buckets[bucket] = calls
		}
		entry = &telnetFamilyEntry{bucket: bucket, key: key}
		calls[key] = entry
		s.appendTailLocked(entry)
		s.totalEntries++
	}
	ownedLiteral := entry.key
	if literal != entry.key {
		ownedLiteral = ""
		for _, witness := range entry.lanes {
			if witness.call == literal {
				ownedLiteral = witness.call
				break
			}
		}
		if ownedLiteral == "" {
			ownedLiteral = strings.Clone(literal)
		}
	}
	for lane, admitted := range [3]bool{offered.fast, offered.med, offered.slow} {
		if admitted {
			entry.lanes[lane] = telnetFamilyWitness{ownedLiteral, freqKHz, now}
		}
	}
	entry.seenAt = now
	s.moveToTailLocked(entry)
	for s.totalEntries > s.maxEntries {
		s.evictHeadLocked()
	}
}

// pruneExpiredLocked enforces the time window before size eviction so support
// questions can be answered in terms of "recently admitted offers" rather than
// arbitrary memory pressure.
func (s *telnetFamilySuppressor) pruneExpiredLocked(now time.Time) {
	cutoff := now.Add(-s.window)
	for s.head != nil && s.head.seenAt.Before(cutoff) {
		s.removeEntryLocked(s.head)
	}
}

// evictHeadLocked is the hard-size fallback when traffic exceeds the windowed
// expectation; retiring the oldest admitted offer conservatively shows more.
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
