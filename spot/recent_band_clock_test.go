// File role: Falsifies legacy-history admission/query/maintenance clocks while
// checking physical receiver retention and coupled active-call references.
package spot

import (
	"fmt"
	"maps"
	"reflect"
	"sync"
	"testing"
	"time"
)

type recentBandClockEntry struct {
	spotters map[string]time.Time
	lastSeen time.Time
}

type recentBandClockState struct {
	entries []map[recentBandKey]recentBandClockEntry
	active  activeBandCallCounter
}

// Capture only at quiescent barriers. Physical state is inspected before a
// query can prune a broken admission or conceal incomplete maintenance.
func recentBandClockSnapshot(t testing.TB, s *RecentBandStore) recentBandClockState {
	t.Helper()
	out := recentBandClockState{entries: make([]map[recentBandKey]recentBandClockEntry, len(s.shards))}
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		if shard.entries != nil {
			out.entries[i] = make(map[recentBandKey]recentBandClockEntry, len(shard.entries))
			for key, entry := range shard.entries {
				if entry == nil {
					shard.mu.Unlock()
					t.Fatal("retained legacy key has no owner entry")
				}
				out.entries[i][key] = recentBandClockEntry{maps.Clone(entry.spotters), entry.lastSeen}
			}
		}
		shard.mu.Unlock()
	}
	s.statsMu.RLock()
	out.active = activeBandCallCounter{callRefs: maps.Clone(s.active.callRefs), bandRefs: maps.Clone(s.active.bandRefs),
		bandCounts: maps.Clone(s.active.bandCounts), total: s.active.total}
	s.statsMu.RUnlock()
	return out
}

func recentBandClockAssertCoupling(t testing.TB, s *RecentBandStore, wantEmpty bool) {
	t.Helper()
	state := recentBandClockSnapshot(t, s)
	callRefs := make(map[string]int)
	bandRefs := make(map[activeBandCallKey]int)
	entries := 0
	for _, shard := range state.entries {
		if len(shard) > s.perShardMax {
			t.Fatalf("legacy shard retained %d keys above cap %d", len(shard), s.perShardMax)
		}
		for key, entry := range shard {
			entries++
			if len(entry.spotters) == 0 || len(entry.spotters) > s.maxSpottersPerCall {
				t.Fatalf("legacy key %+v retained %d receivers, cap %d", key, len(entry.spotters), s.maxSpottersPerCall)
			}
			callRefs[key.call]++
			bandRefs[activeBandCallKey{key.band, key.call}]++
		}
	}
	bandCounts := make(map[string]int)
	for key := range bandRefs {
		bandCounts[key.band]++
	}
	if !maps.Equal(callRefs, state.active.callRefs) || !maps.Equal(bandRefs, state.active.bandRefs) ||
		!maps.Equal(bandCounts, state.active.bandCounts) || state.active.total != len(callRefs) {
		t.Fatalf("physical keys and reference maps disagree: entries=%v active=%+v", state.entries, state.active)
	}
	if wantEmpty && (entries != 0 || len(state.active.callRefs) != 0 || len(state.active.bandRefs) != 0 ||
		len(state.active.bandCounts) != 0 || state.active.total != 0) {
		t.Fatalf("expired primary or reference state survived: %+v", state)
	}
}

func newRecentBandClockStore(capacity int) *RecentBandStore {
	return NewRecentBandStoreWithOptions(RecentBandOptions{
		Window: time.Hour, Shards: 1, MaxEntries: 3, CleanupInterval: 10 * time.Minute, MaxSpottersPerCall: capacity,
	})
}

func TestRecentBandClockRequirementsPreserveCapacity(t *testing.T) {
	now := time.Date(2030, 2, 14, 12, 0, 0, 123, time.UTC)
	for _, capacity := range []int{1, 2, 16, 64} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			s := newRecentBandClockStore(capacity)
			for i := 0; i < capacity; i++ {
				s.RecordAt("K1ABC", "20m", "CW", fmt.Sprintf("W%03d", i), now, now)
			}
			if count := s.RecentSupportCount("K1ABC", "20m", "CW", now); count != capacity {
				t.Fatalf("count=%d want actual retained capacity %d", count, capacity)
			}
			if !s.HasRecentSupport("K1ABC", "20m", "CW", capacity, now) || s.HasRecentSupport("K1ABC", "20m", "CW", capacity+1, now) {
				t.Fatal("exact-capacity support must qualify and impossible requirement must remain false")
			}
			for _, defaultMinimum := range []int{-1, 0} {
				if got := s.HasRecentSupport("K1ABC", "20m", "CW", defaultMinimum, now); got != (capacity >= 2) {
					t.Fatalf("effective default two at capacity %d unexpectedly qualified=%t", capacity, got)
				}
			}
			recentBandClockAssertCoupling(t, s, false)
		})
	}
}

func TestRecentBandClockFutureAdmissionPreservesPhysicalHistory(t *testing.T) {
	now := time.Date(2030, 2, 14, 12, 0, 0, 123, time.UTC)
	for _, target := range []string{"same_receiver", "new_receiver", "new_key"} {
		for _, offset := range []time.Duration{time.Nanosecond, 2 * time.Hour} {
			t.Run(target+"/"+offset.String(), func(t *testing.T) {
				s := newRecentBandClockStore(2)
				// Fill both receiver capacity and primary key capacity, including
				// repeated call/band references that totals alone cannot audit.
				s.RecordAt("K1ABC", "20m", "CW", "W1AAA", now.Add(-2*time.Minute), now)
				s.RecordAt("K1ABC", "20m", "CW", "W1BBB", now.Add(-time.Minute), now)
				s.RecordAt("K1ABC", "20m", "RTTY", "W1CCC", now.Add(-30*time.Second), now)
				s.RecordAt("K1ABD", "40m", "CW", "W1DDD", now, now)
				before := recentBandClockSnapshot(t, s)
				call, receiver := "K1ABC", "W1AAA"
				if target == "new_receiver" {
					receiver = "W1ZZZ"
				}
				if target == "new_key" {
					call = "K1ABE"
				}
				s.RecordAt(call, "20m", "CW", receiver, now.Add(offset), now)
				if after := recentBandClockSnapshot(t, s); !reflect.DeepEqual(before, after) {
					t.Fatalf("rejected future admission changed timestamps, lastSeen, eviction or references:\nbefore=%+v\nafter=%+v", before, after)
				}
				recentBandClockAssertCoupling(t, s, false)
				if n := s.RecentSupportCount("K1ABC", "20m", "CW", now); n != 2 {
					t.Fatalf("valid preexisting support=%d want 2", n)
				}
			})
		}
	}
}

func TestRecentBandClockRejectedAdmissionCreatesNoState(t *testing.T) {
	now := time.Date(2030, 2, 14, 12, 0, 0, 123, time.UTC)
	for _, tc := range []struct {
		name            string
		seen, admission time.Time
	}{
		{"future", now.Add(time.Nanosecond), now},
		{"zero_admission", now, time.Time{}},
		{"both_zero", time.Time{}, time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newRecentBandClockStore(2)
			before := recentBandClockSnapshot(t, s)
			s.RecordAt(" k1abc ", "20M", "cw", " w1aaa ", tc.seen, tc.admission)
			if after := recentBandClockSnapshot(t, s); !reflect.DeepEqual(before, after) {
				t.Fatalf("refused first receipt allocated or mutated retained state: %+v", after)
			}
		})
	}
}

func TestRecentBandClockAdmissionNanosecondBoundary(t *testing.T) {
	now := time.Date(2030, 2, 14, 12, 0, 0, 123, time.UTC)
	for _, offset := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
		t.Run(offset.String(), func(t *testing.T) {
			s := newRecentBandClockStore(8)
			s.RecordAt("K1ABC", "20m", "CW", "W1AAA", now.Add(offset), now)
			state := recentBandClockSnapshot(t, s)
			want := 0
			if offset <= 0 {
				want = 1
			}
			if got := len(state.entries[0]); got != want {
				t.Fatalf("admission offset %v retained %d keys", offset, got)
			}
			if offset <= 0 {
				got := state.entries[0][recentBandKey{"K1ABC", "20m", "CW"}].spotters["W1AAA"]
				if !got.Equal(now.Add(offset)) {
					t.Fatal("accepted nanosecond timestamp lost precision")
				}
			}
		})
	}
}

func TestRecentBandClockQueryInclusiveBounds(t *testing.T) {
	now := time.Date(2030, 2, 14, 12, 0, 0, 123, time.UTC)
	for _, normalized := range []bool{false, true} {
		for _, tc := range []struct {
			name string
			seen time.Time
			want int
		}{
			{"before_cutoff", now.Add(-time.Hour - time.Nanosecond), 0},
			{"at_cutoff", now.Add(-time.Hour), 1},
			{"after_cutoff", now.Add(-time.Hour + time.Nanosecond), 1},
			{"at_query", now, 1},
			{"after_query", now.Add(time.Nanosecond), 0},
		} {
			t.Run(fmt.Sprintf("normalized_%t/%s", normalized, tc.name), func(t *testing.T) {
				// Separate populations preserve existing valid-write pruning:
				// this store does not reconstruct rows removed by later writes.
				s := newRecentBandClockStore(8)
				s.RecordAt("K1ABC", "20m", "CW", "W1AAA", tc.seen, now.Add(time.Minute))
				query := s.RecentSupportCount
				if normalized {
					query = s.RecentSupportCountNormalized
				}
				if got := query("K1ABC", "20m", "CW", now); got != tc.want {
					t.Fatalf("boundary count=%d want %d at observation=%s query=%s", got, tc.want, tc.seen, now)
				}
				state := recentBandClockSnapshot(t, s)
				if tc.name == "after_query" && len(state.entries[0]) != 1 {
					t.Fatal("upper-bound exclusion deleted a valid later observation")
				}
				recentBandClockAssertCoupling(t, s, tc.name == "before_cutoff")
			})
		}
	}
}

func TestRecentBandClockBackwardQueryPreservesLaterRows(t *testing.T) {
	now := time.Date(2030, 2, 14, 12, 0, 0, 123, time.UTC)
	for _, normalized := range []bool{false, true} {
		t.Run(fmt.Sprintf("normalized_%t", normalized), func(t *testing.T) {
			s := newRecentBandClockStore(8)
			later := now.Add(10 * time.Minute)
			s.RecordAt("K1ABC", "20m", "CW", "W1AAA", now.Add(-time.Minute), later)
			s.RecordAt("K1ABC", "20m", "CW", "W1BBB", later, later)
			before := recentBandClockSnapshot(t, s)
			query := s.RecentSupportCount
			if normalized {
				query = s.RecentSupportCountNormalized
			}
			if got := query("K1ABC", "20m", "CW", now); got != 1 {
				t.Fatalf("backward query counted later support: %d want 1", got)
			}
			if after := recentBandClockSnapshot(t, s); !reflect.DeepEqual(before, after) {
				t.Fatal("excluding upper rows changed retained valid history")
			}
			if got := query("K1ABC", "20m", "CW", later); got != 2 {
				t.Fatalf("later query lost previously excluded support: %d want 2", got)
			}
		})
	}
}

func TestRecentBandClockZeroObservationNormalization(t *testing.T) {
	wall := time.Now().UTC().Truncate(time.Second)
	zone := time.FixedZone("admission-zone", -4*60*60)
	for _, offset := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
		t.Run(offset.String(), func(t *testing.T) {
			s := newRecentBandClockStore(8)
			admission := wall.Add(offset).In(zone)
			s.RecordAt("K1ZERO", "20m", "CW", "W1AAA", time.Time{}, admission)
			key := recentBandKey{"K1ZERO", "20m", "CW"}
			entry := recentBandClockSnapshot(t, s).entries[0][key]
			got := entry.spotters["W1AAA"]
			if !got.Equal(admission) || got.Location() != time.UTC || !entry.lastSeen.Equal(admission) || entry.lastSeen.Location() != time.UTC {
				t.Fatalf("zero observation normalized to %v lastSeen=%v, want exact admission UTC %v", got, entry.lastSeen, admission.UTC())
			}
			boundary := admission.Add(time.Hour)
			if s.RecentSupportCount("K1ZERO", "20m", "CW", boundary) != 1 ||
				s.RecentSupportCountNormalized("K1ZERO", "20m", "CW", boundary) != 1 {
				t.Fatal("normalized zero timestamp expired before inclusive window equality")
			}
			if s.RecentSupportCount("K1ZERO", "20m", "CW", boundary.Add(time.Nanosecond)) != 0 {
				t.Fatal("normalized zero timestamp survived window+1ns")
			}
			recentBandClockAssertCoupling(t, s, true)
		})
	}
}

func TestRecentBandClockMaintenanceExpiresPhysicalUntouchedKeys(t *testing.T) {
	// This is safely historical relative to the machine wall clock. Replay
	// wiring tests own dispatch; here explicit maintenance owns physical expiry.
	now := time.Date(2020, 2, 14, 12, 0, 0, 123, time.UTC)
	s := newRecentBandClockStore(8)
	for _, row := range []struct{ call, band, mode string }{
		{"K1ABC", "20m", "CW"}, {"K1ABC", "20m", "RTTY"}, {"K1ABC", "40m", "CW"},
	} {
		s.RecordAt(row.call, row.band, row.mode, "W1AAA", time.Time{}, now)
	}
	before := recentBandClockSnapshot(t, s)
	if interval := s.CleanupAt(time.Time{}); interval != 0 || !reflect.DeepEqual(before, recentBandClockSnapshot(t, s)) {
		t.Fatal("zero maintenance clock mutated history or supplied an actionable interval")
	}
	if interval := s.CleanupAt(now.Add(time.Hour)); interval != 10*time.Minute {
		t.Fatalf("explicit maintenance interval=%v want existing ten minutes", interval)
	}
	if after := recentBandClockSnapshot(t, s); !reflect.DeepEqual(before, after) {
		t.Fatal("historical evaluation-clock maintenance deleted boundary-eligible untouched keys")
	}
	recentBandClockAssertCoupling(t, s, false)
	// No support query precedes this readback: cleanup alone must delete all
	// primary keys and each call/band reference, including repeated references.
	s.CleanupAt(now.Add(time.Hour + time.Nanosecond))
	recentBandClockAssertCoupling(t, s, true)
	var absent *RecentBandStore
	if absent.CleanupAt(now) != 0 {
		t.Fatal("nil maintenance owner supplied an interval")
	}
}

func TestRecentBandClockBoundedChurnAndReferenceReclamation(t *testing.T) {
	s := NewRecentBandStoreWithOptions(RecentBandOptions{Window: time.Hour, Shards: 2, MaxEntries: 8, MaxSpottersPerCall: 2})
	base := time.Date(2030, 2, 14, 12, 0, 0, 123, time.UTC)
	for round := 0; round < 8; round++ {
		now := base.Add(time.Duration(round) * 2 * time.Hour)
		for i := 0; i < 64; i++ {
			call := fmt.Sprintf("K%dABC", round*64+i)
			for _, receiver := range []string{"W1AAA", "W1BBB", "W1CCC"} {
				s.RecordAt(call, "20m", "CW", receiver, now, now)
			}
			recentBandClockAssertCoupling(t, s, false)
		}
		s.CleanupAt(now)
		recentBandClockAssertCoupling(t, s, false)
		s.CleanupAt(now.Add(time.Hour + time.Nanosecond))
		recentBandClockAssertCoupling(t, s, true)
	}
}

func TestRecentBandClockConcurrentAdmissionQueryMaintenance(t *testing.T) {
	s := NewRecentBandStoreWithOptions(RecentBandOptions{Window: time.Hour, Shards: 2, MaxEntries: 8, MaxSpottersPerCall: 2})
	now := time.Date(2030, 2, 14, 12, 0, 0, 123, time.UTC)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for i := 0; i < 200; i++ {
				call := fmt.Sprintf("K%dABC", i%16)
				switch worker {
				case 0, 1:
					s.RecordAt(call, "20m", "CW", fmt.Sprintf("W%dAAA", worker), now, now)
					s.RecordAt(call, "20m", "CW", "W1FUT", now.Add(time.Nanosecond), now)
				case 2:
					s.RecentSupportCount(call, "20m", "CW", now)
					s.RecentSupportCountNormalized(call, "20m", "CW", now.Add(-time.Nanosecond))
				case 3:
					s.CleanupAt(now)
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()
	recentBandClockAssertCoupling(t, s, false)
	s.CleanupAt(now.Add(time.Hour + time.Nanosecond))
	recentBandClockAssertCoupling(t, s, true)
}
