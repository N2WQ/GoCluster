package pathreliability

import (
	"cmp"
	"fmt"
	"maps"
	"math/rand"
	"testing"
	"time"
)

// This oracle scans the full population at each overflow. Its eviction rule
// uses the operator contract's field order independently of the shortlist.
func referenceDelayAdmission(entries map[voacapDelayKey]voacapDelayEntry, limit int, key voacapDelayKey, entry voacapDelayEntry) {
	if _, exists := entries[key]; !exists && len(entries) == limit {
		var victim voacapDelayKey
		var oldest time.Time
		found := false
		for k, e := range entries {
			if !found || e.lastSeen.Before(oldest) || e.lastSeen.Equal(oldest) && cmp.Or(
				cmp.Compare(k.userCell, victim.userCell), cmp.Compare(k.dxCell, victim.dxCell),
				cmp.Compare(k.band, victim.band), cmp.Compare(k.frequencyKHz, victim.frequencyKHz),
				cmp.Compare(k.direction, victim.direction),
			) > 0 {
				victim, oldest, found = k, e.lastSeen, true
			}
		}
		delete(entries, victim)
	}
	entries[key] = entry
}

func TestVOACAPDelayAdmissionReplay(t *testing.T) {
	for _, limit := range []int{1, 2, 63, 64, 65, 129} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			runDelayAdmissionReplay(t, limit)
		})
	}
}

func TestVOACAPDelaySavedVictims(t *testing.T) {
	const limit = 65
	seed := func(t *testing.T, base time.Time) *VOACAPClosedFallback {
		t.Helper()
		f, _ := newLivePolicyFixture(t, limit)
		for id := 1; id <= limit+1; id++ {
			_, key := livePolicyKeys(id)
			at := base
			if id >= limit {
				at = base.Add(time.Hour)
			}
			f.addDelayLocked(key, voacapDelayEntry{firstSeen: at, lastSeen: at})
		}
		_, first := livePolicyKeys(64)
		if _, exists := f.delays[first]; exists {
			t.Fatal("greatest of the 64 tied keys survived the first eviction")
		}
		return f
	}
	assertNextVictim := func(t *testing.T, f *VOACAPClosedFallback, want int, at time.Time) {
		t.Helper()
		_, incoming := livePolicyKeys(67)
		f.addDelayLocked(incoming, voacapDelayEntry{firstSeen: at, lastSeen: at})
		_, victim := livePolicyKeys(want)
		if _, exists := f.delays[victim]; exists || len(f.delays) != limit {
			t.Fatalf("wanted victim %d and %d survivors", want, limit)
		}
	}
	for _, base := range []time.Time{
		{}, liveQualificationNow(), time.Unix(0, 1<<63-1).UTC(), time.Unix(0, -1<<63).UTC(),
		time.Date(1000, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC),
	} {
		for _, delta := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
			t.Run(fmt.Sprintf("outsider/%d/%s", base.Year(), delta), func(t *testing.T) {
				f := seed(t, base)
				_, outsider := livePolicyKeys(65)
				f.delayElapsedLocked(outsider, base.Add(delta))
				want := 65
				if delta > 0 {
					want = 63
				}
				assertNextVictim(t, f, want, base.Add(time.Hour))
			})
		}
	}
	t.Run("all-captured-paths-revisited", func(t *testing.T) {
		now := liveQualificationNow()
		f := seed(t, now)
		for id := 1; id <= 63; id++ {
			_, key := livePolicyKeys(id)
			f.delayElapsedLocked(key, now.Add(2*time.Hour))
		}
		assertNextVictim(t, f, 66, now.Add(3*time.Hour))
	})
}

func runDelayAdmissionReplay(t *testing.T, limit int) {
	t.Helper()
	f, now := newLivePolicyFixture(t, limit)
	f.fallback.DelaySeconds, f.fallback.CacheTTLSeconds = 17, 73
	want := make(map[voacapDelayKey]voacapDelayEntry)
	keys := make([]voacapDelayKey, limit+67)
	for i := range keys {
		keys[i] = voacapDelayKey{userCell: CellID(i%5 + 1), dxCell: CellID(i/5 + 1),
			band: []string{"20m", "40m"}[i%2], frequencyKHz: 7030 + i%3, direction: voacapDirectionBidirectionalID}
	}
	times := []time.Time{
		now, now.Add(-time.Nanosecond), now.Add(time.Nanosecond), now.Add(-time.Second), now.Add(time.Hour),
		time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2400, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	// The production clock always supplies wall-only UTC. These instants also
	// straddle both sides of the wrap in int64 Unix nanoseconds.
	for _, at := range []time.Time{time.Unix(0, -1<<63).UTC(), time.Unix(0, 1<<63-1).UTC()} {
		times = append(times, at.Add(-time.Nanosecond), at, at.Add(time.Nanosecond))
	}
	rng := rand.New(rand.NewSource(int64(65000 + limit)))
	var lastSweep time.Time
	for step := range 3000 {
		key := keys[rng.Intn(len(keys))]
		at := times[rng.Intn(len(times))]
		op := rng.Intn(12)
		// Start at full capacity with equal times, then keep inserting newer
		// paths through several refill boundaries before random edits.
		if step < len(keys) {
			key, at, op = keys[step], now, 0
			if step >= limit {
				at = now.Add(time.Second * time.Duration(step))
			}
		}
		switch {
		case op < 5:
			entry := voacapDelayEntry{firstSeen: at.Add(-time.Minute), lastSeen: at, kind: voacapDelayKind(step % 3)}
			referenceDelayAdmission(want, limit, key, entry)
			f.addDelayLocked(key, entry)
		case op < 10:
			entry, exists := want[key]
			eligible := false
			switch {
			case !exists:
				referenceDelayAdmission(want, limit, key, voacapDelayEntry{firstSeen: at, lastSeen: at})
			case entry.kind == voacapDelayRetired:
				eligible = true
			default:
				eligible = !at.Before(entry.firstSeen.Add(17 * time.Second))
				entry.lastSeen = at
				want[key] = entry
			}
			if got := f.delayElapsedLocked(key, at); got != eligible {
				t.Fatalf("step %d eligibility %t, want %t", step, got, eligible)
			}
		case op == 10:
			delete(want, key)
			delete(f.delays, key)
		default:
			if lastSweep.IsZero() || at.Sub(lastSweep) >= time.Minute {
				lastSweep = at
				maps.DeleteFunc(want, func(_ voacapDelayKey, e voacapDelayEntry) bool { return at.Sub(e.lastSeen) > 90*time.Second })
			}
			f.pruneLocked(at)
		}
		if !maps.Equal(want, f.delays) || len(f.delays) > limit {
			t.Fatalf("step %d op %d at %v: live owners differ from full-scan oracle", step, op, at)
		}
	}
}
