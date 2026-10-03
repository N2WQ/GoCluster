package spot

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func v16HarmonicSpot(call string, frequency float64, report int, at time.Time) *Spot {
	s := NewSpotNormalized(call, "W1AW", frequency, "CW")
	s.Report, s.Time = report, at
	return s
}

// Check independent parent/child timestamps and both owner maps, including
// unused slice capacity. Comparing only a root or visible length misses stale
// reverse indexes and callsign references left behind after a pop.
func v16CheckHarmonicIndex(t *testing.T, hd *HarmonicDetector) {
	t.Helper()
	if len(hd.entries) != len(hd.lastSeen) || len(hd.expiry) != len(hd.lastSeen) {
		t.Fatalf("ownership cardinality: entries=%d recency=%d expiry=%d", len(hd.entries), len(hd.lastSeen), len(hd.expiry))
	}
	seen := make(map[string]bool)
	for index, call := range hd.expiry {
		owner, ok := hd.lastSeen[call]
		if !ok || seen[call] || owner.index != index || len(hd.entries[call]) == 0 {
			t.Fatalf("expiry index %d call=%s owner=%+v duplicate=%v", index, call, owner, seen[call])
		}
		seen[call] = true
		if index != 0 && owner.at.Before(hd.lastSeen[hd.expiry[(index-1)/2]].at) {
			t.Fatalf("expiry order invalid at %d", index)
		}
	}
	for _, call := range hd.expiry[len(hd.expiry):cap(hd.expiry)] {
		if call != "" {
			t.Fatal("retired expiry backing retained a callsign")
		}
	}
	stats := hd.RetentionStats()
	if stats.Calls != len(hd.entries) || stats.RecencyEntries != len(hd.lastSeen) || stats.ExpiryEntries != len(hd.expiry) || stats.ExpiryCapacity != cap(hd.expiry) || stats.ExpiryBackingBytes != uint64(cap(hd.expiry))*uint64(unsafe.Sizeof(string(""))) {
		t.Fatalf("scalar retention report=%+v", stats)
	}
	if len(hd.expiry) == 0 && cap(hd.expiry) != 0 || len(hd.expiry) != 0 && cap(hd.expiry) > max(8, 4*len(hd.expiry)) {
		t.Fatalf("index backing did not converge: %d/%d", len(hd.expiry), cap(hd.expiry))
	}
}

func TestV16HarmonicReferenceTrace(t *testing.T) {
	settings := v16HarmonicSettings()
	settings.RecencyWindow = 20 * time.Second
	hd := NewHarmonicDetector(settings)
	reference := newV16ReferenceDetector(v16ReferenceSettings(settings))
	base := time.Unix(1_700_000_000, 0).UTC()
	rng := rand.New(rand.NewSource(1603))
	for step := 0; step < 3000; step++ {
		now := base.Add(time.Duration(step/6) * time.Second)
		if step%7 == 0 {
			now = now.Add(-30 * time.Second)
		}
		if step%97 == 0 {
			now = now.Add(100 * time.Second)
		}
		s := v16HarmonicSpot(fmt.Sprintf("K%dV", rng.Intn(37)), 7011*float64(1+rng.Intn(4)), rng.Intn(30), now.Add(time.Duration(rng.Intn(100)-50)*time.Second))
		if step%17 == 0 {
			s.Time = time.Time{}
		}
		if step%13 == 0 {
			s.Mode = "SSB"
		}
		gotDrop, gotFreq, gotCount, gotDelta := hd.ShouldDrop(s, now)
		wantDrop, wantFreq, wantCount, wantDelta := reference.ShouldDrop(s, now)
		if gotDrop != wantDrop || gotFreq != wantFreq || gotCount != wantCount || gotDelta != wantDelta {
			t.Fatalf("step%d return tuple changed: (%v,%v,%d,%d) != (%v,%v,%d,%d)", step, gotDrop, gotFreq, gotCount, gotDelta, wantDrop, wantFreq, wantCount, wantDelta)
		}
		if len(hd.entries) != len(reference.entries) || len(hd.lastSeen) != len(reference.lastSeen) {
			t.Fatalf("step%d owner population differs", step)
		}
		for call, entries := range reference.entries {
			want := make([]harmonicEntry, len(entries))
			for i, entry := range entries {
				want[i] = harmonicEntry(entry)
			}
			if !reflect.DeepEqual(hd.entries[call], want) || hd.lastSeen[call].at != reference.lastSeen[call] {
				t.Fatalf("step%d call=%s retained payload/order/recency changed", step, call)
			}
		}
		v16CheckHarmonicIndex(t, hd)
	}
}

func TestV16HarmonicExpiryBoundaries(t *testing.T) {
	base := time.Unix(100, 0).UTC()
	for _, tc := range []struct {
		name       string
		seedAt     time.Time
		seedNow    time.Time
		candidate  time.Time
		now        time.Time
		otherFirst bool
		wantDrop   bool
	}{
		{"global_equal", base.Add(100 * time.Second), base, base.Add(105 * time.Second), base.Add(10 * time.Second), true, true},
		{"global_beyond", base.Add(100 * time.Second), base, base.Add(105 * time.Second), base.Add(10*time.Second + time.Nanosecond), true, false},
		{"entry_equal", base, base.Add(9 * time.Second), base.Add(10 * time.Second), base.Add(10 * time.Second), false, false},
		{"entry_before_boundary", base, base.Add(9 * time.Second), base.Add(10*time.Second - time.Nanosecond), base.Add(10*time.Second - time.Nanosecond), false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := v16HarmonicSettings()
			settings.RecencyWindow = 10 * time.Second
			hd := NewHarmonicDetector(settings)
			hd.ShouldDrop(v16HarmonicSpot("K1V", 7000, 20, tc.seedAt), tc.seedNow)
			if tc.otherFirst {
				hd.ShouldDrop(v16HarmonicSpot("K2V", 8000, 20, tc.now), tc.now)
			}
			drop, freq, corroborators, delta := hd.ShouldDrop(v16HarmonicSpot("K1V", 14000, 10, tc.candidate), tc.now)
			if drop != tc.wantDrop || drop && (freq != 7000 || corroborators != 1 || delta != 10) || !drop && (freq != 0 || corroborators != 0 || delta != 0) {
				t.Fatalf("literal boundary tuple=(%v,%v,%d,%d)", drop, freq, corroborators, delta)
			}
			v16CheckHarmonicIndex(t, hd)
		})
	}
}

func TestV16HarmonicRefreshAndBackwardTime(t *testing.T) {
	settings := v16HarmonicSettings()
	settings.RecencyWindow = 10 * time.Second
	hd := NewHarmonicDetector(settings)
	base := time.Unix(100, 0).UTC()
	future := base.Add(100 * time.Second)
	hd.ShouldDrop(v16HarmonicSpot("K1V", 7000, 20, future), base)
	hd.ShouldDrop(v16HarmonicSpot("K2V", 8000, 20, future), base.Add(6*time.Second))
	harmonic := v16HarmonicSpot("K1V", 14000, 10, future.Add(time.Second))
	if drop, _, _, _ := hd.ShouldDrop(harmonic, base.Add(9*time.Second)); !drop {
		t.Fatal("future-dated retained fundamental did not suppress")
	}
	hd.ShouldDrop(v16HarmonicSpot("K3V", 9000, 20, future), base.Add(11*time.Second))
	if hd.entries["K1V"] == nil || hd.lastSeen["K1V"].at != base.Add(9*time.Second) {
		t.Fatal("suppressed harmonic did not refresh existing recency")
	}
	if drop, _, _, _ := hd.ShouldDrop(harmonic, base.Add(3*time.Second)); !drop {
		t.Fatal("backward now changed harmonic output")
	}
	v16CheckHarmonicIndex(t, hd)
	hd.ShouldDrop(v16HarmonicSpot("K4V", 10000, 20, future), base.Add(14*time.Second))
	if hd.entries["K1V"] != nil {
		t.Fatal("backward recency did not become eligible for exact global expiry")
	}
	if drop, _, _, _ := hd.ShouldDrop(harmonic, base.Add(3*time.Second)); drop {
		t.Fatal("globally deleted fundamental resurrected after time rollback")
	}
	v16CheckHarmonicIndex(t, hd)
}

func TestV16HarmonicHeapOwnershipChurn(t *testing.T) {
	hd := NewHarmonicDetector(v16HarmonicSettings())
	base := time.Unix(100, 0).UTC()
	future := base.Add(time.Hour)
	for i := 0; i < 127; i++ {
		hd.ShouldDrop(v16HarmonicSpot(fmt.Sprintf("K%dV", i), 7000, 20, future), base.Add(time.Duration(i)*time.Millisecond))
	}
	for i := 0; i < 2000; i++ {
		now := base.Add(time.Duration((i*19)%127) * time.Millisecond)
		hd.ShouldDrop(v16HarmonicSpot(fmt.Sprintf("K%dV", i%127), 14000, 10, future.Add(time.Second)), now)
		v16CheckHarmonicIndex(t, hd)
	}
	// An old source timestamp with fresh admission recency is valid retained
	// input. Its next call prunes an interior/root/last owner before reinsertion.
	for _, position := range []int{0, 31, 126} {
		call := hd.expiry[position]
		hd.entries[call][0].at = base.Add(-time.Hour)
		hd.ShouldDrop(v16HarmonicSpot(call, 7000, 20, future), base.Add(time.Second))
		v16CheckHarmonicIndex(t, hd)
	}
	hd.mu.Lock()
	hd.cleanup(base.Add(2 * time.Hour))
	hd.mu.Unlock()
	v16CheckHarmonicIndex(t, hd)
}

func TestV16HarmonicHeapBackingConverges(t *testing.T) {
	settings := v16HarmonicSettings()
	settings.RecencyWindow = 10000 * time.Second
	hd := NewHarmonicDetector(settings)
	base := time.Unix(100, 0).UTC()
	for i := 0; i < 1000; i++ {
		at := base.Add(time.Duration(i) * time.Second)
		hd.ShouldDrop(v16HarmonicSpot(fmt.Sprintf("K%dV", i), 7000, 20, at), at)
	}
	before := hd.RetentionStats()
	hd.mu.Lock()
	hd.cleanup(base.Add(10900 * time.Second))
	hd.mu.Unlock()
	if stats := hd.RetentionStats(); stats.Calls != 100 || stats.ExpiryCapacity != 200 || stats.ExpiryCapacity >= before.ExpiryCapacity {
		t.Fatalf("partial drain backing=%+v before=%+v", stats, before)
	}
	v16CheckHarmonicIndex(t, hd)
	hd.mu.Lock()
	hd.cleanup(base.Add(11000 * time.Second))
	hd.mu.Unlock()
	v16CheckHarmonicIndex(t, hd)
	if hd.expiry != nil {
		t.Fatal("empty heap still owns backing")
	}
	if unsafe.Sizeof(harmonicRecency{})-unsafe.Sizeof(time.Time{}) != unsafe.Sizeof(int(0)) {
		t.Fatal("recency value backing increase differs from one index")
	}
	t.Logf("map value: old=%d new=%d; heap slot=%d; peak capacity=%d; pre-compaction overlap capacity=%d", unsafe.Sizeof(time.Time{}), unsafe.Sizeof(harmonicRecency{}), unsafe.Sizeof(string("")), before.ExpiryCapacity, before.ExpiryCapacity+200)
}

func TestV16HarmonicRefreshReleasesOldCallBacking(t *testing.T) {
	hd := NewHarmonicDetector(v16HarmonicSettings())
	base := time.Unix(100, 0).UTC()
	oldFrame := "K1V" + strings.Repeat("x", 4096)
	first := v16HarmonicSpot(oldFrame[:3], 7000, 20, base)
	// Ingest may supply already-normalized fields directly. Bypass the global
	// normalization cache so this fixture really presents separate owners.
	first.DXCall, first.DXCallNorm = oldFrame[:3], oldFrame[:3]
	hd.ShouldDrop(first, base)
	call := strings.Clone("K1V")
	second := v16HarmonicSpot(call, 7100, 20, base.Add(time.Second))
	second.DXCall, second.DXCallNorm = call, call
	if unsafe.StringData(second.DXCallNorm) == unsafe.StringData(first.DXCallNorm) {
		t.Fatal("fixture did not create distinct equal callsign backing")
	}
	hd.ShouldDrop(second, base.Add(time.Second))
	index := hd.lastSeen[call].index
	if unsafe.StringData(hd.expiry[index]) != unsafe.StringData(second.DXCallNorm) {
		t.Fatal("expiry index retained the obsolete input-frame backing")
	}
	for retained := range hd.lastSeen {
		if unsafe.StringData(retained) != unsafe.StringData(hd.expiry[index]) {
			t.Fatal("expiry string does not share the refreshed recency owner backing")
		}
	}
	v16CheckHarmonicIndex(t, hd)
}

func TestV16HarmonicConcurrentCleanup(t *testing.T) {
	hd := NewHarmonicDetector(v16HarmonicSettings())
	base := time.Unix(100, 0).UTC()
	var workers sync.WaitGroup
	for worker := 0; worker < 3; worker++ {
		workers.Go(func() {
			for i := 0; i < 500; i++ {
				now := base.Add(time.Duration(i) * time.Second)
				switch worker {
				case 0:
					hd.ShouldDrop(v16HarmonicSpot(fmt.Sprintf("K%dV", i%97), 7000, 20, now), now)
				case 1:
					hd.mu.Lock()
					hd.cleanup(now)
					hd.mu.Unlock()
				case 2:
					stats := hd.RetentionStats()
					if stats.Calls != stats.RecencyEntries || stats.Calls != stats.ExpiryEntries {
						t.Error("observation saw partial owner/index transaction")
					}
				}
			}
		})
	}
	workers.Wait()
	hd.mu.Lock()
	hd.cleanup(base.Add(time.Hour))
	hd.mu.Unlock()
	v16CheckHarmonicIndex(t, hd)
}
