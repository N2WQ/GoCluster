package dedup

import (
	"encoding/binary"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/spot"

	"github.com/zeebo/xxh3"
)

func TestSecondaryFrequencyEstablishedAllocations(t *testing.T) {
	for _, path := range []string{"repeat", "report", "change"} {
		t.Run(path, func(t *testing.T) {
			o := frequencyOptions("on")
			o.MaxChanges = 10000
			d, err := NewSecondaryDeduperWithFrequencyChanges(time.Minute, path == "report", SecondaryKeyGrid2, o)
			if err != nil {
				t.Fatal(err)
			}
			s := frequencySpot(14025000, 0, true, -10, "CW", spot.SourceRBN)
			d.ShouldForward(s)
			allocations := testing.AllocsPerRun(100, func() {
				switch path {
				case "report":
					s.Report++
				case "change":
					if s.Frequency == 14025 {
						s.Frequency = 14027
					} else {
						s.Frequency = 14025
					}
				}
				d.Evaluate(s)
			})
			if allocations != 0 {
				t.Fatalf("established %s allocates %.2f per call", path, allocations)
			}
		})
	}
}

// Established-entry fixtures exclude insertion and normalization setup from
// timing, but include each real decision and returned evidence. Reporting and
// populated retention are measured separately, not hidden in duplicate numbers.
func BenchmarkSecondaryFrequencyEstablished(b *testing.B) {
	for _, state := range []string{"off", "on", "shadow"} {
		for _, path := range []string{"repeat", "report", "change"} {
			b.Run(state+"/"+path, func(b *testing.B) {
				o := frequencyOptions(state)
				o.MaxChanges = int(^uint(0) >> 1)
				d, err := NewSecondaryDeduperWithFrequencyChanges(179*time.Second, path == "report", SecondaryKeyGrid2, o)
				if err != nil {
					b.Fatal(err)
				}
				s := frequencySpot(14025000, 0, true, -20, "CW", spot.SourceRBN)
				if !d.ShouldForward(s) {
					b.Fatal("first spot suppressed")
				}
				b.ReportAllocs()
				var last bool
				for b.Loop() {
					switch path {
					case "report":
						s.Report++
					case "change":
						if s.Frequency == 14025 {
							s.Frequency = 14027
						} else {
							s.Frequency = 14025
						}
					}
					last, _ = d.Evaluate(s)
				}
				want := path == "report" || (path == "change" && state == "on")
				if last != want {
					b.Fatalf("path=%s output=%t want=%t", path, last, want)
				}
			})
		}
	}
}

func BenchmarkSecondaryFrequencyInsertion(b *testing.B) {
	for _, state := range []string{"off", "on", "shadow"} {
		b.Run(state, func(b *testing.B) {
			d := frequencyDeduper(b, state, false)
			s := frequencySpot(14025000, 0, false, 0, "CW", spot.SourceRBN)
			var key [12]byte
			var index uint64
			b.ReportAllocs()
			for b.Loop() {
				binary.LittleEndian.PutUint64(key[:8], index)
				index++
				// DX normalization is already done; raw bytes create complete
				// distinct fixed-width keys without callsign-formatting noise.
				s.DXCallNorm = string(key[:])
				if !d.ShouldForward(s) {
					b.Fatal("new key suppressed")
				}
			}
		})
	}
}

func BenchmarkSecondaryFrequencyContended(b *testing.B) {
	d := frequencyDeduper(b, "shadow", false)
	s := frequencySpot(14025000, 0, true, -10, "CW", spot.SourceRBN)
	d.ShouldForward(s)
	var forwarded atomic.Uint64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		local := s.Clone()
		for pb.Next() {
			if d.ShouldForward(local) {
				forwarded.Add(1)
			}
		}
	})
	if forwarded.Load() != 0 {
		b.Fatal("contended repeats forwarded")
	}
}

// MixedCleanup stresses the two output callers against populated paired
// cleanup. Use -cpu=2 for the production caller count. The 2-ms sweep is an
// intentional stress cadence, not a forecast of the 60-second runtime sweep.
func BenchmarkSecondaryFrequencyMixedCleanup(b *testing.B) {
	d := frequencyDeduper(b, "shadow", false)
	s := frequencySpot(14025000, 0, true, -10, "CW", spot.SourceRBN)
	s.Time = time.Now().UTC() // The owned cleanup timer uses the real clock.
	calls := make([]string, 100000)
	var encoded [12]byte
	for i := range calls {
		binary.LittleEndian.PutUint64(encoded[:8], uint64(i))
		calls[i] = string(encoded[:])
		s.DXCallNorm, s.Mode, s.ModeNorm, s.Frequency = calls[i], "CW", "CW", 14025
		d.ShouldForward(s)
		if i%5 == 0 {
			s.Mode, s.ModeNorm, s.Frequency = "FT8", "FT8", 14074
			d.ShouldForward(s)
		}
	}
	d.cleanupInterval = 2 * time.Millisecond
	d.Start()
	b.Cleanup(d.Stop)
	var unexpected atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		local := s.Clone()
		index := 0
		for pb.Next() {
			local.DXCallNorm = calls[index]
			local.Mode, local.ModeNorm, local.Frequency = "CW", "CW", 14025
			if index%5 == 0 {
				local.Mode, local.ModeNorm, local.Frequency = "FT8", "FT8", 14074
			}
			if d.ShouldForward(local) {
				unexpected.Add(1)
			}
			index++
			if index == len(calls) {
				index = 0
			}
		}
	})
	b.StopTimer()
	if unexpected.Load() != 0 {
		b.Fatal("fresh mixed repeats forwarded")
	}
}

// This observation includes both map backings and fixed comparison counters.
// It is not an RF-accuracy test or a production logger-budget qualification.
func TestSecondaryFrequencyRetainedHeapMeasurement(t *testing.T) {
	if testing.Short() {
		t.Skip("controlled retained-heap observation")
	}
	measure := func(state string, keys int) int64 {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		d := frequencyDeduper(t, state, false)
		s := frequencySpot(14025000, 0, false, 0, "CW", spot.SourceRBN)
		var key [12]byte
		for i := range keys {
			binary.LittleEndian.PutUint64(key[:8], uint64(i))
			s.DXCallNorm = string(key[:])
			// One fifth of historical keys also carry a second mode.
			s.Mode, s.ModeNorm = "CW", "CW"
			d.ShouldForward(s)
			if i%5 == 0 {
				s.Mode, s.ModeNorm = "FT8", "FT8"
				d.ShouldForward(s)
			}
		}
		_, _, retained := d.GetStats()
		candidateKeys := keys + (keys+4)/5
		want := keys
		if state == "on" {
			want = candidateKeys
		}
		if retained != want {
			t.Fatalf("%s retained %d keys, want %d", state, retained, want)
		}
		if state == "shadow" {
			snapshot, enabled := d.ShadowStats()
			if !enabled || snapshot.BaselineCacheSize != keys || snapshot.CandidateCacheSize != candidateKeys {
				t.Fatalf("shadow populations differ from fixture: %+v", snapshot)
			}
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(d)
		return int64(after.HeapAlloc) - int64(before.HeapAlloc)
	}
	for _, keys := range []int{100000, 200000} {
		off, on, shadow := measure("off", keys), measure("on", keys), measure("shadow", keys)
		if off <= 0 || on <= 0 || shadow <= 0 {
			t.Fatalf("nonpositive heap observation off=%d on=%d shadow=%d", off, on, shadow)
		}
		ratio := float64(shadow) / float64(off)
		t.Logf("%d historical keys,%d candidate keys,20%% dual-mode; off=%d on=%d shadow=%d bytes; shadow/off=%.3f", keys, keys+(keys+4)/5, off, on, shadow, ratio)
		if ratio > 4 {
			t.Fatalf("approved v7 shadow lab budget exceeded: %.3f > 4", ratio)
		}
	}
}

func BenchmarkSecondaryFrequencyPopulatedCleanup(b *testing.B) {
	for _, state := range []string{"off", "on", "shadow"} {
		b.Run(state, func(b *testing.B) {
			d := frequencyDeduper(b, state, false)
			s := frequencySpot(14025000, 0, false, 0, "CW", spot.SourceRBN)
			var call [12]byte
			for i := range 100000 {
				binary.LittleEndian.PutUint64(call[:8], uint64(i))
				s.DXCallNorm = string(call[:])
				d.ShouldForward(s)
			}
			b.ReportAllocs()
			for b.Loop() {
				d.cleanupAt(frequencyTestTime)
			}
		})
	}
}

func TestSecondaryFrequencyCandidateExpiryAndCompaction(t *testing.T) {
	for _, state := range []string{"on", "shadow"} {
		d := frequencyDeduper(t, state, false)
		for i := range 3000 {
			var key [32]byte
			binary.LittleEndian.PutUint64(key[:8], uint64(i))
			d.changes.shards[0].cache[key] = frequencyEntry{when: frequencyTestTime}
			d.changes.shards[0].updatePeak()
		}
		d.cleanupAt(frequencyTestTime.Add(180 * time.Second))
		if len(d.changes.shards[0].cache) != 0 || d.changes.shards[0].peak != 0 {
			t.Fatal("candidate backing did not converge after expiry")
		}
		s := frequencySpot(14025000, 180, false, 0, "CW", spot.SourceRBN)
		if !d.ShouldForward(s) {
			t.Fatal("new entry suppressed after expired state")
		}
		key := secondaryFrequencyKey(s, 291, d.keyMode)
		if shard := d.changes.shards[uint32(xxh3.Hash(key[:]))&63]; len(shard.cache) != 1 {
			t.Fatal("new candidate not retained")
		}
	}
}
