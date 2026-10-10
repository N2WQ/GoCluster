package spot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// Unlike memory-only lookup benchmarks, this includes real Pebble writes and
// cleanup contention. It measures this local workload, not production p99.
func BenchmarkCustomSCPPersistedObservation(b *testing.B) {
	for _, concurrentCleanup := range []bool{false, true} {
		b.Run(fmt.Sprintf("cleanup=%v", concurrentCleanup), func(b *testing.B) {
			s, err := OpenCustomSCPStore(CustomSCPOptions{Path: filepath.Join(b.TempDir(), "scp")})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			now := time.Now().UTC().Truncate(time.Second)
			for i := 0; i < 128; i++ {
				s.recordObservation(fmt.Sprintf("K%dHST", i), "20m", "CW", "N0AAA", 101, 10, true, now)
			}
			quit, done := make(chan struct{}), make(chan struct{})
			if concurrentCleanup {
				go func() {
					defer close(done)
					for {
						select {
						case <-quit:
							return
						default:
							s.cleanup(now)
						}
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.recordObservation("K1BEN", "20m", "CW", "N0BBB", 202, 10, true, now.Add(time.Duration(i)*time.Second))
			}
			b.StopTimer()
			if concurrentCleanup {
				close(quit)
				<-done
			}
			// Require mutation, including memory-only compatibility; a no-op
			// admission path must not look like a performance improvement.
			key := customSCPKey{call: "K1BEN", band: "20m", bucket: "cw"}
			obs, ok := entrySpotterObs(s.entries[key], "N0BBB")
			want := now.Add(time.Duration(b.N-1) * time.Second).Unix()
			if !ok || obs.seenUnix != want || obs.cellRes1 != 202 {
				b.Fatalf("benchmark did not retain the final write: %+v, ok=%v", obs, ok)
			}
			value, closer, err := s.db.Get([]byte(observationKeyString(key, "N0BBB")))
			if err != nil {
				b.Fatal(err)
			}
			if len(value) != 10 || int64(binary.BigEndian.Uint64(value[:8])) != want || binary.BigEndian.Uint16(value[8:]) != 202 {
				b.Fatal("benchmark did not persist the final timestamp")
			}
			if err := closer.Close(); err != nil {
				b.Fatal(err)
			}
		})
	}
}

// One op is a known workload: 519 expired disk rows reclaimed while 64 live
// observations persist. Refilling the expired rows and correctness readbacks
// are outside the timer, so the deletion phase cannot disappear as b.N grows.
func BenchmarkCustomSCPCleanupDeletesWithWriter(b *testing.B) {
	s, err := OpenCustomSCPStore(CustomSCPOptions{Path: filepath.Join(b.TempDir(), "scp")})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	now := time.Now().UTC().Truncate(time.Second)
	var stale [10]byte
	binary.BigEndian.PutUint64(stale[:8], uint64(now.Add(-61*24*time.Hour).Unix()))
	keys := make([][]byte, 519)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("o|K1DRAIN|20m|cw|N0%04d", i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		batch := s.db.NewBatch()
		for _, key := range keys {
			if err := batch.Set(key, stale[:], nil); err != nil {
				_ = batch.Close()
				b.Fatal(err)
			}
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			_ = batch.Close()
			b.Fatal(err)
		}
		if err := batch.Close(); err != nil {
			b.Fatal(err)
		}
		done := make(chan struct{})
		b.StartTimer()
		go func() {
			defer close(done)
			for j := 0; j < 64; j++ {
				s.recordObservation("K1LIVE", "20m", "CW", "N0LIVE", 202, 10, true, now.Add(time.Duration(i*64+j)*time.Second))
			}
		}()
		s.cleanup(now)
		<-done
		b.StopTimer()
		for _, key := range keys {
			_, closer, err := s.db.Get(key)
			if closer != nil {
				_ = closer.Close()
			}
			if !errors.Is(err, pebble.ErrNotFound) {
				b.Fatalf("cleanup benchmark did not delete %s: %v", key, err)
			}
		}
		obs, ok := entrySpotterObs(s.entries[customSCPKey{call: "K1LIVE", band: "20m", bucket: "cw"}], "N0LIVE")
		if !ok || obs.seenUnix != now.Add(time.Duration(i*64+63)*time.Second).Unix() || obs.cellRes1 != 202 {
			b.Fatalf("cleanup benchmark lost the final live observation: %+v ok=%v", obs, ok)
		}
		value, closer, err := s.db.Get([]byte("o|K1LIVE|20m|cw|N0LIVE"))
		if err != nil {
			b.Fatal(err)
		}
		persisted := len(value) == 10 && int64(binary.BigEndian.Uint64(value[:8])) == now.Add(time.Duration(i*64+63)*time.Second).Unix() && binary.BigEndian.Uint16(value[8:]) == 202
		if err := closer.Close(); err != nil {
			b.Fatal(err)
		}
		if !persisted {
			b.Fatal("cleanup benchmark did not persist the final live timestamp/cell")
		}
	}
}
