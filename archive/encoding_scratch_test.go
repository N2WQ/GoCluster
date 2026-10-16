package archive

import (
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"dxcluster/spot"
)

func TestEncodingScratchOverwriteAndCapacity(t *testing.T) {
	rich := ownedFlushInput()
	rich.EnsureNormalized()
	rich.DEMetadata.GridDerived = true
	rich.Comment = strings.Repeat("large", 900)
	record := ownedFlushRecord()
	record.comment = rich.Comment
	wantRich := ownedFlushExpectedBytes(record)
	wantRich[1] = 15 // All four flags; the original oracle intentionally uses 7.
	// Empty legacy toxicity is UNKNOWN, not an empty field. All other header
	// values and payload lengths are literal zero; toxicity is field 14.
	wantEmpty := make([]byte, 81)
	wantEmpty[0], wantEmpty[64] = 6, 0
	wantEmpty[65] = 7
	copy(wantEmpty[74:], "UNKNOWN")
	for _, size := range []int{0, 1, len(wantRich), len(wantRich) + 64} {
		t.Run(fmt.Sprintf("capacity_%d", size), func(t *testing.T) {
			buf := make([]byte, size)
			for i, input := range []*spot.Spot{rich, {}, rich, {}} {
				want := wantRich
				if i%2 != 0 {
					want = wantEmpty
				}
				storage := buf[:cap(buf)]
				for j := range storage {
					storage[j] = 0xa5
				}
				got := encodeRecordInto(buf, input)
				if !bytes.Equal(got, want) {
					t.Fatalf("record %d: literal bytes or length changed", i)
				}
				if len(storage) >= len(want) && &got[0] != &storage[0] {
					t.Fatalf("record %d allocated despite sufficient capacity", i)
				}
				if len(storage) > 0 && len(storage) < len(want) && &got[0] == &storage[0] {
					t.Fatal("growth did not replace insufficient storage")
				}
				buf = got
			}
		})
	}
}

func TestEncodingScratchOwnedResults(t *testing.T) {
	s := ownedFlushInput()
	s.EnsureNormalized()
	want := ownedFlushExpectedBytes(ownedFlushRecord())
	first, second := encodeRecord(s), encodeRecord(s)
	first[0] = 0xff
	if !bytes.Equal(second, want) {
		t.Fatal("owned wrapper results share storage")
	}
	scratch := encodeRecordInto(nil, s)
	scratch[0] = 0xff
	if !bytes.Equal(second, want) {
		t.Fatal("scratch aliases an owned wrapper result")
	}
}

var encodingScratchAllocationWitness []byte
var encodingScratchByteWitness byte

func TestEncodingScratchNoAllocations(t *testing.T) {
	// Keep unrelated event/category/string-normalization allocations out of
	// this exact buffer contract. Whole-flush benchmarks include realistic work.
	s := &spot.Spot{DXCallNorm: "K1ABC", DECallNorm: "N0XYZ", ModeNorm: "CW", BandNorm: "20m", ToxicityStatus: spot.ToxicitySafe}
	comments := []string{"", "x", strings.Repeat("z", 4096), "short"}
	buf := make([]byte, 8192)
	for range 32 {
		buf = encodeRecordInto(buf, s)
	}
	// AllocsPerRun integer-divides its count and could hide a sparse allocation.
	// This nonparallel check observes the raw process-wide malloc total instead.
	measure := func(sparse bool) uint64 {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for i := range 2048 {
			s.Comment = comments[i%len(comments)]
			buf = encodeRecordInto(buf, s)
			encodingScratchByteWitness ^= buf[len(buf)-1]
			if sparse && i == 1024 {
				encodingScratchAllocationWitness = make([]byte, 1)
			}
		}
		runtime.ReadMemStats(&after)
		return after.Mallocs - before.Mallocs
	}
	if count := measure(false); count != 0 {
		t.Fatalf("warmed sufficient-capacity encoding allocated %d times", count)
	}
	if count := measure(true); count < 1 {
		t.Fatal("raw allocation observer missed a single escaping allocation")
	}
	encodingScratchAllocationWitness = nil
	runtime.KeepAlive(buf)
}

func TestEncodingScratchMixedFlushes(t *testing.T) {
	w := newCleanupTestWriter(t, 3600)
	defer w.Stop()

	comments := []string{"short", strings.Repeat("large ", 700), "x", "", strings.Repeat("medium", 80)}
	var sequence uint32
	for round := range 2 {
		batch := make([]*spot.Spot, len(comments)+1)
		before := make([]map[string]any, len(comments))
		for i, comment := range comments {
			s := ownedFlushInput()
			s.Comment = fmt.Sprintf("%d:%s", round, comment)
			if i%2 == 0 {
				s.EnsureNormalized()
			}
			batch[i] = s.SealForAsync()
			before[i] = ownedFlushPublicFields(s)
		}
		w.flush(batch)
		for i, s := range batch[:len(comments)] {
			sequence++
			if !reflect.DeepEqual(ownedFlushPublicFields(s), before[i]) {
				t.Fatalf("round %d row %d: immutable input changed", round, i)
			}
			want := ownedFlushRecord()
			want.comment = strings.TrimSpace(s.Comment)
			raw, closer, err := w.db.Get(spotKeyBytes(normalizeUnixNano(s.Time), sequence))
			if err != nil {
				t.Fatal(err)
			}
			equal := bytes.Equal(raw, ownedFlushExpectedBytes(want))
			if err := closer.Close(); err != nil {
				t.Fatal(err)
			}
			if !equal {
				t.Fatalf("round %d row %d: stored bytes differ", round, i)
			}
		}
	}
}
