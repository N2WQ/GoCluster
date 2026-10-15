package pathreliability

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
	"unsafe"
)

// This common S6/S7 harness intentionally names only S6 APIs and fields. The
// variant controls the layout oracle, never the workload or retained values.
const recentSupportUpdateBase int64 = 1_700_000_000

type recentSupportUpdateFixture struct {
	predictor                       *Predictor
	shape, mode, variant, inputHash string
	owners                          [2]*bucket
	extraOwner                      *bucketExtraState
	slotsOwner                      *[6]receiverSlot
	sidecars                        int
}

type recentSupportUpdateSlot struct {
	hash          uint64
	units         float64
	lastOperation int
}

type recentSupportUpdateWant struct {
	mass         float64
	fine, coarse [12]recentSupportUpdateSlot
}

// These are fixed outcomes for the only authorized operation counts, rather
// than a simulation of production admission or slot replacement. At 100000
// half-lives the aggregate rounds to 2. Coarse slots recur every 12 updates;
// their rounded fixed point is 4096/4095, shifted by final-touch age. Fine
// ownership rotates sooner and retains only the six latest unit increments.
func recentSupportUpdateExpected(tb testing.TB, shape string, operations int) recentSupportUpdateWant {
	tb.Helper()
	want := recentSupportUpdateWant{mass: 96}
	for i, hash := range [6]uint64{12, 2, 3, 4, 5, 6} {
		want.fine[i] = recentSupportUpdateSlot{hash, 8, 0}
	}
	for i := range want.coarse {
		want.coarse[i] = recentSupportUpdateSlot{uint64(i + 1), 8, 0}
	}
	if operations == 0 {
		return want
	}
	if operations != 1 && operations != 2 && operations != 100000 {
		tb.Fatalf("update control requires 1, 2, or 100000 operations; got %d", operations)
	}
	if shape == "same_reference_rejected" {
		want.mass += float64(operations)
		return want
	}
	if shape != "half_life_admitted" {
		tb.Fatalf("unknown update control shape %q", shape)
	}
	switch operations {
	case 1:
		want.mass = 49
		want.fine[0] = recentSupportUpdateSlot{1, 1, 1}
		for i := 1; i < 6; i++ {
			want.fine[i].units = 4
		}
		want.coarse[0] = recentSupportUpdateSlot{1, 5, 1}
		for i := 1; i < 12; i++ {
			want.coarse[i].units = 4
		}
	case 2:
		want.mass = 25.5
		want.fine[0], want.fine[1] = recentSupportUpdateSlot{1, .5, 1}, recentSupportUpdateSlot{2, 3, 2}
		for i := 2; i < 6; i++ {
			want.fine[i].units = 2
		}
		want.coarse[0], want.coarse[1] = recentSupportUpdateSlot{1, 2.5, 1}, recentSupportUpdateSlot{2, 3, 2}
		for i := 2; i < 12; i++ {
			want.coarse[i].units = 2
		}
	case 100000:
		want.mass = 2
		want.fine = [12]recentSupportUpdateSlot{
			{1, .125, 99997}, {2, .25, 99998}, {3, .5, 99999}, {4, 1, 100000}, {11, .03125, 99995}, {12, .0625, 99996},
		}
		want.coarse = [12]recentSupportUpdateSlot{
			{1, 0x1.001001001001p-3, 99997}, {2, 0x1.001001001001p-2, 99998},
			{3, 0x1.001001001001p-1, 99999}, {4, 0x1.001001001001p0, 100000},
			{5, 0x1.001001001001p-11, 99989}, {6, 0x1.001001001001p-10, 99990},
			{7, 0x1.001001001001p-9, 99991}, {8, 0x1.001001001001p-8, 99992},
			{9, 0x1.001001001001p-7, 99993}, {10, 0x1.001001001001p-6, 99994},
			{11, 0x1.001001001001p-5, 99995}, {12, 0x1.001001001001p-4, 99996},
		}
	}
	return want
}

func newRecentSupportUpdateFixture(tb testing.TB, shape, mode, variant string, operations int) *recentSupportUpdateFixture {
	tb.Helper()
	f := &recentSupportUpdateFixture{predictor: NewPredictor(populationBenchConfig(mode), []string{"20m"}), shape: shape, mode: mode, variant: variant}
	assertPopulationBenchConfig(tb, f.predictor.Config(), mode)
	if index, ok := f.predictor.combined.bandIndex.Lookup("20m"); !ok || index != 0 {
		tb.Fatal("update control requires literal 20m band index 0")
	}
	at := time.Unix(recentSupportUpdateBase, 0).UTC()
	var seedHashes [96]uint64
	for i := range seedHashes {
		hash := uint64(i/8 + 1) // Grouped eight-report seed, not round-robin.
		seedHashes[i] = hash
		a := f.predictor.UpdateWithReceiverHash(BucketCombined, 1, 2, 3, 4, "20m", -10.5, 1,
			ObservationTimes{ObservedAt: at, ArrivedAt: at}, false, hash)
		if a.Status != AdmissionAccepted || !a.EffectiveObservation.Equal(at) {
			tb.Fatalf("update control seed rejected or retimed: %+v", a)
		}
	}
	cycle := []uint64{12, 2, 3, 4, 5, 6}
	step := int64(0)
	if shape == "half_life_admitted" {
		cycle, step = []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, 360
	}
	input := struct {
		Schema, Shape, Mode   string
		Config                Config
		Path                  [4]CellID
		Band                  string
		SNR, Weight           float64
		BaseUnix, StepSeconds int64
		SeedHashes            [96]uint64
		UpdateHashCycle       []uint64
		Operations            int
	}{"s7-update-controls-v1", shape, mode, f.predictor.Config(), [4]CellID{1, 2, 3, 4}, "20m", -10.5, 1,
		recentSupportUpdateBase, step, seedHashes, cycle, operations}
	encoded, err := json.Marshal(input)
	if err != nil {
		tb.Fatal(err)
	}
	sum := sha256.Sum256(encoded)
	f.inputHash = hex.EncodeToString(sum[:])
	f.assertState(tb, 0)
	return f
}

func (f *recentSupportUpdateFixture) assertState(tb testing.TB, operations int) {
	tb.Helper()
	want := recentSupportUpdateExpected(tb, f.shape, operations)
	logical := uintptr(456)
	if f.variant == "baseline" {
		logical = 448
	}
	if unsafe.Sizeof(bucket{}) != logical {
		tb.Fatalf("update control bucket layout=%d want %d for %s", unsafe.Sizeof(bucket{}), logical, f.variant)
	}
	var seen uint8
	sidecars := 0
	for i := range f.predictor.combined.shards {
		sh := &f.predictor.combined.shards[i]
		func() {
			sh.mu.RLock()
			defer sh.mu.RUnlock()
			for key, b := range sh.buckets {
				index := -1
				switch key {
				case 0x100020000:
					index = 0
				case 0x30004:
					index = 1
				}
				if b == nil || index < 0 || seen&(1<<index) != 0 {
					tb.Fatalf("unexpected/missing/duplicate physical owner at %x", key)
				}
				seen |= 1 << index
				if operations == 0 {
					f.owners[index] = b
				}
				capped, histogram := want.mass, want.mass
				if f.shape == "same_reference_rejected" {
					capped = 96
					if f.mode == ReceiverContributionEnforce {
						histogram = 96
					}
				}
				if f.mode == ReceiverContributionOff {
					capped = 0
				}
				clock := recentSupportUpdateBase
				if f.shape == "half_life_admitted" {
					clock += 360 * int64(operations)
				}
				var bins snrHistogram
				bins[14] = float32(histogram)
				if b != f.owners[index] || b.count != uint32(96+operations) || b.weight != want.mass || b.cappedWeight != capped || b.cappedCount != capped || b.snrBins != bins || b.lastUpdate != clock || b.newestObservation != clock {
					tb.Fatalf("update control lost work, admission or exact state at %x: %+v", key, b)
				}
				wantExtra := index == 1 && f.mode != ReceiverContributionOff
				if (b.extraSlots != nil) != wantExtra || wantExtra && b.extraSlots.slots == nil {
					tb.Fatalf("update control sidecar presence changed at %x", key)
				}
				if wantExtra {
					sidecars++
					if operations == 0 {
						f.extraOwner, f.slotsOwner = b.extraSlots, b.extraSlots.slots
					}
					if b.extraSlots != f.extraOwner || b.extraSlots.slots != f.slotsOwner {
						tb.Fatal("update control replaced retained coarse sidecar owners")
					}
				}
				f.assertSlots(tb, b, index, want)
			}
		}()
	}
	wantSidecars := 1
	if f.mode == ReceiverContributionOff {
		wantSidecars = 0
	}
	if seen != 3 || sidecars != wantSidecars || f.predictor.AdmissionStats() != (AdmissionStats{}) {
		tb.Fatalf("update control owners/admissions changed: seen=%b sidecars=%d admissions=%+v", seen, sidecars, f.predictor.AdmissionStats())
	}
	f.sidecars = sidecars
}

func (f *recentSupportUpdateFixture) assertSlots(tb testing.TB, b *bucket, index int, want recentSupportUpdateWant) {
	tb.Helper()
	wanted := want.fine
	if index == 1 {
		wanted = want.coarse
	}
	for i, expected := range wanted {
		var actual receiverSlot
		if i < 6 {
			actual = b.slots[i]
		} else if b.extraSlots != nil && b.extraSlots.slots != nil {
			actual = b.extraSlots.slots[i-6]
		}
		var slot receiverSlot
		if f.mode != ReceiverContributionOff && expected.hash != 0 {
			clock := recentSupportUpdateBase
			if f.shape == "half_life_admitted" {
				clock += 360 * int64(expected.lastOperation)
			}
			slot = receiverSlot{hash: expected.hash, count: expected.units, weight: expected.units, lastUpdate: clock}
		}
		if actual != slot {
			tb.Fatalf("update control physical slot %d bucket %d=%+v want %+v", i, index, actual, slot)
		}
	}
}

func BenchmarkRecentSupportUpdateControls(b *testing.B) {
	variant := os.Getenv("GOCLUSTER_S7_SUPPORT_VARIANT")
	if variant == "" {
		b.Skip("requires controller-frozen GOCLUSTER_S7_SUPPORT_VARIANT=baseline or candidate")
	}
	if variant != "baseline" && variant != "candidate" {
		b.Fatalf("invalid recent-support update variant %q", variant)
	}
	for _, shape := range []string{"same_reference_rejected", "half_life_admitted"} {
		b.Run(shape, func(b *testing.B) {
			for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
				b.Run(mode, func(b *testing.B) {
					recentSupportUpdateExpected(b, shape, b.N)
					f := newRecentSupportUpdateFixture(b, shape, mode, variant, b.N)
					profileDir := os.Getenv("GOCLUSTER_S6_POPULATION_PROFILE_DIR")
					if profileDir != "" && !filepath.IsAbs(profileDir) {
						b.Fatal("recent-support profile directory must be absolute")
					}
					stopProfile := populationBenchStartCPU(b, profileDir)
					var last ReportAdmission
					completed := 0
					at := time.Unix(recentSupportUpdateBase, 0).UTC()
					cycle := [6]uint64{12, 2, 3, 4, 5, 6}
					var before, after runtime.MemStats
					b.ReportAllocs()
					runtime.ReadMemStats(&before)
					b.ResetTimer()
					if shape == "same_reference_rejected" {
						for i := range b.N {
							last = f.predictor.UpdateWithReceiverHash(BucketCombined, 1, 2, 3, 4, "20m", -10.5, 1,
								ObservationTimes{ObservedAt: at, ArrivedAt: at}, false, cycle[i%6])
							completed++
						}
					} else {
						for i := range b.N {
							at = time.Unix(recentSupportUpdateBase+360*int64(i+1), 0).UTC()
							last = f.predictor.UpdateWithReceiverHash(BucketCombined, 1, 2, 3, 4, "20m", -10.5, 1,
								ObservationTimes{ObservedAt: at, ArrivedAt: at}, false, uint64(i%12+1))
							completed++
						}
					}
					b.StopTimer()
					runtime.ReadMemStats(&after)
					allocationDelta := after.Mallocs - before.Mallocs
					stopProfile()
					if completed != b.N || last.Status != AdmissionAccepted || !last.EffectiveObservation.Equal(at) {
						b.Fatalf("incomplete/retimed update control: completed=%d N=%d last=%+v", completed, b.N, last)
					}
					f.assertState(b, completed)
					if profileDir == "" && b.N > 1 && allocationDelta != 0 {
						b.Fatalf("warmed update batch allocated: allocation_delta=%d operations=%d", allocationDelta, completed)
					}
					if profileDir != "" && b.N > 1 {
						f.writeLiveHeap(b, profileDir, completed)
					}
					want := recentSupportUpdateExpected(b, shape, completed)
					capped, histogram, fineTracked, coarseTracked := want.mass, want.mass, 6, 12
					if shape == "same_reference_rejected" {
						capped = 96
						if mode == ReceiverContributionEnforce {
							histogram = 96
						}
					}
					if mode == ReceiverContributionOff {
						capped, fineTracked, coarseTracked = 0, 0, 0
					}
					b.ReportMetric(float64(completed), "updates")
					b.Logf("S7_UPDATE_CONTROL shape=%s mode=%s variant=%s completed=%d elapsed_ns=%d allocation_delta=%d seed_reports=96 physical_buckets=2 receiver_sidecars=%d input_sha256=%s count=%d raw_weight=%g capped_count=%g capped_weight=%g histogram_mass=%g fine_tracked=%d coarse_tracked=%d query_unix=%d logical_bucket_bytes=%d",
						shape, mode, variant, completed, b.Elapsed().Nanoseconds(), allocationDelta, f.sidecars, f.inputHash, 96+completed, want.mass, capped, capped, histogram, fineTracked, coarseTracked, at.Unix(), unsafe.Sizeof(bucket{}))
				})
			}
		})
	}
}

// Heap capture follows the stopped/joined CPU profiler and two GC epochs.
// The full fixture remains reachable through both the profile and memory record.
func (f *recentSupportUpdateFixture) writeLiveHeap(b *testing.B, directory string, operations int) {
	b.Helper()
	runtime.GC()
	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	output, err := os.Create(filepath.Join(directory, "heap.pprof"))
	if err != nil {
		b.Fatal(err)
	}
	writeErr := pprof.WriteHeapProfile(output)
	closeErr := output.Close()
	if writeErr != nil {
		b.Fatal(writeErr)
	}
	if closeErr != nil {
		b.Fatal(closeErr)
	}
	record := struct {
		Buckets, Sidecars, Operations      int
		HeapAlloc, HeapObjects, StackInuse uint64
		StoreKeptAlive                     bool
	}{2, f.sidecars, operations, memory.HeapAlloc, memory.HeapObjects, memory.StackInuse, true}
	encoded, err := json.Marshal(record)
	if err != nil {
		b.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(directory, "live-heap.json"), encoded, 0600); err != nil {
		b.Fatal(err)
	}
	runtime.KeepAlive(f)
}
