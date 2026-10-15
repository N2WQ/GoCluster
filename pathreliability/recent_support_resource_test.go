package pathreliability

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
	"unsafe"
)

const (
	recentResourceBucketCount = 65536
	recentResourcePairCount   = 8192
	recentResourceUnix        = 1700000000
)

type recentResourcePath struct {
	RX, TX, CoarseRX, CoarseTX CellID
	FineKey, CoarseKey         uint64
}

type recentResourceOwners struct {
	buckets []*bucket
	store   *Store
	warm    *Store
	paths   []recentResourcePath
	at      time.Time
	reports int
	ops     int
	bad     int
}

type recentResourceStats struct {
	HeapAlloc, HeapObjects, Mallocs, Frees, TotalAlloc uint64
}

type recentResourceDelta struct {
	HeapAlloc, HeapObjects, Mallocs, Frees, TotalAlloc int64
}

type recentResourceClass struct {
	Size                               uint32
	StartMallocs, StartFrees           uint64
	SeedMallocs, SeedFrees             uint64
	PostGCMallocs, PostGCFrees         uint64
	SeedMallocDelta, SeedFreeDelta     int64
	PostGCMallocDelta, PostGCFreeDelta int64
}

type recentResourceReceipt struct {
	Schema, Case, Mode, Variant, InputSHA256 string
	LogicalBucketBytes                       uintptr
	ExpectedAllocatorBytes                   uint32
	ObservedAllocatorBytes                   uint32
	SidecarOwnerBytes, SidecarArrayBytes     uintptr
	Buckets, FineBuckets, CoarseBuckets      int
	Sidecars, Reports, PreexistingMapKeys    int
	Operations, WarmBuckets, WarmSidecars    int
	ObservedBuckets, ObservedSidecarOwners   int
	ObservedSidecarArrays                    int
	ExpectedAllocations, ExpectedHeapBytes   uint64
	PreviousGCPercent                        int
	AutoGCDisabled, AllKeysPresentBefore     bool
	Start, Seed, PostGC                      recentResourceStats
	SeedDelta, PostGCDelta                   recentResourceDelta
	RuntimeResidual                          recentResourceDelta
	Classes                                  []recentResourceClass
	HeapProfile                              string `json:",omitempty"`
	ValidationError                          string `json:",omitempty"`
}

// TestRecentSupportResourceFixture is shared unchanged by the S6/S7 builds.
// Pointer slices, nil-valued map backing storage, warm state and measurement
// buffers are owned before the base GC. Only separately retained buckets and
// coarse sidecars fund the resource gate. V6 charges all candidate TotalAlloc
// bytes, including temporary/runtime objects. Only newly owned baseline bytes
// may be subtracted. Warm owners and freed objects never subsidize the bound.
func TestRecentSupportResourceFixture(t *testing.T) {
	caseName := os.Getenv("GOCLUSTER_S7_RESOURCE_CASE")
	if caseName == "" {
		t.Skip("recent-support resource fixture requires GOCLUSTER_S7_RESOURCE_CASE")
	}
	mode, variant := os.Getenv("GOCLUSTER_S7_RESOURCE_MODE"), os.Getenv("GOCLUSTER_S7_RESOURCE_VARIANT")
	if caseName == "buckets" && mode == "" {
		mode = ReceiverContributionOff
	}
	if mode != ReceiverContributionOff && mode != ReceiverContributionShadow && mode != ReceiverContributionEnforce {
		t.Fatal("resource mode must be off, shadow or enforce")
	}
	wantLogical, wantAllocator := uintptr(448), uint32(448)
	if variant == "candidate" {
		wantLogical, wantAllocator = 456, 480
	} else if variant != "baseline" {
		t.Fatal("resource variant must be baseline or candidate")
	}
	if unsafe.Sizeof(bucket{}) != wantLogical {
		t.Fatalf("logical bucket size=%d, want %d for %s", unsafe.Sizeof(bucket{}), wantLogical, variant)
	}
	receivers := 0
	switch caseName {
	case "buckets":
	case "store6":
		receivers = 6
	case "store7":
		receivers = 7
	case "store12":
		receivers = 12
	default:
		t.Fatal("resource case must be buckets, store6, store7 or store12")
	}
	profileDir := recentResourceProfileDirectory(t, os.Getenv("GOCLUSTER_S7_RESOURCE_PROFILE_DIR"))
	owners, inputSHA := recentResourcePrepare(t, caseName, mode, receivers)
	receipt := &recentResourceReceipt{
		Schema: "gocluster.recent-support-resource.v3", Case: caseName, Mode: mode, Variant: variant, InputSHA256: inputSHA,
		LogicalBucketBytes: wantLogical, ExpectedAllocatorBytes: wantAllocator,
		SidecarOwnerBytes: unsafe.Sizeof(bucketExtraState{}), SidecarArrayBytes: unsafe.Sizeof([6]receiverSlot{}),
		Buckets: recentResourceBucketCount, Classes: make([]recentResourceClass, len(runtime.MemStats{}.BySize)),
		AutoGCDisabled: true, AllKeysPresentBefore: true, WarmBuckets: 2,
	}
	if mode != ReceiverContributionOff {
		receipt.WarmSidecars = 1
	}
	if receivers != 0 {
		receipt.FineBuckets, receipt.CoarseBuckets = recentResourcePairCount, recentResourcePairCount
		receipt.Buckets, receipt.PreexistingMapKeys = 2*recentResourcePairCount, 2*recentResourcePairCount
		if mode != ReceiverContributionOff && receivers > 6 {
			receipt.Sidecars = recentResourcePairCount
		}
	}
	receipt.ExpectedAllocations = uint64(receipt.Buckets + 2*receipt.Sidecars)
	receipt.ExpectedHeapBytes = uint64(receipt.Buckets)*uint64(wantAllocator) + uint64(receipt.Sidecars)*(8+192)
	if receipt.SidecarOwnerBytes != 8 || receipt.SidecarArrayBytes != 192 {
		t.Fatal("the common bounded sidecar layout changed")
	}
	// Disable only this fixture process's automatic GC; explicit boundary GCs
	// still run. Both builds use this same control, restored even on failure.
	receipt.PreviousGCPercent = debug.SetGCPercent(-1)
	t.Cleanup(func() { debug.SetGCPercent(receipt.PreviousGCPercent) })
	var before, seeded, after runtime.MemStats
	runtime.ReadMemStats(&before) // Prewarm the accounting API before the bracket.
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&before)
	recentResourceSeed(owners, receivers)
	runtime.ReadMemStats(&seeded)
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(owners)

	receipt.Reports, receipt.Operations = owners.reports, owners.ops
	receipt.Start, receipt.Seed, receipt.PostGC = recentResourceSnapshot(&before), recentResourceSnapshot(&seeded), recentResourceSnapshot(&after)
	receipt.SeedDelta, receipt.PostGCDelta = recentResourceDifference(&seeded, &before), recentResourceDifference(&after, &before)
	for i, start := range before.BySize {
		seed, post := seeded.BySize[i], after.BySize[i]
		receipt.Classes[i] = recentResourceClass{start.Size, start.Mallocs, start.Frees, seed.Mallocs, seed.Frees, post.Mallocs, post.Frees,
			int64(seed.Mallocs) - int64(start.Mallocs), int64(seed.Frees) - int64(start.Frees), int64(post.Mallocs) - int64(start.Mallocs), int64(post.Frees) - int64(start.Frees)}
	}
	// These frames and their temporary validation maps return before profiling.
	validation := recentResourceValidate(owners, receipt, receivers, mode)
	if validation == nil {
		validation = recentResourceValidateAccounting(receipt)
	}
	if validation != nil {
		receipt.ValidationError = validation.Error()
	}
	if profileDir != "" {
		receipt.HeapProfile = filepath.Join(profileDir, "heap.pprof")
		recentResourceWriteHeap(t, owners, receipt.HeapProfile)
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("S7_RESOURCE_CONTROL %s", encoded)
	runtime.KeepAlive(owners)
	if validation != nil {
		t.Fatal(validation)
	}
}

func recentResourcePrepare(t *testing.T, caseName, mode string, receivers int) (*recentResourceOwners, string) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = mode
	cfg.ReceiverFineSlots, cfg.ReceiverCoarseSlots = 6, 12
	cfg.ReceiverMaxEffectiveCount, cfg.ReceiverMaxEffectiveWeight = 8, 8
	cfg.BandHalfLifeSec = map[string]int{"20m": 360}
	f := &recentResourceOwners{at: time.Unix(recentResourceUnix, 0).UTC(), warm: NewStore(cfg, []string{"20m"})}
	for hash := uint64(1); hash <= 12; hash++ {
		if !f.warm.UpdateWithReceiverHash(1, 2, 3, 4, "20m", -10.5, 1, ObservationTimes{ObservedAt: f.at, ArrivedAt: f.at}, hash).Accepted() {
			t.Fatal("resource API prewarm rejected a fresh unit report")
		}
	}
	f.warm.Lookup(1, 2, 3, 4, "20m", f.at)
	if receivers == 0 {
		f.buckets = make([]*bucket, recentResourceBucketCount)
	} else {
		f.store = NewStore(cfg, []string{"20m"})
		if band, ok := f.store.bandIndex.Lookup("20m"); !ok || band != 0 {
			t.Fatal("resource store requires literal band index zero")
		}
		f.paths = make([]recentResourcePath, recentResourcePairCount)
		for i := range f.paths {
			p := &f.paths[i]
			p.RX, p.TX, p.CoarseRX, p.CoarseTX = CellID(i+1), CellID(i+10001), CellID(i+20001), CellID(i+40001)
			p.FineKey, p.CoarseKey = uint64(p.RX)<<32|uint64(p.TX)<<16, uint64(p.CoarseRX)<<16|uint64(p.CoarseTX)
			for _, key := range [2]uint64{p.FineKey, p.CoarseKey} {
				sh := f.store.shardForKey(key)
				if _, exists := sh.buckets[key]; exists {
					t.Fatal("resource literal key collision")
				}
				sh.buckets[key] = nil
			}
		}
		for i := range f.store.shards {
			sh := &f.store.shards[i]
			sh.peak = len(sh.buckets)
		}
		for _, p := range f.paths {
			for _, key := range [2]uint64{p.FineKey, p.CoarseKey} {
				value, exists := f.store.shardForKey(key).buckets[key]
				if !exists || value != nil {
					t.Fatal("resource bracket must overwrite present nil-valued keys")
				}
			}
		}
	}
	bucketCount := recentResourceBucketCount
	if receivers != 0 {
		bucketCount = 2 * recentResourcePairCount
	}
	input := struct {
		Schema, Case, Mode, Band, HashOrder string
		Unix                                int64
		Buckets, Pairs, Receivers           int
		SNR, Weight                         float64
		Config                              Config
		Paths                               []recentResourcePath
	}{"s7-resource-input-v1", caseName, mode, "20m", "ascending hashes 1..R per pair", recentResourceUnix,
		bucketCount, len(f.paths), receivers, -10.5, 1, f.warm.cfg, f.paths}
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(encoded)
	return f, hex.EncodeToString(sum[:])
}

//go:noinline
func recentResourceSeed(f *recentResourceOwners, receivers int) {
	if receivers == 0 {
		for i := range f.buckets {
			f.buckets[i] = new(bucket)
			f.ops++
		}
		return
	}
	for _, p := range f.paths {
		for hash := 1; hash <= receivers; hash++ {
			admission := f.store.UpdateWithReceiverHash(p.RX, p.TX, p.CoarseRX, p.CoarseTX, "20m", -10.5, 1,
				ObservationTimes{ObservedAt: f.at, ArrivedAt: f.at}, uint64(hash))
			f.reports++
			f.ops++
			if admission.Status != AdmissionAccepted || !admission.EffectiveObservation.Equal(f.at) {
				f.bad++
			}
		}
	}
}

func recentResourceSnapshot(s *runtime.MemStats) recentResourceStats {
	return recentResourceStats{s.HeapAlloc, s.HeapObjects, s.Mallocs, s.Frees, s.TotalAlloc}
}

func recentResourceDifference(after, before *runtime.MemStats) recentResourceDelta {
	return recentResourceStatsDifference(recentResourceSnapshot(after), recentResourceSnapshot(before))
}

//go:noinline
func recentResourceValidate(f *recentResourceOwners, r *recentResourceReceipt, receivers int, mode string) error {
	seen := make(map[*bucket]bool, r.Buckets)
	if receivers == 0 {
		for _, b := range f.buckets {
			if b == nil || seen[b] || *b != (bucket{}) {
				return fmt.Errorf("separate bucket owner is absent, duplicated or nonzero")
			}
			seen[b] = true
		}
		r.ObservedBuckets = len(seen)
		if f.ops != recentResourceBucketCount || f.reports != 0 {
			return fmt.Errorf("separate bucket operation count differs: ops=%d reports=%d", f.ops, f.reports)
		}
		return nil
	}
	if f.bad != 0 || f.reports != recentResourcePairCount*receivers || f.ops != f.reports || f.store.admissions.snapshot() != (AdmissionStats{}) {
		return fmt.Errorf("resource report work differs: completed=%d bad=%d", f.reports, f.bad)
	}
	sideOwners, sideArrays := make(map[*bucketExtraState]bool), make(map[*[6]receiverSlot]bool)
	entries := 0
	for i := range f.store.shards {
		sh := &f.store.shards[i]
		if sh.peak != len(sh.buckets) {
			return fmt.Errorf("preexisting shard cardinality/peak changed")
		}
		entries += len(sh.buckets)
	}
	for _, p := range f.paths {
		for direction, key := range [2]uint64{p.FineKey, p.CoarseKey} {
			b, exists := f.store.shardForKey(key).buckets[key]
			if !exists || b == nil || seen[b] {
				return fmt.Errorf("missing or aliased resource key %x", key)
			}
			seen[b] = true
			var bins snrHistogram
			bins[14] = float32(receivers)
			capped := float64(receivers)
			if mode == ReceiverContributionOff {
				capped = 0
			}
			if b.count != uint32(receivers) || b.weight != float64(receivers) || b.cappedCount != capped || b.cappedWeight != capped || b.snrBins != bins || b.lastUpdate != recentResourceUnix || b.newestObservation != recentResourceUnix {
				return fmt.Errorf("resource key %x has incorrect report/histogram mass", key)
			}
			wantSide := mode != ReceiverContributionOff && direction == 1 && receivers > 6
			if (b.extraSlots != nil) != wantSide {
				return fmt.Errorf("resource key %x has incorrect sidecar ownership", key)
			}
			if wantSide {
				if b.extraSlots.slots == nil || sideOwners[b.extraSlots] || sideArrays[b.extraSlots.slots] {
					return fmt.Errorf("resource sidecar missing or aliased")
				}
				sideOwners[b.extraSlots], sideArrays[b.extraSlots.slots] = true, true
			}
			for i := 0; i < 12; i++ {
				var got receiverSlot
				if slot := b.receiverSlotAt(i, false); slot != nil {
					got = *slot
				}
				var want receiverSlot
				if mode != ReceiverContributionOff && i < receivers && (direction == 1 || i < 6) {
					hash := uint64(i + 1)
					if direction == 0 && i == 0 && receivers > 6 {
						hash = uint64(receivers)
					}
					want = receiverSlot{hash: hash, weight: 1, count: 1, lastUpdate: recentResourceUnix}
				}
				if got != want {
					return fmt.Errorf("resource key %x slot %d: got=%+v want=%+v", key, i, got, want)
				}
			}
		}
	}
	r.ObservedBuckets, r.ObservedSidecarOwners, r.ObservedSidecarArrays = len(seen), len(sideOwners), len(sideArrays)
	if entries != r.PreexistingMapKeys || len(seen) != r.Buckets || len(sideOwners) != r.Sidecars || len(sideArrays) != r.Sidecars {
		return fmt.Errorf("resource cardinality differs: keys=%d buckets=%d owners=%d arrays=%d", entries, len(seen), len(sideOwners), len(sideArrays))
	}
	return nil
}

func recentResourceProfileDirectory(t *testing.T, directory string) string {
	t.Helper()
	if directory == "" {
		return ""
	}
	approved := filepath.Clean(`C:\src\gocluster\.tmp\path-reliability-first-wave`)
	abs, err := filepath.Abs(directory)
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(approved, abs)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		t.Fatal("resource profile directory must be inside the approved .tmp artifact root")
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil || !strings.EqualFold(filepath.Clean(resolved), filepath.Clean(abs)) {
		t.Fatal("resource profile directory must exist without symlinks or junctions")
	}
	return abs
}

func recentResourceWriteHeap(t *testing.T, owners *recentResourceOwners, path string) {
	t.Helper()
	runtime.GC()
	runtime.GC()
	output, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	writeErr, closeErr := pprof.WriteHeapProfile(output), output.Close()
	runtime.KeepAlive(owners)
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
}
