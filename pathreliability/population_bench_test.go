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

// This common S5/S6 harness uses only APIs and retained fields present in S5.
// Cell IDs are declared synthetic identities, with consistent fine/coarse
// associations; geographic mapping is covered by the separate S6 H3 tests.
// All reports are fresh unit contributions in [-11,-10). A 6 dB RX penalty
// puts the receive representative at -16.5; the legacy .6 receive majority
// and the selected receive-only hints therefore both have that literal p50.
type populationBenchPath struct {
	Receiver, Sender, ReceiverCoarse, SenderCoarse CellID
}

type populationBenchReport struct {
	Path populationBenchPath
	Hash uint64
}

type populationBenchWant struct {
	count, receivers uint32
	weight           float64
}

type populationBenchControl struct {
	name                string
	beacon              bool
	baseline, candidate populationBenchWant
}

func populationBenchControls() []populationBenchControl {
	return []populationBenchControl{
		{"ordinary_all_aliases", false, populationBenchWant{96, 12, 48}, populationBenchWant{48, 6, 24}},
		{"ordinary_shared_coarse_distinct_fine", false, populationBenchWant{64, 24, 32}, populationBenchWant{32, 12, 16}},
		{"ordinary_distinct_fine_pair", false, populationBenchWant{96, 12, 48}, populationBenchWant{96, 12, 48}},
		{"ordinary_coarse_12x12_disjoint", false, populationBenchWant{192, 24, 96}, populationBenchWant{192, 24, 96}},
		{"ordinary_coarse_12x12_shared_hashes", false, populationBenchWant{192, 24, 96}, populationBenchWant{192, 12, 96}},
		{"beacon_fine", true, populationBenchWant{48, 6, 48}, populationBenchWant{48, 6, 48}},
		{"beacon_coarse", true, populationBenchWant{96, 12, 96}, populationBenchWant{96, 12, 96}},
	}
}

type populationBenchBucket struct {
	count     uint32
	receivers map[uint64]uint32
	owner     *bucket
}

type populationBenchFixture struct {
	predictor *Predictor
	query     populationBenchPath
	at        time.Time
	control   populationBenchControl
	mode      string
	want      Result
	reports   []populationBenchReport
	buckets   map[uint64]*populationBenchBucket
	inputHash string
	sidecars  int
}

func populationBenchConfig(mode string) Config {
	cfg := DefaultConfig()
	cfg.AllowedBands = []string{"20m"}
	cfg.DefaultHalfLifeSec, cfg.BandHalfLifeSec = 360, map[string]int{"20m": 360}
	cfg.MaxPredictionAgeHalfLifeMultiplier = 1.5
	cfg.MinObservationCount, cfg.BeaconMinObservationCount = 21, 11
	cfg.ReceiverContributionMode = mode
	cfg.ReceiverFineSlots, cfg.ReceiverCoarseSlots = 6, 12
	cfg.ReceiverMaxEffectiveCount, cfg.ReceiverMaxEffectiveWeight = 8, 8
	cfg.MinFineWeight, cfg.FineOnlyWeight = 5, 20
	cfg.MergeReceiveWeight, cfg.MergeTransmitWeight, cfg.ReverseHintDiscount = .6, .4, .5
	cfg.MinEffectiveWeight, cfg.BeaconWeightCap = .5, 1
	cfg.GlyphSymbols = GlyphSymbols{High: ">", Medium: "=", Low: "<", Unlikely: "-", Insufficient: " ", Closed: "#"}
	cfg.ModeThresholds["FT8"] = GlyphThresholds{High: -13, Medium: -17, Low: -21, Unlikely: -24, Closed: -29, hasHigh: true, hasMedium: true, hasLow: true, hasUnlikely: true, hasClosed: true}
	cfg.VOACAPFallback.Enabled, cfg.Native160Fallback.Enabled = false, false
	return cfg
}

func assertPopulationBenchConfig(tb testing.TB, cfg Config, mode string) {
	tb.Helper()
	threshold := cfg.ModeThresholds["FT8"]
	if !cfg.Enabled || cfg.ReceiverContributionMode != mode || cfg.MinObservationCount != 21 || cfg.BeaconMinObservationCount != 11 ||
		cfg.ReceiverFineSlots != 6 || cfg.ReceiverCoarseSlots != 12 || cfg.ReceiverMaxEffectiveCount != 8 || cfg.ReceiverMaxEffectiveWeight != 8 ||
		cfg.MinFineWeight != 5 || cfg.FineOnlyWeight != 20 || cfg.MergeReceiveWeight != .6 || cfg.MergeTransmitWeight != .4 ||
		cfg.ReverseHintDiscount != .5 || cfg.MinEffectiveWeight != .5 || cfg.BeaconWeightCap != 1 ||
		threshold.High != -13 || threshold.Medium != -17 || threshold.Low != -21 || cfg.GlyphSymbols.Medium != "=" ||
		cfg.BandHalfLifeSec["20m"] != 360 || cfg.MaxPredictionAgeHalfLifeMultiplier != 1.5 || cfg.VOACAPFallback.Enabled || cfg.Native160Fallback.Enabled {
		tb.Fatal("population benchmark configuration drifted from its frozen shipped-policy controls")
	}
}

func newPopulationBenchFixture(tb testing.TB, control populationBenchControl, mode, variant string) *populationBenchFixture {
	tb.Helper()
	f := &populationBenchFixture{predictor: NewPredictor(populationBenchConfig(mode), []string{"20m"}), query: populationBenchPath{1, 2, 3, 4},
		at: time.Unix(1_700_000_000, 0).UTC(), control: control, mode: mode, buckets: make(map[uint64]*populationBenchBucket)}
	assertPopulationBenchConfig(tb, f.predictor.Config(), mode)
	if band, ok := f.predictor.combined.bandIndex.Lookup("20m"); !ok || band != 0 {
		tb.Fatal("single-band control requires the literal 20m index zero")
	}
	switch control.name {
	case "ordinary_all_aliases":
		f.query = populationBenchPath{1, 1, 3, 3}
		f.seedDirection(tb, 0, 6, 48, false, false)
	case "ordinary_shared_coarse_distinct_fine":
		f.query.ReceiverCoarse, f.query.SenderCoarse = 3, 3
		f.seedDirection(tb, 0, 6, 16, false, false)
		f.seedDirection(tb, 1, 6, 16, false, false)
	case "ordinary_distinct_fine_pair":
		f.seedDirection(tb, 0, 6, 48, false, false)
		f.seedDirection(tb, 1, 6, 48, false, false)
	case "ordinary_coarse_12x12_disjoint", "ordinary_coarse_12x12_shared_hashes":
		shared := control.name == "ordinary_coarse_12x12_shared_hashes"
		f.seedDirection(tb, 0, 12, 96, true, shared)
		f.seedDirection(tb, 1, 12, 96, true, shared)
	case "beacon_fine":
		f.seedDirection(tb, 0, 6, 48, false, false)
	case "beacon_coarse":
		f.seedDirection(tb, 0, 12, 96, true, false)
	default:
		tb.Fatalf("unknown frozen population shape %q", control.name)
	}
	counts := control.baseline
	switch variant {
	case "baseline":
	case "candidate":
		counts = control.candidate
	default:
		tb.Fatalf("population benchmark requires explicit baseline/candidate variant, got %q", variant)
	}
	required := uint32(3)
	if control.beacon {
		required = 2
	}
	if mode == ReceiverContributionOff {
		counts.receivers, required = 0, 0
	}
	f.want = Result{Glyph: "=", Class: "MEDIUM", P50DB: -16.5, HasP50: true, P50Glyph: "=", ObservedSamplingUnix: f.at.Unix(),
		HasObservedSamplingTime: true, ObservedDistributionAvailable: true, Weight: counts.weight, Count: counts.count,
		ObservationCount: counts.count, RawCount: counts.count, RawWeight: counts.weight, CappedCount: counts.count, CappedWeight: counts.weight,
		ReceiverCount: counts.receivers, ReceiverRequired: required, Source: SourceCombined, BeaconRX: control.beacon}
	input := struct {
		Schema, Shape, Mode string
		Query               populationBenchPath
		Unix                int64
		Config              Config
		Reports             []populationBenchReport
	}{"s6-population-controls-v1", control.name, mode, f.query, f.at.Unix(), f.predictor.Config(), f.reports}
	encoded, err := json.Marshal(input)
	if err != nil {
		tb.Fatal(err)
	}
	sum := sha256.Sum256(encoded)
	f.inputHash = hex.EncodeToString(sum[:])
	f.assertState(tb)
	f.assertPrediction(tb, f.predict())
	return f
}

// Coarse controls route exactly 16 reports through the queried fine pair:
// 3,3,3,3,2,2 among six hashes. Other reports use separate fine pairs with the
// same coarse endpoints. Each coarse hash has eight units, so no cap rejects,
// fine-slot replacement or fine/coarse report duplication can fund an oracle.
func (f *populationBenchFixture) seedDirection(tb testing.TB, direction, receivers, reports int, coarse, shared bool) {
	tb.Helper()
	path := f.query
	if direction == 1 {
		path.Receiver, path.Sender = path.Sender, path.Receiver
		path.ReceiverCoarse, path.SenderCoarse = path.SenderCoarse, path.ReceiverCoarse
	}
	for i := 0; i < reports; i++ {
		hashIndex := i % receivers
		if shared && direction == 1 {
			hashIndex = receivers - 1 - hashIndex
		}
		hashDirection := direction
		if shared {
			hashDirection = 0
		}
		hash := uint64(0x5300000000000000) | uint64(hashDirection)<<16 | uint64(hashIndex+1)
		reportPath := path
		if coarse {
			local := [12]int{3, 3, 3, 3, 2, 2}
			if i/receivers >= local[hashIndex] {
				reportPath.Receiver = CellID(1000 + 100*direction + hashIndex)
				reportPath.Sender = CellID(2000 + 100*direction + hashIndex)
			}
		}
		admission := f.predictor.UpdateWithReceiverHash(BucketCombined, reportPath.Receiver, reportPath.Sender, reportPath.ReceiverCoarse, reportPath.SenderCoarse,
			"20m", -10.5, 1, ObservationTimes{ObservedAt: f.at, ArrivedAt: f.at}, f.control.beacon, hash)
		if admission.Status != AdmissionAccepted || !admission.EffectiveObservation.Equal(f.at) {
			tb.Fatalf("fresh unit control report rejected: %+v", admission)
		}
		f.reports = append(f.reports, populationBenchReport{reportPath, hash})
		// Literal unchanged band-zero key layout, independent of production
		// packing and shard selection. Each physical report is supplied once.
		keys := [2]uint64{uint64(reportPath.Receiver)<<32 | uint64(reportPath.Sender)<<16, uint64(reportPath.ReceiverCoarse)<<16 | uint64(reportPath.SenderCoarse)}
		for _, key := range keys {
			want := f.buckets[key]
			if want == nil {
				want = &populationBenchBucket{receivers: make(map[uint64]uint32)}
				f.buckets[key] = want
			}
			want.count++
			want.receivers[hash]++
			if want.receivers[hash] > 8 {
				tb.Fatal("control report would exceed its per-bucket receiver cap")
			}
		}
	}
}

func (f *populationBenchFixture) predict() Result {
	p := f.query
	if f.control.beacon {
		return f.predictor.PredictBeaconReceiveOnlyWithMinObservationCount(p.Receiver, p.Sender, p.ReceiverCoarse, p.SenderCoarse, "20m", "FT8", 6, 11, f.at)
	}
	return f.predictor.PredictWithMinObservationCount(p.Receiver, p.Sender, p.ReceiverCoarse, p.SenderCoarse, "20m", "FT8", 6, 21, f.at)
}

func (f *populationBenchFixture) assertPrediction(tb testing.TB, got Result) {
	tb.Helper()
	if got != f.want {
		tb.Fatalf("population %s/%s output differs from literal funded-work oracle:\ngot  %+v\nwant %+v", f.control.name, f.mode, got, f.want)
	}
}

func (f *populationBenchFixture) assertState(tb testing.TB) {
	tb.Helper()
	if unsafe.Sizeof(bucket{}) != 448 {
		tb.Fatalf("S6 retained logical bucket size=%d, want 448", unsafe.Sizeof(bucket{}))
	}
	seen, sidecars := 0, 0
	for i := range f.predictor.combined.shards {
		sh := &f.predictor.combined.shards[i]
		func() {
			sh.mu.RLock()
			defer sh.mu.RUnlock()
			for key, b := range sh.buckets {
				want := f.buckets[key]
				if b == nil || want == nil {
					tb.Fatalf("unexpected retained population key %x", key)
				}
				if want.owner == nil {
					want.owner = b
				}
				var bins snrHistogram
				bins[14] = float32(want.count) // [-11,-10), exact fresh unit mass.
				capped := float64(want.count)
				if f.mode == ReceiverContributionOff {
					capped = 0 // No capped measurement or slot ownership in off.
				}
				if b != want.owner || b.count != want.count || b.weight != float64(want.count) || b.cappedCount != capped || b.cappedWeight != capped ||
					b.lastUpdate != f.at.Unix() || b.newestObservation != f.at.Unix() || b.snrBins != bins {
					tb.Fatalf("retained population %x changed or lost funded unit mass: %+v", key, b)
				}
				actual := make(map[uint64]uint32)
				for slotIndex := 0; slotIndex < 12; slotIndex++ {
					var slot receiverSlot
					if slotIndex < 6 {
						slot = b.slots[slotIndex]
					} else if b.extraSlots != nil && b.extraSlots.slots != nil {
						slot = b.extraSlots.slots[slotIndex-6]
					}
					if slot.hash == 0 {
						if slot != (receiverSlot{}) {
							tb.Fatal("empty receiver slot retains unexpected values")
						}
						continue
					}
					n := want.receivers[slot.hash]
					if actual[slot.hash] != 0 || n == 0 || slot.count != float64(n) || slot.weight != float64(n) || slot.lastUpdate != f.at.Unix() {
						tb.Fatalf("retained receiver mass/identity differs at key %x: %+v", key, slot)
					}
					actual[slot.hash] = n
				}
				wantReceivers := len(want.receivers)
				if f.mode == ReceiverContributionOff {
					wantReceivers = 0
				}
				if len(actual) != wantReceivers {
					tb.Fatalf("retained receiver identities=%d, want %d for key %x", len(actual), wantReceivers, key)
				}
				wantExtra := wantReceivers > 6
				if (b.extraSlots != nil) != wantExtra || (wantExtra && b.extraSlots.slots == nil) {
					tb.Fatalf("coarse sidecar ownership differs for key %x", key)
				}
				if wantExtra {
					sidecars++
				}
				seen++
			}
		}()
	}
	if seen != len(f.buckets) || f.predictor.AdmissionStats() != (AdmissionStats{}) {
		tb.Fatalf("population owners/admissions changed: buckets=%d want=%d admissions=%+v", seen, len(f.buckets), f.predictor.AdmissionStats())
	}
	f.sidecars = sidecars
}

// Controller qualification requires the explicit build role and exact 100000x
// final result. Standard B.run1's N=1 and a separate N=2 schema preflight stay
// recorded but cannot be accepted as qualification measurements.
func BenchmarkPopulationSelectionControls(b *testing.B) {
	variant := os.Getenv("GOCLUSTER_S6_POPULATION_VARIANT")
	if variant == "" {
		b.Skip("requires controller-frozen GOCLUSTER_S6_POPULATION_VARIANT=baseline or candidate")
	}
	if variant != "baseline" && variant != "candidate" {
		b.Fatalf("invalid population benchmark variant %q", variant)
	}
	for _, control := range populationBenchControls() {
		b.Run(control.name, func(b *testing.B) {
			for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
				b.Run(mode, func(b *testing.B) {
					f := newPopulationBenchFixture(b, control, mode, variant)
					var result Result
					completed := 0
					p := f.query
					profileDir := os.Getenv("GOCLUSTER_S6_POPULATION_PROFILE_DIR")
					stopProfile := populationBenchStartCPU(b, profileDir)
					b.ReportAllocs()
					b.ResetTimer()
					if control.beacon {
						for range b.N {
							result = f.predictor.PredictBeaconReceiveOnlyWithMinObservationCount(p.Receiver, p.Sender, p.ReceiverCoarse, p.SenderCoarse, "20m", "FT8", 6, 11, f.at)
							completed++
						}
					} else {
						for range b.N {
							result = f.predictor.PredictWithMinObservationCount(p.Receiver, p.Sender, p.ReceiverCoarse, p.SenderCoarse, "20m", "FT8", 6, 21, f.at)
							completed++
						}
					}
					b.StopTimer()
					stopProfile()
					if completed != b.N || completed <= 0 {
						b.Fatalf("incomplete population control: completed=%d N=%d", completed, b.N)
					}
					f.assertPrediction(b, result)
					f.assertPrediction(b, f.predict())
					f.assertState(b)
					if profileDir != "" && b.N > 1 {
						f.writeLiveHeap(b, profileDir)
					}
					b.ReportMetric(float64(completed), "predictions")
					b.Logf("S6_POPULATION_CONTROL shape=%s mode=%s variant=%s beacon=%t query_unix=%d completed=%d elapsed_ns=%d seed_reports=%d physical_buckets=%d receiver_sidecars=%d input_sha256=%s count=%d weight=%g receivers=%d required=%d p50=-16.5 class=MEDIUM glyph== logical_bucket_bytes=%d",
						control.name, mode, variant, control.beacon, f.at.Unix(), completed, b.Elapsed().Nanoseconds(), len(f.reports), len(f.buckets), f.sidecars, f.inputHash,
						result.Count, result.Weight, result.ReceiverCount, result.ReceiverRequired, unsafe.Sizeof(bucket{}))
				})
			}
		})
	}
}

// Profiling is unscored and opt-in. This owner starts after seeding, stops
// after the measured loop, and joins the standard profiler before heap capture.
func populationBenchStartCPU(b *testing.B, directory string) func() {
	if directory == "" || b.N == 1 {
		return func() {}
	}
	if runtime.MemProfileRate != 1 {
		b.Fatal("live profile requires memprofilerate=1")
	}
	output, err := os.Create(filepath.Join(directory, "cpu.pprof"))
	if err != nil {
		b.Fatal(err)
	}
	if err = pprof.StartCPUProfile(output); err != nil {
		if closeErr := output.Close(); closeErr != nil {
			b.Error(closeErr)
		}
		b.Fatal(err)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		pprof.StopCPUProfile()
		if err := output.Close(); err != nil {
			b.Error(err)
		}
	}
	b.Cleanup(stop)
	return stop
}

// Keep the complete fixture reachable through the post-GC profile: a standard
// end-of-test heap profile can run after its Store has already been released.
func (f *populationBenchFixture) writeLiveHeap(b *testing.B, directory string) {
	b.Helper()
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
	}{len(f.buckets), f.sidecars, b.N, memory.HeapAlloc, memory.HeapObjects, memory.StackInuse, true}
	encoded, err := json.Marshal(record)
	if err != nil {
		b.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(directory, "live-heap.json"), encoded, 0600); err != nil {
		b.Fatal(err)
	}
	runtime.KeepAlive(f)
}
