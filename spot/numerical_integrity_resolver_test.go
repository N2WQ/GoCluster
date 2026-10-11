package spot

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"
)

func b21ResolverDriver(t *testing.T, cfg SignalResolverConfig) (*SignalResolver, *SignalResolverDriver) {
	t.Helper()
	cfg.QueueSize, cfg.MaxActiveKeys = 32, 8
	cfg.MaxCandidatesPerKey, cfg.MaxReportersPerCand = 8, 8
	cfg.InactiveTTL = time.Minute
	cfg.EvalMinInterval, cfg.SweepInterval = time.Nanosecond, time.Nanosecond
	cfg.HysteresisWindows, cfg.MaxEditDistance = 1, 3
	resolver := NewSignalResolver(cfg)
	driver, err := NewSignalResolverDriver(resolver)
	if err != nil {
		t.Fatal(err)
	}
	return resolver, driver
}

func b21Evidence(key ResolverSignalKey, call, receiver string, at time.Time) ResolverEvidence {
	return ResolverEvidence{ObservedAt: at, Key: key, DXCall: call, Spotter: receiver,
		Report: 5, FrequencyKHz: 14020, RecencyWindow: time.Minute}
}

func b21RequireReceiverAdmission(t *testing.T, cfg SignalResolverConfig, mode string, allowed bool, wantWeight int) {
	t.Helper()
	resolver, driver := b21ResolverDriver(t, cfg)
	at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	key := NewResolverSignalKey(14020, "20m", mode, 500)
	if !resolver.Enqueue(b21Evidence(key, "K1ABC", "N0AAA", at)) {
		t.Fatal("bounded fixture evidence was not queued")
	}
	if got := driver.Step(at.Add(time.Millisecond)); got != 1 {
		t.Fatalf("driver processed=%d want 1", got)
	}
	snapshot, found := resolver.Lookup(key)
	metrics := resolver.MetricsSnapshot()
	if metrics.Accepted != 1 || metrics.Processed != 1 || metrics.QueueDepth != 0 {
		t.Errorf("unexpected queue accounting: %+v", metrics)
	}
	if !allowed {
		if found || metrics.ActiveKeys != 0 || metrics.PublishedReceiverRecords != 0 || metrics.DropReliability != 1 {
			t.Errorf("invalid or below-floor evidence participated in publication: found=%t snapshot=%+v metrics=%+v", found, snapshot, metrics)
		}
		return
	}
	if !found || snapshot.TotalReporters != 1 || snapshot.WinnerWeightedSupportMilli != wantWeight || snapshot.ReceiverEvidence().Len() != 1 || metrics.DropReliability != 0 {
		t.Fatalf("valid finite evidence lost or reweighted: found=%t snapshot=%+v metrics=%+v want weight=%d", found, snapshot, metrics, wantWeight)
	}
	snapshot.ReceiverEvidence().Range(func(call string, observation ResolverReceiverObservation) bool {
		if call != "K1ABC" || observation.Receiver != "N0AAA" || observation.WeightMilli != wantWeight {
			t.Errorf("published receiver row changed: call=%s row=%+v", call, observation)
		}
		return true
	})
}

func TestB21ResolverRejectsSelectedNonfiniteReliability(t *testing.T) {
	for _, invalid := range []struct {
		name  string
		value float64
	}{{"nan", math.NaN()}, {"positive_inf", math.Inf(1)}, {"negative_inf", math.Inf(-1)}} {
		for _, source := range []string{"global", "cw", "rtty"} {
			t.Run(invalid.name+"/"+source, func(t *testing.T) {
				cfg := SignalResolverConfig{SpotterReliability: SpotterReliability{"N0AAA": 1}}
				mode := "CW"
				switch source {
				case "global":
					cfg.SpotterReliability["N0AAA"] = invalid.value
				case "cw":
					cfg.SpotterReliabilityCW = SpotterReliability{"N0AAA": invalid.value}
				case "rtty":
					mode = "RTTY"
					cfg.SpotterReliabilityRTTY = SpotterReliability{"N0AAA": invalid.value}
				}
				// Floor zero must not let an invalid weight converted to zero
				// enter the raw count. A valid global entry cannot rescue a
				// present invalid mode-specific entry.
				b21RequireReceiverAdmission(t, cfg, mode, false, 0)
			})
		}
		t.Run(invalid.name+"/unused_global", func(t *testing.T) {
			cfg := SignalResolverConfig{
				SpotterReliability:   SpotterReliability{"N0AAA": invalid.value},
				SpotterReliabilityCW: SpotterReliability{"N0AAA": 0.9},
			}
			b21RequireReceiverAdmission(t, cfg, "CW", true, 900)
		})
	}
}

func TestB21ResolverRejectsNonfiniteActiveRawParameters(t *testing.T) {
	model, err := buildConfusionModel(b21RawConfusionModel())
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []struct {
		name  string
		value float64
	}{{"nan", math.NaN()}, {"positive_inf", math.Inf(1)}, {"negative_inf", math.Inf(-1)}} {
		t.Run("floor/"+invalid.name, func(t *testing.T) {
			// The default receiver weighs one. Checking only after +Inf
			// clamps to floor one would falsely admit this receiver.
			b21RequireReceiverAdmission(t, SignalResolverConfig{MinSpotterReliability: invalid.value}, "CW", false, 0)
		})
		t.Run("active_model_weight/"+invalid.name, func(t *testing.T) {
			b21RequireReceiverAdmission(t, SignalResolverConfig{ConfusionModel: model, ConfusionWeight: invalid.value}, "CW", false, 0)
		})
		t.Run("unused_model_weight/"+invalid.name, func(t *testing.T) {
			b21RequireReceiverAdmission(t, SignalResolverConfig{ConfusionWeight: invalid.value}, "CW", true, 1000)
		})
	}
}

func TestB21ResolverPreservesFiniteWeightAndFloorClamps(t *testing.T) {
	model, err := buildConfusionModel(b21RawConfusionModel())
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []struct {
		name    string
		weight  float64
		floor   float64
		allowed bool
		milli   int
	}{
		{"negative_weight_and_floor", -2, -2, true, 0},
		{"zero_weight_and_floor", 0, 0, true, 0},
		{"above_one_weight_and_floor", 2, 2, true, 1000},
		{"above_one_floor_refuses_fraction", 0.9, 2, false, 0},
		{"equal_floor", 0.5, 0.5, true, 500},
		{"below_floor", 0.499, 0.5, false, 0},
	} {
		t.Run(query.name, func(t *testing.T) {
			b21RequireReceiverAdmission(t, SignalResolverConfig{
				SpotterReliability: SpotterReliability{"N0AAA": query.weight}, MinSpotterReliability: query.floor,
			}, "CW", query.allowed, query.milli)
		})
	}
	for _, weight := range []float64{-2, 0, 2, math.MaxFloat64} {
		t.Run(fmt.Sprintf("finite_model_weight/%g", weight), func(t *testing.T) {
			b21RequireReceiverAdmission(t, SignalResolverConfig{ConfusionModel: model, ConfusionWeight: weight}, "CW", true, 1000)
		})
	}
}

func TestB21ConfusionTieRejectsNonfiniteWeightWithoutMutation(t *testing.T) {
	model := mustBuildResolverConfusionModelForTieBreak(t)
	for _, weight := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		t.Run(fmt.Sprint(weight), func(t *testing.T) {
			ranked := []rankedResolverCandidate{
				{call: "AA1AA", callRunes: []rune("AA1AA"), support: 1, weightedSupportMilli: 1000, lastReport: 5},
				{call: "ZZ1ZZ", callRunes: []rune("ZZ1ZZ"), support: 1, weightedSupportMilli: 1000, lastReport: 5},
			}
			before := append([]rankedResolverCandidate(nil), ranked...)
			var workspace confusionAlignmentWorkspace
			applyResolverConfusionTieBreak(ranked, "CW", model, weight, &workspace)
			if !reflect.DeepEqual(ranked, before) {
				t.Errorf("invalid direct tie weight changed the cohort: before=%+v after=%+v", before, ranked)
			}
		})
	}
}

func TestB21ConfusionTiePreservesFiniteOrdering(t *testing.T) {
	model := mustBuildResolverConfusionModelForTieBreak(t)
	for _, query := range []struct {
		weight float64
		winner string
	}{{-2, "AA1AA"}, {0, "AA1AA"}, {math.SmallestNonzeroFloat64, "ZZ1ZZ"}, {1, "ZZ1ZZ"}, {math.MaxFloat64, "ZZ1ZZ"}} {
		t.Run(fmt.Sprint(query.weight), func(t *testing.T) {
			ranked := []rankedResolverCandidate{
				{call: "AA1AA", callRunes: []rune("AA1AA"), support: 1, weightedSupportMilli: 1000, lastReport: 5},
				{call: "ZZ1ZZ", callRunes: []rune("ZZ1ZZ"), support: 1, weightedSupportMilli: 1000, lastReport: 5},
			}
			var workspace confusionAlignmentWorkspace
			applyResolverConfusionTieBreak(ranked, "CW", model, query.weight, &workspace)
			// The existing learned fixture favors Z observed as A. Equal
			// support makes every finite positive weight select that same
			// order, including rounded and overflowing blend ties.
			if ranked[0].call != query.winner || ranked[0].weightedSupportMilli != 1000 || ranked[1].weightedSupportMilli != 1000 {
				t.Errorf("finite weight %g changed compatible tie ordering or support: %+v want winner %s", query.weight, ranked, query.winner)
			}
		})
	}
}

func TestB21ResolverIndependentSupportShareAcrossRepeatedBuckets(t *testing.T) {
	resolver, driver := b21ResolverDriver(t, SignalResolverConfig{
		SpotterReliability: SpotterReliability{"N0AAA": 0.9, "N0BBB": 0.95},
	})
	at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	for bucket := 0; bucket < 3; bucket++ {
		frequency := 14020 + float64(bucket)
		key := NewResolverSignalKey(frequency, "20m", "CW", 500)
		bucketAt := at.Add(time.Duration(bucket) * 10 * time.Millisecond)
		for repeat := 0; repeat < 3; repeat++ {
			for _, row := range []struct{ call, receiver string }{{"K1ABC", "N0AAA"}, {"K1ABC", "N0BBB"}, {"K1ABD", "N0CCC"}} {
				evidence := b21Evidence(key, row.call, row.receiver, bucketAt.Add(time.Duration(repeat)*time.Millisecond))
				evidence.FrequencyKHz = frequency
				if !resolver.Enqueue(evidence) {
					t.Fatal("bounded weighted-share evidence was not queued")
				}
			}
			driver.Step(bucketAt.Add(time.Duration(repeat)*time.Millisecond + time.Nanosecond))
			snapshot, ok := resolver.LookupEligible(key, bucketAt.Add(time.Duration(repeat)*time.Millisecond+time.Nanosecond))
			if !ok || snapshot.Winner != "K1ABC" || snapshot.WinnerWeightedSupportMilli != 1850 || snapshot.RunnerWeightedSupportMilli != 1000 || snapshot.TotalWeightedSupportMilli != 2850 || snapshot.TotalReporters != 3 || snapshot.ReceiverEvidence().Len() != 3 {
				t.Fatalf("bucket=%d repeat=%d changed literal support totals: available=%t snapshot=%+v", bucket, repeat, ok, snapshot)
			}
			// Hand arithmetic: 1850 / 2850 = 37 / 57, whose integer
			// percentage is 64. Repeated reports and other buckets do
			// not create additional receivers in this denominator.
			if share := snapshot.WinnerWeightedSupportMilli * 100 / snapshot.TotalWeightedSupportMilli; share != 64 {
				t.Errorf("support share=%d want literal 64", share)
			}
		}
	}
}

func TestB21ConcurrentImmutableNumericalReaders(t *testing.T) {
	raw := b21RawConfusionModel()
	model, err := buildConfusionModel(raw)
	if err != nil {
		t.Fatal(err)
	}
	beforeModel, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	global := SpotterReliability{"N0AAA": 0.8, "N0BBB": 0.95}
	cw := SpotterReliability{"N0AAA": 0.9}
	rtty := SpotterReliability{"N0AAA": 0.7}
	cfg := SignalResolverConfig{SpotterReliability: global, SpotterReliabilityCW: cw, SpotterReliabilityRTTY: rtty}
	resolver, driver := b21ResolverDriver(t, cfg)
	at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	key := NewResolverSignalKey(14020, "20m", "CW", 500)
	for _, row := range []struct{ call, receiver string }{{"K1ABC", "N0AAA"}, {"K1ABC", "N0BBB"}, {"K1ABD", "N0CCC"}} {
		if !resolver.Enqueue(b21Evidence(key, row.call, row.receiver, at)) {
			t.Fatal("immutable reader fixture was not queued")
		}
	}
	driver.Step(at.Add(time.Millisecond))
	beforeSnapshot, ok := resolver.Lookup(key)
	if !ok || beforeSnapshot.TotalWeightedSupportMilli != 2850 || beforeSnapshot.ReceiverEvidence().Len() != 3 {
		t.Fatalf("immutable fixture publication missing: available=%t snapshot=%+v", ok, beforeSnapshot)
	}
	beforeRanks := append([]ResolverCandidateSupport(nil), beforeSnapshot.CandidateRanks...)
	beforeMetrics := resolver.MetricsSnapshot()
	const workers, passes = 8, 128
	start := make(chan struct{})
	failures := make(chan error, workers)
	var readers sync.WaitGroup
	readers.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer readers.Done()
			<-start
			for pass := 0; pass < passes; pass++ {
				if score := model.ScoreCandidate("?", "A", "CW", 5); math.IsNaN(score) || math.Abs(score-(-0.40546510810816444)) > 1e-14 {
					failures <- fmt.Errorf("reader %d model score=%v", worker, score)
					return
				}
				for _, query := range []struct {
					mode string
					want int
				}{{"CW", 900}, {"RTTY", 700}, {"USB", 800}} {
					if weight, allowed := resolverSpotterWeightMilli(cfg, query.mode, "N0AAA"); !allowed || weight != query.want {
						failures <- fmt.Errorf("reader %d mode=%s weight=%d allowed=%t want %d", worker, query.mode, weight, allowed, query.want)
						return
					}
				}
				if snapshot, available := resolver.LookupEligible(key, at.Add(time.Millisecond)); !available || snapshot.WinnerWeightedSupportMilli != 1850 || snapshot.TotalWeightedSupportMilli != 2850 || snapshot.ReceiverEvidence().Len() != 3 {
					failures <- fmt.Errorf("reader %d fresh snapshot changed: available=%t snapshot=%+v", worker, available, snapshot)
					return
				}
				if snapshot, found := resolver.Lookup(key); !found || snapshot.ReceiverEvidence() != beforeSnapshot.ReceiverEvidence() || !reflect.DeepEqual(snapshot.CandidateRanks, beforeRanks) {
					failures <- fmt.Errorf("reader %d owner publication mutated: found=%t snapshot=%+v", worker, found, snapshot)
					return
				}
			}
		}(worker)
	}
	close(start)
	readers.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	afterModel, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(beforeModel, afterModel) || !reflect.DeepEqual(model.subRowSums, [][][]int64{{{4, 0}, {4, 0}}, {{4, 0}, {4, 0}}}) || !reflect.DeepEqual(model.delSums, [][]int64{{4, 4}, {4, 4}}) || !reflect.DeepEqual(model.insSums, [][]int64{{4, 4}, {4, 4}}) {
		t.Error("concurrent model scoring mutated loaded counts, edges or totals")
	}
	if !reflect.DeepEqual(global, SpotterReliability{"N0AAA": 0.8, "N0BBB": 0.95}) || !reflect.DeepEqual(cw, SpotterReliability{"N0AAA": 0.9}) || !reflect.DeepEqual(rtty, SpotterReliability{"N0AAA": 0.7}) {
		t.Error("concurrent receiver selection mutated reliability inputs")
	}
	if after := resolver.MetricsSnapshot(); after != beforeMetrics {
		t.Errorf("immutable snapshot readers changed owner accounting: before=%+v after=%+v", beforeMetrics, after)
	}
}
