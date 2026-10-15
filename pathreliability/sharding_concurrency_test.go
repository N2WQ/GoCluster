package pathreliability

import (
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPredictorShardedConcurrentMaintenanceReadback(t *testing.T) {
	configured := shardingConfiguredConfig(t)
	paths := shardingDisjointPaths(t)
	now := time.Date(2026, time.October, 14, 20, 0, 0, 0, time.UTC)
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			cfg := configured
			cfg.ReceiverContributionMode = mode
			cfg.MinObservationCount = 1
			cfg.BeaconMinObservationCount = 1
			cfg.MinEffectiveWeight = 0.1
			cfg.ReceiverMaxEffectiveCount = 8
			cfg.ReceiverMaxEffectiveWeight = 8
			predictor := NewPredictor(cfg, cfg.AllowedBands)
			for _, path := range paths {
				for _, reverse := range []bool{false, true} {
					if admission := emitShardingContractReport(predictor, path, "20m", reverse, 0, now); admission.Status != AdmissionAccepted {
						t.Fatalf("fresh warm report was not admitted: %+v", admission)
					}
				}
			}
			want := copyShardingContractBuckets(predictor.combined)
			if len(want) != 64 {
				t.Fatalf("16 disjoint forward/reverse fine/coarse paths must warm 64 buckets, got %d", len(want))
			}
			for _, band := range cfg.AllowedBands {
				if band == "20m" {
					continue
				}
				for _, path := range paths {
					for _, reverse := range []bool{false, true} {
						if admission := emitShardingContractReport(predictor, path, band, reverse, 0, now.Add(-48*time.Hour)); admission.Status != AdmissionAccepted {
							t.Fatalf("stale setup report was not admitted: %+v", admission)
						}
					}
				}
			}
			if got := predictor.TotalBuckets(); got != 64*len(cfg.AllowedBands) {
				t.Fatalf("stale setup buckets=%d, want %d before concurrent purge", got, 64*len(cfg.AllowedBands))
			}
			runShardingConcurrentContract(t, predictor, paths, now)
			if got := copyShardingContractBuckets(predictor.combined); !maps.Equal(got, want) {
				t.Fatalf("concurrent maintenance changed exact live keys/bucket identities: got=%d want=%d", len(got), len(want))
			}
			if got := predictor.TotalBuckets(); got != 64 {
				t.Fatalf("physical final bucket count=%d, want 64 without duplicate keys in other shards", got)
			}
			for _, path := range paths {
				for _, reverse := range []bool{false, true} {
					receiver, sender := path.receiver, path.sender
					if reverse {
						receiver, sender = sender, receiver
					}
					fine, coarse := predictor.combined.Lookup(receiver.fine, sender.fine, receiver.coarse, sender.coarse, "20m", now)
					if fine.RawCount != 24 || coarse.RawCount != 24 || fine.Weight != 24 || coarse.Weight != 24 {
						t.Fatalf("final directional capture lost reports reverse=%v fine=%+v coarse=%+v", reverse, fine, coarse)
					}
				}
				ordinary := predictor.Predict(path.receiver.fine, path.sender.fine, path.receiver.coarse, path.sender.coarse, "20m", "FT8", 0, now)
				beacon := predictor.PredictBeaconReceiveOnlyWithMinObservationCount(path.receiver.fine, path.sender.fine, path.receiver.coarse, path.sender.coarse, "20m", "FT8", 0, 1, now)
				if !usableShardingContractResult(ordinary) || ordinary.RawCount != 48 || !usableShardingContractResult(beacon) || beacon.RawCount != 24 || !beacon.BeaconRX {
					t.Fatalf("final known p50/count readback failed: ordinary=%+v beacon=%+v", ordinary, beacon)
				}
			}
			if stats := predictor.AdmissionStats(); stats != (AdmissionStats{}) {
				t.Fatalf("concurrent valid reports changed input admission: %+v", stats)
			}
		})
	}
}

func runShardingConcurrentContract(t *testing.T, predictor *Predictor, paths []shardingContractPath, now time.Time) {
	t.Helper()
	start := make(chan struct{})
	failures := make(chan error, 1)
	recordFailure := func(err error) {
		select {
		case failures <- err:
		default:
		}
	}
	var admitted atomic.Int32
	var workers sync.WaitGroup
	// Both production ingress routes call this public owner. Independent
	// directional producers contend with copied captures and maintenance here;
	// connected runtime ingress/transport remains a separate validation fixture.
	for _, reverse := range []bool{false, true} {
		workers.Go(func() {
			<-start
			for report := 1; report < 24; report++ {
				for _, path := range paths {
					if admission := emitShardingContractReport(predictor, path, "20m", reverse, report, now); admission.Status != AdmissionAccepted {
						recordFailure(fmt.Errorf("report %d reverse=%v admission=%+v", report, reverse, admission))
					} else {
						admitted.Add(1)
					}
				}
			}
		})
	}
	workers.Go(func() {
		<-start
		for range 64 {
			for _, path := range paths {
				ordinary := predictor.Predict(path.receiver.fine, path.sender.fine, path.receiver.coarse, path.sender.coarse, "20m", "FT8", 0, now)
				beacon := predictor.PredictBeaconReceiveOnlyWithMinObservationCount(path.receiver.fine, path.sender.fine, path.receiver.coarse, path.sender.coarse, "20m", "FT8", 0, 1, now)
				fine, coarse := predictor.combined.Lookup(path.receiver.fine, path.sender.fine, path.receiver.coarse, path.sender.coarse, "20m", now)
				if !usableShardingContractResult(ordinary) || !usableShardingContractResult(beacon) || fine.Weight <= 0 || coarse.Weight <= 0 || fine.RawCount == 0 || coarse.RawCount == 0 {
					recordFailure(fmt.Errorf("concurrent nonempty readback: ordinary=%+v beacon=%+v fine=%+v coarse=%+v", ordinary, beacon, fine, coarse))
				}
			}
		}
	})
	workers.Go(func() {
		<-start
		for range 64 {
			predictor.RefreshStatsSnapshot(now)
			stats := predictor.StatsSnapshot()
			hist := predictor.WeightHistogramByBand(now)
			if !stats.Available || stats.Stats != (PredictorStats{CombinedFine: 32, CombinedCoarse: 32}) || !hist.Available {
				recordFailure(fmt.Errorf("concurrent active reporting: stats=%+v histogram=%+v", stats, hist))
			}
			total := 0
			for _, band := range hist.Bands {
				total += band.Total
				want := 0
				if band.Band == "20m" {
					want = 64
				}
				if band.Total != want {
					recordFailure(fmt.Errorf("concurrent histogram band=%s total=%d want=%d", band.Band, band.Total, want))
				}
			}
			if total != 64 || len(hist.Bands) != len(predictor.cfg.AllowedBands) {
				recordFailure(fmt.Errorf("concurrent histogram omitted live bands: total=%d bands=%d", total, len(hist.Bands)))
			}
			predictor.PurgeStale(now)
			predictor.Compact(1, 0.5)
		}
	})
	close(start)
	workers.Wait()
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	if got, want := admitted.Load(), int32(2*23*len(paths)); got != want {
		t.Fatalf("concurrent admitted reports=%d, want exactly %d", got, want)
	}
}

type shardingContractPath struct{ receiver, sender shardingMappedCell }

func shardingDisjointPaths(t *testing.T) []shardingContractPath {
	t.Helper()
	seenFine, seenCoarse := make(map[CellID]bool), make(map[CellID]bool)
	endpoints := make([]shardingMappedCell, 0, 32)
	for _, cell := range shardingMappedCells(t) {
		if seenFine[cell.fine] || seenCoarse[cell.coarse] {
			continue
		}
		seenFine[cell.fine], seenCoarse[cell.coarse] = true, true
		endpoints = append(endpoints, cell)
		if len(endpoints) == 32 {
			break
		}
	}
	if len(endpoints) != 32 {
		t.Fatal("mapped concurrency fixture needs 32 distinct fine/coarse endpoints")
	}
	paths := make([]shardingContractPath, 16)
	for i := range paths {
		paths[i] = shardingContractPath{endpoints[2*i], endpoints[2*i+1]}
	}
	return paths
}

func emitShardingContractReport(predictor *Predictor, path shardingContractPath, band string, reverse bool, report int, at time.Time) ReportAdmission {
	receiver, sender := path.receiver, path.sender
	if reverse {
		receiver, sender = sender, receiver
	}
	return predictor.UpdateWithReceiverHash(BucketCombined, receiver.fine, sender.fine, receiver.coarse, sender.coarse, band, -10.5, 1,
		ObservationTimes{ObservedAt: at, ArrivedAt: at}, false, uint64(report/8+1))
}

func usableShardingContractResult(result Result) bool {
	return result.Source == SourceCombined && result.HasP50 && result.ObservedDistributionAvailable && result.P50DB == -10.5 && result.RawCount > 0
}

func copyShardingContractBuckets(store *Store) map[uint64]*bucket {
	result := make(map[uint64]*bucket)
	for i := range store.shards {
		sh := &store.shards[i]
		sh.mu.RLock()
		maps.Copy(result, sh.buckets)
		sh.mu.RUnlock()
	}
	return result
}
