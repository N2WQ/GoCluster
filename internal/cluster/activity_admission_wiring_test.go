package cluster

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func activityWiringConfig(flood bool, action string) *config.Config {
	cfg := &config.Config{}
	cfg.Dedup.ClusterWindowSeconds, cfg.Dedup.OutputBufferSize = 60, 1
	// Correction and its optional refresh worker stay disabled. Activity owns
	// neither switch; the controller remains enabled independently.
	cfg.CallCorrection.AdaptiveMinReports = config.AdaptiveMinReportsConfig{Enabled: true, WindowMinutes: 1,
		EvaluationPeriodSeconds: 1, HysteresisWindows: 1,
		Groups: []config.AdaptiveMinReportsGroup{{Name: "one", Bands: []string{"20m"}, QuietBelow: 1, BusyAbove: 2,
			QuietMinReports: 2, NormalMinReports: 3, BusyMinReports: 4}}}
	cfg.FloodControl = config.FloodControlConfig{Enabled: flood, LogIntervalSeconds: 30,
		PartitionMode: config.FloodControlPartitionExactSourceType,
		Rails: config.FloodControlRailsConfig{DECall: config.FloodRailConfig{Enabled: true, Action: action,
			WindowSeconds: 60, MaxEntriesPerPartition: 16, ThresholdsPerSourceType: map[string]int{"RBN": 1}}}}
	return cfg
}

func activityWiringRuntime(t *testing.T, flood bool, action string) *clusterRuntime {
	t.Helper()
	cfg := activityWiringConfig(flood, action)
	r := newClusterRuntime(BuildInfo{}, cfg, "", config.LoadDiagnostics{})
	r.adaptiveMinReports = spot.NewAdaptiveMinReports(cfg.CallCorrection)
	r.ctyDB.Store(loadIngestCTY(t))
	// Invoke actual production construction, not a test-assigned admission hook.
	r.initializePipelineCore()
	t.Cleanup(func() {
		close(r.ingestValidator.input)
		if r.floodController != nil {
			r.floodController.Stop()
		}
		r.deduplicator.Stop()
		r.freqAverager.StopCleanup()
	})
	return r
}

func activityWiringSpot(dx, de, mode string) *spot.Spot {
	s := spot.NewSpot(dx, de, 14030, mode)
	s.SourceType = spot.SourceRBN
	return s
}

func activityWiringSend(t *testing.T, r *clusterRuntime, s *spot.Spot) {
	t.Helper()
	select {
	case r.ingestInput <- s:
	case <-time.After(time.Second):
		t.Fatal("bounded actual ingest refused fixture")
	}
}

func activityWiringWait(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("actual admission pipeline made no progress")
		}
		time.Sleep(time.Millisecond)
	}
}

func activityWiringReceive(t *testing.T, r *clusterRuntime) *spot.Spot {
	t.Helper()
	select {
	case s := <-r.deduplicator.GetOutputChannel():
		return s
	case <-time.After(time.Second):
		t.Fatal("actual dedup output missing")
		return nil
	}
}

func TestActivityProductionAdmissionWiringAndExclusions(t *testing.T) {
	for _, flood := range []bool{false, true} {
		name := "direct"
		if flood {
			name = "flood"
		}
		t.Run(name, func(t *testing.T) {
			r := activityWiringRuntime(t, flood, config.FloodActionObserve)
			for _, tc := range []struct {
				dx, de, mode string
				beacon, self bool
				count        int
			}{
				{"DL1ABC", "DL1AAA", "CW", false, false, 1},
				{"DL1ABD", "DL1AAB", "RTTY", false, false, 2},
				{"DL1ABE", "DL1AAC", "", false, false, 2},
				{"DL1ABF", "DL1AAD", "USB", false, false, 2},
				{"DL1ABG", "DL1AAE", "CW", true, false, 2},
				{"DL1AAH", "DL1AAH", "CW", false, true, 2},
			} {
				s := activityWiringSpot(tc.dx, tc.de, tc.mode)
				s.IsBeacon = tc.beacon
				if tc.self {
					s.SourceType = spot.SourceManual
				}
				activityWiringSend(t, r, s)
				activityWiringReceive(t, r)
				if _, got := r.adaptiveMinReports.ActivityCardinality(); got != tc.count {
					t.Fatalf("%s/%s activity=%d want%d", tc.dx, tc.mode, got, tc.count)
				}
			}
			// A validation rejection precedes both admission topologies. A valid
			// FIFO barrier witnesses completion without a timing-only absence check.
			activityWiringSend(t, r, activityWiringSpot("SET", "DL1BAD", "CW"))
			activityWiringSend(t, r, activityWiringSpot("DL1BAR", "DL1AAF", "CW"))
			activityWiringReceive(t, r)
			if _, got := r.adaptiveMinReports.ActivityCardinality(); got != 3 {
				t.Fatalf("rejected input counted: got%d", got)
			}
		})
	}
}

func TestActivityProductionDuplicateRefreshAndOutputDrop(t *testing.T) {
	for _, flood := range []bool{false, true} {
		name := "direct"
		if flood {
			name = "flood"
		}
		t.Run(name, func(t *testing.T) {
			r := activityWiringRuntime(t, flood, config.FloodActionObserve)
			first := activityWiringSpot("DL1ABC", "DL1AAA", "CW")
			activityWiringSend(t, r, first)
			activityWiringReceive(t, r)
			// Age the receiver independently, then prove a duplicate rejected by
			// primary dedup refreshed its incoming activity nevertheless.
			r.adaptiveMinReports.Observe("20m", "DL1AAA", time.Now().Add(-2*time.Minute))
			duplicate := activityWiringSpot("DL1ABC", "DL1AAA", "CW")
			duplicate.Time = first.Time
			activityWiringSend(t, r, duplicate)
			activityWiringSend(t, r, activityWiringSpot("DL1BAR", "DL1AAB", "CW"))
			activityWiringReceive(t, r)
			processed, duplicates, _ := r.deduplicator.GetStats()
			if processed != 3 || duplicates != 1 {
				t.Fatalf("fixture missed actual duplicate: processed%d duplicates%d", processed, duplicates)
			}
			r.adaptiveMinReports.MinReportsForBand("20m", time.Now().Add(30*time.Second))
			if _, got := r.adaptiveMinReports.ActivityCardinality(); got != 2 {
				t.Fatalf("duplicate did not refresh accepted activity: %d", got)
			}
			// Leave one output queued and let a different admitted input hit the
			// full output. This is later congestion, not input refusal.
			activityWiringSend(t, r, activityWiringSpot("DL1OUT", "DL1AAC", "CW"))
			activityWiringWait(t, func() bool { p, _, _ := r.deduplicator.GetStats(); return p == 4 })
			dropped := activityWiringSpot("DL1DRP", "DL1AAD", "CW")
			droppedAt := dropped.Time
			activityWiringSend(t, r, dropped)
			// A FIFO duplicate barrier cannot emit output and is processed only
			// after the preceding output-full send attempt has completed.
			barrier := activityWiringSpot("DL1DRP", "DL1AAD", "CW")
			barrier.Time = droppedAt
			activityWiringSend(t, r, barrier)
			activityWiringWait(t, func() bool { p, d, _ := r.deduplicator.GetStats(); return p == 6 && d == 2 })
			if _, got := r.adaptiveMinReports.ActivityCardinality(); got != 4 {
				t.Fatalf("output congestion undid admitted activity: %d", got)
			}
			if got := activityWiringReceive(t, r).DXCallNorm; got != "DL1OUT" {
				t.Fatalf("output pressure witness changed: %s", got)
			}
			select {
			case extra := <-r.deduplicator.GetOutputChannel():
				t.Fatalf("output pressure fixture did not drop %s", extra.DXCallNorm)
			default:
			}
		})
	}
}

func TestActivityProductionFloodRejectionDoesNotRefresh(t *testing.T) {
	for _, action := range []string{config.FloodActionSuppress, config.FloodActionDrop} {
		t.Run(action, func(t *testing.T) {
			r := activityWiringRuntime(t, true, action)
			activityWiringSend(t, r, activityWiringSpot("DL1ABC", "DL1AAA", "CW"))
			activityWiringReceive(t, r)
			r.adaptiveMinReports.Observe("20m", "DL1AAA", time.Now().Add(-2*time.Minute))
			activityWiringSend(t, r, activityWiringSpot("DL1ABD", "DL1AAA", "CW"))
			activityWiringSend(t, r, activityWiringSpot("DL1BAR", "DL1AAB", "CW"))
			activityWiringReceive(t, r)
			r.adaptiveMinReports.MinReportsForBand("20m", time.Now().Add(30*time.Second))
			if _, got := r.adaptiveMinReports.ActivityCardinality(); got != 1 {
				t.Fatalf("flood rejected traffic refreshed activity: %d", got)
			}
			_, _, suppressed, dropped, _, _ := r.floodController.GetStats()
			if suppressed+dropped != 1 {
				t.Fatal("fixture failed to exercise actual flood rejection")
			}
		})
	}
}
