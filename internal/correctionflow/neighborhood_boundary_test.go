package correctionflow

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestNeighborhoodOriginalRunnerAcrossBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, subject string
		winnerReports int
		gapKHz        float64
		wantSplit     bool
		exactExit     string
	}{
		{"four_to_two", "K1ABD", 4, .1, true, "exact_only"},
		{"three_to_two", "K1ABD", 3, .1, true, "exact_split"},
		{"distinct_stations", "DL9ZZZ", 4, .32, false, "exact_only"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, offset := range []float64{0, .18} {
				crosses := offset != 0
				winnerFreq, subjectFreq := 7010+offset-tc.gapKHz/2, 7010+offset+tc.gapKHz/2
				winnerKey := spot.NewResolverSignalKey(winnerFreq, "40m", "CW", 400)
				subjectKey := spot.NewResolverSignalKey(subjectFreq, "40m", "CW", 400)
				if (winnerKey != subjectKey) != crosses {
					t.Fatalf("fixture did not place the reports in the expected buckets: %v %v", winnerKey, subjectKey)
				}
				now := time.Date(2026, 11, 7, 0, 1, 0, 0, time.UTC)
				r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 32, HysteresisWindows: 1,
					EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond, FreqGuardRunnerUpRatio: .5,
					FreqGuardMinSeparationKHz: .3, MaxEditDistance: 3, DistanceModelCW: "morse"})
				driver, err := spot.NewSignalResolverDriver(r)
				if err != nil {
					t.Fatal(err)
				}
				for i := range tc.winnerReports + 2 {
					call, key, freq := "K1ABC", winnerKey, winnerFreq
					if i >= tc.winnerReports {
						call, key, freq = tc.subject, subjectKey, subjectFreq
					}
					if !r.Enqueue(spot.ResolverEvidence{Key: key, DXCall: call, FrequencyKHz: freq, ObservedAt: now,
						Spotter: fmt.Sprintf("N0AA%c", 'A'+i), RecencyWindow: time.Minute}) {
						t.Fatal("fixture evidence was not queued")
					}
				}
				driver.Drain()
				now = now.Add(time.Millisecond)
				driver.Sweep(now)
				cfg := accountingConfig()
				cfg.FreqGuardRunnerUpRatio, cfg.FreqGuardMinSeparationKHz, cfg.MaxEditDistance = .5, .3, 3
				diagnostics := &ResolverSelectionDiagnostics{}
				result := selectResolverPrimarySnapshotForCallDiagnostics(r, subjectKey, cfg, tc.subject, now, diagnostics)
				if diagnostics.budget.err != nil || !result.SnapshotOK {
					t.Fatalf("fixture did not reach selection: %+v %v", result, diagnostics.budget.err)
				}
				nominee := false
				for _, row := range diagnostics.Nominations {
					nominee = nominee || row.Nominee && row.Identity == tc.subject
				}
				wantSplit := crosses && tc.wantSplit
				if diagnostics.RunnerConflictChecked != wantSplit || diagnostics.RunnerConflict != wantSplit ||
					result.NeighborhoodSplit != wantSplit || nominee != wantSplit {
					t.Fatalf("offset %.2f: want original nomination and ratio split=%t, result=%+v diagnostics=%+v", offset, wantSplit, result, diagnostics)
				}
				if wantSplit && (result.Snapshot.State != spot.ResolverStateSplit || result.Snapshot.Winner != "" ||
					result.Snapshot.RunnerWeightedSupportMilli != 2000 || diagnostics.RunnerRatioThreshold != .5) {
					t.Fatalf("cross-slot conflict failed to clear the winner: %+v", result.Snapshot)
				}
				if crosses && !tc.wantSplit && (result.UsedNeighborhood || result.Snapshot.Winner != tc.subject || result.NeighborhoodExcludedDistance+result.NeighborhoodExcludedUnrelated != 1) {
					t.Fatalf("the distinct station leaked into the subject's bucket: %+v", result)
				}
				if !crosses && diagnostics.Exit != tc.exactExit {
					t.Fatalf("same-slot case wanted %s, got %s", tc.exactExit, diagnostics.Exit)
				}
				if !crosses && tc.exactExit == "exact_only" && result.Snapshot.Winner != "K1ABC" {
					t.Fatalf("same-slot winner did not survive selection: %+v", result.Snapshot)
				}
			}
		})
	}
}
