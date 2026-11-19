package correctionflow

import (
	"sync"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestCCV2CanonicalTemporalIdentityAndCurrentDisplay(t *testing.T) {
	cfg := config.CallCorrectionConfig{TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{Enabled: true, MaxActiveKeys: 2, StayBonus: 120, SwitchPenalty: 160}}
	d := NewTemporalDecoder(cfg)
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	if d.ConfirmCorrection(key, "KH6/W1AW") || d.ConfirmCorrection(key, "W1AW/KH6") {
		t.Fatal("equivalent display counted as switch")
	}
	if got := d.transitionScore("KH6/W1AW", "W1AW/KH6", "CW"); got != 120 {
		t.Fatalf("equivalent displays did not stay: %d", got)
	}
	s := ResolverPrimarySelection{SnapshotOK: true, Snapshot: spot.ResolverSnapshot{Key: key, State: spot.ResolverStateConfident, Winner: "KH6/W1AW", TotalReporters: 2, TotalWeightedSupportMilli: 2000, CandidateRanks: []spot.ResolverCandidateSupport{{Call: "KH6/W1AW", VoteKey: "W1AW/KH6", Support: 2, WeightedSupportMilli: 2000}}}}
	selected := selectionForTemporalWinner(s, "W1AW/KH6")
	if selected.Snapshot.Winner != "KH6/W1AW" || ResolverSupportForCall(selected.Snapshot, "W1AW/KH6") != 2 {
		t.Fatalf("proposal fabricated canonical display: %+v", selected)
	}
	event := temporalEvent{subject: "W1AW/KH6", observedAt: time.Unix(1, 0), selection: s}
	d.cfg.MaxObsCandidates = 8
	if candidates := d.candidatesForEvent(event, "KH6/W1AW"); len(candidates) != 1 || candidates[0] != "W1AW/KH6" {
		t.Fatalf("duplicate semantic decoder states: %v", candidates)
	}
}

func TestCCV2CanonicalNeighborhoodUnionKeepsSettledPopulationNominees(t *testing.T) {
	r := spot.NewSignalResolver(spot.SignalResolverConfig{HysteresisWindows: 1, EvalMinInterval: time.Nanosecond, SlashPrecedenceMinReports: 2})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	for offset, call := range []string{"KH6/W1AW", "W1AW/KH6"} {
		for _, rx := range []string{"N0AAA", "N0BBB"} {
			if !r.Enqueue(spot.ResolverEvidence{Key: key, DXCall: call, Spotter: rx, ObservedAt: at, FrequencyKHz: 7010 + float64(offset)*.5, RecencyWindow: time.Minute}) {
				t.Fatal("union fixture refused")
			}
			d.Drain()
		}
	}
	d.Sweep(at.Add(time.Second))
	cfg := accountingConfig()
	cfg.ResolverNeighborhoodEnabled = true
	cfg.ResolverNeighborhoodBucketRadius = 1
	cfg.FamilyPolicy.SlashPrecedenceMinReports = 2
	s := SelectResolverPrimarySnapshotForCall(r, key, cfg, "W1AW")
	if !s.UsedNeighborhood || s.Snapshot.TotalReporters != 2 || len(s.Snapshot.CandidateRanks) != 1 || ResolverSupportForCall(s.Snapshot, "W1AW/KH6") != 2 || !s.allowsReplacement("KH6/W1AW") {
		t.Fatalf("canonical neighborhood changed union/nomination: %+v", s)
	}
}

func TestCCV2CanonicalConcurrentSelectionKeepsPublishedRows(t *testing.T) {
	r := spot.NewSignalResolver(spot.SignalResolverConfig{HysteresisWindows: 1, EvalMinInterval: time.Nanosecond})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	for offset, call := range []string{"K1ABC", "K1ABD"} {
		for rx := 0; rx < 2; rx++ {
			if !r.Enqueue(spot.ResolverEvidence{Key: key, DXCall: call, Spotter: []string{"N0AAA", "N0BBB", "N0CCC", "N0DDD"}[offset*2+rx], ObservedAt: at, FrequencyKHz: 7010 + float64(offset)*.5, Report: 3, HasReport: true, RecencyWindow: time.Minute}) {
				t.Fatal("concurrent fixture refused")
			}
			d.Drain()
		}
	}
	d.Sweep(at.Add(time.Second))
	original, ok := r.Lookup(key)
	if !ok {
		t.Fatal("missing original publication")
	}
	cfg := accountingConfig()
	cfg.ResolverNeighborhoodEnabled = true
	cfg.ResolverNeighborhoodBucketRadius = 1
	var reader sync.WaitGroup
	reader.Add(1)
	go func() {
		defer reader.Done()
		for i := 0; i < 200; i++ {
			original.ReceiverEvidence().Range(func(call string, row spot.ResolverReceiverObservation) bool {
				if call != "K1ABC" || row.FrequencyKHz != 7010 || row.FrequencyConflict || !row.HasReport || row.Report != 3 {
					t.Error("selector mutated published receiver")
				}
				return true
			})
		}
	}()
	for i := 0; i < 200; i++ {
		selection := SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABE")
		if !selection.UsedNeighborhood {
			t.Error("fixture bypassed neighborhood merge")
		}
	}
	reader.Wait()
}
