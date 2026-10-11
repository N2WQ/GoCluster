//go:build !qualification

// File role: Carries actual report presence through shared resolver evidence
// into local TCP, original peer publication, and independently reopened storage.
package cluster

import (
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

// The distinguishing substitutions have likelihoods 101/153 for true D seen
// as C and 2/54 for true C seen as D. Other call characters match and add no
// substitution contribution. This fixture asserts direction, not calibration.
const correctionSNRLiteralModel = `{"modes":["CW"],"snr_band_edges":[-50,50],"alphabet":"CD?","unknown_char":"?","sub_counts":[[[[50,1,0],[100,50,0],[0,0,50]]]],"del_counts":[[[1,1,1]]],"ins_counts":[[[1,1,1]]]}`

type correctionSNRDeliveryCase struct {
	name, update                       string
	initialReport, outputReport        int
	initialPresent, outputPresent      bool
	wantCall, wantGlyph, normalComment string
	corrections                        uint64
}

func TestCorrectionSNRDeliveredPresenceAndDomain(t *testing.T) {
	for _, tc := range []correctionSNRDeliveryCase{
		{name: "missing_zero", wantCall: "K1ABC", wantGlyph: "P", normalComment: "CW"},
		{name: "missing_nonzero", initialReport: 20, outputReport: 20, wantCall: "K1ABC", wantGlyph: "P", normalComment: "CW"},
		{name: "reported_zero", initialPresent: true, outputPresent: true, wantCall: "K1ABD", wantGlyph: "C", normalComment: "CW 0 dB", corrections: 1},
		{name: "outer_lower_excluded", initialReport: -50, outputReport: -50, initialPresent: true, outputPresent: true, wantCall: "K1ABC", wantGlyph: "P", normalComment: "CW -50 dB"},
		{name: "outer_upper_included", initialReport: 50, outputReport: 50, initialPresent: true, outputPresent: true, wantCall: "K1ABD", wantGlyph: "C", normalComment: "CW 50 dB", corrections: 1},
		{name: "below_domain", initialReport: -51, outputReport: -51, initialPresent: true, outputPresent: true, wantCall: "K1ABC", wantGlyph: "P", normalComment: "CW -51 dB"},
		{name: "above_domain", initialReport: 51, outputReport: 51, initialPresent: true, outputPresent: true, wantCall: "K1ABC", wantGlyph: "P", normalComment: "CW 51 dB"},
		{name: "newest_missing_supersedes_present", initialPresent: true, update: "newer_missing", wantCall: "K1ABC", wantGlyph: "P", normalComment: "CW"},
		{name: "equal_time_present_then_missing", initialPresent: true, update: "equal_missing", wantCall: "K1ABC", wantGlyph: "P", normalComment: "CW"},
		{name: "equal_time_missing_then_present", update: "equal_present", outputPresent: true, wantCall: "K1ABD", wantGlyph: "C", normalComment: "CW 0 dB", corrections: 1},
		{name: "older_missing_does_not_supersede_present", initialPresent: true, update: "older_missing", outputPresent: true, wantCall: "K1ABD", wantGlyph: "C", normalComment: "CW 0 dB", corrections: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			correctionSNRCheckDelivery(t, tc)
		})
	}
}

func correctionSNRCheckDelivery(t *testing.T, tc correctionSNRDeliveryCase) {
	t.Helper()
	temporalDeliveryDirectory(t)
	at := time.Now().UTC().Truncate(time.Second)
	observed := at.Add(-3 * time.Second)
	modelPath := correctionNumericFile(t, "snr-model.json", correctionSNRLiteralModel)
	model, err := spot.LoadConfusionModel(modelPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.CallCorrectionConfig{
		Enabled: true, RecencySeconds: 120, FrequencyToleranceHz: 500,
		MaxEditDistance: 1, MinConsensusReports: 2, MinAdvantage: 0, MinConfidencePercent: 50,
		DistanceModelCW: "plain", DistanceModelRTTY: "plain", InvalidAction: "broadcast",
		ConfusionModelEnabled: true, ConfusionModelWeight: 1,
	}
	r := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 16, MaxActiveKeys: 4, MaxCandidatesPerKey: 4, MaxReportersPerCand: 8,
		InactiveTTL: 5 * time.Minute, EvalMinInterval: time.Second, SweepInterval: time.Millisecond,
		HysteresisWindows: 1, MaxEditDistance: 1, DistanceModelCW: "plain", DistanceModelRTTY: "plain",
		ConfusionModel: model, ConfusionWeight: 1,
	})
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(14020, "20m", "CW", 500)
	for _, row := range []struct{ call, receiver string }{
		{"K1ABC", "N0AAA"}, {"K1ABC", "N0BBB"}, {"K1ABD", "N0CCC"}, {"K1ABD", "N0DDD"},
	} {
		correctionSNRAddSpot(t, r, cfg, row.call, row.receiver, tc.initialReport, tc.initialPresent, observed)
	}
	wantProcessed := 4
	if tc.update != "" {
		updateAt, present := observed, false
		switch tc.update {
		case "newer_missing":
			updateAt = observed.Add(500 * time.Millisecond)
		case "older_missing":
			updateAt = observed.Add(-500 * time.Millisecond)
		case "equal_present":
			present = true
		case "equal_missing":
		default:
			t.Fatalf("unknown fixture representative update %q", tc.update)
		}
		correctionSNRAddSpot(t, r, cfg, "K1ABC", "N0BBB", 0, present, updateAt)
		correctionSNRAddSpot(t, r, cfg, "K1ABD", "N0DDD", 0, present, updateAt)
		wantProcessed += 2
	}
	if got := driver.Drain(); got != wantProcessed {
		t.Fatalf("processing barrier consumed %d reports, want %d", got, wantProcessed)
	}
	// Batch all metadata updates before ranking. Settling an intermediate B
	// would introduce the separate existing hysteresis contract into this oracle.
	// The evaluation clock is after every update and before actual delivery.
	driver.Sweep(observed.Add(2 * time.Second))
	snapshot, ok := r.Lookup(key)
	if !ok || snapshot.State != spot.ResolverStateProbable || snapshot.WinnerSupport != 2 || snapshot.RunnerSupport != 2 || snapshot.TotalReporters != 4 || snapshot.WinnerWeightedSupportMilli != 2000 || snapshot.TotalWeightedSupportMilli != 4000 {
		t.Fatalf("fixture must publish the complete probable A2/B2 population before delivery: available=%t snapshot=%+v", ok, snapshot)
	}
	if snapshot.Winner != tc.wantCall {
		t.Errorf("literal report eligibility must select %s: snapshot=%+v", tc.wantCall, snapshot)
	}
	if snapshot.ReceiverEvidence() == nil || snapshot.ReceiverEvidence().Len() != 4 || r.MetricsSnapshot().DropReliability != 0 {
		t.Fatal("unavailable SNR must not remove receiver support or reject its weight")
	}
	f := newAmbiguityDeliveryFixture(t, r, cfg)
	f.pipeline.confusionModel = model
	s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
	s.SourceType, s.Report, s.HasReport = spot.SourceManual, tc.outputReport, tc.outputPresent
	f.pipeline.processSpot(s, nil)
	if got := f.pipeline.tracker.CallCorrections(); got != tc.corrections {
		t.Errorf("accepted identity changes=%d want %d", got, tc.corrections)
	}
	if tc.corrections == 0 && f.pipeline.tracker.CorrectionDecisionReasons()[resolverDecisionSameCall] != 1 {
		t.Errorf("missing/domain omission must retain the same settled A: %v", f.pipeline.tracker.CorrectionDecisionReasons())
	}
	for _, output := range []struct{ name, got, want string }{
		{"normal", correctionEvidenceReadDX(t, f.normalConn, f.normalReader), correctionEvidenceWire(tc.wantCall, tc.wantGlyph, tc.normalComment, at)},
		{"DIAG", correctionEvidenceReadDX(t, f.diagConn, f.diagReader), correctionEvidenceWire(tc.wantCall, tc.wantGlyph, tc.normalComment+" 50%", at)},
	} {
		if output.got != output.want {
			t.Errorf("actual %s TCP:\n got %q\nwant %q", output.name, output.got, output.want)
		}
	}
	correctionEvidenceAssertOriginalPeer(t, f.peer, "K1ABC", at)
	want := correctionEvidenceDeliveryCase{wantCall: tc.wantCall, wantGlyph: tc.wantGlyph, wantPercent: 50}
	rows := f.ring.GetRecent(2)
	if len(rows) != 1 {
		t.Errorf("sealed ring count=%d want 1", len(rows))
	} else {
		correctionEvidenceAssertSpot(t, "sealed ring", rows[0], want, at, true)
		correctionSNRAssertReport(t, "sealed ring", rows[0], tc)
		if rows[0].SnapshotForAsync() != rows[0] {
			t.Error("delivered ring row was not sealed")
		}
	}
	f.writer.Stop()
	reopened, err := archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	rows, err = reopened.Recent(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Errorf("reopened archive count=%d want 1", len(rows))
	} else {
		correctionEvidenceAssertSpot(t, "reopened archive", rows[0], want, at, false)
		correctionSNRAssertReport(t, "reopened archive", rows[0], tc)
	}
}

func correctionSNRAddSpot(t *testing.T, r *spot.SignalResolver, cfg config.CallCorrectionConfig, call, receiver string, report int, present bool, observed time.Time) {
	t.Helper()
	s := spot.NewSpotNormalized(call, receiver, 14020, "CW")
	s.Time, s.Report, s.HasReport = observed, report, present
	evidence, ok := correctionflow.BuildResolverEvidenceSnapshot(s, cfg, nil, observed)
	if !ok || evidence.Report != report || !r.Enqueue(evidence) {
		t.Fatal("shared builder or bounded queue refused normalized fixture spot")
	}
}

func correctionSNRAssertReport(t *testing.T, destination string, s *spot.Spot, tc correctionSNRDeliveryCase) {
	t.Helper()
	if s == nil || s.HasReport != tc.outputPresent || s.Report != tc.outputReport {
		t.Errorf("%s changed original report presence/value: spot=%+v want present=%t report=%d", destination, s, tc.outputPresent, tc.outputReport)
	}
}
