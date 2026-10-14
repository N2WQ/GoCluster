package main

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	rbnfeed "dxcluster/rbn"
	"dxcluster/spot"
	"dxcluster/uls"
)

func replayLicenseDB(t *testing.T, calls ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fcc.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(t.Context(), "CREATE TABLE AM(call_sign TEXT PRIMARY KEY,state TEXT); PRAGMA user_version=1;"); err != nil {
		t.Fatal(err)
	}
	for _, call := range calls {
		if _, err = db.ExecContext(t.Context(), "INSERT INTO AM VALUES(?,'CA');", call); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func replayLicenseCTY(t *testing.T) *cty.CTYDatabase {
	t.Helper()
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(`<?xml version="1.0"?><plist version="1.0"><dict><key>K</key><dict><key>Country</key><string>United States</string><key>Prefix</key><string>K</string><key>ADIF</key><integer>291</integer></dict><key>W</key><dict><key>Prefix</key><string>K</string><key>ADIF</key><integer>291</integer></dict><key>VE</key><dict><key>Prefix</key><string>VE</string><key>ADIF</key><integer>1</integer></dict><key>CG</key><dict><key>Prefix</key><string>VE</string><key>ADIF</key><integer>1</integer></dict></dict></plist>`))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func replayLicenseGlobals(t *testing.T, path string) {
	t.Helper()
	uls.SetLicenseDBPath(path)
	uls.SetLicenseChecksEnabled(true)
	uls.SetAllowlistPath("")
	t.Cleanup(func() {
		uls.CloseLicenseDatabases()
		uls.SetAllowlistPath("")
		uls.SetLicenseChecksEnabled(true)
		uls.SetCanadianLicenseChecksEnabled(true)
	})
}

// This calls the existing replay API with admitted real resolver evidence. It
// detects identity mutation before licensing, rather than merely checking the
// newly shared predicate.
func TestReplayLicenseRejectsReplacementBeforeMutation(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	replayLicenseGlobals(t, replayLicenseDB(t, "K1ABD"))
	cfg := replayTemporalFreshConfig()
	r, key := replayTemporalFreshRunner(t, cfg, at, 2*time.Minute)
	r.ctyDB = replayLicenseCTY(t)
	s := replayTemporalFreshSpot("K1ABD", at)
	s.ConfidencePercent, s.ConfidencePercentOK = 25, true
	s.DXMetadata.State, s.DXMetadata.Grid = "TX", "EM12"
	before := s.Clone()
	out := maybeApplyResolverCorrectionReplay(s, r.resolver, spot.ResolverEvidence{Key: key}, true, cfg, r.ctyDB, r.tracker, nil, nil, at)
	if out.Applied || out.Suppress || s.DXCallNorm != "K1ABD" || s.Confidence == "C" || r.tracker.CallCorrections() != 0 {
		t.Fatalf("ineligible replacement mutated original: spot=%+v outcome=%+v corrections=%d", s, out, r.tracker.CallCorrections())
	}
	if s.ConfidencePercent != 25 || !s.ConfidencePercentOK || s.DXMetadata != before.DXMetadata || !s.Time.Equal(before.Time) {
		t.Fatalf("rejected proposal changed original confidence/metadata/time: %+v", s)
	}
	if r.tracker.CorrectionDecisionReasons()[resolverDecisionLicenseReject] != 1 {
		t.Fatal("fixture did not reach replacement license gate")
	}
	r.processOutcome(s, out, at)
	if r.abMetrics.Output.ConfidenceCounts.Samples != 1 || r.abMetrics.Output.ConfidenceCounts.C != 0 {
		t.Fatal("licensed retained original was not emitted normally")
	}
}

func TestReplayLicenseAcceptedMutationSurvivesFinalDropAccounting(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	replayLicenseGlobals(t, replayLicenseDB(t, "K1ABC", "K1ABD"))
	r, key := replayTemporalFreshRunner(t, replayTemporalFreshConfig(), at, 2*time.Minute)
	r.ctyDB = replayLicenseCTY(t)
	r.recentBandStore = spot.NewRecentBandStore(2 * time.Minute)
	s := replayTemporalFreshSpot("K1ABD", at)
	replayTemporalFreshHold(t, r, key, s, at, "K1ABC")
	releaseAt := at.Add(2 * time.Second)
	var id uint64
	for pendingID := range r.temporalPending {
		id = pendingID
	}
	decision := r.temporalDecoder.Evaluate(id, releaseAt, false)
	if decision.Action != correctionflow.TemporalDecisionActionApply {
		t.Fatalf("real temporal proposal=%+v", decision)
	}
	out := r.temporalReleaseOutcome(s, spot.ResolverEvidence{Key: key}, true, decision, releaseAt)
	if !out.Applied || s.DXCallNorm != "K1ABC" || r.tracker.CallCorrections() != 1 {
		t.Fatalf("positive replacement control did not commit: %+v", out)
	}
	// Fault injection models a changed final authority after accepted mutation.
	// Frozen replay never refreshes its source; accounting still must preserve
	// the accepted mutation if a final gate rejects it.
	uls.SetLicenseDBPath(replayLicenseDB(t, "K1ABD"))
	r.processOutcome(s, out, releaseAt)
	if r.tracker.CallCorrections() != 1 || r.abMetrics.Temporal.Committed != 1 || r.abMetrics.Temporal.ConfirmedHistoryKeys != 1 || r.abMetrics.Temporal.HandoffAttempts != 0 || r.abMetrics.Output.ConfidenceCounts.Samples != 0 || len(r.stabilityCollector.corrections) != 0 {
		t.Fatalf("final drop conflated mutation and output: temporal=%+v output=%+v", r.abMetrics.Temporal, r.abMetrics.Output)
	}
	if r.recentBandStore.ActiveCallCount(releaseAt) != 0 {
		t.Fatal("final rejection learned recent-band support")
	}
}

func TestReplayLicenseTemporalReplacementRejectionKeepsOriginal(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	replayLicenseGlobals(t, replayLicenseDB(t, "K1ABD"))
	r, key := replayTemporalFreshRunner(t, replayTemporalFreshConfig(), at, 2*time.Minute)
	r.ctyDB = replayLicenseCTY(t)
	s := replayTemporalFreshSpot("K1ABD", at)
	s.ConfidencePercent, s.ConfidencePercentOK = 25, true
	replayTemporalFreshHold(t, r, key, s, at, "K1ABC")
	r.drainTemporal(at.Add(2*time.Second), false)
	if s.DXCallNorm != "K1ABD" || s.Confidence == "C" || s.ConfidencePercent != 25 || !s.ConfidencePercentOK || r.tracker.CallCorrections() != 0 || r.abMetrics.Temporal.Committed != 0 || r.abMetrics.Temporal.ConfirmedHistoryKeys != 0 || r.abMetrics.Temporal.HandoffAttempts != 1 || r.abMetrics.Output.ConfidenceCounts.Samples != 1 || len(r.stabilityCollector.corrections) != 0 {
		t.Fatalf("rejected temporal replacement changed accounting: spot=%+v temporal=%+v", s, r.abMetrics.Temporal)
	}
}

func TestReplayLicenseCSVFinalGate(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	replayLicenseGlobals(t, replayLicenseDB(t, "K1ABD"))
	r, _ := replayTemporalFreshRunner(t, replayTemporalFreshConfig(), at, 2*time.Minute)
	r.temporalDecoder = nil
	r.ctyDB = replayLicenseCTY(t)
	r.dayStart, r.dayEnd = at.Add(-12*time.Hour), at.Add(12*time.Hour)
	r.nextSampleAt = r.dayEnd
	row := rbnHistoryRow{Time: at.Add(time.Second), DXCall: "K1ABC", Spotter: "N0DDD", FreqKHz: 14020, Mode: "CW", Band: "20m", SpotClass: rbnfeed.SpotClassCQ}
	if err := r.processCSVRow(row, true); err != nil {
		t.Fatal(err)
	}
	if r.recordsProcessed != 1 || r.abMetrics.Output.ConfidenceCounts.Samples != 0 {
		t.Fatalf("real CSV row bypassed final absence: processed=%d output=%+v", r.recordsProcessed, r.abMetrics.Output)
	}
}

func replayCanadianLicenseDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ised.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(t.Context(), `CREATE TABLE CA(call_sign TEXT PRIMARY KEY,state TEXT);
CREATE TABLE Events(id INTEGER PRIMARY KEY,special TEXT,start_day TEXT,end_day TEXT,trustee TEXT,use_by TEXT,uncertain INTEGER,prefix_kind INTEGER);
CREATE INDEX idx_Events_special ON Events(special);
CREATE TABLE SourceMeta(id INTEGER PRIMARY KEY,main_sha TEXT,special_sha TEXT);
INSERT INTO SourceMeta VALUES(1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb');
INSERT INTO CA VALUES('VE3ABC','ON'),('VE3ABD','ON');
INSERT INTO Events VALUES(1,'CG3','2026-02-15','2026-02-15','','VE3',0,1);
PRAGMA user_version=1;`)
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("Canadian fixture: %v %v", err, closeErr)
	}
	return path
}

func TestReplayLicenseUsesHistoricalDateIncludingDelayedRelease(t *testing.T) {
	at := time.Date(2026, 2, 15, 23, 59, 59, 0, time.UTC)
	uls.SetCanadianLicenseDBPath(replayCanadianLicenseDB(t))
	uls.SetCanadianLicenseChecksEnabled(true)
	uls.SetAllowlistPath("")
	t.Cleanup(func() { uls.CloseLicenseDatabases(); uls.SetCanadianLicenseChecksEnabled(true) })
	for _, delayed := range []bool{false, true} {
		name := "immediate_historical_active"
		if delayed {
			name = "delayed_historical_expired"
		}
		t.Run(name, func(t *testing.T) {
			cfg := replayTemporalFreshConfig()
			r, _ := replayTemporalFreshRunner(t, cfg, at, 2*time.Minute)
			r.ctyDB = replayLicenseCTY(t)
			// Admit a separate Canadian 3:1 population, fresh on both sides of
			// historical midnight, without any US helper evidence remaining.
			r.resolver = spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 32, MaxActiveKeys: 4, MaxCandidatesPerKey: 4, MaxReportersPerCand: 8,
				InactiveTTL: 5 * time.Minute, EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond, HysteresisWindows: 1,
				FreqGuardMinSeparationKHz: 0.3, FreqGuardRunnerUpRatio: 0.6, MaxEditDistance: 6, DistanceModelCW: "morse", DistanceModelRTTY: "baudot"})
			var err error
			r.driver, err = spot.NewSignalResolverDriver(r.resolver)
			if err != nil {
				t.Fatal(err)
			}
			key := spot.NewResolverSignalKey(14020, "20m", "CW", 500)
			replayTemporalFreshSeed(t, r, key, at, 2*time.Minute, "CG3ABC", "CG3ABD", 1)
			s := replayTemporalFreshSpot("CG3ABD", at)
			if !delayed {
				// Eligibility is the historical check instant, independent of
				// the host's October date.
				out := maybeApplyResolverCorrectionReplay(s, r.resolver, spot.ResolverEvidence{Key: key}, true, cfg, r.ctyDB, r.tracker, nil, nil, at)
				if !out.Applied || s.DXCallNorm != "CG3ABC" {
					t.Fatalf("historical active replacement: %+v", out)
				}
				return
			}
			replayTemporalFreshHold(t, r, key, s, at, "CG3ABC")
			r.drainTemporal(at.Add(5*time.Second), false)
			if s.DXCallNorm != "CG3ABD" || s.Confidence == "C" || r.tracker.CallCorrections() != 0 || r.abMetrics.Output.ConfidenceCounts.Samples != 0 || r.abMetrics.Temporal.Committed != 0 || r.abMetrics.Temporal.HandoffAttempts != 0 {
				t.Fatalf("release used admission/host date: spot=%+v temporal=%+v", s, r.abMetrics.Temporal)
			}
		})
	}
}

func TestReplayLicenseFinalAbsenceDropsOutput(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	replayLicenseGlobals(t, replayLicenseDB(t, "K1ABD"))
	r, _ := replayTemporalFreshRunner(t, replayTemporalFreshConfig(), at, 2*time.Minute)
	r.ctyDB = replayLicenseCTY(t)
	s := replayTemporalFreshSpot("K1ABC", at)
	r.processOutcome(s, replayResolverApplyOutcome{Confidence: replayConfidenceOutcome{Final: "V"}}, at)
	if r.abMetrics.Output.ConfidenceCounts.Samples != 0 {
		t.Fatalf("confirmed-absent emitted DX advanced output metrics: %+v", r.abMetrics.Output)
	}
}
