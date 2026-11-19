package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/internal/testsupport"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

var methodApplyAt = time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)

func methodApplyOwner(t *testing.T, licensed ...string) (*replayRunner, spot.ResolverSignalKey) {
	t.Helper()
	replayLicenseGlobals(t, replayLicenseDB(t, licensed...))
	cfg := replayTemporalFreshConfig()
	cfg.DistanceModelCW, cfg.DistanceModelRTTY, cfg.MaxEditDistance = "plain", "plain", 2
	cfg.MinConsensusReports = 3
	r, key := ccv2CanonicalReplayOwner(t, cfg)
	if err := r.driver.SetProcessElapsed(0); err != nil {
		t.Fatal(err)
	}
	r.method = methodTestRun(t)
	r.method.prepared.Plan.Enabled = true
	for _, candidate := range r.method.prepared.Plan.Candidates {
		if candidate.Ranking == "geometry" && reflect.DeepEqual(candidate.CWDistances, []int{1, 2}) {
			r.method.prepared.Candidate = candidate
			return r, key
		}
	}
	t.Fatal("complete candidate grid has no geometry distance-2 candidate")
	return nil, key
}

// Distinct calls deliberately reuse receivers: their union is not the sum of
// supports. The far spelling remains an ordinary settled source of authority.
func methodApplyRows(t *testing.T, r *replayRunner, key spot.ResolverSignalKey, call string, count int, frequency float64, at time.Time, window time.Duration) {
	t.Helper()
	for n := 0; n < count; n++ {
		if !r.resolver.Enqueue(spot.ResolverEvidence{Key: key, DXCall: call, Spotter: fmt.Sprintf("N0AA%c", 'A'+rune(n)),
			ObservedAt: at, FrequencyKHz: frequency, RecencyWindow: window, Report: 18, HasReport: true}) {
			t.Fatal("actual resolver refused method evidence")
		}
	}
}

func methodApplyPublish(t *testing.T, r *replayRunner, count int, at time.Time) {
	t.Helper()
	if got := r.driver.Drain(); got != count {
		t.Fatalf("published %d rows, want %d", got, count)
	}
	r.driver.Sweep(at.Add(time.Millisecond))
}

func methodApplySeparated(t *testing.T, r *replayRunner, key spot.ResolverSignalKey) {
	t.Helper()
	methodApplyRows(t, r, key, "K1ABC", 4, 14020, methodApplyAt, 2*time.Minute)
	methodApplyRows(t, r, key, "DL9ZZZ", 6, 14020.2, methodApplyAt, 2*time.Minute)
	methodApplyPublish(t, r, 10, methodApplyAt)
}

func methodApplyEvidence(key spot.ResolverSignalKey) spot.ResolverEvidence {
	return spot.ResolverEvidence{Key: key, ObservedAt: methodApplyAt, Report: 18, HasReport: true}
}

func methodApplyReceipts(t *testing.T, r *replayRunner) []studyMethodReceipt {
	t.Helper()
	if err := r.method.receipts.Sync(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(r.method.receipts.path)
	if err != nil {
		t.Fatal(err)
	}
	var receipts []studyMethodReceipt
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var receipt studyMethodReceipt
		if err := json.Unmarshal(line, &receipt); err != nil {
			t.Fatal(err)
		}
		receipts = append(receipts, receipt)
	}
	return receipts
}

func TestCCV2F2OrdinaryFirst(t *testing.T) {
	for _, licensedWinner := range []bool{true, false} {
		t.Run(fmt.Sprintf("ordinary_licensed_%t", licensedWinner), func(t *testing.T) {
			calls := []string{"K1ABD", "K1ABE"}
			if licensedWinner {
				calls = append(calls, "K1ABC")
			}
			r, key := methodApplyOwner(t, calls...)
			methodApplyRows(t, r, key, "K1ABC", 4, 14020, methodApplyAt, time.Minute)
			methodApplyRows(t, r, key, "K1ABE", 3, 14020, methodApplyAt, time.Minute)
			methodApplyPublish(t, r, 7, methodApplyAt)
			s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
			out := r.applyResolverOutcome(s, methodApplyEvidence(key), true, methodApplyAt.Add(time.Millisecond))
			if !out.GateEvaluated || !out.Gate.Allow || out.Applied != licensedWinner || out.replacementRejected == licensedWinner {
				t.Fatalf("ordinary control did not reach final validation: %+v", out)
			}
			want := "K1ABD"
			if licensedWinner {
				want = "K1ABC"
			}
			if s.DXCallNorm != want || len(methodApplyReceipts(t, r)) != 0 || out.methodWinner != "" {
				t.Fatalf("terminal ordinary result searched another candidate: call=%s outcome=%+v", s.DXCallNorm, out)
			}
		})
	}
}

func TestCCV2F2GateAndLicenseBoundary(t *testing.T) {
	t.Run("first_allowed_cty_failure_is_terminal", func(t *testing.T) {
		r, key := methodApplyOwner(t, "K1ABD", "K1ABC", "K1ABE")
		var err error
		r.ctyDB, err = cty.LoadCTYDatabaseFromReader(strings.NewReader(`<plist><dict><key>K1ABE</key><dict><key>Country</key><string>United States</string><key>Prefix</key><string>K1ABE</string><key>ADIF</key><integer>291</integer><key>ExactCallsign</key><true/></dict></dict></plist>`))
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := r.ctyDB.LookupCallsignPortable("K1ABE"); !ok {
			t.Fatal("valid CTY runner-up control missing")
		}
		r.cfg.CallCorrection.InvalidAction = "suppress"
		methodApplyRows(t, r, key, "K1ABC", 4, 14020, methodApplyAt, time.Minute)
		methodApplyRows(t, r, key, "K1ABE", 3, 14020, methodApplyAt, time.Minute)
		methodApplyRows(t, r, key, "DL9ZZZ", 6, 14020.2, methodApplyAt, time.Minute)
		methodApplyPublish(t, r, 13, methodApplyAt)
		s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
		out := r.applyResolverOutcome(s, methodApplyEvidence(key), true, methodApplyAt.Add(time.Millisecond))
		receipts := methodApplyReceipts(t, r)
		if out.Applied || !out.Suppress || !out.replacementRejected || out.methodReason != "frequency_"+resolverDecisionCTYMiss || len(receipts) != 1 || len(receipts[0].Trials) != 1 || s.DXCallNorm != "K1ABD" {
			t.Fatalf("CTY validation failure advanced or retried: %+v", out)
		}
	})
	t.Run("gate_rejection_advances", func(t *testing.T) {
		r, key := methodApplyOwner(t, "K1ABD", "K1ABC")
		r.cfg.CallCorrection.FamilyPolicy.Truncation = config.CallCorrectionTruncationFamilyConfig{
			Enabled: true, MaxLengthDelta: 2, MinShorterLength: 3, AllowPrefixMatch: true,
			Delta2Rails: config.CallCorrectionTruncationDelta2RailsConfig{Enabled: true, RequireCandidateValidated: true}}
		methodApplyRows(t, r, key, "K1ABDXY", 4, 14020, methodApplyAt, time.Minute)
		methodApplyRows(t, r, key, "K1ABC", 3, 14020, methodApplyAt, time.Minute)
		methodApplyRows(t, r, key, "DL9ZZZ", 6, 14020.2, methodApplyAt, time.Minute)
		methodApplyPublish(t, r, 13, methodApplyAt)
		s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
		out := r.applyResolverOutcome(s, methodApplyEvidence(key), true, methodApplyAt.Add(time.Millisecond))
		receipts := methodApplyReceipts(t, r)
		if !out.Applied || s.DXCallNorm != "K1ABC" || len(receipts) != 1 || len(receipts[0].Trials) != 2 {
			t.Fatalf("gate rejection did not advance to valid proposal: %+v receipts=%+v", out, receipts)
		}
		trials := receipts[0].Trials
		if trials[0].Proposal.Call != "K1ABDXY" || trials[0].Gate.Reason != "truncation_delta2_candidate_unvalidated" || !trials[1].Gate.Allow {
			t.Fatalf("fixture missed distinct gate/validation boundaries: %+v", trials)
		}
	})
	t.Run("first_allowed_license_failure_is_terminal", func(t *testing.T) {
		r, key := methodApplyOwner(t, "K1ABD", "K1ABE")
		methodApplyRows(t, r, key, "K1ABC", 4, 14020, methodApplyAt, time.Minute)
		methodApplyRows(t, r, key, "DL9ZZZ", 6, 14020.2, methodApplyAt, time.Minute)
		methodApplyRows(t, r, key, "K1ABE", 3, 14020, methodApplyAt, time.Minute)
		methodApplyPublish(t, r, 13, methodApplyAt)
		s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
		out := r.applyResolverOutcome(s, methodApplyEvidence(key), true, methodApplyAt.Add(time.Millisecond))
		receipts := methodApplyReceipts(t, r)
		if out.Applied || !out.replacementRejected || s.DXCallNorm != "K1ABD" || len(receipts) != 1 || len(receipts[0].Trials) != 1 || len(receipts[0].Assessment.Proposals) != 2 {
			t.Fatalf("license rejection searched a licensed runner-up: %+v receipts=%+v", out, receipts)
		}
		if trial := receipts[0].Trials[0]; trial.Proposal.Call != "K1ABC" || !trial.Gate.Allow || out.methodReason != "frequency_"+resolverDecisionLicenseReject {
			t.Fatalf("first candidate never crossed the license boundary: %+v", out)
		}
	})
}

func TestCCV2F2SubjectLicenseRescue(t *testing.T) {
	r, key := methodApplyOwner(t, "K1ABC")
	methodApplySeparated(t, r, key)
	s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
	if !rejectReplayDXLicense(r.ctyDB, s.DXCallNorm, false, methodApplyAt) {
		t.Fatal("negative subject-license control is not absent")
	}
	out := r.applyResolverOutcome(s, methodApplyEvidence(key), true, methodApplyAt.Add(time.Millisecond))
	if !out.Applied || out.replacementRejected || s.DXCallNorm != "K1ABC" || s.Confidence != "C" || r.tracker.CallCorrections() != 1 {
		t.Fatalf("valid rescue did not precede final subject licensing: %+v", out)
	}
	r.processOutcome(s, out, methodApplyAt.Add(time.Millisecond))
	if r.abMetrics.Output.ConfidenceCounts.Samples != 1 || r.abMetrics.Output.ConfidenceCounts.C != 1 || len(r.stabilityCollector.corrections) != 1 {
		t.Fatal("accepted replacement failed the actual final-output boundary")
	}
}

func TestCCV2F2DualEvidence(t *testing.T) {
	r, key := methodApplyOwner(t, "K1ABC", "K1ABD")
	methodApplySeparated(t, r, key)
	before := correctionflow.SelectResolverPrimarySnapshotForCallAt(r.resolver, key, r.cfg.CallCorrection, "K1ABD", methodApplyAt.Add(time.Millisecond))
	if !before.SnapshotOK || before.Snapshot.Winner != "DL9ZZZ" || before.Snapshot.TotalReporters != 6 {
		t.Fatalf("ordinary settled-population control failed: %+v", before)
	}
	s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
	s.ConfidencePercent, s.ConfidencePercentOK, s.Report, s.HasReport, s.DEGridNorm = 23, true, 18, true, "FN31AA"
	out := r.applyResolverOutcome(s, methodApplyEvidence(key), true, methodApplyAt.Add(time.Millisecond))
	receipts := methodApplyReceipts(t, r)
	if !out.Applied || !reflect.DeepEqual(out.Selection, before) || len(receipts) != 1 || len(receipts[0].Trials) != 1 {
		t.Fatalf("group assessment replaced ordinary selection: %+v receipts=%+v", out, receipts)
	}
	receipt := receipts[0]
	if receipt.OrdinaryPublicPercent != 66 || !receipt.OrdinaryPublicPercentPresent || receipt.Trials[0].Gate.WinnerConfidence != 100 || s.ConfidencePercent != 23 || !s.ConfidencePercentOK || s.Confidence != "C" {
		t.Fatalf("group gate/public population or existing replay percentage changed: %+v spot=%+v", receipt, s)
	}
	if err := pathreliability.InitH3MappingsFromDir(testsupport.H3TableDir(t)); err != nil {
		t.Fatal(err)
	}
	history, err := spot.OpenCustomSCPStore(spot.CustomSCPOptions{Path: filepath.Join(t.TempDir(), "history"), HorizonDays: 90, CoreMinScore: 1, CoreMinH3Cells: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = history.Close() })
	r.recentBandStore, r.cfg.CallCorrection.CustomSCP.Enabled = history, true
	r.processOutcome(s, out, methodApplyAt.Add(time.Millisecond))
	if stats := history.StatsSnapshot(); stats.ObservationKeys != 0 || stats.ObservationSpotters != 0 {
		t.Fatalf("actual C output entered V-only learning: %+v", stats)
	}
	positive := s.Clone()
	positive.Confidence = "V"
	recordRecentBandObservation(positive, history, r.cfg.CallCorrection, methodApplyAt.Add(time.Millisecond))
	if stats := history.StatsSnapshot(); stats.ObservationKeys != 1 || stats.ObservationSpotters != 1 {
		t.Fatalf("identical-metadata V control did not learn: %+v", stats)
	}
}

func TestCCV2F2CurrentPublishedMatch(t *testing.T) {
	r, key := methodApplyOwner(t, "K1ABC", "K1ABD")
	methodApplyRows(t, r, key, "DL9ZZZ", 6, 14020.2, methodApplyAt, time.Minute)
	methodApplyPublish(t, r, 6, methodApplyAt)
	methodApplyRows(t, r, key, "K1ABC", 4, 14020, methodApplyAt, time.Minute)
	s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
	out := r.applyResolverOutcome(s, methodApplyEvidence(key), true, methodApplyAt.Add(time.Millisecond))
	if out.Applied || s.DXCallNorm != "K1ABD" || len(methodApplyReceipts(t, r)) != 1 {
		t.Fatal("unpublished evidence produced a replacement")
	}
	if got := r.driver.Drain(); got != 4 {
		t.Fatalf("method consumed the queued focal population: remaining=%d", got)
	}
	r.driver.Sweep(methodApplyAt.Add(2 * time.Millisecond))
	positive := replayTemporalFreshSpot("K1ABD", methodApplyAt)
	if out := r.applyResolverOutcome(positive, methodApplyEvidence(key), true, methodApplyAt.Add(2*time.Millisecond)); !out.Applied || positive.DXCallNorm != "K1ABC" {
		t.Fatalf("explicit publication positive control failed: %+v", out)
	}
}

func TestCCV2F2TemporalAuthority(t *testing.T) {
	for _, action := range []correctionflow.TemporalDecisionAction{correctionflow.TemporalDecisionActionApply, correctionflow.TemporalDecisionActionAbstain, correctionflow.TemporalDecisionActionBypass, correctionflow.TemporalDecisionActionFallbackResolver} {
		t.Run(string(action), func(t *testing.T) {
			r, key := methodApplyOwner(t, "K1ABC", "K1ABD", "K1ABE")
			methodApplyRows(t, r, key, "K1ABC", 4, 14020, methodApplyAt, time.Minute)
			methodApplyRows(t, r, key, "K1ABE", 3, 14020, methodApplyAt, time.Minute)
			methodApplyRows(t, r, key, "DL9ZZZ", 10, 14020.2, methodApplyAt, time.Minute)
			methodApplyPublish(t, r, 17, methodApplyAt)
			now := methodApplyAt.Add(time.Millisecond)
			selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(r.resolver, key, r.cfg.CallCorrection, "K1ABD", now)
			decision := correctionflow.TemporalDecision{Action: action, Key: key, Winner: "K1ABE", Selection: selection}
			saved := correctionflow.FrequencySourceLifecycles(correctionflow.ReadFrequencySources(r.resolver, key, r.cfg.CallCorrection, now))
			if action == correctionflow.TemporalDecisionActionApply {
				if _, permitted := correctionflow.SelectTemporalReleaseSnapshotWithAuthority(r.resolver, key, r.cfg.CallCorrection, "K1ABD", decision, now); !permitted {
					t.Fatal("actual current publication did not permit the saved lower-rank identity")
				}
			}
			s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
			out := r.temporalReleaseOutcomeMethod(s, methodApplyEvidence(key), true, decision, now, &saved, true)
			want := "K1ABD"
			switch action {
			case correctionflow.TemporalDecisionActionApply:
				want = "K1ABE"
			case correctionflow.TemporalDecisionActionFallbackResolver:
				want = "K1ABC"
			}
			if s.DXCallNorm != want || out.Applied != (want != "K1ABD") {
				t.Fatalf("temporal action changed authority: %+v call=%s want=%s", out, s.DXCallNorm, want)
			}
			if receipts := methodApplyReceipts(t, r); action == correctionflow.TemporalDecisionActionApply && (len(receipts) != 1 || len(receipts[0].Trials) != 1 || receipts[0].Trials[0].Proposal.Call != "K1ABE") {
				t.Fatalf("Apply searched an identity other than the permitted proposal: %+v", receipts)
			}
			if action == correctionflow.TemporalDecisionActionApply {
				bad := decision
				bad.Selection.Snapshot.Key.Bucket++
				before := len(methodApplyReceipts(t, r))
				if _, permitted := correctionflow.SelectTemporalReleaseSnapshotWithAuthority(r.resolver, key, r.cfg.CallCorrection, "K1ABD", bad, now); permitted {
					t.Fatal("invalid saved anchor did not fail release guard")
				}
				guarded := replayTemporalFreshSpot("K1ABD", methodApplyAt)
				refused := r.temporalReleaseOutcomeMethod(guarded, methodApplyEvidence(key), true, bad, now, &saved, true)
				if refused.Applied || guarded.DXCallNorm != "K1ABD" || len(methodApplyReceipts(t, r)) != before {
					t.Fatal("failed temporal release guard allowed group rescue")
				}
			}
			// Overflow uses the existing wrapper and never receives extra rescue.
			overflow := replayTemporalFreshSpot("K1ABD", methodApplyAt)
			decision.Action = correctionflow.TemporalDecisionActionFallbackResolver
			if out := r.temporalReleaseOutcome(overflow, methodApplyEvidence(key), true, decision, now); out.Applied || overflow.DXCallNorm != "K1ABD" {
				t.Fatal("overflow wrapper introduced experimental nomination")
			}
		})
	}
}

func TestCCV2F2ReleaseFreshness(t *testing.T) {
	for _, change := range []string{"frequency_same_generation", "clock_expiry_same_generation", "previously_missing_neighbor", "reused_key"} {
		t.Run(change, func(t *testing.T) {
			r, key := methodApplyOwner(t, "K1ABC", "K1ABD")
			if change == "previously_missing_neighbor" {
				r.cfg.CallCorrection.ResolverNeighborhoodEnabled, r.cfg.CallCorrection.ResolverNeighborhoodBucketRadius = true, 1
			}
			methodApplySeparated(t, r, key)
			now := methodApplyAt.Add(time.Millisecond)
			sources := correctionflow.ReadFrequencySources(r.resolver, key, r.cfg.CallCorrection, now)
			saved := correctionflow.FrequencySourceLifecycles(sources)
			ordinary := correctionflow.SelectResolverPrimarySnapshotForCallAt(r.resolver, key, r.cfg.CallCorrection, "K1ABD", now)
			control := correctionflow.AssessFrequencyProposals(key, "K1ABD", 14020, ordinary, sources, r.cfg.CallCorrection,
				correctionflow.FrequencyProposalOptions{GapHz: 150, MaxWitnessSeparation: time.Minute, MaxDistance: 2})
			if len(control.Proposals) != 1 || control.Proposals[0].Call != "K1ABC" {
				t.Fatalf("freshness test lacks positive proposal control: %+v", control)
			}
			switch change {
			case "reused_key":
				r.driver.Sweep(methodApplyAt.Add(3 * time.Minute))
				if _, present := r.resolver.Lookup(key); present {
					t.Fatal("retirement control still has the old key")
				}
				now = methodApplyAt.Add(3*time.Minute + time.Second)
				methodApplyRows(t, r, key, "K1ABC", 4, 14020, now, 2*time.Minute)
				methodApplyRows(t, r, key, "DL9ZZZ", 6, 14020.2, now, 2*time.Minute)
				methodApplyPublish(t, r, 10, now)
			case "frequency_same_generation":
				now = methodApplyAt.Add(time.Second)
				methodApplyRows(t, r, key, "K1ABC", 4, 14020.2, now, 2*time.Minute)
				methodApplyPublish(t, r, 4, now)
			case "clock_expiry_same_generation":
				now = methodApplyAt.Add(2*time.Minute + time.Second)
			case "previously_missing_neighbor":
				now = methodApplyAt.Add(time.Second)
				neighbor := key
				neighbor.Bucket++
				methodApplyRows(t, r, neighbor, "DL9ZZZ", 2, 14020.5, now, 2*time.Minute)
				methodApplyPublish(t, r, 2, now)
			}
			current := correctionflow.FrequencySourceLifecycles(correctionflow.ReadFrequencySources(r.resolver, key, r.cfg.CallCorrection, now))
			lifecycleChange := change == "previously_missing_neighbor" || change == "reused_key"
			if !lifecycleChange && current != saved {
				t.Fatal("same-generation freshness counterexample accidentally changed lifecycle")
			}
			s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
			base := maybeApplyResolverCorrectionReplay(s, r.resolver, methodApplyEvidence(key), true, r.cfg.CallCorrection, r.ctyDB, r.tracker, nil, nil, now)
			out := r.applyMethodRescue(s, methodApplyEvidence(key), true, now, base, "K1ABC", &saved)
			receipts := methodApplyReceipts(t, r)
			if out.Applied || s.DXCallNorm != "K1ABD" || len(receipts) != 1 || len(receipts[0].Trials) != 0 {
				t.Fatalf("saved counts/association revived a stale proposal: %+v receipts=%+v", out, receipts)
			}
			if lifecycleChange && (current == saved || receipts[0].LifecycleCurrent || receipts[0].Assessment.Reason != "lifecycle_changed") {
				t.Fatal("formerly excluded source creation escaped lifecycle vector")
			}
		})
	}
}

type methodHistoryProbe struct {
	spot.RecentSupportStore
	queries int
}

func (p *methodHistoryProbe) RecentSupportCount(string, string, string, time.Time) int {
	p.queries++
	return 0
}

func TestCCV2F2BeamAndHistory(t *testing.T) {
	t.Run("ninth_sparse_candidate_forces_whole_geometry", func(t *testing.T) {
		r, key := methodApplyOwner(t, "K1ABD")
		r.cfg.CallCorrection.MinConsensusReports = 100
		model, err := studyLoadOrdinalModel(studyFile{Path: "testdata/ordinal_fitted_v7.json", SHA256: "968caf263f3bf44c13509ded8b5e48faf0bd39b1d7cc42dcd9d1e6c0a45e5e1a"})
		if err != nil {
			t.Fatal(err)
		}
		r.method.prepared.Model = model
		r.method.prepared.Candidate.Ranking = "ordinal"
		calls := []string{"K1ABA", "K1ABB", "K1ABC", "K1ABE", "K1ABF", "K1ABG", "K1ABH", "K1ABI", "K1AB/K"}
		for i, call := range calls {
			methodApplyRows(t, r, key, call, 9-i, 14020, methodApplyAt, time.Minute)
		}
		methodApplyRows(t, r, key, "DL9ZZZ", 20, 14020.2, methodApplyAt, time.Minute)
		methodApplyPublish(t, r, 65, methodApplyAt)
		evidence := methodApplyEvidence(key)
		evidence.Report = 6
		if _, ok := model.Score("K1ABA", "K1ABD", "CW", 6, true); !ok {
			t.Fatal("scored first-eight control unavailable")
		}
		if _, ok := model.Score("K1AB/K", "K1ABD", "CW", 6, true); ok {
			t.Fatal("ninth sparse slash control scored")
		}
		s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
		r.applyResolverOutcome(s, evidence, true, methodApplyAt.Add(time.Millisecond))
		receipts := methodApplyReceipts(t, r)
		if len(receipts) != 1 || len(receipts[0].Assessment.Proposals) != 9 || receipts[0].OrdinalAvailable || len(receipts[0].Trials) != 8 {
			t.Fatalf("consumer ranked/truncated before availability check: %+v", receipts)
		}
		for i, trial := range receipts[0].Trials {
			if trial.Proposal.Call != calls[i] {
				t.Fatalf("partial score changed geometry ordering: %+v", receipts[0].Trials)
			}
		}
	})
	t.Run("actual_nine_nominees_eight_gate_trials", func(t *testing.T) {
		r, key := methodApplyOwner(t, "K1ABD")
		r.cfg.CallCorrection.MinConsensusReports = 100
		for i, suffix := range "ABCEFGHIJ" {
			methodApplyRows(t, r, key, "K1AB"+string(suffix), 9-i, 14020, methodApplyAt, time.Minute)
		}
		methodApplyRows(t, r, key, "DL9ZZZ", 20, 14020.2, methodApplyAt, time.Minute)
		methodApplyPublish(t, r, 65, methodApplyAt)
		s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
		out := r.applyResolverOutcome(s, methodApplyEvidence(key), true, methodApplyAt.Add(time.Millisecond))
		receipts := methodApplyReceipts(t, r)
		if out.Applied || len(receipts) != 1 || len(receipts[0].Assessment.Proposals) != 9 || !receipts[0].Assessment.BeamLimited || len(receipts[0].Trials) != 8 {
			t.Fatalf("real consumer did not enforce eight trials after nomination: %+v receipts=%+v", out, receipts)
		}
	})
	t.Run("existing_gate_alias_queries_bounded_per_trial", func(t *testing.T) {
		r, _ := methodApplyOwner(t)
		probe := &methodHistoryProbe{}
		r.recentBandStore = probe
		r.cfg.CallCorrection.FamilyPolicy.Truncation = config.CallCorrectionTruncationFamilyConfig{
			Enabled: true, MaxLengthDelta: 2, MinShorterLength: 3, AllowPrefixMatch: true}
		s := replayTemporalFreshSpot("K1ABC", methodApplyAt)
		p := correctionflow.FrequencyProposal{Call: "K1ABCD", Identity: "K1ABCD", Support: 2, WeightedSupportMilli: 2000, ConfidencePercent: 100}
		for range 8 {
			before := probe.queries
			r.evaluateMethodGate(s, "K1ABC", p, methodApplyAt)
			if added := probe.queries - before; added == 0 || added > 8 {
				t.Fatalf("one existing gate queried %d aliases, want 1..8", added)
			}
		}
		if probe.queries > 64 {
			t.Fatalf("eight extra gates exceeded 64 existing alias queries: %d", probe.queries)
		}
	})
}

func TestCCV2F2DecoderEmissionIsolation(t *testing.T) {
	var decisions []correctionflow.TemporalDecision
	var selections []correctionflow.ResolverPrimarySelection
	for _, enabled := range []bool{false, true} {
		r, key := methodApplyOwner(t, "K1ABD", "K1ABC")
		r.method.prepared.Plan.Enabled = enabled
		methodApplySeparated(t, r, key)
		now := methodApplyAt.Add(time.Second)
		e := spot.ResolverEvidence{Key: key, DXCall: "K1ABD", Spotter: "N0ZZZ", ObservedAt: now, FrequencyKHz: 14020, RecencyWindow: 2 * time.Minute, HasReport: true, Report: 6}
		if !r.resolver.Enqueue(e) {
			t.Fatal("focal evidence refused")
		}
		s := replayTemporalFreshSpot("K1ABD", now)
		if !r.maybeHoldTemporal(s, e, true, now) || len(r.temporalPending) != 1 {
			t.Fatal("decoder hold control not reached")
		}
		pending := r.temporalPending[1]
		selections = append(selections, pending.selection)
		decision := r.evaluateTemporal(1, s, now.Add(2*time.Second), true)
		if decision.Action == correctionflow.TemporalDecisionActionDefer {
			t.Fatal("forced decoder decision remained deferred")
		}
		decisions = append(decisions, decision)
	}
	if !reflect.DeepEqual(selections[0], selections[1]) || !reflect.DeepEqual(decisions[0], decisions[1]) {
		t.Fatalf("method altered ordinary temporal observation/emission: %+v / %+v", decisions[0], decisions[1])
	}
}
