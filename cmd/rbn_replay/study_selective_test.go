package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
)

func methodCandidatesV2() []studyMethodCandidate {
	return []studyMethodCandidate{
		{ID: "c_f150_t60_d3", GapHz: 150, MaxWitnessSeconds: 60, Ranking: "selective", CWDistances: []int{1, 2, 3}},
		{ID: "b_f150_t60_d3", GapHz: 150, MaxWitnessSeconds: 60, Ranking: "receiver_compatibility", CWDistances: []int{1, 2, 3}},
	}
}

func methodTestPlanV2(t testing.TB) studyMethodPlan {
	t.Helper()
	plan := methodTestPlan(t)
	plan.SchemaVersion, plan.Candidates = 2, methodCandidatesV2()
	return plan
}

func TestCCV2V8MethodPlan(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		plan := methodTestPlanV2(t)
		plan.Enabled = enabled
		req, arm := methodTestRequest(t, plan)
		req.Date = "2026-11-07"
		for _, candidate := range plan.Candidates {
			req.MethodCandidate = candidate.ID
			got, err := prepareStudyMethod(req, arm)
			if err != nil || got == nil || got.Plan.SchemaVersion != 2 || got.Candidate.Ranking != candidate.Ranking {
				t.Fatalf("explicit v2 enabled=%t candidate=%s: %v", enabled, candidate.ID, err)
			}
		}
		for _, date := range []string{"2026-11-15", "2026-11-16"} {
			req.Date = date
			_, err := prepareStudyMethod(req, arm)
			if (err == nil) == enabled {
				t.Fatalf("active=%t date=%s: %v", enabled, date, err)
			}
		}
		req.Date, req.Phase = "2026-11-07", "holdout"
		if _, err := prepareStudyMethod(req, arm); err == nil {
			t.Fatal("v2 granted confirmation authority")
		}
	}
	for _, mutate := range []func(*studyMethodPlan){
		func(p *studyMethodPlan) { p.SchemaVersion = 1 },
		func(p *studyMethodPlan) { p.Candidates = p.Candidates[:1] },
		func(p *studyMethodPlan) { p.Candidates[1] = p.Candidates[0] },
		func(p *studyMethodPlan) { p.Candidates[0].GapHz = 250 },
		func(p *studyMethodPlan) { p.Candidates[0].CWDistances = []int{1, 2} },
		func(p *studyMethodPlan) { p.Candidates[0].CWDistances = []int{1, 2, 3, 4} },
		func(p *studyMethodPlan) { p.Candidates[1].Ranking = "unknown" },
		func(p *studyMethodPlan) { p.SelectedCandidate = p.Candidates[0].ID },
	} {
		p := methodTestPlanV2(t)
		mutate(&p)
		req, arm := methodTestRequest(t, p)
		req.Date = "2026-11-07"
		if _, err := prepareStudyMethod(req, arm); err == nil {
			t.Fatalf("accepted incomplete/misversioned plan: %+v", p)
		}
	}
}

func TestCCV2V8EffectiveOffFingerprint(t *testing.T) {
	// Derived by marshaling the actual 21c763e5 config.go through a Go source
	// overlay before adding this setting, not by deleting a field at test time.
	const oldCorrection = "189fa83f3a4e73ef8aeb2d7c396536233237a730bc0a1de0c790bb12ae7165ba"
	const oldPipeline = "2fd37decebc30b84231594e3559f7bb1d8510f0a0a194a80bbff40f9abab33a3"
	cfg := config.Config{}
	if hashAnyStable(cfg.CallCorrection) != oldCorrection {
		t.Fatal("false changed the legacy correction fingerprint")
	}
	got, err := studyPipelineFingerprint(&cfg)
	if err != nil || got != oldPipeline {
		t.Fatalf("false changed legacy pipeline fingerprint: %s %v", got, err)
	}
	cfg.CallCorrection.SelectiveRescueEnabled = true
	data, err := json.Marshal(cfg.CallCorrection)
	if err != nil || !bytes.Contains(data, []byte(`"SelectiveRescueEnabled":true`)) || hashAnyStable(cfg.CallCorrection) == oldCorrection {
		t.Fatal("true was omitted from effective settings")
	}
	got, err = studyPipelineFingerprint(&cfg)
	if err != nil || got == oldPipeline {
		t.Fatal("true did not change pipeline fingerprint")
	}
	for _, enabled := range []bool{false, true} {
		method := &studyMethodPrepared{Plan: studyMethodPlan{Enabled: enabled}}
		if err := studyMethodRuntimeAuthority(&cfg, method); err == nil {
			t.Fatal("live flag granted a second experimental authority")
		}
		out := filepath.Join(t.TempDir(), "must-not-exist")
		err := studyRunDayPreparedMethod(studyPlan{}, "", studyCLIRequest{Output: out}, studyArm{}, studyCandidate{}, studyInput{}, &cfg, nil, false, nil, method)
		if err == nil || !strings.Contains(err.Error(), "selective_rescue_enabled false") {
			t.Fatalf("real runner did not refuse conflicting authority first: %v", err)
		}
		if _, err := os.Stat(out); !os.IsNotExist(err) {
			t.Fatal("conflicting authority constructed output/input owners")
		}
	}
}

func TestCCV2V8SelectiveReplay(t *testing.T) {
	for _, competitor := range []bool{false, true} {
		r, key := methodApplyOwner(t, "K1ABC", "K1ABD")
		r.cfg.CallCorrection.MaxEditDistance = 3
		r.method.prepared.Plan.SchemaVersion = 2
		r.method.prepared.Candidate = methodCandidatesV2()[0]
		methodApplyRows(t, r, key, "K1ABC", 4, 14020, methodApplyAt, 2*time.Minute)
		methodApplyRows(t, r, key, "DL9ZZZ", 6, 14020.2, methodApplyAt, 2*time.Minute)
		count := 10
		if competitor {
			methodApplyRows(t, r, key, "W1ZZZ", 2, 14020, methodApplyAt, time.Minute)
			count += 2
		}
		methodApplyPublish(t, r, count, methodApplyAt)
		s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
		out := r.applyResolverOutcome(s, methodApplyEvidence(key), true, methodApplyAt.Add(time.Millisecond))
		receipts := methodApplyReceipts(t, r)
		if len(receipts) != 1 || out.Applied == competitor {
			t.Fatalf("selective outcome competitor=%t: %+v receipts=%+v", competitor, out, receipts)
		}
		if competitor {
			if s.DXCallNorm != "K1ABD" || len(receipts[0].Trials) != 0 || len(receipts[0].SelectiveDeclines) == 0 {
				t.Fatal("credible nonnominee did not decline the additional rescue")
			}
		} else if s.DXCallNorm != "K1ABC" || s.Confidence != "C" || !s.ConfidencePercentOK || s.ConfidencePercent != 66 || receipts[0].Trials[0].Gate.WinnerConfidence != 100 {
			t.Fatalf("public confidence leaked group values: %+v", s)
		}
	}
}

func TestCCV2V8BayesOnlyComparatorRestriction(t *testing.T) {
	r, _ := methodApplyOwner(t)
	r.method.prepared.Plan.SchemaVersion = 2
	r.cfg.CallCorrection.ResolverRecentPlus1Enabled = false
	r.cfg.CallCorrection.MinConsensusReports = 4
	r.cfg.CallCorrection.BayesBonus.Enabled = true
	r.cfg.CallCorrection.BayesBonus.EnabledDistancesByMode = map[string][]int{"CW": {1, 2}}
	r.recentBandStore = &methodHistoryProbe{}
	p := correctionflow.FrequencyProposal{Call: "K1ABC", Identity: "K1ABC", Support: 3, SubjectSupport: 1,
		WeightedSupportMilli: 3000, SubjectWeightedSupportMilli: 1000, ConfidencePercent: 75, ComparableEditNeighbor: true}
	s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
	gate := r.evaluateMethodGate(s, "K1ABD", p, methodApplyAt)
	if !gate.BayesReportBonusConsidered || gate.BayesReportBonusApplied || gate.BayesReportBonusReject != resolverRecentPlus1DisallowEditNeighborGate {
		t.Fatalf("legacy Bayes structural mode lost the ordinary comparator restriction: %+v", gate)
	}
}

func TestCCV2V8DevelopmentEvaluation(t *testing.T) {
	plan, file := methodEvalTestFixtureVersion(t, 2)
	out := filepath.Join(t.TempDir(), "development")
	if err := runStudyMethodEvaluationWithAuthority(file.Path, file.SHA256, out, studyDiagnosticArtifactFileOps(), methodEvalFixtureAuthority(plan)); err != nil {
		t.Fatal(err)
	}
	result := methodEvalTestReadResult(t, out)
	if result.SchemaVersion != 2 || result.OrdinalVsGeometry != nil || result.CompatibilityVsSelective == nil || len(result.Variants) != 2 {
		t.Fatal("development result reused legacy attribution/schema")
	}
	for _, variant := range result.Variants {
		if variant.Eligible || variant.EligibilityAssessed || variant.Counts.RawIDs != 4 || variant.Counts.Control.Applied != 2 || variant.Counts.Variant.Applied != 4 || variant.Counts.Variant.Stable != 2 || len(variant.Days) != 2 {
			t.Fatalf("development changed population/fraction or granted release authority: %+v", variant)
		}
	}
	for _, mutate := range []func(*studyMethodEvaluationPlan){
		func(p *studyMethodEvaluationPlan) { p.Phase = "holdout" },
		func(p *studyMethodEvaluationPlan) { p.Groups[0].DateUTC = "2026-11-15" },
		func(p *studyMethodEvaluationPlan) { p.Groups = p.Groups[:1] },
		func(p *studyMethodEvaluationPlan) { p.Parity = p.Parity[:3] },
		func(p *studyMethodEvaluationPlan) { p.SchemaVersion = 1 },
	} {
		copy := plan
		copy.Groups = append([]studyMethodEvalGroup(nil), plan.Groups...)
		mutate(&copy)
		if err := validateStudyMethodEvaluationPlan(copy); err == nil {
			t.Fatal("development accepted incomplete dates/parity or legacy version")
		}
	}
}
