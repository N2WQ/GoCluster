package main

import (
	"errors"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestOrdinaryCWGuardImmediateReplacement(t *testing.T) {
	for _, tc := range []struct {
		name     string
		active   bool
		licensed []string
		apply    bool
		retain   bool
	}{
		{name: "ordinary control", licensed: []string{"K1ABC", "K1ABD"}, apply: true},
		{name: "guard retains licensed original", active: true, licensed: []string{"K1ABC", "K1ABD"}, retain: true},
		{name: "unlicensed original follows ordinary path", active: true, licensed: []string{"K1ABC"}, apply: true},
		{name: "replacement license rejection precedes guard", active: true, licensed: []string{"K1ABD"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
			replayLicenseGlobals(t, replayLicenseDB(t, tc.licensed...))
			r, key := replayTemporalFreshRunner(t, replayTemporalFreshConfig(), at, 2*time.Minute)
			r.ctyDB = replayLicenseCTY(t)
			s := replayTemporalFreshSpot("K1ABD", at)
			s.Frequency = 14020.2
			r.ordinaryGuard = tc.active
			r.study = &studyObserver{day: &studyRawDay{}, pending: map[*spot.Spot]*studyDecision{s: {ReceiverFamily: "N0AAA"}}}
			now := at.Add(time.Second)
			for _, row := range []struct {
				call, reporter string
				frequency      float64
			}{{"K1ABD", "N0BBB", 14020.2}, {"K1ABC", "N0DDD", 14020}} {
				if !r.resolver.Enqueue(spot.ResolverEvidence{ObservedAt: now, Key: key, DXCall: row.call,
					Spotter: row.reporter, FrequencyKHz: row.frequency, RecencyWindow: 2 * time.Minute}) {
					t.Fatal("evidence not admitted")
				}
			}
			r.driver.Step(now)
			now = now.Add(time.Second)
			r.driver.Step(now)
			out := r.applyResolverOutcome(s, spot.ResolverEvidence{Key: key}, true, now)
			gotRetain := out.methodReason == studyOrdinaryGuardRetained
			wantCall, wantCount := "K1ABD", uint64(0)
			if tc.apply {
				wantCall, wantCount = "K1ABC", 1
			}
			if gotRetain != tc.retain || out.Applied != tc.apply || out.Suppress ||
				out.replacementRejected != (!tc.apply && !tc.retain) ||
				s.DXCallNorm != wantCall || r.tracker.CallCorrections() != wantCount {
				t.Fatalf("call=%s corrections=%d outcome=%+v", s.DXCallNorm, r.tracker.CallCorrections(), out)
			}
		})
	}
}

func TestOrdinaryCWGuardStudyBoundary(t *testing.T) {
	approved := studyCLIRequest{OrdinaryGuard: true, Arm: "repaired_off", Phase: "tuning", Date: "2026-11-07"}
	if err := validateOrdinaryGuardRequest(approved); err != nil {
		t.Fatal(err)
	}
	for _, req := range []studyCLIRequest{
		{OrdinaryGuard: true, Arm: "repaired_off", Phase: "holdout", Date: "2026-11-07"},
		{OrdinaryGuard: true, Arm: "repaired_off", Phase: "tuning", Date: "2026-11-21"},
		{OrdinaryGuard: true, Arm: "repaired_off", Phase: "tuning", Date: "2026-11-07", MethodPlan: "method.json"},
	} {
		if err := validateOrdinaryGuardRequest(req); err == nil {
			t.Fatalf("accepted outside experimental boundary: %+v", req)
		}
	}
	if err := studyDiagnosticRejectReportMarker([]byte(`{"ordinary_guard":true}`)); !errors.Is(err, errStudyExperimentalReportMarker) {
		t.Fatalf("active completion passed ordinary qualification boundary: %v", err)
	}
}
