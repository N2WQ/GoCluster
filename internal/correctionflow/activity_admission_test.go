package correctionflow

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/dedup"
	"dxcluster/spot"
)

func activityFlowController() *spot.AdaptiveMinReports {
	return spot.NewAdaptiveMinReports(config.CallCorrectionConfig{MinConsensusReports: 3,
		AdaptiveMinReports: config.AdaptiveMinReportsConfig{Enabled: true, WindowMinutes: 1, EvaluationPeriodSeconds: 1, HysteresisWindows: 1,
			Groups: []config.AdaptiveMinReportsGroup{{Name: "one", Bands: []string{"20m"}, QuietBelow: 1, BusyAbove: 2,
				QuietMinReports: 2, NormalMinReports: 3, BusyMinReports: 4}}}})
}

func activityFlowSpot() *spot.Spot {
	s := spot.NewSpot("K1ABC", "N0AAA", 14030, "CW")
	s.SourceType = spot.SourceRBN
	return s
}

func TestResolverActivityEligibilityAndSinkCount(t *testing.T) {
	for _, tc := range []struct {
		name     string
		edit     func(*spot.Spot)
		eligible bool
	}{
		{"cw", func(*spot.Spot) {}, true},
		{"rtty", func(s *spot.Spot) { s.Mode, s.ModeNorm = "RTTY", "RTTY" }, true},
		{"samecall_external", func(s *spot.Spot) { s.DXCall, s.DXCallNorm = "N0AAA", "N0AAA" }, true},
		{"unknown", func(s *spot.Spot) { s.Mode, s.ModeNorm = "", "" }, false},
		{"voice", func(s *spot.Spot) { s.Mode, s.ModeNorm = "USB", "USB" }, false},
		{"ft8", func(s *spot.Spot) { s.Mode, s.ModeNorm = "FT8", "FT8" }, false},
		{"beacon", func(s *spot.Spot) { s.IsBeacon = true }, false},
		{"self", func(s *spot.Spot) { s.SourceType = spot.SourceManual; s.DXCall, s.DXCallNorm = "N0AAA", "N0AAA" }, false},
		{"testself", func(s *spot.Spot) {
			s.SourceType = spot.SourceManual
			s.IsTestSpotter = true
			s.DXCall, s.DXCallNorm = "N0AAA", "N0AAA"
		}, true},
		{"reporter_fallback", func(s *spot.Spot) { s.DECallNorm = ""; s.DECall = " n0aaa "; s.Band = "" }, true},
		{"blank_reporter", func(s *spot.Spot) { s.DECallNorm, s.DECall = "", "" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := activityFlowController()
			s := activityFlowSpot()
			tc.edit(s)
			at := time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC)
			calls := 0
			if !AdmitResolverActivity(s, a, func() time.Time { return at }, func() bool { calls++; return true }) || calls != 1 {
				t.Fatal("sink was not invoked exactly once")
			}
			want := 0
			if tc.eligible {
				want = 1
			}
			if _, got := a.ActivityCardinality(); got != want {
				t.Fatalf("retained reporter count=%d want%d", got, want)
			}
			b := activityFlowController()
			ObserveResolverActivity(s, b, at)
			if _, got := b.ActivityCardinality(); got != want {
				t.Fatalf("replay eligibility differs: got%d want%d", got, want)
			}
		})
	}
}

func TestResolverActivityMutationAndRefusal(t *testing.T) {
	a := activityFlowController()
	s := activityFlowSpot()
	at := time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC)
	s.Time = at.Add(-24 * time.Hour)
	done := make(chan struct{})
	if !AdmitResolverActivity(s, a, func() time.Time { return at }, func() bool {
		go func() {
			s.DECall, s.DECallNorm, s.Band, s.Mode, s.ModeNorm = "CHANGED", "CHANGED", "40m", "USB", "USB"
			close(done)
		}()
		return true
	}) {
		t.Fatal("input refused")
	}
	<-done
	if bands, reporters := a.ActivityCardinality(); bands != 1 || reporters != 1 {
		t.Fatal("published spot mutation changed captured activity")
	}
	// The original source timestamp must not age out a just-admitted receiver.
	a.MinReportsForBand("20m", at.Add(30*time.Second))
	if _, reporters := a.ActivityCardinality(); reporters != 1 {
		t.Fatal("source timestamp replaced admission timestamp")
	}
	s = activityFlowSpot()
	s.DECallNorm = "REFUSED"
	if AdmitResolverActivity(s, a, func() time.Time { return at }, func() bool { return false }) {
		t.Fatal("refusal became admission")
	}
	if _, reporters := a.ActivityCardinality(); reporters != 1 {
		t.Fatal("refused receiver counted")
	}
	for range 5 {
		ResolveRuntimeSettings(config.CallCorrectionConfig{MinConsensusReports: 3}, activityFlowSpot(), a, at.Add(2*time.Minute), false)
	}
	if _, reporters := a.ActivityCardinality(); reporters != 0 {
		t.Fatal("settings/retry queries refreshed original admission")
	}
}

func TestResolverActivityDisabledAdaptationAndUnknownInference(t *testing.T) {
	calls := 0
	if !AdmitResolverActivity(activityFlowSpot(), nil, nil, func() bool { calls++; return true }) || calls != 1 {
		t.Fatal("disabled adaptation changed admission")
	}
	a := activityFlowController()
	s := activityFlowSpot()
	s.Mode, s.ModeNorm = "", ""
	AdmitResolverActivity(s, a, nil, func() bool { calls++; return true })
	s.Mode, s.ModeNorm = "CW", "CW" // Later inference is outside activity admission.
	if _, reporters := a.ActivityCardinality(); reporters != 0 {
		t.Fatal("unknown admission counted after later inference")
	}
}

func TestResolverActivityPrimaryInputPressureAndQueuedStop(t *testing.T) {
	a := activityFlowController()
	d := dedup.NewDeduplicator(time.Minute, false, 1)
	input := d.GetInputChannel()
	s := activityFlowSpot()
	at := time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC)
	admit := func() bool {
		select {
		case input <- s:
			return true
		default:
			return false
		}
	}
	// Deliberately hold consumption to expose the real primary channel capacity.
	if !AdmitResolverActivity(s, a, func() time.Time { return at }, admit) {
		t.Fatal("empty primary queue refused admission")
	}
	for i := 1; i < cap(input); i++ {
		input <- s
	}
	refused := activityFlowSpot()
	refused.DECallNorm = "REFUSED"
	if AdmitResolverActivity(refused, a, func() time.Time { return at }, admit) {
		t.Fatal("full primary queue was not refused")
	}
	d.Stop() // Queued work can be discarded without undoing accepted activity.
	if _, reporters := a.ActivityCardinality(); reporters != 1 {
		t.Fatalf("input refusal or queued retirement changed accepted activity: %d", reporters)
	}
}

func BenchmarkResolverActivityAdmission(b *testing.B) {
	a := activityFlowController()
	s := activityFlowSpot()
	at := time.Now()
	queue := make(chan *spot.Spot, 1)
	clock := time.Now
	admit := func() bool {
		select {
		case queue <- s:
			return true
		default:
			return false
		}
	}
	ObserveResolverActivity(s, a, at)
	b.ReportAllocs()
	for b.Loop() {
		if !AdmitResolverActivity(s, a, clock, admit) {
			b.Fatal("warmed empty sink refused activity")
		}
		<-queue
	}
	if _, reporters := a.ActivityCardinality(); reporters != 1 {
		b.Fatal("known-receiver benchmark changed retained cardinality")
	}
}
