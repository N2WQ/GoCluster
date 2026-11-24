package pathreliability

import (
	"math"
	"testing"
)

func TestReceiverCapDiagnosticSupportTolerance(t *testing.T) {
	const admitted = .75
	boundary := admitted + effectiveCountEpsilon
	for _, tc := range []struct {
		name string
		raw  float64
		want bool
	}{
		{"below", math.Nextafter(boundary, 0), false},
		{"equal", boundary, false},
		{"above", math.Nextafter(boundary, math.Inf(1)), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := receiverCapLimited(tc.raw, admitted, 2, 2); got != tc.want {
				t.Fatalf("support %0.17g vs %g: limited=%v, want %v", tc.raw, admitted, got, tc.want)
			}
		})
	}
	if !receiverCapLimited(admitted, admitted, 2.5, 2) {
		t.Fatal("signal-weight reduction must still set the diagnostic")
	}
}

func TestReceiverCapDiagnosticUncappedDecayAndKnownZero(t *testing.T) {
	for _, mode := range []string{ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			p := NewPredictor(recentSupportConfig(mode, 10), []string{"20m"})
			recentSupportReport(t, p.combined, 0, 0, 1, 1)
			recentSupportReport(t, p.combined, 10, 10, 1, 2)
			recentSupportReport(t, p.combined, 20, 20, 1, 3)
			at := recentSupportTime(30)
			fine, coarse := p.combined.Lookup(1, 2, 3, 4, "20m", at)
			for _, sample := range []Sample{fine, coarse} {
				recentSupportWant(t, sample.RecentSupport, mode, .875, .875)
				if sample.CapLimited || sample.RawCount != 3 || sample.CappedCount != 0 {
					t.Fatalf("age alone changed the cap diagnostic: %+v", sample)
				}
			}
			got := p.Predict(1, 2, 3, 4, "20m", "FT8", 0, at)
			recentSupportWant(t, got.ObservedRecentSupport, mode, .875, .875)
			if got.CapLimited || got.RawCount != 3 || got.CappedCount != 0 ||
				got.CappedWeight != .4375 || got.P50DB != -10.5 ||
				got.Source != SourceInsufficient || got.InsufficientReason != InsufficientLowCount {
				t.Fatalf("merge confused known capped zero with unavailable data: %+v", got)
			}
		})
	}
}

func TestReceiverCapDiagnosticDetectsFractionalLossBeforeRounding(t *testing.T) {
	for _, mode := range []string{ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			p := NewPredictor(recentSupportConfig(mode, 10), []string{"20m"})
			for range 8 {
				recentSupportReport(t, p.combined, 0, 0, 1e-6, 1)
			}
			// The report-count cap rejects the ninth report. Its weight loss
			// alone is below epsilon and both aged counts round down to zero.
			recentSupportReport(t, p.combined, 0, 0, 0x1p-40, 1)
			at := recentSupportTime(40)
			_, coarse := p.combined.Lookup(1, 2, 3, 4, "20m", at)
			recentSupportWant(t, coarse.RecentSupport, mode, .5625, .5)
			if coarse.CappedCount != 0 || coarse.RawWeight <= coarse.CappedWeight ||
				coarse.RawWeight > coarse.CappedWeight+effectiveCountEpsilon || !coarse.CapLimited {
				t.Fatalf("fractional loss was not carried by the support comparison: %+v", coarse)
			}
			got := p.Predict(1, 2, 3, 4, "20m", "FT8", 0, at)
			if !got.CapLimited || got.CappedCount != 0 || got.Source != SourceInsufficient ||
				got.InsufficientReason != InsufficientLowCount || !got.HasP50 || got.P50DB != -10.5 {
				t.Fatalf("fractional cap diagnostic altered the observed result: %+v", got)
			}
		})
	}
}

func TestReceiverCapDiagnosticUnattributed(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			p := NewPredictor(recentSupportConfig(mode, 10), []string{"20m"})
			// Its usable report unit suffices to expose the reduction even
			// though its signal weight is within diagnostic tolerance.
			recentSupportReport(t, p.combined, 0, 0, 0x1p-40, 0)
			_, coarse := p.combined.Lookup(1, 2, 3, 4, "20m", recentSupportTime(0))
			recentSupportWant(t, coarse.RecentSupport, mode, 1, 0)
			wantLimited := mode != ReceiverContributionOff
			wantCappedCount := uint32(0)
			if mode == ReceiverContributionOff {
				wantCappedCount = 1
			}
			if coarse.CapLimited != wantLimited || coarse.CappedCount != wantCappedCount ||
				coarse.RawCount != 1 || coarse.CappedReceiverCount != 0 {
				t.Fatalf("unattributed diagnostic/admission changed: %+v", coarse)
			}
		})
	}
}

func TestReceiverCapDiagnosticMergeAvailability(t *testing.T) {
	cfg := recentSupportConfig(ReceiverContributionShadow, 10)
	for _, tc := range []struct {
		name      string
		available bool
		count     uint32
		weight    float64
	}{
		{"legacy", false, 4, 4},
		{"known_zero", true, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sample := Sample{Count: 4, Weight: 4, RecentSupport: ReportSupport{AdmittedAvailable: tc.available}}
			single, ok := mergeSamples(sample, Sample{}, cfg)
			if !ok || !single.RecentSupport.AdmittedAvailable ||
				single.CappedCount != tc.count || single.CappedWeight != tc.weight*.5 {
				t.Fatalf("single-direction merge lost component availability: %+v", single)
			}
			both, ok := mergeSamples(sample, sample, cfg)
			if !ok || both.CappedCount != 2*tc.count || both.CappedWeight != tc.weight {
				t.Fatalf("two-direction merge confused zero and unavailable: %+v", both)
			}
		})
	}
}
