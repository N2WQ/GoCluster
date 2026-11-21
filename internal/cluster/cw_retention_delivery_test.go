//go:build !qualification

package cluster

import (
	"strings"
	"testing"
	"time"

	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

// Exercise a genuine eligible ordinary rename through the real TCP output.
// The same two nonfocal receivers hear the original and winner 300 Hz apart.
func TestCWFrequencyRetentionDelivery(t *testing.T) {
	temporalDeliveryDirectory(t)
	for _, tc := range []struct {
		name    string
		enabled bool
		burst   bool
		held    bool
		want    string
	}{
		{"ordinary", false, false, false, "K1ABD"},
		{"immediate", true, false, false, "K1ABC"},
		{"stabilizer", true, false, true, "K1ABC"},
		{"both_enabled", true, true, false, "K1ABC"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := selectiveLiveConfig()
			cfg.CWFrequencySeparationEnabled = tc.enabled
			cfg.CWBurstWPMEnabled = tc.burst
			cfg.SelectiveRescueEnabled = false
			at := time.Now().UTC().Add(-time.Second)
			r, driver, key := selectiveLiveResolver(t, cfg)
			for _, receiver := range []string{"N0BBB", "N0CCC"} {
				ambiguityDeliveryAdd(t, r, key, "K1ABC", receiver, 14020, at)
			}
			for _, receiver := range []string{"N0BBB", "N0CCC", "N0DDD", "N0EEE", "N0FFF"} {
				ambiguityDeliveryAdd(t, r, key, "K1ABD", receiver, 14020.3, at)
			}
			driver.Drain()
			driver.Sweep(at.Add(time.Millisecond))
			f := newAmbiguityDeliveryFixture(t, r, cfg)
			s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
			s.SourceType = spot.SourceRBN
			s.IsHuman = false
			s.RBNCQReceivedAt = at
			if tc.held {
				f.pipeline.handleStabilizerRelease(&telnetStabilizerEnvelope{
					spot: s, peerOriginal: captureOriginalPeerPublication(s),
					resolverKey: key, hasResolverKey: true, evidenceEnqueued: true,
				})
			} else {
				f.pipeline.processSpotBody(s, nil)
			}
			rows := f.ring.GetRecent(2)
			if len(rows) != 1 || rows[0].DXCallNorm != tc.want || s.DXCallNorm != tc.want {
				t.Fatalf("delivered %d rows, spot=%s, want %s", len(rows), s.DXCallNorm, tc.want)
			}
			line := correctionEvidenceReadDX(t, f.normalConn, f.normalReader)
			if len(line) < 39 || strings.TrimSpace(line[27:39]) != tc.want {
				t.Fatalf("unexpected telnet spot %q", line)
			}
			if tc.enabled != (f.pipeline.tracker.CallCorrections() == 0) {
				t.Fatalf("correction count=%d", f.pipeline.tracker.CallCorrections())
			}
			reasons := f.pipeline.tracker.CorrectionDecisionReasons()
			if tc.enabled && reasons[cwFrequencyRetained] != 1 ||
				!tc.enabled && reasons[cwFrequencyRetained] != 0 || reasons[cwBurstRetained] != 0 {
				t.Fatalf("unexpected retention reasons: %v", reasons)
			}
		})
	}
}

func TestCWBurstWPMRetentionDelivery(t *testing.T) {
	temporalDeliveryDirectory(t)
	for _, tc := range []struct {
		name, incoming, want string
		frequency, burst     bool
		rescue               bool
		retained             uint64
	}{
		{"both_off", "26 WPM", "K1ABD", false, false, false, 0},
		{"frequency_only_cochannel", "26 WPM", "K1ABD", true, false, false, 0},
		{"burst_only", "26 WPM", "K1ABC", false, true, false, 1},
		{"both_on_cochannel", "26 WPM", "K1ABC", true, true, false, 1},
		{"agrees_with_older_replacement", "20 WPM", "K1ABD", false, true, false, 0},
		{"method_c_disables_burst", "26 WPM", "K1ABD", false, true, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := selectiveLiveConfig()
			cfg.CWFrequencySeparationEnabled = tc.frequency
			cfg.CWBurstWPMEnabled, cfg.SelectiveRescueEnabled = tc.burst, tc.rescue
			cfg.RecencySeconds = 300
			comparison := cwBurstComparisonEnabled(&cfg)
			r := spot.NewSignalResolver(spot.SignalResolverConfig{
				CWDecayComparison: comparison, QueueSize: 32, MaxActiveKeys: 4,
				MaxCandidatesPerKey: 4, MaxReportersPerCand: 8, HysteresisWindows: 1,
				InactiveTTL: 5 * time.Minute, EvalMinInterval: time.Nanosecond,
				SweepInterval: time.Nanosecond, MaxEditDistance: cfg.MaxEditDistance,
			})
			t.Cleanup(r.Stop)
			driver, err := spot.NewSignalResolverDriver(r)
			if err != nil {
				t.Fatal(err)
			}
			key := spot.NewResolverSignalKey(14020, "20m", "CW", cfg.FrequencyToleranceHz)
			at := time.Now().UTC().Add(-time.Second)
			var history *correctionflow.CWBurstHistory
			if comparison {
				history = correctionflow.NewCWBurstHistory()
			}
			report := func(call, receiver, speed string, age time.Duration) *spot.Spot {
				arrival := at.Add(-age)
				s := newDeliveryTestSpot(call, receiver, arrival)
				s.IsHuman, s.SourceType = false, spot.SourceRBN
				s.RBNCQReceivedAt, s.RBNCQRawKHz = arrival, 14020
				s.CWSpeeds = []string{speed}
				history.Observe(s)
				return s
			}
			for i, receiver := range []string{"N0BBB", "N0CCC", "N0DDD", "N0FFF", "N0GGG", "N0HHH", "N0III"} {
				call, speed, age := "K1ABD", "20 WPM", 240*time.Second
				if i >= 5 {
					call, speed, age = "K1ABC", "26 WPM", 2*time.Second
				}
				s := report(call, receiver, speed, age)
				evidence, ok := buildResolverEvidenceSnapshot(s, cfg, nil, s.RBNCQReceivedAt)
				if !ok || !r.Enqueue(evidence) {
					t.Fatal("independent receiver evidence was refused")
				}
			}
			driver.Drain()
			driver.Sweep(at.Add(-time.Second))
			ordinary, ok := r.Lookup(key)
			if !ok || ordinary.Winner != "K1ABD" {
				t.Fatalf("ordinary majority missing: %+v", ordinary)
			}
			f := newAmbiguityDeliveryFixture(t, r, cfg)
			f.pipeline.cwBursts = history
			s := report("K1ABC", "N0TTT", tc.incoming, 0)
			f.pipeline.processSpotBody(s, nil)
			rows := f.ring.GetRecent(2)
			if len(rows) != 1 || rows[0].DXCallNorm != tc.want {
				t.Fatalf("delivered %d rows, spot=%s, want %s", len(rows), s.DXCallNorm, tc.want)
			}
			line := correctionEvidenceReadDX(t, f.normalConn, f.normalReader)
			if len(line) < 39 || strings.TrimSpace(line[27:39]) != tc.want {
				t.Fatalf("unexpected telnet spot %q", line)
			}
			reasons := f.pipeline.tracker.CorrectionDecisionReasons()
			if reasons[cwBurstRetained] != tc.retained || reasons[cwFrequencyRetained] != 0 {
				t.Fatalf("unexpected retention reasons: %v", reasons)
			}
		})
	}
}
