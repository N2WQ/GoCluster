package cluster

import (
	"testing"
	"time"

	"dxcluster/buffer"
	"dxcluster/internal/toxicity"
	"dxcluster/spot"
)

func TestOutputPipelineGraceProcessingAdmissionBoundary(t *testing.T) {
	deadline := time.Date(2026, 10, 10, 12, 0, 5, 0, time.UTC)
	p := &outputPipeline{stopCh: make(chan struct{}), drainDeadline: deadline}
	if !p.canProcessToxicityResult(deadline.Add(time.Hour)) {
		t.Fatal("ordinary processing gained a deadline before any stop request")
	}
	close(p.stopCh)
	for _, tc := range []struct {
		name string
		now  time.Time
		want bool
	}{
		{"one nanosecond before", deadline.Add(-time.Nanosecond), true},
		{"exact expiry", deadline, false},
		{"one nanosecond after", deadline.Add(time.Nanosecond), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.canProcessToxicityResult(tc.now); got != tc.want {
				t.Fatalf("processing admission at %s: got %v want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestOutputPipelineDiscardsSelectedExpiredResultBeforeApplying(t *testing.T) {
	p := &outputPipeline{stopCh: make(chan struct{}), drainDeadline: time.Now().Add(-time.Second), buf: buffer.NewRingBuffer(4)}
	close(p.stopCh)
	s := spot.NewSpot("K1LATE", "W1XYZ", 14030, "CW")
	s.ToxicityStatus = spot.ToxicityUnknown
	s.Comment = "expired selected conversation"
	result := toxicity.Result{Spot: s, Decision: toxicity.Decision{Status: spot.ToxicityToxic, Categories: []string{"S10"}, Model: "expired-fixture"}}
	if p.processToxicityResult(result) || s.ToxicityStatus != spot.ToxicityUnknown || len(s.ToxicityCategories) != 0 || s.ToxicityModel != "" || p.buf.GetCount() != 0 {
		t.Fatal("selected expired result crossed into normal processing/handoff")
	}
	if result.Spot != s || result.Decision.Model != "expired-fixture" {
		t.Fatal("discard revoked consumer-owned result data")
	}
}
