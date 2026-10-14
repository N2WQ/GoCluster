package spot

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"dxcluster/config"
)

func adaptiveActivityConfig() config.CallCorrectionConfig {
	return config.CallCorrectionConfig{
		MinConsensusReports: 3,
		AdaptiveMinReports: config.AdaptiveMinReportsConfig{
			Enabled: true, WindowMinutes: 1, EvaluationPeriodSeconds: 1, HysteresisWindows: 2,
			Groups: []config.AdaptiveMinReportsGroup{{Name: "two", Bands: []string{"20m", "40m"}, QuietBelow: 2, BusyAbove: 3,
				QuietMinReports: 2, NormalMinReports: 3, BusyMinReports: 4}},
		},
	}
}

func TestAdaptiveActivityAdmissionTransaction(t *testing.T) {
	a := NewAdaptiveMinReports(adaptiveActivityConfig())
	at := time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC)
	a.Observe("40m", "OLD", at)
	maintenanceAt := at.Add(2 * time.Minute)
	admissionAt := maintenanceAt.Add(9 * time.Nanosecond)
	clockCalls, sinkCalls := 0, 0
	clock := func() time.Time {
		clockCalls++
		if clockCalls == 1 {
			return maintenanceAt
		}
		return admissionAt
	}
	if !a.AdmitObservation(" 20M ", " n0new ", clock, func() bool { sinkCalls++; return true }) {
		t.Fatal("successful sink was refused")
	}
	if clockCalls != 2 || sinkCalls != 1 {
		t.Fatalf("due-prune clock/sink calls=%d/%d, want2/1", clockCalls, sinkCalls)
	}
	a.mu.Lock()
	seen := a.reporterSeen["20m"]["N0NEW"]
	oldBands, evaluated := len(a.reporterSeen), !a.lastEval.IsZero()
	a.mu.Unlock()
	if !seen.Equal(admissionAt) || oldBands != 1 || evaluated {
		t.Fatalf("admission timestamp/empty-band cleanup/lazy evaluation violated: seen=%v bands=%d evaluated=%v", seen, oldBands, evaluated)
	}
	if a.AdmitObservation("20m", "REFUSED", func() time.Time { return admissionAt }, func() bool { sinkCalls++; return false }) {
		t.Fatal("full input changed to successful admission")
	}
	if bands, reporters := a.ActivityCardinality(); bands != 1 || reporters != 1 || sinkCalls != 2 {
		t.Fatalf("refused input changed retained population: %d bands %d reporters calls%d", bands, reporters, sinkCalls)
	}
}

func TestAdaptiveActivityAdmissionReadersSerialize(t *testing.T) {
	a := NewAdaptiveMinReports(adaptiveActivityConfig())
	at := time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC)
	sent, finish := make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	defer finishOnce.Do(func() { close(finish) })
	admissionDone := make(chan bool, 1)
	go func() {
		admissionDone <- a.AdmitObservation("20m", "N0NEW", func() time.Time { return at }, func() bool {
			close(sent)
			<-finish // Controlled test barrier after successful publication.
			return true
		})
	}()
	<-sent
	if a.mu.TryLock() {
		a.mu.Unlock()
		t.Fatal("successful publication did not retain the activity transaction lock")
	}
	queryStarted, queryDone := make(chan struct{}), make(chan int, 1)
	go func() { close(queryStarted); queryDone <- a.MinReportsForBand("20m", at) }()
	<-queryStarted
	select {
	case <-queryDone:
		t.Fatal("adaptive query overtook the success-only activity update")
	case <-time.After(20 * time.Millisecond):
	}
	finishOnce.Do(func() { close(finish) })
	if !<-admissionDone {
		t.Fatal("published input was refused")
	}
	select {
	case <-queryDone:
	case <-time.After(time.Second):
		t.Fatal("adaptive reader remained blocked after transaction completed")
	}
	if _, reporters := a.ActivityCardinality(); reporters != 1 {
		t.Fatalf("query missed admitted receiver: %d", reporters)
	}
}

func TestAdaptiveActivityExpiryAndGroupDenominator(t *testing.T) {
	a := NewAdaptiveMinReports(adaptiveActivityConfig())
	at := time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC)
	for i := range 6 {
		a.Observe("20m", fmt.Sprintf("N0R%d", i), at)
	}
	// Six reporters averaged across two configured bands equals three, which
	// is normal; averaging only the active band would incorrectly become busy.
	if got := a.MinReportsForBand("20m", at); got != 3 {
		t.Fatalf("group denominator changed: got%d", got)
	}
	if got := a.MinReportsForBand("40m", at.Add(time.Second)); got != 3 {
		t.Fatalf("inactive configured band not included: got%d", got)
	}
	a.Observe("20m", "CURRENT", at.Add(time.Minute))
	if _, reporters := a.ActivityCardinality(); reporters != 7 {
		t.Fatalf("exact window boundary expired: got%d", reporters)
	}
	a.Observe("40m", "LATEST", at.Add(time.Minute+time.Second))
	if bands, reporters := a.ActivityCardinality(); bands != 2 || reporters != 2 {
		t.Fatalf("after-boundary expiry mismatch: bands%d reporters%d", bands, reporters)
	}
	lastEval := a.lastEval
	for range 5 {
		a.ActivityCardinality()
	}
	if !a.lastEval.Equal(lastEval) {
		t.Fatal("cardinality snapshot changed evaluation clock")
	}
}

// Counting must remain bounded when correction and the optional refresher are
// disabled. This uses the old public Observe API so the pre-repair failure is
// an actual retained-state defect rather than a missing new method.
func TestAdaptiveActivityObservePrunesWithoutQueries(t *testing.T) {
	a := NewAdaptiveMinReports(adaptiveActivityConfig())
	at := time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC)
	for i := range 100 {
		a.Observe("20m", fmt.Sprintf("N0R%03d", i), at.Add(time.Duration(i)*time.Minute))
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if got := len(a.reporterSeen["20m"]); got != 2 {
		t.Fatalf("100 observations over 99 windows retained %d reporters; want the current and exact-boundary reporter only (2)", got)
	}
	if !a.lastEval.IsZero() || a.groups["two"].pendingStreak != 0 {
		t.Fatal("observation maintenance evaluated adaptive hysteresis")
	}
}
