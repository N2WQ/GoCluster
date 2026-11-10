package cluster

import (
	"bytes"
	"context"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/dedup"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/ui"
)

func newSummaryTestEngine(t *testing.T, state string) *dedup.SecondaryDeduper {
	t.Helper()
	d, err := dedup.NewSecondaryDeduperWithFrequencyChanges(179*time.Second, false, dedup.SecondaryKeyGrid2,
		dedup.FrequencyChangeOptions{State: state, CWRTTYToleranceHz: 500, FTToleranceHz: 3000,
			OtherSkimmerToleranceHz: 1000, HumanToleranceHz: 2000, MaxChanges: 2})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func summaryTestSpot(at time.Time, frequency float64, reported bool) *spot.Spot {
	s := spot.NewSpot("K1ABC", "W1XYZ", frequency, "CW")
	s.Time, s.HasReport = at, reported
	s.DEMetadata.ADIF, s.DEGrid2 = 291, "FN"
	return s
}

func summaryRunToken(t *testing.T, line string) string {
	t.Helper()
	for _, field := range strings.Fields(line) {
		if strings.HasPrefix(field, "run=") {
			return field
		}
	}
	t.Fatalf("missing run identity: %s", line)
	return ""
}

func TestSecondaryShadowSummaryGoldenAndRestart(t *testing.T) {
	d := newSummaryTestEngine(t, "shadow")
	base := time.Unix(600, 0)
	for _, input := range []struct {
		seconds   int
		frequency float64
		reported  bool
	}{
		{0, 14025, true}, {10, 14030, false}, {20, 14030, true}, {180, 14030, true}, {200, 14030, true},
	} {
		d.ShouldForward(summaryTestSpot(base.Add(time.Duration(input.seconds)*time.Second), input.frequency, input.reported))
	}
	reporter := &secondaryShadowReporter{}
	var lines []string
	emit := func(line string) { lines = append(lines, line) }
	engines := [3]*dedup.SecondaryDeduper{d}
	reporter.report(base.Add(201*time.Second), engines, emit)
	if len(lines) != 4 {
		t.Fatalf("got %d records, want one per class for enabled policy", len(lines))
	}
	for _, want := range []string{"policy=FAST class=human", "run=", "seq=1", "base=2 suppressed=3 added_new=1 added_report=1 added_frequency=1 lost=1", "net_pct=100.00", "frequency_admissions=1 limit_blocked=0", "cache[baseline=1 candidate=1]"} {
		if !strings.Contains(lines[0], want) {
			t.Fatalf("missing %q: %s", want, lines[0])
		}
	}
	firstRun := summaryRunToken(t, lines[0])
	lines = nil
	reporter.report(base.Add(202*time.Second), engines, emit)
	if got := summaryRunToken(t, lines[0]); got != firstRun {
		t.Fatalf("same engine changed run identity: %s -> %s", firstRun, got)
	}
	if !strings.Contains(lines[0], "interval[base=0 suppressed=0 added_new=0 added_report=0 added_frequency=0 lost=0") {
		t.Fatalf("report reset or repeated cumulative counters: %s", lines[0])
	}
	replacement := newSummaryTestEngine(t, "shadow")
	replacement.ShouldForward(summaryTestSpot(base, 14025, true))
	// Force the collision even on clocks precise enough to distinguish these
	// constructions. Keep the old counters so a missed reset cannot pass.
	replacementSnapshot, _ := replacement.ShadowStats()
	reporter.previous[0].StartedAt = replacementSnapshot.StartedAt
	lines = nil
	reporter.report(base, [3]*dedup.SecondaryDeduper{replacement}, emit)
	if !strings.Contains(lines[0], "interval[base=1 suppressed=0 added_new=0 added_report=0 added_frequency=0 lost=0 frequency_admissions=0 limit_blocked=0]") {
		t.Fatalf("replacement run inherited old interval: %s", lines[0])
	}
	if got := summaryRunToken(t, lines[0]); got == firstRun {
		t.Fatalf("replacement inherited log run identity: %s", got)
	}
	lines = nil
	reporter.report(base, [3]*dedup.SecondaryDeduper{newSummaryTestEngine(t, "off"), newSummaryTestEngine(t, "on")}, emit)
	if len(lines) != 0 {
		t.Fatal("off/on emitted comparison records")
	}
}

func TestSecondaryShadowSummaryRunIdentityWithEqualStarts(t *testing.T) {
	first := dedup.ShadowSnapshot{StartedAt: time.Unix(600, 0).UTC(), RunID: 11}
	second := first
	second.RunID = 12
	for _, input := range []struct {
		snapshot dedup.ShadowSnapshot
		want     string
	}{
		{first, "run=1970-01-01T00:10:00Z-11"},
		{second, "run=1970-01-01T00:10:00Z-12"},
	} {
		line := formatSecondaryShadowSummary(0, dedup.ShadowHuman, 1, first.StartedAt, input.snapshot, dedup.ShadowCounters{}, dedup.ShadowCounters{})
		if got := summaryRunToken(t, line); got != input.want {
			t.Fatalf("run identity=%q, want %q", got, input.want)
		}
	}
}

func TestSecondaryShadowSummaryRatiosAndMaximumRecord(t *testing.T) {
	if shadowNetPercent(dedup.ShadowCounters{}) != "unavailable" || shadowLimitPercent(dedup.ShadowCounters{}) != "unavailable" {
		t.Fatal("empty populations satisfied a percentage gate")
	}
	if got := shadowNetPercent(dedup.ShadowCounters{BaselineForwarded: 10, LostForwards: 3, AddedNewEntry: 1}); got != "-20.00" {
		t.Fatalf("net=%s", got)
	}
	if got := shadowLimitPercent(dedup.ShadowCounters{FrequencyAdmissions: 99, LimitBlocked: 1}); got != "1.00" {
		t.Fatalf("limit=%s", got)
	}
	max := ^uint64(0)
	c := dedup.ShadowCounters{BaselineForwarded: max, BaselineSuppressed: max, AddedNewEntry: max, AddedReportException: max,
		AddedFrequencyChange: max, LostForwards: max, FrequencyAdmissions: max, LimitBlocked: max}
	now := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	s := dedup.ShadowSnapshot{StartedAt: now, RunID: max, BaselineCacheSize: int(^uint(0) >> 1), CandidateCacheSize: int(^uint(0) >> 1)}
	line := formatSecondaryShadowSummary(2, dedup.ShadowOther, max, now, s, c, c)
	if size := len(formatLogTimestamp(now)) + 1 + len(line); size > 2048 {
		t.Fatalf("record bytes=%d", size)
	}
}

func TestSecondaryShadowReportingDoesNotRetainDedupeLock(t *testing.T) {
	d := newSummaryTestEngine(t, "shadow")
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	go func() {
		defer close(done)
		(&secondaryShadowReporter{}).report(time.Now(), [3]*dedup.SecondaryDeduper{d}, func(string) {
			once.Do(func() { close(entered) })
			<-release
		})
	}()
	<-entered
	progress := make(chan struct{})
	go func() { d.ShouldForward(summaryTestSpot(time.Now(), 14025, true)); close(progress) }()
	select {
	case <-progress:
	case <-time.After(time.Second):
		close(release)
		<-done
		t.Fatal("reporting held a dedupe lock")
	}
	close(release)
	<-done
}

type shadowSummaryLogCapture struct {
	mu   sync.Mutex
	data bytes.Buffer
	seen chan struct{}
	once sync.Once
}

func (w *shadowSummaryLogCapture) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.data.Write(p)
	if bytes.Contains(p, []byte("Dedupe shadow admissions")) {
		w.once.Do(func() { close(w.seen) })
	}
	return n, err
}

func TestSecondaryShadowSummaryFromActualStatsTicker(t *testing.T) {
	for _, dashboard := range []bool{false, true} {
		name := "headless"
		if dashboard {
			name = "dashboard"
		}
		t.Run(name, func(t *testing.T) {
			writer := &shadowSummaryLogCapture{seen: make(chan struct{})}
			old := log.Writer()
			log.SetOutput(writer)
			defer log.SetOutput(old)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var surface ui.Surface
			if dashboard {
				surface = &captureSurface{}
			}
			done := make(chan struct{})
			engine := newSummaryTestEngine(t, "shadow")
			go func() {
				defer close(done)
				displayStatsWithFCC(ctx, 5*time.Millisecond,
					stats.NewTracker(), nil, nil, engine, nil, nil, nil, nil, func() *cty.CTYDatabase { return nil }, nil, nil,
					nil, nil, nil, surface, nil, nil, "", nil, nil, nil, nil,
					nil, nil, nil, nil, nil, nil, nil, nil, "", "", dashboardIngestSourceConfig{})
			}()
			select {
			case <-writer.seen:
			case <-time.After(3 * time.Second):
				cancel()
				<-done
				t.Fatal("stats ticker emitted no shadow summary")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("stats ticker did not retire")
			}
		})
	}
}
