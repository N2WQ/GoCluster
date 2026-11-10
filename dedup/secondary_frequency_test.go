package dedup

import (
	"encoding/hex"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/spot"

	"github.com/zeebo/xxh3"
)

var frequencyTestTime = time.Unix(600000, 0).UTC()

func TestFrequencyRunIdentityWithEqualStarts(t *testing.T) {
	first := frequencyDeduper(t, "shadow", false)
	second := frequencyDeduper(t, "shadow", false)
	first.changes.startedAt = frequencyTestTime
	second.changes.startedAt = frequencyTestTime
	a, _ := first.ShadowStats()
	b, _ := second.ShadowStats()
	if a.RunID == 0 || b.RunID == 0 || a.RunID == b.RunID || !a.StartedAt.Equal(b.StartedAt) {
		t.Fatalf("equal-start engines need distinct nonzero identities: %v / %v", a, b)
	}
	first.ShouldForward(frequencySpot(14025000, 0, true, 10, "CW", spot.SourceManual))
	after, _ := first.ShadowStats()
	if after.RunID != a.RunID || !after.StartedAt.Equal(a.StartedAt) {
		t.Fatal("admission or snapshot changed run identity")
	}
}

func TestFrequencyRunIdentityConcurrentConstruction(t *testing.T) {
	const constructors = 64
	ids := make(chan uint64, constructors)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range constructors {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			d, err := NewSecondaryDeduperWithFrequencyChanges(time.Minute, false, SecondaryKeyGrid2, frequencyOptions("shadow"))
			if err != nil {
				t.Error(err)
				return
			}
			snapshot, _ := d.ShadowStats()
			ids <- snapshot.RunID
		}()
	}
	close(start)
	workers.Wait()
	close(ids)
	seen := make(map[uint64]bool, constructors)
	for id := range ids {
		if id == 0 || seen[id] {
			t.Fatalf("zero or repeated concurrent run identity: %d", id)
		}
		seen[id] = true
	}
	if len(seen) != constructors {
		t.Fatalf("got %d identities, want %d", len(seen), constructors)
	}
}

func TestFrequencyRunIdentityExhaustion(t *testing.T) {
	// A private counter exercises exhaustion without resetting shared process
	// state or making other tests depend on their execution order.
	var counter atomic.Uint64
	if id, err := nextFrequencyRunID(&counter); err != nil || id != 1 {
		t.Fatalf("first identity: %d, %v", id, err)
	}
	maximum := ^uint64(0)
	counter.Store(maximum - 1)
	if id, err := nextFrequencyRunID(&counter); err != nil || id != maximum {
		t.Fatalf("last identity: %d, %v", id, err)
	}
	for range 2 {
		if id, err := nextFrequencyRunID(&counter); err == nil || id != 0 || counter.Load() != maximum {
			t.Fatalf("exhaustion must fail without wrapping: %d, %v, counter=%d", id, err, counter.Load())
		}
	}
}

func frequencyOptions(state string) FrequencyChangeOptions {
	return FrequencyChangeOptions{State: state, CWRTTYToleranceHz: 500, FTToleranceHz: 3000,
		OtherSkimmerToleranceHz: 1000, HumanToleranceHz: 2000, MaxChanges: 2}
}

func frequencyDeduper(t testing.TB, state string, prefer bool) *SecondaryDeduper {
	t.Helper()
	d, err := NewSecondaryDeduperWithFrequencyChanges(179*time.Second, prefer, SecondaryKeyGrid2, frequencyOptions(state))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func frequencySpot(hz int64, seconds int, report bool, snr int, mode string, source spot.SourceType) *spot.Spot {
	s := spot.NewSpot("K1ABC", "W1XYZ", float64(hz)/1000, mode)
	// NewSpot deliberately rounds frequency to 10 Hz. Operational fixtures
	// overwrite it to make adjacent whole-Hz threshold vectors genuine.
	s.Frequency = float64(hz) / 1000
	s.Time, s.HasReport, s.Report = frequencyTestTime.Add(time.Duration(seconds)*time.Second), report, snr
	s.SourceType, s.DEMetadata.ADIF, s.DEMetadata.CQZone, s.DEGrid2 = source, 291, 5, "FN"
	s.EnsureNormalized()
	return s
}

func candidateEntryFor(t testing.TB, d *SecondaryDeduper, s *spot.Spot) frequencyEntry {
	t.Helper()
	key := secondaryFrequencyKey(s, s.DEMetadata.ADIF, d.keyMode)
	index := uint32(xxh3.Hash(key[:])) & 63
	if spot.IsSkimmerSource(s.SourceType) {
		key[21], _ = spot.SecondaryModeIdentity(s.ModeNorm)
	}
	entry, ok := d.changes.shards[index].cache[key]
	if !ok {
		t.Fatal("candidate entry absent")
	}
	return entry
}

func TestSecondaryFrequencyBudgetAndAnchor(t *testing.T) {
	d := frequencyDeduper(t, "on", false)
	var last *spot.Spot
	for i, tt := range []struct {
		hz   int64
		want bool
	}{{14025000, true}, {14025000, false}, {14026000, true}, {14026000, false}, {14025000, true}, {14027000, false}} {
		last = frequencySpot(tt.hz, i*10, false, 0, "CW", spot.SourceRBN)
		if got := d.ShouldForward(last); got != tt.want {
			t.Fatalf("step %d forward=%t want=%t", i, got, tt.want)
		}
	}
	e := candidateEntryFor(t, d, last)
	if e.changes != 2 || e.anchorHz != 14025000 || e.anchorMinute != frequencyTestTime.Add(40*time.Second).Truncate(time.Minute).Unix() || !e.when.Equal(frequencyTestTime) {
		t.Fatalf("incorrect anchor/budget/expiry: %+v", e)
	}
	if processed, duplicates, size := d.GetStats(); processed != 6 || duplicates != 3 || size != 1 {
		t.Fatalf("driving stats=%d/%d/%d", processed, duplicates, size)
	}
	for i := range d.shards {
		if d.shards[i].cache != nil {
			t.Fatal("on allocated a baseline map")
		}
	}
}

func TestSecondaryFrequencyToleranceBoundaries(t *testing.T) {
	for _, tt := range []struct {
		mode      string
		source    spot.SourceType
		tolerance int64
	}{{"CW", spot.SourceRBN, 500}, {"RTTY", spot.SourceRBN, 500}, {"FT8", spot.SourceRBN, 3000},
		{"FT4", spot.SourceRBN, 3000}, {"FT2", spot.SourceRBN, 3000}, {"PSK", spot.SourcePSKReporter, 1000},
		{"CW", spot.SourceManual, 2000}, {"USB", spot.SourceManual, 2000}, {"", spot.SourceManual, 2000}} {
		for _, direction := range []int64{-1, 1} {
			for _, extra := range []int64{0, 1} {
				d := frequencyDeduper(t, "on", false)
				first := frequencySpot(14025000, 0, false, 0, tt.mode, tt.source)
				nextHz := int64(14025000) + direction*(tt.tolerance+extra)
				next := frequencySpot(nextHz, 10, false, 0, tt.mode, tt.source)
				if hz, ok := wholeHz(next.Frequency); !ok || hz != nextHz {
					t.Fatalf("fixture %s converted=%d want=%d", tt.mode, hz, nextHz)
				}
				if !d.ShouldForward(first) || d.ShouldForward(next) != (extra == 1) {
					t.Fatalf("mode=%q source=%s direction=%d delta=%d", tt.mode, tt.source, direction, tt.tolerance+extra)
				}
			}
		}
	}
	if got, ok := wholeHz(14025.50049); !ok || got != 14025500 {
		t.Fatalf("nearest-Hz lower boundary=%d/%t", got, ok)
	}
	if got, ok := wholeHz(14025.50051); !ok || got != 14025501 {
		t.Fatalf("nearest-Hz upper boundary=%d/%t", got, ok)
	}
}

func TestSecondaryFrequencyMinuteRepresentation(t *testing.T) {
	for _, seconds := range []int64{math.MinInt64, -62135596801, -62135596800, -121, -120, -61, -60, -59, -1, 0, 1, 59, 60, 61, 253402300799, 253402300800, math.MaxInt64} {
		for _, nanos := range []int64{0, 1, 999999999} {
			at := time.Unix(seconds, nanos)
			if got, want := anchorMinuteUnix(at), at.Truncate(time.Minute).Unix(); got != want {
				t.Fatalf("seconds=%d nanos=%d minute=%d want=%d", seconds, nanos, got, want)
			}
		}
	}
}

func TestSecondaryFrequencyReportExceptionMovesAnchorWithoutBudget(t *testing.T) {
	for _, prefer := range []bool{false, true} {
		d := frequencyDeduper(t, "on", prefer)
		for i, hz := range []int64{14025000, 14030000, 14035000} {
			if !d.ShouldForward(frequencySpot(hz, 120+i*30, false, -30, "CW", spot.SourceManual)) {
				t.Fatal("initial frequency sequence suppressed")
			}
		}
		// Budget is exhausted and this spot's minute is earlier than the
		// current anchor. Report-upgrade precedence still makes it forward.
		reported := frequencySpot(14040000, 60, true, -20, "CW", spot.SourceManual)
		forward, evidence := d.Evaluate(reported)
		if !forward || !evidence.HasAnchor || evidence.AnchorHz != 14035000 {
			t.Fatalf("report upgrade/evidence=%t/%+v", forward, evidence)
		}
		e := candidateEntryFor(t, d, reported)
		if e.changes != 2 || e.anchorHz != 14040000 || e.anchorMinute != reported.Time.Truncate(time.Minute).Unix() || !e.when.Equal(reported.Time) || !e.hasReport || e.snr != -20 {
			t.Fatalf("report exception state=%+v", e)
		}
		stronger := frequencySpot(14045000, 30, true, -10, "CW", spot.SourceManual)
		if got := d.ShouldForward(stronger); got != prefer {
			t.Fatalf("stronger earlier-minute forward=%t prefer=%t", got, prefer)
		}
		if prefer {
			e = candidateEntryFor(t, d, stronger)
			if e.changes != 2 || e.anchorHz != 14045000 || e.anchorMinute != stronger.Time.Truncate(time.Minute).Unix() {
				t.Fatalf("stronger reset count or did not move anchor: %+v", e)
			}
		}
	}
}

func TestSecondaryFrequencyChangeResetsReportState(t *testing.T) {
	for _, prefer := range []bool{false, true} {
		d := frequencyDeduper(t, "on", prefer)
		a := frequencySpot(14025000, 0, true, 20, "CW", spot.SourceRBN)
		b := frequencySpot(14027000, 10, false, -20, "CW", spot.SourceRBN)
		if !d.ShouldForward(a) || !d.ShouldForward(b) {
			t.Fatal("A/B did not forward")
		}
		e := candidateEntryFor(t, d, b)
		if e.hasReport || e.snr != -20 || e.changes != 1 || !e.when.Equal(a.Time) {
			t.Fatalf("frequency change did not replace report state: %+v", e)
		}
		report := frequencySpot(14027000, 20, true, -30, "CW", spot.SourceRBN)
		if !d.ShouldForward(report) {
			t.Fatal("B report upgrade compared against A")
		}
		if got := d.ShouldForward(frequencySpot(14027000, 30, true, -29, "CW", spot.SourceRBN)); got != prefer {
			t.Fatal("stronger report policy differs")
		}
		e = candidateEntryFor(t, d, report)
		if e.changes != 1 {
			t.Fatal("report renewal replenished allowance")
		}
	}
}

func TestSecondaryFrequencyModeIdentityAndHistoricalKeys(t *testing.T) {
	for _, keyMode := range []SecondaryKeyMode{SecondaryKeyGrid2, SecondaryKeyCQZone} {
		for _, band := range []string{"20m", " 20m ", "é20m", "ßm", "", "2m"} {
			s := frequencySpot(14025000, 0, false, 0, "CW", spot.SourceRBN)
			s.BandNorm = band
			if got, want := secondaryFrequencyKey(s, 291, keyMode), secondaryKey(s, 291, keyMode); got != want {
				t.Fatalf("band=%q optimized key=%x want=%x", band, got, want)
			}
		}
	}
	d := frequencyDeduper(t, "on", false)
	for _, mode := range []string{"FT8", "CW"} {
		if !d.ShouldForward(frequencySpot(14025000, 0, false, 0, mode, spot.SourceRBN)) {
			t.Fatalf("skimmer mode %s suppressed", mode)
		}
	}
	for i, mode := range []string{"CW", "USB", "FT8", ""} {
		if got := d.ShouldForward(frequencySpot(14025000+int64(i)*500, i, false, 0, mode, spot.SourceManual)); got != (i == 0) {
			t.Fatalf("human inferred mode %q changes equality/tolerance", mode)
		}
	}
}

func TestSecondaryFrequencyKeyLiteralVectors(t *testing.T) {
	// Default taxonomy IDs are sorted canonical names: CW=1, FT8=4. The
	// literal vectors independently pin the mode byte to offset 21 while
	// preserving every historical country/region/call/source byte.
	for _, tt := range []struct {
		mode   string
		source spot.SourceType
		want   string
	}{
		{"CW", spot.SourceRBN, "32304d002301464e4b3141424300000000000000020100000000000000000000"},
		{"FT8", spot.SourceRBN, "32304d002301464e4b3141424300000000000000020400000000000000000000"},
		{"CW", spot.SourceManual, "32304d002301464e4b3141424300000000000000010000000000000000000000"},
	} {
		d := frequencyDeduper(t, "on", false)
		s := frequencySpot(14025000, 0, false, 0, tt.mode, tt.source)
		d.ShouldForward(s)
		found := ""
		for _, shard := range d.changes.shards {
			for key := range shard.cache {
				found = hex.EncodeToString(key[:])
			}
		}
		if found != tt.want {
			t.Fatalf("mode=%s source=%s key=%s want=%s", tt.mode, tt.source, found, tt.want)
		}
	}
}

func TestSecondaryFrequencyMinuteAndExpiryBoundaries(t *testing.T) {
	for _, offset := range []int{-180, -179, -178, 178, 179, 180} {
		d := frequencyDeduper(t, "on", false)
		d.ShouldForward(frequencySpot(14025000, 0, false, 0, "CW", spot.SourceRBN))
		if got := d.ShouldForward(frequencySpot(14025000, offset, false, 0, "CW", spot.SourceRBN)); got != (offset <= -179 || offset >= 179) {
			t.Fatalf("offset=%d forward=%t", offset, got)
		}
	}
	d := frequencyDeduper(t, "on", false)
	a := frequencySpot(14025000, 70, false, 0, "CW", spot.SourceRBN)
	d.ShouldForward(a)
	if d.ShouldForward(frequencySpot(14027000, 59, false, 0, "CW", spot.SourceRBN)) {
		t.Fatal("earlier minute forwarded")
	}
	if !d.ShouldForward(frequencySpot(14027000, 60, false, 0, "CW", spot.SourceRBN)) {
		t.Fatal("same minute change suppressed")
	}
	d.cleanupAt(a.Time.Add(179 * time.Second))
	if _, _, n := d.GetStats(); n != 1 {
		t.Fatal("cleanup removed exact-window entry")
	}
	d.cleanupAt(a.Time.Add(179*time.Second + time.Nanosecond))
	if _, _, n := d.GetStats(); n != 0 {
		t.Fatal("cleanup retained expired entry")
	}
}

func TestSecondaryFrequencyShadowLifetimeGolden(t *testing.T) {
	d := frequencyDeduper(t, "shadow", false)
	for i, tt := range []struct {
		hz     int64
		at     int
		report bool
		want   bool
	}{{14025000, 0, true, true}, {14030000, 10, false, false}, {14030000, 20, true, false}, {14030000, 180, true, true}, {14030000, 200, true, false}} {
		got, evidence := d.Evaluate(frequencySpot(tt.hz, tt.at, tt.report, -10, "CW", spot.SourceManual))
		if got != tt.want || evidence.Enabled {
			t.Fatalf("step %d output/evidence=%t/%+v", i, got, evidence)
		}
	}
	snapshot, ok := d.ShadowStats()
	want := ShadowCounters{BaselineForwarded: 2, BaselineSuppressed: 3, AddedNewEntry: 1,
		AddedReportException: 1, AddedFrequencyChange: 1, LostForwards: 1, FrequencyAdmissions: 1}
	if !ok || snapshot.Cohorts[ShadowHuman] != want || snapshot.BaselineCacheSize != 1 || snapshot.CandidateCacheSize != 1 {
		t.Fatalf("snapshot=%+v wantHuman=%+v", snapshot, want)
	}
}

func TestSecondaryFrequencyShadowLimitDenominator(t *testing.T) {
	d := frequencyDeduper(t, "shadow", false)
	for i, hz := range []int64{14025000, 14030000, 14035000, 14040000} {
		d.ShouldForward(frequencySpot(hz, i*10, false, 0, "CW", spot.SourceManual))
	}
	d.ShouldForward(frequencySpot(14040000, 40, true, -10, "CW", spot.SourceManual))
	d.ShouldForward(frequencySpot(14045000, -1, false, 0, "CW", spot.SourceManual))
	stats, _ := d.ShadowStats()
	want := ShadowCounters{BaselineForwarded: 2, BaselineSuppressed: 4, AddedFrequencyChange: 2, FrequencyAdmissions: 2, LimitBlocked: 1}
	if got := stats.Cohorts[ShadowHuman]; got != want {
		t.Fatalf("allowance/earlier-minute/report counters=%+v want=%+v", got, want)
	}
	// A permitted frequency change can also be a baseline expiry forward.
	// It still belongs in the admission denominator, not the added tally.
	d = frequencyDeduper(t, "shadow", false)
	for _, event := range []struct {
		hz     int64
		at     int
		report bool
	}{
		{14025000, 0, true}, {14030000, 10, false}, {14030000, 20, true}, {14035000, 180, true},
	} {
		d.ShouldForward(frequencySpot(event.hz, event.at, event.report, -10, "CW", spot.SourceManual))
	}
	stats, _ = d.ShadowStats()
	if got := stats.Cohorts[ShadowHuman]; got.FrequencyAdmissions != 2 || got.AddedFrequencyChange != 1 {
		t.Fatalf("both-forward admission omitted from denominator: %+v", got)
	}
}

func TestSecondaryFrequencyDisabledWindowAndStopBeforeStart(t *testing.T) {
	for _, state := range []string{"shadow", "on"} {
		for _, window := range []time.Duration{0, -time.Second} {
			d, err := NewSecondaryDeduperWithFrequencyChanges(window, false, SecondaryKeyCQZone, frequencyOptions(state))
			if err != nil {
				t.Fatal(err)
			}
			s := frequencySpot(14025000, 0, false, 0, "CW", spot.SourceRBN)
			for range 3 {
				forward, evidence := d.Evaluate(s)
				if !forward || evidence.Enabled {
					t.Fatal("disabled window changed output")
				}
			}
			if p, dup, size := d.GetStats(); p != 0 || dup != 0 || size != 0 {
				t.Fatal("disabled window counted or cached")
			}
			d.Stop()
			d.Start()
			d.Stop()
		}
	}
}

func TestSecondaryFrequencyShadowMatchesOffMixedTrace(t *testing.T) {
	for _, prefer := range []bool{false, true} {
		d := frequencyDeduper(t, "shadow", prefer)
		baseline := NewSecondaryDeduper(179*time.Second, prefer)
		for i := range 400 {
			mode := []string{"CW", "FT8", "USB"}[i%3]
			source := []spot.SourceType{spot.SourceRBN, spot.SourceManual}[i%2]
			s := frequencySpot(14025000+int64(i%7)*1000, (i%19)*17, i%4 != 0, i%21-20, mode, source)
			if i%29 == 0 {
				s.DEMetadata.ADIF = 0
			}
			if i%37 == 0 {
				now := frequencyTestTime.Add(time.Duration(i%23) * time.Minute)
				d.cleanupAt(now)
				baseline.cleanupAt(now)
			}
			if got, want := d.ShouldForward(s), baseline.ShouldForward(s); got != want {
				t.Fatalf("prefer=%t step=%d output=%t want=%t", prefer, i, got, want)
			}
		}
		p, u, n := d.GetStats()
		bp, bu, bn := baseline.GetStats()
		if p != bp || u != bu || n != bn {
			t.Fatalf("baseline stats differ: %d/%d/%d versus %d/%d/%d", p, u, n, bp, bu, bn)
		}
	}
}

func TestSecondaryFrequencyShadowForcedPairAndCleanup(t *testing.T) {
	d := frequencyDeduper(t, "shadow", false)
	a := frequencySpot(14025000, 0, false, 0, "CW", spot.SourceRBN)
	b, other := a.Clone(), a.Clone()
	key := secondaryFrequencyKey(a, 291, d.keyMode)
	index := xxh3.Hash(key[:]) & 63
	found := false
	for _, call := range []string{"K2ABC", "K3ABC", "K4ABC", "K5ABC"} {
		other.DXCallNorm = call
		otherKey := secondaryFrequencyKey(other, 291, d.keyMode)
		if xxh3.Hash(otherKey[:])&63 != index {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("unrelated-shard fixture did not find a different shard")
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	d.changes.pairHook = func() {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
	}
	var wg sync.WaitGroup
	var releaseOnce sync.Once
	finish := func() { releaseOnce.Do(func() { close(release) }); wg.Wait() }
	t.Cleanup(finish)
	wg.Go(func() { d.ShouldForward(a) })
	<-entered
	bDone, cleanupDone := make(chan struct{}), make(chan struct{})
	wg.Go(func() { d.ShouldForward(b); close(bDone) })
	wg.Go(func() { d.cleanupAt(frequencyTestTime); close(cleanupDone) })
	select {
	case <-bDone:
		t.Fatal("competing same-key call escaped pair lock")
	case <-cleanupDone:
		t.Fatal("paired cleanup escaped pair lock")
	case <-time.After(20 * time.Millisecond):
	}
	otherDone := make(chan struct{})
	wg.Go(func() { d.ShouldForward(other); close(otherDone) })
	select {
	case <-otherDone:
	case <-time.After(time.Second):
		t.Fatal("unrelated shard did not progress")
	}
	finish()
	snapshot, _ := d.ShadowStats()
	for _, cohort := range snapshot.Cohorts {
		if cohort.AddedNewEntry != 0 || cohort.AddedReportException != 0 || cohort.AddedFrequencyChange != 0 || cohort.LostForwards != 0 {
			t.Fatalf("identical decisions produced shadow differences: %+v", cohort)
		}
	}
}

func TestSecondaryFrequencyLifecycleBypassAndValidation(t *testing.T) {
	for _, state := range []string{"off", "shadow", "on"} {
		d := frequencyDeduper(t, state, false)
		if state == "off" && d.changes != nil {
			t.Fatal("off constructed candidate state")
		}
		for _, adif := range []int{0, -1} {
			s := frequencySpot(14025000, 0, false, 0, "CW", spot.SourceRBN)
			s.DEMetadata.ADIF = adif
			if !d.ShouldForward(s) {
				t.Fatal("missing country suppressed")
			}
		}
		if !d.ShouldForward(nil) {
			t.Fatal("nil input suppressed")
		}
		if _, _, size := d.GetStats(); size != 0 {
			t.Fatal("bypass retained cache state")
		}
		if state == "shadow" {
			stats, _ := d.ShadowStats()
			if stats.Cohorts[ShadowCWRTTY].BaselineForwarded != 2 {
				t.Fatal("bypass forwards missing from denominator")
			}
		}
		d.Start()
		d.Stop()
		if state != "off" {
			d.Stop()
			d.Start()
			select {
			case <-d.changes.done:
			default:
				t.Fatal("cleanup worker survived stop")
			}
		}
	}
	for _, mutate := range []func(*FrequencyChangeOptions){
		func(o *FrequencyChangeOptions) { o.State = "bogus" },
		func(o *FrequencyChangeOptions) { o.MaxChanges = 0 },
		func(o *FrequencyChangeOptions) { o.HumanToleranceHz = -1 },
	} {
		o := frequencyOptions("on")
		mutate(&o)
		if _, err := NewSecondaryDeduperWithFrequencyChanges(time.Minute, false, SecondaryKeyGrid2, o); err == nil {
			t.Fatal("invalid constructor accepted")
		}
	}
	for _, hz := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -1, 0, math.MaxFloat64} {
		if _, ok := wholeHz(hz); ok {
			t.Fatalf("invalid frequency %v converted", hz)
		}
	}
}

func TestSecondaryFrequencyOrdinaryRepeatDoesNotWriteEntry(t *testing.T) {
	d := frequencyDeduper(t, "on", false)
	s := frequencySpot(14025000, 0, true, -10, "CW", spot.SourceRBN)
	d.ShouldForward(s)
	before := candidateEntryFor(t, d, s)
	s.Time = s.Time.Add(time.Second)
	if d.ShouldForward(s) {
		t.Fatal("repeat forwarded")
	}
	if after := candidateEntryFor(t, d, s); !reflect.DeepEqual(before, after) {
		t.Fatal("plain repeat changed retained entry")
	}
}
