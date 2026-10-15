package pathreliability

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"dxcluster/internal/testsupport"
)

type pathShardAPIInterval struct {
	StartNS   int64 `json:"start_ns"`
	EndNS     int64 `json:"end_ns"`
	ElapsedNS int64 `json:"elapsed_ns"`
}

type pathShardLatencyClass struct {
	Samples         int                    `json:"sample_count"`
	P999NS          int64                  `json:"empirical_p999_ns"`
	MaximumNS       int64                  `json:"maximum_ns"`
	SparseEmpirical bool                   `json:"sparse_empirical_maximum"`
	ZeroNS          int                    `json:"zero_ns_samples"`
	Phases          []pathShardAPIInterval `json:"api_intervals"`
}

type pathShardMaintenanceResult struct {
	EndNS         int64                 `json:"end_ns"`
	Removed       [2]int                `json:"removed"`
	Compacted     [2]int                `json:"compacted_shards"`
	RefillReports int                   `json:"refill_reports"`
	Refill        pathShardLatencyClass `json:"refill"`
	Purge         pathShardLatencyClass `json:"purge"`
	Compact       pathShardLatencyClass `json:"compact"`
	CountReport   pathShardLatencyClass `json:"count_report"`
	Histogram     pathShardLatencyClass `json:"histogram_report"`
}

type pathShardMaintenanceExpected struct {
	removed    int
	compacted  [2]int
	finalPeaks []int
	hotCounts  []int
}

type pathShardMaintenanceEvidence struct {
	expected   pathShardMaintenanceExpected
	admissions []ReportAdmission
	counts     [2]pathStoreStatsSnapshot
	histograms [2]WeightHistogram
}

// Derive expected contraction from the immutable grown physical population.
// This is untimed setup, never an oracle scan during the concurrent campaign.
func (f *pathShardFixture) prepareMaintenance(t testing.TB) {
	t.Helper()
	expected := pathShardMaintenanceExpected{
		removed:    len(f.allKeys) - len(f.hotKeys),
		finalPeaks: make([]int, len(f.predictor.combined.shards)),
		hotCounts:  make([]int, len(f.predictor.combined.shards)),
	}
	grown, initialPeaks := make([]int, len(expected.hotCounts)), make([]int, len(expected.hotCounts))
	for i := range grown {
		sh := &f.predictor.combined.shards[i]
		sh.mu.RLock()
		grown[i], initialPeaks[i] = len(sh.buckets), sh.peak
		for key := range sh.buckets {
			if f.hotKeys[key] {
				expected.hotCounts[i]++
			}
		}
		sh.mu.RUnlock()
	}
	peaks := initialPeaks
	for phase := range 2 {
		totalHot, totalPeak := 0, 0
		for i := range peaks {
			if phase == 1 {
				peaks[i] = max(peaks[i], grown[i])
			}
			totalHot += expected.hotCounts[i]
			totalPeak += peaks[i]
		}
		distributed := totalPeak >= 1000 && 2*totalHot < totalPeak
		for i, peak := range peaks {
			current := expected.hotCounts[i]
			if peak >= 1000 && current < peak/2 || peak < 1000 && distributed && 2*current < peak {
				expected.compacted[phase]++
				peaks[i] = current
			}
		}
		if expected.compacted[phase] == 0 {
			t.Fatalf("frozen physical corpus does not exercise contraction phase %d", phase)
		}
	}
	copy(expected.finalPeaks, peaks)
	f.maintenanceEvidence = pathShardMaintenanceEvidence{expected: expected, admissions: make([]ReportAdmission, len(f.coldReports))}
	f.maintenanceRun = pathShardMaintenanceResult{
		RefillReports: len(f.coldReports),
		Refill:        pathShardLatencyClass{Phases: make([]pathShardAPIInterval, len(f.coldReports))},
		Purge:         pathShardLatencyClass{Phases: make([]pathShardAPIInterval, 2)},
		Compact:       pathShardLatencyClass{Phases: make([]pathShardAPIInterval, 2)},
		CountReport:   pathShardLatencyClass{Phases: make([]pathShardAPIInterval, 2)},
		Histogram:     pathShardLatencyClass{Phases: make([]pathShardAPIInterval, 2)},
	}
}

// The timed path contains production API work and preallocated result recording
// only. It performs no oracle scans, expected-count updates, or repeat Compact.
func (f *pathShardFixture) runMaintenance(ctx context.Context, epoch int64, clock *testsupport.Clock) error {
	done := ctx.Done()
	result := &f.maintenanceRun
	for phase := range 2 {
		select {
		case <-done:
			return ctx.Err()
		default:
		}
		interval := &result.Purge.Phases[phase]
		interval.StartNS = clock.NowNS() - epoch
		result.Removed[phase] = f.predictor.PurgeStale(f.query)
		interval.EndNS = clock.NowNS() - epoch
		interval.ElapsedNS = interval.EndNS - interval.StartNS
		interval = &result.Compact.Phases[phase]
		interval.StartNS = clock.NowNS() - epoch
		result.Compacted[phase] = f.predictor.Compact(1000, 0.5)
		interval.EndNS = clock.NowNS() - epoch
		interval.ElapsedNS = interval.EndNS - interval.StartNS
		interval = &result.CountReport.Phases[phase]
		interval.StartNS = clock.NowNS() - epoch
		f.predictor.RefreshStatsSnapshot(f.query)
		interval.EndNS = clock.NowNS() - epoch
		interval.ElapsedNS = interval.EndNS - interval.StartNS
		// Maintenance is the sole stats refresh owner. A refresh publishes a new
		// immutable byBand slice, so this value copy retains its actual result
		// without an extra store scan, cache lock or allocation in the campaign.
		f.maintenanceEvidence.counts[phase] = f.predictor.combined.stats
		interval = &result.Histogram.Phases[phase]
		interval.StartNS = clock.NowNS() - epoch
		f.maintenanceEvidence.histograms[phase] = f.predictor.WeightHistogramByBand(f.query)
		interval.EndNS = clock.NowNS() - epoch
		interval.ElapsedNS = interval.EndNS - interval.StartNS
		if phase == 0 {
			if err := f.refillCold(ctx, epoch, clock); err != nil {
				return err
			}
		}
	}
	result.EndNS = clock.NowNS() - epoch
	return ctx.Err()
}

func (f *pathShardFixture) refillCold(ctx context.Context, epoch int64, clock *testsupport.Clock) error {
	done := ctx.Done()
	coldAt := f.query.Add(-7201 * time.Second)
	for i, path := range f.coldReports {
		select {
		case <-done:
			return ctx.Err()
		default:
		}
		interval := &f.maintenanceRun.Refill.Phases[i]
		interval.StartNS = clock.NowNS() - epoch
		f.maintenanceEvidence.admissions[i] = f.predictor.UpdateWithReceiverHash(BucketCombined,
			path.Receiver.Fine, path.Sender.Fine, path.Receiver.Coarse, path.Sender.Coarse,
			path.Band, -10.5, 1, ObservationTimes{ObservedAt: coldAt, ArrivedAt: coldAt}, false,
			0x7100000000000000|uint64(path.Receiver.Fine))
		interval.EndNS = clock.NowNS() - epoch
		interval.ElapsedNS = interval.EndNS - interval.StartNS
	}
	return nil
}

func (f *pathShardFixture) verifyMaintenance(t testing.TB, roles [4]pathShardRoleResult) {
	t.Helper()
	result, expected := &f.maintenanceRun, f.maintenanceEvidence.expected
	for phase := range 2 {
		if result.Removed[phase] != expected.removed || result.Compacted[phase] != expected.compacted[phase] {
			t.Fatalf("maintenance phase %d removed/compacted=(%d,%d), precomputed=(%d,%d)", phase, result.Removed[phase], result.Compacted[phase], expected.removed, expected.compacted[phase])
		}
		interval := result.Compact.Phases[phase]
		for _, role := range roles {
			if interval.StartNS >= role.EndNS || interval.EndNS <= role.StartNS {
				t.Fatalf("actual Compact phase %d interval=(%d,%d) did not overlap %s=(%d,%d)", phase, interval.StartNS, interval.EndNS, role.Role, role.StartNS, role.EndNS)
			}
		}
		f.verifyMaintenanceReports(t, phase)
	}
	for key := range f.wantCounts {
		if !f.hotKeys[key] {
			delete(f.wantCounts, key)
		}
	}
	for i, path := range f.coldReports {
		if !f.maintenanceEvidence.admissions[i].Accepted() {
			t.Fatalf("refill report %d rejected: %+v", i, f.maintenanceEvidence.admissions[i])
		}
		for _, key := range path.keys() {
			if f.hotKeys[key] {
				f.wantCounts[key]++
			}
		}
	}
	for i, peak := range expected.finalPeaks {
		sh := &f.predictor.combined.shards[i]
		sh.mu.RLock()
		current, gotPeak := len(sh.buckets), sh.peak
		sh.mu.RUnlock()
		if current != expected.hotCounts[i] || gotPeak != peak {
			t.Fatalf("post-campaign shard %d occupancy/peak=(%d,%d), precomputed=(%d,%d)", i, current, gotPeak, expected.hotCounts[i], peak)
		}
	}
	if repeated := f.predictor.Compact(1000, 0.5); repeated != 0 {
		t.Fatalf("post-campaign unchanged population compacted %d shards", repeated)
	}
	for _, class := range []*pathShardLatencyClass{&result.Refill, &result.Purge, &result.Compact, &result.CountReport, &result.Histogram} {
		latencies := make([]int64, len(class.Phases))
		for i, interval := range class.Phases {
			if interval.StartNS <= 0 || interval.EndNS < interval.StartNS || interval.ElapsedNS != interval.EndNS-interval.StartNS {
				t.Fatalf("missing or inconsistent actual API interval: %+v", interval)
			}
			latencies[i] = interval.ElapsedNS
			if interval.ElapsedNS == 0 {
				class.ZeroNS++
			}
		}
		slices.Sort(latencies)
		class.Samples = len(latencies)
		class.P999NS, class.MaximumNS = latencies[(999*len(latencies)+999)/1000-1], latencies[len(latencies)-1]
		// Two rare API observations give an empirical maximum, not evidence
		// about a population tail. Keep their classes separate from refill.
		class.SparseEmpirical = len(latencies) == 2
	}
}

func (f *pathShardFixture) verifyMaintenanceReports(t testing.TB, phase int) {
	t.Helper()
	fine, coarse := 0, 0
	bands := f.predictor.combined.bandIndex.idxToBand
	wantBands := make([]bandCounts, len(bands))
	for key := range f.hotKeys {
		if key&0xffff == 0 {
			fine++
			wantBands[uint16(key>>48)].fine++
		} else {
			coarse++
			wantBands[uint16(key>>32)].coarse++
		}
	}
	stats := f.maintenanceEvidence.counts[phase]
	if !stats.available || !stats.hasSnapshot || stats.asOfUnix != f.query.Unix() || stats.fine != fine || stats.coarse != coarse {
		t.Fatalf("phase %d count report differs from fixed hot population: %+v", phase, stats)
	}
	if !slices.Equal(stats.byBand, wantBands) {
		t.Fatalf("phase %d per-band count publication=%+v want=%+v", phase, stats.byBand, wantBands)
	}
	histogram := f.maintenanceEvidence.histograms[phase]
	if !histogram.Available || histogram.AsOfUnix != f.query.Unix() || len(histogram.Bands) != len(bands) || !slices.Equal(histogram.Edges, []float64{1, 2, 3, 5, 10}) {
		t.Fatalf("phase %d weight report unavailable or incoherent: %+v", phase, histogram)
	}
	total := 0
	for i, band := range histogram.Bands {
		binTotal := 0
		for _, count := range band.Bins {
			if count < 0 {
				t.Fatalf("phase %d band %s has negative histogram count", phase, band.Band)
			}
			binTotal += count
		}
		if band.Band != bands[i] || band.Total != wantBands[i].fine+wantBands[i].coarse || binTotal != band.Total || len(band.Bins) != 6 {
			t.Fatalf("phase %d per-band histogram differs from fixed physical population: %+v", phase, band)
		}
		total += band.Total
	}
	if total != len(f.hotKeys) {
		t.Fatalf("phase %d weight report contains %d buckets, want %d", phase, total, len(f.hotKeys))
	}
}

// Execution owns only fixed fixture goroutines and one-shot lifecycle state.
// Every owner is parked before the scored epoch. stop cancels before releasing
// and joining them, including when a callback unwinds through Fatal or Skip.
type pathShardExecution struct {
	fixture            *pathShardFixture
	ctx                context.Context
	cancel             context.CancelFunc
	scenario           string
	operations         int
	clocks             [4]*testsupport.Clock
	result             *pathShardRunResult
	epoch              int64
	start              chan struct{}
	active             chan struct{}
	allActive          chan struct{}
	workers            sync.WaitGroup
	released           bool
	rolesRetired       [4]bool
	maintenanceRetired bool
	maintenanceErr     error
}

func (f *pathShardFixture) prepareExecution(ctx context.Context, cancel context.CancelFunc, scenario string, operations int, clocks [4]*testsupport.Clock, maintenanceClock *testsupport.Clock, result *pathShardRunResult) *pathShardExecution {
	e := &pathShardExecution{fixture: f, ctx: ctx, cancel: cancel, scenario: scenario,
		operations: operations, clocks: clocks, result: result,
		start: make(chan struct{}), active: make(chan struct{}, 4), allActive: make(chan struct{})}
	if scenario == "Serial" {
		return e
	}
	done := ctx.Done()
	var ready sync.WaitGroup
	for role := range result.Roles {
		e.workers.Add(1)
		ready.Add(1)
		go func(role int) {
			defer e.workers.Done()
			defer func() { e.rolesRetired[role] = true }()
			ready.Done()
			select {
			case <-done:
				e.result.Roles[role].err = ctx.Err()
				return
			case <-e.start:
			}
			e.result.Roles[role] = f.runRole(ctx, cancel, role, operations, e.epoch, clocks[role], e.result.Roles[role], e.active)
		}(role)
	}
	if f.maintenance {
		e.workers.Add(1)
		ready.Add(1)
		go func() {
			defer e.workers.Done()
			defer func() { e.maintenanceRetired = true }()
			ready.Done()
			for range 4 {
				select {
				case <-done:
					e.maintenanceErr = ctx.Err()
					return
				case <-e.active:
				}
			}
			close(e.allActive)
			e.maintenanceErr = f.runMaintenance(ctx, e.epoch, maintenanceClock)
		}()
	}
	ready.Wait()
	return e
}

func (e *pathShardExecution) release() {
	if !e.released {
		e.released = true
		close(e.start)
	}
}

func (e *pathShardExecution) run(epoch int64) {
	e.epoch = epoch
	if e.scenario == "Serial" {
		for role := range e.result.Roles {
			e.result.Roles[role] = e.fixture.runRole(e.ctx, e.cancel, role, e.operations, epoch, e.clocks[role], e.result.Roles[role], nil)
			e.rolesRetired[role] = true
			if e.result.Roles[role].err != nil {
				return
			}
		}
	} else {
		e.release()
		e.workers.Wait()
	}
}

// Error/result inspection is outside the scored interval and profile window.
func (e *pathShardExecution) err() error {
	// Preserve the originating worker failure rather than replacing it with a
	// sibling's cancellation. Every result is inspected only after its owner joins.
	for _, role := range e.result.Roles {
		if role.err != nil && !errors.Is(role.err, context.Canceled) && !errors.Is(role.err, context.DeadlineExceeded) {
			return role.err
		}
	}
	if e.maintenanceErr != nil {
		return e.maintenanceErr
	}
	return e.ctx.Err()
}

func (e *pathShardExecution) stop() {
	e.cancel()
	e.release()
	e.workers.Wait()
}
