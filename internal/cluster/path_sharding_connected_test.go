// File role: Times bounded submission-to-consumer completion through the actual
// normal/path-only item consumers and public Predictor readers. Telnet wire
// correctness is a separate package witness; this is not wire latency evidence.
package cluster

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/testsupport"
	"dxcluster/spot"
)

var pathShardingRoleNames = [4]string{"normal", "path_only", "ordinary", "beacon"}

type pathShardingWork struct {
	sequence    uint64
	sample      int
	submittedNS int64
	report      *spot.Spot
}

type pathShardingRoleRun struct {
	Name              string    `json:"name"`
	StartSequence     uint64    `json:"start_sequence"`
	StartedUTC        time.Time `json:"started_utc"`
	FirstCompletedUTC time.Time `json:"first_completed_utc"`
	FinishedUTC       time.Time `json:"finished_utc"`
	StartedNS         int64     `json:"started_ns"`
	FinishedNS        int64     `json:"finished_ns"`
	Submitted         uint64    `json:"submitted"`
	Completed         uint64    `json:"completed"`
	QueuePeak         int       `json:"queue_peak"`
	Samples           []int64   `json:"-"`
	firstSubmittedNS  int64
	firstCompletedNS  int64
}

type pathShardingRun struct {
	StartedUTC       time.Time              `json:"started_utc"`
	FinishedUTC      time.Time              `json:"finished_utc"`
	StartedNS        int64                  `json:"started_ns"`
	FinishedNS       int64                  `json:"finished_ns"`
	ElapsedNS        int64                  `json:"elapsed_ns"`
	OverlapNS        int64                  `json:"all_roles_overlap_ns"`
	ClockSource      string                 `json:"clock_source"`
	ClockFrequencyHz int64                  `json:"clock_frequency_hz"`
	Roles            [4]pathShardingRoleRun `json:"roles"`
	DomainFinishedNS int64                  `json:"domain_finished_ns"`
	DomainElapsedNS  int64                  `json:"domain_elapsed_ns"`
	OwnersRetired    int                    `json:"owners_retired"`
}

type pathShardingRunControl struct {
	firstSuccess   *[4]chan struct{}
	beforeStart    func() error
	retireExternal func()
	afterJoin      func()
}

// One bounded queue and one item consumer own each role. Separate submitters
// apply backpressure. Samples start before constructing/submitting work, not
// after dequeue; no completion is sampled away and no event history is kept.
func (f *pathShardingConnectedFixture) run(ctx context.Context, t testing.TB, counts [4]int) (pathShardingRun, error) {
	ctx, cancel := context.WithCancel(ctx)
	return f.runOwned(ctx, cancel, t, counts, nil, pathShardingRunControl{})
}

// Only the unscored profile fixture supplies first-success channels. Scored
// runs have no shared progress counters or per-operation checker atomics. The
// one-shot profile callbacks bracket only released work, excluding fixture
// setup and post-join sequence reconstruction and correctness checks.
func (f *pathShardingConnectedFixture) runOwned(ctx context.Context, cancel context.CancelFunc, t testing.TB, counts [4]int, shared *testsupport.Clock, control pathShardingRunControl) (result pathShardingRun, runErr error) {
	defer cancel()
	done := ctx.Done()
	var queues [4]chan pathShardingWork
	var base [4]uint64
	var submitClocks, consumeClocks [4]*testsupport.Clock
	var consumerRuns [4]pathShardingRoleRun
	var producerRuns [4]struct {
		submitted uint64
		queuePeak int
	}
	elapsedClock := shared
	if elapsedClock == nil {
		elapsedClock = testsupport.NewClock(t)
	}
	result.ClockSource, result.ClockFrequencyHz = elapsedClock.Source(), elapsedClock.FrequencyHz()
	for role := range queues {
		if counts[role] <= 0 {
			return result, fmt.Errorf("nonpositive frozen role count: %d", counts[role])
		}
		queues[role] = make(chan pathShardingWork, pathShardingQueueSize)
		base[role] = f.completed[role]
		if shared != nil {
			submitClocks[role], consumeClocks[role] = shared, shared
		} else {
			submitClocks[role], consumeClocks[role] = testsupport.NewClock(t), testsupport.NewClock(t)
		}
		consumerRuns[role] = pathShardingRoleRun{Name: pathShardingRoleNames[role], StartSequence: base[role], Samples: make([]int64, counts[role])}
		for index := range consumerRuns[role].Samples {
			consumerRuns[role].Samples[index] = -1
		}
	}
	var workers, ready sync.WaitGroup
	var errorOnce sync.Once
	var workErr error
	fail := func(err error) { errorOnce.Do(func() { workErr = err; cancel() }) }
	start := make(chan struct{})
	retired := make(chan struct{}, 8)
	retirementReceipts := 0
	released, stopped := false, false
	stopProfiles := func() {
		if !stopped {
			stopped = true
			if control.retireExternal != nil {
				control.retireExternal()
			}
			result.DomainFinishedNS = elapsedClock.NowNS()
			if result.StartedNS != 0 {
				result.DomainElapsedNS = result.DomainFinishedNS - result.StartedNS
			}
			if control.afterJoin != nil {
				control.afterJoin()
			}
		}
	}
	ready.Add(8)
	workers.Add(8)
	// A fatal profile-setup helper must still release and join the parked role
	// owners. Callbacks do not run in those owners or on an operation boundary.
	defer func() {
		cancel()
		if !released {
			close(start)
		}
		workers.Wait()
		for {
			select {
			case <-retired:
				retirementReceipts++
			default:
				result.OwnersRetired = retirementReceipts
				stopProfiles()
				if workErr != nil {
					runErr = workErr
				}
				return
			}
		}
	}()
	for role := range queues {
		go func(role int) {
			defer workers.Done()
			defer func() { retired <- struct{}{} }()
			defer close(queues[role])
			clock := submitClocks[role]
			local := producerRuns[role]
			defer func() { producerRuns[role] = local }()
			ready.Done()
			select {
			case <-done:
				return
			case <-start:
			}
			for index := 0; index < counts[role]; index++ {
				select {
				case <-done:
					return
				default:
				}
				job := pathShardingWork{sequence: base[role] + uint64(index), sample: index, submittedNS: clock.NowNS()}
				if role < 2 {
					job.report = f.report(job.sequence, role)
				}
				select {
				case queues[role] <- job:
					local.submitted++
					local.queuePeak = max(local.queuePeak, len(queues[role]))
				case <-done:
					return
				}
			}
		}(role)
		go func(role int) {
			defer workers.Done()
			defer func() { retired <- struct{}{} }()
			clock := consumeClocks[role]
			local := consumerRuns[role]
			defer func() { consumerRuns[role] = local }()
			ready.Done()
			select {
			case <-done:
				return
			case <-start:
			}
			for {
				select {
				case <-done:
					return
				case job, ok := <-queues[role]:
					if !ok {
						return
					}
					select {
					case <-done:
						return
					default:
					}
					if local.Completed == 0 {
						local.StartedUTC = time.Now().UTC()
						local.StartedNS = clock.NowNS()
					}
					if err := f.consume(role, job); err != nil {
						fail(err)
						return
					}
					completedNS := clock.NowNS()
					completedUTC := time.Now().UTC()
					local.Samples[job.sample] = completedNS - job.submittedNS
					local.Completed++
					local.FinishedNS, local.FinishedUTC = completedNS, completedUTC
					if local.Completed == 1 {
						local.FirstCompletedUTC = completedUTC
						local.firstSubmittedNS, local.firstCompletedNS = job.submittedNS, completedNS
						if control.firstSuccess != nil {
							close(control.firstSuccess[role])
						}
					}
				}
			}
		}(role)
	}
	ready.Wait()
	if shared != nil {
		runtime.GC()
	}
	if control.beforeStart != nil {
		if err := control.beforeStart(); err != nil {
			return result, err
		}
	}
	result.StartedUTC = time.Now().UTC()
	startedNS := elapsedClock.NowNS()
	result.StartedNS = startedNS
	released = true
	close(start)
	for range 8 {
		select {
		case <-retired:
			retirementReceipts++
		case <-done:
			return result, ctx.Err()
		}
	}
	contextErr := ctx.Err()
	cancel()
	workers.Wait()
	result.OwnersRetired = retirementReceipts
	result.FinishedNS = elapsedClock.NowNS()
	result.ElapsedNS = result.FinishedNS - startedNS
	result.FinishedUTC = time.Now().UTC()
	stopProfiles()
	// All role owners have joined and timing has stopped. Reconstruct accepted
	// sequence prefixes here, then compare them with production admission/raw
	// and source counters in the guard. No checker totals are shared in-flight.
	for role := range result.Roles {
		result.Roles[role] = consumerRuns[role]
		result.Roles[role].Submitted, result.Roles[role].QueuePeak = producerRuns[role].submitted, producerRuns[role].queuePeak
		f.completed[role] += consumerRuns[role].Completed
		if role < 2 {
			for sequence := base[role]; sequence < base[role]+consumerRuns[role].Completed; sequence++ {
				index, direction, _ := f.selection(sequence, role)
				f.counts[index][direction]++
			}
		}
	}
	if workErr != nil {
		return result, workErr
	}
	if contextErr != nil {
		return result, contextErr
	}
	lastStart, firstFinish := result.Roles[0].StartedNS, result.Roles[0].FinishedNS
	for role, completed := range result.Roles {
		if completed.Submitted != uint64(counts[role]) || completed.Completed != uint64(counts[role]) {
			return result, fmt.Errorf("%s lost work: submitted=%d completed=%d want=%d", completed.Name, completed.Submitted, completed.Completed, counts[role])
		}
		if completed.StartedNS > lastStart {
			lastStart = completed.StartedNS
		}
		if completed.FinishedNS < firstFinish {
			firstFinish = completed.FinishedNS
		}
		for index, sample := range completed.Samples {
			if sample < 0 {
				return result, fmt.Errorf("%s sample %d/%d is missing/negative: %d ns; submitted=%d completed=%d interval_ns=%d first_submitted_ns=%d first_completed_ns=%d first_delta_ns=%d first_completed_utc=%s", completed.Name,
					index, len(completed.Samples), sample, completed.Submitted, completed.Completed, completed.FinishedNS-completed.StartedNS, completed.firstSubmittedNS, completed.firstCompletedNS, completed.firstCompletedNS-completed.firstSubmittedNS, completed.FirstCompletedUTC.Format(time.RFC3339Nano))
			}
		}
	}
	result.OverlapNS = firstFinish - lastStart
	return result, nil
}

func (f *pathShardingConnectedFixture) consume(role int, job pathShardingWork) error {
	switch role {
	case 0:
		original := job.report.Time
		if !f.pipeline.prepareFanoutSpot(&outputSpotContext{spot: job.report}) || job.report.Time != original {
			return fmt.Errorf("normal consumer rejected or rewrote report %d", job.sequence)
		}
	case 1:
		original := job.report.Time
		// The production channel owner delegates exactly this item operation.
		// The default preflight separately witnesses that unchanged channel loop.
		processPSKRPathOnlySpot(job.report, time.Now().UTC(), f.predictor, f.metrics, &f.pathStats, config.SpotPolicy{}, f.allowed)
		if job.report.Time != original {
			return fmt.Errorf("path-only consumer rewrote report %d", job.sequence)
		}
	case 2, 3:
		if result := f.prediction(job.sequence, role); !f.valid(result, role == 3) {
			return fmt.Errorf("%s prediction %d returned a fallback/invalid result: %+v", pathShardingRoleNames[role], job.sequence, result)
		}
	default:
		return fmt.Errorf("unknown connected role %d", role)
	}
	return nil
}

func TestPathShardingConnectedPreflight(t *testing.T) {
	for _, mode := range pathShardingModes {
		t.Run(mode, func(t *testing.T) {
			fixture := newPathShardingConnectedFixture(t, mode)
			fixture.channelWitness(t)
			// Enough fixed work is needed for four actual consumer intervals to
			// overlap; very short batches can finish before peers are scheduled.
			run, err := fixture.run(t.Context(), t, [4]int{16384, 16384, 16384, 16384})
			if err != nil {
				t.Fatal(err)
			}
			if run.OverlapNS <= 0 {
				t.Fatal("connected preflight roles had no actual work overlap")
			}
			fixture.guard(t)
			t.Logf("connected consumer preflight: mode=%s completed=%d/%d/%d/%d resident=%d; telnet wire is separate", mode,
				run.Roles[0].Completed, run.Roles[1].Completed, run.Roles[2].Completed, run.Roles[3].Completed, fixture.predictor.TotalBuckets())
		})
	}
}
