package cluster

import (
	"context"
	"io"
	"log"
	"sync"
	"testing"
	"time"
)

type stubReportRunner struct {
	started chan propReportJob
	block   chan struct{}
	err     error
}

type stuckReportRunner struct{ started, release chan struct{} }

func (r *stuckReportRunner) Run(context.Context, propReportJob) error {
	close(r.started)
	<-r.release
	return nil
}

func TestPropReportSchedulerRetainsStuckWorkerAcrossRestart(t *testing.T) {
	runner := &stuckReportRunner{started: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	var release sync.Once
	defer func() { cancel(); release.Do(func() { close(runner.release) }) }()
	s := newPropReportScheduler(runner, nil, time.Minute)
	if !s.Start(ctx) {
		t.Fatal("initial worker not started")
	}
	if !s.Enqueue(propReportJob{Date: time.Now()}) {
		t.Fatal("job rejected")
	}
	select {
	case <-runner.started:
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not start")
	}
	cancel()
	waitCtx, stopWait := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stopWait()
	if s.WaitContext(waitCtx) {
		t.Fatal("blocked runner reported retired")
	}
	replacement := newPropReportScheduler(&stubReportRunner{}, nil, time.Second)
	if replacement.Start(context.Background()) {
		t.Fatal("replacement started while old runner remained blocked")
	}
	if replacement.Enqueue(propReportJob{Date: time.Now()}) {
		t.Fatal("rejected scheduler accepted a job")
	}
	if !replacement.WaitContext(context.Background()) {
		t.Fatal("unstarted scheduler did not retire")
	}
	release.Do(func() { close(runner.release) })
	finishCtx, finish := context.WithTimeout(context.Background(), 5*time.Second)
	defer finish()
	if !s.WaitContext(finishCtx) {
		t.Fatal("old worker did not retire")
	}
	newCtx, cancelNew := context.WithCancel(context.Background())
	newScheduler := newPropReportScheduler(&stubReportRunner{}, nil, time.Second)
	if !newScheduler.Start(newCtx) {
		t.Fatal("replacement refused after confirmed retirement")
	}
	cancelNew()
	if !newScheduler.WaitContext(finishCtx) {
		t.Fatal("replacement did not retire")
	}
}

func TestPropReportSchedulerConcurrentStartAndWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newPropReportScheduler(&stubReportRunner{}, nil, time.Second)
	const attempts = 8
	results := make(chan bool, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- s.Start(ctx) }()
	}
	wg.Wait()
	close(results)
	starts := 0
	for started := range results {
		if started {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("successful starts = %d", starts)
	}
	cancel()
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !s.WaitContext(context.Background()) {
				t.Error("wait failed")
			}
		}()
	}
	wg.Wait()
}

func TestPropReportSchedulerWaitBeforeStartPreventsLaterStart(t *testing.T) {
	s := newPropReportScheduler(&stubReportRunner{}, nil, time.Second)
	if !s.WaitContext(context.Background()) {
		t.Fatal("unstarted scheduler did not retire")
	}
	if s.Start(context.Background()) {
		t.Fatal("retired scheduler started later")
	}
	if s.Enqueue(propReportJob{Date: time.Now()}) {
		t.Fatal("retired scheduler accepted a job")
	}
	if !s.WaitContext(context.Background()) {
		t.Fatal("repeated wait did not return")
	}
}

func (s *stubReportRunner) Run(ctx context.Context, job propReportJob) error {
	if s.started != nil {
		s.started <- job
	}
	if s.block != nil {
		select {
		case <-s.block:
			return s.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.err
}

func TestPropReportSchedulerSkipsDuplicate(t *testing.T) {
	runner := &stubReportRunner{
		started: make(chan propReportJob, 1),
		block:   make(chan struct{}),
	}
	logger := log.New(io.Discard, "", 0)
	scheduler := newPropReportScheduler(runner, logger, 250*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler.Start(ctx)

	jobDate := time.Date(2026, time.January, 23, 0, 0, 0, 0, time.UTC)
	if !scheduler.Enqueue(propReportJob{Date: jobDate, LogPath: "data/logs/23-Jan-2026.log"}) {
		t.Fatalf("expected initial enqueue to succeed")
	}

	select {
	case <-runner.started:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("runner did not start")
	}

	if scheduler.Enqueue(propReportJob{Date: jobDate, LogPath: "data/logs/23-Jan-2026.log"}) {
		t.Fatalf("expected duplicate enqueue to be skipped")
	}

	close(runner.block)
	cancel()
	scheduler.Wait()
}

func TestPropReportSchedulerDropsWhenQueueFull(t *testing.T) {
	runner := &stubReportRunner{}
	logger := log.New(io.Discard, "", 0)
	scheduler := newPropReportScheduler(runner, logger, 250*time.Millisecond)

	job1 := propReportJob{Date: time.Date(2026, time.January, 24, 0, 0, 0, 0, time.UTC), LogPath: "data/logs/24-Jan-2026.log"}
	job2 := propReportJob{Date: time.Date(2026, time.January, 25, 0, 0, 0, 0, time.UTC), LogPath: "data/logs/25-Jan-2026.log"}

	if !scheduler.Enqueue(job1) {
		t.Fatalf("expected first enqueue to succeed")
	}
	if scheduler.Enqueue(job2) {
		t.Fatalf("expected second enqueue to drop when queue is full")
	}
}
