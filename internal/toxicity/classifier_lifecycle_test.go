// File role: Proves terminal classifier admission, startup bounds and exact
// retirement without relying on production lifecycle field names. Fixture
// clients cooperate with cancellation and also have independent cleanup releases.
package toxicity

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/spot"
)

const lifecycleWait = 2 * time.Second

func lifecycleConfig(workers, queue int) Config {
	return Config{Enabled: true, Workers: workers, QueueSize: queue,
		CacheMaxEntries: 8, CacheTTLSeconds: 60, MaxCommentBytes: 512, TimeoutMS: 30000}
}

func lifecycleSpot(comment string) *spot.Spot {
	return &spot.Spot{IsHuman: true, Comment: comment, ToxicityStatus: spot.ToxicityUnknown}
}

func lifecycleAwait(t *testing.T, done <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(lifecycleWait):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func lifecycleEventually(t *testing.T, description string, condition func() bool) {
	t.Helper()
	timer := time.NewTimer(lifecycleWait)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatalf("timed out waiting for %s", description)
		}
	}
}

func lifecycleStop(c *Classifier) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		c.Stop()
		close(done)
	}()
	return done
}

func lifecycleCleanup(t *testing.T, c *Classifier, release func()) {
	t.Helper()
	t.Cleanup(func() {
		if release != nil {
			release()
		}
		select {
		case <-lifecycleStop(c):
		case <-time.After(lifecycleWait):
			t.Error("classifier fixture did not join during cleanup")
		}
	})
}

func lifecycleRetired(t *testing.T, c *Classifier) {
	t.Helper()
	if c.Pending() != 0 || c.ResultBacklog() != 0 || len(c.jobs) != 0 {
		t.Fatalf("retained work: pending=%d results=%d jobs=%d", c.Pending(), c.ResultBacklog(), len(c.jobs))
	}
	if c.Results() == nil {
		t.Fatal("Results channel identity was discarded")
	}
	select {
	case result, open := <-c.Results():
		t.Fatalf("Results must remain open and empty: open=%v result=%+v", open, result)
	default:
	}
}

type lifecycleReplyClient struct {
	calls atomic.Int64
}

func (c *lifecycleReplyClient) Classify(context.Context, string) (Decision, error) {
	c.calls.Add(1)
	return Decision{Status: spot.ToxicityToxic, Categories: []string{"S10"}, Model: "lifecycle-fixture"}, nil
}

type lifecycleBarrierClient struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int64
	active  atomic.Int64
	peak    atomic.Int64
}

func newLifecycleBarrierClient() *lifecycleBarrierClient {
	return &lifecycleBarrierClient{entered: make(chan struct{}, 64), release: make(chan struct{})}
}

func (c *lifecycleBarrierClient) unblock() { c.once.Do(func() { close(c.release) }) }

func (c *lifecycleBarrierClient) Classify(ctx context.Context, _ string) (Decision, error) {
	c.calls.Add(1)
	active := c.active.Add(1)
	defer c.active.Add(-1)
	for previous := c.peak.Load(); active > previous; previous = c.peak.Load() {
		if c.peak.CompareAndSwap(previous, active) {
			break
		}
	}
	select {
	case c.entered <- struct{}{}:
	case <-ctx.Done():
		return Decision{}, ctx.Err()
	}
	select {
	case <-c.release:
		return Decision{Status: spot.ToxicityToxic}, nil
	case <-ctx.Done():
		return Decision{}, ctx.Err()
	}
}

func lifecycleEntered(t *testing.T, c *lifecycleBarrierClient, count int) {
	t.Helper()
	for range count {
		select {
		case <-c.entered:
		case <-time.After(lifecycleWait):
			t.Fatal("classifier client entry barrier absent")
		}
	}
}

func TestClassifierPreStartAdmission(t *testing.T) {
	client := &lifecycleReplyClient{}
	c := NewClassifier(lifecycleConfig(1, 2), testGate(t), client)
	lifecycleCleanup(t, c, nil)
	first, second := lifecycleSpot("first conversational comment"), lifecycleSpot("second conversational comment")
	for _, s := range []*spot.Spot{first, second} {
		if c.ClassifyOrEnqueue(s, time.Now().UTC()) {
			t.Fatal("pre-Start work was not accepted")
		}
	}
	if c.Pending() != 2 || len(c.jobs) != 2 || client.calls.Load() != 0 {
		t.Fatalf("wrong pre-Start ownership: %+v calls=%d", c.Snapshot(), client.calls.Load())
	}
	c.Start()
	for _, s := range []*spot.Spot{first, second} {
		select {
		case result := <-c.Results():
			if result.Spot != s || result.Decision.Status != spot.ToxicityToxic {
				t.Fatalf("wrong accepted result: %+v", result)
			}
			ApplyResult(result)
		case <-time.After(lifecycleWait):
			t.Fatal("accepted pre-Start work was lost")
		}
	}
	lifecycleEventually(t, "completed pre-Start accounting", func() bool { return c.Pending() == 0 })
	if client.calls.Load() != 2 || first.ToxicityStatus != spot.ToxicityToxic || second.ToxicityStatus != spot.ToxicityToxic {
		t.Fatal("pre-Start work was not classified exactly once")
	}
}

func TestClassifierStopBeforeStart(t *testing.T) {
	client := &lifecycleReplyClient{}
	c := NewClassifier(lifecycleConfig(2, 2), testGate(t), client)
	lifecycleCleanup(t, c, nil)
	spots := []*spot.Spot{lifecycleSpot("first pending wording"), lifecycleSpot("second pending wording")}
	for _, s := range spots {
		if c.ClassifyOrEnqueue(s, time.Now().UTC()) {
			t.Fatal("expected bounded pre-Start admission")
		}
	}
	lifecycleAwait(t, lifecycleStop(c), "Stop before Start")
	c.Start()
	c.Start()
	lifecycleAwait(t, lifecycleStop(c), "repeated terminal Stop")
	lifecycleRetired(t, c)
	if got := c.Snapshot(); got != (Stats{}) {
		t.Fatalf("retired jobs invented outcomes: %+v", got)
	}
	if client.calls.Load() != 0 {
		t.Fatal("Start after Stop revived queued work")
	}
	for _, s := range spots {
		if s.ToxicityStatus != spot.ToxicityUnknown {
			t.Fatal("discarded pre-Start spot was stamped")
		}
	}
}

func TestClassifierStartOnceBoundsActiveWorkers(t *testing.T) {
	client := newLifecycleBarrierClient()
	c := NewClassifier(lifecycleConfig(2, 12), testGate(t), client)
	lifecycleCleanup(t, c, client.unblock)
	var starts sync.WaitGroup
	for range 12 {
		starts.Go(c.Start)
	}
	started := make(chan struct{})
	go func() { starts.Wait(); close(started) }()
	t.Cleanup(func() {
		client.unblock()
		select {
		case <-started:
		case <-time.After(lifecycleWait):
			t.Error("concurrent Start fixture did not join")
		}
	})
	lifecycleAwait(t, started, "all concurrent Start calls")
	for index := range 10 {
		if c.ClassifyOrEnqueue(lifecycleSpot(fmt.Sprintf("bounded worker comment %d", index)), time.Now().UTC()) {
			t.Fatal("fixture queue unexpectedly refused work")
		}
	}
	lifecycleEntered(t, client, 2)
	// Establish active and queued ownership before Stop; recorded peak/call
	// counts then detect extra workers that entered during this fixture.
	lifecycleEventually(t, "two active workers and eight queued jobs", func() bool {
		return client.active.Load() == 2 && len(c.jobs) == 8
	})
	if got := c.Snapshot(); got != (Stats{AICalls: 2, Pending: 10}) {
		t.Fatalf("accepted queued and in-flight ownership was miscounted: %+v", got)
	}
	lifecycleAwait(t, lifecycleStop(c), "bounded-worker retirement")
	lifecycleRetired(t, c)
	if client.peak.Load() != 2 || client.calls.Load() != 2 || client.active.Load() != 0 {
		t.Fatalf("worker limit changed: calls=%d active=%d peak=%d", client.calls.Load(), client.active.Load(), client.peak.Load())
	}
	if got := c.Snapshot(); got != (Stats{AICalls: 2}) {
		t.Fatalf("terminal cancellation invented outcome counters: %+v", got)
	}
}

func TestClassifierConcurrentStartStopAdmission(t *testing.T) {
	for round := range 12 {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			client := newLifecycleBarrierClient()
			c := NewClassifier(lifecycleConfig(2, 32), testGate(t), client)
			lifecycleCleanup(t, c, client.unblock)
			if round%2 == 0 {
				c.Start()
				for index := range 2 {
					if c.ClassifyOrEnqueue(lifecycleSpot(fmt.Sprintf("primed comment %d", index)), time.Now().UTC()) {
						t.Fatal("active-worker control was not accepted")
					}
				}
				lifecycleEntered(t, client, 2)
			}
			start, done := make(chan struct{}), make(chan struct{})
			var operations sync.WaitGroup
			type admission struct {
				spot      *spot.Spot
				immediate bool
			}
			admitted := make(chan admission, 24)
			for range 6 {
				operations.Go(func() { <-start; c.Start() })
			}
			for range 4 {
				operations.Go(func() { <-start; c.Stop() })
			}
			for index := range 24 {
				operations.Go(func() {
					<-start
					s := lifecycleSpot(fmt.Sprintf("racing submission %d", index))
					admitted <- admission{s, c.ClassifyOrEnqueue(s, time.Now().UTC())}
				})
			}
			go func() { operations.Wait(); close(done) }()
			t.Cleanup(func() {
				client.unblock()
				select {
				case <-done:
				case <-time.After(lifecycleWait):
					t.Error("concurrent lifecycle fixture did not join")
				}
			})
			close(start)
			lifecycleAwait(t, done, "all overlapping lifecycle calls")
			var unavailable uint64
			for range 24 {
				outcome := <-admitted
				want := spot.ToxicityUnknown
				if outcome.immediate {
					unavailable++
					want = spot.ToxicityUnavailable
				}
				if outcome.spot.ToxicityStatus != want {
					t.Fatalf("wrong raced submission ownership: immediate=%v status=%s", outcome.immediate, outcome.spot.ToxicityStatus)
				}
			}
			postStop := lifecycleSpot("submission definitely after Stop")
			if !c.ClassifyOrEnqueue(postStop, time.Now().UTC()) || postStop.ToxicityStatus != spot.ToxicityUnavailable {
				t.Fatal("terminal admission was reopened")
			}
			unavailable++
			lifecycleRetired(t, c)
			want := Stats{AICalls: uint64(client.calls.Load()), Unavailable: unavailable}
			if got := c.Snapshot(); got != want || client.active.Load() != 0 || client.peak.Load() > 2 {
				t.Fatalf("raced accounting or worker limit: got=%+v want=%+v active=%d peak=%d", got, want, client.active.Load(), client.peak.Load())
			}
		})
	}
}
