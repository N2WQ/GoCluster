// File role: Checks pending/publication retirement, Stop outcome precedence and
// consumer-owned results. Terminal discard releases classifier references
// without stamping the spots or inventing classification outcome counters.
package toxicity

import (
	"context"
	"sync"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestClassifierStopRetiresBlockedPublication(t *testing.T) {
	client := &lifecycleReplyClient{}
	c := NewClassifier(lifecycleConfig(1, 1), testGate(t), client)
	lifecycleCleanup(t, c, nil)
	c.Start()
	spots := []*spot.Spot{
		lifecycleSpot("first buffered classification"),
		lifecycleSpot("second blocked classification"),
		lifecycleSpot("third waiting classification"),
	}
	if c.ClassifyOrEnqueue(spots[0], time.Now().UTC()) {
		t.Fatal("first control was not accepted")
	}
	lifecycleEventually(t, "one completed buffered result", func() bool {
		return c.Pending() == 0 && c.ResultBacklog() == 1 && c.Snapshot().Toxic == 1
	})
	if c.ClassifyOrEnqueue(spots[1], time.Now().UTC()) {
		t.Fatal("blocked-publication control was not accepted")
	}
	lifecycleEventually(t, "second outcome committed before blocked publication", func() bool {
		stats := c.Snapshot()
		return stats.Pending == 1 && stats.ResultBacklog == 1 && stats.Toxic == 2 && stats.CacheEntries == 2
	})
	if c.ClassifyOrEnqueue(spots[2], time.Now().UTC()) {
		t.Fatal("queued-retirement control was not accepted")
	}
	if c.Pending() != 2 || len(c.jobs) != 1 || client.calls.Load() != 2 {
		t.Fatalf("retirement controls not established: %+v jobs=%d calls=%d", c.Snapshot(), len(c.jobs), client.calls.Load())
	}
	lifecycleAwait(t, lifecycleStop(c), "full-result-channel retirement")
	lifecycleRetired(t, c)
	if got := c.Snapshot(); got != (Stats{AICalls: 2, Toxic: 2, CacheEntries: 2}) || client.calls.Load() != 2 {
		t.Fatalf("Stop changed committed outcomes or classified queued work: %+v calls=%d", got, client.calls.Load())
	}
	for _, s := range spots {
		if s.ToxicityStatus != spot.ToxicityUnknown || len(s.ToxicityCategories) != 0 || s.ToxicityModel != "" {
			t.Fatal("discarded result or queued spot was stamped")
		}
	}
}

func TestClassifierStoppedAdmissionBeforeLocalCache(t *testing.T) {
	client := &lifecycleReplyClient{}
	c := NewClassifier(lifecycleConfig(1, 2), testGate(t), client)
	lifecycleCleanup(t, c, nil)
	now := time.Now().UTC()
	c.cache.Put("cached toxic wording", Decision{Status: spot.ToxicityToxic, Categories: []string{"S10"}, Model: "cached"}, now)
	c.cache.Put("cached safe wording", Decision{Status: spot.ToxicitySafe}, now)
	lifecycleAwait(t, lifecycleStop(c), "terminal admission boundary")
	for _, tc := range []struct {
		name, comment string
		when          time.Time
	}{
		{"empty", "", now},
		{"routine", "POTA 73", now},
		{"cached toxic", "cached toxic wording", now},
		{"cached safe", "cached safe wording", now},
		{"uncached", "unseen conversational wording", now},
		{"expired cache", "cached toxic wording", now.Add(2 * time.Minute)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := lifecycleSpot(tc.comment)
			s.ToxicityCategories, s.ToxicityModel = []string{"old"}, "old"
			if !c.ClassifyOrEnqueue(s, tc.when) {
				t.Fatal("post-Stop work was enqueued")
			}
			if s.ToxicityStatus != spot.ToxicityUnavailable || len(s.ToxicityCategories) != 0 || s.ToxicityModel != "" {
				t.Fatalf("post-Stop precedence lost: status=%s categories=%v model=%q", s.ToxicityStatus, s.ToxicityCategories, s.ToxicityModel)
			}
		})
	}
	known := lifecycleSpot("already classified wording")
	known.ToxicityStatus, known.ToxicityCategories, known.ToxicityModel = spot.ToxicityToxic, []string{"S10"}, "known"
	nonhuman := lifecycleSpot("nonhuman wording")
	nonhuman.IsHuman = false
	if !c.ClassifyOrEnqueue(known, now) || !c.ClassifyOrEnqueue(nonhuman, now) || !c.ClassifyOrEnqueue(nil, now) {
		t.Fatal("ineligible submission no longer bypasses")
	}
	if known.ToxicityStatus != spot.ToxicityToxic || known.ToxicityModel != "known" || len(known.ToxicityCategories) != 1 || known.ToxicityCategories[0] != "S10" || nonhuman.ToxicityStatus != spot.ToxicityUnknown {
		t.Fatal("ineligible submission was stamped during retirement")
	}
	c.Start()
	lifecycleAwait(t, lifecycleStop(c), "idempotent admission retirement")
	lifecycleRetired(t, c)
	if got := c.Snapshot(); got != (Stats{Unavailable: 6, CacheEntries: 2}) || client.calls.Load() != 0 {
		t.Fatalf("post-Stop outcomes changed unrelated counters/cache: %+v calls=%d", got, client.calls.Load())
	}
}

func TestClassifierReceivedResultRemainsConsumerOwned(t *testing.T) {
	c := NewClassifier(lifecycleConfig(1, 1), testGate(t), &lifecycleReplyClient{})
	lifecycleCleanup(t, c, nil)
	results := c.Results()
	c.Start()
	s := lifecycleSpot("consumer owned classified wording")
	if c.ClassifyOrEnqueue(s, time.Now().UTC()) {
		t.Fatal("consumer ownership control was not accepted")
	}
	var result Result
	select {
	case result = <-results:
	case <-time.After(lifecycleWait):
		t.Fatal("consumer did not receive its result")
	}
	lifecycleAwait(t, lifecycleStop(c), "retirement after consumer receipt")
	lifecycleRetired(t, c)
	if c.Results() != results {
		t.Fatal("Stop replaced the consumer's result channel")
	}
	if s.ToxicityStatus != spot.ToxicityUnknown {
		t.Fatal("Stop stamped a consumer-owned spot")
	}
	if result.Spot != s || result.Decision.Status != spot.ToxicityToxic || result.Decision.Model != "lifecycle-fixture" || len(result.Decision.Categories) != 1 || result.Decision.Categories[0] != "S10" {
		t.Fatalf("Stop changed a received result: %+v", result)
	}
	ApplyResult(result)
	if s.ToxicityStatus != spot.ToxicityToxic || s.ToxicityModel != "lifecycle-fixture" || len(s.ToxicityCategories) != 1 || s.ToxicityCategories[0] != "S10" {
		t.Fatal("consumer could not apply its result after Stop")
	}
	if got := c.Snapshot(); got != (Stats{AICalls: 1, Toxic: 1, CacheEntries: 1}) {
		t.Fatalf("consumer receipt changed committed counters: %+v", got)
	}
}

type lifecycleLateSuccessClient struct {
	entered  chan struct{}
	canceled chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (c *lifecycleLateSuccessClient) unblock() { c.once.Do(func() { close(c.release) }) }

func (c *lifecycleLateSuccessClient) Classify(ctx context.Context, _ string) (Decision, error) {
	close(c.entered)
	select {
	case <-ctx.Done():
		close(c.canceled)
		// The fixture deliberately delays return after observing cancellation
		// so Stop must join rather than merely request cancellation.
		<-c.release
		return Decision{Status: spot.ToxicityToxic, Categories: []string{"S10"}, Model: "late"}, nil
	case <-c.release:
		return Decision{}, context.Canceled
	}
}

func TestClassifierStopRejectsLateSuccessCommit(t *testing.T) {
	client := &lifecycleLateSuccessClient{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	c := NewClassifier(lifecycleConfig(1, 1), testGate(t), client)
	lifecycleCleanup(t, c, client.unblock)
	c.Start()
	s := lifecycleSpot("late worker success wording")
	if c.ClassifyOrEnqueue(s, time.Now().UTC()) {
		t.Fatal("late-completion control was not accepted")
	}
	lifecycleAwait(t, client.entered, "late-completion client entry")
	stopped := lifecycleStop(c)
	lifecycleAwait(t, client.canceled, "Stop cancellation observed by client")
	select {
	case <-stopped:
		t.Fatal("Stop returned before its worker joined")
	default:
	}
	if got := c.Snapshot(); got != (Stats{AICalls: 1, Pending: 1}) {
		t.Fatalf("work was retired before its worker returned: %+v", got)
	}
	client.unblock()
	lifecycleAwait(t, stopped, "late-completion retirement")
	lifecycleRetired(t, c)
	if got := c.Snapshot(); got != (Stats{AICalls: 1}) {
		t.Fatalf("post-boundary success committed an outcome/cache entry: %+v", got)
	}
	if s.ToxicityStatus != spot.ToxicityUnknown || len(s.ToxicityCategories) != 0 || s.ToxicityModel != "" {
		t.Fatal("late retired success stamped its spot")
	}
}
