package cluster

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"dxcluster/buffer"
	"dxcluster/cty"
	"dxcluster/internal/toxicity"
	"dxcluster/spot"
)

type clusterToxicityClient struct{}

func (clusterToxicityClient) Classify(context.Context, string) (toxicity.Decision, error) {
	return toxicity.Decision{Status: spot.ToxicityToxic, Categories: []string{"S10"}, Model: "mock"}, nil
}

type blockingClusterToxicityClient struct {
	release chan struct{}
}

func (b blockingClusterToxicityClient) Classify(ctx context.Context, _ string) (toxicity.Decision, error) {
	select {
	case <-b.release:
		return toxicity.Decision{Status: spot.ToxicityToxic, Categories: []string{"S10"}, Model: "mock"}, nil
	case <-ctx.Done():
		return toxicity.Decision{}, ctx.Err()
	}
}

func TestOutputPipelineToxicityStageQueuesHumanOnly(t *testing.T) {
	gate, err := toxicity.NewSafeGateFromLists([]string{"CQ", "73"}, []string{"POTA"}, 8)
	if err != nil {
		t.Fatal(err)
	}
	classifier := toxicity.NewClassifier(toxicity.Config{
		Enabled:         true,
		Workers:         1,
		QueueSize:       2,
		CacheMaxEntries: 8,
		CacheTTLSeconds: 60,
		MaxCommentBytes: 512,
		TimeoutMS:       100,
	}, gate, clusterToxicityClient{})
	classifier.Start()
	defer classifier.Stop()
	pipeline := &outputPipeline{toxicityClassifier: classifier}

	skimmer := spot.NewSpot("K1ABC", "W1XYZ", 14074, "FT8")
	skimmer.SourceType = spot.SourceRBN
	skimmer.Comment = "non routine"
	if !pipeline.applyToxicityStage(skimmer) {
		t.Fatalf("skimmer spot should bypass toxicity queue")
	}

	human := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
	human.Comment = "non routine"
	if pipeline.applyToxicityStage(human) {
		t.Fatalf("non-routine human comment should queue for AI")
	}
	select {
	case result := <-classifier.Results():
		toxicity.ApplyResult(result)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for toxicity result")
	}
	if human.ToxicityStatus != spot.ToxicityToxic {
		t.Fatalf("expected toxic status, got %q", human.ToxicityStatus)
	}
	if !pipeline.applyToxicityStage(human) {
		t.Fatalf("classified spot should not requeue")
	}
}

func TestBlankBeaconFallbackDoesNotBecomeToxicityInput(t *testing.T) {
	gate, err := toxicity.NewSafeGateFromLists([]string{"CQ", "73"}, []string{"POTA"}, 8)
	if err != nil {
		t.Fatal(err)
	}
	classifier := toxicity.NewClassifier(toxicity.Config{
		Enabled:         true,
		Workers:         1,
		QueueSize:       2,
		CacheMaxEntries: 8,
		CacheTTLSeconds: 60,
		MaxCommentBytes: 512,
		TimeoutMS:       100,
	}, gate, clusterToxicityClient{})
	classifier.Start()
	defer classifier.Stop()
	pipeline := &outputPipeline{
		toxicityClassifier: classifier,
		ctyLookup:          func() *cty.CTYDatabase { return nil },
	}

	humanBeacon := spot.NewSpot("W1ABC/B", "W1XYZ", 14074, "CW")
	humanBeacon.IsHuman = true
	humanBeacon.Comment = ""
	if !pipeline.applyToxicityStage(humanBeacon) {
		t.Fatalf("blank human beacon comment should not queue for AI")
	}
	if humanBeacon.ToxicityStatus != spot.ToxicitySafeLocal {
		t.Fatalf("expected blank comment to be classified before fallback as SAFE_LOCAL, got %q", humanBeacon.ToxicityStatus)
	}
	if humanBeacon.Comment != "" {
		t.Fatalf("expected toxicity stage not to synthesize comment, got %q", humanBeacon.Comment)
	}
	ctx, ok := pipeline.prepareSpotContext(humanBeacon)
	if !ok {
		t.Fatalf("expected spot context")
	}
	if got := ctx.spot.FormatDXCluster(); !strings.Contains(got, "CW BEACON") {
		t.Fatalf("expected formatter fallback after toxicity, got %q", got)
	}
	if ctx.spot.Comment != "" {
		t.Fatalf("expected formatter fallback not to mutate comment, got %q", ctx.spot.Comment)
	}
}

func TestOutputPipelineDrainsToxicityOnInputClose(t *testing.T) {
	gate, err := toxicity.NewSafeGateFromLists([]string{"CQ", "73"}, []string{"POTA"}, 8)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	classifier := toxicity.NewClassifier(toxicity.Config{
		Enabled:         true,
		Workers:         1,
		QueueSize:       2,
		CacheMaxEntries: 8,
		CacheTTLSeconds: 60,
		MaxCommentBytes: 512,
		TimeoutMS:       500,
	}, gate, blockingClusterToxicityClient{release: release})
	classifier.Start()

	output := make(chan *spot.Spot, 1)
	recent := buffer.NewRingBuffer(4)
	pipeline := &outputPipeline{
		outputChan:         output,
		toxicityClassifier: classifier,
		buf:                recent,
		ctyLookup:          func() *cty.CTYDatabase { return nil },
	}

	done := make(chan struct{})
	go func() {
		pipeline.run()
		close(done)
	}()

	human := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
	human.Comment = "non routine"
	output <- human

	deadline := time.After(time.Second)
	for classifier.Pending() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for pending classifier job")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(output)
	close(release)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pipeline did not stop")
	}

	recentSpots := recent.GetRecent(1)
	if len(recentSpots) != 1 {
		t.Fatalf("expected drained spot in recent buffer, got %d", len(recentSpots))
	}
	if recentSpots[0].ToxicityStatus != spot.ToxicityToxic {
		t.Fatalf("expected drained toxicity status TOXIC, got %q", recentSpots[0].ToxicityStatus)
	}
}

type toxicityDrainFixture struct {
	pending atomic.Int64
	results chan toxicity.Result
	idle    chan struct{}
}

func (f *toxicityDrainFixture) Pending() int64                  { return f.pending.Load() }
func (f *toxicityDrainFixture) ResultBacklog() int              { return len(f.results) }
func (f *toxicityDrainFixture) Results() <-chan toxicity.Result { return f.results }
func (f *toxicityDrainFixture) PendingIdle() <-chan struct{}    { return f.idle }

func TestOutputPipelineDrainsBeforeGraceAfterFinalResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recent := buffer.NewRingBuffer(4)
		p := &outputPipeline{buf: recent, ctyLookup: func() *cty.CTYDatabase { return nil }}
		p.initLifecycle()
		p.drainDeadline = time.Now().Add(time.Second)
		close(p.stopCh)

		source := &toxicityDrainFixture{
			results: make(chan toxicity.Result, 1),
			idle:    make(chan struct{}, 1),
		}
		source.pending.Store(1)
		source.idle <- struct{}{} // A notification from an earlier job may be queued.
		s := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
		s.Comment = "non routine"
		source.results <- toxicity.Result{Spot: s, Decision: toxicity.Decision{Status: spot.ToxicityToxic}}

		done := make(chan struct{})
		go func() {
			p.drainToxicityPending(source, p.drainDeadline)
			close(done)
		}()
		synctest.Wait()
		if got := recent.GetRecent(2); len(got) != 1 || got[0].ToxicityStatus != spot.ToxicityToxic {
			t.Fatalf("expected one delivered classification while worker was pending, got %v", got)
		}
		select {
		case <-done:
			t.Fatal("pipeline finished before the worker")
		default:
		}

		source.pending.Store(0)
		select {
		case source.idle <- struct{}{}:
		default:
		}
		<-done
		if !time.Now().Before(p.drainDeadline) || len(recent.GetRecent(2)) != 1 {
			t.Fatal("expected drain before grace expired with exactly one delivery")
		}
	})
}

func TestFormatToxicitySummaryExposesBoundedCounters(t *testing.T) {
	gate, err := toxicity.NewSafeGateFromLists([]string{"CQ", "73"}, []string{"POTA"}, 8)
	if err != nil {
		t.Fatal(err)
	}
	classifier := toxicity.NewClassifier(toxicity.Config{
		Enabled:         true,
		Workers:         1,
		QueueSize:       1,
		CacheMaxEntries: 8,
		CacheTTLSeconds: 60,
		MaxCommentBytes: 512,
		TimeoutMS:       100,
	}, gate, clusterToxicityClient{})
	classifier.Start()
	defer classifier.Stop()

	routine := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
	routine.IsHuman = true
	routine.Comment = "CQ 73"
	if !classifier.ClassifyOrEnqueue(routine, time.Now().UTC()) {
		t.Fatal("routine comment should not enqueue")
	}

	human := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
	human.IsHuman = true
	human.Comment = "non routine"
	if classifier.ClassifyOrEnqueue(human, time.Now().UTC()) {
		t.Fatal("non-routine comment should enqueue")
	}
	select {
	case result := <-classifier.Results():
		toxicity.ApplyResult(result)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for classifier result")
	}

	// Receiving a result transfers ownership before the publishing worker's
	// deferred pending decrement. Join it before asserting terminal counters.
	classifier.Stop()
	wantStats := toxicity.Stats{SafeBypass: 1, AICalls: 1, Toxic: 1, CacheEntries: 1}
	if got := classifier.Snapshot(); got != wantStats {
		t.Fatalf("joined classifier counters: got %+v want %+v", got, wantStats)
	}

	line := formatToxicitySummary(classifier)
	for _, want := range []string{"Toxicity:", "(AI)", "(L)", "(H)", "(T)", "(U)", "q=0", "cache=", "err"} {
		if !strings.Contains(line, want) {
			t.Fatalf("expected toxicity summary to contain %q, got %q", want, line)
		}
	}
	if strings.Contains(line, human.Comment) || strings.Contains(line, "mock") || strings.Contains(line, "S10") {
		t.Fatalf("toxicity summary leaked classifier payload detail: %q", line)
	}
}
