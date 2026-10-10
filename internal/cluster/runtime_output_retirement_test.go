package cluster

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/dedup"
	"dxcluster/internal/toxicity"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/telnet"
)

const runtimeRetirementWaitLimit = 2 * time.Second

// Each barrier has an independent release, including on failed assertions.
// Client gates also honor cancellation; callback gates deliberately remain
// owned until the test releases them, rather than pretending Stop revoked work.
type runtimeRetirementBarrier struct {
	entered     chan struct{}
	released    chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func newRuntimeRetirementBarrier(t *testing.T) *runtimeRetirementBarrier {
	t.Helper()
	b := &runtimeRetirementBarrier{entered: make(chan struct{}), released: make(chan struct{})}
	t.Cleanup(b.release)
	return b
}

func (b *runtimeRetirementBarrier) enter() {
	b.enterOnce.Do(func() { close(b.entered) })
	<-b.released
}

func (b *runtimeRetirementBarrier) release() {
	b.releaseOnce.Do(func() { close(b.released) })
}

type runtimeRetirementClient struct {
	gates map[string]*runtimeRetirementBarrier // Immutable after construction; at most three fixture jobs.
	calls atomic.Int64
}

func (c *runtimeRetirementClient) Classify(ctx context.Context, comment string) (toxicity.Decision, error) {
	c.calls.Add(1)
	b := c.gates[comment]
	if b == nil {
		return toxicity.Decision{}, fmt.Errorf("unexpected retirement comment %q", comment)
	}
	b.enterOnce.Do(func() { close(b.entered) })
	select {
	case <-b.released:
		return toxicity.Decision{Status: spot.ToxicityToxic, Categories: []string{"S10"}, Model: "retirement-fixture"}, nil
	case <-ctx.Done():
		return toxicity.Decision{}, ctx.Err()
	}
}

func (c *runtimeRetirementClient) releaseAll() {
	for _, b := range c.gates {
		b.release()
	}
}

func runtimeRetirementClassifier(t *testing.T, comments ...string) (*toxicity.Classifier, *runtimeRetirementClient) {
	t.Helper()
	client := &runtimeRetirementClient{gates: make(map[string]*runtimeRetirementBarrier, len(comments))}
	for _, comment := range comments {
		client.gates[comment] = newRuntimeRetirementBarrier(t)
	}
	gate, err := toxicity.NewSafeGateFromLists([]string{"CQ", "73"}, []string{"POTA"}, 8)
	if err != nil {
		t.Fatal(err)
	}
	classifier := toxicity.NewClassifier(toxicity.Config{
		Enabled: true, Workers: 3, QueueSize: 8, CacheMaxEntries: 8,
		CacheTTLSeconds: 60, MaxCommentBytes: 512, TimeoutMS: 30_000,
	}, gate, client)
	return classifier, client
}

func runtimeRetirementRuntime(t *testing.T, classifier *toxicity.Classifier, queueSize int) *clusterRuntime {
	t.Helper()
	writer, err := archive.NewWriter(config.ArchiveConfig{
		DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: queueSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &clusterRuntime{
		cfg: &config.Config{CallCorrection: config.CallCorrectionConfig{
			FT8QuietGapSeconds: 60, FT8HardCapSeconds: 120,
		}},
		ctx: ctx, cancel: cancel,
		deduplicator: dedup.NewDeduplicator(0, false, 8),
		spotBuffer:   buffer.NewRingBuffer(8), toxicityClassifier: classifier,
		archiveWriter: writer, telnetServer: telnet.NewServer(telnet.ServerOptions{BroadcastQueue: queueSize}, nil),
	}
	// Register before launching workers. Later callback/client releases run
	// first, so a failed assertion does not strand cleanup behind a fixture gate.
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() { defer close(done); r.close() }()
		select {
		case <-done:
		case <-time.After(runtimeRetirementWaitLimit):
			t.Error("runtime fixture close did not join after all fixture gates released")
		}
	})
	return r
}

func runtimeRetirementStart(r *clusterRuntime) {
	r.deduplicator.Start()
	if r.toxicityClassifier != nil {
		r.toxicityClassifier.Start()
	}
	r.startOutputPipeline()
}

// Configure before runtime construction starts the scheduler. Never mutate an
// active scheduler's delay just to shorten a test.
func runtimeRetirementConfigureStabilizer(r *clusterRuntime, delaySeconds, maxChecks int, action string) {
	r.cfg.CallCorrection.Enabled = true
	r.cfg.CallCorrection.StabilizerEnabled = true
	r.cfg.CallCorrection.StabilizerDelaySeconds = delaySeconds
	r.cfg.CallCorrection.StabilizerMaxChecks = maxChecks
	r.cfg.CallCorrection.StabilizerMaxPending = 4
	r.cfg.CallCorrection.StabilizerTimeoutAction = action
	r.recentBandStore = newRecentBandStoreForStabilizerAdmissionTests()
	r.statsTracker = stats.NewTracker()
}

func runtimeRetirementSpot(dx, mode, comment string) *spot.Spot {
	s := spot.NewSpot(dx, "N0AAA", 14074, mode)
	s.ToxicityStatus = spot.ToxicityUnknown
	s.Comment = comment
	s.DXMetadata.Grid = "FN31"
	s.DEMetadata.ADIF, s.DEMetadata.CQZone = 291, 5
	s.DEMetadata.Grid = "FN31"
	s.EnsureNormalized()
	return s
}

func runtimeRetirementSend(t *testing.T, r *clusterRuntime, s *spot.Spot) {
	t.Helper()
	select {
	case r.deduplicator.GetInputChannel() <- s:
	case <-time.After(runtimeRetirementWaitLimit):
		t.Fatal("timed out sending to actual primary dedup input")
	}
}

func runtimeRetirementCall(t *testing.T, release func(), fn func()) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	t.Cleanup(func() {
		if release != nil {
			release()
		}
		select {
		case <-done:
		case <-time.After(runtimeRetirementWaitLimit):
			t.Error("test-owned runtime retirement operation did not join")
		}
	})
	go func() { defer close(done); fn() }()
	return done
}

func runtimeRetirementWait(t *testing.T, label string, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(runtimeRetirementWaitLimit):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func runtimeRetirementWaitFor(t *testing.T, label string, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(runtimeRetirementWaitLimit)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", label)
		case <-tick.C:
		}
	}
}

func runtimeRetirementAssertDone(t *testing.T, r *clusterRuntime) {
	t.Helper()
	p := r.outputPipeline
	if p == nil {
		t.Fatal("runtime did not install its output owner")
	}
	runtimeRetirementWait(t, "pipeline completion", p.done)
	if c := r.toxicityClassifier; c != nil && (c.Pending() != 0 || c.ResultBacklog() != 0) {
		t.Fatalf("classifier not retired: pending=%d backlog=%d", c.Pending(), c.ResultBacklog())
	}
	// These fields belong solely to the output goroutine; inspect only after
	// completion, never by polling them while a result may create a new hold.
	if p.ftConfidence.pendingSpots != 0 || len(p.ftConfidence.pending) != 0 || len(p.ftConfidence.queue) != 0 {
		t.Fatalf("FT holds survived completion: spots=%d groups=%d heap=%d",
			p.ftConfidence.pendingSpots, len(p.ftConfidence.pending), len(p.ftConfidence.queue))
	}
	if len(p.temporal.pending) != 0 || len(p.temporal.queue) != 0 {
		t.Fatal("temporal holds survived completion")
	}
	if p.temporalTimer != nil || p.temporalTimerCh != nil || p.ftTimer != nil || p.ftTimerCh != nil || !p.ftTimerDue.IsZero() {
		t.Fatal("pipeline timer references survived completion")
	}
	if p.stabilizerEnabled {
		if s := p.telnetStabilizer; s.Pending() != 0 || len(s.in) != 0 || len(s.release) != 0 {
			t.Fatal("stabilizer reservations or queued references survived pipeline completion")
		}
		select {
		case <-p.stabilizerReleaseDone:
		default:
			t.Fatal("pipeline completion preceded release-consumer completion")
		}
	}
}

func runtimeRetirementAssertSpot(t *testing.T, got, want *spot.Spot) {
	t.Helper()
	if got == nil || got.DXCallNorm != want.DXCallNorm || got.DECallNorm != want.DECallNorm ||
		got.ModeNorm != want.ModeNorm || got.Comment != want.Comment || got.Frequency != want.Frequency ||
		got.ToxicityStatus != spot.NormalizeToxicityStatus(string(want.ToxicityStatus)) || got.ToxicityModel != want.ToxicityModel ||
		!reflect.DeepEqual(got.ToxicityCategories, want.ToxicityCategories) {
		t.Fatalf("wrong owned handoff: got=%+v want=%+v", got, want)
	}
}

func runtimeRetirementAssertHandoffs(t *testing.T, r *clusterRuntime, expected ...*spot.Spot) {
	t.Helper()
	for _, want := range expected {
		archived, archiveOK := tryReadArchiveQueuedSpot(r.archiveWriter)
		broadcast, broadcastOK := tryReadTelnetBroadcastSpot(r.telnetServer)
		if !archiveOK || !broadcastOK {
			t.Fatalf("missing exact handoff for %s: archive=%v broadcast=%v", want.DXCall, archiveOK, broadcastOK)
		}
		runtimeRetirementAssertSpot(t, archived, want)
		runtimeRetirementAssertSpot(t, broadcast, want)
	}
	if _, ok := tryReadArchiveQueuedSpot(r.archiveWriter); ok {
		t.Fatal("unexpected additional archive handoff")
	}
	if _, ok := tryReadTelnetBroadcastSpot(r.telnetServer); ok {
		t.Fatal("unexpected additional telnet handoff")
	}
}

func runtimeRetirementResourceSignals(r *clusterRuntime) (archiveStopped, serverStopped <-chan struct{}) {
	archiveStopped = unsafeValue(reflect.ValueOf(r.archiveWriter).Elem().FieldByName("stop")).Interface().(chan struct{})
	serverStopped = unsafeValue(reflect.ValueOf(r.telnetServer).Elem().FieldByName("shutdown")).Interface().(chan struct{})
	return archiveStopped, serverStopped
}
