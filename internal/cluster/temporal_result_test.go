//go:build !qualification

package cluster

import (
	"sync"
	"testing"
	"time"

	"dxcluster/buffer"
	"dxcluster/stats"
)

func TestRuntimeTemporalResultConsumerHandoffAttempts(t *testing.T) {
	for _, mode := range []string{"no_consumers", "refused_archive", "several_consumers"} {
		t.Run(mode, func(t *testing.T) {
			tracker := stats.NewTracker()
			result := &runtimeTemporalResult{tracker: tracker}
			writer := newDeliveryTestArchiveWriter(t)
			p := &outputPipeline{archiveWriter: writer}
			if mode == "refused_archive" {
				// A stopped real writer refuses this call before queue admission.
				// Attempt accounting must not claim successful delivery.
				writer.Stop()
			} else {
				p.buf = buffer.NewRingBuffer(4)
			}
			ctx := outputSpotContext{spot: newDeliveryTestSpot("K1ABC", "N0AAA", time.Now().UTC()), temporalResult: result}
			plan := outputDeliveryPlan{allowMed: mode != "no_consumers"}
			p.emitSpot(&ctx, plan)
			want := uint64(1)
			if mode == "no_consumers" {
				want = 0
			}
			if got := tracker.TemporalHandoffAttempts(); got != want {
				t.Errorf("actual consumer call attempts=%d want %d", got, want)
			}
			queued, ok := tryReadArchiveQueuedSpot(writer)
			if mode == "several_consumers" {
				rows := p.buf.GetRecent(2)
				if !ok || len(rows) != 1 || queued != rows[0] || rows[0].SnapshotForAsync() != rows[0] {
					t.Fatal("two actual consumers did not receive the same sealed request snapshot")
				}
			} else if ok || p.buf != nil && p.buf.GetCount() != 0 {
				t.Fatal("no-consumer/refused-consumer control unexpectedly admitted output")
			}
			if tracker.TemporalCommitted() != 0 || tracker.TemporalPathSwitches() != 0 {
				t.Fatal("handoff attempt invented acceptance or confirmed path history")
			}
		})
	}
}

func TestRuntimeTemporalResultConcurrentHandoffOnce(t *testing.T) {
	tracker := stats.NewTracker()
	result := &runtimeTemporalResult{tracker: tracker}
	var workers sync.WaitGroup
	for i := 0; i < 64; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 100; j++ {
				result.observeHandoff()
			}
		}()
	}
	workers.Wait()
	if got := tracker.TemporalHandoffAttempts(); got != 1 {
		t.Fatalf("concurrent use of one request marker counted %d handoffs, want 1", got)
	}
}
