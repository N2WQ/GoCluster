package cluster

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/dedup"
	"dxcluster/spot"
	"dxcluster/telnet"
)

func TestOutputPipelineStabilizerHoldDefersFinalFanout(t *testing.T) {
	ring := buffer.NewRingBuffer(4)
	writer := newDeliveryTestArchiveWriter(t)
	srv := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil)
	pipeline := newDeliveryTestPipeline(ring, writer, srv)
	pipeline.stabilizerEnabled = true
	pipeline.telnetStabilizer = newTelnetSpotStabilizer(time.Minute, 4)

	pipeline.deliverSpot(&outputSpotContext{spot: newDeliveryTestSpot("K1HLD", "N0AAA", time.Now().UTC())})

	if got := ring.GetCount(); got != 0 {
		t.Fatalf("expected held spot to skip ring buffer until release, got %d", got)
	}
	if _, ok := tryReadArchiveQueuedSpot(writer); ok {
		t.Fatalf("expected held spot to skip archive until release")
	}
	if _, ok := tryReadTelnetBroadcastSpot(srv); ok {
		t.Fatalf("expected held spot to skip telnet broadcast until release")
	}
	if pending := pipeline.telnetStabilizer.Pending(); pending != 1 {
		t.Fatalf("expected one pending stabilizer spot, got %d", pending)
	}
}

func TestOutputPipelineStabilizerDelayedReleaseUsesFinalFanout(t *testing.T) {
	ring := buffer.NewRingBuffer(4)
	writer := newDeliveryTestArchiveWriter(t)
	srv := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil)
	pipeline := newDeliveryTestPipeline(ring, writer, srv)

	delayed := newDeliveryTestSpot("K1REL", "N0AAA", time.Now().UTC())
	pipeline.handleStabilizerRelease(&telnetStabilizerEnvelope{
		spot:            delayed,
		checksCompleted: 0,
		delayReason:     stabilizerDelayReasonUnknownOrNonRecent.String(),
	})

	if got := ring.GetCount(); got != 1 {
		t.Fatalf("expected delayed release in ring buffer, got %d", got)
	}
	if archived, ok := tryReadArchiveQueuedSpot(writer); !ok || archived.DXCallNorm != "K1REL" {
		t.Fatalf("expected delayed release archived, got ok=%v spot=%v", ok, archived)
	}
	if broadcasted, ok := tryReadTelnetBroadcastSpot(srv); !ok || broadcasted.DXCallNorm != "K1REL" {
		t.Fatalf("expected delayed release broadcast, got ok=%v spot=%v", ok, broadcasted)
	}
}

func TestOutputPipelineStabilizerSuppressSkipsFinalFanout(t *testing.T) {
	ring := buffer.NewRingBuffer(4)
	writer := newDeliveryTestArchiveWriter(t)
	srv := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil)
	pipeline := newDeliveryTestPipeline(ring, writer, srv)
	pipeline.correctionCfg.StabilizerTimeoutAction = stabilizerTimeoutSuppress

	pipeline.handleStabilizerRelease(&telnetStabilizerEnvelope{
		spot:            newDeliveryTestSpot("K1SUP", "N0AAA", time.Now().UTC()),
		checksCompleted: 0,
		delayReason:     stabilizerDelayReasonUnknownOrNonRecent.String(),
	})

	if got := ring.GetCount(); got != 0 {
		t.Fatalf("expected suppressed delayed spot to skip ring buffer, got %d", got)
	}
	if _, ok := tryReadArchiveQueuedSpot(writer); ok {
		t.Fatalf("expected suppressed delayed spot to skip archive")
	}
	if _, ok := tryReadTelnetBroadcastSpot(srv); ok {
		t.Fatalf("expected suppressed delayed spot to skip telnet broadcast")
	}
}

func TestOutputPipelineFinalMedDedupeGatesArchiveAndBroadcastAfterDelay(t *testing.T) {
	ring := buffer.NewRingBuffer(4)
	writer := newDeliveryTestArchiveWriter(t)
	srv := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil)
	pipeline := newDeliveryTestPipeline(ring, writer, srv)
	pipeline.secondaryMed = dedup.NewSecondaryDeduper(time.Minute, false)
	pipeline.secondaryActive = true

	now := time.Now().UTC()
	if !pipeline.secondaryMed.ShouldForward(newDeliveryTestSpot("K1DUP", "N0AAA", now)) {
		t.Fatalf("expected first secondary MED observation to pass")
	}
	pipeline.handleStabilizerRelease(&telnetStabilizerEnvelope{
		spot:            newDeliveryTestSpot("K1DUP", "N0AAA", now.Add(10*time.Second)),
		checksCompleted: 0,
		delayReason:     stabilizerDelayReasonUnknownOrNonRecent.String(),
	})

	if got := ring.GetCount(); got != 0 {
		t.Fatalf("expected MED duplicate to skip ring buffer, got %d", got)
	}
	if _, ok := tryReadArchiveQueuedSpot(writer); ok {
		t.Fatalf("expected MED duplicate to skip archive")
	}
	if _, ok := tryReadTelnetBroadcastSpot(srv); ok {
		t.Fatalf("expected MED duplicate to skip telnet broadcast")
	}
}

func TestOutputPipelineStabilizerOverflowFailOpenUsesFinalFanout(t *testing.T) {
	ring := buffer.NewRingBuffer(4)
	writer := newDeliveryTestArchiveWriter(t)
	srv := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil)
	pipeline := newDeliveryTestPipeline(ring, writer, srv)
	pipeline.stabilizerEnabled = true
	pipeline.telnetStabilizer = newTelnetSpotStabilizer(time.Minute, 1)
	if !pipeline.telnetStabilizer.Enqueue(newDeliveryTestSpot("K1FILL", "N0AAA", time.Now().UTC())) {
		t.Fatalf("expected prefill enqueue to reserve stabilizer capacity")
	}

	pipeline.deliverSpot(&outputSpotContext{spot: newDeliveryTestSpot("K1OVR", "N0BBB", time.Now().UTC())})

	if got := ring.GetCount(); got != 1 {
		t.Fatalf("expected overflow fail-open spot in ring buffer, got %d", got)
	}
	if archived, ok := tryReadArchiveQueuedSpot(writer); !ok || archived.DXCallNorm != "K1OVR" {
		t.Fatalf("expected overflow fail-open archive, got ok=%v spot=%v", ok, archived)
	}
	if broadcasted, ok := tryReadTelnetBroadcastSpot(srv); !ok || broadcasted.DXCallNorm != "K1OVR" {
		t.Fatalf("expected overflow fail-open broadcast, got ok=%v spot=%v", ok, broadcasted)
	}
}

func BenchmarkOutputPipelineBuildFinalDeliveryPlan(b *testing.B) {
	pipeline := &outputPipeline{
		secondaryMed: dedup.NewSecondaryDeduper(time.Minute, false),
	}
	s := newDeliveryTestSpot("K1BENCH", "N0AAA", time.Now().UTC())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Time = s.Time.Add(time.Nanosecond)
		plan := pipeline.buildFinalDeliveryPlan(s, spot.ResolverSnapshot{}, false, time.Now().UTC())
		if !plan.allowMed && i == 0 {
			b.Fatalf("expected first final delivery plan to allow MED")
		}
	}
}

func newDeliveryTestPipeline(ring *buffer.RingBuffer, writer *archive.Writer, srv *telnet.Server) *outputPipeline {
	return &outputPipeline{
		buf:             ring,
		archiveWriter:   writer,
		telnet:          srv,
		ctyLookup:       func() *cty.CTYDatabase { return nil },
		recentBandStore: newRecentBandStoreForStabilizerAdmissionTests(),
		correctionCfg: config.CallCorrectionConfig{
			StabilizerEnabled:       true,
			StabilizerMaxChecks:     1,
			StabilizerTimeoutAction: stabilizerTimeoutRelease,
		},
	}
}

func newDeliveryTestArchiveWriter(t *testing.T) *archive.Writer {
	t.Helper()
	writer, err := archive.NewWriter(config.ArchiveConfig{
		DBPath:    filepath.Join(t.TempDir(), "archive"),
		QueueSize: 8,
	})
	if err != nil {
		t.Fatalf("new archive writer: %v", err)
	}
	t.Cleanup(writer.Stop)
	return writer
}

func newDeliveryTestSpot(dx, de string, at time.Time) *spot.Spot {
	s := spot.NewSpot(dx, de, 14020.0, "CW")
	s.Time = at
	s.Confidence = "?"
	s.DEMetadata.ADIF = 291
	s.DEMetadata.CQZone = 5
	s.DEMetadata.Grid = "FN31"
	s.EnsureNormalized()
	return s
}

func tryReadArchiveQueuedSpot(writer *archive.Writer) (*spot.Spot, bool) {
	queue := unsafeValue(reflect.ValueOf(writer).Elem().FieldByName("queue"))
	select {
	case snapshot := <-queue.Interface().(chan *spot.Spot):
		return snapshot, true
	default:
		return nil, false
	}
}

func tryReadTelnetBroadcastSpot(srv *telnet.Server) (*spot.Spot, bool) {
	broadcast := unsafeValue(reflect.ValueOf(srv).Elem().FieldByName("broadcast"))
	recv, ok := broadcast.TryRecv()
	if !ok {
		return nil, false
	}
	if recv.Kind() == reflect.Pointer {
		recv = recv.Elem()
	}
	return unsafeValue(recv.FieldByName("spot")).Interface().(*spot.Spot), true
}
