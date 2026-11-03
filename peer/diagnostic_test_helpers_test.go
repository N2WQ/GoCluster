package peer

import (
	"testing"
	"time"

	"dxcluster/internal/asynclog"
	"dxcluster/internal/peerdiag"
)

const diagnosticTestDaily = 29
const diagnosticTestOverlong = 30

type diagnosticTestEvent struct {
	stream uint8
	at     time.Time
	line   string
}

type diagnosticTestSink struct {
	stream  uint8
	events  chan diagnosticTestEvent
	entered chan struct{}
	release <-chan struct{}
}

func (s *diagnosticTestSink) Write(at time.Time, _ uint8, data []byte) asynclog.WriteResult {
	if s.entered != nil {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		<-s.release
	}
	select {
	case s.events <- diagnosticTestEvent{stream: s.stream, at: at, line: string(data)}:
	default:
	}
	return asynclog.Written
}
func (*diagnosticTestSink) Close() error { return nil }

type diagnosticTestEmitter struct{ daily, overlong asynclog.Handle }

func (e diagnosticTestEmitter) Emit(stream uint8, at time.Time, payload []byte) bool {
	if stream == diagnosticTestDaily {
		return e.daily.Emit(at, 0, payload)
	}
	return e.overlong.Emit(at, 0, payload)
}
func (e diagnosticTestEmitter) Status(stream uint8) peerdiag.StreamStatus {
	owner := e.overlong
	if stream == diagnosticTestDaily {
		owner = e.daily
	}
	state := owner.Stats()
	return peerdiag.StreamStatus{Generation: state.Generation, Queued: state.Queued, Written: state.Written,
		Dropped: state.Dropped, Closed: state.Closed, CleanupFailed: state.CleanupFailed,
		ChargedBytes: int64(2 * asynclog.SlotBackingBytes)}
}

func newDiagnosticTestService(t *testing.T, enabled bool, block bool) (*peerdiag.Service, <-chan diagnosticTestEvent, func()) {
	t.Helper()
	events := make(chan diagnosticTestEvent, 1024)
	release := make(chan struct{})
	bind := func(stream uint8) asynclog.Handle {
		sink := &diagnosticTestSink{stream: stream, events: events}
		if block && stream == diagnosticTestDaily {
			sink.entered, sink.release = make(chan struct{}, 1), release
		}
		owner, err := asynclog.StartSingle(stream, func() asynclog.Sink { return sink })
		if err != nil {
			t.Fatal(err)
		}
		return owner
	}
	emitter := diagnosticTestEmitter{daily: bind(diagnosticTestDaily), overlong: bind(diagnosticTestOverlong)}
	service := peerdiag.New(enabled, emitter, diagnosticTestDaily, diagnosticTestOverlong)
	closed := false
	cleanup := func() {
		if !closed {
			closed = true
			close(release)
			service.Stop()
			emitter.daily.Stop()
			emitter.overlong.Stop()
			for _, done := range []<-chan struct{}{emitter.daily.Done(), emitter.overlong.Done()} {
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("diagnostic test worker did not retire")
				}
			}
		}
	}
	t.Cleanup(cleanup)
	return service, events, cleanup
}
