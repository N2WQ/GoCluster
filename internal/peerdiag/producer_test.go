package peerdiag

import (
	"bytes"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"dxcluster/internal/asynclog"
)

type captureEmitter struct {
	mu     sync.Mutex
	stream uint8
	data   []byte
	count  int
	stats  [asynclog.MaxStreams]StreamStatus
}

func (c *captureEmitter) Emit(stream uint8, _ time.Time, data []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stream = stream
	c.data = append(c.data[:0], data...)
	c.count++
	return true
}
func (c *captureEmitter) Status(stream uint8) StreamStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats[stream]
}

func TestPeerProducerFormatsBoundedTypedFields(t *testing.T) {
	output := &captureEmitter{}
	s := New(true, output, 20, 21)
	if !s.Emit(Connection, Fields{Direction: "out bound", Action: "dial_failed", Peer: "N0PEER-1", Endpoint: "peer.example:7300", Reason: "connection_refused"}) {
		t.Fatal("refused")
	}
	if output.stream != 20 || string(output.data) != "event=peer_connection direction=out_bound action=dial_failed peer=N0PEER-1 endpoint=peer.example:7300 reason=connection_refused" {
		t.Fatalf("unexpected connection: %q", output.data)
	}
	large := strings.Repeat("x\n", 10000)
	fields := Fields{Action: large, Peer: large, Endpoint: large, Reason: large, Detail: large, Direction: large, DX: large, DE: large, Count: math.MinInt64, Limit: math.MaxInt64}
	if !s.Emit(Diagnostic, fields) || len(output.data) > asynclog.MaxRecordBytes || bytes.ContainsAny(output.data, "\r\n") {
		t.Fatalf("unbounded or multiline event: %q", output.data)
	}
	if !strings.Contains(string(output.data), "count=-9223372036854775808 limit=9223372036854775807") {
		t.Fatal("numeric fields changed")
	}
	if allocations := testing.AllocsPerRun(100, func() { s.Emit(Diagnostic, fields) }); allocations != 0 {
		t.Fatalf("fixed producer formatting allocated %.2f times", allocations)
	}
}

func TestPeerProducerDisabledOverlongAndStop(t *testing.T) {
	output := &captureEmitter{}
	s := New(false, output, 22, 23)
	if s.Emit(Connection, Fields{Action: "ignored"}) {
		t.Fatal("disabled daily accepted")
	}
	if !s.Emit(Overlong, Fields{Detail: "sample"}) || output.stream != 23 || string(output.data) != "event=peer_overlong detail=sample" {
		t.Fatal("overlong should remain enabled")
	}
	s.Stop()
	if s.Emit(Overlong, Fields{Detail: "late"}) {
		t.Fatal("stopped producer admitted")
	}
	if got := s.Snapshot(); got.DisabledEvents != 1 || !got.Degraded {
		t.Fatalf("wrong stopped status: %+v", got)
	}
}

func TestPeerProducerConcurrentStop(t *testing.T) {
	output := &captureEmitter{}
	s := New(true, output, 24, 25)
	var wg sync.WaitGroup
	var accepted, refused atomic.Uint64
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				if s.Emit(Diagnostic, Fields{Action: "parallel"}) {
					accepted.Add(1)
				} else {
					refused.Add(1)
				}
			}
		}()
	}
	s.Stop()
	wg.Wait()
	output.mu.Lock()
	written := output.count
	output.mu.Unlock()
	if uint64(written) != accepted.Load() || accepted.Load()+refused.Load() != 16000 {
		t.Fatalf("wrong admission accounting: delivered=%d accepted=%d refused=%d", written, accepted.Load(), refused.Load())
	}
}

func TestPeerProducerStatusDoesNotTreatOldWritesAsCurrent(t *testing.T) {
	output := &captureEmitter{}
	output.stats[26] = StreamStatus{Generation: 17, Written: 2}
	output.stats[27] = StreamStatus{Generation: 1}
	s := New(true, output, 26, 27)
	if got := s.Snapshot(); got.Generation != 17 || got.CurrentWritten != 2 {
		t.Fatalf("current owner status: %+v", got)
	}
	output.stats[26] = StreamStatus{Generation: 18}
	if got := s.Snapshot(); got.Generation != 18 || got.CurrentWritten != 0 {
		t.Fatalf("new owner inherited writes: %+v", got)
	}
	output.stats[27].Written = 3
	if got := s.Snapshot(); got.CurrentWritten != 0 {
		t.Fatalf("overlong writes falsely proved daily owner activity: %+v", got)
	}
	bad := New(true, output, asynclog.MaxStreams, 27)
	if bad.Emit(Diagnostic, Fields{}) || !bad.Snapshot().Degraded {
		t.Fatal("invalid streams did not fail closed")
	}
}

func TestPeerProducerCombinesCurrentStreamStatus(t *testing.T) {
	output := &captureEmitter{}
	output.stats[20] = StreamStatus{Generation: 10, Queued: 2, Written: 3, Dropped: 4, Suppressed: 5, ChargedBytes: 4 << 20}
	output.stats[21] = StreamStatus{Generation: 11, Queued: 7, Written: 11, Dropped: 13, Suppressed: 17, ChargedBytes: 4 << 20}
	s := New(true, output, 20, 21)
	got := s.Snapshot()
	if got.Generation != 10 || got.CurrentWritten != 3 || got.Queued != 9 || got.Written != 14 || got.Dropped != 17 || got.Suppressed != 22 || got.ChargedBytes != 4<<20 || got.Degraded {
		t.Fatalf("combined status: %+v", got)
	}
	output.stats[20].Dropped = math.MaxUint64
	if got := s.Snapshot(); got.Dropped != math.MaxUint64 {
		t.Fatalf("dropped counter overflowed: %+v", got)
	}
}

func TestDisabledPeerLogDoesNotDegradeHealthyOverlongLog(t *testing.T) {
	output := &captureEmitter{}
	output.stats[20] = StreamStatus{Closed: true, Degraded: true}
	output.stats[21] = StreamStatus{Generation: 1}
	got := New(false, output, 20, 21).Snapshot()
	if !got.Disabled || got.Degraded {
		t.Fatalf("disabled peer status: %+v", got)
	}
}

func TestPeerProducerStaticScratchReservation(t *testing.T) {
	// One active manager owns the Service plus its diagnostic pointer, lock,
	// and compact retirement metadata; all share the same static peer page.
	size := unsafe.Sizeof(formatBuffer) + unsafe.Sizeof(Service{}) + unsafe.Sizeof(Retired{}) + unsafe.Sizeof(sync.RWMutex{}) + unsafe.Sizeof((*Service)(nil))
	if size > 8192 {
		t.Fatalf("peer producer and manager controls exceed their 8192-byte reservation: %d", size)
	}
}

func TestRetiredServiceRejectsStaleDisabledCalls(t *testing.T) {
	s := New(false, nil, 20, 21)
	if s.Emit(Diagnostic, Fields{}) {
		t.Fatal("disabled record admitted")
	}
	retired := s.Retire()
	if s.Emit(Diagnostic, Fields{}) {
		t.Fatal("retired record admitted")
	}
	got := retired.Snapshot()
	if !got.Disabled || !got.Degraded || got.DisabledEvents != 1 || got.DeduperKeys != -1 {
		t.Fatalf("retired state: %+v", got)
	}
	if unsafe.Sizeof(retired) > 16 {
		t.Fatalf("retired metadata too large: %d", unsafe.Sizeof(retired))
	}
}

func TestPeerProducerStopClearsEmitterAndRejectsSuccessor(t *testing.T) {
	old := &captureEmitter{}
	old.stats[20].Generation = 17
	s := New(true, old, 20, 21)
	if !s.Emit(Connection, Fields{Action: "before_stop"}) {
		t.Fatal("initial record refused")
	}
	s.Stop()
	s.Stop()
	if s.emitter != nil {
		t.Fatal("stopped service still retains emitter")
	}
	old.stats[20].Generation = 18 // An emitter may later route to a successor generation.
	if s.Emit(Connection, Fields{Action: "after_stop"}) {
		t.Fatal("stopped service emitted through successor")
	}
	if old.count != 1 {
		t.Fatalf("emitter was called %d times", old.count)
	}
	status := s.Snapshot()
	if !status.Degraded || status.Generation != 0 || status.CurrentWritten != 0 {
		t.Fatalf("stopped service reports active generation: %+v", status)
	}
}

type borrowingEmitter struct {
	entered, release chan struct{}
	statusCalls      atomic.Int32
	emitCalls        atomic.Int32
}

func (e *borrowingEmitter) Emit(uint8, time.Time, []byte) bool { e.emitCalls.Add(1); return true }
func (e *borrowingEmitter) Status(uint8) StreamStatus {
	if e.statusCalls.Add(1) == 1 {
		close(e.entered)
		<-e.release
	}
	return StreamStatus{Generation: 1}
}

func TestPeerProducerStopWaitsForStatusBorrow(t *testing.T) {
	e := &borrowingEmitter{entered: make(chan struct{}), release: make(chan struct{})}
	s := New(true, e, 22, 23)
	var release sync.Once
	defer release.Do(func() { close(e.release) })
	snapshotDone := make(chan struct{})
	go func() { _ = s.Snapshot(); close(snapshotDone) }()
	select {
	case <-e.entered:
	case <-time.After(time.Second):
		t.Fatal("Status was not called")
	}
	stopDone := make(chan struct{})
	go func() { s.Stop(); close(stopDone) }()
	select {
	case <-stopDone:
		t.Fatal("Stop retired an in-flight Status borrow")
	case <-time.After(20 * time.Millisecond):
	}
	release.Do(func() { close(e.release) })
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not complete")
	}
	<-snapshotDone
	if s.emitter != nil {
		t.Fatal("Stop retained the emitter")
	}
	before := e.statusCalls.Load()
	_ = s.Snapshot()
	if got := e.statusCalls.Load(); got != before {
		t.Fatalf("stopped snapshot called old emitter: %d -> %d", before, got)
	}
	if s.Emit(Overlong, Fields{}) || e.emitCalls.Load() != 0 {
		t.Fatal("stopped producer called old emitter")
	}
}
