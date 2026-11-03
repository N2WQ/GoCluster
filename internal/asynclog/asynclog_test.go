package asynclog

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type delivered struct {
	at    time.Time
	frame uint8
	data  []byte
}

type testSink struct {
	entered  chan struct{}
	gate     <-chan struct{}
	output   chan delivered
	closed   chan struct{}
	closeErr error
	once     sync.Once
}

func (s *testSink) Write(at time.Time, frame uint8, data []byte) WriteResult {
	if s.entered != nil {
		s.once.Do(func() { close(s.entered) })
	}
	if s.gate != nil {
		<-s.gate
	}
	if s.output != nil {
		s.output <- delivered{at: at, frame: frame, data: bytes.Clone(data)}
	}
	return Written
}

func (s *testSink) Close() error {
	if s.closed != nil {
		close(s.closed)
	}
	return s.closeErr
}

func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
	}
}

func assertOpen(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("completion closed while operation remains blocked")
	default:
	}
}

func TestFactoryStallBoundsQueueAndStop(t *testing.T) {
	entered, gate := make(chan struct{}), make(chan struct{})
	sink := &testSink{closed: make(chan struct{})}
	h, err := Start(0, func() Sink { close(entered); <-gate; return sink })
	if err != nil {
		t.Fatal(err)
	}
	await(t, entered)
	if !h.Emit(time.Now(), 1, make([]byte, MaxRecordBytes+1)) {
		t.Fatal("two-record message refused")
	}
	for i := 0; i < QueueSize-2; i++ {
		if !h.Emit(time.Now(), 0, []byte("x")) {
			t.Fatalf("record %d refused", i)
		}
	}
	if h.Emit(time.Now(), 0, nil) {
		t.Fatal("full queue accepted a record")
	}
	if got := h.Stats().Queued; got != QueueSize-1 {
		t.Fatalf("queued groups = %d", got)
	}
	h.Stop()
	assertOpen(t, h.Done())
	if _, err = Start(0, func() Sink { return sink }); !errors.Is(err, ErrOwned) {
		t.Fatalf("replacement during blocked factory = %v", err)
	}
	close(gate)
	await(t, h.Done())
	await(t, sink.closed)
	if got := h.Stats().Dropped; got != 1 {
		t.Fatalf("drops = %d", got)
	}
	if got := h.Stats().Written; got != QueueSize-1 {
		t.Fatalf("drained messages = %d", got)
	}
}

func TestBlockedWriteIsolatedAndGenerationSafe(t *testing.T) {
	gate, entered := make(chan struct{}), make(chan struct{})
	sink := &testSink{gate: gate, entered: entered, closed: make(chan struct{})}
	h, err := Start(1, func() Sink { return sink })
	if err != nil {
		t.Fatal(err)
	}
	if !h.Emit(time.Now(), 0, []byte("blocked")) {
		t.Fatal("admission failed")
	}
	await(t, entered)
	h.Stop()
	assertOpen(t, h.Done())
	if _, err := Start(1, func() Sink { return sink }); !errors.Is(err, ErrOwned) {
		t.Fatalf("replacement during blocked write = %v", err)
	}
	other := &testSink{output: make(chan delivered, 1)}
	otherH, err := Start(2, func() Sink { return other })
	if err != nil {
		t.Fatal(err)
	}
	if !otherH.Emit(time.Now(), 0, []byte("continues")) {
		t.Fatal("other stream refused")
	}
	select {
	case got := <-other.output:
		if string(got.data) != "continues" {
			t.Fatalf("other stream = %q", got.data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("other stream stalled")
	}
	otherH.Stop()
	await(t, otherH.Done())
	close(gate)
	await(t, h.Done())

	replacement := &testSink{output: make(chan delivered, 1)}
	next, err := Start(1, func() Sink { return replacement })
	if err != nil {
		t.Fatal(err)
	}
	if h.Emit(time.Now(), 0, []byte("stale")) {
		t.Fatal("stale handle emitted")
	}
	h.Stop()
	if !next.Emit(time.Now(), 0, []byte("fresh")) {
		t.Fatal("replacement refused")
	}
	select {
	case got := <-replacement.output:
		if string(got.data) != "fresh" {
			t.Fatalf("replacement = %q", got.data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("replacement stalled")
	}
	next.Stop()
	await(t, next.Done())
}

func TestGroupedMessagesAreCopiedAndAtomic(t *testing.T) {
	entered, gate := make(chan struct{}), make(chan struct{})
	sink := &testSink{output: make(chan delivered, 2)}
	h, err := Start(3, func() Sink { close(entered); <-gate; return sink })
	if err != nil {
		t.Fatal(err)
	}
	await(t, entered)
	at := time.Date(2026, 11, 2, 15, 5, 1, 0, time.UTC)
	message := bytes.Repeat([]byte("g"), 3073)
	if !h.Emit(at, 7, message) {
		t.Fatal("group refused")
	}
	clear(message)
	if !h.Emit(at, 8, nil) {
		t.Fatal("empty message refused")
	}
	if h.Emit(at, 0, make([]byte, MaxMessageBytes+1)) {
		t.Fatal("oversize accepted")
	}
	close(gate)
	select {
	case got := <-sink.output:
		if !got.at.Equal(at) || got.frame != 7 || !bytes.Equal(got.data, bytes.Repeat([]byte("g"), 3073)) {
			t.Fatalf("corrupt grouped message: time=%v frame=%d length=%d", got.at, got.frame, len(got.data))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("group not delivered")
	}
	select {
	case got := <-sink.output:
		if got.frame != 8 || len(got.data) != 0 {
			t.Fatalf("empty message = %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("empty message not delivered")
	}
	h.Stop()
	await(t, h.Done())
}

func TestStopImmediatelyAfterStartDrainsAdmittedMessages(t *testing.T) {
	entered, gate := make(chan struct{}), make(chan struct{})
	sink := &testSink{output: make(chan delivered, 2)}
	h, err := Start(5, func() Sink { close(entered); <-gate; return sink })
	if err != nil {
		t.Fatal(err)
	}
	await(t, entered)
	if !h.Emit(time.Now(), 0, []byte("first")) || !h.Emit(time.Now(), 0, []byte("second")) {
		t.Fatal("message refused")
	}
	h.Stop()
	if h.Emit(time.Now(), 0, []byte("late")) {
		t.Fatal("stop allowed another admission")
	}
	close(gate)
	await(t, h.Done())
	for _, want := range []string{"first", "second"} {
		select {
		case got := <-sink.output:
			if string(got.data) != want {
				t.Fatalf("drained message = %q, want %q", got.data, want)
			}
		default:
			t.Fatalf("missing drained message %q", want)
		}
	}
}

type gatedCloseSink struct {
	written chan struct{}
	entered chan struct{}
	gate    <-chan struct{}
}

func (s *gatedCloseSink) Write(time.Time, uint8, []byte) WriteResult {
	close(s.written)
	return Written
}
func (s *gatedCloseSink) Close() error {
	close(s.entered)
	<-s.gate
	return nil
}

func TestBlockedCloseRetainsStreamUntilReturn(t *testing.T) {
	gate := make(chan struct{})
	sink := &gatedCloseSink{written: make(chan struct{}), entered: make(chan struct{}), gate: gate}
	h, err := Start(6, func() Sink { return sink })
	if err != nil {
		t.Fatal(err)
	}
	if !h.Emit(time.Now(), 0, []byte("x")) {
		t.Fatal("message refused")
	}
	await(t, sink.written)
	h.Stop()
	await(t, sink.entered)
	assertOpen(t, h.Done())
	for i := 0; i < 100; i++ {
		if _, err := Start(6, func() Sink { return sink }); !errors.Is(err, ErrOwned) {
			t.Fatalf("blocked close attempt %d = %v", i, err)
		}
	}
	close(gate)
	await(t, h.Done())
	next, err := Start(6, func() Sink { return &countingSink{} })
	if err != nil {
		t.Fatal(err)
	}
	next.Stop()
	await(t, next.Done())
}

func TestUncertainCleanupPoisonsSlot(t *testing.T) {
	sink := &testSink{output: make(chan delivered, 1), closeErr: errors.New("uncertain close")}
	h, err := Start(7, func() Sink { return sink })
	if err != nil {
		t.Fatal(err)
	}
	if !h.Emit(time.Now(), 0, []byte("x")) {
		t.Fatal("message refused")
	}
	select {
	case <-sink.output:
	case <-time.After(3 * time.Second):
		t.Fatal("message not delivered")
	}
	h.Stop()
	await(t, h.Done())
	if !h.Stats().CleanupFailed {
		t.Fatal("uncertain cleanup was not retained")
	}
	if _, err := Start(7, func() Sink { return sink }); !errors.Is(err, ErrOwned) {
		t.Fatalf("replacement after uncertain cleanup = %v", err)
	}
	// This test sink has no real external resources. Reset its slot so repeated
	// test runs can verify the same behavior within a single process.
	s := &slots[7]
	s.mu.Lock()
	s.retained = nil
	s.poisoned = false
	s.mu.Unlock()
}

func TestFactoryPanicPoisonsSlot(t *testing.T) {
	h, err := Start(8, func() Sink { panic("factory failed") })
	if err != nil {
		t.Fatal(err)
	}
	await(t, h.Done())
	if !h.Stats().CleanupFailed {
		t.Fatal("factory panic allowed an unverified replacement")
	}
	if _, err := Start(8, func() Sink { return &countingSink{} }); !errors.Is(err, ErrOwned) {
		t.Fatalf("replacement after panic = %v", err)
	}
	s := &slots[8]
	s.mu.Lock()
	s.poisoned = false
	s.mu.Unlock()
}

type countingSink struct{ writes atomic.Uint64 }

func (s *countingSink) Write(time.Time, uint8, []byte) WriteResult { s.writes.Add(1); return Written }
func (*countingSink) Close() error                                 { return nil }

func TestConcurrentEmitAndStop(t *testing.T) {
	sink := &countingSink{}
	h, err := Start(4, func() Sink { return sink })
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				h.Emit(time.Now(), 0, []byte("record"))
			}
		}()
	}
	h.Stop()
	wg.Wait()
	await(t, h.Done())
	if h.Emit(time.Now(), 0, []byte("late")) {
		t.Fatal("stopped handle emitted")
	}
}

func TestStaticSlotBudget(t *testing.T) {
	if StaticBackingBytes > 64<<20 {
		t.Fatalf("slot backing = %d", StaticBackingBytes)
	}
	if 2*SlotBackingBytes > 4<<20 {
		t.Fatalf("two peer slots = %d", 2*SlotBackingBytes)
	}
}

func TestEmitStringBoundsBeforeCopying(t *testing.T) {
	out := make(chan delivered, 1)
	ready := make(chan struct{})
	h, err := Start(18, func() Sink { close(ready); return &testSink{output: out} })
	if err != nil {
		t.Fatal(err)
	}
	await(t, ready)
	large := strings.Repeat("x", MaxMessageBytes+1)
	if allocs := testing.AllocsPerRun(20, func() {
		if h.EmitString(time.Time{}, 0, large) {
			t.Error("oversized message admitted")
		}
	}); allocs != 0 {
		t.Fatalf("oversized string admission allocated %v times", allocs)
	}
	want := strings.Repeat("a", 3000)
	if !h.EmitString(time.Now(), 0, want) {
		t.Fatal("valid grouped string was refused")
	}
	select {
	case got := <-out:
		if string(got.data) != want {
			t.Fatal("grouped string changed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("grouped string was not delivered")
	}
	h.Stop()
	await(t, h.Done())
}

func TestSingleRecordOwnerRejectsLargerMessages(t *testing.T) {
	out := make(chan delivered, 1)
	h, err := StartSingle(19, func() Sink { return &testSink{output: out} })
	if err != nil {
		t.Fatal(err)
	}
	if h.EmitString(time.Now(), 0, strings.Repeat("x", MaxRecordBytes+1)) {
		t.Fatal("single-record destination admitted an oversized message")
	}
	if !h.EmitString(time.Now(), 0, strings.Repeat("y", MaxRecordBytes)) {
		t.Fatal("single-record destination rejected its maximum message")
	}
	select {
	case got := <-out:
		if len(got.data) != MaxRecordBytes {
			t.Fatalf("got %d bytes", len(got.data))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("maximum message not delivered")
	}
	h.Stop()
	await(t, h.Done())
}

type outcomeSink struct{ next int }

func (s *outcomeSink) Write(time.Time, uint8, []byte) WriteResult {
	s.next++
	switch s.next {
	case 1:
		return NotWritten
	case 2:
		return Suppressed
	default:
		return Written
	}
}
func (*outcomeSink) Close() error { return nil }

func TestWriteStatusDistinguishesFailureAndSuppression(t *testing.T) {
	h, err := StartSingle(20, func() Sink { return &outcomeSink{} })
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if !h.EmitString(time.Now(), 0, "record") {
			t.Fatal("record not admitted")
		}
	}
	h.Stop()
	await(t, h.Done())
	got := h.Stats()
	if got.Written != 1 || got.Dropped != 1 || got.Suppressed != 1 {
		t.Fatalf("delivery outcomes = %+v", got)
	}
}

func TestSingleDestinationDoesNotNeedDashboardGroupBuffers(t *testing.T) {
	var handles []Handle
	for i := 0; i < MaxGrouped; i++ {
		h, err := Start(uint8(21+i), func() Sink { return &countingSink{} })
		if err != nil {
			t.Fatal(err)
		}
		handles = append(handles, h)
	}
	if _, err := Start(27, func() Sink { return &countingSink{} }); !errors.Is(err, ErrGrouped) {
		t.Fatalf("seventh grouped destination: %v", err)
	}
	single, err := StartSingle(27, func() Sink { return &countingSink{} })
	if err != nil {
		t.Fatalf("single destination refused: %v", err)
	}
	if !single.EmitString(time.Now(), 0, "record") {
		t.Fatal("record refused")
	}
	single.Stop()
	await(t, single.Done())
	for _, h := range handles {
		h.Stop()
	}
	for _, h := range handles {
		await(t, h.Done())
	}
}
