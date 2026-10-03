package peerdiag

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func TestV15DiagnosticReserveBeforeEncoding(t *testing.T) {
	m := NewMailbox(true)
	for range QueueSize {
		if !m.Emit(Diagnostic, Fields{Action: "queued"}) {
			t.Fatal("early refusal")
		}
	}
	large := strings.Repeat("hostile", 1<<20)
	allocations := testing.AllocsPerRun(100, func() {
		if m.Emit(Diagnostic, Fields{Detail: large}) {
			t.Fatal("overflow accepted")
		}
	})
	if allocations != 0 {
		t.Fatalf("refused event allocated %f times", allocations)
	}
	if stats := m.Snapshot(); stats.Queued != QueueSize || stats.Dropped != 101 {
		t.Fatalf("overflow stats=%+v", stats)
	}
}

func TestV15DiagnosticEventOwnsFixedBacking(t *testing.T) {
	if unsafe.Sizeof(Event{}) != RecordBytes {
		t.Fatal("event exceeds 2 KiB backing")
	}
	m := NewMailbox(true)
	input := strings.Repeat("abcdef\n", 10000)
	m.Emit(Diagnostic, Fields{Action: "failure", Detail: input})
	event, ok := m.Next()
	if !ok || event.Length > 1024 || bytes.Contains(event.Data[:event.Length], []byte{'\n'}) {
		t.Fatal("unbounded or multiline event")
	}
	var encoded [RecordBytes]byte
	event.encode(&encoded)
	decoded, valid := decodeEvent(&encoded)
	if !valid || decoded != event {
		t.Fatal("fixed record round trip failed")
	}
}

func TestV15DiagnosticAllFieldsFitRecord(t *testing.T) {
	m := NewMailbox(true)
	large := strings.Repeat("x", 4096)
	fields := Fields{Action: large, Peer: large, Endpoint: large, Reason: large, Detail: large, Direction: large, DX: large, DE: large, Count: math.MinInt64, Limit: math.MaxInt64}
	if allocations := testing.AllocsPerRun(100, func() {
		if !m.Emit(Diagnostic, fields) {
			t.Fatal("empty mailbox refused event")
		}
		event, ok := m.Next()
		if !ok || event.Length > uint16(len(event.Data)) {
			t.Fatal("all-fields event exceeded fixed backing")
		}
	}); allocations != 0 {
		t.Fatalf("admitted max-fields event allocated %f times", allocations)
	}
}

func TestV15DiagnosticConcurrentCloseAccounting(t *testing.T) {
	m := NewMailbox(true)
	var workers sync.WaitGroup
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 1000 {
				m.Emit(Diagnostic, Fields{Action: "parallel"})
			}
		}()
	}
	m.closeAdmission()
	workers.Wait()
	if stats := m.Snapshot(); stats.Queued != 0 || stats.Dropped != 16000 {
		t.Fatalf("closed admission accounting=%+v", stats)
	}
	if m.Emit(Diagnostic, Fields{}) {
		t.Fatal("admitted after close")
	}
}

func TestV15DiagnosticDedupeContract(t *testing.T) {
	s := helperSink{options: Options{DedupeWindow: time.Minute}}
	now := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	line := []byte("event=peer_connection action=connected")
	if got, ok := s.deduplicate(line, now); !ok || !bytes.Equal(got, line) {
		t.Fatal("first record suppressed")
	}
	if _, ok := s.deduplicate(line, now.Add(59*time.Second)); ok {
		t.Fatal("inside-window duplicate emitted")
	}
	if got, ok := s.deduplicate(line, now.Add(time.Minute)); !ok || string(got) != "event=peer_connection action=connected suppressed=1 window=1m0s" {
		t.Fatalf("exact boundary=%q %v", got, ok)
	}
	for i := 1; i <= 512; i++ {
		s.deduplicate([]byte("unique="+strconv.Itoa(i)), now.Add(time.Duration(i+100)*time.Second))
	}
	if s.used != 512 {
		t.Fatal("deduper exceeded fixed slot bound")
	}
	if _, ok := s.deduplicate(line, now.Add(700*time.Second)); !ok {
		t.Fatal("oldest identity was not evicted")
	}
	s.options.DedupeWindow = 0
	if _, ok := s.deduplicate(line, now); !ok {
		t.Fatal("explicit zero did not disable dedupe")
	}
}

func TestV15DiagnosticBackingEnvelope(t *testing.T) {
	// These are the fixed allocations, not a proof of stdlib/OS/argv overhead.
	parentFixed := unsafe.Sizeof(Service{}) + unsafe.Sizeof(Mailbox{}) + QueueSize*RecordBytes + unsafe.Sizeof(helperProcess{}) + 2*RecordBytes + ackBytes
	helperFixed := unsafe.Sizeof(helperSink{}) + 2*RecordBytes + ackBytes + optionsHeaderBytes
	if parentFixed > parentReservation || helperFixed > helperReservation {
		t.Fatalf("fixed backing parent=%d helper=%d", parentFixed, helperFixed)
	}
	t.Logf("fixed parent=%d helper=%d; remaining host/library/native allowance parent=%d helper=%d", parentFixed, helperFixed, parentReservation-parentFixed, helperReservation-helperFixed)
	if parentReservation+helperReservation != BackingLimit {
		t.Fatal("aggregate reservation changed")
	}
}

func FuzzV15DiagnosticIPC(f *testing.F) {
	f.Add([]byte{1, 2, 3})
	m := NewMailbox(true)
	m.Emit(Diagnostic, Fields{Action: "seed"})
	event, _ := m.Next()
	var seed [RecordBytes]byte
	event.encode(&seed)
	f.Add(seed[:])
	f.Fuzz(func(t *testing.T, input []byte) {
		var wire [RecordBytes]byte
		copy(wire[:], input)
		event, ok := decodeEvent(&wire)
		if ok {
			if int(event.Length) > len(event.Data) || event.Sequence == 0 || event.Kind < Diagnostic || event.Kind > LossSummary {
				t.Fatal("accepted invalid header")
			}
			var second [RecordBytes]byte
			event.encode(&second)
			roundTrip, valid := decodeEvent(&second)
			if !valid || roundTrip != event {
				t.Fatal("unstable accepted event")
			}
		}
	})
}
