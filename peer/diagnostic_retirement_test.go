package peer

import (
	"sync"
	"testing"
	"time"

	"dxcluster/internal/peerdiag"
)

// This injected stall deliberately violates the production emitter's bounded
// in-memory contract; it exercises borrow ownership, not production latency.
type stalledDiagnosticEmitter struct {
	entered, release chan struct{}
	once             sync.Once
}

func (*stalledDiagnosticEmitter) Emit(uint8, time.Time, []byte) bool { return true }
func (e *stalledDiagnosticEmitter) Status(uint8) peerdiag.StreamStatus {
	e.once.Do(func() { close(e.entered); <-e.release })
	return peerdiag.StreamStatus{}
}

func TestDiagnosticRetirementWaitsForInFlightStatusBorrow(t *testing.T) {
	emitter := &stalledDiagnosticEmitter{entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(emitter.release) }) })
	service := peerdiag.New(true, emitter, 20, 21)
	m := &Manager{diagnostics: service}
	statusDone := make(chan struct{})
	go func() { _ = m.DiagnosticStats(); close(statusDone) }()
	select {
	case <-emitter.entered:
	case <-time.After(time.Second):
		t.Fatal("status did not enter")
	}
	stopDone := make(chan struct{})
	go func() { m.Stop(); close(stopDone) }()
	select {
	case <-stopDone:
		t.Fatal("stop released an in-flight borrow")
	case <-time.After(20 * time.Millisecond):
	}
	once.Do(func() { close(emitter.release) })
	select {
	case <-statusDone:
	case <-time.After(time.Second):
		t.Fatal("status did not complete")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("stop did not complete")
	}
	m.diagnosticsMu.RLock()
	retained := m.diagnostics
	m.diagnosticsMu.RUnlock()
	if retained != nil {
		t.Fatal("service remained attached after borrow returned")
	}
}

func TestDiagnosticRetirementJoinsBorrowAndDetachesService(t *testing.T) {
	service, _, _ := newDiagnosticTestService(t, false, false)
	if service.Emit(peerdiag.Diagnostic, peerdiag.Fields{}) {
		t.Fatal("disabled diagnostic admitted")
	}
	m := &Manager{diagnostics: service}
	before := m.DiagnosticStats()
	m.diagnosticsMu.RLock()
	borrowed := m.diagnostics
	done := make(chan struct{})
	go func() { m.Stop(); close(done) }()
	select {
	case <-done:
		t.Fatal("manager retired an active diagnostic borrow")
	case <-time.After(20 * time.Millisecond):
	}
	m.diagnosticsMu.RUnlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("manager failed to retire")
	}
	m.diagnosticsMu.RLock()
	retained := m.diagnostics
	m.diagnosticsMu.RUnlock()
	if retained != nil {
		t.Fatal("stopped manager retained logger service")
	}
	if borrowed.Emit(peerdiag.Diagnostic, peerdiag.Fields{}) {
		t.Fatal("stale service admitted record")
	}
	after := m.DiagnosticStats()
	if !after.Disabled || !after.Degraded || after.Generation != 0 || after.DeduperKeys != -1 || after.DisabledEvents != before.DisabledEvents {
		t.Fatalf("stopped diagnostic status: before=%+v after=%+v", before, after)
	}
}

func TestDiagnosticRetirementConcurrentEmitAndStatus(t *testing.T) {
	service, _, _ := newDiagnosticTestService(t, true, false)
	m := &Manager{diagnostics: service}
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 200 {
				m.reportDiagnostic("concurrent", "peer", "test")
				_ = m.DiagnosticStats()
			}
		}()
	}
	m.Stop()
	workers.Wait()
	m.diagnosticsMu.RLock()
	retained := m.diagnostics
	m.diagnosticsMu.RUnlock()
	if retained != nil {
		t.Fatal("stopped manager retained logger service")
	}
	if stats := m.DiagnosticStats(); !stats.Degraded || stats.Disabled || stats.Generation != 0 {
		t.Fatalf("stopped status %+v", stats)
	}
}

func TestManyRetainedStoppedManagersDoNotOwnServices(t *testing.T) {
	managers := make([]*Manager, 128)
	for i := range managers {
		managers[i] = &Manager{diagnostics: peerdiag.New(true, nil, 20, 21)}
		managers[i].Stop()
	}
	for i, m := range managers {
		m.diagnosticsMu.RLock()
		retained := m.diagnostics
		m.diagnosticsMu.RUnlock()
		if retained != nil {
			t.Fatalf("manager %d retained a stopped service", i)
		}
		if got := m.DiagnosticStats(); !got.Degraded || got.Disabled {
			t.Fatalf("manager %d status %+v", i, got)
		}
	}
}
