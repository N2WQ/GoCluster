package telnet

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"dxcluster/filter"
)

func TestCommandPacingRetirementRegistryState(t *testing.T) {
	register := func(t *testing.T, s *Server, c *Client) *Client {
		t.Helper()
		old, total, err := s.registerClientOwned(c, loginReservation{}, time.Time{})
		if err != nil || total != 1 {
			t.Fatalf("register %s: old=%p total=%d error=%v", c.callsign, old, total, err)
		}
		return old
	}
	assertSignal := func(t *testing.T, ch <-chan struct{}, closed bool) {
		t.Helper()
		select {
		case <-ch:
			if !closed {
				t.Fatal("current session's retirement signal closed")
			}
		default:
			if closed {
				t.Fatal("displaced session's retirement signal remained open")
			}
		}
	}

	t.Run("zero valued signal", func(t *testing.T) {
		s := &Server{}
		first := &Client{callsign: "W1ABC-1"}
		second := &Client{callsign: first.callsign}
		if old := register(t, s, first); old != nil {
			t.Fatalf("unexpected first owner: %p", old)
		}
		if old := register(t, s, second); old != first {
			t.Fatalf("displaced owner=%p, want %p", old, first)
		}
		if s.clients[first.callsign] != second {
			t.Fatal("replacement with nil retirement signals lost ownership")
		}
	})

	t.Run("same pointer and repeated replacement", func(t *testing.T) {
		s := presetTestServer(t)
		a := configurationTestClient(s, "W1ABC-1")
		b := configurationTestClient(s, a.callsign)
		c := configurationTestClient(s, a.callsign)
		for _, client := range []*Client{a, b, c} {
			client.retired = make(chan struct{})
		}
		if old := register(t, s, a); old != nil {
			t.Fatalf("unexpected first owner: %p", old)
		}
		if old := register(t, s, a); old != a {
			t.Fatalf("same pointer returned old=%p, want %p", old, a)
		}
		assertSignal(t, a.retired, false)
		if err := a.saveFilter(); err != nil {
			t.Fatalf("save original owner: %v", err)
		}
		path := filepath.Join(filter.UserDataDir, "W1ABC-1.yaml")
		original, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if old := register(t, s, b); old != a {
			t.Fatalf("first displacement old=%p, want %p", old, a)
		}
		assertSignal(t, a.retired, true)
		assertSignal(t, b.retired, false)
		if old := register(t, s, c); old != b {
			t.Fatalf("second displacement old=%p, want %p", old, b)
		}
		assertSignal(t, a.retired, true)
		assertSignal(t, b.retired, true)
		assertSignal(t, c.retired, false)
		c.configuredSettings = filter.SettingsConfiguration{Dialect: "go", NoiseClass: "URBAN", DedupePolicy: "FAST"}
		c.configurationInitialized = true
		if err := c.saveFilter(); err != nil {
			t.Fatalf("save replacement profile: %v", err)
		}
		before, err := os.ReadFile(path)
		if err != nil || bytes.Equal(before, original) || !bytes.Contains(before, []byte("noise_class: URBAN")) {
			t.Fatalf("replacement profile was not independently persisted: %v", err)
		}
		for _, retired := range []*Client{a, b} {
			retired.configuredSettings = filter.SettingsConfiguration{Dialect: "go", NoiseClass: "INDUSTRIAL", DedupePolicy: "FAST"}
			retired.configurationInitialized = true
			if err := retired.saveFilter(); !errors.Is(err, errRetiredConfiguration) {
				t.Fatalf("retired owner saved profile: %v", err)
			}
			s.unregisterClient(retired)
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatalf("retired teardown altered the saved profile: %v", err)
		}
		if s.clients[a.callsign] != c || len(s.clients) != 1 {
			t.Fatal("retired teardown changed replacement ownership")
		}
	})

	t.Run("already closed signal", func(t *testing.T) {
		s := &Server{}
		first := &Client{callsign: "W1ABC-1", retired: make(chan struct{})}
		second := &Client{callsign: first.callsign}
		register(t, s, first)
		close(first.retired)
		if old := register(t, s, second); old != first || s.clients[first.callsign] != second {
			t.Fatal("already-retired signal prevented replacement")
		}
	})
}

func TestCommandPacingRetirementCountsOneWait(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := &Server{shutdown: make(chan struct{})}
	c := &Client{done: make(chan struct{}), retired: make(chan struct{})}
	p := newCommandPacer(1, 1, start)
	if delay := p.take(start); delay != 0 {
		t.Fatalf("initial burst delay=%v", delay)
	}
	started, retiring := make(chan struct{}), make(chan struct{})
	s.testCommandPacingNow = func() time.Time { return start }
	s.testCommandPacingTimerStarted = func(*Client) { close(started) }
	s.testCommandPacingRetired = func(*Client) { close(retiring) }
	result := make(chan bool, 1)
	var doneOnce sync.Once
	t.Cleanup(func() { doneOnce.Do(func() { close(c.done) }) })
	go func() { result <- s.awaitCommandAllowance(c, p) }()
	waitConfigurationTest(t, started)
	close(c.retired)
	waitConfigurationTest(t, retiring)
	if got := s.CommandPacingWaits(); got != 1 {
		t.Fatalf("retired attempt counted %d waits, want 1", got)
	}
	select {
	case <-result:
		t.Fatal("retired reader returned before the writer closed")
	default:
	}
	doneOnce.Do(func() { close(c.done) })
	select {
	case allowed := <-result:
		if allowed {
			t.Fatal("retired attempt received a command allowance")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("retired reader did not exit after writer closure")
	}
	if got := s.CommandPacingWaits(); got != 1 {
		t.Fatalf("retired attempt counted %d waits after close, want 1", got)
	}
}
