package peer

import (
	"strings"
	"testing"
	"time"
)

// Tests observe records after admission through the common logger queue.
func observeConnectionEvents(t *testing.T, m *Manager, observe func(ConnectionEvent)) {
	t.Helper()
	observeTimedConnectionEvents(t, m, func(event ConnectionEvent, _ time.Time) { observe(event) })
}

// The timestamp belongs to the producer's immutable diagnostic record. Polling or
// callback delay must not become the clock for retry/phase measurements.
func observeTimedConnectionEvents(t *testing.T, m *Manager, observe func(ConnectionEvent, time.Time)) {
	t.Helper()
	service, events, _ := newDiagnosticTestService(t, true, false)
	m.diagnostics = service
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case event := <-events:
				if !strings.HasPrefix(event.line, "event=peer_connection ") {
					continue
				}
				var value ConnectionEvent
				for _, field := range strings.Fields(event.line) {
					key, text, ok := strings.Cut(field, "=")
					if !ok {
						continue
					}
					switch key {
					case "direction":
						value.Direction = text
					case "action":
						value.Action = text
					case "peer":
						value.Peer = text
					case "endpoint":
						value.Endpoint = text
					case "reason":
						value.Reason = text
					}
				}
				observe(value, event.at)
			}
		}
	}()
	t.Cleanup(func() { close(stop); <-done })
}
