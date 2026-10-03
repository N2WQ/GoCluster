package peer

import (
	"strings"
	"testing"
	"time"

	"dxcluster/internal/peerdiag"
)

// Tests consume the concrete mailbox independently. No callback or test hook is
// installed in any production actor; the polling observer owns its own join.
func observeConnectionEvents(t *testing.T, m *Manager, observe func(ConnectionEvent)) {
	t.Helper()
	m.diagnostics = peerdiag.New(peerdiag.Options{Enabled: true})
	t.Cleanup(m.diagnostics.Stop)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
			for {
				event, ok := m.diagnostics.Next()
				if !ok {
					break
				}
				if event.Kind != peerdiag.Connection {
					continue
				}
				var value ConnectionEvent
				for _, field := range strings.Fields(string(event.Data[:event.Length])) {
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
				observe(value)
			}
		}
	}()
	t.Cleanup(func() { close(stop); <-done })
}
