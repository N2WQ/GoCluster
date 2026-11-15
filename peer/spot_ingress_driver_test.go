package peer

import (
	"dxcluster/spot"
	"testing"
)

func newTestSpotIngress(t testing.TB, input chan<- *spot.Spot) *SpotIngress {
	t.Helper()
	q := NewSpotIngress()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-q.Done():
				return
			case <-q.Ready():
				j := q.Take()
				if j == nil {
					continue
				}
				s := j.TakeSpot()
				select {
				case <-q.Done():
				default:
					select {
					case input <- s:
						j.Relay()
					default:
					}
				}
				j.Finish()
			}
		}
	}()
	t.Cleanup(func() { q.Close(); <-done })
	return q
}

// FIFO completion is explicit: HandleFrame itself acknowledges only enqueue.
func handleTestPeerFrame(m *Manager, f *Frame, source *session) {
	m.mu.Lock()
	if source != nil && source.identity == nil {
		source.identity = new(sourceIdentity)
	}
	if m.sessions != nil {
		for _, s := range m.sessions.All() {
			if s != nil && s.identity == nil {
				s.identity = new(sourceIdentity)
			}
		}
	}
	m.mu.Unlock()
	m.HandleFrame(f, source)
	waitTestPeerIngress(m)
}

func waitTestPeerIngress(m *Manager) {
	if m.ingress != nil {
		m.ingress.mu.Lock()
		for m.ingress.count != 0 {
			m.ingress.changed.Wait()
		}
		m.ingress.mu.Unlock()
	}
}
