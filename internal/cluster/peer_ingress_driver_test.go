package cluster

import (
	"dxcluster/peer"
	"dxcluster/spot"
	"testing"
)

func testPeerSpotIngress(t testing.TB, input chan<- *spot.Spot) *peer.SpotIngress {
	t.Helper()
	q := peer.NewSpotIngress()
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
