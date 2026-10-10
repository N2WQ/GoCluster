package peer

import (
	"sync"
	"testing"
	"time"
)

func TestConnectionObserverPreservesProducerTimestamp(t *testing.T) {
	m := &Manager{}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	got := make(chan time.Time, 1)
	observeTimedConnectionEvents(t, m, func(event ConnectionEvent, at time.Time) {
		switch event.Action {
		case "barrier":
			close(entered)
			<-release
		case "clock_witness":
			got <- at
		}
	})
	// Unblock the held callback before the observer's cleanup tries to join it.
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	m.reportConnection(ConnectionEvent{Action: "barrier"})
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("observer barrier was not entered")
	}
	before := time.Now().UTC()
	m.reportConnection(ConnectionEvent{Action: "clock_witness"})
	after := time.Now().UTC()
	// Delay delivery of an already emitted event. An observer-time implementation
	// will return a timestamp after resumedAt and fail the literal emit interval.
	timer := time.NewTimer(20 * time.Millisecond)
	defer timer.Stop()
	<-timer.C
	resumedAt := time.Now().UTC()
	releaseOnce.Do(func() { close(release) })
	select {
	case at := <-got:
		if at.Before(before) || at.After(after) || !at.Before(resumedAt) {
			t.Fatalf("producer clock replaced by delivery clock: %s outside [%s,%s], resumed %s", at, before, after, resumedAt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("emitted clock witness was not delivered")
	}
}
