//go:build qualification

package telnet

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestQualificationObserverOnlySuccessfulEnqueue(t *testing.T) {
	var events []QualificationEnqueue
	SetQualificationEnqueueObserver(func(event QualificationEnqueue) { events = append(events, event) })
	t.Cleanup(func() { SetQualificationEnqueueObserver(nil) })
	client := &Client{callsign: "dl1aaa", peerSessionID: 42, done: make(chan struct{}), spotChan: make(chan *spotEnvelope, 1)}
	s := spot.NewSpot("DL1AABB", "DL1AAA", 14020, "CW")
	s.Comment = "QID0000001"
	start := time.Now()
	client.enqueueSpot(&spotEnvelope{spot: s})
	client.enqueueSpot(&spotEnvelope{spot: s}) // full: no successful event
	close(client.done)
	<-client.spotChan
	client.enqueueSpot(&spotEnvelope{spot: s}) // closed: no event
	if len(events) != 1 || events[0].SessionID != 42 || events[0].Login != "dl1aaa" || events[0].Comment != s.Comment || events[0].DXCall != s.DXCall || events[0].ObservedAt.Before(start) {
		t.Fatalf("incorrect successful-admission observations: %+v", events)
	}
}

func TestQualificationObserverConcurrentReplacement(t *testing.T) {
	var seen atomic.Uint64
	callback := func(QualificationEnqueue) { seen.Add(1) }
	client := &Client{callsign: "DL1AAA", peerSessionID: 1}
	env := &spotEnvelope{spot: spot.NewSpot("DL1AABB", "DL1AAA", 14020, "CW")}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 1000 {
			SetQualificationEnqueueObserver(callback)
			SetQualificationEnqueueObserver(nil)
		}
	}()
	for range 1000 {
		client.observeQualificationEnqueue(env)
	}
	wg.Wait()
	SetQualificationEnqueueObserver(callback)
	client.observeQualificationEnqueue(env)
	SetQualificationEnqueueObserver(nil)
	if seen.Load() == 0 {
		t.Fatal("installed observer was not called")
	}
}

func BenchmarkQualificationEnqueueObserver(b *testing.B) {
	client := &Client{callsign: "DL1AAA", peerSessionID: 1}
	env := &spotEnvelope{spot: spot.NewSpot("DL1AABB", "DL1AAA", 14020, "CW")}
	for _, enabled := range []bool{false, true} {
		name := "nil"
		if enabled {
			name = "enabled"
		}
		b.Run(name, func(b *testing.B) {
			SetQualificationEnqueueObserver(nil)
			if enabled {
				SetQualificationEnqueueObserver(func(QualificationEnqueue) {})
			}
			b.Cleanup(func() { SetQualificationEnqueueObserver(nil) })
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				client.observeQualificationEnqueue(env)
			}
		})
	}
}
