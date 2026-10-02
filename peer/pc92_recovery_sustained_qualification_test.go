//go:build qualification

package peer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The producer uses ordinary HandleFrame admission. Its D/A pair changes one
// non-witness parent's edge/refcount while the user stays reachable elsewhere.
// Global user capacity therefore remains full, and all63 C witnesses remain
// genuinely infeasible. This is continuing traffic, not repeated stale input.
func recoveryV12Pressure(ctx context.Context, p *protocolController, live *session, f *QualificationTopology, offered *atomic.Int64, failures chan<- error, schedule *retryWorkloadSchedule) {
	var small, large, message TimestampGenerator
	index, pairs := 0, 0
	fail := func(err error) {
		select {
		case failures <- err:
		default:
		}
	}
	for index < schedule.expected {
		target := schedule.started.Add(time.Duration(index+1) * schedule.interval)
		if err := qualificationWait(ctx, max(0, time.Until(target))); err != nil {
			return
		}
		now := time.Now()
		if err := schedule.offer(index, now); err != nil {
			fail(err)
			return
		}
		node, action, entries, generator := 1, "D", f.members(1)[:1], &small
		switch index % 100 {
		case 0, 50:
			node, action, entries, generator = 0, "C", f.members(0), &large
		case 1, 2, 3, 4, 5, 6, 7, 8:
			action, entries = "K", nil
		default:
			if pairs%2 != 0 {
				action = "A"
			}
			pairs++
		}
		stamp, err := generator.NextAt(now)
		if err != nil {
			fail(err)
			return
		}
		frame, err := ParseFrame(qualificationFrame(qualificationCall("N0", node), stamp, action, entries, 1))
		if err != nil {
			fail(err)
			return
		}
		offered.Add(1)
		p.manager.HandleFrame(frame, live)
		if live.ctx.Err() != nil {
			fail(fmt.Errorf("ordinary source closed after%d offered PC92 records", offered.Load()))
			return
		}
		if index%60 == 0 {
			stamp, err = message.NextAt(now)
			if err != nil {
				fail(err)
				return
			}
			frame, err = ParseFrame("PC93^M0AAAA^" + stamp + "^*^W0TEST^^v12-pressure^H1^")
			if err != nil {
				fail(err)
				return
			}
			p.manager.HandleFrame(frame, live)
		}
		index++
	}
}

// The expected population comes from the fixed wall-time interval, never from
// delivered ticker notifications. A delayed producer cannot silently lower the
// offered workload. The final scheduled event precedes the fixed endpoint by
// one interval; catch-up work delivered after that endpoint fails the fixture.
type retryWorkloadSchedule struct {
	started, deadline time.Time
	interval          time.Duration
	expected, offered int
	maxLateness       time.Duration
}

func newRetryWorkloadSchedule(start time.Time, duration, interval time.Duration) *retryWorkloadSchedule {
	return &retryWorkloadSchedule{started: start, deadline: start.Add(duration), interval: interval, expected: int(duration/interval) - 1}
}

func (s *retryWorkloadSchedule) offer(index int, at time.Time) error {
	if index != s.offered || index >= s.expected || !at.Before(s.deadline) {
		return fmt.Errorf("offered workload mismatch: index=%d offered=%d scheduled=%d endpoint=%s actual=%s", index, s.offered, s.expected, s.deadline, at)
	}
	target := s.started.Add(time.Duration(index+1) * s.interval)
	if at.Before(target) {
		return fmt.Errorf("workload offered before scheduled time: index=%d", index)
	}
	s.maxLateness = max(s.maxLateness, at.Sub(target))
	s.offered++
	return nil
}

func (s *retryWorkloadSchedule) complete() error {
	if s.offered != s.expected {
		return fmt.Errorf("workload underproduced: scheduled=%d actual=%d max_lateness=%s", s.expected, s.offered, s.maxLateness)
	}
	return nil
}

func TestPC92V14WorkloadUnderproductionRejected(t *testing.T) {
	start := time.Now()
	for _, scenario := range []string{"valid", "missing", "late", "early", "duplicate"} {
		t.Run(scenario, func(t *testing.T) {
			s := newRetryWorkloadSchedule(start, 40*time.Millisecond, 10*time.Millisecond)
			var failure error
			for index := range s.expected {
				if scenario == "missing" && index == 2 {
					break
				}
				at := start.Add(time.Duration(index+1) * 10 * time.Millisecond)
				if scenario == "late" && index == 2 {
					at = s.deadline
				}
				if scenario == "early" && index == 1 {
					at = start
				}
				actualIndex := index
				if scenario == "duplicate" && index == 1 {
					actualIndex = 0
				}
				if err := s.offer(actualIndex, at); err != nil {
					failure = err
					break
				}
			}
			if failure == nil {
				failure = s.complete()
			}
			if (scenario == "valid") != (failure == nil) {
				t.Fatalf("scenario=%s failure=%v", scenario, failure)
			}
		})
	}
}

func runPC92V12RecoverySustained(t *testing.T, p *protocolController, f *QualificationTopology, live *session, membership *atomic.Pointer[LocalMembership], before *LocalMembership, workers *sync.WaitGroup, expectedBlocked int) {
	t.Helper()
	m := p.manager
	ctx, stop := context.WithCancel(m.ctx)
	done := make(chan struct{})
	producerDone := make(chan struct{})
	var offered, committed, messages atomic.Int64
	var peak atomic.Int64
	failures := make(chan error, 1)
	m.QualificationSetAdmissionObserver(func(event QualificationAdmissionEvent) {
		if event.Kind == "pc92_commit" && event.Call == live.remoteCall {
			committed.Add(1)
		}
		if event.Kind == "mailbox" {
			value := int64(event.InputPC92)
			for old := peak.Load(); value > old && !peak.CompareAndSwap(old, value); old = peak.Load() {
			}
		}
	})
	m.SetAnnouncementBroadcast(func(string) { messages.Add(1) })
	events := make(chan QualificationPublication, 4)
	var overflow atomic.Bool
	m.QualificationSetPublicationObserver(func(event QualificationPublication) {
		if event.Peer == live.remoteCall && event.Action == "A" && strings.Contains(event.Wire, "203.0.113.9") {
			select {
			case events <- event:
			default:
				overflow.Store(true)
			}
		}
	})
	started := time.Now()
	schedule := newRetryWorkloadSchedule(started, 4*time.Second, 10*time.Millisecond)
	workers.Add(1)
	go func() {
		defer workers.Done()
		defer close(producerDone)
		recoveryV12Pressure(ctx, p, live, f, &offered, failures, schedule)
	}()
	go func() { defer close(done); p.run(ctx) }()
	defer func() { stop(); <-done; <-producerDone }()
	// Publish a stable independent change while input/refcount mutations keep
	// every failure episode unresolved. It may not be postponed by that work.
	if err := qualificationWait(ctx, 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	after := &LocalMembership{Revision: 2, Complete: true, RawCount: 1000, Users: append([]LocalUser(nil), before.Users...)}
	after.Users[0].IP = "203.0.113.9"
	available := time.Now()
	membership.Store(after)
	m.NotifyMembershipChanged()
	var admitted time.Time
	var failed error
	timer := time.NewTimer(time.Until(started.Add(4 * time.Second)))
	defer timer.Stop()
observe:
	for {
		select {
		case event := <-events:
			admitted = event.At
		case failed = <-failures:
			break observe
		case <-timer.C:
			break observe
		}
	}
	<-producerDone
	if err := schedule.complete(); err != nil && failed == nil {
		failed = err
	}
	// All admitted input must drain, not merely a convenient sampled subset.
	// This is a separate completion allowance after the fixed offered window.
	if failed == nil {
		_, err := qualificationAwaitUntil(ctx, time.Now().Add(time.Second), time.Millisecond, func(context.Context) (bool, error) {
			return committed.Load() == offered.Load() && messages.Load() == int64((schedule.expected+59)/60), nil
		}, func(ready bool) bool { return ready })
		if err != nil {
			failed = fmt.Errorf("mandatory offered outputs did not reconcile: %w", err)
		}
	}
	stop()
	<-done
	elapsed := time.Since(started)
	p.queueMu.Lock()
	queued, queuedBytes := p.queued[0], p.bytes[0]
	p.queueMu.Unlock()
	currentUsers, blocked := p.graph.users.Len(), p.blocked.Len()
	delay := admitted.Sub(available)
	t.Logf("sustained necessary gate: configured=64 initialBlocked=%d live=1 nodes=%d users=%d edges=%d ingress=%d freshness=%d graphBytes=%d witnessMembers=8000 localUsers=1000 elapsed=%s scheduledPC92=%d offeredPC92=%d maxOfferLateness=%s committedPC92=%d deliveredPC93=%d peakInput=%d remainingInput=%d inputBytes=%d blocked=%d membershipQueueAdmission=%s admitted=%v sourceClosed=%v failure=%v", expectedBlocked, p.graph.nodes.Len(), currentUsers, p.graph.edges, p.graph.ingress.Len(), p.graph.freshness.Len(), p.graph.retainedCharge(), elapsed, schedule.expected, offered.Load(), schedule.maxLateness, committed.Load(), messages.Load(), peak.Load(), queued, queuedBytes, blocked, delay, !admitted.IsZero(), live.ctx.Err() != nil, failed)
	if failed != nil || overflow.Load() || live.ctx.Err() != nil || admitted.IsZero() || delay < 0 || delay > time.Second || currentUsers != 65536 || blocked != expectedBlocked {
		t.Fatalf("v14 retry service failed continuing ordinary workload: %v; stop before broader qualification", failed)
	}
	if messages.Load() == 0 {
		t.Fatal("fixture did not demonstrate ordinary PC93 delivery; compare the zero-blocker control before attributing this to recovery cost")
	}
	// Backlog is diagnostic, not a new acceptance threshold. This bounded case
	// fails only an existing deadline/resource obligation; it cannot establish
	// sustainability or replace the complete qualification workload.
	t.Logf("diagnostic throughput: offeredPC92=%d committedPC92=%d outstanding=%d; PASS ONLY for%d-blocked necessary condition, complete v14 obligations remain", offered.Load(), committed.Load(), offered.Load()-committed.Load(), expectedBlocked)
}
