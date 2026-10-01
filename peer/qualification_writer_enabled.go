//go:build qualification

package peer

import (
	"context"
	"errors"
	"sync"
	"time"
)

// This scheduling fault delays an already dequeued write, never admission or
// accounting. The original socket deadline keeps running; cancellation or that
// deadline ends the wait even if the driver forgets its release function.
type qualificationWriterState struct {
	mu   sync.Mutex
	hold chan struct{}
}

func (q *qualificationWriterState) wait(ctx context.Context, deadline time.Time) error {
	q.mu.Lock()
	hold := q.hold
	q.mu.Unlock()
	if hold == nil {
		return nil
	}
	timer := time.NewTimer(max(0, time.Until(deadline)))
	defer timer.Stop()
	select {
	case <-hold:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return context.DeadlineExceeded
	}
}

// QualificationHoldWrites arms at most one bounded latch per established
// owner. It changes no queue, counter, identity, deadline or wire record.
func (m *Manager) QualificationHoldWrites(ctx context.Context, calls []string) (func(), error) {
	if len(calls) == 0 || len(calls) > 64 {
		return nil, errors.New("qualification writer target count outside1..64")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	owners := make([]*session, 0, len(calls))
	for _, call := range calls {
		var found *session
		for _, s := range m.sessions.All() {
			if s.remoteCall == call {
				found = s
				break
			}
		}
		if found == nil {
			return nil, errors.New("qualification writer target is not established")
		}
		for _, previous := range owners {
			if previous == found {
				return nil, errors.New("duplicate qualification writer target")
			}
		}
		owners = append(owners, found)
	}
	var once sync.Once
	armed := 0
	release := func() {
		once.Do(func() {
			for _, s := range owners[:armed] {
				s.qualificationWriter.mu.Lock()
				close(s.qualificationWriter.hold)
				s.qualificationWriter.hold = nil
				s.qualificationWriter.mu.Unlock()
			}
		})
	}
	for _, s := range owners {
		s.qualificationWriter.mu.Lock()
		if s.qualificationWriter.hold != nil {
			s.qualificationWriter.mu.Unlock()
			release()
			return nil, errors.New("qualification writer target already held")
		}
		s.qualificationWriter.hold = make(chan struct{})
		armed++
		s.qualificationWriter.mu.Unlock()
	}
	return release, nil
}

type QualificationTransportState struct {
	Call                                 string
	DataCount, ControlCount, ActiveBytes int
	DataBytes, ControlBytes              int
	DataCapacity                         int
}

// QualificationTransports is a read-only per-owner sample. Runtime lock order
// remains manager then queue; driver snapshots never invent owned population.
func (m *Manager) QualificationTransports() []QualificationTransportState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]QualificationTransportState, 0, m.sessions.Len())
	for _, s := range m.sessions.All() {
		s.queueMu.Lock()
		out = append(out, QualificationTransportState{Call: s.remoteCall, DataCount: len(s.writeCh), ControlCount: s.controlCount, ActiveBytes: s.activeBytes, DataBytes: s.dataBytes, ControlBytes: s.controlBytes, DataCapacity: cap(s.writeCh)})
		s.queueMu.Unlock()
	}
	return out
}

// QualificationCacheCounts avoids a complete graph walk while a wire driver
// waits for one bounded cache batch. The separately locked counts are samples.
func (m *Manager) QualificationCacheCounts() [4]int {
	var counts [4]int
	counts[0], _, _ = m.dedupe.occupancy()
	counts[1], _, _ = m.protocol.pc92.occupancy()
	counts[2], _, _ = m.protocol.pc93.occupancy()
	counts[3], _, _ = m.bulletinDedupe.occupancy()
	return counts
}
