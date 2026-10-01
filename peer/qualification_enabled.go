//go:build qualification

package peer

import (
	"context"
	"errors"
	"time"
)

// QualificationState is collected by the graph owner. Transport counters are
// read under their own ownership locks; they are samples, not an allocation
// proof or an assertion that independent peaks occurred simultaneously.
type QualificationState struct {
	ProtocolStats
	AuthorityTime, NextObservation                  time.Time
	MessageOrigins, DetachedTopologyWatermarks      int
	CompleteNodes, IncompleteNodes                  int
	ObservationCounts                               [4]int
	Established, OwnedSessions, PendingReservations int
	OwnerReservations                               int
	ControlQueued, DataQueued, ActiveWrites         int
	ControlBytes, DataBytes, ActiveBytes            int
	ProjectionReservedBytes                         int64
	ReaderBackingBytes, ReaderRawLineBytes          int64
}

type protocolQualificationState struct{ offset time.Duration }
type protocolQualificationRequest struct {
	setOffset bool
	offset    time.Duration
	state     QualificationState
	origin    string
	watermark originWatermark
	present   bool
}

func (p *protocolController) qualificationAuthorityTime(now time.Time) time.Time {
	return now.Add(p.qualification.offset)
}

func (p *protocolController) handleQualificationRequest(req *protocolQualificationRequest) error {
	if req == nil {
		return errors.New("missing qualification request")
	}
	if req.setOffset {
		p.qualification.offset = req.offset
		p.wallNow = func() time.Time { return p.qualificationAuthorityTime(time.Now()) }
	}
	if req.origin != "" {
		req.watermark, req.present = p.graph.freshness.Get(req.origin)
		return nil
	}
	p.sampleStats()
	s := QualificationState{ProtocolStats: p.manager.ProtocolStats(), AuthorityTime: p.qualificationAuthorityTime(time.Now()), NextObservation: p.graph.nextObservation,
		MessageOrigins: p.graph.messageOrigins, ProjectionReservedBytes: p.projectionBytes.Load()}
	for call, wm := range p.graph.freshness.All() {
		if !wm.MessageOnly && p.graph.nodes.Value(call) == nil {
			s.DetachedTopologyWatermarks++
		}
	}
	for _, n := range p.graph.nodes.All() {
		if n.Complete {
			s.CompleteNodes++
		} else {
			s.IncompleteNodes++
		}
		if n.Observations >= 0 && n.Observations < len(s.ObservationCounts) {
			s.ObservationCounts[n.Observations]++
		}
	}
	m := p.manager
	m.mu.RLock()
	s.Established, s.OwnedSessions, s.PendingReservations = m.sessions.Len(), m.ownedRuns.Len(), len(m.pendingSlots)
	s.OwnerReservations = len(m.ownerSlots)
	for session := range m.ownedRuns.All() {
		if session.reader != nil {
			backing, raw := session.reader.allocation.snapshot()
			s.ReaderBackingBytes += backing
			s.ReaderRawLineBytes += raw
		}
		session.queueMu.Lock()
		s.ControlQueued += session.controlCount
		s.DataQueued += len(session.writeCh)
		s.ControlBytes += session.controlBytes
		s.DataBytes += session.dataBytes
		s.ActiveBytes += session.activeBytes
		if session.activeBytes > 0 {
			s.ActiveWrites++
		}
		session.queueMu.Unlock()
	}
	m.mu.RUnlock()
	req.state = s
	return nil
}

func (m *Manager) qualificationCall(ctx context.Context, req *protocolQualificationRequest) (QualificationState, error) {
	if m == nil || m.ctx == nil {
		return QualificationState{}, errors.New("peer manager not started")
	}
	r := protocolRequest{kind: "qualification", qualification: req, done: make(chan error, 1)}
	select {
	case m.protocol.lifecycle <- r:
	case <-ctx.Done():
		return QualificationState{}, ctx.Err()
	case <-m.ctx.Done():
		return QualificationState{}, m.ctx.Err()
	}
	select {
	case err := <-r.done:
		return req.state, err
	case <-ctx.Done():
		return QualificationState{}, ctx.Err()
	case <-m.ctx.Done():
		return QualificationState{}, m.ctx.Err()
	}
}

// QualificationSnapshot reads authority through its owner; it never grants it.
func (m *Manager) QualificationSnapshot(ctx context.Context) (QualificationState, error) {
	return m.qualificationCall(ctx, &protocolQualificationRequest{})
}

// QualificationOriginWatermark checks one origin on the actor so a staged
// record cannot falsely pass a count-only authority test. It is read-only and
// absent from normal builds; requests retain at most one valid protocol call.
func (m *Manager) QualificationOriginWatermark(ctx context.Context, origin string) (float64, time.Time, bool, error) {
	call, ok := CanonicalPC92Call(origin)
	if !ok {
		return 0, time.Time{}, false, errors.New("invalid qualification origin")
	}
	req := &protocolQualificationRequest{origin: call}
	if _, err := m.qualificationCall(ctx, req); err != nil {
		return 0, time.Time{}, false, err
	}
	return req.watermark.Value, req.watermark.Accepted, req.present, nil
}

// QualificationSetClockOffset changes only authority UTC. Payload TTL, handshake
// deadlines, writes, gate stability, and qualification durations remain real.
// This API exists only in explicitly tagged qualification builds.
func (m *Manager) QualificationSetClockOffset(ctx context.Context, offset time.Duration) error {
	_, err := m.qualificationCall(ctx, &protocolQualificationRequest{setOffset: true, offset: offset})
	return err
}
