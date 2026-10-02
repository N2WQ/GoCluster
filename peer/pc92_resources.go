package peer

import (
	"strings"
	"time"
)

type admissionFailure struct {
	wire string
	at   time.Time
}

// ProtocolStats is a sampled immutable view. Counters distinguish byte/count
// refusal from ordinary duplicates and expose every controller-owned index.
type ProtocolStats struct {
	Nodes, Users, Edges, Ingress, Freshness                                                                int
	GraphChargedBytes                                                                                      int
	ParseScratchBytes, ParseScratchPeakBytes                                                               int64
	SpotKeys, SpotKeyBytes, PC92Keys, PC92KeyBytes, PC93Keys, PC93KeyBytes, BulletinKeys, BulletinKeyBytes int
	SpotRefused, PC92Refused, PC93Refused, BulletinRefused                                                 uint64
	PC93InputRefused                                                                                       uint64
	Pending, StagedRecords, StagedBytes                                                                    int
	InputPC92, InputPC93, InputPC92Bytes, InputPC93Bytes                                                   int
	Recovering, BlockedPeers                                                                               int
	ClockGated, PublicationGated                                                                           bool
}

func (m *Manager) ProtocolStats() ProtocolStats {
	if m == nil {
		return ProtocolStats{}
	}
	stats := m.protocolStats.Load()
	if stats == nil {
		return ProtocolStats{}
	}
	return *stats
}
func (p *protocolController) sampleStats() {
	g := p.graph
	stats := ProtocolStats{Nodes: g.nodes.Len(), Users: g.users.Len(), Edges: g.edges, Ingress: g.ingress.Len(), Freshness: g.freshness.Len(), GraphChargedBytes: g.retainedCharge(), Recovering: p.recovering.Len(), BlockedPeers: p.blocked.Len(), ClockGated: p.clockGate, PublicationGated: p.capacityGate}
	stats.ParseScratchBytes, stats.ParseScratchPeakBytes = p.manager.parseBudget.usage()
	stats.SpotKeys, stats.SpotKeyBytes, stats.SpotRefused = p.manager.dedupe.occupancy()
	stats.PC92Keys, stats.PC92KeyBytes, stats.PC92Refused = p.pc92.occupancy()
	stats.PC93Keys, stats.PC93KeyBytes, stats.PC93Refused = p.pc93.occupancy()
	stats.BulletinKeys, stats.BulletinKeyBytes, stats.BulletinRefused = p.manager.bulletinDedupe.occupancy()
	p.manager.mu.RLock()
	stats.Pending = p.manager.candidates.Len()
	stats.StagedRecords = p.manager.stagedRecords
	stats.StagedBytes = p.manager.stagedBytes
	p.manager.mu.RUnlock()
	p.queueMu.Lock()
	stats.InputPC92 = p.queued[0]
	stats.InputPC93 = p.queued[1]
	stats.InputPC92Bytes = p.bytes[0]
	stats.InputPC93Bytes = p.bytes[1]
	stats.PC93InputRefused = p.inputPC93Refused
	p.queueMu.Unlock()
	previous := p.manager.protocolStats.Load()
	p.manager.protocolStats.Store(&stats)
	// The cumulative counter survives drain. Emit a rate-limited fixed reason
	// only for newly sampled refusals, not forever after the first incident.
	if stats.PC93InputRefused > 0 && (previous == nil || stats.PC93InputRefused > previous.PC93InputRefused) {
		p.diagnostic("PC93 input admission refused")
	}
	if stats.SpotRefused > 0 {
		p.diagnostic("spot forwarding dedupe refusals recorded")
	}
	if stats.BulletinRefused > 0 {
		p.diagnostic("bulletin dedupe refusals recorded")
	}
}
func (m *Manager) recordAdmissionFailure(s *session, f *Frame, now time.Time) {
	if s == nil {
		return
	}
	m.mu.Lock()
	// Only current registered ownership can create a reconnect gate. Keys are
	// configured remote identities, so the side table has at most64 entries.
	if m.sessions.Value(s.id) == s {
		m.admissionFailures.Set(s.remoteCall, admissionFailure{strings.Clone(f.Encode(f.Hop)), now})
		m.blockedPeers.Set(s.remoteCall, true)
	}
	m.mu.Unlock()
	m.NotifyMembershipChanged()
	s.close()
}
func (p *protocolController) drainFailures() {
	m := p.manager
	m.mu.Lock()
	failures := m.admissionFailures
	m.admissionFailures = newFixedIndex[string, admissionFailure](64)
	m.mu.Unlock()
	for call, failure := range failures.All() {
		p.graph.loseIngress(call)
		p.blocked.Set(call, time.Time{})
		p.blockedRecords.Set(call, failure.wire)
		p.blockedInput.Set(call, true)
		p.diagnostic("PC92 input admission refused")
	}
}
func (p *protocolController) admissionHeadroom(call string) bool {
	wire := p.blockedRecords.Value(call)
	if wire == "" {
		return false
	}
	p.queueMu.Lock()
	queued, queuedBytes := p.queued[0], p.bytes[0]
	p.queueMu.Unlock()
	if queued >= 192 || queuedBytes+allocationBytes(len(wire))+64 > 3<<20 {
		return false
	}
	// Mailbox refusal precedes semantic decoding. Its recovery depends only on
	// mailbox space; a malformed refused line must not create a permanent gate.
	if p.blockedInput.Value(call) {
		return true
	}
	frame, err := ParseFrame(wire)
	if err != nil {
		return false
	}
	record, err := DecodePC92(frame)
	if err != nil {
		return false
	}
	count, bytes, _ := p.pc92.occupancy()
	if count >= 65536 || bytes+len(pc92Key(frame)) > 8<<20 {
		return false
	}
	g := p.graph
	origins := []string{record.Origin}
	if record.Subject.IsExternal() && record.Subject.Call != record.Origin {
		origins = append(origins, record.Subject.Call)
	}
	neededFresh, neededIngress, extraBytes := 0, 0, 0
	for _, origin := range origins {
		if _, ok := g.freshness.Get(origin); !ok {
			neededFresh++
			extraBytes += 196
		}
		if _, ok := g.ingress.Get(ingressKey{origin, call}); !ok {
			neededIngress++
			extraBytes += 160 + ingressEntryBytes(origin, call)
		}
	}
	if g.freshness.Len()+neededFresh > maxFreshnessOrigins || g.ingress.Len()+neededIngress > maxIngressObservations {
		return false
	}
	plan, err := g.prepare(record, p.manager.localCall, call, p.directNodes())
	if err != nil {
		return false
	}
	if plan != nil && g.projectedCharge(plan)+extraBytes > 96<<20 {
		return false
	}
	return true
}

func (m *Manager) drainControl(limit time.Duration) {
	if limit <= 0 {
		return
	}
	deadline := time.NewTimer(limit)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		m.mu.RLock()
		sessions := make([]*session, 0, m.sessions.Len())
		for _, s := range m.sessions.All() {
			sessions = append(sessions, s)
		}
		m.mu.RUnlock()
		pending := false
		for _, s := range sessions {
			s.queueMu.Lock()
			pending = pending || s.controlCount > 0 || (s.activeControl && s.activeBytes > 0)
			s.queueMu.Unlock()
		}
		if !pending {
			return
		}
		select {
		case <-m.ctx.Done():
			return
		case <-deadline.C:
			return
		case <-tick.C:
		}
	}
}
