package peer

import (
	"log"
	"time"
)

type admissionFailure struct {
	at         time.Time
	generation uint64
	cause      admissionCause
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
	RetryCooldown, RetryWaiting, RetryActive, RetryHealthy, RetryEligible, RetryGated                      int
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
	counts := p.manager.retryCountsLocked(p.elapsedNow())
	stats.RetryCooldown, stats.RetryWaiting, stats.RetryActive = counts[0], counts[1], counts[2]
	stats.RetryHealthy, stats.RetryEligible, stats.RetryGated = counts[3], counts[4], counts[5]
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
	if previous != nil && (stats.RetryCooldown != previous.RetryCooldown || stats.RetryWaiting != previous.RetryWaiting || stats.RetryActive != previous.RetryActive || stats.RetryHealthy != previous.RetryHealthy || stats.RetryEligible != previous.RetryEligible || stats.RetryGated != previous.RetryGated) {
		// Sampled aggregate transitions expose operational state without a
		// per-attempt history, sensitive payload, or logging under manager.mu.
		log.Printf("Peering: PC92 retries cooldown=%d waiting=%d active=%d healthy-reset=%d eligible=%d global-gated=%d", stats.RetryCooldown, stats.RetryWaiting, stats.RetryActive, stats.RetryHealthy, stats.RetryEligible, stats.RetryGated)
	}
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
func (m *Manager) recordAdmissionFailure(s *session, _ *Frame, now time.Time) {
	if s != nil {
		m.retryRefused(s, admissionMailbox, now)
	}
}

// Drain on the topology owner before acknowledging invalidation. Holding mu
// only for scalar handoff leaves graph mutation outside the registry lock.
func (p *protocolController) drainFailures() {
	m := p.manager
	for {
		m.mu.Lock()
		var call string
		var failure admissionFailure
		for key, value := range m.admissionFailures.All() {
			call, failure = key, value
			break
		}
		if call != "" {
			m.admissionFailures.Delete(call)
		}
		m.mu.Unlock()
		if call == "" {
			return
		}
		p.graph.loseIngress(call)
		p.blocked.Set(call, admissionEpisode{generation: failure.generation, cause: failure.cause, at: failure.at})
		m.mu.Lock()
		if r := m.retryIdentityLocked(call); r != nil && r.invalidationGeneration == failure.generation {
			r.invalidating = false
			p.qualificationAdmissionEvent(qualificationAdmissionEvent{Kind: "ingress_invalidated", Call: call, Generation: failure.generation, Cause: failure.cause.String(), At: p.elapsedNow()})
		}
		m.mu.Unlock()
		p.diagnostic("PC92 input admission refused")
	}
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
