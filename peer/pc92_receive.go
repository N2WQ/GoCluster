package peer

import (
	"net/netip"
	"time"
)

func (p *protocolController) failAdmission(s *session, f *Frame, reason string) {
	p.failAdmissionCause(s, f, reason, admissionAuthority)
}

func (p *protocolController) failAdmissionCause(s *session, _ *Frame, reason string, cause admissionCause) {
	p.diagnostic(reason)
	if s == nil {
		return
	}
	p.manager.retryRefused(s, cause, p.elapsedNow())
	p.drainFailures()
}
func (p *protocolController) receive(f *Frame, s *session, now time.Time) {
	if f == nil || s == nil || !s.pc9x || f.Hop == 0 || (s.ctx != nil && s.ctx.Err() != nil) {
		return
	}
	m := p.manager
	m.mu.RLock()
	current := m.sessions.Value(s.id) == s
	m.mu.RUnlock()
	if !current {
		return
	}
	if f.Type == "PC93" {
		p.receiveMessage(f, now)
		return
	}
	r, eligible := eligiblePC92Record(f, s, m.localCall)
	if !eligible {
		return
	}
	old, exists := p.graph.freshness.Get(r.Origin)
	authorityTime := p.qualificationAuthorityTime(now)
	key := pc92Key(f)
	if !freshTime(r.TimestampValue, authorityTime, old, exists) {
		if admitted, known := p.pc92.firstAdmission(key, now); known && p.graph.nodes.Value(r.Origin) != nil {
			origins := recordIngressOrigins(r)
			needed, charge := p.graph.observationReservation(origins, s.remoteCall)
			if p.graph.ingress.Len()+needed > maxIngressObservations || p.graph.retainedCharge()+charge > 96<<20 {
				p.failAdmissionCause(s, f, "PC92 alternate ingress capacity exhausted", admissionIngress)
				return
			}
			for _, origin := range origins {
				if origin != "" {
					p.graph.observe(origin, s.remoteCall, f.Hop, admitted, true)
				}
			}
		}
		return
	}
	external := r.Subject.Call != r.Origin && r.Subject.IsExternal()
	if external {
		sw, known := p.graph.freshness.Get(r.Subject.Call)
		if !freshTime(r.TimestampValue, authorityTime, sw, known) {
			return
		}
	}
	plan, err := p.graph.prepare(r, m.localCall, s.remoteCall, p.directNodes())
	if err != nil {
		p.failAdmission(s, f, "PC92 graph admission refused")
		return
	}
	if plan == nil {
		return
	}
	freshSlots := 0
	if _, ok := p.graph.freshness.Get(r.Origin); !ok {
		freshSlots++
	}
	if external {
		if _, ok := p.graph.freshness.Get(r.Subject.Call); !ok {
			freshSlots++
		}
	}
	extraBytes := freshSlots * 196
	origins := recordIngressOrigins(r)
	neededIngress, ingressCharge := p.graph.observationReservation(origins, s.remoteCall)
	extraBytes += ingressCharge
	if p.graph.freshness.Len()+freshSlots > maxFreshnessOrigins || p.graph.ingress.Len()+neededIngress > maxIngressObservations {
		p.failAdmission(s, f, "PC92 authority capacity exhausted")
		return
	}
	if p.graph.projectedCharge(plan)+extraBytes > 96<<20 {
		p.failAdmission(s, f, "PC92 retained-byte capacity exhausted")
		return
	}
	result := p.pc92.admitAt(key, now, laterAdmissionTime(now, p.elapsedNow()))
	if result == dedupeFull {
		p.failAdmission(s, f, "PC92 payload cache exhausted")
		return
	}
	if result == dedupeDuplicate {
		return
	}
	p.graph.commit(plan, authorityTime)
	p.graph.commitWatermark(r.Origin, r.TimestampValue, authorityTime, false)
	if external {
		p.graph.commitWatermark(r.Subject.Call, r.TimestampValue, authorityTime, false)
		p.graph.observe(r.Subject.Call, s.remoteCall, f.Hop, now, false)
	}
	p.graph.observe(r.Origin, s.remoteCall, f.Hop, now, false)
	committed := p.elapsedNow()
	p.qualificationAdmissionEvent(qualificationAdmissionEvent{Kind: "pc92_commit", Call: s.remoteCall, At: committed, Origin: r.Origin, Timestamp: r.Timestamp, Nodes: p.graph.nodes.Len(), Users: p.graph.users.Len(), Edges: p.graph.edges, Ingress: p.graph.ingress.Len(), Freshness: p.graph.freshness.Len()})
	if f.Hop > 1 {
		m.forwardFrame(f, f.Hop-1, s, true)
	}
}
func (p *protocolController) receiveMessage(f *Frame, now time.Time) {
	msg, ok := parsePC93(f)
	if !ok || msg.NodeCall == p.manager.localCall {
		return
	}
	origin, ok := CanonicalPC92Call(msg.NodeCall)
	if !ok || origin == p.manager.localCall {
		return
	}
	value, ok := parseWireTimestamp(msg.Timestamp)
	if !ok {
		return
	}
	old, exists := p.graph.freshness.Get(origin)
	authorityTime := p.qualificationAuthorityTime(now)
	if !freshTime(value, authorityTime, old, exists) || !p.graph.canWatermark(origin, true) {
		return
	}
	if !exists && p.graph.retainedCharge()+196 > 96<<20 {
		p.diagnostic("PC93 freshness byte capacity exhausted")
		return
	}
	result := p.pc93.admit(pc93Key(f), now)
	if result == dedupeFull {
		p.diagnostic("PC93 payload cache exhausted")
		return
	}
	if result != dedupeAccepted {
		return
	}
	p.graph.commitWatermark(origin, value, authorityTime, true)
	p.manager.routePC93(msg)
}
func parseWireTimestamp(s string) (float64, bool) {
	v, err := ParsePC9xTimestamp(s)
	return v, err == nil
}
func (p *protocolController) remoteEntry(s *session) PC92Entry {
	call, ok := CanonicalPC92Call(s.remoteCall)
	if !ok || len(call) > 15 {
		return PC92Entry{}
	}
	flags := uint8(5)
	if !s.pc9x {
		flags = 7
	}
	e := PC92Entry{Call: call, Flags: flags, Version: s.remoteVersion, Build: s.remoteBuild}
	if s.conn != nil {
		ip := remoteAddrIP(s.conn.RemoteAddr())
		if a, ok := netip.AddrFromSlice(ip); ok {
			e.IP = a.Unmap()
		}
	}
	return e
}

// An external subject carries independent authority through the same ingress.
// The fixed pair is shared by fresh and duplicate admission so both slot and
// rounded string charges are reserved before either observation is committed.
func recordIngressOrigins(r *PC92Record) [2]string {
	origins := [2]string{r.Origin, ""}
	if r.Subject.Call != r.Origin && r.Subject.IsExternal() {
		origins[1] = r.Subject.Call
	}
	return origins
}
func (g *protocolGraph) observationReservation(origins [2]string, ingress string) (slots, bytes int) {
	if ingress == "" {
		return 0, 0
	}
	for _, origin := range origins {
		if origin == "" {
			continue
		}
		if _, known := g.ingress.Get(ingressKey{origin, ingress}); !known {
			slots++
			bytes += 160 + ingressEntryBytes(origin, ingress)
		}
	}
	return slots, bytes
}
