package peer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// LocalMembership is an immutable bounded snapshot of actual telnet owners.
// Complete=false never authorizes publishing its partial population.
type LocalUser struct {
	SessionID uint64
	Login, IP string
}
type LocalMembership struct {
	Revision uint64
	RawCount int
	Complete bool
	Users    []LocalUser
}

type candidateState struct {
	staged       []string
	bytes        int
	pc9x         bool
	initialASent bool // the timestamp retry resumes K without duplicating startup A
}
type protocolInput struct {
	wire   string
	source *session
	at     time.Time
	class  int
	charge int
}
type protocolRequest struct {
	kind          string
	source        *session
	done          chan error
	qualification *protocolQualificationRequest
}
type recoveryState struct {
	phase    int
	revision uint64
}
type protocolController struct {
	qualification           protocolQualificationState
	projectionBytes         atomic.Int64
	startupSecond           int64
	clockFloor              int64
	observedWall            time.Time
	wallAdvancedAt          time.Time
	lastExpiryWall          time.Time
	recoveryWall            time.Time
	quiescing               bool
	blockedRecords          *boundedIndex[string, string]
	blockedInput            *boundedIndex[string, bool]
	current                 *boundedIndex[string, PC92Entry]
	revision                uint64
	wallNow                 func() time.Time
	elapsedNow              func() time.Time
	manager                 *Manager
	input                   chan protocolInput
	lifecycle               chan protocolRequest
	wake                    chan struct{}
	queueMu                 sync.Mutex
	queued                  [2]int
	bytes                   [2]int
	graph                   *protocolGraph
	pc92, pc93              *dedupeCache
	timestamps              *TimestampGenerator
	published               *boundedIndex[string, PC92Entry]
	recovering              *boundedIndex[*session, recoveryState]
	pendingK                *boundedIndex[*session, bool]
	dirty                   bool
	clockGate, capacityGate bool
	unsafeSince, safeSince  time.Time
	lastPublish             time.Time
	lastProjection          time.Time
	blocked                 *boundedIndex[string, time.Time]
	diagnosticAt            *boundedIndex[string, time.Time]
	projection              chan graphProjection
}

func newProtocolController(m *Manager) *protocolController {
	// These actor-owned indexes inherit the64-peer and1000-local-user limits.
	// Fixed arrays have no growth-generation overlap; the two retained membership
	// generations can coexist with a tick's replacement and a nested K snapshot.
	// The diagnostic capacity covers every literal call-site reason (tested).
	return &protocolController{manager: m, blockedRecords: newFixedIndex[string, string](64), blockedInput: newFixedIndex[string, bool](64), wallNow: time.Now, elapsedNow: time.Now, input: make(chan protocolInput, 256), lifecycle: make(chan protocolRequest, 128), wake: make(chan struct{}, 1),
		graph: newProtocolGraph(time.Now()), pc92: newBoundedDedupe(600*time.Second, 65536, 8<<20), pc93: newBoundedDedupe(600*time.Second, 65536, 8<<20),
		timestamps: NewTimestampGenerator(), published: newFixedIndex[string, PC92Entry](1064), recovering: newFixedIndex[*session, recoveryState](64), pendingK: newFixedIndex[*session, bool](64), blocked: newFixedIndex[string, time.Time](64),
		diagnosticAt: newFixedIndex[string, time.Time](18), projection: make(chan graphProjection, 1), dirty: true}
}
func (m *Manager) SetMembershipProvider(fn func() LocalMembership) {
	m.mu.Lock()
	m.membershipFn = fn
	m.mu.Unlock()
}
func (m *Manager) NotifyMembershipChanged() {
	if m.protocol == nil {
		return
	}
	select {
	case m.protocol.wake <- struct{}{}:
	default:
	}
}
func (m *Manager) SetCurrentDirectMessage(fn func(string, uint64, uint64, string) bool) {
	m.mu.Lock()
	m.currentDirect = fn
	m.mu.Unlock()
}
func (m *Manager) SetBuildIdentity(version, commit, buildTime, vcsModified, goVersion string) error {
	banner, err := BuildPC18Banner(version, commit, buildTime, vcsModified, goVersion)
	if err != nil {
		return err
	}
	line, err := FormatPC18(banner, m.cfg.NodeVersion, true)
	if err != nil {
		return err
	}
	if m.cfg.MaxLineLength > 0 && len(line) > m.cfg.MaxLineLength {
		return fmt.Errorf("PC18 exceeds transport limit")
	}
	m.pc18Banner = banner
	return nil
}
func (m *Manager) membership() LocalMembership {
	m.mu.RLock()
	fn := m.membershipFn
	m.mu.RUnlock()
	if fn == nil {
		return LocalMembership{Complete: true}
	}
	return fn()
}
func (m *Manager) protocolCall(kind string, s *session) error {
	if m.protocol == nil || m.ctx == nil {
		return fmt.Errorf("peer controller not started")
	}
	req := protocolRequest{kind: kind, source: s, done: make(chan error, 1)}
	ctx := m.ctx
	if s != nil && s.ctx != nil && kind != "closed" {
		ctx = s.ctx
	}
	select {
	case m.protocol.lifecycle <- req:
	case <-m.ctx.Done():
		return m.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-req.done:
		return err
	case <-m.ctx.Done():
		return m.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (m *Manager) publishInitial(s *session) error {
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		err := m.protocolCall("initial", s)
		if !errors.Is(err, ErrTimestampRate) {
			return err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-timer.C:
		case <-deadline.C:
			timer.Stop()
			return fmt.Errorf("PC92 timestamp progress timeout")
		case <-s.ctx.Done():
			timer.Stop()
			return s.ctx.Err()
		}
	}
}
func (m *Manager) publishPeriodic(s *session, action string) error { return m.protocolCall(action, s) }
func (m *Manager) establishSession(s *session) error               { return m.protocolCall("establish", s) }

func (m *Manager) trackCandidate(s *session) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopping {
		m.releaseCandidateSlotsLocked(s)
		return fmt.Errorf("peer manager stopping")
	}
	if !s.pendingReserved {
		if !m.reserveCandidateSlotsLocked() {
			return fmt.Errorf("pending handshake or transport owner capacity")
		}
		s.pendingReserved, s.ownerReserved = true, true
	}
	m.candidates.Set(s, &candidateState{})
	m.ownedRuns.Set(s, true)
	m.wg.Add(1)
	return nil
}
func (m *Manager) releaseCandidate(s *session) {
	m.mu.Lock()
	if c := m.candidates.Value(s); c != nil {
		m.releaseStagedLocked(c)
		m.candidates.Delete(s)
	}
	m.releaseCandidateSlotsLocked(s)
	owned := m.ownedRuns.Value(s)
	m.ownedRuns.Delete(s)
	m.mu.Unlock()
	if owned {
		m.wg.Done()
	}
}

// Staging reservations follow the batch, including the controller's active
// establishment drain after it has left candidates. The caller owns m.mu.
func (m *Manager) releaseStagedLocked(c *candidateState) {
	m.stagedRecords -= len(c.staged)
	m.stagedBytes -= c.bytes
	clear(c.staged)
	c.staged, c.bytes = nil, 0
}

func (m *Manager) releaseStaged(c *candidateState) {
	if c == nil {
		return
	}
	m.mu.Lock()
	m.releaseStagedLocked(c)
	m.mu.Unlock()
}

func (m *Manager) stagePC92Record(s *session, f *Frame, r *PC92Record) error {
	if f.Hop == 0 || r.Origin == m.localCall {
		return nil
	}
	if r.Origin == s.remoteCall && r.Subject.Call == s.remoteCall {
		if r.Subject.Version != "" {
			s.setRemoteVersion(r.Subject.Version)
		}
		if r.Subject.Build != "" {
			s.setRemoteBuild(r.Subject.Build)
		}
		s.remoteBitmap = int(r.Subject.Flags)
	}
	wire := f.Encode(f.Hop)
	charge := allocationBytes(len(wire))
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.candidates.Value(s)
	if c == nil {
		return fmt.Errorf("unowned handshake")
	}
	c.pc9x = true
	if m.pc9xGated.Load() {
		return fmt.Errorf("PC9x admission gated")
	}
	if c.staged == nil {
		// A fixed backing array has no append-growth overlap. Its pointer-bearing
		// allocation is reserved together with the first retained wire.
		charge += pointerAllocationBytes(256 * 16)
	}
	if len(c.staged) >= 256 || c.bytes+charge > 512<<10 || m.stagedRecords >= 8192 || m.stagedBytes+charge > 16<<20 {
		return fmt.Errorf("handshake staging capacity")
	}
	if c.staged == nil {
		c.staged = make([]string, 0, 256)
	}
	c.staged = append(c.staged, strings.Clone(wire))
	c.bytes += charge
	m.stagedRecords++
	m.stagedBytes += charge
	return nil
}
func (p *protocolController) enqueue(f *Frame, s *session, now time.Time) bool {
	class := 0
	maxCount, maxBytes := 192, 3<<20
	if f.Type == "PC93" {
		class = 1
		maxCount, maxBytes = 64, 1<<20
	}
	wire := f.Encode(f.Hop)
	charge := allocationBytes(len(wire)) + 64
	p.queueMu.Lock()
	defer p.queueMu.Unlock()
	if p.queued[class] >= maxCount || p.bytes[class]+charge > maxBytes {
		return false
	}
	p.queued[class]++
	p.bytes[class] += charge
	p.input <- protocolInput{strings.Clone(wire), s, now, class, charge}
	return true
}
func (p *protocolController) run(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	maintenance := time.NewTicker(time.Second)
	defer maintenance.Stop()
	for {
		// Lifecycle requests and membership wakeups cannot be displaced by received
		// record capacity. All state transitions still run on this sole owner.
		select {
		case req := <-p.lifecycle:
			req.done <- p.request(req)
			continue
		default:
		}
		select {
		case <-ctx.Done():
			return
		case req := <-p.lifecycle:
			req.done <- p.request(req)
		case <-p.wake:
			p.drainFailures()
			p.dirty = true
		case work := <-p.input:
			p.queueMu.Lock()
			p.queued[work.class]--
			p.bytes[work.class] -= work.charge
			p.queueMu.Unlock()
			f, err := ParseFrame(work.wire)
			if err == nil {
				p.receive(f, work.source, work.at)
			}
		case now := <-ticker.C:
			p.tick(now)
		case now := <-maintenance.C:
			p.pc92.prune(now)
			p.pc93.prune(now)
			p.manager.dedupe.prune(now)
			p.manager.bulletinDedupe.prune(now)
			wall := p.wallNow().UTC()
			clockSafe := !p.clockGate && wall.After(p.lastExpiryWall) && p.timestamps.ClockSafe(wall) == nil
			if wall.After(p.lastExpiryWall) {
				p.lastExpiryWall = wall
			}
			p.graph.expire(p.qualificationAuthorityTime(now), clockSafe, p.directNodes())
			p.project(now)
			p.sampleStats()
		}
	}
}
func (p *protocolController) request(req protocolRequest) error {
	s := req.source
	switch req.kind {
	case "qualification":
		return p.handleQualificationRequest(req.qualification)
	case "ready":
		p.tick(time.Now())
		return nil
	case "initial":
		if p.clockGate || p.capacityGate || p.manager.outboundGated(s.peer) {
			return fmt.Errorf("PC9x publication gated")
		}
		p.manager.mu.Lock()
		candidate := p.manager.candidates.Value(s)
		if candidate != nil {
			candidate.pc9x = true
		}
		p.manager.mu.Unlock()
		if candidate == nil {
			return fmt.Errorf("unowned handshake")
		}
		remote := p.remoteEntry(s)
		if remote.Call == "" || !s.remotePublicationMetadataOK() {
			return fmt.Errorf("remote peer identity unavailable")
		}
		if !candidate.initialASent {
			if err := p.sendRecord([]*session{s}, "A", []PC92Entry{remote}, false); err != nil {
				return err
			}
			if s.ctx != nil && s.ctx.Err() != nil {
				return s.ctx.Err()
			}
			candidate.initialASent = true
		}
		return p.sendRecord([]*session{s}, "K", nil, false)
	case "establish":
		if s.pc9x && (p.clockGate || p.capacityGate || p.manager.outboundGated(s.peer)) {
			return fmt.Errorf("PC9x establishment gated")
		}
		if err := p.manager.registerSession(s); err != nil {
			return err
		}
		p.manager.mu.Lock()
		c := p.manager.candidates.Value(s)
		var staged []string
		if c != nil {
			staged = c.staged
			p.manager.candidates.Delete(s)
			if s.pendingReserved {
				<-p.manager.pendingSlots
				s.pendingReserved = false
			}
		}
		p.manager.mu.Unlock()
		defer p.manager.releaseStaged(c)
		for _, wire := range staged {
			f, err := ParseFrame(wire)
			if err == nil {
				p.receive(f, s, time.Now())
			}
			if s.ctx != nil && s.ctx.Err() != nil {
				return s.ctx.Err()
			}
		}
		if s.pc9x {
			p.recovering.Set(s, recoveryState{})
		}
		p.dirty = true
		p.tick(time.Now())
		return nil
	case "closed":
		p.manager.mu.Lock()
		owned := p.manager.sessions.Value(s.id) == s
		if owned {
			p.manager.sessions.Delete(s.id)
		}
		p.manager.mu.Unlock()
		if !owned {
			return nil
		}
		p.graph.loseIngress(s.remoteCall)
		p.recovering.Delete(s)
		p.pendingK.Delete(s)
		p.dirty = true
		return nil
	case "K", "C":
		if p.clockGate || p.capacityGate {
			return fmt.Errorf("PC9x publication gated")
		}
		if _, pending := p.recovering.Get(s); pending {
			return nil
		}
		if req.kind == "C" {
			p.recovering.Set(s, recoveryState{})
			return nil
		}
		p.pendingK.Set(s, true)
		p.tick(p.elapsedNow())
		return nil
	case "withdraw":
		p.quiescing = true
		return p.sendRecord(p.sessions(), "D", entryValues(p.published), false)
	default:
		return fmt.Errorf("unknown controller request")
	}
}
func (p *protocolController) sessions() []*session {
	m := p.manager
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*session, 0, m.sessions.Len())
	for _, s := range m.sessions.All() {
		if s.pc9x {
			out = append(out, s)
		}
	}
	return out
}
func (p *protocolController) directNodes() *boundedIndex[string, bool] {
	m := p.manager
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := newFixedIndex[string, bool](64)
	for _, s := range m.sessions.All() {
		out.Set(s.remoteCall, true)
	}
	return out
}
func (p *protocolController) diagnostic(reason string) {
	now := time.Now()
	if last := p.diagnosticAt.Value(reason); !last.IsZero() && now.Sub(last) < time.Minute {
		return
	}
	// Reasons are fixed literals at call sites; this is bounded by their finite set.
	p.diagnosticAt.Set(reason, now)
	log.Printf("Peering: %s (nodes=%d users=%d edges=%d ingress=%d freshness=%d)", reason, p.graph.nodes.Len(), p.graph.users.Len(), p.graph.edges, p.graph.ingress.Len(), p.graph.freshness.Len())
}
func (p *protocolController) failAdmission(s *session, f *Frame, reason string) {
	p.diagnostic(reason)
	if s == nil {
		return
	}
	p.graph.loseIngress(s.remoteCall)
	p.blocked.Set(s.remoteCall, time.Time{})
	p.blockedRecords.Set(s.remoteCall, strings.Clone(f.Encode(f.Hop)))
	p.blockedInput.Delete(s.remoteCall)
	p.manager.mu.Lock()
	p.manager.blockedPeers.Set(s.remoteCall, true)
	p.manager.mu.Unlock()
	s.close()
}
func (p *protocolController) receive(f *Frame, s *session, now time.Time) {
	if s == nil || !s.pc9x || f.Hop == 0 || (s.ctx != nil && s.ctx.Err() != nil) {
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
	r, err := DecodePC92(f)
	if err != nil || r.Origin == m.localCall {
		return
	}
	old, exists := p.graph.freshness.Get(r.Origin)
	authorityTime := p.qualificationAuthorityTime(now)
	key := pc92Key(f)
	if !freshTime(r.TimestampValue, authorityTime, old, exists) {
		if p.pc92.contains(key, now) && p.graph.nodes.Value(r.Origin) != nil {
			if _, known := p.graph.ingress.Get(ingressKey{r.Origin, s.remoteCall}); !known &&
				(!p.graph.canObserve(r.Origin, s.remoteCall) || p.graph.retainedCharge()+160+ingressEntryBytes(r.Origin, s.remoteCall) > 96<<20) {
				p.failAdmission(s, f, "PC92 alternate ingress capacity exhausted")
				return
			}
			p.graph.observe(r.Origin, s.remoteCall, f.Hop, old.Accepted, true)
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
	origins := []string{r.Origin}
	if external {
		origins = append(origins, r.Subject.Call)
	}
	neededIngress := 0
	for _, origin := range origins {
		if _, ok := p.graph.ingress.Get(ingressKey{origin, s.remoteCall}); !ok {
			neededIngress++
			extraBytes += 160 + ingressEntryBytes(origin, s.remoteCall)
		}
	}
	if p.graph.freshness.Len()+freshSlots > maxFreshnessOrigins || p.graph.ingress.Len()+neededIngress > maxIngressObservations {
		p.failAdmission(s, f, "PC92 authority capacity exhausted")
		return
	}
	if p.graph.projectedCharge(plan)+extraBytes > 96<<20 {
		p.failAdmission(s, f, "PC92 retained-byte capacity exhausted")
		return
	}
	result := p.pc92.admit(key, now)
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
		p.graph.observe(r.Subject.Call, s.remoteCall, f.Hop, authorityTime, false)
	}
	p.graph.observe(r.Origin, s.remoteCall, f.Hop, authorityTime, false)
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
