package peer

import (
	"context"
	"fmt"
	"log"
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
	replayAt     int
	replayDone   chan error
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
	deadline      time.Time
	attempt       *establishmentAttempt
}
type recoveryState struct {
	phase    int
	metadata string // immutable encoded A baseline; at most64 bounded wires
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
	current                 *boundedIndex[string, PC92Entry]
	wallNow                 func() time.Time
	elapsedNow              func() time.Time
	manager                 *Manager
	input                   chan protocolInput
	lifecycle               chan protocolRequest
	wake                    chan struct{}
	queueMu                 sync.Mutex
	queued                  [2]int
	bytes                   [2]int
	inputPC93Refused        uint64 // cumulative mailbox refusals; queueMu owns it
	graph                   *protocolGraph
	pc92, pc93              *dedupeCache
	timestamps              *TimestampGenerator
	published               *boundedIndex[string, PC92Entry]
	recovering              *boundedIndex[*session, recoveryState]
	pendingK                *boundedIndex[*session, bool]
	dirty                   bool
	clockGate, capacityGate bool
	unsafeSince, safeSince  time.Time
	lastProjection          time.Time
	blocked                 *boundedIndex[string, admissionEpisode]
	diagnosticAt            *boundedIndex[string, time.Time]
	projection              chan graphProjection
	replays                 *boundedIndex[*session, *candidateState]
	replayOrder             [64]*session
	replayCursor            int
}

func newProtocolController(m *Manager) *protocolController {
	// These actor-owned indexes inherit the64-peer and1000-local-user limits.
	// Fixed arrays have no growth-generation overlap; the two retained membership
	// generations can coexist with a tick's replacement and a nested K snapshot.
	// The diagnostic capacity covers every literal call-site reason (tested).
	p := &protocolController{manager: m, wallNow: time.Now, elapsedNow: time.Now, input: make(chan protocolInput, 256), lifecycle: make(chan protocolRequest, 128), wake: make(chan struct{}, 1),
		graph: newProtocolGraph(time.Now()), pc92: newBoundedDedupe(600*time.Second, 65536, 8<<20), pc93: newBoundedDedupe(600*time.Second, 65536, 8<<20),
		timestamps: NewTimestampGenerator(), published: newFixedIndex[string, PC92Entry](1000 + m.cfg.MaxPeers), recovering: newFixedIndex[*session, recoveryState](m.cfg.MaxPeers), pendingK: newFixedIndex[*session, bool](m.cfg.MaxPeers), blocked: newFixedIndex[string, admissionEpisode](m.cfg.MaxPeers),
		diagnosticAt: newFixedIndex[string, time.Time](19), projection: make(chan graphProjection, 1), dirty: true,
		replays: newFixedIndex[*session, *candidateState](m.cfg.MaxPeers)}
	p.pc92.recovery = &dedupeRecovery{emit: p.qualificationAdmissionEvent}
	p.emitMailboxLocked(p.elapsedNow())
	return p
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
	m.mu.RLock()
	ready := s.replayReady
	m.mu.RUnlock()
	if ready != nil {
		<-ready // controller has retired the batch before transport permit release
	}
	m.mu.Lock()
	if c := m.candidates.Value(s); c != nil {
		m.releaseStagedLocked(c)
		m.candidates.Delete(s)
	}
	m.releaseCandidateSlotsLocked(s)
	m.retryRetireLocked(s, time.Now())
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
	if class == 0 {
		p.emitMailboxLocked(p.elapsedNow())
	}
	if p.queued[class] >= maxCount || p.bytes[class]+charge > maxBytes {
		if class == 1 {
			p.inputPC93Refused++
		}
		return false
	}
	p.queued[class]++
	p.bytes[class] += charge
	if class == 0 {
		p.emitMailboxLocked(p.elapsedNow())
	}
	p.input <- protocolInput{strings.Clone(wire), s, now, class, charge}
	return true
}
func (p *protocolController) request(req protocolRequest) error {
	s := req.source
	if req.kind == "initial" || req.kind == "establish" {
		if !req.deadline.IsZero() && !time.Now().Before(req.deadline) {
			return context.DeadlineExceeded
		}
		if s == nil {
			return fmt.Errorf("missing handshake session")
		}
		if s.ctx != nil && s.ctx.Err() != nil {
			return s.ctx.Err()
		}
	}
	switch req.kind {
	case "qualification":
		return p.handleQualificationRequest(req.qualification)
	case "ready":
		p.tick(time.Now())
		return nil
	case "initial":
		if p.clockGate || p.capacityGate || !p.manager.retrySessionAllowed(s) {
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
		if err := p.nonMembershipCapacity(1); err != nil {
			return err
		}
		remote := p.remoteEntry(s)
		if remote.Call == "" || !s.remotePublicationMetadataOK() {
			return fmt.Errorf("remote peer identity unavailable")
		}
		if !candidate.initialASent {
			if err := p.sendRecordBefore([]*session{s}, "A", []PC92Entry{remote}, req.deadline); err != nil {
				return err
			}
			if s.ctx != nil && s.ctx.Err() != nil {
				return s.ctx.Err()
			}
			candidate.initialASent = true
		}
		if err := p.nonMembershipCapacity(1); err != nil {
			return err
		}
		return p.sendRecordBefore([]*session{s}, "K", nil, req.deadline)
	case "establish":
		if s.pc9x && (p.clockGate || p.capacityGate || !p.manager.retrySessionAllowed(s)) {
			return fmt.Errorf("PC9x establishment gated")
		}
		p.manager.mu.RLock()
		candidate := p.manager.candidates.Value(s)
		p.manager.mu.RUnlock()
		if candidate == nil {
			return fmt.Errorf("unowned handshake")
		}
		if err := p.manager.registerSessionAttempt(s, req.attempt, req.done); err != nil {
			return err
		}
		return p.beginReplay(s, req)
	case "closed":
		p.finishReplay(s, context.Canceled)
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
	out := newFixedIndex[string, bool](m.cfg.MaxPeers)
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
