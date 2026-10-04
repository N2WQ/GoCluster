package peer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/config"
	"dxcluster/internal/netutil"
	"dxcluster/internal/peerdiag"
	"dxcluster/spot"
	"dxcluster/strutil"
)

type ConnectionEvent struct {
	Direction string
	Action    string
	Peer      string
	Endpoint  string
	Reason    string
}

type Manager struct {
	// Mutable ownership indexes use fixed bucket backing under mu. Pending/owner
	// reservations bound candidates/runs to128/(N+128); registry admission
	// bounds sessions to configured N<=64. Retry keys belong to those identities.
	parseBudget         *frameParseBudget
	protocolStats       atomic.Pointer[ProtocolStats]
	admissionFailures   *boundedIndex[string, admissionFailure]
	admissionGeneration uint64           // mu owns failure episode identity, including undrained failures
	retry               retryCoordinator // mu owns shared directional retry history
	pendingSlots        chan struct{}
	ownerSlots          chan struct{}
	contextOwners       []contextOwner
	projectionContexts  [2]contextOwner
	ownedRuns           *boundedIndex[*session, bool]
	cfg                 config.PeeringConfig
	localCall           string
	ingest              chan<- *spot.Spot
	maxAgeSeconds       int
	topology            *topologyStore
	sessions            *boundedIndex[string, *session]
	outboundPeers       []PeerEndpoint
	inboundPeers        map[string]PeerEndpoint
	mu                  sync.RWMutex
	allowIPs            []*net.IPNet
	allowCalls          map[string]struct{}
	dedupe              *dedupeCache
	ctx                 context.Context
	cancel              context.CancelFunc
	listener            net.Listener

	legacyCh                   chan legacyWork
	rawBroadcast               func(string) // optional hook to emit raw lines (e.g., PC26) to telnet clients
	wwvBroadcast               func(kind, line string)
	announceBroadcast          func(line string)
	directMessage              func(to, line string)
	reconnects                 atomic.Uint64
	diagnostics                *peerdiag.Service
	protocol                   *protocolController
	bulletinDedupe             *dedupeCache
	candidates                 *boundedIndex[*session, *candidateState]
	stagedRecords, stagedBytes int
	blockedPeers               *boundedIndex[string, bool]
	membershipFn               func() LocalMembership
	currentDirect              func(string, uint64, uint64, string) bool
	pc18Banner                 string
	pc9xGated                  atomic.Bool
	stopping                   bool
	wg                         sync.WaitGroup
	stopOnce                   sync.Once
}

// legacyWork wraps legacy topology frames so disk I/O never blocks the read loop.
type legacyWork struct {
	frame *Frame
	ts    time.Time
}

const (
	defaultLegacyQueue = 64
)

func buildPeerRegistry(peers []config.PeeringPeer) ([]PeerEndpoint, map[string]PeerEndpoint, error) {
	outbound := make([]PeerEndpoint, 0, len(peers))
	inbound := make(map[string]PeerEndpoint)
	for i := range peers {
		peerCfg := &peers[i]
		if !peerCfg.Enabled {
			continue
		}
		endpoint, err := newPeerEndpoint(*peerCfg)
		if err != nil {
			return nil, nil, err
		}
		if peerCfg.AllowsOutbound() {
			outbound = append(outbound, endpoint)
		}
		if peerCfg.AllowsInbound() {
			inbound[endpoint.remoteCall] = endpoint
		}
	}
	return outbound, inbound, nil
}

func NewManager(cfg config.PeeringConfig, localCall string, ingest chan<- *spot.Spot, maxAgeSeconds int, diagnostics *peerdiag.Service) (*Manager, error) {
	var err error
	cfg, localCall, err = config.NormalizeActivePeeringWireContract(cfg, localCall)
	if err != nil {
		return nil, err
	}
	retention := time.Duration(cfg.Topology.RetentionHours) * time.Hour
	if retention <= 0 {
		retention = 24 * time.Hour
	}
	allowIPs, err := parseIPACL(cfg.ACL.AllowIPs)
	if err != nil {
		return nil, err
	}
	outboundPeers, inboundPeers, err := buildPeerRegistry(cfg.Peers)
	if err != nil {
		return nil, err
	}
	var topo *topologyStore
	if strings.TrimSpace(cfg.Topology.DBPath) != "" {
		topo, err = openTopologyStore(cfg.Topology.DBPath, retention)
		if err != nil {
			return nil, err
		}
	}
	allowCalls := make(map[string]struct{})
	for _, call := range cfg.ACL.AllowCallsigns {
		call = strutil.NormalizeUpper(call)
		if call == "" {
			continue
		}
		allowCalls[call] = struct{}{}
	}

	m := &Manager{
		cfg:            cfg,
		localCall:      strutil.NormalizeUpper(localCall),
		ingest:         ingest,
		maxAgeSeconds:  maxAgeSeconds,
		topology:       topo,
		sessions:       newFixedIndex[string, *session](cfg.MaxPeers),
		outboundPeers:  outboundPeers,
		inboundPeers:   inboundPeers,
		allowIPs:       allowIPs,
		allowCalls:     allowCalls,
		dedupe:         newDedupeCache(10 * time.Minute),
		bulletinDedupe: newBoundedDedupe(10*time.Minute, 8192, 2<<20),
		candidates:     newFixedIndex[*session, *candidateState](128),
		blockedPeers:   newFixedIndex[string, bool](cfg.MaxPeers),
		diagnostics:    diagnostics,
	}
	m.admissionFailures = newFixedIndex[string, admissionFailure](cfg.MaxPeers)
	m.pendingSlots = make(chan struct{}, 128)
	m.ownerSlots = make(chan struct{}, cfg.MaxPeers+128)
	m.ownedRuns = newFixedIndex[*session, bool](cfg.MaxPeers + 128)
	m.parseBudget = newFrameParseBudget()
	m.protocol = newProtocolController(m)
	return m, nil
}

// Start creates all state owners before listeners or dialers can deliver traffic.
func (m *Manager) Start(ctx context.Context) error {
	if m == nil {
		return fmt.Errorf("nil manager")
	}
	m.mu.Lock()
	if m.ctx != nil || m.stopping {
		m.mu.Unlock()
		return fmt.Errorf("peer manager already started or stopped")
	}
	startCtx, cancel := context.WithCancel(ctx)
	m.ctx, m.cancel = startCtx, cancel
	m.initializeContextOwnersLocked()
	m.mu.Unlock()
	started := false
	defer func() {
		if !started {
			cancel()
		}
	}()
	if err := m.protocol.waitStartupSecond(startCtx); err != nil {
		m.Stop()
		return err
	}
	m.wg.Add(1)
	go func() { defer m.wg.Done(); m.protocol.run(m.ctx) }()
	if m.topology != nil {
		m.wg.Add(1)
		go func() { defer m.wg.Done(); m.projectionLoop(m.projectionContexts[0].parent) }()
		m.legacyCh = make(chan legacyWork, defaultLegacyQueue)
		m.wg.Add(1)
		go func() { defer m.wg.Done(); m.legacyWorker(m.projectionContexts[1].parent) }()
	}
	if err := m.protocolCall("ready", nil); err != nil {
		m.Stop()
		return err
	}
	if m.cfg.ListenPort > 0 {
		var lc net.ListenConfig
		ln, err := lc.Listen(startCtx, "tcp", fmt.Sprintf(":%d", m.cfg.ListenPort))
		if err != nil {
			m.Stop()
			return fmt.Errorf("peering listen: %w", err)
		}
		m.listener = ln
		m.wg.Add(1)
		go func() { defer m.wg.Done(); m.acceptLoop() }()
	}
	for _, endpoint := range m.outboundPeers {
		m.wg.Add(1)
		go func(ep PeerEndpoint) { defer m.wg.Done(); m.runOutbound(ep) }(endpoint)
	}
	started = true
	return nil
}

// Stop is also valid after a construction/startup failure. Withdrawal gets at
// most two seconds while transports live; cancellation then joins every owner
// before closing optional storage. It never holds the registry lock while joining.
func (m *Manager) Stop() {
	if m == nil {
		return
	}
	m.stopOnce.Do(func() {
		m.mu.Lock()
		m.stopping = true
		m.mu.Unlock()
		if m.listener != nil {
			_ = m.listener.Close()
		}
		if m.ctx != nil && m.ctx.Err() == nil {
			req := protocolRequest{kind: "withdraw", done: make(chan error, 1)}
			withdrawDeadline := time.Now().Add(2 * time.Second)
			timer := time.NewTimer(2 * time.Second)
			select {
			case m.protocol.lifecycle <- req:
				select {
				case <-req.done:
				case <-timer.C:
				case <-m.ctx.Done():
				}
			case <-timer.C:
			case <-m.ctx.Done():
			}
			timer.Stop()
			m.drainControl(time.Until(withdrawDeadline))
		}
		if m.cancel != nil {
			m.cancel()
		}
		m.mu.RLock()
		all := make([]*session, 0, m.ownedRuns.Len()+m.sessions.Len())
		for s := range m.ownedRuns.All() {
			all = append(all, s)
		}
		for _, s := range m.sessions.All() {
			if !slices.Contains(all, s) {
				all = append(all, s)
			}
		}
		m.mu.RUnlock()
		for _, s := range all {
			s.close()
		}
		m.wg.Wait()
		m.stopContextOwners()
		m.diagnostics.Stop()
		if m.protocol != nil {
			m.protocol.drainQueuedProjections()
		}
		if m.topology != nil {
			_ = m.topology.Close()
		}
	})
}

// PublishDX publishes a locally produced spot to peers when the shared
// forwarding policy allows it. Receive-only mode still permits DX command
// spots while suppressing transit relay.
func (m *Manager) PublishDX(s *spot.Spot) bool {
	if s == nil {
		return false
	}
	return m.PublishDXWithComment(s, s.Comment)
}

// PublishDXWithComment publishes a locally produced spot to peers using the
// provided comment text for PC11/PC61 formatting.
func (m *Manager) PublishDXWithComment(s *spot.Spot, comment string) bool {
	if m == nil || s == nil || !m.ShouldPublishLocalSpot(s) {
		return false
	}
	hop := m.cfg.HopCount
	if hop <= 0 {
		hop = defaultHopCount
	}
	m.broadcastSpot(s, comment, hop, m.localCall, nil)
	return true
}

func (m *Manager) PublishWWV(ev WWVEvent) {
	if m == nil {
		return
	}
	m.broadcastWWV(ev)
}

func (m *Manager) HandleFrame(frame *Frame, sess *session) {
	if frame == nil {
		return
	}
	now := time.Now()
	switch frame.Type {
	case "PC92":
		if sess == nil || !sess.pc9x || frame.Hop == 0 {
			return
		}
		if m.cfg.PC92MaxBytes > 0 && len(frame.Raw) > m.cfg.PC92MaxBytes {
			return
		}
		if m.protocol == nil || !m.protocol.enqueue(frame, sess, now) {
			// Only valid authority records warrant closing and gating a link.
			// The normal path decodes on the owner; full-mailbox rejection is rare.
			if _, eligible := eligiblePC92Record(frame, sess, m.localCall); eligible {
				m.recordAdmissionFailure(sess, frame, now)
			}
		}
	case "PC19", "PC16", "PC17", "PC21":
		if m.topology != nil && m.legacyCh != nil {
			select {
			case m.legacyCh <- legacyWork{frame: &Frame{Type: strings.Clone(frame.Type)}, ts: now}:
			default:
				m.reportDiagnostic("legacy_queue_full", sessionLabel(sess), frame.Type)
			}
		}
	case "PC26", "PC11", "PC61":
		m.handlePeerSpot(frame, sess, now)
	case "PC23", "PC73":
		if ev, ok := parseWWV(frame); ok {
			if m.bulletinDedupe.markSeen(wwvKey(frame), now) {
				m.broadcastWWV(ev)
			}
		}
	case "PC93":
		if sess == nil || !sess.pc9x || frame.Hop == 0 {
			return
		}
		if _, ok := parsePC93(frame); ok && m.protocol != nil {
			m.protocol.enqueue(frame, sess, now)
		}
	}
}

func (m *Manager) ingestSpot(s *spot.Spot) bool {
	if m == nil || s == nil || m.ingest == nil {
		return false
	}
	if m.maxAgeSeconds > 0 {
		if age := time.Since(s.Time); age > time.Duration(m.maxAgeSeconds)*time.Second {
			// Drop stale spots before they enter the shared pipeline to avoid wasting dedupe/work.
			return false
		}
	}
	select {
	case m.ingest <- s:
		return true
	default:
		m.reportDiagnostic("ingest_queue_full", s.SourceNode, "spot_dropped")
		return false
	}
}

// reportBadCallParseDrop emits fixed metadata; detailed records are owned by
// the isolated peer diagnostic process and never call shared UI/log callbacks.
func (m *Manager) reportBadCallParseDrop(frame *Frame, sess *session, err error) {
	if m == nil || m.diagnostics == nil || frame == nil || err == nil {
		return
	}
	fields := frame.payloadFields()
	dx, de := "", ""
	if len(fields) > 1 {
		dx = fields[1]
	}
	if len(fields) > 5 {
		de = fields[5]
	}
	m.diagnostics.Emit(peerdiag.Diagnostic, peerdiag.Fields{Action: "parse_rejected", Peer: sessionLabel(sess), Reason: frame.Type, DX: dx, DE: de})
}

func (m *Manager) registerSession(s *session) error {
	return m.registerSessionAttempt(s, nil, nil)
}

func (m *Manager) registerSessionAttempt(s *session, attempt *establishmentAttempt, replayDone chan error) error {
	if m == nil || s == nil {
		return nil
	}
	key := strings.TrimSpace(s.id)
	if key == "" {
		key = strings.TrimSpace(s.remoteCall)
	}
	if key == "" {
		return fmt.Errorf("peer session identity is empty")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.sessions.Get(key); ok && existing != s {
		return fmt.Errorf("duplicate peer session: %s", key)
	}
	if m.stopping || m.sessions.Len() >= m.cfg.MaxPeers {
		return fmt.Errorf("established peer capacity or stopping")
	}
	if s.ctx != nil && s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	if s.pc9x && !m.retrySessionAllowedLocked(s) {
		return fmt.Errorf("PC9x establishment gated")
	}
	if !attempt.commit() {
		return context.DeadlineExceeded
	}
	if err := s.activateNormalQueue(); err != nil {
		return err
	}
	s.id = key
	m.sessions.Set(key, s)
	if c := m.candidates.Value(s); c != nil && len(c.staged) != 0 {
		// Publish the terminal ownership fence before dropping manager.mu.
		// Even cancellation between registration and replay transfer cannot
		// retire this transport while the controller retains its staged batch.
		s.replayReady = replayDone
	}
	return nil
}

func (m *Manager) unregisterSession(s *session) {
	if m == nil || s == nil {
		return
	}
	if m.ctx != nil && m.ctx.Err() == nil {
		if err := m.protocolCall("closed", s); err == nil {
			return
		}
	}
	// The owner is absent only before Start or after cancellation. No new live
	// authority can then be established; release registry ownership directly.
	m.mu.Lock()
	if m.sessions.Value(s.id) == s {
		m.sessions.Delete(s.id)
	}
	m.mu.Unlock()
}

// SetRawBroadcast installs a callback used to forward raw lines (e.g., PC26) to telnet clients.
// This is optional; when unset, PC26 is only forwarded to peers.
func (m *Manager) SetRawBroadcast(fn func(string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rawBroadcast = fn
}

func (m *Manager) reportConnection(ev ConnectionEvent) {
	if m == nil || m.diagnostics == nil {
		return
	}
	m.diagnostics.Emit(peerdiag.Connection, peerdiag.Fields{Direction: ev.Direction, Action: ev.Action, Peer: ev.Peer, Endpoint: ev.Endpoint, Reason: ev.Reason})
}

// reportDiagnostic accepts only borrowed primitive values. The mailbox reserves its
// fixed record before sanitizing/copying; no actor formats an arbitrary error.
func (m *Manager) reportDiagnostic(action, peer, reason string) {
	if m == nil || m.diagnostics == nil {
		return
	}
	m.diagnostics.Emit(peerdiag.Diagnostic, peerdiag.Fields{Action: action, Peer: peer, Reason: reason})
}

func (m *Manager) DiagnosticStats() peerdiag.Stats {
	if m == nil || m.diagnostics == nil {
		return peerdiag.Stats{Disabled: true}
	}
	return m.diagnostics.Snapshot()
}

// SetWWVBroadcast installs a callback used to forward WWV/WCY bulletins to telnet clients.
// When unset, PC23/PC73 frames are parsed but not delivered.
func (m *Manager) SetWWVBroadcast(fn func(kind, line string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.wwvBroadcast = fn
}

// SetAnnouncementBroadcast installs a callback used for PC93 announcements ("To ALL").
func (m *Manager) SetAnnouncementBroadcast(fn func(line string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.announceBroadcast = fn
}

// SetDirectMessage installs a callback for PC93 talk messages addressed to a specific callsign.
func (m *Manager) SetDirectMessage(fn func(to, line string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.directMessage = fn
}

func (m *Manager) broadcastSpot(s *spot.Spot, comment string, hop int, origin string, exclude *session) {
	if m == nil || s == nil {
		return
	}
	if m.maxAgeSeconds > 0 {
		if age := time.Since(s.Time); age > time.Duration(m.maxAgeSeconds)*time.Second {
			// Belt-and-suspenders: never forward stale spots to peers.
			return
		}
	}
	if strings.TrimSpace(origin) == "" {
		origin = m.localCall
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, sess := range m.sessions.All() {
		if exclude != nil && sess == exclude {
			continue
		}
		if sess.pc9x {
			line := formatPC61(s, comment, origin, hop)
			m.trySendLine(sess, line, "spot")
		} else {
			line := formatPC11(s, comment, origin, hop)
			m.trySendLine(sess, line, "spot")
		}
	}
}

func (m *Manager) broadcastWWV(ev WWVEvent) {
	if m == nil {
		return
	}
	kind, line := formatWWVLine(ev)
	if line == "" {
		return
	}
	m.mu.RLock()
	cb := m.wwvBroadcast
	m.mu.RUnlock()
	if cb != nil {
		cb(kind, line)
	}
}

func (m *Manager) routePC93(msg pc93Message) {
	if m == nil {
		return
	}
	line := formatPC93Line(msg)
	if line == "" {
		return
	}
	target, broadcast := pc93Target(msg)
	if !broadcast && target == "" {
		return
	}
	m.mu.RLock()
	announce := m.announceBroadcast
	direct := m.directMessage
	currentDirect := m.currentDirect
	m.mu.RUnlock()
	if !broadcast && target != "" {
		if currentDirect != nil {
			snapshot := m.membership()
			if snapshot.Complete && snapshot.RawCount <= 1000 && len(snapshot.Users) <= 1000 {
				var match LocalUser
				count := 0
				for _, user := range snapshot.Users {
					call, ok := CanonicalPC92Call(user.Login)
					if ok && len(call) <= 15 && call == target {
						match = user
						count++
					}
				}
				reserved := m.protocol.reservedNodes()
				if count == 1 && reserved != nil && !reserved.Value(target) {
					currentDirect(match.Login, match.SessionID, snapshot.Revision, line)
				}
			}
		} else if direct != nil {
			direct(target, line)
		}
		return
	}
	if announce != nil {
		announce(line)
	}
}

func (m *Manager) forwardFrame(frame *Frame, hop int, exclude *session, pc9xOnly bool) {
	if m == nil || frame == nil {
		return
	}
	line := frame.Encode(hop)
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, sess := range m.sessions.All() {
		if exclude != nil && sess == exclude {
			continue
		}
		if pc9xOnly && (!sess.pc9x || (frame.Type == "PC92" && sess.peer.family == config.PeeringPeerFamilyCCluster)) {
			continue
		}
		if frame.Type == "PC92" {
			if err := sess.sendControlLine(line); err != nil {
				sess.close()
			}
		} else {
			m.trySendLine(sess, line, "frame")
		}
	}
}

func (m *Manager) trySendLine(sess *session, line string, kind string) {
	if m == nil || sess == nil || strings.TrimSpace(line) == "" {
		return
	}
	if err := sess.sendLine(line); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, errSessionWriteQueueFull) {
			return
		}
		m.reportDiagnostic("enqueue_failed", sessionLabel(sess), kind)
	}
}

func remoteAddrIP(addr net.Addr) net.IP {
	if addr == nil {
		return nil
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		host = addr.String()
	}
	return net.ParseIP(host)
}

func ipAllowed(blocks []*net.IPNet, addr net.Addr) bool {
	if len(blocks) == 0 {
		return true
	}
	ip := remoteAddrIP(addr)
	if ip == nil {
		return false
	}
	for _, block := range blocks {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

func (m *Manager) hasActiveSession(id string) bool {
	if m == nil || strings.TrimSpace(id) == "" {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.sessions.Get(id)
	return ok
}

func (m *Manager) authorizeInbound(call string, addr net.Addr) (PeerEndpoint, error) {
	call = strutil.NormalizeUpper(call)
	if len(m.allowCalls) > 0 {
		if _, ok := m.allowCalls[call]; !ok {
			return PeerEndpoint{}, fmt.Errorf("unauthorized inbound peer: %s", call)
		}
	}
	if !ipAllowed(m.allowIPs, addr) {
		return PeerEndpoint{}, fmt.Errorf("unauthorized inbound peer ip: %s", addr.String())
	}
	peer, ok := m.inboundPeers[call]
	if !ok {
		return PeerEndpoint{}, fmt.Errorf("unauthorized inbound peer: %s", call)
	}
	if !ipAllowed(peer.allowIPs, addr) {
		return PeerEndpoint{}, fmt.Errorf("unauthorized inbound peer ip: %s", addr.String())
	}
	if m.hasActiveSession(peer.ID()) || (peer.preferPC9x && m.pc9xGated.Load()) {
		return PeerEndpoint{}, fmt.Errorf("duplicate peer session: %s", call)
	}
	if strings.TrimSpace(peer.host) == "" {
		peer.host = addr.String()
	}
	return peer, nil
}

func (m *Manager) acceptLoop() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || (m.ctx != nil && m.ctx.Err() != nil) {
				return
			}
			m.reportDiagnostic("accept_failed", "", "network_error")
			continue
		}
		if isTCP, enableErr, periodErr := netutil.EnableTCPKeepAlive(conn, 2*time.Minute); isTCP {
			if enableErr != nil {
				m.reportDiagnostic("keepalive_enable_failed", "", "network_error")
			}
			if periodErr != nil {
				m.reportDiagnostic("keepalive_period_failed", "", "network_error")
			}
		}
		if !m.reserveCandidateSlots() {
			_ = conn.Close()
			continue
		}
		operation := m.beginContextOperation()
		if operation == nil {
			m.releaseUnstartedCandidateSlots()
			_ = conn.Close()
			continue
		}
		peer := PeerEndpoint{host: conn.RemoteAddr().String(), port: 0}
		m.reportConnection(ConnectionEvent{Direction: "inbound", Action: "accepted", Endpoint: conn.RemoteAddr().String(), Reason: "none"})
		settings := m.sessionSettings(peer)
		sess := newSession(conn, dirInbound, m, peer, settings)
		sess.operation = operation
		sess.pendingReserved = true
		sess.ownerReserved = true
		sess.id = conn.RemoteAddr().String()
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			if err := sess.Run(); err != nil && m.ctx.Err() == nil {
				m.reportDiagnostic("inbound_session_ended", "", "session_error")
			}
		}()
	}
}

func (m *Manager) runOutbound(peer PeerEndpoint) {
	// Constructor validation bounds positive integers before conversion. Clamp
	// nonpositive sentinels first as well: an extreme negative integer could
	// otherwise overflow time.Duration into a spurious positive delay.
	baseMS, maxMS := max(0, m.cfg.Backoff.BaseMS), max(0, m.cfg.Backoff.MaxMS)
	backoff := newBackoff(time.Duration(baseMS)*time.Millisecond, time.Duration(maxMS)*time.Millisecond)
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 2 * time.Minute, // OS-level keepalive for peer links
	}
	for {
		if m.ctx != nil && m.ctx.Err() != nil {
			return
		}
		if m.outboundGated(peer) {
			if !waitPeerRetry(m.ctx, time.Second) {
				return
			}
			continue
		}
		if m.hasActiveSession(peer.ID()) {
			delay := time.Duration(baseMS) * time.Millisecond
			if delay <= 0 {
				delay = 2 * time.Second
			}
			if !waitPeerRetry(m.ctx, delay) {
				return
			}
			continue
		}
		addr := net.JoinHostPort(peer.host, strconv.Itoa(peer.port))
		if !m.reserveCandidateSlots() {
			if !waitPeerRetry(m.ctx, time.Second) {
				return
			}
			continue
		}
		generation, admitted := m.reserveRetryDial(peer.remoteCall, time.Now())
		if !admitted {
			m.releaseUnstartedCandidateSlots()
			if !waitPeerRetry(m.ctx, time.Second) {
				return
			}
			continue
		}
		operation := m.beginContextOperation()
		if operation == nil {
			m.releaseUnstartedCandidateSlots()
			m.finishRetryDial(peer.remoteCall, generation, nil, time.Now())
			return
		}
		m.reportDiagnostic("dialing", peer.remoteCall, "outbound")
		conn, err := dialer.DialContext(operation.ctx, "tcp", addr)
		if err != nil {
			m.endContextOperation(operation)
			m.releaseUnstartedCandidateSlots()
			if generation != 0 {
				m.finishRetryDial(peer.remoteCall, generation, nil, time.Now())
				m.NotifyMembershipChanged()
				m.reportConnection(ConnectionEvent{Direction: "outbound", Action: "dial_failed", Peer: peer.remoteCall, Endpoint: addr, Reason: "dial_error"})
				continue
			}
			delay := backoff.Next()
			m.reportConnection(ConnectionEvent{Direction: "outbound", Action: "dial_failed", Peer: peer.remoteCall, Endpoint: addr, Reason: "dial_error"})
			m.reportDiagnostic("dial_failed", peer.remoteCall, "network_error")
			if !waitPeerRetry(m.ctx, delay) {
				return
			}
			continue
		}
		m.reportDiagnostic("connected", peer.remoteCall, "outbound")
		m.reportConnection(ConnectionEvent{Direction: "outbound", Action: "connected", Peer: peer.remoteCall, Endpoint: addr, Reason: "none"})
		settings := m.sessionSettings(peer)
		sess := newSession(conn, dirOutbound, m, peer, settings)
		sess.operation = operation
		if !m.finishRetryDial(peer.remoteCall, generation, sess, time.Now()) {
			sess.close()
			m.endContextOperation(operation)
			m.releaseUnstartedCandidateSlots()
			continue
		}
		sess.pendingReserved = true
		sess.ownerReserved = true
		sess.remoteCall = peer.remoteCall
		if strings.TrimSpace(sess.remoteCall) == "" {
			sess.remoteCall = "*"
		}
		if err := sess.Run(); err != nil && m.ctx.Err() == nil {
			m.reportDiagnostic("outbound_session_ended", peer.remoteCall, "session_error")
		}
		m.mu.RLock()
		retrying := m.blockedPeers.Value(peer.remoteCall)
		m.mu.RUnlock()
		if retrying {
			m.reconnects.Add(1)
			continue // shared retry history supplies the sole overload delay
		}
		if sess.established {
			backoff.Reset()
		}
		m.reconnects.Add(1)
		delay := backoff.Next()
		if !waitPeerRetry(m.ctx, delay) {
			return
		}
	}
}

// ReconnectCount returns the number of outbound peer reconnect attempts.
func (m *Manager) ReconnectCount() uint64 {
	if m == nil {
		return 0
	}
	return m.reconnects.Load()
}

// ActiveSessionCount returns the number of active peer sessions.
// Purpose: Provide liveness info for dashboards.
// Key aspects: Safe with nil manager; uses read lock.
// Upstream: main stats loop.
// Downstream: none.
func (m *Manager) ActiveSessionCount() int {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions.Len()
}

// ActiveSessionSSIDs returns active peer callsigns (including SSID suffixes) for dashboard display.
// Purpose: Expose operator-friendly peer identity instead of aggregate counts.
// Key aspects: Safe with nil manager, read-locked snapshot, sorted and de-duplicated.
// Upstream: main stats loop.
// Downstream: overview ingest sources box.
func (m *Manager) ActiveSessionSSIDs() []string {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.sessions.Len() == 0 {
		return nil
	}
	ssids := make([]string, 0, m.sessions.Len())
	for _, sess := range m.sessions.All() {
		if sess == nil {
			continue
		}
		label := strutil.NormalizeUpper(sess.remoteCall)
		if label == "" || label == "*" {
			continue
		}
		if slices.Contains(ssids, label) {
			continue
		}
		ssids = append(ssids, label)
	}
	sort.Strings(ssids)
	return ssids
}

func (m *Manager) resolveLocalCall(peer PeerEndpoint) string {
	local := peer.loginCall
	if local == "" {
		local = m.cfg.LocalCallsign
	}
	if local == "" {
		local = m.localCall
	}
	return strutil.NormalizeUpper(local)
}

func (m *Manager) sessionSettings(peer PeerEndpoint) sessionSettings {
	return sessionSettings{
		localCall:       m.resolveLocalCall(peer),
		preferPC9x:      peer.preferPC9x,
		nodeVersion:     m.cfg.NodeVersion,
		nodeBuild:       m.cfg.NodeBuild,
		legacyVersion:   m.cfg.LegacyVersion,
		pc92Bitmap:      m.cfg.PC92Bitmap,
		nodeCount:       m.cfg.NodeCount,
		userCount:       m.cfg.UserCount,
		hopCount:        m.cfg.HopCount,
		telnetTransport: m.cfg.TelnetTransport,
		loginTimeout:    time.Duration(m.cfg.Timeouts.LoginSeconds) * time.Second,
		initTimeout:     time.Duration(m.cfg.Timeouts.InitSeconds) * time.Second,
		idleTimeout:     time.Duration(m.cfg.Timeouts.IdleSeconds) * time.Second,
		keepalive:       time.Duration(m.cfg.KeepaliveSeconds) * time.Second,
		configEvery:     time.Duration(m.cfg.ConfigSeconds) * time.Second,
		writeQueue:      m.cfg.WriteQueueSize,
		maxLine:         m.cfg.MaxLineLength,
		pc92MaxBytes:    m.cfg.PC92MaxBytes,
		password:        peer.password,
		logKeepalive:    m.cfg.LogKeepalive,
		logLineTooLong:  m.cfg.LogLineTooLong,
	}
}

// legacyWorker applies legacy topology frames off the socket read goroutine so
// synchronous SQLite calls never delay keepalive handling.
func (m *Manager) legacyWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case work := <-m.legacyCh:
			if m.topology == nil || work.frame == nil {
				continue
			}
			if err := m.topology.applyLegacy(ctx, work.frame, work.ts); err != nil && ctx.Err() == nil {
				m.reportDiagnostic("legacy_projection_failed", "", "storage_error")
			}
		}
	}
}

func sessionLabel(s *session) string {
	if s == nil {
		return ""
	}
	if s.diagnosticLabel != "" {
		return s.diagnosticLabel
	}
	return s.id
}

func waitPeerRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func (m *Manager) outboundGated(endpoint PeerEndpoint) bool {
	if endpoint.preferPC9x && m.pc9xGated.Load() {
		return true
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	r := m.retryIdentityLocked(endpoint.remoteCall)
	return r != nil && r.active && (r.owner != nil || r.dialing || r.invalidating || time.Now().Before(r.due))
}
