package peer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/config"
	"dxcluster/internal/netutil"
	"dxcluster/spot"
	"dxcluster/strutil"
)

type Manager struct {
	cfg               config.PeeringConfig
	localCall         string
	ingest            chan<- *spot.Spot
	maxAgeSeconds     int
	topology          *topologyStore
	sessions          map[string]*session
	mu                sync.RWMutex
	allowIPs          []*net.IPNet
	allowCalls        map[string]struct{}
	dedupe            *dedupeCache
	ctx               context.Context
	cancel            context.CancelFunc
	listener          net.Listener
	pc92Ch            chan pc92Work
	legacyCh          chan legacyWork
	rawBroadcast      func(string) // optional hook to emit raw lines (e.g., PC26) to telnet clients
	wwvBroadcast      func(kind, line string)
	announceBroadcast func(line string)
	directMessage     func(to, line string)
	reconnects        atomic.Uint64
	userCountFn       func() int
	dropReporter      func(line string)
}

// pc92Work wraps an inbound PC92 frame with the time it was observed so topology
// updates can be applied off the socket read goroutine.
type pc92Work struct {
	frame *Frame
	ts    time.Time
}

// legacyWork wraps legacy topology frames so disk I/O never blocks the read loop.
type legacyWork struct {
	frame *Frame
	ts    time.Time
}

const (
	defaultPC92Queue   = 64
	defaultLegacyQueue = 64
)

func NewManager(cfg config.PeeringConfig, localCall string, ingest chan<- *spot.Spot, maxAgeSeconds int, dropReporter func(string)) (*Manager, error) {
	if strings.TrimSpace(localCall) == "" {
		return nil, fmt.Errorf("peering local callsign is empty")
	}
	retention := time.Duration(cfg.Topology.RetentionHours) * time.Hour
	if retention <= 0 {
		retention = 24 * time.Hour
	}
	var topo *topologyStore
	var err error
	if strings.TrimSpace(cfg.Topology.DBPath) != "" {
		topo, err = openTopologyStore(cfg.Topology.DBPath, retention)
		if err != nil {
			return nil, err
		}
	}
	allowIPs, err := parseIPACL(cfg.ACL.AllowIPs)
	if err != nil {
		return nil, err
	}
	allowCalls := make(map[string]struct{})
	for _, call := range cfg.ACL.AllowCallsigns {
		call = strutil.NormalizeUpper(call)
		if call == "" {
			continue
		}
		allowCalls[call] = struct{}{}
	}

	return &Manager{
		cfg:           cfg,
		localCall:     strutil.NormalizeUpper(localCall),
		ingest:        ingest,
		maxAgeSeconds: maxAgeSeconds,
		topology:      topo,
		sessions:      make(map[string]*session),
		allowIPs:      allowIPs,
		allowCalls:    allowCalls,
		dedupe:        newDedupeCache(10 * time.Minute),
		dropReporter:  dropReporter,
	}, nil
}

func (m *Manager) Start(ctx context.Context) error {
	if m == nil {
		return fmt.Errorf("nil manager")
	}
	runCtx, cancel := context.WithCancel(ctx)
	m.ctx = runCtx
	m.cancel = cancel

	if m.cfg.ListenPort > 0 {
		addr := fmt.Sprintf(":%d", m.cfg.ListenPort)
		var lc net.ListenConfig
		ln, err := lc.Listen(ctx, "tcp", addr)
		if err != nil {
			return fmt.Errorf("peering listen: %w", err)
		}
		m.listener = ln
		go m.acceptLoop()
	}

	for _, peerCfg := range m.cfg.Peers {
		if !peerCfg.Enabled {
			continue
		}
		peer := newPeerEndpoint(peerCfg)
		go m.runOutbound(peer)
	}

	// Always run maintenance to prune the peer dedupe cache even when topology
	// persistence is disabled.
	go m.maintenanceLoop(runCtx)

	// Topology updates are handled off the session read goroutine to prevent
	// large PC92 maps from stalling spot delivery. The channel is deliberately
	// bounded; oversize or overflow frames are dropped with a warning.
	if m.topology != nil {
		m.pc92Ch = make(chan pc92Work, defaultPC92Queue)
		go m.topologyWorker(runCtx)
		m.legacyCh = make(chan legacyWork, defaultLegacyQueue)
		go m.legacyWorker(runCtx)
	}
	return nil
}

func (m *Manager) Stop() {
	if m == nil {
		return
	}
	if m.cancel != nil {
		m.cancel()
	}
	if m.listener != nil {
		_ = m.listener.Close()
	}
	m.mu.Lock()
	for _, sess := range m.sessions {
		sess.close()
	}
	m.mu.Unlock()
	if m.topology != nil {
		_ = m.topology.Close()
	}
}

func (m *Manager) PublishDX(s *spot.Spot) {
	if m == nil || s == nil {
		return
	}
	hop := m.cfg.HopCount
	if hop <= 0 {
		hop = defaultHopCount
	}
	m.broadcastSpot(s, hop, m.localCall, nil)
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
	now := time.Now().UTC()
	switch frame.Type {
	case "PC92":
		if m.topology != nil && m.pc92Ch != nil {
			if m.cfg.PC92MaxBytes > 0 && len(frame.Raw) > m.cfg.PC92MaxBytes {
				log.Printf("Peering: dropping PC92 (%d bytes) from %s: over size limit", len(frame.Raw), sessionLabel(sess))
			} else {
				select {
				case m.pc92Ch <- pc92Work{frame: frame, ts: now}:
				default:
					log.Printf("Peering: dropping PC92 from %s: topology queue full", sessionLabel(sess))
				}
			}
		}
		if frame.Hop > 1 && m.dedupe.markSeen(pc92Key(frame), now) {
			m.forwardFrame(frame, frame.Hop-1, sess, true)
		}
	case "PC19", "PC16", "PC17", "PC21":
		if m.topology != nil && m.legacyCh != nil {
			select {
			case m.legacyCh <- legacyWork{frame: frame, ts: now}:
			default:
				log.Printf("Peering: dropping legacy %s from %s: topology queue full", frame.Type, sessionLabel(sess))
			}
		}
	case "PC26", "PC11", "PC61":
		spotEntry, err := parseSpotFromFrame(frame, sess.remoteCall)
		if err != nil {
			if frame.Type == "PC61" {
				m.reportDrop(formatPC61DropLine(frame, sess, err))
				return
			}
			log.Printf("Peering: parse %s from %s failed: %v", frame.Type, sessionLabel(sess), err)
			return
		}
		m.ingestSpot(spotEntry)
		if frame.Hop > 1 {
			key := dxKey(frame, spotEntry)
			if m.dedupe.markSeen(key, now) {
				if frame.Type == "PC26" {
					// Preserve merge semantics by forwarding PC26; pc9x peers only. Telnet clients
					// see the formatted spot via normal broadcast after ingest.
					m.forwardFrame(frame, frame.Hop-1, sess, true)
				} else {
					m.broadcastSpot(spotEntry, frame.Hop-1, spotEntry.SourceNode, sess)
				}
			}
		}
	case "PC23", "PC73":
		if ev, ok := parseWWV(frame); ok {
			if m.dedupe.markSeen(wwvKey(frame), now) {
				m.broadcastWWV(ev)
			}
		}
	case "PC93":
		if msg, ok := parsePC93(frame); ok {
			if m.dedupe.markSeen(pc93Key(frame), now) {
				m.routePC93(msg)
			}
		}
	}
}

func (m *Manager) ingestSpot(s *spot.Spot) {
	if m == nil || s == nil || m.ingest == nil {
		return
	}
	if m.maxAgeSeconds > 0 {
		if age := time.Since(s.Time); age > time.Duration(m.maxAgeSeconds)*time.Second {
			// Drop stale spots before they enter the shared pipeline to avoid wasting dedupe/work.
			return
		}
	}
	select {
	case m.ingest <- s:
	default:
		log.Printf("Peering: ingest queue full, dropping spot from %s", s.SourceNode)
	}
}

// Purpose: Route a drop line to the UI reporter or logs.
// Key aspects: Uses the optional dropReporter to avoid system log duplication.
// Upstream: PC61 parse failures.
// Downstream: dropReporter or log.Print.
func (m *Manager) reportDrop(line string) {
	if line == "" {
		return
	}
	if m != nil && m.dropReporter != nil {
		m.dropReporter(line)
		return
	}
	log.Print(line)
}

// Purpose: Format a standardized PC61 drop line for the dropped pane.
// Key aspects: Best-effort extraction of fields; reason is stable for parsing.
// Upstream: HandleFrame parse errors.
// Downstream: spot.FreqToBand and spot.NormalizeBand.
func formatPC61DropLine(frame *Frame, sess *session, err error) string {
	reason := pc61DropReason(err)
	dx := "unknown"
	de := "unknown"
	freq := 0.0
	band := "unknown"
	source := sessionLabel(sess)
	if frame != nil {
		fields := frame.payloadFields()
		if len(fields) > 0 {
			if parsed, parseErr := strconv.ParseFloat(strings.TrimSpace(fields[0]), 64); parseErr == nil {
				freq = parsed
				band = spot.NormalizeBand(spot.FreqToBand(parsed))
				if band == "" {
					band = "unknown"
				}
			}
		}
		if len(fields) > 1 {
			dx = strutil.NormalizeUpper(fields[1])
			if dx == "" {
				dx = "unknown"
			}
		}
		if len(fields) > 5 {
			de = strutil.NormalizeUpper(fields[5])
			if de == "" {
				de = "unknown"
			}
		}
		if len(fields) > 6 {
			origin := strings.TrimSpace(fields[6])
			if origin != "" {
				source = origin
			}
		}
	}
	if source == "" {
		source = "unknown"
	}
	return fmt.Sprintf("PC61 drop: reason=%s de=%s dx=%s band=%s freq=%.1f source=%s", reason, de, dx, band, freq, source)
}

func pc61DropReason(err error) string {
	if err == nil {
		return "unknown"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "insufficient fields"):
		return "insufficient_fields"
	case strings.Contains(msg, "freq parse"):
		return "freq_parse"
	case strings.Contains(msg, "invalid dx"):
		return "invalid_dx"
	case strings.Contains(msg, "invalid de"):
		return "invalid_de"
	default:
		return "parse_error"
	}
}

func (m *Manager) registerSession(s *session) {
	if m == nil || s == nil {
		return
	}
	m.mu.Lock()
	m.sessions[s.id] = s
	m.mu.Unlock()
}

func (m *Manager) unregisterSession(s *session) {
	if m == nil || s == nil {
		return
	}
	m.mu.Lock()
	delete(m.sessions, s.id)
	m.mu.Unlock()
}

// SetRawBroadcast installs a callback used to forward raw lines (e.g., PC26) to telnet clients.
// This is optional; when unset, PC26 is only forwarded to peers.
func (m *Manager) SetRawBroadcast(fn func(string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rawBroadcast = fn
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

func (m *Manager) broadcastSpot(s *spot.Spot, hop int, origin string, exclude *session) {
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
	for _, sess := range m.sessions {
		if exclude != nil && sess == exclude {
			continue
		}
		if sess.pc9x {
			line := formatPC61(s, origin, hop)
			m.trySendLine(sess, line, "spot")
		} else {
			line := formatPC11(s, origin, hop)
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
	m.mu.RLock()
	announce := m.announceBroadcast
	direct := m.directMessage
	m.mu.RUnlock()
	if !broadcast && target != "" {
		if direct != nil {
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
	for _, sess := range m.sessions {
		if exclude != nil && sess == exclude {
			continue
		}
		if pc9xOnly && !sess.pc9x {
			continue
		}
		m.trySendLine(sess, line, "frame")
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
		log.Printf("Peering: failed to enqueue %s for %s: %v", kind, sessionLabel(sess), err)
	}
}

func (m *Manager) allowInbound(call string, addr net.Addr) bool {
	call = strutil.NormalizeUpper(call)
	if len(m.allowCalls) > 0 {
		if _, ok := m.allowCalls[call]; !ok {
			return false
		}
	}
	if len(m.allowIPs) == 0 {
		return true
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		host = addr.String()
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, block := range m.allowIPs {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

func (m *Manager) acceptLoop() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			if m.ctx != nil && m.ctx.Err() != nil {
				return
			}
			log.Printf("Peering: accept failed: %v", err)
			continue
		}
		if isTCP, enableErr, periodErr := netutil.EnableTCPKeepAlive(conn, 2*time.Minute); isTCP {
			if enableErr != nil {
				log.Printf("Peering: failed to enable keepalive for %s: %v", conn.RemoteAddr(), enableErr)
			}
			if periodErr != nil {
				log.Printf("Peering: failed to set keepalive period for %s: %v", conn.RemoteAddr(), periodErr)
			}
		}
		peer := PeerEndpoint{host: conn.RemoteAddr().String(), port: 0}
		settings := m.sessionSettings(peer)
		sess := newSession(conn, dirInbound, m, peer, settings)
		sess.id = conn.RemoteAddr().String()
		go func() {
			if err := sess.Run(m.ctx); err != nil && m.ctx.Err() == nil {
				log.Printf("Peering: inbound session ended: %v", err)
			}
		}()
	}
}

func (m *Manager) runOutbound(peer PeerEndpoint) {
	backoff := newBackoff(time.Duration(m.cfg.Backoff.BaseMS)*time.Millisecond, time.Duration(m.cfg.Backoff.MaxMS)*time.Millisecond)
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 2 * time.Minute, // OS-level keepalive for peer links
	}
	for {
		if m.ctx != nil && m.ctx.Err() != nil {
			return
		}
		addr := net.JoinHostPort(peer.host, strconv.Itoa(peer.port))
		log.Printf("Peering: dialing %s as %s", addr, peer.loginCall)
		conn, err := dialer.Dial("tcp", addr)
		if err != nil {
			delay := backoff.Next()
			log.Printf("Peering: dial %s failed: %v (retry in %s)", addr, err, delay)
			time.Sleep(delay)
			continue
		}
		log.Printf("Peering: connected to %s", addr)
		backoff.Reset()
		settings := m.sessionSettings(peer)
		sess := newSession(conn, dirOutbound, m, peer, settings)
		sess.remoteCall = peer.remoteCall
		if strings.TrimSpace(sess.remoteCall) == "" {
			sess.remoteCall = "*"
		}
		sess.id = addr
		if err := sess.Run(m.ctx); err != nil && m.ctx.Err() == nil {
			log.Printf("Peering: session to %s ended: %v", addr, err)
		}
		m.reconnects.Add(1)
		delay := backoff.Next()
		time.Sleep(delay)
	}
}

// ReconnectCount returns the number of outbound peer reconnect attempts.
func (m *Manager) ReconnectCount() uint64 {
	if m == nil {
		return 0
	}
	return m.reconnects.Load()
}

// SetUserCountProvider wires a live user count callback (e.g., from the telnet server)
// so PC92 K keepalives can advertise current users instead of a static config value.
func (m *Manager) SetUserCountProvider(fn func() int) {
	if m == nil {
		return
	}
	m.userCountFn = fn
}

// liveNodeCount returns 1 (self) plus the number of active peer sessions.
func (m *Manager) liveNodeCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return 1 + len(m.sessions)
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
	return len(m.sessions)
}

// liveUserCount returns the current telnet client count when provided, otherwise falls back to config.
func (m *Manager) liveUserCount() int {
	if m.userCountFn != nil {
		if v := m.userCountFn(); v >= 0 {
			return v
		}
	}
	return m.cfg.UserCount
}

func (m *Manager) sessionSettings(peer PeerEndpoint) sessionSettings {
	local := peer.loginCall
	if local == "" {
		local = m.cfg.LocalCallsign
	}
	if local == "" {
		local = m.localCall
	}
	return sessionSettings{
		localCall:       strutil.NormalizeUpper(local),
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

func (m *Manager) maintenanceLoop(ctx context.Context) {
	interval := time.Duration(m.cfg.Topology.PersistIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UTC()
			if m.topology != nil {
				m.topology.prune(ctx, now)
			}
			if m.dedupe != nil {
				m.dedupe.prune(now)
			}
		}
	}
}

// topologyWorker applies PC92 frames off the socket read goroutine so spot
// traffic never blocks behind topology I/O. Oversize/overflow drops happen at
// enqueue time; this worker best-effort applies what it receives.
func (m *Manager) topologyWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case work := <-m.pc92Ch:
			if m.topology == nil || work.frame == nil {
				continue
			}
			start := time.Now().UTC()
			m.topology.applyPC92Frame(ctx, work.frame, work.ts)
			if dur := time.Since(start); dur > 2*time.Second {
				log.Printf("Peering: PC92 apply slow (%s) from %s", dur.Truncate(time.Millisecond), pc92Origin(work.frame))
			}
		}
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
			m.topology.applyLegacy(ctx, work.frame, work.ts)
		}
	}
}

func pc92Origin(f *Frame) string {
	if f == nil {
		return ""
	}
	fields := f.payloadFields()
	if len(fields) > 0 {
		return strings.TrimSpace(fields[0])
	}
	return ""
}

func sessionLabel(s *session) string {
	if s == nil {
		return ""
	}
	if strings.TrimSpace(s.remoteCall) != "" {
		return s.remoteCall
	}
	return s.id
}
