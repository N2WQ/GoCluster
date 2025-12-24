package peer

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

type Manager struct {
	cfg           config.PeeringConfig
	localCall     string
	ingest        chan<- *spot.Spot
	maxAgeSeconds int
	topology      *topologyStore
	sessions      map[string]*session
	mu            sync.RWMutex
	allowIPs      []*net.IPNet
	allowCalls    map[string]struct{}
	dedupe        *dedupeCache
	ctx           context.Context
	cancel        context.CancelFunc
	listener      net.Listener
	rawBroadcast  func(string) // optional hook to emit raw lines (e.g., PC26) to telnet clients
}

func NewManager(cfg config.PeeringConfig, localCall string, ingest chan<- *spot.Spot, maxAgeSeconds int) (*Manager, error) {
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
		call = strings.ToUpper(strings.TrimSpace(call))
		if call == "" {
			continue
		}
		allowCalls[call] = struct{}{}
	}

	return &Manager{
		cfg:           cfg,
		localCall:     strings.ToUpper(strings.TrimSpace(localCall)),
		ingest:        ingest,
		maxAgeSeconds: maxAgeSeconds,
		topology:      topo,
		sessions:      make(map[string]*session),
		allowIPs:      allowIPs,
		allowCalls:    allowCalls,
		dedupe:        newDedupeCache(10 * time.Minute),
	}, nil
}

func (m *Manager) Start(ctx context.Context) error {
	if m == nil {
		return fmt.Errorf("nil manager")
	}
	m.ctx, m.cancel = context.WithCancel(ctx)

	if m.cfg.ListenPort > 0 {
		addr := fmt.Sprintf(":%d", m.cfg.ListenPort)
		ln, err := net.Listen("tcp", addr)
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

	if m.topology != nil && m.cfg.Topology.PersistIntervalSeconds > 0 {
		go m.maintenanceLoop()
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
	hop := m.cfg.HopCount
	if hop <= 0 {
		hop = defaultHopCount
	}
	m.broadcastWWV(ev, hop, nil)
}

func (m *Manager) HandleFrame(frame *Frame, sess *session) {
	if frame == nil {
		return
	}
	now := time.Now().UTC()
	switch frame.Type {
	case "PC92":
		if m.topology != nil {
			m.topology.applyPC92Frame(frame, now)
		}
		if frame.Hop > 1 && m.dedupe.markSeen(pc92Key(frame), now) {
			m.forwardFrame(frame, frame.Hop-1, sess, true)
		}
	case "PC19", "PC16", "PC17", "PC21":
		if m.topology != nil {
			m.topology.applyLegacy(frame, now)
		}
	case "PC26", "PC11", "PC61":
		spotEntry, err := parseSpotFromFrame(frame, sess.remoteCall)
		if err == nil {
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
		}
	case "PC23", "PC73":
		if ev, ok := parseWWV(frame); ok {
			if frame.Hop > 1 && m.dedupe.markSeen(wwvKey(frame), now) {
				m.broadcastWWV(ev, frame.Hop-1, sess)
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
			_ = sess.sendLine(line)
		} else {
			line := formatPC11(s, origin, hop)
			_ = sess.sendLine(line)
		}
	}
}

func (m *Manager) broadcastWWV(ev WWVEvent, hop int, exclude *session) {
	if m == nil {
		return
	}
	// WWV forwarding omitted; placeholder for future implementation.
	_ = ev
	_ = hop
	_ = exclude
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
		_ = sess.sendLine(line)
	}
}

func (m *Manager) allowInbound(call string, addr net.Addr) bool {
	call = strings.ToUpper(strings.TrimSpace(call))
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
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetKeepAlive(true)
			_ = tcp.SetKeepAlivePeriod(2 * time.Minute)
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
		addr := fmt.Sprintf("%s:%d", peer.host, peer.port)
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
		delay := backoff.Next()
		time.Sleep(delay)
	}
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
		localCall:     strings.ToUpper(strings.TrimSpace(local)),
		preferPC9x:    peer.preferPC9x,
		nodeVersion:   m.cfg.NodeVersion,
		nodeBuild:     m.cfg.NodeBuild,
		legacyVersion: m.cfg.LegacyVersion,
		pc92Bitmap:    m.cfg.PC92Bitmap,
		nodeCount:     m.cfg.NodeCount,
		userCount:     m.cfg.UserCount,
		hopCount:      m.cfg.HopCount,
		loginTimeout:  time.Duration(m.cfg.Timeouts.LoginSeconds) * time.Second,
		initTimeout:   time.Duration(m.cfg.Timeouts.InitSeconds) * time.Second,
		idleTimeout:   time.Duration(m.cfg.Timeouts.IdleSeconds) * time.Second,
		keepalive:     time.Duration(m.cfg.KeepaliveSeconds) * time.Second,
		configEvery:   time.Duration(m.cfg.ConfigSeconds) * time.Second,
		writeQueue:    m.cfg.WriteQueueSize,
		maxLine:       m.cfg.MaxLineLength,
		password:      peer.password,
	}
}

func (m *Manager) maintenanceLoop() {
	interval := time.Duration(m.cfg.Topology.PersistIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UTC()
			if m.topology != nil {
				m.topology.prune(now)
			}
			if m.dedupe != nil {
				m.dedupe.prune(now)
			}
		}
	}
}
