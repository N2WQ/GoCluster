package peer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
	"unicode"

	"dxcluster/config"
	ztelnet "github.com/ziutek/telnet"
)

type direction int

const (
	dirInbound direction = iota
	dirOutbound
)

const (
	defaultPriorityQueue     = 128
	defaultPeerWriteDeadline = 2 * time.Second
)

var (
	errSessionWriteQueueFull    = errors.New("peer: write queue full")
	errSessionPriorityQueueFull = errors.New("peer: priority queue full")
	errSessionContextUnset      = errors.New("peer: session context not initialized")
)

type session struct {
	qualificationWriter  qualificationWriterState
	pendingReserved      bool
	ownerReserved        bool
	operation            *contextOperation
	id                   string
	diagnosticLabel      string
	conn                 net.Conn
	reader               *lineReader
	writer               *bufio.Writer
	writeCh              chan string
	normalReady          chan struct{}
	normalCapacity       int
	priorityLineCh       chan string
	priorityRawCh        chan []byte
	writeMu              sync.Mutex
	queueMu              sync.Mutex
	dataBytes            int
	controlBytes         int
	dataFixedBytes       int
	controlFixedBytes    int
	controlCount         int
	controlLineEnqueued  uint64 // queueMu: ordered control-line receipt progress
	controlLineFlushed   uint64
	recoveryFlushTarget  uint64
	activeBytes          int
	activeControl        bool
	lineTimes            controlTimes
	rawTimes             controlTimes
	workers              sync.WaitGroup
	manager              *Manager
	peer                 PeerEndpoint
	localCall            string
	remoteCall           string
	remoteVersion        string
	remoteBuild          string
	remoteVersionTooLong bool
	remoteBuildTooLong   bool
	remoteBitmap         int
	established          bool
	inboundCC            bool
	pc9x                 bool
	preferPC9x           bool
	password             string
	nodeVersion          string
	nodeBuild            string
	legacyVer            string
	pc92Bitmap           int
	nodeCount            int
	userCount            int
	hopCount             int
	loginTimeout         time.Duration
	initTimeout          time.Duration
	// The reader owns phaseDeadline. Requests copy it before crossing to the
	// controller; replayReady is published under manager.mu at commitment.
	phaseDeadline time.Time
	replayReady   <-chan error
	idleTimeout   time.Duration
	keepalive     time.Duration
	configEvery   time.Duration
	dir           direction
	ctx           context.Context
	// cancelMu protects cancellation installation and terminal closure. Never
	// hold it across cancellation, socket I/O, manager locks or worker joins.
	// ctx is installed once by Run before workers/controller requests observe it.
	cancelMu       sync.Mutex
	cancel         context.CancelFunc
	cancelClosed   bool
	closeOnce      sync.Once
	logKeepalive   bool
	logLineTooLong bool
}

func newSession(conn net.Conn, dir direction, manager *Manager, peer PeerEndpoint, settings sessionSettings) *session {
	diagnosticLabel := peer.ID()
	if diagnosticLabel == "" && conn.RemoteAddr() != nil {
		diagnosticLabel = conn.RemoteAddr().String()
	}
	useZiutek := strings.EqualFold(settings.telnetTransport, "ziutek")
	writerConn := conn
	readFn := conn.Read
	if useZiutek {
		if tconn, err := ztelnet.NewConn(conn); err == nil {
			writerConn = tconn
			readFn = tconn.Read
		} else {
			manager.reportDiagnostic("telnet_wrap_failed", diagnosticLabel, "transport_error")
			useZiutek = false
		}
	}
	writer := bufio.NewWriter(writerConn)
	s := &session{
		id:              peer.ID(),
		diagnosticLabel: diagnosticLabel,
		conn:            conn,
		writer:          writer,
		normalReady:     make(chan struct{}),
		normalCapacity:  boundedDataQueueCount(settings.writeQueue),
		priorityLineCh:  make(chan string, defaultPriorityQueue),
		priorityRawCh:   make(chan []byte, defaultPriorityQueue),
		manager:         manager,
		peer:            peer,
		localCall:       settings.localCall,
		remoteCall:      peer.remoteCall,
		preferPC9x:      settings.preferPC9x,
		password:        settings.password,
		nodeVersion:     settings.nodeVersion,
		nodeBuild:       settings.nodeBuild,
		legacyVer:       settings.legacyVersion,
		pc92Bitmap:      settings.pc92Bitmap,
		nodeCount:       settings.nodeCount,
		userCount:       settings.userCount,
		hopCount:        settings.hopCount,
		loginTimeout:    settings.loginTimeout,
		initTimeout:     settings.initTimeout,
		idleTimeout:     settings.idleTimeout,
		keepalive:       settings.keepalive,
		configEvery:     settings.configEvery,
		dir:             dir,
		logKeepalive:    settings.logKeepalive,
		logLineTooLong:  settings.logLineTooLong,
	}
	s.initializeOutputBudgetLocked()
	if useZiutek {
		s.reader = newLineReaderWithTransport(conn, settings.maxLine, settings.pc92MaxBytes, readFn, nil, nil)
	} else {
		s.reader = newLineReaderWithTransport(conn, settings.maxLine, settings.pc92MaxBytes, readFn, &telnetParser{}, func(data []byte) {
			_ = s.sendPriorityRaw(data)
		})
	}
	if manager != nil {
		s.reader.acquireScratch = func(deadline time.Time) (frameParseLease, error) {
			return manager.parseBudget.acquireCharge(s.ctx, deadline, readerScratchBytes)
		}
	}
	return s
}

// installContext hands cancellation to close without losing an earlier close.
// Run owns this one-time installation; the operation remains Run's retirement
// responsibility even when terminal closure refuses startup here.
func (s *session) installContext(ctx context.Context, cancel context.CancelFunc) error {
	s.cancelMu.Lock()
	s.ctx, s.cancel = ctx, cancel
	closed := s.cancelClosed
	s.cancelMu.Unlock()
	if closed {
		cancel()
		return context.Canceled
	}
	return ctx.Err()
}

func (s *session) Run() error {
	if s.conn == nil {
		if s.manager != nil {
			s.manager.endContextOperation(s.operation)
			s.manager.mu.Lock()
			s.manager.releaseCandidateSlotsLocked(s)
			s.manager.retryRetireLocked(s, time.Now())
			s.manager.mu.Unlock()
		}
		return errors.New("nil conn")
	}
	if s.manager == nil {
		s.close()
		return errors.New("peer: session manager not initialized")
	}
	if err := s.manager.trackCandidate(s); err != nil {
		s.close()
		s.manager.endContextOperation(s.operation)
		s.manager.mu.Lock()
		s.manager.releaseCandidateSlotsLocked(s)
		s.manager.retryRetireLocked(s, time.Now())
		s.manager.mu.Unlock()
		return err
	}
	// Production construction hands Run the operation that already owns the
	// inbound reservation or outbound dial. Package harnesses use the same
	// fixed owner pool; there is no alternate parent or cancellation bridge.
	if s.operation == nil {
		s.operation = s.manager.beginContextOperation()
	}
	if s.operation == nil {
		s.close()
		s.manager.releaseCandidate(s)
		return errors.New("peer: context owner capacity or manager stopped")
	}
	// Run owns every session worker and closes the socket before joining. The
	// cancellation watcher also interrupts reads when idle timeouts are disabled.
	defer func() {
		s.manager.retrySessionEnded(s, time.Now())
		s.close()
		s.workers.Wait()
		s.discardQueuedOutput()
		s.manager.unregisterSession(s)
		// Once registry ownership is gone, stale queued input may still retain
		// this identity but cannot require its potentially large remote metadata.
		s.manager.mu.Lock()
		s.remoteVersion, s.remoteBuild = "", ""
		s.remoteVersionTooLong, s.remoteBuildTooLong = false, false
		s.manager.mu.Unlock()
		if s.established {
			s.manager.reportConnection(ConnectionEvent{
				Direction: directionLabel(s.dir), Action: "disconnected", Peer: s.remoteCall,
				Endpoint: s.peer.host, Reason: "session_end",
			})
		}
		// releaseCandidate returns transport credits. Cancellation and removal
		// from the permanent parent must complete before another owner can use it.
		s.operation.cancel()
		s.manager.endContextOperation(s.operation)
		s.manager.releaseCandidate(s)
	}()
	if err := s.installContext(s.operation.ctx, s.operation.cancel); err != nil {
		return err
	}
	s.startWorker(s.writerLoop)
	s.startWorker(s.controlAgeLoop)
	s.startWorker(func() {
		<-s.ctx.Done()
		s.close()
	})

	var err error
	switch s.dir {
	case dirInbound:
		err = s.runInboundHandshake()
	case dirOutbound:
		err = s.runOutboundHandshake()
	default:
		err = errors.New("peer: invalid session direction")
	}
	if err != nil {
		s.manager.reportConnection(ConnectionEvent{
			Direction: directionLabel(s.dir),
			Action:    "rejected",
			Peer:      s.remoteCall,
			Endpoint:  s.peer.host,
			Reason:    "handshake_error",
		})
		s.close()
		return err
	}

	if err := s.manager.establishSession(s); err != nil {
		s.close()
		return err
	}
	s.established = true
	s.manager.retryEstablished(s, time.Now())
	s.manager.reportConnection(ConnectionEvent{
		Direction: directionLabel(s.dir),
		Action:    "established",
		Peer:      s.remoteCall,
		Endpoint:  s.peer.host,
		Reason:    "none",
	})

	if s.keepalive > 0 || s.configEvery > 0 {
		s.startWorker(s.keepaliveLoop)
	}

	for {
		if s.ctx.Err() != nil {
			return s.ctx.Err()
		}
		var deadline time.Time
		if s.idleTimeout > 0 {
			deadline = time.Now().UTC().Add(s.idleTimeout)
		}
		line, err := s.reader.ReadLine(deadline)
		if err != nil {
			var tooLong ErrLineTooLong
			if errors.As(err, &tooLong) {
				if s.logLineTooLong {
					s.manager.reportDiagnostic("line_too_long", s.diagnosticLabel, tooLong.Reason)
				}
				s.reportOverlong(tooLong)
				continue
			}
			return err
		}
		if line == "" {
			continue
		}
		if _, err := s.withParsedFrame(line, deadline, s.handleEstablishedFrame); err != nil {
			return err
		}
	}
}

func (s *session) handleEstablishedFrame(frame *Frame) (bool, error) {
	if frame.Type == "PC51" {
		s.handlePing(frame)
		return false, nil
	}
	if frame.Type == "PC20" && s.inboundCC {
		// A delayed CC completion does not start another exchange.
		return false, s.sendControlLine("PC22^")
	}
	s.manager.HandleFrame(frame, s)
	return false, nil
}

func directionLabel(dir direction) string {
	switch dir {
	case dirInbound:
		return "inbound"
	case dirOutbound:
		return "outbound"
	default:
		return "unknown"
	}
}

// keepaliveLoop has independent timers: zero disables only its own periodic
// publication. One-shot establishment/recovery is owned by the manager.
func (s *session) keepaliveLoop() {
	var keepC, configC <-chan time.Time
	if s.keepalive > 0 {
		ticker := time.NewTicker(s.keepalive)
		defer ticker.Stop()
		keepC = ticker.C
	}
	if s.configEvery > 0 {
		ticker := time.NewTicker(s.configEvery)
		defer ticker.Stop()
		configC = ticker.C
	}
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-keepC:
			if err := s.sendControlLine(fmt.Sprintf("PC51^%s^%s^1^", s.remoteCall, s.localCall)); err != nil {
				return
			}
			if s.pc9x {
				if err := s.manager.publishPeriodic(s, "K"); err != nil {
					s.close()
					return
				}
			}
		case <-configC:
			if s.pc9x {
				if err := s.manager.publishPeriodic(s, "C"); err != nil {
					s.close()
					return
				}
			}
		}
	}
}

func (s *session) handlePing(frame *Frame) {
	fields := frame.payloadFields()
	if len(fields) < 3 {
		return
	}
	toNode := strings.TrimSpace(fields[0])
	fromNode := strings.TrimSpace(fields[1])
	flag := strings.TrimSpace(fields[2])
	if flag != "1" {
		return
	}
	call := strings.TrimSpace(s.localCall)
	if call != "" && !strings.EqualFold(toNode, call) && toNode != "*" && toNode != "" {
		if s.logKeepalive {
			s.manager.reportDiagnostic("pc51_other_destination", toNode, "skipped")
		}
		return
	}
	resp := fmt.Sprintf("PC51^%s^%s^0^", fromNode, toNode)
	if s.logKeepalive {
		s.manager.reportDiagnostic("pc51_ping", fromNode, "ack")
	}
	if err := s.sendControlLine(resp); err != nil && s.logKeepalive {
		s.manager.reportDiagnostic("pc51_ack_failed", toNode, "queue_error")
	}
}

// runOutboundHandshake requires the protocol completion marker. Spot traffic
// cannot grant establishment authority or leak staged topology into live state.
func (s *session) runOutboundHandshake() error {
	deadline := time.Now().Add(s.loginTimeout + s.initTimeout)
	s.phaseDeadline = deadline
	if err := s.manager.waitRetryStartup(s, deadline); err != nil {
		return err
	}
	if err := s.sendOutboundStartup(); err != nil {
		return err
	}
	initSent := false
	for {
		if time.Now().After(deadline) {
			return errors.New("handshake timeout")
		}
		line, err := s.reader.ReadLine(deadline)
		if err != nil {
			var tooLong ErrLineTooLong
			if errors.As(err, &tooLong) {
				s.reportOverlong(tooLong)
				continue
			}
			return err
		}
		done, err := s.withParsedFrame(line, deadline, func(frame *Frame) (bool, error) { return s.handleOutboundFrame(frame, &initSent) })
		if err != nil || done {
			return err
		}
	}
}

// The final grant/gate check and bounded startup queue admission share mu with
// global closure. No socket I/O runs under it; the writer remains independent.
func (s *session) sendOutboundStartup() (err error) {
	s.manager.mu.RLock()
	defer func() {
		s.manager.mu.RUnlock()
		if err != nil {
			s.close()
		}
	}()
	if s.manager.stopping || (s.preferPC9x && !s.manager.retrySessionAllowedLocked(s)) {
		return errors.New("PC9x startup gated")
	}
	if s.localCall != "" {
		if err := s.enqueueStartupLineLocked(s.localCall, true); err != nil {
			return err
		}
	}
	if s.password != "" {
		// Borrow immutable configuration storage while preserving logical queue
		// charges; ordinary untrusted control frames always clone their input.
		if err := s.enqueueStartupLineLocked(s.password, false); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) handleOutboundFrame(frame *Frame, initSent *bool) (bool, error) {
	switch frame.Type {
	case "PC18":
		if *initSent {
			return false, nil
		}
		if err := s.acceptPC18(frame); err != nil {
			return false, err
		}
		if err := s.sendInit(true); err != nil {
			return false, err
		}
		*initSent = true
	case "PC92":
		if !*initSent && s.preferPC9x {
			s.pc9x = true
		}
		accepted, err := s.stageStartupPC92(frame)
		if err != nil {
			return false, err
		}
		if accepted && !*initSent && s.peer.family == config.PeeringPeerFamilyCCluster {
			if err := s.sendInit(true); err != nil {
				return false, err
			}
			*initSent = true
		}
	case "PC19", "PC16", "PC17", "PC21":
		if !*initSent {
			s.pc9x = false
			if err := s.sendInit(true); err != nil {
				return false, err
			}
			*initSent = true
		}
	case "PC22":
		return *initSent, nil
	}
	return false, nil
}

func (s *session) runInboundHandshake() error {
	loginDeadline := time.Now().Add(s.loginTimeout)
	s.phaseDeadline = loginDeadline
	if err := s.sendHandshakeLine("login:"); err != nil {
		return fmt.Errorf("send login prompt: %w", err)
	}
	var call string
	for call == "" {
		if time.Now().After(loginDeadline) {
			return errors.New("login timeout")
		}
		line, err := s.reader.ReadLine(loginDeadline)
		if err != nil {
			return err
		}
		call = strings.TrimSpace(line)
	}
	peer, err := s.manager.authorizeInbound(call, s.conn.RemoteAddr())
	if err != nil {
		return err
	}
	s.peer = peer
	s.localCall = s.manager.resolveLocalCall(peer)
	s.remoteCall = peer.remoteCall
	s.password = peer.password
	s.preferPC9x = peer.preferPC9x
	s.id = peer.ID()
	if s.password != "" {
		s.phaseDeadline = time.Now().Add(s.loginTimeout)
		if err := s.sendHandshakeLine("password:"); err != nil {
			return err
		}
		line, err := s.reader.ReadLine(s.phaseDeadline)
		if err != nil {
			return err
		}
		if strings.TrimSpace(line) != s.password {
			return errors.New("unauthorized password")
		}
	}
	deadline := time.Now().Add(s.initTimeout)
	s.phaseDeadline = deadline
	if err := s.manager.waitRetryStartup(s, deadline); err != nil {
		return err
	}
	if err := s.sendInboundStartup(); err != nil {
		return err
	}
	bannerSeen := false
	for {
		if time.Now().After(deadline) {
			return errors.New("init timeout")
		}
		line, err := s.reader.ReadLine(deadline)
		if err != nil {
			return err
		}
		done, err := s.withParsedFrame(line, deadline, func(frame *Frame) (bool, error) { return s.handleInboundFrame(frame, &bannerSeen) })
		if err != nil || done {
			return err
		}
	}
}

func (s *session) sendInboundStartup() (err error) {
	line, err := FormatPC18(s.manager.pc18Banner, s.nodeVersion, s.preferPC9x)
	if err != nil {
		return err
	}
	s.manager.mu.RLock()
	defer func() {
		s.manager.mu.RUnlock()
		if err != nil {
			s.close()
		}
	}()
	if s.manager.stopping || (s.preferPC9x && !s.manager.retrySessionAllowedLocked(s)) {
		return errors.New("PC9x startup gated")
	}
	return s.enqueueStartupLineLocked(line, true)
}

// manager.mu fences global closure through startup admission. Refusal is
// returned without closing the socket; the caller unlocks before closing.
func (s *session) enqueueStartupLineLocked(line string, clone bool) error {
	if s.ctx == nil {
		return errSessionContextUnset
	}
	if len(line) > MaxPeerFrameBytes {
		return errSessionPriorityQueueFull
	}
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	return s.enqueueControlLineLocked(line, clone, s.phaseDeadline, false)
}

func (s *session) handleInboundFrame(frame *Frame, bannerSeen *bool) (bool, error) {
	switch frame.Type {
	case "PC18":
		if *bannerSeen {
			return false, nil
		}
		if err := s.acceptPC18(frame); err != nil {
			return false, err
		}
		*bannerSeen = true
		if s.peer.family == config.PeeringPeerFamilyCCluster {
			s.inboundCC = true
			return true, s.sendInit(true)
		}
	case "PC92":
		if !*bannerSeen && s.preferPC9x {
			s.pc9x = true
		}
		accepted, err := s.stageStartupPC92(frame)
		if err != nil {
			return false, err
		}
		if accepted && s.peer.family == config.PeeringPeerFamilyCCluster {
			s.inboundCC = true
			return true, s.sendInit(true)
		}
	case "PC19", "PC16", "PC17", "PC21":
		s.pc9x = false
		s.manager.HandleFrame(frame, s)
	case "PC20":
		// Inbound configuration is acknowledged by PC22, not another PC20.
		if err := s.sendInit(false); err != nil {
			return false, err
		}
		return true, s.sendHandshakeLine("PC22^")
	}
	return false, nil
}

func (s *session) stageStartupPC92(frame *Frame) (bool, error) {
	record, eligible := eligiblePC92Record(frame, s, s.localCall)
	if !eligible {
		return false, nil
	}
	if err := s.manager.stagePC92Record(s, frame, record); err != nil {
		return false, err
	}
	return true, nil
}

func isCCClusterBanner(frame *Frame) bool {
	if frame == nil || frame.Type != "PC18" || len(frame.Fields) == 0 {
		return false
	}
	// Normalize whitespace without a potentially32K-entry Fields array. The
	// normalized copy is bounded by the frame; parser scratch stays byte-based.
	var normalized strings.Builder
	normalized.Grow(len(frame.Fields[0]))
	space := false
	for _, r := range frame.Fields[0] {
		if unicode.IsSpace(r) {
			space = normalized.Len() > 0
			continue
		}
		if space {
			normalized.WriteByte(' ')
			space = false
		}
		normalized.WriteRune(unicode.ToLower(r))
	}
	banner := normalized.String()
	return strings.Contains(banner, "cc cluster version:") || strings.Contains(banner, "cccluster version:")
}

func (s *session) acceptPC18(frame *Frame) error {
	if len(frame.Fields) < 2 {
		return errors.New("peer: malformed PC18")
	}
	cc := isCCClusterBanner(frame)
	if s.peer.family == config.PeeringPeerFamilyCCluster && !cc || s.peer.family == config.PeeringPeerFamilyDXSpider && cc {
		return fmt.Errorf("peer: family mismatch for %s (%s)", s.remoteCall, s.peer.family)
	}
	// CC's documented startup implies PC9x, but local preference still controls
	// negotiation. Other peers must advertise the standalone capability token.
	s.pc9x = s.preferPC9x && (cc || bannerHasPC9x(frame.Fields[0]))
	s.setRemoteVersion(pc92Numeric(strings.TrimSpace(frame.Fields[1]), false))
	buildNext := false
	for word := range strings.FieldsSeq(frame.Fields[0]) {
		if buildNext {
			s.setRemoteBuild(pc92Numeric(word, true))
			break
		}
		buildNext = strings.EqualFold(word, "Build:")
	}
	if s.pc9x {
		s.remoteBitmap = 5
	} else {
		s.remoteBitmap = 3
	}
	return nil
}

// Session metadata feeds only the bounded local publication. Oversized values
// remain accepted in the ordinary incoming frame/staged wire/graph, but cannot
// be published under the10-digit contract. Retain explicit presence rather than
// a second large string owner. An absent optional field does not call a setter;
// a later present valid value clears its own oversized marker.
func (s *session) setRemoteVersion(value string) {
	s.remoteVersionTooLong = len(value) > 10
	s.remoteVersion = ""
	if !s.remoteVersionTooLong {
		s.remoteVersion = strings.Clone(value)
	}
}

func (s *session) setRemoteBuild(value string) {
	s.remoteBuildTooLong = len(value) > 10
	s.remoteBuild = ""
	if !s.remoteBuildTooLong {
		s.remoteBuild = strings.Clone(value)
	}
}

func (s *session) remotePublicationMetadataOK() bool {
	return !s.remoteVersionTooLong && !s.remoteBuildTooLong && len(s.remoteVersion) <= 10 && len(s.remoteBuild) <= 10
}

func bannerHasPC9x(banner string) bool {
	lower := strings.ToLower(banner)
	for start := 0; start+4 <= len(lower); start++ {
		if lower[start:start+4] == "pc9x" &&
			(start == 0 || !pc18WordByte(lower[start-1])) {
			return true
		}
	}
	return false
}

// sendInit preserves the direction-specific exchange: the initiating side sends
// PC20; the inbound DXSpider response sends its records followed by PC22 only.
func (s *session) sendInit(sendEnd bool) error {
	if s.pc9x {
		if err := s.manager.publishInitial(s); err != nil {
			return err
		}
	} else {
		line := fmt.Sprintf("PC19^1^%s^0^%s^H%d^", s.localCall, s.legacyVer, s.hopCount)
		if err := s.sendHandshakeLine(line); err != nil {
			return err
		}
	}
	if sendEnd {
		return s.sendHandshakeLine("PC20^")
	}
	return nil
}

type sessionSettings struct {
	localCall       string
	preferPC9x      bool
	nodeVersion     string
	nodeBuild       string
	legacyVersion   string
	pc92Bitmap      int
	nodeCount       int
	userCount       int
	hopCount        int
	telnetTransport string
	loginTimeout    time.Duration
	initTimeout     time.Duration
	idleTimeout     time.Duration
	keepalive       time.Duration
	configEvery     time.Duration
	writeQueue      int
	maxLine         int
	pc92MaxBytes    int
	password        string
	logKeepalive    bool
	logLineTooLong  bool
}
