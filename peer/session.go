package peer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	ztelnet "github.com/ziutek/telnet"
)

type direction int

const (
	dirInbound direction = iota
	dirOutbound
)

const (
	defaultPriorityQueue     = 32
	defaultPeerWriteDeadline = 2 * time.Second
)

type session struct {
	id             string
	conn           net.Conn
	reader         *lineReader
	writer         *bufio.Writer
	writeCh        chan string
	priorityLineCh chan string
	priorityRawCh  chan []byte
	writeMu        sync.Mutex
	manager        *Manager
	peer           PeerEndpoint
	localCall      string
	remoteCall     string
	pc9x           bool
	preferPC9x     bool
	password       string
	nodeVersion    string
	nodeBuild      string
	legacyVer      string
	pc92Bitmap     int
	nodeCount      int
	userCount      int
	hopCount       int
	loginTimeout   time.Duration
	initTimeout    time.Duration
	idleTimeout    time.Duration
	keepalive      time.Duration
	configEvery    time.Duration
	dir            direction
	tsGen          *timestampGenerator
	ctx            context.Context
	cancel         context.CancelFunc
	closeOnce      sync.Once
	overlongPath   string
	logKeepalive   bool
	logLineTooLong bool
}

func newSession(conn net.Conn, dir direction, manager *Manager, peer PeerEndpoint, settings sessionSettings) *session {
	useZiutek := strings.EqualFold(settings.telnetTransport, "ziutek")
	writerConn := conn
	readFn := conn.Read
	if useZiutek {
		if tconn, err := ztelnet.NewConn(conn); err == nil {
			writerConn = tconn
			readFn = tconn.Read
		} else {
			log.Printf("Peering: failed to wrap telnet transport: %v", err)
			useZiutek = false
		}
	}
	writer := bufio.NewWriter(writerConn)
	s := &session{
		id:             peer.ID(),
		conn:           conn,
		writer:         writer,
		writeCh:        make(chan string, settings.writeQueue),
		priorityLineCh: make(chan string, defaultPriorityQueue),
		priorityRawCh:  make(chan []byte, defaultPriorityQueue),
		manager:        manager,
		peer:           peer,
		localCall:      settings.localCall,
		preferPC9x:     settings.preferPC9x,
		password:       settings.password,
		nodeVersion:    settings.nodeVersion,
		nodeBuild:      settings.nodeBuild,
		legacyVer:      settings.legacyVersion,
		pc92Bitmap:     settings.pc92Bitmap,
		nodeCount:      settings.nodeCount,
		userCount:      settings.userCount,
		hopCount:       settings.hopCount,
		loginTimeout:   settings.loginTimeout,
		initTimeout:    settings.initTimeout,
		idleTimeout:    settings.idleTimeout,
		keepalive:      settings.keepalive,
		configEvery:    settings.configEvery,
		dir:            dir,
		tsGen:          &timestampGenerator{},
		overlongPath:   "logs/peering_overlong.log",
		logKeepalive:   settings.logKeepalive,
		logLineTooLong: settings.logLineTooLong,
	}
	if useZiutek {
		s.reader = newLineReaderWithTransport(conn, settings.maxLine, settings.pc92MaxBytes, readFn, nil, nil)
	} else {
		s.reader = newLineReaderWithTransport(conn, settings.maxLine, settings.pc92MaxBytes, readFn, &telnetParser{}, func(data []byte) {
			_ = s.sendPriorityRaw(data)
		})
	}
	return s
}

func (s *session) Run(ctx context.Context) error {
	if s.conn == nil {
		return errors.New("nil conn")
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	defer s.cancel()

	go s.writerLoop()

	var err error
	switch s.dir {
	case dirInbound:
		err = s.runInboundHandshake()
	case dirOutbound:
		err = s.runOutboundHandshake()
	}
	if err != nil {
		s.close()
		return err
	}

	s.manager.registerSession(s)
	defer s.manager.unregisterSession(s)

	if s.keepalive > 0 {
		go s.keepaliveLoop()
	}

	for {
		if s.ctx.Err() != nil {
			return s.ctx.Err()
		}
		var deadline time.Time
		if s.idleTimeout > 0 {
			deadline = time.Now().Add(s.idleTimeout)
		}
		line, err := s.reader.ReadLine(deadline)
		if err != nil {
			var tooLong errLineTooLong
			if errors.As(err, &tooLong) {
				if s.logLineTooLong {
					log.Printf("Peering: line too long from %s, dropping and continuing", s.peer.host)
				}
				appendOverlongSample(s.overlongPath, s.peer.host, tooLong.preview, tooLong.length)
				continue
			}
			return err
		}
		if line == "" {
			continue
		}
		frame, err := ParseFrame(line)
		if err != nil {
			continue
		}
		if frame.Type == "PC51" {
			s.handlePing(frame)
			continue
		}
		s.manager.HandleFrame(frame, s)
	}
}

func (s *session) writerLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case raw, ok := <-s.priorityRawCh:
			if !ok {
				return
			}
			_ = s.writeRaw(raw)
			continue
		case line, ok := <-s.priorityLineCh:
			if !ok {
				return
			}
			_ = s.writeLine(line)
			continue
		default:
		}
		select {
		case <-s.ctx.Done():
			return
		case raw, ok := <-s.priorityRawCh:
			if !ok {
				return
			}
			_ = s.writeRaw(raw)
		case line, ok := <-s.priorityLineCh:
			if !ok {
				return
			}
			_ = s.writeLine(line)
		case line, ok := <-s.writeCh:
			if !ok {
				return
			}
			_ = s.writeLine(line)
		}
	}
}

func (s *session) sendLine(line string) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.writeCh <- line:
		return nil
	default:
		return errors.New("peer: write queue full")
	}
}

// sendPriorityLine enqueues a line ahead of normal traffic so PC51 ACKs don't
// sit behind a large spot backlog. It never blocks the read loop.
func (s *session) sendPriorityLine(line string) bool {
	if s == nil {
		return false
	}
	if s.ctx != nil {
		select {
		case <-s.ctx.Done():
			return false
		default:
		}
	}
	select {
	case s.priorityLineCh <- line:
		return true
	default:
		return false
	}
}

// sendPriorityRaw enqueues telnet negotiation replies without blocking.
func (s *session) sendPriorityRaw(data []byte) bool {
	if s == nil || len(data) == 0 {
		return false
	}
	if s.ctx != nil {
		select {
		case <-s.ctx.Done():
			return false
		default:
		}
	}
	select {
	case s.priorityRawCh <- data:
		return true
	default:
		return false
	}
}

func (s *session) sendRaw(data []byte) error {
	if s.conn == nil {
		return errors.New("peer: nil conn")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.conn.SetWriteDeadline(time.Now().Add(defaultPeerWriteDeadline)); err != nil {
		return err
	}
	if _, err := s.writer.Write(data); err != nil {
		return err
	}
	return s.writer.Flush()
}

func (s *session) writeLine(line string) error {
	if s.conn == nil {
		return errors.New("peer: nil conn")
	}
	if !strings.HasSuffix(line, "\n") {
		line += "\r\n"
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.conn.SetWriteDeadline(time.Now().Add(defaultPeerWriteDeadline)); err != nil {
		return err
	}
	_, err := s.writer.WriteString(line)
	if err != nil {
		return err
	}
	return s.writer.Flush()
}

func (s *session) writeRaw(data []byte) error {
	if s.conn == nil {
		return errors.New("peer: nil conn")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.conn.SetWriteDeadline(time.Now().Add(defaultPeerWriteDeadline)); err != nil {
		return err
	}
	if _, err := s.writer.Write(data); err != nil {
		return err
	}
	return s.writer.Flush()
}

func (s *session) close() {
	s.closeOnce.Do(func() {
		if s.conn != nil {
			_ = s.conn.Close()
		}
		close(s.writeCh)
	})
}

func (s *session) keepaliveLoop() {
	ticker := time.NewTicker(s.keepalive)
	defer ticker.Stop()
	var cfgC <-chan time.Time
	var cfgTicker *time.Ticker
	if s.configEvery > 0 {
		cfgTicker = time.NewTicker(s.configEvery)
		cfgC = cfgTicker.C
		defer cfgTicker.Stop()
	}
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			// Always emit PC51 pings so peers that still expect legacy liveness
			// see activity even when the session uses pc9x.
			line := fmt.Sprintf("PC51^%s^%s^1^", s.remoteCall, s.localCall)
			_ = s.sendLine(line)
			// For pc9x sessions, also send a PC92 keepalive to refresh topology.
			if s.pc9x {
				line := s.buildPC92Keepalive()
				_ = s.sendLine(line)
			}
		case <-cfgC:
			// Periodic PC92 C config refresh to keep topology alive on peers; DXSpider
			// purges nodes that miss several config intervals.
			if s.pc9x {
				line := s.buildPC92Config()
				_ = s.sendLine(line)
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
			log.Printf("Peering: PC51 ping addressed to %q (local %q); skipping response", toNode, call)
		}
		return
	}
	resp := fmt.Sprintf("PC51^%s^%s^0^", fromNode, toNode)
	if s.logKeepalive {
		log.Printf("Peering: PC51 ping from %s to %s; sending ACK", fromNode, toNode)
	}
	if !s.sendPriorityLine(resp) {
		if s.logKeepalive {
			log.Printf("Peering: PC51 ACK to %s dropped: priority queue full", toNode)
		}
	}
}

func (s *session) runOutboundHandshake() error {
	start := time.Now()
	deadline := start.Add(s.loginTimeout + s.initTimeout)
	initSent := false
	waitInit := true
	sentCall := false
	sentPass := false

	// Send credentials immediately to match common DXSpider expectations (banner often precedes prompts).
	if s.localCall != "" {
		if err := s.sendLine(s.localCall); err != nil {
			return err
		}
		sentCall = true
	}
	if s.password != "" {
		if err := s.sendLine(s.password); err != nil {
			return err
		}
		sentPass = true
	}

	logPrefix := fmt.Sprintf("Peering hs %s -> %s", s.localCall, s.peer.host)

	for {
		if time.Now().After(deadline) {
			return errors.New("handshake timeout")
		}
		if !sentCall && time.Since(start) > s.loginTimeout/2 {
			if s.localCall != "" {
				if err := s.sendLine(s.localCall); err != nil {
					return err
				}
				sentCall = true
			}
			if !sentPass && s.password != "" {
				if err := s.sendLine(s.password); err != nil {
					return err
				}
				sentPass = true
			}
		}
		line, err := s.reader.ReadLine(deadline)
		if err != nil {
			var tooLong errLineTooLong
			if errors.As(err, &tooLong) {
				log.Printf("%s RX line too long, dropping", logPrefix)
				appendOverlongSample(s.overlongPath, s.peer.host, tooLong.preview, tooLong.length)
				continue
			}
			return err
		}
		if line == "" {
			continue
		}
		log.Printf("%s RX %s", logPrefix, line)
		if strings.Contains(line, "PC18^") {
			s.pc9x = s.preferPC9x && strings.Contains(strings.ToLower(line), "pc9x")
			if !sentCall && s.localCall != "" {
				if err := s.sendLine(s.localCall); err != nil {
					return err
				}
				sentCall = true
			}
			if s.password != "" && !sentPass {
				if err := s.sendLine(s.password); err != nil {
					return err
				}
				sentPass = true
			}
			if !initSent && sentCall && (s.password == "" || sentPass) {
				if err := s.sendInit(); err != nil {
					return err
				}
				log.Printf("%s TX init (pc9x=%v)", logPrefix, s.pc9x)
				initSent = true
			}
			waitInit = true
			continue
		}
		if (strings.HasPrefix(line, "PC19^") || strings.HasPrefix(line, "PC16^") || strings.HasPrefix(line, "PC17^") || strings.HasPrefix(line, "PC21^")) && !initSent {
			s.pc9x = s.preferPC9x
			if !sentCall && s.localCall != "" {
				if err := s.sendLine(s.localCall); err != nil {
					return err
				}
				sentCall = true
			}
			if s.password != "" && !sentPass {
				if err := s.sendLine(s.password); err != nil {
					return err
				}
				sentPass = true
			}
			if sentCall && (s.password == "" || sentPass) {
				if err := s.sendInit(); err != nil {
					return err
				}
				log.Printf("%s TX init (legacy)", logPrefix)
				initSent = true
				waitInit = true
			}
			continue
		}
		if strings.HasPrefix(line, "PC22") && initSent {
			return nil
		}
		if initSent && (strings.HasPrefix(line, "PC11^") || strings.HasPrefix(line, "PC61^")) {
			// DXSpider peers often stream spots before sending PC22; treat spot traffic as implicit establishment.
			if strings.HasPrefix(line, "PC61^") {
				s.pc9x = true
			}
			if frame, err := ParseFrame(line); err == nil && s.manager != nil {
				s.manager.HandleFrame(frame, s)
			}
			log.Printf("%s established via incoming spots (pc9x=%v)", logPrefix, s.pc9x)
			return nil
		}
		if waitInit {
			if prompt, ok := classifyPrompt(line); ok {
				if prompt == promptLogin && !sentCall && s.localCall != "" {
					if err := s.sendLine(s.localCall); err != nil {
						return err
					}
					sentCall = true
				}
				if prompt == promptPassword && s.password != "" && !sentPass {
					if err := s.sendLine(s.password); err != nil {
						return err
					}
					sentPass = true
				}
			}
		}
	}
}

func (s *session) runInboundHandshake() error {
	s.pc9x = true
	_ = s.sendLine("login:")
	loginDeadline := time.Now().Add(s.loginTimeout)
	var call string
	for {
		if time.Now().After(loginDeadline) {
			return errors.New("login timeout")
		}
		line, err := s.reader.ReadLine(loginDeadline)
		if err != nil {
			return err
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		call = line
		break
	}
	if !s.manager.allowInbound(call, s.conn.RemoteAddr()) {
		return fmt.Errorf("unauthorized inbound peer: %s", call)
	}
	s.remoteCall = call
	if s.password != "" {
		_ = s.sendLine("password:")
		passDeadline := time.Now().Add(s.loginTimeout)
		line, err := s.reader.ReadLine(passDeadline)
		if err != nil {
			return err
		}
		if strings.TrimSpace(line) != s.password {
			return fmt.Errorf("unauthorized password")
		}
	}
	if err := s.sendPC18(); err != nil {
		return err
	}

	initDeadline := time.Now().Add(s.initTimeout)
	for {
		if time.Now().After(initDeadline) {
			return errors.New("init timeout")
		}
		line, err := s.reader.ReadLine(initDeadline)
		if err != nil {
			return err
		}
		frame, err := ParseFrame(line)
		if err != nil {
			continue
		}
		switch frame.Type {
		case "PC92":
			s.pc9x = true
			s.manager.HandleFrame(frame, s)
		case "PC19", "PC16", "PC17", "PC21":
			s.pc9x = false
			s.manager.HandleFrame(frame, s)
		case "PC20":
			if err := s.sendInit(); err != nil {
				return err
			}
			if err := s.sendLine("PC22^"); err != nil {
				return err
			}
			return nil
		}
	}
}

func (s *session) sendPC18() error {
	// DXSpider peers classify remote software based on the PC18 banner. Include the
	// literal "DXSpider Version: <ver>" prefix so their regex matches and we are
	// treated as a known sort, while still advertising our own variant/build after
	// the required fields.
	if strings.TrimSpace(s.nodeBuild) != "" {
		info := fmt.Sprintf("PC18^DXSpider Version: %s Build: %s gocluster pc9x^%s^", s.nodeVersion, s.nodeBuild, s.nodeVersion)
		return s.sendLine(info)
	}
	info := fmt.Sprintf("PC18^DXSpider Version: %s gocluster pc9x^%s^", s.nodeVersion, s.nodeVersion)
	return s.sendLine(info)
}

func (s *session) sendInit() error {
	if s.pc9x {
		if err := s.sendLine(s.buildPC92Add()); err != nil {
			return err
		}
		if err := s.sendLine(s.buildPC92Keepalive()); err != nil {
			return err
		}
		return s.sendLine("PC20^")
	}
	line := fmt.Sprintf("PC19^1^%s^0^%s^H%d^", s.localCall, s.legacyVer, s.hopCount)
	if err := s.sendLine(line); err != nil {
		return err
	}
	return s.sendLine("PC20^")
}

func (s *session) buildPC92Add() string {
	entry := s.pc92Entry()
	ts := s.tsGen.Next()
	return fmt.Sprintf("PC92^%s^%s^A^^%s^H%d^", s.localCall, ts, entry, s.hopCount)
}

func (s *session) buildPC92Keepalive() string {
	entry := s.pc92Entry()
	ts := s.tsGen.Next()
	nodes := s.nodeCount
	users := s.userCount
	if s.manager != nil {
		nodes = s.manager.liveNodeCount()
		users = s.manager.liveUserCount()
	}
	if nodes <= 0 {
		nodes = 1
	}
	if users < 0 {
		users = 0
	}
	return fmt.Sprintf("PC92^%s^%s^K^%s^%d^%d^H%d^", s.localCall, ts, entry, nodes, users, s.hopCount)
}

func (s *session) buildPC92Config() string {
	entry := s.pc92Entry()
	ts := s.tsGen.Next()
	return fmt.Sprintf("PC92^%s^%s^C^%s^H%d^", s.localCall, ts, entry, s.hopCount)
}

func (s *session) pc92Entry() string {
	entry := fmt.Sprintf("%d%s:%s", s.pc92Bitmap, s.localCall, s.nodeVersion)
	if strings.TrimSpace(s.nodeBuild) != "" {
		entry += ":" + strings.TrimSpace(s.nodeBuild)
	}
	return entry
}

type promptType int

const (
	promptUnknown promptType = iota
	promptLogin
	promptPassword
)

func classifyPrompt(line string) (promptType, bool) {
	lower := strings.ToLower(line)
	if strings.Contains(lower, "password") || strings.Contains(lower, "passcode") {
		return promptPassword, true
	}
	if strings.Contains(lower, "login") || strings.Contains(lower, "callsign") || strings.Contains(lower, "call sign") || strings.Contains(lower, "username") {
		return promptLogin, true
	}
	return promptUnknown, false
}

type timestampGenerator struct {
	lastSec int
	seq     int
	mu      sync.Mutex
}

func (g *timestampGenerator) Next() string {
	now := time.Now().UTC()
	sec := now.Hour()*3600 + now.Minute()*60 + now.Second()
	g.mu.Lock()
	defer g.mu.Unlock()
	if sec != g.lastSec {
		g.lastSec = sec
		g.seq = 0
		return fmt.Sprintf("%d", sec)
	}
	g.seq++
	return fmt.Sprintf("%d.%02d", sec, g.seq)
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
