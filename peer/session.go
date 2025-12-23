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
)

type direction int

const (
	dirInbound direction = iota
	dirOutbound
)

type session struct {
	id           string
	conn         net.Conn
	reader       *lineReader
	writer       *bufio.Writer
	writeCh      chan string
	writeMu      sync.Mutex
	manager      *Manager
	peer         PeerEndpoint
	localCall    string
	remoteCall   string
	pc9x         bool
	preferPC9x   bool
	password     string
	nodeVersion  string
	nodeBuild    string
	legacyVer    string
	pc92Bitmap   int
	nodeCount    int
	userCount    int
	hopCount     int
	loginTimeout time.Duration
	initTimeout  time.Duration
	idleTimeout  time.Duration
	keepalive    time.Duration
	dir          direction
	tsGen        *timestampGenerator
	ctx          context.Context
	cancel       context.CancelFunc
	closeOnce    sync.Once
}

func newSession(conn net.Conn, dir direction, manager *Manager, peer PeerEndpoint, settings sessionSettings) *session {
	writer := bufio.NewWriter(conn)
	s := &session{
		id:           peer.ID(),
		conn:         conn,
		writer:       writer,
		writeCh:      make(chan string, settings.writeQueue),
		manager:      manager,
		peer:         peer,
		localCall:    settings.localCall,
		preferPC9x:   settings.preferPC9x,
		password:     settings.password,
		nodeVersion:  settings.nodeVersion,
		nodeBuild:    settings.nodeBuild,
		legacyVer:    settings.legacyVersion,
		pc92Bitmap:   settings.pc92Bitmap,
		nodeCount:    settings.nodeCount,
		userCount:    settings.userCount,
		hopCount:     settings.hopCount,
		loginTimeout: settings.loginTimeout,
		initTimeout:  settings.initTimeout,
		idleTimeout:  settings.idleTimeout,
		keepalive:    settings.keepalive,
		dir:          dir,
		tsGen:        &timestampGenerator{},
	}
	s.reader = newLineReader(conn, &s.writeMu, settings.maxLine)
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
			if errors.Is(err, ErrLineTooLong) {
				log.Printf("Peering: line too long from %s, dropping and continuing", s.peer.host)
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

func (s *session) sendRaw(data []byte) error {
	if s.conn == nil {
		return errors.New("peer: nil conn")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
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
	_, err := s.writer.WriteString(line)
	if err != nil {
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
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if s.pc9x {
				line := s.buildPC92Keepalive()
				_ = s.sendLine(line)
			} else {
				line := fmt.Sprintf("PC51^%s^%s^1^", s.remoteCall, s.localCall)
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
		return
	}
	resp := fmt.Sprintf("PC51^%s^%s^0^", fromNode, toNode)
	_ = s.sendLine(resp)
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
			if errors.Is(err, ErrLineTooLong) {
				log.Printf("%s RX line too long, dropping", logPrefix)
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
	info := fmt.Sprintf("PC18^gocluster peer pc9x^%s^", s.nodeVersion)
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
	return fmt.Sprintf("PC92^%s^%s^K^%s^%d^%d^H%d^", s.localCall, ts, entry, s.nodeCount, s.userCount, s.hopCount)
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
	localCall     string
	preferPC9x    bool
	nodeVersion   string
	nodeBuild     string
	legacyVersion string
	pc92Bitmap    int
	nodeCount     int
	userCount     int
	hopCount      int
	loginTimeout  time.Duration
	initTimeout   time.Duration
	idleTimeout   time.Duration
	keepalive     time.Duration
	writeQueue    int
	maxLine       int
	password      string
}
