package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

type probeProtocol struct {
	session       *probeSession
	pc9x          bool
	established   bool
	sentCall      bool
	sentPass      bool
	initSent      bool
	phaseDeadline time.Time
}

var errProbeHandshakeTimeout = errors.New("handshake timeout")

func (s *probeSession) runProtocol() error {
	p := probeProtocol{session: s}
	if err := p.startPhase(s.cfg.loginTimeout + s.cfg.initTimeout); err != nil {
		return err
	}
	if s.cfg.autoLogin {
		if err := p.sendLogin(); err != nil {
			return err
		}
		if err := p.sendPassword(); err != nil {
			return err
		}
	}
	for {
		now := time.Now()
		if err := p.admitIngress(now); err != nil {
			return err
		}
		line, err := s.reader.ReadLine(p.readDeadline(now))
		if err != nil {
			return err
		}
		// Socket deadlines cannot reject already acquired or buffered input.
		// A callback failure also forbids dispatch, even if ReadLine succeeds.
		if err := p.admitIngress(time.Now()); err != nil {
			return err
		}
		if line == "" {
			continue
		}
		if err := p.handleLine(line); err != nil {
			return err
		}
	}
}

// The synchronous protocol owner alone changes phase state. time.Now/Add keep
// monotonic elapsed budgets; protocol timestamps retain their separate UTC use.
func (p *probeProtocol) startPhase(duration time.Duration) error {
	p.phaseDeadline = time.Now().Add(duration)
	return p.session.transport.SetWriteDeadline(p.phaseDeadline)
}

func (p *probeProtocol) admitIngress(now time.Time) error {
	if err := context.Cause(p.session.ctx); err != nil {
		return err
	}
	if !p.phaseDeadline.IsZero() && !now.Before(p.phaseDeadline) {
		return errProbeHandshakeTimeout
	}
	return nil
}

func (p *probeProtocol) readDeadline(now time.Time) time.Time {
	idle := now.Add(p.session.cfg.idleTimeout)
	if !p.phaseDeadline.IsZero() && p.phaseDeadline.Before(idle) {
		return p.phaseDeadline
	}
	return idle
}

func (p *probeProtocol) establish() error {
	if p.established {
		return nil
	}
	if err := p.session.transport.SetWriteDeadline(time.Time{}); err != nil {
		return err
	}
	p.established = true
	p.phaseDeadline = time.Time{}
	return nil
}

func (p *probeProtocol) sendLogin() error {
	if p.sentCall {
		return nil
	}
	log.Printf("TX login %s", p.session.cfg.call)
	if err := p.session.sendLine(p.session.cfg.call); err != nil {
		return err
	}
	p.sentCall = true
	return nil
}

func (p *probeProtocol) sendPassword() error {
	if p.sentPass || p.session.cfg.password == "" {
		return nil
	}
	log.Printf("TX password (redacted)")
	if err := p.session.sendLine(p.session.cfg.password); err != nil {
		return err
	}
	p.sentPass = true
	return nil
}

func (p *probeProtocol) initialize(pc9x bool) error {
	p.pc9x = pc9x
	if !p.established && !p.initSent {
		if err := p.startPhase(p.session.cfg.initTimeout); err != nil {
			return err
		}
	}
	if err := p.session.sendInit(pc9x); err != nil {
		return err
	}
	p.initSent = true
	return nil
}

func (p *probeProtocol) handleLine(line string) error {
	cfg := p.session.cfg
	lower := strings.ToLower(line)
	if !p.initSent && (strings.HasPrefix(line, "PC19^") || strings.HasPrefix(line, "PC16^") || strings.HasPrefix(line, "PC17^") || strings.HasPrefix(line, "PC21^")) {
		log.Printf("RX %s (init marker, sending %s init)", line, modeLabel(cfg.preferPC9x))
		return p.initialize(cfg.preferPC9x)
	}
	if strings.Contains(lower, "login") || strings.Contains(lower, "callsign") || strings.Contains(lower, "call sign") {
		return p.sendLogin()
	}
	if strings.Contains(lower, "password") || strings.Contains(lower, "passcode") {
		return p.sendPassword()
	}
	if strings.HasPrefix(line, "PC51^") {
		fields := strings.Split(line, "^")
		if len(fields) >= 4 && strings.TrimSpace(fields[3]) == "1" {
			toNode := strings.TrimSpace(fields[1])
			fromNode := strings.TrimSpace(fields[2])
			if toNode == "*" || strings.EqualFold(toNode, cfg.call) {
				resp := fmt.Sprintf("PC51^%s^%s^0^", fromNode, toNode)
				if err := p.session.sendLine(resp); err != nil {
					return fmt.Errorf("send PC51 ack: %w", err)
				}
				log.Printf("RX %s | TX %s", line, resp)
				return nil
			}
		}
	}
	if strings.Contains(line, "PC18^") {
		pc9x := cfg.preferPC9x || strings.Contains(lower, "pc9x")
		log.Printf("RX %s (%s)", line, modeLabel(pc9x))
		// Compatibility responses remain allowed after PC22. They update init
		// mode without changing the mode captured by the existing keeper.
		return p.initialize(pc9x)
	}
	if strings.HasPrefix(line, "PC22") {
		log.Printf("RX %s (established)", line)
		if err := p.establish(); err != nil {
			return err
		}
		p.session.startKeeper(p.pc9x)
		return nil
	}
	if strings.HasPrefix(line, "PC61^") || strings.HasPrefix(line, "PC11^") {
		log.Printf("RX spot %s", line)
		if p.initSent && !p.established {
			return p.establish()
		}
		return nil
	}
	if strings.HasPrefix(line, "PC23^") || strings.HasPrefix(line, "PC73^") {
		log.Printf("RX wwv %s", line)
		return nil
	}
	log.Printf("RX %s", line)
	return nil
}
