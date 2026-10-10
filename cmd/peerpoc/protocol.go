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
	session           *probeSession
	pc9x              bool
	established       bool
	sentCall          bool
	sentPass          bool
	initSent          bool
	handshakeDeadline time.Time
}

func (s *probeSession) runProtocol() error {
	p := probeProtocol{session: s}
	if s.cfg.autoLogin {
		if err := p.sendLogin(); err != nil {
			return err
		}
		if err := p.sendPassword(); err != nil {
			return err
		}
	}
	// Phase budgets are unchanged here; checkpoint 34b owns absolute budgets
	// and moving this initial combined duration before credential writes.
	p.handshakeDeadline = time.Now().UTC().Add(s.cfg.loginTimeout + s.cfg.initTimeout)
	for {
		if err := context.Cause(s.ctx); err != nil {
			return err
		}
		line, err := s.reader.ReadLine(time.Now().UTC().Add(s.cfg.idleTimeout))
		// A Telnet callback may fail while ReadLine still returns buffered data.
		// Never dispatch that line after the callback has retired the session.
		if cause := context.Cause(s.ctx); cause != nil {
			return cause
		}
		if err != nil {
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
	if err := sendInit(p.session.sendLine, p.session.timestamps, p.session.cfg, pc9x); err != nil {
		return err
	}
	p.initSent = true
	p.handshakeDeadline = time.Now().UTC().Add(p.session.cfg.initTimeout)
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
		p.established = true
		p.handshakeDeadline = time.Now().UTC().Add(24 * time.Hour)
		p.session.startKeeper(p.pc9x)
		return nil
	}
	if strings.HasPrefix(line, "PC61^") || strings.HasPrefix(line, "PC11^") {
		log.Printf("RX spot %s", line)
		if p.initSent && !p.established {
			p.established = true
			p.handshakeDeadline = time.Now().UTC().Add(24 * time.Hour)
		}
		return nil
	}
	if strings.HasPrefix(line, "PC23^") || strings.HasPrefix(line, "PC73^") {
		log.Printf("RX wwv %s", line)
		return nil
	}
	log.Printf("RX %s", line)
	if time.Now().UTC().After(p.handshakeDeadline) && !p.established {
		return errors.New("handshake timeout")
	}
	return nil
}
