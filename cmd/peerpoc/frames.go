package main

import (
	"context"
	"fmt"
	"strings"
)

func (s *probeSession) sendInit(pc9x bool) error {
	cfg := s.cfg
	if pc9x {
		if _, err := s.sendPC92(true); err != nil {
			return err
		}
		if _, err := s.sendPC92(false); err != nil {
			return err
		}
		return s.sendLine("PC20^")
	}
	line := fmt.Sprintf("PC19^1^%s^0^%s^H%d^", cfg.call, cfg.legacyVersion, cfg.hop)
	if err := s.sendLine(line); err != nil {
		return err
	}
	return s.sendLine("PC20^")
}

// Each record owns the common wire gate from admission through Flush. Init
// releases it between A, K and PC20, retaining the existing interleaving. A
// failed/uncertain publication consumes its stamp and cancels before unlock;
// a waiting producer can neither allocate another stamp nor replay this one.
func (s *probeSession) sendPC92(add bool) (string, error) {
	s.wireMu.Lock()
	defer s.wireMu.Unlock()
	if err := context.Cause(s.ctx); err != nil {
		return "", err
	}
	ts, err := s.timestamps.NextAt(s.timestampNow())
	if err != nil {
		s.fail(err)
		return "", context.Cause(s.ctx)
	}
	var line string
	if add {
		line = buildPC92Add(ts, s.cfg)
	} else {
		line = buildPC92Keepalive(ts, s.cfg)
	}
	if err := s.writeLine(line); err != nil {
		s.fail(err)
		return "", context.Cause(s.ctx)
	}
	return line, nil
}

func buildPC92Add(ts string, cfg probeConfig) string {
	entry := fmt.Sprintf("%d%s:%s", cfg.pc92Bitmap, cfg.call, cfg.nodeVersion)
	if strings.TrimSpace(cfg.build) != "" {
		entry += ":" + strings.TrimSpace(cfg.build)
	}
	return fmt.Sprintf("PC92^%s^%s^A^^%s^H%d^", cfg.call, ts, entry, cfg.hop)
}

func buildPC92Keepalive(ts string, cfg probeConfig) string {
	entry := fmt.Sprintf("%d%s:%s", cfg.pc92Bitmap, cfg.call, cfg.nodeVersion)
	if strings.TrimSpace(cfg.build) != "" {
		entry += ":" + strings.TrimSpace(cfg.build)
	}
	return fmt.Sprintf("PC92^%s^%s^K^%s^%d^%d^H%d^", cfg.call, ts, entry, cfg.nodeCount, cfg.userCount, cfg.hop)
}

func modeLabel(pc9x bool) string {
	if pc9x {
		return "pc9x"
	}
	return "legacy"
}
