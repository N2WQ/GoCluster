package main

import (
	"fmt"
	"log"
	"time"
)

type probeTicker interface {
	Ticks() <-chan time.Time
	Stop()
}

type clockProbeTicker struct{ *time.Ticker }

func (t clockProbeTicker) Ticks() <-chan time.Time { return t.C }

func newProbeTicker(interval time.Duration) probeTicker {
	return clockProbeTicker{time.NewTicker(interval)}
}

// First PC22 owns keeper/BYE independently of spot-based establishment. The
// reader alone writes pc22Seen/keeperDone and captures the selected mode here.
func (s *probeSession) startKeeper(pc9x bool) {
	if s.pc22Seen {
		return
	}
	s.pc22Seen = true
	if s.cfg.keepalive <= 0 {
		return
	}
	ticker := s.newTicker(s.cfg.keepalive)
	s.keeperDone = make(chan struct{})
	go func() {
		defer close(s.keeperDone)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.Ticks():
				if s.ctx.Err() != nil {
					return
				}
				if err := s.sendKeepalive(pc9x); err != nil {
					s.fail(fmt.Errorf("keepalive: %w", err))
					return
				}
			}
		}
	}()
}

func (s *probeSession) sendKeepalive(pc9x bool) error {
	line := fmt.Sprintf("PC51^%s^%s^1^", s.cfg.call, s.cfg.call)
	if pc9x {
		var err error
		line, err = buildPC92Keepalive(s.timestamps, s.cfg)
		if err != nil {
			return err
		}
	}
	if err := s.sendLine(line); err != nil {
		return err
	}
	log.Printf("TX %s", line)
	return nil
}
