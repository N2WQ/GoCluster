package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"dxcluster/peer"
)

// These private transport and ticker controls serve the connected-session and
// retry fixtures; production retains the existing dial timeout and backoff.
type probeRuntime struct {
	dial         func(context.Context, string, string) (net.Conn, error)
	newTicker    func(time.Duration) probeTicker
	timestampNow func() time.Time
}

func defaultProbeRuntime() probeRuntime {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	return probeRuntime{dial: dialer.DialContext, newTicker: newProbeTicker, timestampNow: time.Now}
}

func runProbeCommand(ctx context.Context, cfg probeConfig, runtime probeRuntime) error {
	// One command origin retains constant-size timestamp state through every
	// failed attempt. Session retirement precedes the next attempt's admission.
	timestamps := peer.NewTimestampGenerator()
	backoff := cfg.backoffBase
	for ctx.Err() == nil {
		err := runtime.runProbe(ctx, cfg, timestamps)
		if err != nil {
			log.Printf("session ended: %v", err)
		}
		// A child's cancellation is not a successful user-requested shutdown.
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			break
		}
		if !cfg.reconnect {
			return err
		}
		if backoff > cfg.backoffMax {
			backoff = cfg.backoffMax
		}
		log.Printf("reconnecting in %s", backoff)
		if !waitProbeBackoff(ctx, backoff) {
			break
		}
		if backoff < cfg.backoffMax {
			backoff *= 2
		}
	}
	return nil
}

func waitProbeBackoff(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

func (r probeRuntime) runProbe(ctx context.Context, cfg probeConfig, timestamps *peer.TimestampGenerator) error {
	addr := fmt.Sprintf("%s:%d", cfg.host, cfg.port)
	log.Printf("dialing %s as %s", addr, cfg.call)
	conn, err := r.dial(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	return newProbeSession(ctx, cfg, conn, timestamps, r.timestampNow, r.newTicker).run()
}
