// peerpoc is a diagnostic peer client. Each attempt owns and joins its session
// before the command admits a reconnect; see session.go for transport ownership.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type probeConfig struct {
	host          string
	port          int
	call          string
	password      string
	preferPC9x    bool
	loginTimeout  time.Duration
	initTimeout   time.Duration
	idleTimeout   time.Duration
	keepalive     time.Duration
	autoLogin     bool
	hop           int
	pc92Bitmap    int
	nodeVersion   string
	build         string
	legacyVersion string
	nodeCount     int
	userCount     int
	reconnect     bool
	backoffBase   time.Duration
	backoffMax    time.Duration
}

func main() {
	os.Exit(runProbeCLI())
}

// The CLI owner retires signal registration before main publishes its status.
func runProbeCLI() int {
	var cfg probeConfig
	flag.StringVar(&cfg.host, "host", "dxs.n2wq.com", "peer host")
	flag.IntVar(&cfg.port, "port", 7300, "peer port")
	flag.StringVar(&cfg.call, "call", "N2WQ-33", "login/node callsign")
	flag.StringVar(&cfg.password, "password", "", "login password (if required)")
	flag.BoolVar(&cfg.preferPC9x, "prefer-pc9x", true, "prefer PC9X mode when banner advertises it")
	flag.DurationVar(&cfg.loginTimeout, "login-timeout", 15*time.Second, "login timeout")
	flag.DurationVar(&cfg.initTimeout, "init-timeout", 60*time.Second, "init handshake timeout")
	flag.DurationVar(&cfg.idleTimeout, "idle-timeout", 10*time.Minute, "idle timeout")
	flag.DurationVar(&cfg.keepalive, "keepalive", 60*time.Second, "keepalive interval (PC92 K or PC51)")
	flag.BoolVar(&cfg.autoLogin, "auto-login", true, "send login/password immediately on connect")
	flag.IntVar(&cfg.hop, "hop", 99, "hop count to advertise")
	flag.IntVar(&cfg.pc92Bitmap, "pc92-bitmap", 5, "PC92 bitmap (5 = node here)")
	flag.StringVar(&cfg.nodeVersion, "node-version", "5457", "node version for PC92")
	flag.StringVar(&cfg.build, "build", "", "optional build tag for PC92")
	flag.StringVar(&cfg.legacyVersion, "legacy-version", "5401", "legacy version for PC19")
	flag.IntVar(&cfg.nodeCount, "node-count", 1, "node count for PC92 K")
	flag.IntVar(&cfg.userCount, "user-count", 0, "user count for PC92 K")
	flag.BoolVar(&cfg.reconnect, "reconnect", true, "auto reconnect on disconnect")
	flag.DurationVar(&cfg.backoffBase, "backoff-base", 2*time.Second, "initial reconnect backoff")
	flag.DurationVar(&cfg.backoffMax, "backoff-max", 5*time.Minute, "max reconnect backoff")
	flag.Parse()
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runProbeCommand(ctx, cfg, defaultProbeRuntime()); err != nil {
		return 1
	}
	return 0
}
