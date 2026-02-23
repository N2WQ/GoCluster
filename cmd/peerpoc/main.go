package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"dxcluster/peer"
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

	backoff := cfg.backoffBase
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for {
		if err := runProbe(ctx, cfg); err != nil {
			log.Printf("session ended: %v", err)
		}
		if !cfg.reconnect {
			return
		}
		if backoff > cfg.backoffMax {
			backoff = cfg.backoffMax
		}
		log.Printf("reconnecting in %s", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < cfg.backoffMax {
			backoff *= 2
		}
	}
}

func runProbe(ctx context.Context, cfg probeConfig) error {
	addr := fmt.Sprintf("%s:%d", cfg.host, cfg.port)
	log.Printf("dialing %s as %s", addr, cfg.call)
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	var sendMu sync.Mutex
	reader := peer.NewLineReader(conn, 4096, 0, func(data []byte) {
		sendMu.Lock()
		if _, err := conn.Write(data); err != nil {
			log.Printf("telnet reply write failed: %v", err)
		}
		sendMu.Unlock()
	})
	writer := bufio.NewWriter(conn)
	tsGen := peer.NewTimestampGenerator()

	sendLine := func(line string) error {
		if !strings.HasSuffix(line, "\n") {
			line += "\r\n"
		}
		sendMu.Lock()
		defer sendMu.Unlock()
		if _, err := writer.WriteString(line); err != nil {
			return err
		}
		return writer.Flush()
	}

	var (
		pc9x          bool
		established   bool
		sentCall      bool
		sentPass      bool
		keepaliveTick <-chan time.Time
		initSent      bool
		shouldSendBye bool
	)

	defer func() {
		if shouldSendBye {
			if err := conn.SetWriteDeadline(time.Now().UTC().Add(2 * time.Second)); err != nil {
				log.Printf("failed to set BYE deadline: %v", err)
			}
			if err := sendLine("BYE"); err != nil {
				log.Printf("bye send error: %v", err)
			} else {
				log.Printf("TX BYE")
			}
		}
	}()

	if cfg.autoLogin {
		log.Printf("TX login %s (auto)", cfg.call)
		if err := sendLine(cfg.call); err != nil {
			return err
		}
		sentCall = true
		if cfg.password != "" {
			log.Printf("TX password (redacted) (auto)")
			if err := sendLine(cfg.password); err != nil {
				return err
			}
			sentPass = true
		}
	}

	handshakeDeadline := time.Now().UTC().Add(cfg.loginTimeout + cfg.initTimeout)

	for {
		deadline := time.Now().UTC().Add(cfg.idleTimeout)
		line, err := reader.ReadLine(deadline)
		if err != nil {
			return err
		}
		if line == "" {
			continue
		}
		lower := strings.ToLower(line)

		// Init frames before PC18 (seen in wild). Prefer PC9X unless user disabled it.
		if !initSent && (strings.HasPrefix(line, "PC19^") || strings.HasPrefix(line, "PC16^") || strings.HasPrefix(line, "PC17^") || strings.HasPrefix(line, "PC21^")) {
			pc9x = cfg.preferPC9x
			log.Printf("RX %s (init marker, sending %s init)", line, modeLabel(pc9x))
			if err := sendInit(sendLine, tsGen, cfg, pc9x); err != nil {
				return err
			}
			initSent = true
			handshakeDeadline = time.Now().UTC().Add(cfg.initTimeout)
			continue
		}

		if strings.Contains(lower, "login") || strings.Contains(lower, "callsign") || strings.Contains(lower, "call sign") {
			if !sentCall {
				log.Printf("TX login %s", cfg.call)
				if err := sendLine(cfg.call); err != nil {
					return err
				}
				sentCall = true
			}
			continue
		}
		if strings.Contains(lower, "password") || strings.Contains(lower, "passcode") {
			if cfg.password != "" && !sentPass {
				log.Printf("TX password (redacted)")
				if err := sendLine(cfg.password); err != nil {
					return err
				}
				sentPass = true
			}
			continue
		}

		// PC51 ping reply
		if strings.HasPrefix(line, "PC51^") {
			fields := strings.Split(line, "^")
			if len(fields) >= 4 && strings.TrimSpace(fields[3]) == "1" {
				toNode := strings.TrimSpace(fields[1])
				fromNode := strings.TrimSpace(fields[2])
				if toNode == "*" || strings.EqualFold(toNode, cfg.call) {
					resp := fmt.Sprintf("PC51^%s^%s^0^", fromNode, toNode)
					if err := sendLine(resp); err != nil {
						return fmt.Errorf("send PC51 ack: %w", err)
					}
					log.Printf("RX %s | TX %s", line, resp)
					continue
				}
			}
		}

		if strings.Contains(line, "PC18^") {
			pc9x = cfg.preferPC9x || strings.Contains(strings.ToLower(line), "pc9x")
			log.Printf("RX %s (%s)", line, modeLabel(pc9x))
			if err := sendInit(sendLine, tsGen, cfg, pc9x); err != nil {
				return err
			}
			initSent = true
			handshakeDeadline = time.Now().UTC().Add(cfg.initTimeout)
			continue
		}

		if strings.HasPrefix(line, "PC22") {
			log.Printf("RX %s (established)", line)
			established = true
			shouldSendBye = true
			handshakeDeadline = time.Now().UTC().Add(24 * time.Hour)
			if cfg.keepalive > 0 {
				t := time.NewTicker(cfg.keepalive)
				keepaliveTick = t.C
				go func(pc9xMode bool) {
					for {
						select {
						case <-ctx.Done():
							t.Stop()
							return
						case <-keepaliveTick:
							if pc9xMode {
								if err := sendLine(buildPC92Keepalive(tsGen, cfg)); err != nil {
									log.Printf("keepalive error: %v", err)
									return
								}
								log.Printf("TX PC92 K")
							} else {
								line := fmt.Sprintf("PC51^%s^%s^1^", cfg.call, cfg.call)
								if err := sendLine(line); err != nil {
									log.Printf("keepalive error: %v", err)
									return
								}
								log.Printf("TX %s", line)
							}
						}
					}
				}(pc9x)
			}
			continue
		}

		// DX spots
		if strings.HasPrefix(line, "PC61^") || strings.HasPrefix(line, "PC11^") {
			log.Printf("RX spot %s", line)
			if initSent && !established {
				established = true
				handshakeDeadline = time.Now().UTC().Add(24 * time.Hour)
			}
			continue
		}

		// WWV
		if strings.HasPrefix(line, "PC23^") || strings.HasPrefix(line, "PC73^") {
			log.Printf("RX wwv %s", line)
			continue
		}

		log.Printf("RX %s", line)

		if time.Now().UTC().After(handshakeDeadline) && !established {
			return errors.New("handshake timeout")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func sendInit(send func(string) error, tsGen *peer.TimestampGenerator, cfg probeConfig, pc9x bool) error {
	if pc9x {
		if err := send(sendPC92Add(tsGen, cfg)); err != nil {
			return err
		}
		if err := send(buildPC92Keepalive(tsGen, cfg)); err != nil {
			return err
		}
		return send("PC20^")
	}
	// Legacy PC19 + PC20
	line := fmt.Sprintf("PC19^1^%s^0^%s^H%d^", cfg.call, cfg.legacyVersion, cfg.hop)
	if err := send(line); err != nil {
		return err
	}
	return send("PC20^")
}

func sendPC92Add(tsGen *peer.TimestampGenerator, cfg probeConfig) string {
	ts := tsGen.Next()
	entry := fmt.Sprintf("%d%s:%s", cfg.pc92Bitmap, cfg.call, cfg.nodeVersion)
	if strings.TrimSpace(cfg.build) != "" {
		entry += ":" + strings.TrimSpace(cfg.build)
	}
	return fmt.Sprintf("PC92^%s^%s^A^^%s^H%d^", cfg.call, ts, entry, cfg.hop)
}

func buildPC92Keepalive(tsGen *peer.TimestampGenerator, cfg probeConfig) string {
	ts := tsGen.Next()
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
