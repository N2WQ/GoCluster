// Command peerprobe connects to a configured DXSpider peer, exercises the same
// PC frame parser, and prints both the raw inbound line and the parsed frame/spot
// to stdout. It is a standalone debugging utility that shares the main cluster
// configuration but does not start any other services.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"dxcluster/peer"
	"dxcluster/rbn"
	"dxcluster/spot"

	"github.com/ziutek/telnet"

	_ "unsafe" // for go:linkname to reuse the real spot parser
)

//go:linkname parseSpotFromFrame dxcluster/peer.parseSpotFromFrame
func parseSpotFromFrame(frame *peer.Frame, fallbackOrigin string) (*spot.Spot, error)

type probeConfig struct {
	peerHost       string
	peerPort       int
	peerRemoteCall string
	localCall      string
	password       string
	preferPC9x     bool
	nodeVersion    string
	nodeBuild      string
	legacyVersion  string
	pc92Bitmap     int
	hopCount       int
	keepaliveSec   int
	idleSec        int
	loginSec       int
	initSec        int
	maxLine        int
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	clusterHost := flag.String("cluster_host", "localhost", "Host for telnet cluster comparison (hostname or host:port)")
	clusterPort := flag.Int("cluster_port", 0, "Port for telnet cluster comparison (optional when cluster_host includes port)")
	clusterCall := flag.String("cluster_call", "LZ3ZZ", "Callsign to use when logging into cluster telnet")
	windowMinutes := flag.Int("window_minutes", 3, "Matching window (minutes) between peer and telnet spots")
	peerHost := flag.String("peer_host", "dxs.n2wq.com", "Peer host")
	peerPort := flag.Int("peer_port", 7300, "Peer port")
	peerRemote := flag.String("peer_remote_call", "*", "Peer remote callsign (used in PC51)")
	localCall := flag.String("peer_local_call", "N0CALL", "Local callsign for peer login")
	peerPass := flag.String("peer_password", "", "Peer password (optional)")
	preferPC9x := flag.Bool("prefer_pc9x", true, "Prefer pc9x mode if offered")
	keepaliveSec := flag.Int("keepalive_seconds", 30, "Keepalive interval seconds")
	idleSec := flag.Int("idle_seconds", 600, "Idle read deadline seconds")
	loginSec := flag.Int("login_timeout", 10, "Login timeout seconds")
	initSec := flag.Int("init_timeout", 20, "Init timeout seconds")
	maxLine := flag.Int("max_line_length", 4096, "Max line length to accept from peer")
	nodeVer := flag.String("node_version", "5457", "Node version to advertise in PC92")
	nodeBuild := flag.String("node_build", "", "Node build to advertise in PC92")
	legacyVer := flag.String("legacy_version", "1.57", "Legacy version to advertise in PC19")
	pc92Bitmap := flag.Int("pc92_bitmap", 5, "PC92 bitmap")
	hopCount := flag.Int("hop_count", 99, "Hop count to advertise")
	flag.Parse()

	cfg := probeConfig{
		peerHost:       *peerHost,
		peerPort:       *peerPort,
		peerRemoteCall: strings.TrimSpace(*peerRemote),
		localCall:      strings.ToUpper(strings.TrimSpace(*localCall)),
		password:       *peerPass,
		preferPC9x:     *preferPC9x,
		nodeVersion:    *nodeVer,
		nodeBuild:      *nodeBuild,
		legacyVersion:  *legacyVer,
		pc92Bitmap:     *pc92Bitmap,
		hopCount:       *hopCount,
		keepaliveSec:   *keepaliveSec,
		idleSec:        *idleSec,
		loginSec:       *loginSec,
		initSec:        *initSec,
		maxLine:        *maxLine,
	}

	peerEvents := make(chan spotEvent, 1024)
	telnetEvents := make(chan spotEvent, 1024)

	// Start telnet tap to our cluster using the existing minimal parser from rbn.Client.
	clusterHostName, clusterHostPort, err := resolveClusterEndpoint(*clusterHost, *clusterPort, 7300)
	if err != nil {
		log.Fatalf("invalid cluster endpoint: %v", err)
	}
	startTelnetTap(clusterHostName, clusterHostPort, *clusterCall, telnetEvents)

	// Start peer loop (auto-reconnect on EOF) in background.
	tsGen := &timestampGenerator{}
	go peerLoop(cfg, peerEvents, tsGen)

	matchWindow := time.Duration(*windowMinutes) * time.Minute
	runMatcher(matchWindow, peerEvents, telnetEvents)
}

// readPeerFeed consumes PC frames from the peer connection and emits spot events for PC11/PC61.
// It also replies to PC51 pings to keep the session alive. On read error, it reports the error
// to errOut and returns so the caller can reconnect.
func readPeerFeed(conn net.Conn, reader *peer.LineReader, writeMu *sync.Mutex, localCall string, fallbackOrigin string, idleTimeout time.Duration, out chan<- spotEvent, errOut chan<- error) {
	for {
		var deadline time.Time
		if idleTimeout > 0 {
			deadline = time.Now().UTC().Add(idleTimeout)
		}
		line, err := reader.ReadLine(deadline)
		if err != nil {
			var tooLong peer.ErrLineTooLong
			if errors.As(err, &tooLong) {
				log.Printf("peerprobe: line too long (%d bytes), dropping and continuing", tooLong.Length)
				continue
			}
			if errOut != nil {
				errOut <- err
			}
			return
		}
		arrival := time.Now().UTC()
		if strings.TrimSpace(line) == "" {
			continue
		}
		frame, err := peer.ParseFrame(line)
		if err != nil {
			continue
		}
		switch frame.Type {
		case "PC51":
			handlePeerPing(frame, writeMu, conn, localCall)
			log.Printf("KEEPALIVE RECV %s", frame.Raw)
		case "PC92":
			if fields := payloadFields(frame.Fields); len(fields) >= 3 && strings.EqualFold(strings.TrimSpace(fields[2]), "K") {
				log.Printf("KEEPALIVE RECV %s", frame.Raw)
			}
		case "PC11", "PC61":
			if s, err := parseSpotFromFrame(frame, fallbackOrigin); err == nil {
				s.RefreshBeaconFlag()
				s.EnsureNormalized()
				log.Printf("PEER ARRIVAL %s DX %s DE %s", arrival.Format(time.RFC3339Nano), s.DXCall, s.DECall)
				out <- spotEvent{Spot: s, Arrival: arrival, Source: "peer"}
			}
		}
	}
}

// startTelnetTap connects to the local cluster telnet server and streams broadcast spots via the rbn minimal parser.
func startTelnetTap(host string, port int, callsign string, out chan<- spotEvent) {
	client := rbn.NewClient(host, port, callsign, "CLUSTER-TAP", nil, true, 1024)
	client.UseMinimalParser()
	if err := client.Connect(); err != nil {
		log.Fatalf("telnet tap connect failed: %v", err)
	}
	go func() {
		log.Printf("Telnet tap connected to %s:%d as %s", host, port, callsign)
		for s := range client.GetSpotChannel() {
			if s == nil {
				continue
			}
			s.RefreshBeaconFlag()
			s.EnsureNormalized()
			arrival := time.Now().UTC()
			log.Printf("TELNET ARRIVAL %s DX %s DE %s", arrival.Format(time.RFC3339Nano), s.DXCall, s.DECall)
			out <- spotEvent{Spot: s, Arrival: arrival, Source: "telnet"}
		}
	}()
}

type spotEvent struct {
	Spot    *spot.Spot
	Arrival time.Time
	Source  string // peer or telnet
}

// runMatcher correlates peer spots and cluster telnet spots within a sliding window and reports delay stats.
func runMatcher(window time.Duration, peerEvents <-chan spotEvent, telnetEvents <-chan spotEvent) {
	if window <= 0 {
		window = 3 * time.Minute
	}
	peerStore := newEventStore(window)
	telnetStore := newEventStore(window)
	stats := newDelayStats()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case ev := <-peerEvents:
			if ev.Spot == nil {
				continue
			}
			if matched, delta := telnetStore.match(ev); matched {
				stats.add(delta)
				log.Printf("DX %s / DE %s : %s", ev.Spot.DXCall, ev.Spot.DECall, formatDelay(delta))
			} else {
				peerStore.add(ev)
			}
		case ev := <-telnetEvents:
			if ev.Spot == nil {
				continue
			}
			if matched, delta := peerStore.match(ev); matched {
				stats.add(-delta) // telnet arrival minus peer arrival
				log.Printf("DX %s / DE %s : %s", ev.Spot.DXCall, ev.Spot.DECall, formatDelay(-delta))
			} else {
				telnetStore.add(ev)
			}
		case <-ticker.C:
			peerStore.prune()
			telnetStore.prune()
		}
	}
}

// eventStore holds unmatched events for a limited window.
type eventStore struct {
	window time.Duration
	byKey  map[string][]spotEvent
}

func newEventStore(window time.Duration) *eventStore {
	return &eventStore{
		window: window,
		byKey:  make(map[string][]spotEvent),
	}
}

func (s *eventStore) add(ev spotEvent) {
	key := spotKey(ev.Spot)
	s.byKey[key] = append(s.byKey[key], ev)
}

// match tries to find a counterpart in the store within the window. Returns delta=other.Arrival-ev.Arrival when matched.
func (s *eventStore) match(ev spotEvent) (bool, time.Duration) {
	key := spotKey(ev.Spot)
	candidates := s.byKey[key]
	if len(candidates) == 0 {
		return false, 0
	}
	bestIdx := -1
	bestDiff := s.window + time.Second
	for i, c := range candidates {
		diff := ev.Arrival.Sub(c.Arrival)
		if diff < 0 {
			diff = -diff
		}
		if diff <= s.window && diff < bestDiff {
			bestDiff = diff
			bestIdx = i
		}
	}
	if bestIdx == -1 {
		return false, 0
	}
	match := candidates[bestIdx]
	// remove matched
	candidates = append(candidates[:bestIdx], candidates[bestIdx+1:]...)
	if len(candidates) == 0 {
		delete(s.byKey, key)
	} else {
		s.byKey[key] = candidates
	}
	return true, match.Arrival.Sub(ev.Arrival)
}

func (s *eventStore) prune() {
	cutoff := time.Now().UTC().Add(-s.window)
	for key, list := range s.byKey {
		filtered := list[:0]
		for _, ev := range list {
			if ev.Arrival.After(cutoff) {
				filtered = append(filtered, ev)
			}
		}
		if len(filtered) == 0 {
			delete(s.byKey, key)
		} else {
			s.byKey[key] = filtered
		}
	}
}

func spotKey(s *spot.Spot) string {
	if s == nil {
		return ""
	}
	dx := strings.ToUpper(strings.TrimSpace(s.DXCall))
	de := strings.ToUpper(strings.TrimSpace(s.DECall))
	freq := fmt.Sprintf("%.1f", s.Frequency) // round to 100 Hz resolution
	return dx + "|" + de + "|" + freq
}

// keepaliveLoop sends periodic keepalives to prevent remote idle timeouts.
func keepaliveLoop(writeMu *sync.Mutex, conn net.Conn, pc9x bool, cfg probeConfig, tsGen *timestampGenerator, stop <-chan struct{}) {
	interval := time.Duration(cfg.keepaliveSec) * time.Second
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Always emit PC51 pings so peers expecting legacy liveness see activity even on pc9x.
			pc51 := fmt.Sprintf("PC51^%s^%s^1^", cfg.peerRemoteCall, cfg.localCall)
			log.Printf("KEEPALIVE SEND %s", pc51)
			if err := sendLine(writeMu, conn, pc51); err != nil {
				log.Printf("keepalive send failed (PC51): %v", err)
				return
			}
			// For pc9x sessions, also send a PC92 keepalive to refresh topology.
			if pc9x {
				entry := pc92Entry(cfg.localCall, cfg.nodeVersion, cfg.nodeBuild, cfg.pc92Bitmap)
				ts := tsGen.Next()
				nodes := liveNodeCountProbe()
				users := liveUserCountProbe()
				pc92k := fmt.Sprintf("PC92^%s^%s^K^%s^%d^%d^H%d^", cfg.localCall, ts, entry, nodes, users, cfg.hopCount)
				log.Printf("KEEPALIVE SEND %s", pc92k)
				if err := sendLine(writeMu, conn, pc92k); err != nil {
					log.Printf("keepalive send failed (PC92 K): %v", err)
					return
				}
			}
		case <-stop:
			return
		}
	}
}

// handlePeerPing responds to PC51 ping frames with a PC51 ack, matching the production session logic.
func handlePeerPing(frame *peer.Frame, writeMu *sync.Mutex, conn net.Conn, localCall string) {
	fields := payloadFields(frame.Fields)
	if len(fields) < 3 {
		return
	}
	toNode := strings.TrimSpace(fields[0])
	fromNode := strings.TrimSpace(fields[1])
	flag := strings.TrimSpace(fields[2])
	if flag != "1" {
		return
	}
	call := strings.TrimSpace(localCall)
	if call != "" && !strings.EqualFold(toNode, call) && toNode != "*" && toNode != "" {
		log.Printf("Peering: PC51 ping addressed to %q (local %q); skipping response", toNode, call)
		return
	}
	resp := fmt.Sprintf("PC51^%s^%s^0^", fromNode, toNode)
	log.Printf("Peering: PC51 ping from %s to %s; sending ACK", fromNode, toNode)
	if err := sendLine(writeMu, conn, resp); err != nil {
		log.Printf("Peering: failed to send PC51 ACK: %v", err)
	}
}

// resolveClusterEndpoint derives host and port from flag inputs, allowing cluster_host to include port.
func resolveClusterEndpoint(hostFlag string, portFlag int, defaultPort int) (string, int, error) {
	host := strings.TrimSpace(hostFlag)
	if host == "" {
		host = "localhost"
	}
	// If host includes a colon, try to split host:port.
	if strings.Contains(host, ":") {
		h, p, err := net.SplitHostPort(host)
		if err != nil {
			return "", 0, fmt.Errorf("cluster_host parse: %w", err)
		}
		host = h
		if parsed, err := strconv.Atoi(p); err == nil {
			return host, parsed, nil
		}
		return "", 0, fmt.Errorf("cluster_host port parse failed: %s", p)
	}
	if portFlag <= 0 {
		portFlag = defaultPort
	}
	if portFlag <= 0 {
		return "", 0, fmt.Errorf("cluster_port is not set and default telnet port is zero")
	}
	return host, portFlag, nil
}

type delayStats struct {
	count int64
	sum   time.Duration
	min   time.Duration
	max   time.Duration
}

func newDelayStats() *delayStats {
	return &delayStats{min: time.Duration(1<<63 - 1)}
}

func (d *delayStats) add(delta time.Duration) {
	if d.count == 0 {
		d.min = delta
		d.max = delta
	} else {
		if delta < d.min {
			d.min = delta
		}
		if delta > d.max {
			d.max = delta
		}
	}
	d.count++
	d.sum += delta
}

func formatDelay(d time.Duration) string {
	sign := ""
	if d < 0 {
		sign = "-"
		d = -d
	}
	d = d.Round(time.Second)
	return fmt.Sprintf("%s%s", sign, d)
}

// peerLoop maintains the peer connection with auto-reconnect on errors.
func peerLoop(cfg probeConfig, peerEvents chan<- spotEvent, tsGen *timestampGenerator) {
	for {
		if err := runPeerSession(cfg, peerEvents, tsGen); err != nil {
			log.Printf("Peer session ended: %v; reconnecting in 5s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		return
	}
}

func runPeerSession(cfg probeConfig, peerEvents chan<- spotEvent, tsGen *timestampGenerator) error {
	addr := net.JoinHostPort(cfg.peerHost, fmt.Sprintf("%d", cfg.peerPort))
	conn, err := telnet.DialTimeout("tcp", addr, time.Duration(cfg.loginSec)*time.Second)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	writeMu := &sync.Mutex{}
	reader := peer.NewLineReaderWithTransport(conn, cfg.maxLine, 0, conn.Read, nil, nil)
	idleTimeout := time.Duration(cfg.idleSec) * time.Second

	// Send credentials immediately to match common DXSpider expectations (banner often precedes prompts).
	if cfg.localCall != "" {
		if err := sendLine(writeMu, conn, cfg.localCall); err != nil {
			return fmt.Errorf("send local call: %w", err)
		}
	}
	if cfg.password != "" {
		if err := sendLine(writeMu, conn, cfg.password); err != nil {
			return fmt.Errorf("send password: %w", err)
		}
	}

	established, pc9x, err := handshake(reader, writeMu, conn, cfg, tsGen)
	if err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	if !established {
		return fmt.Errorf("handshake incomplete")
	}
	log.Printf("Handshake established (pc9x=%v). Reading frames...", pc9x)

	stopKA := make(chan struct{})
	go keepaliveLoop(writeMu, conn, pc9x, cfg, tsGen, stopKA)

	errCh := make(chan error, 1)
	go readPeerFeed(conn, reader, writeMu, cfg.localCall, cfg.peerRemoteCall, idleTimeout, peerEvents, errCh)

	err = <-errCh
	close(stopKA)
	return err
}

func handshake(reader *peer.LineReader, writeMu *sync.Mutex, conn net.Conn, cfg probeConfig, tsGen *timestampGenerator) (bool, bool, error) {
	initSent := false
	sentCall := cfg.localCall != ""
	sentPass := cfg.password == ""
	pc9x := false
	deadline := time.Now().UTC().Add(time.Duration(cfg.loginSec+cfg.initSec) * time.Second)

	for {
		if time.Now().UTC().After(deadline) {
			return false, pc9x, fmt.Errorf("handshake timeout")
		}
		line, err := reader.ReadLine(deadline)
		if err != nil {
			return false, pc9x, err
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		log.Printf("HS RX %s", line)
		switch {
		case strings.Contains(line, "PC18^"):
			pc9x = cfg.preferPC9x && strings.Contains(strings.ToLower(line), "pc9x")
			if !sentCall && cfg.localCall != "" {
				if err := sendLine(writeMu, conn, cfg.localCall); err != nil {
					return false, pc9x, fmt.Errorf("send local call: %w", err)
				}
				sentCall = true
			}
			if cfg.password != "" && !sentPass {
				if err := sendLine(writeMu, conn, cfg.password); err != nil {
					return false, pc9x, fmt.Errorf("send password: %w", err)
				}
				sentPass = true
			}
			if !initSent && sentCall && (cfg.password == "" || sentPass) {
				if err := sendInit(writeMu, conn, cfg.localCall, pc9x, cfg.nodeVersion, cfg.nodeBuild, cfg.legacyVersion, cfg.pc92Bitmap, cfg.hopCount, tsGen); err != nil {
					return false, pc9x, err
				}
				initSent = true
			}
		case strings.HasPrefix(strings.ToUpper(line), "PC19^") || strings.HasPrefix(strings.ToUpper(line), "PC16^") || strings.HasPrefix(strings.ToUpper(line), "PC17^") || strings.HasPrefix(strings.ToUpper(line), "PC21^"):
			pc9x = cfg.preferPC9x
			if !sentCall && cfg.localCall != "" {
				if err := sendLine(writeMu, conn, cfg.localCall); err != nil {
					return false, pc9x, fmt.Errorf("send local call: %w", err)
				}
				sentCall = true
			}
			if cfg.password != "" && !sentPass {
				if err := sendLine(writeMu, conn, cfg.password); err != nil {
					return false, pc9x, fmt.Errorf("send password: %w", err)
				}
				sentPass = true
			}
			if !initSent && sentCall && (cfg.password == "" || sentPass) {
				if err := sendInit(writeMu, conn, cfg.localCall, pc9x, cfg.nodeVersion, cfg.nodeBuild, cfg.legacyVersion, cfg.pc92Bitmap, cfg.hopCount, tsGen); err != nil {
					return false, pc9x, err
				}
				initSent = true
			}
		case strings.HasPrefix(strings.ToUpper(line), "PC22") && initSent:
			return true, pc9x, nil
		case initSent && (strings.HasPrefix(strings.ToUpper(line), "PC11^") || strings.HasPrefix(strings.ToUpper(line), "PC61^")):
			return true, pc9x, nil
		default:
			// ignore banners and prompts
		}
	}
}

func sendInit(mu *sync.Mutex, conn net.Conn, localCall string, pc9x bool, nodeVersion, nodeBuild, legacy string, pc92Bitmap, hopCount int, tsGen *timestampGenerator) error {
	if pc9x {
		entry := pc92Entry(localCall, nodeVersion, nodeBuild, pc92Bitmap)
		ts := tsGen.Next()
		if err := sendLine(mu, conn, fmt.Sprintf("PC92^%s^%s^A^^%s^H%d^", localCall, ts, entry, hopCount)); err != nil {
			return err
		}
		nodes := liveNodeCountProbe()
		users := liveUserCountProbe()
		if err := sendLine(mu, conn, fmt.Sprintf("PC92^%s^%s^K^%s^%d^%d^H%d^", localCall, tsGen.Next(), entry, nodes, users, hopCount)); err != nil {
			return err
		}
		return sendLine(mu, conn, "PC20^")
	}
	line := fmt.Sprintf("PC19^1^%s^0^%s^H%d^", localCall, legacy, hopCount)
	if err := sendLine(mu, conn, line); err != nil {
		return err
	}
	return sendLine(mu, conn, "PC20^")
}

func sendLine(mu *sync.Mutex, conn net.Conn, line string) error {
	if !strings.HasSuffix(line, "\n") {
		line += "\r\n"
	}
	mu.Lock()
	defer mu.Unlock()
	_, err := conn.Write([]byte(line))
	return err
}

func pc92Entry(localCall, nodeVersion, nodeBuild string, bitmap int) string {
	entry := fmt.Sprintf("%d%s:%s", bitmap, localCall, nodeVersion)
	if strings.TrimSpace(nodeBuild) != "" {
		entry += ":" + strings.TrimSpace(nodeBuild)
	}
	return entry
}

// liveNodeCountProbe mirrors the cluster's "1 + active peers" logic; the probe only
// maintains one peer session, so we report 1 (self). Adjust here if the probe gains
// more endpoints.
func liveNodeCountProbe() int {
	return 1
}

// liveUserCountProbe mirrors the cluster's live telnet user count; the probe has no
// telnet listeners, so we report 0.
func liveUserCountProbe() int {
	return 0
}

// payloadFields returns non-hop payload fields with trailing empties preserved.
func payloadFields(fields []string) []string {
	if len(fields) == 0 {
		return fields
	}
	out := make([]string, len(fields))
	copy(out, fields)
	last := strings.TrimSpace(out[len(out)-1])
	if strings.HasPrefix(last, "H") || strings.HasPrefix(last, "h") {
		out = out[:len(out)-1]
	}
	return out
}

// timestampGenerator mirrors the session helper to produce PC92 timestamps.
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
