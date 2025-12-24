// Command peerprobe connects to a configured DXSpider peer, exercises the same
// PC frame parser, and prints both the raw inbound line and the parsed frame/spot
// to stdout. It is a standalone debugging utility that shares the main cluster
// configuration but does not start any other services.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
	"strconv"

	"dxcluster/config"
	"dxcluster/peer"
	"dxcluster/rbn"
	"dxcluster/spot"

	_ "unsafe" // for go:linkname to reuse the real spot parser
)

//go:linkname parseSpotFromFrame dxcluster/peer.parseSpotFromFrame
func parseSpotFromFrame(frame *peer.Frame, fallbackOrigin string) (*spot.Spot, error)

func main() {
	cfgPath := flag.String("config", "data/config", "Path to config directory or file")
	clusterHost := flag.String("cluster_host", "localhost", "Host for telnet cluster comparison (hostname or host:port)")
	clusterPort := flag.Int("cluster_port", 0, "Port for telnet cluster comparison (optional when cluster_host includes port)")
	clusterCall := flag.String("cluster_call", "LZ3ZZ", "Callsign to use when logging into cluster telnet")
	windowMinutes := flag.Int("window_minutes", 3, "Matching window (minutes) between peer and telnet spots")
	flag.Parse()

	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("config load: %v", err)
	}
	if !cfg.Peering.Enabled {
		log.Fatalf("peering is disabled in config")
	}
	peerCfg, ok := firstEnabledPeer(cfg.Peering.Peers)
	if !ok {
		log.Fatalf("no enabled peers found")
	}

	peerEvents := make(chan spotEvent, 1024)
	telnetEvents := make(chan spotEvent, 1024)

	// Start telnet tap to our cluster using the existing minimal parser from rbn.Client.
	clusterHostName, clusterHostPort, err := resolveClusterEndpoint(*clusterHost, *clusterPort, cfg.Telnet.Port)
	if err != nil {
		log.Fatalf("invalid cluster endpoint: %v", err)
	}
	startTelnetTap(clusterHostName, clusterHostPort, *clusterCall, telnetEvents)

	// Start peer loop (auto-reconnect on EOF) in background.
	tsGen := &timestampGenerator{}
	go peerLoop(cfg, peerEvents, peerCfg, tsGen)

	matchWindow := time.Duration(*windowMinutes) * time.Minute
	runMatcher(matchWindow, peerEvents, telnetEvents)
}

func loadConfig(path string) (*config.Config, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// readPeerFeed consumes PC frames from the peer connection and emits spot events for PC11/PC61.
// It also replies to PC51 pings to keep the session alive. On read error, it reports the error
// to errOut and returns so the caller can reconnect.
func readPeerFeed(conn net.Conn, reader *lineReader, writer *bufio.Writer, writeMu *sync.Mutex, pc9x bool, localCall string, fallbackOrigin string, tsGen *timestampGenerator, out chan<- spotEvent, errOut chan<- error) {
	for {
		_ = conn.SetReadDeadline(time.Time{})
		line, err := reader.ReadLine()
		if err != nil {
			if errOut != nil {
				errOut <- err
			}
			return
		}
		arrival := time.Now()
		if strings.TrimSpace(line) == "" {
			continue
		}
		frame, err := peer.ParseFrame(line)
		if err != nil {
			continue
		}
		switch frame.Type {
		case "PC51":
			handlePeerPing(frame, writeMu, writer, localCall, pc9x, tsGen)
		case "PC11", "PC61":
			if s, err := parseSpotFromFrame(frame, fallbackOrigin); err == nil {
				s.RefreshBeaconFlag()
				s.EnsureNormalized()
				log.Printf("PEER ARRIVAL %s DX %s DE %s", arrival.Format(time.RFC3339Nano), s.DXCall, s.DECall)
				out <- spotEvent{Spot: s, Arrival: arrival, Source: "peer"}
			} else {
				// Silently drop parse errors; match analysis is noise-free.
			}
		}
	}
}

// startTelnetTap connects to the local cluster telnet server and streams broadcast spots via the rbn minimal parser.
func startTelnetTap(host string, port int, callsign string, out chan<- spotEvent) {
	client := rbn.NewClient(host, port, callsign, "CLUSTER-TAP", nil, nil, true, 1024)
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
			arrival := time.Now()
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
			if matched, delta, _ := telnetStore.match(ev); matched {
				stats.add(delta)
				log.Printf("DX %s / DE %s : %s", ev.Spot.DXCall, ev.Spot.DECall, formatDelay(delta))
			} else {
				peerStore.add(ev)
			}
		case ev := <-telnetEvents:
			if ev.Spot == nil {
				continue
			}
			if matched, delta, _ := peerStore.match(ev); matched {
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
func (s *eventStore) match(ev spotEvent) (bool, time.Duration, spotEvent) {
	key := spotKey(ev.Spot)
	candidates := s.byKey[key]
	if len(candidates) == 0 {
		return false, 0, spotEvent{}
	}
	var bestIdx int = -1
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
		return false, 0, spotEvent{}
	}
	match := candidates[bestIdx]
	// remove matched
	candidates = append(candidates[:bestIdx], candidates[bestIdx+1:]...)
	if len(candidates) == 0 {
		delete(s.byKey, key)
	} else {
		s.byKey[key] = candidates
	}
	return true, match.Arrival.Sub(ev.Arrival), match
}

func (s *eventStore) prune() {
	cutoff := time.Now().Add(-s.window)
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

func (s *eventStore) len() int {
	total := 0
	for _, list := range s.byKey {
		total += len(list)
	}
	return total
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
func keepaliveLoop(writeMu *sync.Mutex, writer *bufio.Writer, pc9x bool, cfg config.PeeringConfig, peerCfg config.PeeringPeer, tsGen *timestampGenerator, stop <-chan struct{}) {
	// The probe sends its own keepalives every 30 seconds to mirror common DXSpider expectations.
	interval := 30 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if pc9x {
				entry := pc92Entry(cfg.LocalCallsign, cfg.NodeVersion, cfg.NodeBuild, cfg.PC92Bitmap)
				ts := tsGen.Next()
				_ = sendLine(writeMu, writer, fmt.Sprintf("PC92^%s^%s^K^%s^%d^%d^H%d^", cfg.LocalCallsign, ts, entry, cfg.NodeCount, cfg.UserCount, cfg.HopCount))
			} else {
				_ = sendLine(writeMu, writer, fmt.Sprintf("PC51^%s^%s^1^", peerCfg.RemoteCallsign, cfg.LocalCallsign))
			}
		case <-stop:
			return
		}
	}
}

// handlePeerPing responds to PC51 ping frames.
func handlePeerPing(frame *peer.Frame, writeMu *sync.Mutex, writer *bufio.Writer, localCall string, pc9x bool, tsGen *timestampGenerator) {
	fields := frame.Fields
	if len(fields) < 3 {
		return
	}
	toNode := strings.TrimSpace(fields[0])
	fromNode := strings.TrimSpace(fields[1])
	flag := strings.TrimSpace(fields[2])
	if flag != "1" {
		return
	}
	if localCall != "" && !strings.EqualFold(toNode, localCall) && toNode != "*" {
		return
	}
	if pc9x {
		entry := pc92Entry(localCall, "", "", 0)
		ts := tsGen.Next()
		_ = sendLine(writeMu, writer, fmt.Sprintf("PC92^%s^%s^K^%s^0^0^H0^", localCall, ts, entry))
	} else {
		_ = sendLine(writeMu, writer, fmt.Sprintf("PC51^%s^%s^0^", fromNode, toNode))
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
	return fmt.Sprintf("%s%s", sign, d)
}

// peerLoop maintains the peer connection with auto-reconnect on errors.
func peerLoop(cfg *config.Config, peerEvents chan<- spotEvent, peerCfg config.PeeringPeer, tsGen *timestampGenerator) {
	for {
		if err := runPeerSession(cfg, peerEvents, peerCfg, tsGen); err != nil {
			log.Printf("Peer session ended: %v; reconnecting in 5s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		return
	}
}

func runPeerSession(cfg *config.Config, peerEvents chan<- spotEvent, peerCfg config.PeeringPeer, tsGen *timestampGenerator) error {
	addr := net.JoinHostPort(peerCfg.Host, fmt.Sprintf("%d", peerCfg.Port))
	conn, err := net.DialTimeout("tcp", addr, time.Duration(cfg.Peering.Timeouts.LoginSeconds)*time.Second)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	reader := newLineReader(conn, cfg.Peering.MaxLineLength)
	writer := bufio.NewWriter(conn)
	writeMu := &sync.Mutex{}

	// Send credentials immediately to match common DXSpider expectations (banner often precedes prompts).
	if cfg.Peering.LocalCallsign != "" {
		_ = sendLine(writeMu, writer, cfg.Peering.LocalCallsign)
	}
	if peerCfg.Password != "" {
		_ = sendLine(writeMu, writer, peerCfg.Password)
	}

	established, pc9x, err := handshake(context.Background(), reader, writeMu, writer, cfg, peerCfg, tsGen)
	if err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	if !established {
		return fmt.Errorf("handshake incomplete")
	}
	log.Printf("Handshake established (pc9x=%v). Reading frames...", pc9x)

	stopKA := make(chan struct{})
	go keepaliveLoop(writeMu, writer, pc9x, cfg.Peering, peerCfg, tsGen, stopKA)

	errCh := make(chan error, 1)
	go readPeerFeed(conn, reader, writer, writeMu, pc9x, cfg.Peering.LocalCallsign, peerCfg.RemoteCallsign, tsGen, peerEvents, errCh)

	err = <-errCh
	close(stopKA)
	return err
}

func firstEnabledPeer(peers []config.PeeringPeer) (config.PeeringPeer, bool) {
	for _, p := range peers {
		if p.Enabled {
			return p, true
		}
	}
	return config.PeeringPeer{}, false
}

func handshake(ctx context.Context, reader *lineReader, writeMu *sync.Mutex, writer *bufio.Writer, cfg *config.Config, peerCfg config.PeeringPeer, tsGen *timestampGenerator) (bool, bool, error) {
	initSent := false
	sentCall := cfg.Peering.LocalCallsign != ""
	sentPass := peerCfg.Password == ""
	pc9x := false
	deadline := time.Now().Add(time.Duration(cfg.Peering.Timeouts.LoginSeconds+cfg.Peering.Timeouts.InitSeconds) * time.Second)

	for {
		if time.Now().After(deadline) {
			return false, pc9x, fmt.Errorf("handshake timeout")
		}
		if err := connDeadline(reader.conn, deadline); err != nil {
			return false, pc9x, err
		}
		line, err := reader.ReadLine()
		if err != nil {
			return false, pc9x, err
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		log.Printf("HS RX %s", line)
		switch {
		case strings.Contains(line, "PC18^"):
			pc9x = peerCfg.PreferPC9x && strings.Contains(strings.ToLower(line), "pc9x")
			if !sentCall && cfg.Peering.LocalCallsign != "" {
				sendLine(writeMu, writer, cfg.Peering.LocalCallsign)
				sentCall = true
			}
			if peerCfg.Password != "" && !sentPass {
				sendLine(writeMu, writer, peerCfg.Password)
				sentPass = true
			}
			if !initSent && sentCall && (peerCfg.Password == "" || sentPass) {
				if err := sendInit(writeMu, writer, cfg.Peering.LocalCallsign, pc9x, cfg.Peering.NodeVersion, cfg.Peering.NodeBuild, cfg.Peering.LegacyVersion, cfg.Peering.PC92Bitmap, cfg.Peering.NodeCount, cfg.Peering.UserCount, cfg.Peering.HopCount, tsGen); err != nil {
					return false, pc9x, err
				}
				initSent = true
			}
		case strings.HasPrefix(strings.ToUpper(line), "PC19^") || strings.HasPrefix(strings.ToUpper(line), "PC16^") || strings.HasPrefix(strings.ToUpper(line), "PC17^") || strings.HasPrefix(strings.ToUpper(line), "PC21^"):
			pc9x = peerCfg.PreferPC9x
			if !sentCall && cfg.Peering.LocalCallsign != "" {
				sendLine(writeMu, writer, cfg.Peering.LocalCallsign)
				sentCall = true
			}
			if peerCfg.Password != "" && !sentPass {
				sendLine(writeMu, writer, peerCfg.Password)
				sentPass = true
			}
			if !initSent && sentCall && (peerCfg.Password == "" || sentPass) {
				if err := sendInit(writeMu, writer, cfg.Peering.LocalCallsign, pc9x, cfg.Peering.NodeVersion, cfg.Peering.NodeBuild, cfg.Peering.LegacyVersion, cfg.Peering.PC92Bitmap, cfg.Peering.NodeCount, cfg.Peering.UserCount, cfg.Peering.HopCount, tsGen); err != nil {
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

func connDeadline(conn net.Conn, deadline time.Time) error {
	return conn.SetReadDeadline(deadline)
}

func sendInit(mu *sync.Mutex, w *bufio.Writer, localCall string, pc9x bool, nodeVersion, nodeBuild, legacy string, pc92Bitmap, nodeCount, userCount, hopCount int, tsGen *timestampGenerator) error {
	if pc9x {
		entry := pc92Entry(localCall, nodeVersion, nodeBuild, pc92Bitmap)
		ts := tsGen.Next()
		if err := sendLine(mu, w, fmt.Sprintf("PC92^%s^%s^A^^%s^H%d^", localCall, ts, entry, hopCount)); err != nil {
			return err
		}
		if err := sendLine(mu, w, fmt.Sprintf("PC92^%s^%s^K^%s^%d^%d^H%d^", localCall, tsGen.Next(), entry, nodeCount, userCount, hopCount)); err != nil {
			return err
		}
		return sendLine(mu, w, "PC20^")
	}
	line := fmt.Sprintf("PC19^1^%s^0^%s^H%d^", localCall, legacy, hopCount)
	if err := sendLine(mu, w, line); err != nil {
		return err
	}
	return sendLine(mu, w, "PC20^")
}

func sendLine(mu *sync.Mutex, w *bufio.Writer, line string) error {
	if !strings.HasSuffix(line, "\n") {
		line += "\r\n"
	}
	mu.Lock()
	defer mu.Unlock()
	if _, err := w.WriteString(line); err != nil {
		return err
	}
	return w.Flush()
}

func pc92Entry(localCall, nodeVersion, nodeBuild string, bitmap int) string {
	entry := fmt.Sprintf("%d%s:%s", bitmap, localCall, nodeVersion)
	if strings.TrimSpace(nodeBuild) != "" {
		entry += ":" + strings.TrimSpace(nodeBuild)
	}
	return entry
}

// --- line reader (copied from peer/reader.go to mirror the production parser) ---

type lineReader struct {
	conn   net.Conn
	parser *telnetParser
	buf    []byte
	maxLen int
}

func newLineReader(conn net.Conn, maxLen int) *lineReader {
	if maxLen <= 0 {
		maxLen = 4096
	}
	return &lineReader{
		conn:   conn,
		parser: &telnetParser{},
		buf:    make([]byte, 0, maxLen),
		maxLen: maxLen,
	}
}

func (r *lineReader) ReadLine() (string, error) {
	for {
		chunk := make([]byte, 1024)
		n, err := r.conn.Read(chunk)
		if n > 0 {
			out, replies := r.parser.Feed(chunk[:n])
			if len(replies) > 0 {
				for _, rep := range replies {
					_, _ = r.conn.Write(rep)
				}
			}
			r.buf = append(r.buf, out...)
			for {
				r.buf = trimLeadingTerminators(r.buf)
				if len(r.buf) == 0 {
					break
				}
				if idx, size := bytesIndexTerminator(r.buf); idx >= 0 {
					line := string(trimLine(r.buf[:idx]))
					r.buf = append([]byte{}, r.buf[idx+size:]...)
					return line, nil
				}
				if start := bytesIndexFrameStart(r.buf); start > 0 {
					r.buf = r.buf[start:]
					continue
				}
				if len(r.buf) > r.maxLen && r.maxLen > 0 {
					preview := string(r.buf)
					r.buf = r.buf[:0]
					return "", fmt.Errorf("line too long: %d bytes (preview: %q)", len(preview), preview)
				}
				break
			}
		}
		if err != nil {
			return "", err
		}
	}
}

func trimLine(b []byte) []byte {
	for len(b) > 0 {
		if b[len(b)-1] == '\n' || b[len(b)-1] == '\r' {
			b = b[:len(b)-1]
		} else {
			break
		}
	}
	return b
}

func trimLeadingTerminators(b []byte) []byte {
	for len(b) > 0 {
		if isTerminator(b[0]) {
			b = b[1:]
			continue
		}
		break
	}
	return b
}

func isTerminator(b byte) bool {
	return b == '\n' || b == '\r' || b == '~'
}

func bytesIndexTerminator(b []byte) (int, int) {
	for i := 0; i < len(b); i++ {
		switch b[i] {
		case '~':
			return i, 1
		case '\n':
			return i, 1
		case '\r':
			if i+1 < len(b) && b[i+1] == '\n' {
				return i, 2
			}
			return i, 1
		}
	}
	return -1, 0
}

func bytesIndexFrameStart(b []byte) int {
	if isFrameStartAt(b, 0) {
		return 0
	}
	for i := 1; i < len(b); i++ {
		if !isTerminator(b[i-1]) {
			continue
		}
		if isFrameStartAt(b, i) {
			return i
		}
	}
	return -1
}

func isFrameStartAt(b []byte, i int) bool {
	if i+4 >= len(b) {
		return false
	}
	if b[i] != 'P' || b[i+1] != 'C' {
		return false
	}
	if b[i+2] < '0' || b[i+2] > '9' || b[i+3] < '0' || b[i+3] > '9' {
		return false
	}
	return b[i+4] == '^'
}

// telnetParser strips telnet IAC sequences and returns clean payload bytes plus replies.
// telnetParser strips telnet IAC sequences and returns clean payload bytes plus replies.
// For this probe we pass through bytes as-is to mirror the RBN/human ingest behavior.
type telnetParser struct{}

func (p *telnetParser) Feed(input []byte) (output []byte, replies [][]byte) {
	return input, nil
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
