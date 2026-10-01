//go:build !qualification

package cluster

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/config"
)

// This deliberately exercises the production runtime, not a second pipeline.
// The opt-in smoke establishes functional reachability and externally timed
// delivery. It cannot qualify Q1-Q6: enqueue has no correlated observer, graph
// occupancy is not established, and ten seconds is not a qualification window.
func TestPC92RuntimeQualificationSmoke(t *testing.T) {
	if os.Getenv("GOCLUSTER_PC92_RUNTIME_PROFILE") != "smoke" {
		t.Skip("opt-in real runtime smoke; use scripts/pc92-runtime-qualification.ps1")
	}
	output := os.Getenv("GOCLUSTER_PC92_RUNTIME_OUTPUT")
	if !filepath.IsAbs(output) {
		t.Fatal("GOCLUSTER_PC92_RUNTIME_OUTPUT must name an absolute artifact path")
	}
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(repo, "data", "config")
	cfg, err := config.Load(configDir)
	if err != nil {
		t.Fatal(err)
	}
	// No deployed data or network feed is used. Reference assets remain read-only;
	// persistence and all relative log paths resolve under the isolated workdir.
	t.Chdir(t.TempDir())
	cfg.RBN.Enabled, cfg.RBNDigital.Enabled = false, false
	for i := range cfg.HumanTelnet {
		cfg.HumanTelnet[i].Enabled = false
	}
	cfg.PSKReporter.Enabled, cfg.DXSummit.Enabled = false, false
	cfg.FCCULS.Enabled, cfg.Reputation.Enabled, cfg.Skew.Enabled = false, false, false
	cfg.SolarWeather.Enabled, cfg.PropReport.Enabled = false, false
	cfg.PathReliability.VOACAPFallback.Enabled = false
	cfg.CTY.File, cfg.CTY.URL = filepath.Join(repo, "data", "cty", "cty.plist"), ""
	cfg.H3TablePath = filepath.Join(repo, "data", "h3")
	for _, path := range []*string{&cfg.CallCorrection.ConfusionModelFile, &cfg.CallCorrection.SpotterReliabilityFile, &cfg.CallCorrection.SpotterReliabilityFileCW, &cfg.CallCorrection.SpotterReliabilityFileRTTY} {
		if *path != "" && !filepath.IsAbs(*path) {
			*path = filepath.Join(repo, *path)
		}
	}
	cfg.UI.Mode = "headless"
	cfg.Telnet.Port = runtimeQualificationPort(t)
	cfg.Peering.ListenPort = runtimeQualificationPort(t)
	cfg.Peering.Enabled, cfg.Peering.ForwardSpots = true, true
	cfg.Peering.LocalCallsign = "N0CALL-1"
	cfg.Peering.ACL = config.PeeringACL{}
	cfg.Peering.Peers = nil
	const clients, peers, uniqueSpots = 100, 16, 1670
	for i := 0; i < peers; i++ {
		cfg.Peering.Peers = append(cfg.Peering.Peers, config.PeeringPeer{
			Enabled: true, Family: "dxspider", Direction: "inbound", PreferPC9x: true,
			RemoteCallsign: fmt.Sprintf("DL%dPAA", i+1), LoginCallsign: "N0CALL-1",
		})
	}
	if cfg.GoRuntime.MaxProcs != 2 || cfg.GoRuntime.GCPercent != 50 || cfg.GoRuntime.MemoryLimitMiB != 1536 || cfg.Peering.MaxLineLength != 65536 {
		t.Fatal("shipped qualification runtime settings changed; reconcile the contract")
	}
	applyGoRuntimeTuning(cfg.GoRuntime)
	r := newClusterRuntime(BuildInfo{Version: "runtime-qualification", Commit: "local", BuildTime: time.Now().UTC().Format(time.RFC3339), VCSModified: "true", GoVersion: runtime.Version()}, cfg, configDir, config.LoadDiagnostics{})
	defer r.close()
	if !r.initialize() {
		t.Fatalf("runtime startup: %v", r.startupErr)
	}
	defer r.shutdown()

	tracker := &runtimeQualificationTracker{starts: make(map[string]time.Time, uniqueSpots)}
	var all []*runtimeQualificationRecipient
	defer func() {
		for _, recipient := range all {
			_ = recipient.conn.Close()
		}
		for _, recipient := range all {
			<-recipient.done
		}
	}()
	peerAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.Peering.ListenPort))
	for i := 0; i < peers; i++ {
		conn, reader := runtimeQualificationLogin(t, peerAddr, "", cfg.Peering.Peers[i].RemoteCallsign)
		if _, err := io.WriteString(conn, "PC18^DXSpider Version: 1.57 Build: 633 [pc9x 91]^5457^\r\nPC20^\r\n"); err != nil {
			t.Fatal(err)
		}
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatalf("peer %d handshake: %v", i, err)
			}
			if strings.TrimSpace(line) == "PC22^" {
				break
			}
		}
		_ = conn.SetDeadline(time.Time{})
		recipient := newRuntimeQualificationRecipient(conn, true, fmt.Sprintf("peer-%d", i), tracker)
		all = append(all, recipient)
		go recipient.read(reader)
	}
	clientAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.Telnet.Port))
	for i := 0; i < clients; i++ {
		// Distinct loopback source addresses preserve shipped per-IP admission.
		// 20 logins/sec remains below the shipped subnet admission rate.
		conn, reader := runtimeQualificationLogin(t, clientAddr, fmt.Sprintf("127.0.0.%d", i+2), fmt.Sprintf("DL%dCAA", i+1))
		_ = conn.SetDeadline(time.Time{})
		recipient := newRuntimeQualificationRecipient(conn, false, fmt.Sprintf("client-%d", i), tracker)
		all = append(all, recipient)
		go recipient.read(reader)
		time.Sleep(50 * time.Millisecond)
	}
	deadline := time.Now().Add(10 * time.Second)
	for r.telnetServer.GetClientCount() != clients && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := r.telnetServer.GetClientCount(); got != clients {
		t.Fatalf("only %d/%d clients admitted", got, clients)
	}

	// 1 distinct key each 6 ms = 10,000/minute. Ten repeated copies per key
	// produce the required 100,000/minute duplicate arrival ratio. Time starts
	// before the socket write, never at a downstream queue or surviving output.
	start := time.Now()
	frameCounts := map[string]int{}
	for i := 0; i < uniqueSpots; i++ {
		if wait := time.Until(start.Add(time.Duration(i) * 6 * time.Millisecond)); wait > 0 {
			time.Sleep(wait)
		}
		dx := fmt.Sprintf("DL%dQAA", 10000+i)
		kind := "PC61"
		if i%10 >= 4 {
			kind = "PC11"
		}
		if i%10 >= 8 {
			kind = "PC26"
		}
		freq, mode := "14020.0", "CW"
		if i%2 == 1 {
			freq, mode = "14250.0", "SSB"
		}
		now := time.Now().UTC()
		line := fmt.Sprintf("%s^%s^%s^%s^%s^%s^DL1AAA^DL1PAA^", kind, freq, dx, now.Format("02-Jan-2006"), now.Format("1504Z"), mode)
		if kind == "PC61" {
			line += "192.0.2.1^"
		}
		if kind == "PC26" {
			line += "^"
		}
		line += "H10^\r\n"
		if len(line) > 512 {
			t.Fatal("spot generator exceeded approved ordinary frame size")
		}
		tracker.mu.Lock()
		tracker.starts[dx] = time.Now()
		tracker.mu.Unlock()
		if _, err := io.WriteString(all[0].conn, strings.Repeat(line, 11)); err != nil {
			t.Fatalf("spot %d send: %v", i, err)
		}
		frameCounts[kind]++
	}
	elapsed := time.Since(start)
	// The shipped stabilizer can retain an uncertain valid call for 5*15 s.
	// Keep the observation window long enough to distinguish loss from that
	// existing policy without changing the policy to make the smoke pass.
	t.Logf("after load: ingest=%d stabilizer held=%d immediate=%d delayed=%d", r.ingestValidator.IngestCount(), r.statsTracker.StabilizerHeld(), r.statsTracker.StabilizerReleasedImmediate(), r.statsTracker.StabilizerReleasedDelayed())
	time.Sleep(80 * time.Second)
	for _, recipient := range all {
		_ = recipient.conn.Close()
	}
	for _, recipient := range all {
		<-recipient.done
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	report := runtimeQualificationReport{
		Profile: "smoke", QualificationPassed: false, Clients: clients, Peers: peers,
		UniqueKeys: uniqueSpots, DuplicateArrivals: uniqueSpots * 10, FrameCounts: frameCounts,
		LoadSeconds: elapsed.Seconds(), DrainSeconds: 80, GoVersion: runtime.Version(),
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, CPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		HeapAlloc: mem.HeapAlloc, HeapInuse: mem.HeapInuse, Sys: mem.Sys,
		Limitations: []string{
			"Ten-second smoke only; does not execute Q1-Q6 or fixed full-minute latency windows.",
			"External pre-write to receiver first read byte is measured; no correlated enqueue observer exists.",
			"No required topology occupancy, PC92/PC93/WWV mixed load, pending candidates, faults, or cache saturation is established.",
			"Runtime and generator share the test process and GOMAXPROCS=2; hardware values include generator overhead.",
			"External feeds, downloaders, reputation, solar, skew and propagation schedulers are disabled in the isolated config copy.",
			"All shipped pipeline, broadcast batching, client filters, admission and queue settings are preserved.",
			"Correlation uses the original DX call: absent original-call output is unattributed, not proof of network loss; correction and suppression remain enabled.",
		},
	}
	report.Ingested = r.ingestValidator.IngestCount()
	report.StabilizerHeld = r.statsTracker.StabilizerHeld()
	report.StabilizerImmediate = r.statsTracker.StabilizerReleasedImmediate()
	report.StabilizerDelayed = r.statsTracker.StabilizerReleasedDelayed()
	report.StabilizerReasons = r.statsTracker.StabilizerHeldByReason()
	report.BroadcastBatchMS = cfg.Telnet.BroadcastBatchIntervalMS
	report.PrimaryProcessed, report.PrimaryDuplicates, _ = r.deduplicator.GetStats()
	report.SecondaryProcessed, report.SecondaryDuplicates, _ = r.secondarySlow.GetStats()
	missing := 0
	for i, recipient := range all {
		expected := uniqueSpots
		if i == 0 {
			expected = 0 // Source peer must not receive its own spot back.
		}
		result := recipient.result(expected)
		report.Recipients = append(report.Recipients, result)
		missing += result.Missing
		if !recipient.peer && result.P99MS > report.WorstClientP99MS {
			report.WorstClientP99MS = result.P99MS
		}
		if result.Unexpected > 0 || result.Duplicates > 0 {
			t.Errorf("%s unexpected=%d duplicate=%d", result.Name, result.Unexpected, result.Duplicates)
		}
	}
	report.MissingDeliveries = missing
	report.FirstByteTargetMet = missing == 0 && report.WorstClientP99MS <= 25
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("runtime smoke: %d unique keys, unobserved original-call outputs=%d (unattributed), worst client external p99=%.3fms, artifact=%s; NOT full qualification", uniqueSpots, missing, report.WorstClientP99MS, output)
	if missing != 0 {
		t.Errorf("%d original-call outputs unobserved (unattributed); correction/suppression can change this count, so this does not establish network loss or qualification", missing)
	}
}

type runtimeQualificationReport struct {
	PrimaryProcessed, PrimaryDuplicates, SecondaryProcessed, SecondaryDuplicates uint64
	BroadcastBatchMS                                                             int `json:"broadcast_batch_ms"`
	Ingested, StabilizerHeld, StabilizerImmediate, StabilizerDelayed             uint64
	StabilizerReasons                                                            map[string]uint64
	Profile                                                                      string `json:"profile"`
	QualificationPassed                                                          bool   `json:"qualification_passed"`
	Clients, Peers                                                               int
	UniqueKeys                                                                   int            `json:"unique_keys"`
	DuplicateArrivals                                                            int            `json:"duplicate_arrivals"`
	FrameCounts                                                                  map[string]int `json:"new_key_frame_counts"`
	LoadSeconds, DrainSeconds                                                    float64
	GoVersion, GOOS, GOARCH                                                      string
	CPUs, GOMAXPROCS                                                             int
	HeapAlloc, HeapInuse, Sys                                                    uint64
	MissingDeliveries                                                            int                          `json:"unobserved_original_call_outputs_unattributed"`
	WorstClientP99MS                                                             float64                      `json:"worst_client_external_p99_ms"`
	FirstByteTargetMet                                                           bool                         `json:"smoke_first_byte_target_met"`
	Limitations                                                                  []string                     `json:"limitations"`
	Recipients                                                                   []runtimeQualificationResult `json:"recipients"`
}

type runtimeQualificationTracker struct {
	mu     sync.RWMutex
	starts map[string]time.Time
}

type runtimeQualificationRecipient struct {
	conn              net.Conn
	peer              bool
	name              string
	tracker           *runtimeQualificationTracker
	done              chan struct{}
	seen              map[string]struct{}
	samples           []time.Duration
	duplicates        int
	err               string
	untracked         int
	untrackedExamples []string
}

type runtimeQualificationResult struct {
	Name                                                string `json:"name"`
	Expected, Received, Missing, Unexpected, Duplicates int
	P99MS                                               float64  `json:"external_first_byte_p99_ms"`
	ReadError                                           string   `json:"read_error,omitempty"`
	UntrackedOutput                                     int      `json:"untracked_spot_output"`
	UntrackedExamples                                   []string `json:"untracked_output_examples,omitempty"`
	MissingExamples                                     []string `json:"missing_original_call_examples,omitempty"`
}

type runtimeQualificationReadSpan struct {
	start int
	at    time.Time
}

func newRuntimeQualificationRecipient(conn net.Conn, peer bool, name string, tracker *runtimeQualificationTracker) *runtimeQualificationRecipient {
	return &runtimeQualificationRecipient{conn: conn, peer: peer, name: name, tracker: tracker, done: make(chan struct{}), seen: make(map[string]struct{})}
}

func (r *runtimeQualificationRecipient) read(reader *bufio.Reader) {
	defer close(r.done)
	buf := make([]byte, 8192)
	line := make([]byte, 0, 256)
	spans := make([]runtimeQualificationReadSpan, 0, 4)
	for {
		n, err := reader.Read(buf)
		readAt := time.Now()
		for i, b := range buf[:n] {
			if i == 0 || len(line) == 0 {
				spans = append(spans, runtimeQualificationReadSpan{start: len(line), at: readAt})
			}
			if b == '\n' {
				r.observe(string(line), spans)
				line = line[:0]
				spans = spans[:0]
				continue
			}
			line = append(line, b)
			if len(line) > 65538 {
				r.err = "received line exceeds peer frame bound"
				return
			}
		}
		if err != nil {
			if !strings.Contains(err.Error(), "use of closed network connection") && err != io.EOF {
				r.err = err.Error()
			}
			return
		}
	}
}

func (r *runtimeQualificationRecipient) observe(line string, spans []runtimeQualificationReadSpan) {
	var dx string
	var offset int
	if r.peer {
		fields := strings.Split(line, "^")
		if len(fields) > 2 && (fields[0] == "PC11" || fields[0] == "PC61" || fields[0] == "PC26") {
			dx = fields[2]
		}
	} else if at := strings.Index(line, "DX de "); at >= 0 {
		offset = at
		fields := strings.Fields(line[at:])
		if len(fields) > 4 {
			dx = fields[4]
		}
	}
	r.tracker.mu.RLock()
	start, ok := r.tracker.starts[dx]
	r.tracker.mu.RUnlock()
	if !ok {
		if dx != "" {
			r.untracked++
			if len(r.untrackedExamples) < 10 {
				r.untrackedExamples = append(r.untrackedExamples, line)
			}
		}
		return
	}
	// A prompt can share the first DX line without a newline. Use the read
	// containing the DX prefix itself, not the earlier prompt's timestamp.
	var first time.Time
	for _, span := range spans {
		if span.start > offset {
			break
		}
		first = span.at
	}
	if _, duplicate := r.seen[dx]; duplicate {
		r.duplicates++
		return
	}
	r.seen[dx] = struct{}{}
	r.samples = append(r.samples, first.Sub(start))
}

func TestRuntimeQualificationCorrelationStartsAtDXBytes(t *testing.T) {
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	tracker := &runtimeQualificationTracker{starts: map[string]time.Time{"DL10000QAA": start}}
	recipient := newRuntimeQualificationRecipient(nil, false, "client", tracker)
	prompt := "DL1CAA de N0CALL-1> "
	line := prompt + "DX de DL1AAA: 14020.0 DL10000QAA CW 1200Z"
	recipient.observe(line, []runtimeQualificationReadSpan{{0, start.Add(-time.Second)}, {len(prompt), start.Add(10 * time.Millisecond)}})
	got := recipient.result(1)
	if got.Received != 1 || got.Missing != 0 || got.P99MS != 10 {
		t.Fatalf("prompt timestamp contaminated spot timing: %+v", got)
	}
}

func (r *runtimeQualificationRecipient) result(expected int) runtimeQualificationResult {
	result := runtimeQualificationResult{Name: r.name, Expected: expected, Received: len(r.seen), Duplicates: r.duplicates, ReadError: r.err}
	result.UntrackedOutput, result.UntrackedExamples = r.untracked, r.untrackedExamples
	result.Missing = max(0, expected-len(r.seen))
	if result.Missing > 0 {
		for dx := range r.tracker.starts {
			if _, ok := r.seen[dx]; !ok {
				result.MissingExamples = append(result.MissingExamples, dx)
			}
		}
		slices.Sort(result.MissingExamples)
		if len(result.MissingExamples) > 32 {
			result.MissingExamples = result.MissingExamples[:32]
		}
	}
	result.Unexpected = max(0, len(r.seen)-expected)
	if len(r.samples) > 0 {
		slices.Sort(r.samples)
		// Nearest-rank p99, computed over every required output that arrived.
		rank := (99*len(r.samples)+99)/100 - 1
		result.P99MS = float64(r.samples[rank]) / float64(time.Millisecond)
	}
	return result
}

func runtimeQualificationPort(t *testing.T) int {
	t.Helper()
	listenerConfig := net.ListenConfig{}
	listener, err := listenerConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return port
}

func runtimeQualificationLogin(t *testing.T, address, localIP, call string) (net.Conn, *bufio.Reader) {
	t.Helper()
	dialer := net.Dialer{Timeout: 10 * time.Second}
	if localIP != "" {
		dialer.LocalAddr = &net.TCPAddr{IP: net.ParseIP(localIP)}
	}
	conn, err := dialer.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	reader := bufio.NewReaderSize(conn, 65538)
	var prompt strings.Builder
	for prompt.Len() < 8192 {
		b, err := reader.ReadByte()
		if err != nil {
			t.Fatalf("%s login prompt: %v (%q)", call, err, prompt.String())
		}
		prompt.WriteByte(b)
		if strings.Contains(strings.ToLower(prompt.String()), "login:") {
			if _, err := io.WriteString(conn, call+"\r\n"); err != nil {
				t.Fatal(err)
			}
			return conn, reader
		}
	}
	t.Fatalf("%s login prompt exceeded bound", call)
	return nil, nil
}
