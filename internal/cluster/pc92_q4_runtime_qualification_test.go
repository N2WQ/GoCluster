//go:build qualification

package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/peer"
)

type q4Socket struct {
	conn   net.Conn
	reader *bufio.Reader
	done   chan struct{}
	mu     sync.Mutex
}

func (s *q4Socket) write(line string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	_, err := io.WriteString(s.conn, strings.TrimRight(line, "\r\n")+"\r\n")
	return err
}

func (s *q4Socket) drain(peerLink bool) {
	defer close(s.done)
	for {
		line, err := s.reader.ReadString('\n')
		if err != nil {
			return
		}
		if peerLink && strings.HasPrefix(line, "PC51^") {
			parts := strings.Split(strings.TrimSpace(line), "^")
			if len(parts) >= 4 && parts[3] == "0" {
				if err := s.write("PC51^" + parts[2] + "^" + parts[1] + "^1^"); err != nil {
					return
				}
			}
		}
	}
}

func q4Close(sockets []*q4Socket) {
	for _, s := range sockets {
		_ = s.conn.Close()
	}
	for _, s := range sockets {
		<-s.done
	}
}

type q4Cycle struct {
	Name                                string
	AtSeconds                           float64
	Before, Filled, Pressured, Released peer.QualificationState
	PressureProcessHeapBytes            uint64
	PressureRuntimeStacks               uint64
	WatermarkUnchanged                  bool
	Transports                          []peer.QualificationTransportState
}

type q4Report struct {
	Phase                            string
	RunID                            string
	Diagnostic, MeasurementPassed    bool
	DurationSeconds                  float64
	Clients, Established             int
	Initial, Final                   peer.QualificationState
	Peaks                            peer.QualificationCapacityChecks
	Cycles                           []q4Cycle
	Failures, OpenEvidence           []string
	HeapAlloc, HeapInuse, ProcessSys uint64
	StackInuse, StackSys             uint64
	Goroutines                       int
	GoVersion                        string
}

type q4Runtime struct {
	t               *testing.T
	r               *clusterRuntime
	ctx             context.Context
	phase           string
	peerAddress     string
	peers, clients  []*q4Socket
	topology        *peer.QualificationTopology
	generator       *peer.QualificationCapacityGenerator
	sequence        [4]int
	pressure        int
	pressureWindows int
	lastFull        time.Time
	report          q4Report
	started         time.Time
}

// Q4 retains real runtime/admission/deadline ownership. The short named
// profiles are diagnostics; they cannot substitute for either30-minute phase.
func TestPC92Q4RuntimeQualification(t *testing.T) {
	profile := os.Getenv("GOCLUSTER_PC92_Q4_PROFILE")
	if profile == "" {
		t.Skip("opt-in; scripts/pc92-q4-qualification.ps1")
	}
	diagnostic := strings.HasPrefix(profile, "preflight-")
	phase := strings.TrimPrefix(profile, "preflight-")
	if phase != "a" && phase != "b" {
		t.Fatal("Q4 profile must be a, b, preflight-a or preflight-b")
	}
	output := os.Getenv("GOCLUSTER_PC92_Q4_OUTPUT")
	if !filepath.IsAbs(output) {
		t.Fatal("Q4 output path must be absolute")
	}
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(filepath.Join(repo, "data", "config"))
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(t.TempDir())
	configureRuntimeQualification(t, cfg, repo, qualificationProfile{name: "q4", peers: 64, shipped: true})
	cfg.Telnet.MaxConnections = 1000
	cfg.Peering.Peers[63].Password = "q4-fixture-password"
	applyGoRuntimeTuning(cfg.GoRuntime)
	r := newClusterRuntime(BuildInfo{Version: "q4-capacity", Commit: "local", BuildTime: time.Now().UTC().Format(time.RFC3339), VCSModified: "true", GoVersion: runtime.Version()}, cfg, filepath.Join(repo, "data", "config"), config.LoadDiagnostics{})
	defer r.close()
	if !r.initialize() {
		t.Fatalf("runtime startup: %v", r.startupErr)
	}
	defer r.shutdown()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	d := &q4Runtime{t: t, r: r, ctx: ctx, phase: phase, peerAddress: net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.Peering.ListenPort)), report: q4Report{Phase: phase, Diagnostic: diagnostic, GoVersion: runtime.Version()}}
	defer d.writeReport(output)
	defer func() { q4Close(d.clients); q4Close(d.peers) }()
	if err := d.connect(); err != nil {
		t.Fatal(err)
	}
	d.report.Clients = r.telnetServer.GetClientCount()
	calls := make([]string, len(d.peers))
	for i := range calls {
		calls[i] = cfg.Peering.Peers[i].RemoteCallsign
	}
	d.topology, err = peer.NewQualificationTopologyForCapacity(calls)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.topology.Prepare(ctx, r.peerManager, func(index int, wire string) error { return d.peers[index].write(wire) }); err != nil {
		t.Fatal(err)
	}
	d.report.Initial, err = d.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	d.report.Established = len(d.peers)
	if s := d.report.Initial; s.Nodes != 4096 || s.Users != 65536 || s.Edges != 131072 || s.Ingress != 4096*len(d.peers) || s.Freshness != 16384 {
		t.Fatalf("incomplete reachable starting population: %+v", s)
	}
	d.generator = peer.NewQualificationCapacityGenerator(d.topology)
	if err := d.fillCaches(false); err != nil {
		t.Fatal(err)
	}
	d.started = time.Now()
	duration := 30 * time.Minute
	if diagnostic {
		duration = 10 * time.Second
	}
	plans := peer.QualificationStagingPlans()
	for cycle := 0; time.Since(d.started) < duration || cycle < 2; cycle++ {
		if len(d.report.Cycles) >= 256 {
			d.report.Failures = append(d.report.Failures, "bounded cycle evidence capacity exhausted")
			break
		}
		plan := plans[cycle%len(plans)]
		if phase == "b" && cycle == 0 {
			plan = plans[len(plans)-1] // Establish a winner before cache saturation.
		} else if plan.CompleteRace && d.r.peerManager.QualificationCacheCounts()[1] == 65536 {
			plan = plans[0] // Private staging remains valid while authority cache is full.
		}
		if phase == "a" {
			plan = peer.QualificationStagingPlan{Name: "prelogin", OverflowCandidate: -1, AwaitDeadline: cycle%6 == 5}
		}
		if diagnostic {
			plan.AwaitDeadline = false
		}
		if !(phase == "b" && cycle == 0) && (d.lastFull.IsZero() || time.Since(d.lastFull) >= 601*time.Second) {
			if err := d.fillCaches(false); err != nil {
				d.report.Failures = append(d.report.Failures, err.Error())
				break
			}
			d.pressure = 1 + d.pressureWindows%2
		}
		if err := d.cycle(plan); err != nil {
			d.report.Failures = append(d.report.Failures, err.Error())
			break
		}
		pause := min(10*time.Second, max(time.Duration(0), duration-time.Since(d.started)))
		if phase == "b" && cycle == 0 {
			pause = 0
		}
		if err := qualificationWaitContext(ctx, pause); err != nil {
			t.Fatal(err)
		}
	}
	d.report.DurationSeconds = time.Since(d.started).Seconds()
	if !diagnostic && d.pressureWindows < 3 {
		d.report.Failures = append(d.report.Failures, "fewer than three real-expiry cache/queue refill windows")
	}
	d.report.Final, err = d.snapshot()
	if err != nil {
		d.report.Failures = append(d.report.Failures, err.Error())
	}
	if r.telnetServer.GetClientCount() != 1000 {
		d.report.Failures = append(d.report.Failures, "local client population did not survive capacity cycles")
	}
	// Runtime observations do not themselves prove the conservative aggregate
	// allocation contract. Keep that final source-audit dependency explicit.
	d.report.OpenEvidence = []string{"The complete 480 MiB allocation proof remains subject to the final source-level ownership audit, including enabled diagnostic persistence."}
	if len(d.report.Failures) > 0 {
		t.Fatalf("Q4 wire capacity checks: %v", d.report.Failures)
	}
	if !diagnostic {
		t.Errorf("Q4 evidence incomplete: %v", d.report.OpenEvidence)
	}
}

func (d *q4Runtime) writeReport(path string) {
	d.report.RunID = os.Getenv("GOCLUSTER_PC92_RUN_ID")
	d.report.MeasurementPassed = !d.t.Failed() && len(d.report.Failures) == 0
	if d.t.Failed() && len(d.report.Failures) == 0 {
		d.report.Failures = append(d.report.Failures, "setup/test failure; retained test output contains the exact cause")
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	d.report.HeapAlloc, d.report.HeapInuse, d.report.ProcessSys = memory.HeapAlloc, memory.HeapInuse, memory.Sys
	d.report.StackInuse, d.report.StackSys, d.report.Goroutines = memory.StackInuse, memory.StackSys, runtime.NumGoroutine()
	data, err := json.MarshalIndent(d.report, "", "  ")
	if err != nil {
		d.t.Error(err)
		return
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		d.t.Error(err)
	}
}

func (d *q4Runtime) snapshot() (peer.QualificationState, error) {
	state, err := d.r.peerManager.QualificationSnapshot(d.ctx)
	if err == nil {
		err = d.report.Peaks.Observe(state)
	}
	return state, err
}

func (d *q4Runtime) await(check func(peer.QualificationState) bool, limit time.Duration) (peer.QualificationState, error) {
	until := time.Now().Add(limit)
	var state peer.QualificationState
	for time.Now().Before(until) {
		var err error
		state, err = d.snapshot()
		if err != nil {
			return state, err
		}
		if check(state) {
			return state, nil
		}
		if err := qualificationWaitContext(d.ctx, 10*time.Millisecond); err != nil {
			return state, err
		}
	}
	return state, fmt.Errorf("Q4 ownership/occupancy deadline: %+v", state)
}

func (d *q4Runtime) open(call, password string, complete bool) (*q4Socket, error) {
	var conn net.Conn
	var reader *bufio.Reader
	var err error
	if call == "" {
		conn, err = (&net.Dialer{}).DialContext(d.ctx, "tcp", d.peerAddress)
		if err == nil {
			reader = bufio.NewReader(conn)
		}
	} else {
		conn, reader, err = runtimeQualificationLogin(d.ctx, d.peerAddress, "", call)
	}
	if err != nil {
		return nil, err
	}
	s := &q4Socket{conn: conn, reader: reader, done: make(chan struct{})}
	if password != "" {
		if err := s.write(password); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	if call != "" {
		if err := s.write("PC18^DXSpider Version:1.57 Build:633 [pc9x]^5457^"); err != nil {
			_ = conn.Close()
			return nil, err
		}
		if complete {
			if err := s.write("PC20^"); err != nil {
				_ = conn.Close()
				return nil, err
			}
			if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				_ = conn.Close()
				return nil, err
			}
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					_ = conn.Close()
					return nil, err
				}
				if strings.TrimSpace(line) == "PC22^" {
					break
				}
			}
		}
	}
	_ = conn.SetReadDeadline(time.Time{})
	go s.drain(call != "")
	return s, nil
}

func (d *q4Runtime) connect() error {
	count := 64
	if d.phase == "b" {
		count = 63
	}
	for i := range count {
		configured := d.r.cfg.Peering.Peers[i]
		s, err := d.open(configured.RemoteCallsign, configured.Password, true)
		if err != nil {
			return err
		}
		d.peers = append(d.peers, s)
	}
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(d.r.cfg.Telnet.Port))
	for i := range 1000 {
		conn, reader, err := runtimeQualificationLogin(d.ctx, address, fmt.Sprintf("127.0.%d.%d", i/250, i%250+2), fmt.Sprintf("DL%dCAA", i+1))
		if err != nil {
			return err
		}
		s := &q4Socket{conn: conn, reader: reader, done: make(chan struct{})}
		d.clients = append(d.clients, s)
		go s.drain(false)
		if err := qualificationWaitContext(d.ctx, 50*time.Millisecond); err != nil {
			return err
		}
	}
	if d.r.telnetServer.GetClientCount() != 1000 {
		return fmt.Errorf("only%d/1000 clients admitted", d.r.telnetServer.GetClientCount())
	}
	_, err := d.await(func(s peer.QualificationState) bool { return s.Established == count && s.Pending == 0 }, 5*time.Second)
	return err
}
