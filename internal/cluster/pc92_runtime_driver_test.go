//go:build qualification

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/peer"
)

type qualificationProfile struct {
	name                             string
	load, drain                      time.Duration
	peers                            int
	full, shipped, burst, diagnostic bool
}

func runtimeQualificationProfile(name string) (qualificationProfile, error) {
	p := qualificationProfile{name: name, peers: 16, load: 45 * time.Minute, drain: 11 * time.Minute}
	switch name {
	case "q1":
	case "q2":
		p.peers, p.full, p.load = 64, true, 30*time.Minute
	case "q3":
		p.peers, p.full, p.load, p.burst = 64, true, 30*time.Minute, true
	case "shipped-q1":
		p.shipped = true
	case "preflight":
		p.load, p.drain, p.diagnostic = 20*time.Second, 5*time.Second, true
	case "diagnostic-full":
		p.peers, p.full, p.load, p.drain, p.diagnostic = 64, true, 10*time.Second, 5*time.Second, true
	default:
		return p, fmt.Errorf("unknown qualification profile %q", name)
	}
	return p, nil
}

// Exact approved durations belong to profiles, not environment overrides. The
// short profiles are explicitly diagnostic and can never claim qualification.
func TestPC92RuntimeQualification(t *testing.T) {
	name := os.Getenv("GOCLUSTER_PC92_RUNTIME_PROFILE")
	if name == "" {
		t.Skip("opt-in; scripts/pc92-runtime-qualification.ps1")
	}
	profile, err := runtimeQualificationProfile(name)
	if err != nil {
		t.Fatal(err)
	}
	output := os.Getenv("GOCLUSTER_PC92_RUNTIME_OUTPUT")
	if !filepath.IsAbs(output) {
		t.Fatal("runtime evidence path must be absolute")
	}
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	now, frequency, err := qualificationCounterClock()
	if err != nil {
		t.Fatal(err)
	}
	priorProcs := runtime.GOMAXPROCS(runtime.NumCPU())
	defer runtime.GOMAXPROCS(priorProcs)
	minutes := int((profile.load + time.Minute - 1) / time.Minute)
	maxInputs := int((profile.load+6*time.Millisecond-1)/(6*time.Millisecond)) + minutes*120 + 128
	mappedPath := filepath.Join(filepath.Dir(output), "input-ledger.bin")
	mapping, err := openQualificationSharedInputs(mappedPath, maxInputs, true)
	if err != nil {
		t.Fatal(err)
	}
	defer mapping.close()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	oracle := newQualificationOracleInputs(mapping.inputs, 100, profile.peers, minutes)
	oracle.clockNow, oracle.clockFrequency = now, frequency
	runtime.GC()
	runtime.ReadMemStats(&after)
	driverInitialHeap := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	beforeLaunch := now().UnixNano()
	service := startQualificationService(t, repo, output, mappedPath, maxInputs)
	defer service.close()
	if service.ready.Frequency != frequency || service.ready.Clock < beforeLaunch-1 || service.ready.Clock > now().UnixNano()+1 || service.ready.GOMAXPROCS != 2 || service.ready.Config == nil {
		t.Fatal("child clock domain or runtime tuning differs")
	}
	cfg := service.ready.Config
	driver := &qualificationDriver{t: t, profile: profile, service: service, cfg: cfg, oracle: oracle}
	driver.ctx, driver.cancel = context.WithCancel(t.Context())
	defer driver.close()
	if err := driver.connect(); err != nil {
		t.Fatal(err)
	}
	calls := make([]string, profile.peers)
	for i := range calls {
		calls[i] = cfg.Peering.Peers[i].RemoteCallsign
	}
	fixture, err := peer.NewQualificationTopology(profile.full, calls)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.Prepare(driver.ctx, service, driver.sendPeer); err != nil {
		t.Fatalf("wire population: %v", err)
	}
	driver.topology.Store(fixture)
	if _, err := service.call(driver.ctx, qualificationRequest{Kind: "profile-start"}); err != nil {
		t.Fatal(err)
	}
	driver.oracle.epoch = time.Now()
	driver.oracle.measurementEpoch = now()
	if _, err := service.call(driver.ctx, qualificationRequest{Kind: "arm", Epoch: oracle.measurementEpoch.UnixNano(), Frequency: frequency}); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s: prepared actual graph, starting exact load=%s drain=%s", name, profile.load, profile.drain)
	loadErr := driver.load()
	profileReply, err := service.call(driver.ctx, qualificationRequest{Kind: "profile-stop"})
	if err != nil {
		oracle.fail("profile-stop: %v", err)
	}
	if loadErr != nil {
		oracle.fail("load: %v", loadErr)
	}
	if err := qualificationWaitContext(driver.ctx, profile.drain); err != nil {
		oracle.fail("drain: %v", err)
	}
	finalState, stateErr := service.QualificationSnapshot(driver.ctx)
	if stateErr != nil {
		oracle.fail("final peer snapshot: %v", stateErr)
	}
	if err := fixture.Verify(); err != nil {
		oracle.fail("PC92 relay verification: %v", err)
	}
	childResults, err := service.call(driver.ctx, qualificationRequest{Kind: "results", Used: oracle.used})
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.acceptEnqueueResults(childResults); err != nil {
		t.Fatal(err)
	}
	driver.close()
	service.close()
	if service.closeErr != nil {
		oracle.fail("child shutdown: %v", service.closeErr)
	}
	results := oracle.results(!profile.shipped)
	if finalState.SpotRefused+finalState.PC92Refused+finalState.PC93Refused+finalState.BulletinRefused != 0 {
		oracle.fail("required traffic encountered cache refusal")
	}
	if finalState.ClockGated || finalState.PublicationGated || finalState.BlockedPeers != 0 {
		oracle.fail("unexpected final peer gate")
	}
	runtime.ReadMemStats(&after)
	report := qualificationRuntimeReport{
		Profile: name, Diagnostic: profile.diagnostic, Qualified: !profile.diagnostic && oracle.failures.Load() == 0,
		LoadSeconds: profile.load.Seconds(), DrainSeconds: profile.drain.Seconds(),
		Failures: oracle.failures.Load(), FailureExamples: oracle.examples, Recipients: results,
		NewSpotKeys: driver.newSpots, DuplicateArrivals: driver.duplicates, PC92Records: driver.pc92Count, PC93Records: driver.pc93Count, WWVRecords: driver.wwvCount,
		MaximumProducerLagMS: float64(driver.maxLag) / float64(time.Millisecond),
		FinalState:           finalState, Topology: fixture.Report(), StateSamples: driver.samples,
		OracleInitialHeapBytes: driverInitialHeap, OracleBackingArrayBytes: oracle.allocatedBytes,
		LoadProfile: profileReply.Profile,
		HeapAlloc:   childResults.HeapAlloc, HeapInuse: childResults.HeapInuse, ProcessSys: childResults.ProcessSys,
		DriverHeapAlloc: after.HeapAlloc, DriverHeapInuse: after.HeapInuse, DriverProcessSys: after.Sys,
		ChildOracleBackingArrayBytes: childResults.OracleBytes, SharedInputBytes: uint64(mapping.bytes), DriverGOMAXPROCS: runtime.GOMAXPROCS(0), CounterFrequency: frequency,
		GoVersion: runtime.Version(), CPUs: runtime.NumCPU(), GOMAXPROCS: childResults.GOMAXPROCS,
		BroadcastBatchMS: cfg.Telnet.BroadcastBatchIntervalMS, StabilizerEnabled: cfg.CallCorrection.StabilizerEnabled, TemporalEnabled: cfg.CallCorrection.TemporalDecoder.Enabled,
		Limits: []string{"Separate service process retains shipped2P/GC50/1536MiB; external generator and recipient sockets run in parent with separately reported resources.", "Every endpoint uses shared Windows QPC ticks; interval accounting adds one uncertainty tick and rounds upward. Input metadata is atomically published before source write, without resetting timestamps in transit.", "Histogram p99 is an upper bound; exact counters at5ms/25ms determine acceptance using all required tokens.", "Oracle allocation counts exclude socket and PC92 fixture storage; child profiles include bounded enqueue accounting and are not a governed-subsystem allocation verdict.", "Renamed counts compare full callsigns at successful enqueue or peer reception; DisplayTruncated counts the separate ten-character telnet presentation.", "Network feeds/downloaders disabled in isolated config; reference models read-only; persisted state uses temporary working and evidence directories."},
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("runtime profile=%s diagnostic=%t failures=%d evidence=%s", name, profile.diagnostic, report.Failures, output)
	if report.Failures != 0 {
		t.Errorf("runtime checker failed: %v", report.FailureExamples)
	}
}

func configureRuntimeQualification(t *testing.T, cfg *config.Config, repo string, p qualificationProfile) {
	t.Helper()
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
	for i := 0; i < p.peers; i++ {
		cfg.Peering.Peers = append(cfg.Peering.Peers, config.PeeringPeer{Enabled: true, Family: "dxspider", Direction: "inbound", PreferPC9x: true, RemoteCallsign: fmt.Sprintf("DL%dPAA", i+1), LoginCallsign: "N0CALL-1"})
	}
	if !p.shipped {
		cfg.Telnet.BroadcastBatchIntervalMS = 0
		cfg.CallCorrection.StabilizerEnabled = false
		cfg.CallCorrection.TemporalDecoder.Enabled = false
	}
	if cfg.GoRuntime.MaxProcs != 2 || cfg.GoRuntime.GCPercent != 50 || cfg.GoRuntime.MemoryLimitMiB != 1536 || cfg.Peering.MaxLineLength != 65536 {
		t.Fatal("shipped runtime/peer frame settings changed")
	}
}

type qualificationDriver struct {
	t                                                    *testing.T
	profile                                              qualificationProfile
	service                                              *qualificationService
	cfg                                                  *config.Config
	oracle                                               *qualificationOracle
	ctx                                                  context.Context
	cancel                                               context.CancelFunc
	linksMu                                              sync.RWMutex
	peers, clients                                       []*qualificationSocket
	all                                                  []*qualificationSocket
	topology                                             atomic.Pointer[peer.QualificationTopology]
	closeOnce                                            sync.Once
	faultWG                                              sync.WaitGroup
	newSpots, duplicates, pc92Count, pc93Count, wwvCount int
	maxLag                                               time.Duration
	samples                                              []peer.QualificationState
	messageStamp                                         *peer.TimestampGenerator
}

func (d *qualificationDriver) connect() error {
	cfg := d.cfg
	peerAddress := "127.0.0.1:" + strconv.Itoa(cfg.Peering.ListenPort)
	for i := 0; i < d.profile.peers; i++ {
		link, err := d.connectPeer(peerAddress, i)
		if err != nil {
			return err
		}
		d.peers = append(d.peers, link)
		d.all = append(d.all, link)
		go link.read()
	}
	clientAddress := "127.0.0.1:" + strconv.Itoa(cfg.Telnet.Port)
	for i := range d.oracle.clients {
		conn, reader, err := runtimeQualificationLogin(d.ctx, clientAddress, fmt.Sprintf("127.0.0.%d", i+2), fmt.Sprintf("DL%dCAA", i+1))
		if err != nil {
			return err
		}
		link := newQualificationSocket(d, conn, reader, d.oracle.clients[i])
		d.clients = append(d.clients, link)
		d.all = append(d.all, link)
		go link.read()
		if err := qualificationWaitContext(d.ctx, 50*time.Millisecond); err != nil {
			return err
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	membership, err := d.service.membership(d.ctx)
	for err == nil && membership.RawCount != 100 && time.Now().Before(deadline) {
		if err := qualificationWaitContext(d.ctx, 10*time.Millisecond); err != nil {
			return err
		}
		membership, err = d.service.membership(d.ctx)
	}
	if err != nil {
		return err
	}
	if !membership.Complete || membership.RawCount != 100 {
		return fmt.Errorf("only%d/100 current clients", membership.RawCount)
	}
	for _, user := range membership.Users {
		var index int
		if _, err := fmt.Sscanf(user.Login, "DL%dCAA", &index); err != nil || index < 1 || index > 100 {
			return fmt.Errorf("unexpected current login %s", user.Login)
		}
		d.oracle.sessionIDs[index-1] = user.SessionID
	}
	d.messageStamp = peer.NewTimestampGenerator()
	return nil
}

func (d *qualificationDriver) sendPeer(index int, line string) error {
	d.linksMu.RLock()
	link := d.peers[index]
	d.linksMu.RUnlock()
	return link.write(line)
}

func (d *qualificationDriver) close() {
	d.closeOnce.Do(func() {
		d.oracle.closing.Store(true)
		d.cancel()
		d.faultWG.Wait()
		d.linksMu.Lock()
		links := append([]*qualificationSocket(nil), d.all...)
		d.linksMu.Unlock()
		for _, link := range links {
			_ = link.conn.Close()
		}
		for _, link := range links {
			<-link.done
		}
	})
}

func qualificationWaitContext(ctx context.Context, wait time.Duration) error {
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type qualificationRuntimeReport struct {
	Profile                                                              string
	Diagnostic, Qualified                                                bool
	LoadSeconds, DrainSeconds                                            float64
	Failures                                                             uint64
	FailureExamples                                                      []string
	Recipients                                                           []qualificationRecipientResult
	NewSpotKeys, DuplicateArrivals, PC92Records, PC93Records, WWVRecords int
	MaximumProducerLagMS                                                 float64
	FinalState                                                           peer.QualificationState
	Topology                                                             peer.QualificationTopologyReport
	StateSamples                                                         []peer.QualificationState
	OracleInitialHeapBytes                                               int64
	OracleBackingArrayBytes, HeapAlloc, HeapInuse, ProcessSys            uint64
	LoadProfile                                                          qualificationLoadProfile
	DriverHeapAlloc, DriverHeapInuse, DriverProcessSys                   uint64
	ChildOracleBackingArrayBytes, SharedInputBytes                       uint64
	DriverGOMAXPROCS                                                     int
	CounterFrequency                                                     int64
	GoVersion                                                            string
	CPUs, GOMAXPROCS, BroadcastBatchMS                                   int
	StabilizerEnabled, TemporalEnabled                                   bool
	Limits                                                               []string
}
