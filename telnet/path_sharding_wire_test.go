package telnet

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

// This optional consumer witness complements cluster's real ingress fixture.
// Only this package can use the existing held-listener startup without adding
// a production API or binding a wildcard address. The driver builds this
// package at the already approved cluster executable path before execution.
func TestPathShardingLoopbackGlyphs(t *testing.T) {
	output := os.Getenv("GOCLUSTER_SHARD_WIRE_DIR")
	if output == "" {
		t.Skip("loopback glyph artifact requires the stable-path S4 driver")
	}
	workspace := os.Getenv("GOCLUSTER_SHARD_WORKSPACE")
	executable, err := os.Executable()
	if err != nil || runtime.GOOS != "windows" || workspace == "" ||
		!strings.EqualFold(filepath.Clean(executable), filepath.Join(workspace, ".tmp", "windows-tests", "dxcluster", "internal", "cluster", "package.test.exe")) {
		t.Fatalf("wire witness must execute at the approved cluster test path: %q (%v)", executable, err)
	}
	output, err = filepath.Abs(output)
	if err != nil || !strings.HasPrefix(strings.ToLower(output), strings.ToLower(filepath.Join(workspace, ".tmp"))+string(filepath.Separator)) {
		t.Fatalf("wire artifacts must stay in workspace .tmp: %q (%v)", output, err)
	}
	source, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := pathreliability.LoadFile(filepath.Join(source, "data", "config", "path_reliability.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := pathreliability.InitH3MappingsFromDir(filepath.Join(source, "data", "h3")); err != nil {
		t.Fatal(err)
	}
	previousUsers := filter.UserDataDir
	t.Cleanup(func() { filter.UserDataDir = previousUsers })
	for _, mode := range []string{pathreliability.ReceiverContributionOff, pathreliability.ReceiverContributionShadow, pathreliability.ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			modeDir := filepath.Join(output, mode)
			if err := os.MkdirAll(modeDir, 0o700); err != nil {
				t.Fatal(err)
			}
			t.Chdir(modeDir)
			filter.UserDataDir = filepath.Join(modeDir, "users")
			cfg.ReceiverContributionMode = mode
			runPathShardingWireMode(t, cfg, modeDir)
		})
	}
}

type shardingWireCase struct {
	grid, band   string
	frequency    float64
	fine, coarse pathreliability.CellID
}

type shardingWireReceipt struct {
	Mode, Listener, Boundary string
	Ordinary, Beacon         int
	WriterReports            [2]uint64
	Glyph, Source            string
	P50DB                    float64
}

func runPathShardingWireMode(t *testing.T, cfg pathreliability.Config, modeDir string) {
	t.Helper()
	now := time.Now().UTC()
	predictor := pathreliability.NewPredictor(cfg, cfg.AllowedBands)
	userFine, userCoarse := pathreliability.EncodeCell("FN31"), pathreliability.EncodeCoarseCell("FN31")
	if userFine == 0 || userCoarse == 0 {
		t.Fatal("active H3 backend cannot map user grid")
	}
	cases := seedShardingWireCases(t, predictor, userFine, userCoarse, now)
	server := NewServer(ServerOptions{
		PathPredictor: predictor, PathDisplayEnabled: true, NoiseModel: cfg.NoiseModel(),
		LoginPrompt: "login:\n", LoginGreeting: "ready\n", HandshakeMode: "none", EchoMode: "local",
		BroadcastWorkers: 2, BroadcastQueue: 64, WorkerQueue: 64, ClientBuffer: 64,
		BroadcastBatchIntervalSet: true, BroadcastBatchInterval: time.Millisecond,
		USLicenseCheck: func(string) bool { return true }, CanadianLicenseCheck: func(string) bool { return true },
	}, nil)
	server.nowFn = func() time.Time { return now }
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server.startWithListener(listener)
	// Register retirement before dialing so an early connection failure still
	// joins server owners before the next mode restores shared fixture state.
	t.Cleanup(func() {
		server.Stop()
		waitShardingWireRetirement(t)
	})
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(t.Context(), "tcp4", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})
	if _, err := fmt.Fprint(conn, "DL1SHD\r\n"); err != nil {
		t.Fatal(err)
	}
	client := waitShardingWireClient(t, server)
	reader := bufio.NewReader(conn)
	readShardingWireGreeting(t, conn, reader)
	client.filterMu.Lock()
	client.filter = passRuleTestFilter(t, "MODE=FT8")
	client.filterMu.Unlock()
	client.pathMu.Lock()
	client.grid, client.gridCell, client.gridCoarseCell, client.noiseClass = "FN31", userFine, userCoarse, "QUIET"
	client.pathMu.Unlock()
	var reports [2]atomic.Uint64
	var admissionFailed atomic.Bool
	stop := make(chan struct{})
	var writers sync.WaitGroup
	for role := range reports {
		writers.Go(func() {
			for sequence := uint64(0); ; sequence++ {
				select {
				case <-stop:
					return
				default:
				}
				entry := cases[sequence%uint64(len(cases))]
				identity := uint64(1+role*24) + sequence/uint64(len(cases))%24
				admission := predictor.UpdateWithReceiverHash(pathreliability.BucketCombined, userFine, entry.fine, userCoarse, entry.coarse,
					entry.band, -10.5, 1, pathreliability.ObservationTimes{ObservedAt: now, ArrivedAt: now}, role == 1, identity)
				if !admission.Accepted() {
					admissionFailed.Store(true)
					return
				}
				reports[role].Add(1)
			}
		})
	}
	stopWriters := sync.OnceFunc(func() { close(stop); writers.Wait() })
	defer stopWriters()
	receipt := shardingWireReceipt{Mode: cfg.ReceiverContributionMode, Listener: listener.Addr().String(),
		Boundary: "public Predictor updates to actual telnet wire; actual cluster ingress is a separate witness", Glyph: ">", Source: "combined", P50DB: -10.5}
	for sequence := 0; sequence < 2*len(cases)*16; sequence++ {
		entry := cases[sequence%len(cases)]
		sp := spot.NewSpot("DX1SHD", "DE1SHD", entry.frequency, "FT8")
		sp.Time, sp.DXMetadata.Grid, sp.Comment, sp.IsBeacon = now, entry.grid, fmt.Sprintf("SHARD%06d", sequence), sequence/len(cases)%2 != 0
		sp.DXCellID, sp.DXCoarseCellID = uint16(entry.fine), uint16(entry.coarse)
		sp.InvalidateMetadataCache()
		sp.EnsureNormalized()
		prediction, ok := server.computePathPredictionForClient(client, sp)
		if !ok || prediction.result.Source != pathreliability.SourceCombined || !prediction.result.HasP50 || prediction.result.P50DB != -10.5 || prediction.result.Glyph != ">" {
			t.Fatalf("usable known prediction missing before wire delivery: %+v/%v", prediction.result, ok)
		}
		if !server.TryBroadcastSpotOwned(sp, true, true, true) {
			t.Fatal("wire witness lost a broadcast at admission")
		}
		line := readShardingWireSpot(t, conn, reader, sp.Comment)
		column := spot.CurrentDXClusterLayout().GlyphColumn - 1
		if len(line) <= column || line[column] != '>' {
			t.Fatalf("ordinary/beacon wire glyph missing at column %d: %q", column+1, line)
		}
		if sp.IsBeacon {
			receipt.Beacon++
		} else {
			receipt.Ordinary++
		}
	}
	stopWriters()
	for role := range reports {
		receipt.WriterReports[role] = reports[role].Load()
		if receipt.WriterReports[role] == 0 {
			t.Fatalf("writer %d never overlapped wire queries", role)
		}
	}
	queueDrops, clientDrops, failures := server.BroadcastMetricSnapshot()
	if queueDrops != 0 || clientDrops != 0 || failures != 0 || admissionFailed.Load() || receipt.Ordinary != len(cases)*16 || receipt.Beacon != len(cases)*16 {
		t.Fatalf("wire delivery lost work: queue=%d client=%d failures=%d receipt=%+v", queueDrops, clientDrops, failures, receipt)
	}
	data, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(modeDir, "receipt.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func seedShardingWireCases(t *testing.T, predictor *pathreliability.Predictor, userFine, userCoarse pathreliability.CellID, now time.Time) []shardingWireCase {
	t.Helper()
	var cases []shardingWireCase
	for _, grid := range []string{"IO91", "PM95", "QF56", "GG66"} {
		for _, band := range []struct {
			name string
			freq float64
		}{{"40m", 7074}, {"20m", 14074}, {"10m", 28074}} {
			fine, coarse := pathreliability.EncodeCell(grid), pathreliability.EncodeCoarseCell(grid)
			if fine == 0 || coarse == 0 {
				t.Fatalf("active H3 mapper cannot encode %s", grid)
			}
			cases = append(cases, shardingWireCase{grid, band.name, band.freq, fine, coarse})
			for identity := uint64(1); identity <= 12; identity++ {
				for range 8 {
					for _, reverse := range []bool{false, true} {
						rx, tx, rc, tc := userFine, fine, userCoarse, coarse
						if reverse {
							rx, tx, rc, tc = tx, rx, tc, rc
						}
						admission := predictor.UpdateWithReceiverHash(pathreliability.BucketCombined, rx, tx, rc, tc, band.name, -10.5, 1,
							pathreliability.ObservationTimes{ObservedAt: now, ArrivedAt: now}, false, identity)
						if !admission.Accepted() {
							t.Fatalf("wire seed report rejected: %+v", admission)
						}
					}
				}
			}
		}
	}
	return cases
}

func waitShardingWireClient(t *testing.T, server *Server) *Client {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		server.clientsMutex.RLock()
		client := server.clients["DL1SHD"]
		server.clientsMutex.RUnlock()
		if client != nil {
			return client
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("loopback client did not complete login")
	return nil
}

func readShardingWireSpot(t *testing.T, conn net.Conn, reader *bufio.Reader, marker string) string {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read loopback spot %s: %v", marker, err)
		}
		if strings.Contains(line, marker) && strings.Contains(line, "DX de") {
			return strings.TrimRight(line, "\r\n")
		}
	}
}

func readShardingWireGreeting(t *testing.T, conn net.Conn, reader *bufio.Reader) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read loopback login greeting: %v", err)
		}
		if strings.TrimSpace(line) == "ready" {
			return
		}
	}
}

// Stop closes owned resources but does not join every server goroutine. This
// isolated witness waits for their actual exit before changing cwd/user paths.
func waitShardingWireRetirement(t *testing.T) {
	t.Helper()
	buffer := make([]byte, 1<<20)
	deadline := time.Now().Add(5 * time.Second)
	names := []string{"(*Server).acceptConnections", "(*Server).handleClient", "(*Server).handleBroadcasts",
		"(*Server).broadcastWorker", "(*Server).rejectWorker", "(*Server).keepaliveLoop", "(*Server).solarSummaryLoop", "(*Client).writerLoop"}
	for time.Now().Before(deadline) {
		n := runtime.Stack(buffer, true)
		if n == len(buffer) {
			t.Fatal("bounded goroutine snapshot overflowed during wire retirement")
		}
		stack := string(buffer[:n])
		active := false
		for _, name := range names {
			active = active || strings.Contains(stack, name)
		}
		if !active {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("owned telnet connection/worker goroutines did not retire")
}
