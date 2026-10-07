//go:build !qualification

package cluster

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"database/sql"
	"dxcluster/config"
	"dxcluster/spot"
)

// This opt-in uses the production runtime with one local recipient and a finite
// FCC fixture. It measures enqueue-to-complete-TCP-line latency, not a deployed
// first-byte SLA or the separate large peer-topology qualification.
func TestFCCRuntimeStateTCP(t *testing.T) {
	if os.Getenv("GOCLUSTER_FCC_RUNTIME_PROFILE") != "1" {
		t.Skip("opt-in local FCC runtime validation")
	}
	cfg, configDir := fccRuntimeTestConfig(t)
	const count = 500
	db, err := sql.Open("sqlite", cfg.FCCULS.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		_, err = db.ExecContext(t.Context(), "INSERT INTO AM VALUES(?, ?, 'CA')", i+1, fmt.Sprintf("K%dAAA", i+100))
		if err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	_, err = db.ExecContext(t.Context(), "INSERT INTO AM VALUES(1001,'N2AAA','AP')")
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("FCC fixture: %v %v", err, closeErr)
	}
	r := newClusterRuntime(BuildInfo{Version: "fcc-runtime-test"}, cfg, configDir, config.LoadDiagnostics{})
	defer r.close()
	if !r.initialize() {
		t.Fatalf("runtime startup: %v", r.startupErr)
	}
	defer r.shutdown()
	conn, reader := runtimeQualificationLogin(t, net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.Telnet.Port)), "", "DL1ABC")
	if _, err := fmt.Fprint(conn, "PASS DXSTATE CA\r\n"); err != nil {
		t.Fatal(err)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(line, "PASS DXSTATE CA") {
			break
		}
	}
	latencies := make([]time.Duration, 0, count)
	for i := 0; i < count; i++ {
		call := fmt.Sprintf("K%dAAA", i+100)
		s := spot.NewSpot(call, "N2AAA", 14020+float64(i), "CW")
		s.IsHuman, s.SourceType = true, spot.SourceUpstream
		started := time.Now()
		select {
		case r.ingestInput <- s:
		case <-time.After(3 * time.Second):
			t.Fatal("FCC ingress blocked")
		}
		if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatalf("state-filtered TCP output %s: %v", call, err)
			}
			if strings.Contains(line, call) && strings.Contains(line, "DX de ") {
				break
			}
		}
		latencies = append(latencies, time.Since(started))
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		rows, err := r.archiveWriter.Recent(count)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == count {
			for _, row := range rows {
				if row.DXMetadata.State != "CA" || row.DEMetadata.State != "AP" {
					t.Fatalf("runtime archive lost mailing state: %+v/%+v", row.DXMetadata, row.DEMetadata)
				}
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime archive stored only %d/%d rows", len(rows), count)
		}
		time.Sleep(time.Millisecond)
	}
	slices.Sort(latencies)
	t.Logf("FCC local runtime: %d/%d state-filtered TCP lines and decoded archive rows; complete-line p99=%s max=%s; enforcement=false", count, count, latencies[(99*count+99)/100-1], latencies[len(latencies)-1])
}

func fccRuntimeTestConfig(t *testing.T) (*config.Config, string) {
	t.Helper()
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(repo, "data", "config")
	cfg, err := config.Load(configDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(t.TempDir())
	cfg.RBN.Enabled, cfg.RBNDigital.Enabled, cfg.PSKReporter.Enabled, cfg.DXSummit.Enabled = false, false, false, false
	for i := range cfg.HumanTelnet {
		cfg.HumanTelnet[i].Enabled = false
	}
	cfg.FCCULS.Enabled, cfg.Reputation.Enabled, cfg.Skew.Enabled = false, false, false
	cfg.SolarWeather.Enabled, cfg.PropReport.Enabled, cfg.Peering.Enabled = false, false, false
	cfg.PathReliability.VOACAPFallback.Enabled = false
	cfg.CallCorrection.Enabled, cfg.CallCorrection.StabilizerEnabled, cfg.CallCorrection.TemporalDecoder.Enabled = false, false, false
	configureFCCQualificationSource(t, cfg)
	cfg.CTY.File, cfg.CTY.URL = filepath.Join(repo, "data", "cty", "cty.plist"), ""
	cfg.H3TablePath = filepath.Join(repo, "data", "h3")
	for _, path := range []*string{&cfg.CallCorrection.ConfusionModelFile, &cfg.CallCorrection.SpotterReliabilityFile, &cfg.CallCorrection.SpotterReliabilityFileCW, &cfg.CallCorrection.SpotterReliabilityFileRTTY} {
		if *path != "" && !filepath.IsAbs(*path) {
			*path = filepath.Join(repo, *path)
		}
	}
	cfg.UI.Mode = "headless"
	cfg.Telnet.Port = runtimeQualificationPort(t)
	// Immediate fanout isolates FCC work from the shipped 200 ms batch timer.
	cfg.Telnet.BroadcastBatchIntervalMS = 0
	cfg.Archive.BatchSize, cfg.Archive.BatchIntervalMS = 1, 1
	return cfg, configDir
}
