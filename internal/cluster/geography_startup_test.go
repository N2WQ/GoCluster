//go:build !qualification

// File role: Proves cold live startup owns shared geography independently of prediction.
package cluster

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestGeographyStartup(t *testing.T) {
	const env, marker = "GOCLUSTER_GEOGRAPHY_STARTUP_CHILD", "GEOGRAPHY_STARTUP_CHILD_EXECUTED"
	if mode := os.Getenv(env); mode != "" {
		if pathreliability.EncodeCell("FN31") != pathreliability.InvalidCell {
			t.Fatal("child prewarmed")
		}
		cfg, configDir := fccRuntimeTestConfig(t)
		cfg.PathReliability.Enabled = mode == "prediction_on_missing"
		cfg.PathReliability.Native160Fallback.Enabled = false
		cfg.CallCorrection.Enabled, cfg.CallCorrection.CustomSCP.Enabled = true, true
		cfg.CallCorrection.CustomSCP.BlockCacheMB, cfg.CallCorrection.CustomSCP.MemTableSizeMB = 1, 4
		cfg.CallCorrection.CustomSCP.ResolverMinUniqueSpotters = 2
		cfg.CallCorrection.CustomSCP.ResolverMinUniqueH3Cells = 2
		cfg.CallCorrection.CustomSCP.StabilizerMinUniqueSpotters = 2
		cfg.CallCorrection.CustomSCP.StabilizerMinUniqueH3Cells = 2
		cfg.CallCorrection.CustomSCP.SFloorMinUniqueSpottersExact = 2
		cfg.CallCorrection.CustomSCP.SFloorMinUniqueH3CellsExact = 2
		wantFiles := geographyStartupTables(t, cfg, mode)
		r := newClusterRuntime(BuildInfo{Version: "geography-startup-test"}, cfg, configDir, config.LoadDiagnostics{})
		defer r.close()
		if mode != "prediction_off" {
			if r.initialize() || r.startupErr == nil {
				t.Fatalf("missing geography launched: %v", r.startupErr)
			}
			for _, token := range append([]string{"h3_table_path"}, wantFiles...) {
				if !strings.Contains(r.startupErr.Error(), token) {
					t.Fatalf("missing diagnostic %s: %v", token, r.startupErr)
				}
			}
			if r.ctx != nil || r.signalResolver != nil || r.gridLookup != nil || r.telnetServer != nil || r.outputPipeline != nil {
				t.Fatal("failed geography created downstream owners")
			}
			if strings.HasSuffix(mode, "missing") || strings.HasPrefix(mode, "missing_") {
				for _, name := range wantFiles {
					if _, err := os.Stat(filepath.Join(cfg.H3TablePath, name)); !os.IsNotExist(err) {
						t.Fatalf("missing input regenerated: %s %v", name, err)
					}
				}
			}
		} else {
			if !r.initialize() {
				t.Fatalf("startup: %v", r.startupErr)
			}
			defer r.shutdown()
			if pathreliability.EncodeCell("FN31") == pathreliability.InvalidCell || pathreliability.EncodeCoarseCell("FN31") == pathreliability.InvalidCell {
				t.Fatal("startup did not install geography")
			}
			geographyCustomSCPControls(t, r)
			geographyNearbyControls(t)
			conn, reader := runtimeQualificationLogin(t, net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.Telnet.Port)), "", "DL1ABC")
			defer conn.Close()
			geographyCommand(t, conn, reader, "SET GRID FN31", "Grid set to FN31")
			geographyCommand(t, conn, reader, "SHOW PROP IO91", "Path reliability is disabled.")
			// The shipped new-user modes omit FT8. Select it explicitly so a
			// missing wire line cannot be mistaken for prediction behavior.
			geographyCommand(t, conn, reader, "PASS RULE 0 MODE=FT8", "Rule 0 replaced.")
			at := time.Now().UTC()
			s := spot.NewSpot("K1OFF", "N0OFF", 14074, "FT8")
			s.Time, s.HasReport, s.Report, s.DXMetadata.Grid, s.DEMetadata.Grid = at, true, -10, "FN31", "IO91"
			s.SourceType, s.Confidence = spot.SourceRBN, "V"
			s.EnsureNormalized()
			ctx := &outputSpotContext{spot: s}
			if !r.outputPipeline.prepareFanoutSpot(ctx) {
				t.Fatal("prediction off dropped ordinary fanout")
			}
			if s.DXCellID != 0 || s.DECellID != 0 || s.DXCoarseCellID != 0 || s.DECoarseCellID != 0 {
				t.Fatal("prediction off entered predictor hydration/learning route")
			}
			processPSKRPathOnlySpot(s, at, r.pathPredictor, r.pathReport, r.pskrPathOnlyStats, cfg.SpotPolicy, r.allowedBandSet)
			if r.pathPredictor.Config().Enabled || r.pskrPathOnlyStats.off.Load() != 1 || r.pathPredictor.AdmissionStats() != (pathreliability.AdmissionStats{}) {
				t.Fatal("geography activated disabled prediction ingestion")
			}
			geographyDisplayOff(t, r, conn, reader, s)
		}
		t.Log(marker + " " + mode)
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"prediction_off", "prediction_on_missing", "prediction_off_missing", "missing_res1", "missing_res2", "wrong_count", "directory"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestGeographyStartup$", "-test.v", "-test.timeout=55s")
			cmd.Env = append(os.Environ(), env+"="+mode)
			out, err := cmd.CombinedOutput()
			if err != nil || !bytes.Contains(out, []byte(marker+" "+mode)) {
				t.Fatalf("child: %v\n%s", err, out)
			}
		})
	}
}

func geographyStartupTables(t *testing.T, cfg *config.Config, mode string) []string {
	t.Helper()
	source := cfg.H3TablePath
	cfg.H3TablePath = t.TempDir()
	for _, name := range []string{"res1.bin", "res2.bin"} {
		data, err := os.ReadFile(filepath.Join(source, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(cfg.H3TablePath, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	res1 := filepath.Join(cfg.H3TablePath, "res1.bin")
	switch mode {
	case "prediction_on_missing", "prediction_off_missing":
		cfg.H3TablePath = filepath.Join(t.TempDir(), "missing")
		return []string{"res1.bin", "res2.bin"}
	case "missing_res1", "directory":
		if err := os.Remove(res1); err != nil {
			t.Fatal(err)
		}
		if mode == "directory" {
			if err := os.Mkdir(res1, 0700); err != nil {
				t.Fatal(err)
			}
		}
	case "missing_res2":
		if err := os.Remove(filepath.Join(cfg.H3TablePath, "res2.bin")); err != nil {
			t.Fatal(err)
		}
		return []string{"res2.bin"}
	case "wrong_count":
		if err := os.Truncate(res1, 6736-8); err != nil {
			t.Fatal(err)
		}
	}
	return []string{"res1.bin"}
}

func geographyDisplayOff(t *testing.T, r *clusterRuntime, conn net.Conn, reader *bufio.Reader, s *spot.Spot) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	want := s.FormatDXCluster()
	r.telnetServer.BroadcastSpot(s, true, true, true)
	var received strings.Builder
	for total := 0; total < 65536; {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("spot read: %v; preceding output=%q", err, received.String())
		}
		total += len(line)
		if received.Len() < 4096 {
			received.WriteString(line)
		}
		if !strings.Contains(line, "DX de ") || !strings.Contains(line, "K1OFF") {
			continue
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if !strings.HasSuffix(line, want) || r.telnetServer.PathPredictionStatsSnapshot().Total != 0 {
			t.Fatalf("prediction off changed displayed line: %q want suffix %q", line, want)
		}
		return
	}
	t.Fatal("prediction-off wire spot missing")
}

func geographyCommand(t *testing.T, conn net.Conn, reader *bufio.Reader, command, want string) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprint(conn, command+"\r\n"); err != nil {
		t.Fatal(err)
	}
	for total := 0; total < 65536; {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("%s: %v", command, err)
		}
		total += len(line)
		if strings.Contains(line, want) {
			return
		}
	}
	t.Fatalf("response exceeded bound waiting for %q", want)
}

func geographyCustomSCPControls(t *testing.T, r *clusterRuntime) {
	t.Helper()
	at := time.Now().UTC().Truncate(time.Second)
	if pathreliability.EncodeCoarseCell("FN31") == pathreliability.EncodeCoarseCell("IO91") {
		t.Fatal("geography controls share a coarse cell")
	}
	for _, tc := range []struct {
		call, secondGrid string
		want             bool
	}{{"K1TWO", "IO91", true}, {"K1ONE", "FN31", false}} {
		for i, grid := range []string{"FN31", tc.secondGrid} {
			s := spot.NewSpot(tc.call, fmt.Sprintf("N%dAAA", i), 14020, "CW")
			s.Time, s.Confidence, s.HasReport, s.Report, s.DEMetadata.Grid = at, "V", true, 20, grid
			s.EnsureNormalized()
			recordRecentBandObservation(s, r.recentBandStore, r.customSCPStore, r.cfg.CallCorrection)
		}
		if got := r.recentBandStore.HasRecentSupport(tc.call, "20m", "CW", 2, at); got != tc.want {
			t.Fatalf("%s two-vs-one geographic control: %t", tc.call, got)
		}
		probe := spot.NewSpot(tc.call, "N0ZZZ", 14020, "CW")
		probe.Time, probe.Confidence = at, "S"
		probe.EnsureNormalized()
		policy := r.cfg.CallCorrection
		policy.StabilizerEnabled = true
		policy.RecentBandRecordMinUniqueSpotters = 2
		if got := evaluateTelnetStabilizerDelay(probe, r.recentBandStore, policy, at, spot.ResolverSnapshot{}, false).ShouldDelay; got == tc.want {
			t.Fatalf("%s stabilizer did not consume geographic distinction: hold=%t", tc.call, got)
		}
	}
}

func geographyNearbyControls(t *testing.T) {
	t.Helper()
	rules, err := filter.PrepareRuleSet([]filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "NEARBY"}})
	if err != nil {
		t.Fatal(err)
	}
	context := filter.EvaluationContext{FineCell: pathreliability.EncodeCell("FN31"), CoarseCell: pathreliability.EncodeCoarseCell("FN31")}
	for _, freq := range []float64{14020, 3520} {
		s := spot.NewSpot("K1NEAR", "N0AAA", freq, "CW")
		s.DXMetadata.Grid = "FN31"
		s.EnsureNormalized()
		if !rules.Matches(s, context) {
			t.Fatalf("NEARBY unavailable on %.0f", freq)
		}
		s.DXMetadata.Grid = "IO91"
		s.InvalidateMetadataCache()
		s.EnsureNormalized()
		if rules.Matches(s, context) {
			t.Fatalf("distant NEARBY matched on %.0f", freq)
		}
	}
}
