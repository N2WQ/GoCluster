package cluster

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// Test the shipped main entrypoint, not a test double for the runtime. The
// child runs from an isolated directory containing no peerdiag executable.
func TestV6MainExecutablePeerLoggingWithoutCompanion(t *testing.T) {
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	exeName := "gocluster"
	if runtime.GOOS == "windows" {
		exeName += ".exe"
	}
	exe := filepath.Join(work, exeName)
	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelBuild()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", exe, ".")
	build.Dir = repo
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build main executable: %v\n%s", err, output)
	}
	if _, err := os.Stat(filepath.Join(work, "peerdiag.exe")); !os.IsNotExist(err) {
		t.Fatalf("unexpected helper: %v", err)
	}
	if _, err := os.Stat(filepath.Join(work, "peerdiag")); !os.IsNotExist(err) {
		t.Fatalf("unexpected helper: %v", err)
	}

	var listen net.ListenConfig
	peerListener, err := listen.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer peerListener.Close()
	reference := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "isolated fixture", http.StatusServiceUnavailable)
	}))
	defer reference.Close()
	cfgDir := filepath.Join(work, "config")
	if err := os.Mkdir(cfgDir, 0700); err != nil {
		t.Fatal(err)
	}
	files := []string{"app.yaml", "archive.yaml", "data.yaml", "dedupe.yaml", "floodcontrol.yaml", "ingest.yaml", "mode_seeds.yaml", "peering.yaml", "pipeline.yaml", "prop_report.yaml", "reputation.yaml", "runtime.yaml", "path_reliability.yaml", "solarweather.yaml", "iaru_regions.yaml", "iaru_mode_inference.yaml", "spot_taxonomy.yaml"}
	telnetPort := runtimeQualificationPort(t)
	peerPort := runtimeQualificationPort(t)
	for _, name := range files {
		data, err := os.ReadFile(filepath.Join(repo, "data", "config", name))
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]any
		if err = yaml.Unmarshal(data, &doc); err != nil {
			t.Fatal(err)
		}
		section := func(key string) map[string]any {
			m, ok := doc[key].(map[string]any)
			if !ok {
				t.Fatalf("%s missing section %s", name, key)
			}
			return m
		}
		switch name {
		case "app.yaml":
			section("ui")["mode"] = "headless"
			logging := section("logging")
			logging["enabled"] = false
			for _, category := range []string{"dropped_calls", "login_attempts", "reputation_drops", "telnet_connections", "ingest_connections", "propagation"} {
				logging[category].(map[string]any)["enabled"] = false
			}
			peer := logging["peer_connections"].(map[string]any)
			peer["enabled"] = true
			peer["dir"] = filepath.Join(work, "peer-connections")
			peer["dedupe_window_seconds"] = 0
		case "archive.yaml":
			section("archive")["enabled"] = false
		case "data.yaml":
			cty := section("cty")
			cty["enabled"] = false
			cty["url"] = reference.URL
			section("skew")["enabled"] = false
			fcc := section("fcc_uls")
			fcc["enabled"] = false
			fcc["url"] = reference.URL
			ised := section("ised")
			ised["enabled"] = false
			ised["url"] = reference.URL
			ised["special_url"] = reference.URL
		case "ingest.yaml":
			for _, key := range []string{"rbn", "rbn_digital", "dxsummit", "pskreporter"} {
				section(key)["enabled"] = false
			}
			for _, raw := range doc["human_telnet"].([]any) {
				raw.(map[string]any)["enabled"] = false
			}
		case "peering.yaml":
			peer := section("peering")
			peer["enabled"] = true
			peer["listen_port"] = peerPort
			peer["local_callsign"] = "N0CALL-1"
			peer["peers"] = []any{map[string]any{"enabled": true, "family": "dxspider", "direction": "outbound", "host": "127.0.0.1", "port": peerListener.Addr().(*net.TCPAddr).Port, "password": "", "prefer_pc9x": true, "login_callsign": "N0CALL-1", "remote_callsign": "DL1PAA"}}
			peer["topology"].(map[string]any)["db_path"] = ""
		case "pipeline.yaml":
			correction := section("call_correction")
			correction["enabled"] = false
			correction["confusion_model_enabled"] = false
			for _, key := range []string{"spotter_reliability_file", "spotter_reliability_file_cw", "spotter_reliability_file_rtty"} {
				asset, ok := correction[key].(string)
				if !ok || asset == "" {
					t.Fatalf("fixture missing %s", key)
				}
				if !filepath.IsAbs(asset) {
					asset = filepath.Join(repo, asset)
				}
				correction[key] = asset
			}
		case "prop_report.yaml":
			section("prop_report")["enabled"] = false
		case "reputation.yaml":
			section("reputation")["enabled"] = false
		case "runtime.yaml":
			section("telnet")["port"] = telnetPort
		case "path_reliability.yaml":
			doc["enabled"] = false
			section("voacap_fallback")["enabled"] = false
			section("native_160m_fallback")["enabled"] = false
		case "solarweather.yaml":
			doc["enabled"] = false
		}
		data, err = yaml.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(cfgDir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	outPath := filepath.Join(work, "child-output.log")
	out, err := os.Create(outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	childCtx, cancelChild := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancelChild()
	child := exec.CommandContext(childCtx, exe)
	child.Dir = work
	child.Stdout = out
	child.Stderr = out
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		upper := strings.ToUpper(key)
		if strings.HasPrefix(upper, "DXC_") || upper == "PATH" || upper == "HTTP_PROXY" || upper == "HTTPS_PROXY" || upper == "ALL_PROXY" || upper == "NO_PROXY" {
			continue
		}
		child.Env = append(child.Env, item)
	}
	child.Env = append(child.Env, "DXC_CONFIG_PATH="+cfgDir, "NO_PROXY=127.0.0.1,localhost", "PATH="+work)
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- child.Wait() }()
	exited := false
	defer func() {
		if exited {
			return
		}
		_ = child.Process.Kill()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("child did not exit after termination")
		}
	}()
	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		c, e := peerListener.Accept()
		if e != nil {
			acceptErr <- e
			return
		}
		accepted <- c
	}()
	var conn net.Conn
	select {
	case conn = <-accepted:
		defer conn.Close()
	case err := <-acceptErr:
		t.Fatal(err)
	case err := <-finished:
		exited = true
		data, _ := os.ReadFile(outPath)
		t.Fatalf("child exited before peer connection: %v\n%s", err, data)
	case <-childCtx.Done():
		data, _ := os.ReadFile(outPath)
		t.Fatalf("no peer connection: %v\n%s", childCtx.Err(), data)
	}
	path := filepath.Join(work, "peer-connections", "peer-connections.log")
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		data, readErr := os.ReadFile(path)
		if readErr == nil && strings.Contains(string(data), "event=peer_connection direction=outbound action=connected peer=DL1PAA") {
			return
		}
		select {
		case err := <-finished:
			exited = true
			logData, _ := os.ReadFile(outPath)
			t.Fatalf("child exited without peer log: %v\n%s", err, logData)
		case <-deadline.C:
			logData, _ := os.ReadFile(outPath)
			t.Fatalf("peer log missing: %q (%v)\n%s", data, readErr, logData)
		case <-ticker.C:
		}
	}
}
