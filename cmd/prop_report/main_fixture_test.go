package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestPropMainHelper(t *testing.T) {
	mode := os.Getenv("DXC_PROP_MAIN_HELPER")
	if mode != "run" && mode != "closed-output" {
		t.Skip("actual-main subprocess only")
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("missing helper separator")
	}
	os.Args = append([]string{"prop_report"}, os.Args[separator+1:]...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	// A broken endpoint fixture must fail locally before DNS or external calls.
	http.DefaultClient = &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(address)
		ip := net.ParseIP(host)
		if err != nil || ip == nil || !ip.IsLoopback() {
			return nil, fmt.Errorf("fixture refuses non-loopback address %q", address)
		}
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, address)
	}}}
	if mode == "closed-output" {
		if err := os.Stdout.Close(); err != nil {
			os.Exit(91)
		}
	}
	main()
	os.Exit(0)
}

type propFixture struct {
	config, log, json, report, openai string
}

func newPropFixture(t *testing.T) propFixture {
	t.Helper()
	dir := t.TempDir()
	f := propFixture{config: filepath.Join(dir, "config"), log: filepath.Join(dir, "observations.log"),
		json: filepath.Join(dir, "summary.json"), report: filepath.Join(dir, "report.md"), openai: filepath.Join(dir, "openai.yaml")}
	if err := os.Mkdir(f.config, 0o755); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join("..", "..", "data", "config", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("config inventory: %v / %d", err, len(files))
	}
	for _, file := range files {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(f.config, filepath.Base(file)), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	const logBody = "2026/10/09 12:00:00 Path buckets (5m): 20m f=11 c=1\n" +
		"2026/10/09 12:00:00 Path weight dist (5m): 20m t=20 <1=3 1-2=2 2-3=2 3-5=3 5-10=3 >=10=7\n"
	if err := os.WriteFile(f.log, []byte(logBody), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f propFixture) args() []string {
	return []string{"-date", "2026-10-09", "-config-dir", f.config, "-log", f.log,
		"-json-out", f.json, "-report-out", f.report, "-openai-config", f.openai}
}

func propMainResult(t *testing.T, args []string, mode string) (int, string, string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, append([]string{"-test.run=^TestPropMainHelper$", "--"}, args...)...)
	cmd.Dir = t.TempDir()
	cmd.Env = append(os.Environ(), "DXC_PROP_MAIN_HELPER="+mode, "OPENAI_API_KEY=")
	cmd.WaitDelay = 2 * time.Second
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("main missed execution bound: %v", ctx.Err())
	}
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("main failed to launch/join: %v", err)
		}
		code = exitErr.ExitCode()
	}
	return code, stdout.String(), stderr.String()
}
