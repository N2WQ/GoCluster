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

	"gopkg.in/yaml.v3"
)

func TestSkewMainHelper(t *testing.T) {
	mode := os.Getenv("DXC_SKEW_MAIN_HELPER")
	if mode != "run" && mode != "closed-output" {
		t.Skip("actual-main subprocess only")
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("missing helper separator")
	}
	os.Args = append([]string{"rbnskewfetch"}, os.Args[separator+1:]...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	// Preserve real HTTP/body parsing while refusing any external fallback.
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

func skewFixtureConfig(t *testing.T, url string) string {
	t.Helper()
	dir := t.TempDir()
	src := filepath.Join("..", "..", "data", "config")
	files, err := filepath.Glob(filepath.Join(src, "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("configuration inventory: %v / %d", err, len(files))
	}
	for _, file := range files {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if filepath.Base(file) == "data.yaml" {
			var doc map[string]any
			if err := yaml.Unmarshal(body, &doc); err != nil {
				t.Fatal(err)
			}
			doc["skew"] = map[string]any{"enabled": true, "url": url, "file": "unused.json", "min_abs_skew": 2, "refresh_utc": "00:30"}
			body, err = yaml.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(dir, filepath.Base(file)), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func skewMainResult(t *testing.T, cfg, output, mode string) (int, string, string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestSkewMainHelper$", "--", "-config", cfg, "-out", output)
	cmd.Dir = t.TempDir()
	cmd.Env = append(os.Environ(), "DXC_SKEW_MAIN_HELPER="+mode)
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
