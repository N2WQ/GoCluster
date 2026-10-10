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
	"slices"
	"testing"
	"time"
)

const watcherFixtureBody = `[{"time_tag":"2026-10-09T12:00:00Z","Observatory":"A","Station":"TEST","Numspot":32}]`

func TestWatcherMainHelper(t *testing.T) {
	mode := os.Getenv("DXC_WATCHER_MAIN_HELPER")
	if mode != "run" && mode != "closed-output" {
		t.Skip("actual-main subprocess only")
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("missing helper separator")
	}
	os.Args = append([]string{"voacap_sunspot_watch"}, os.Args[separator+1:]...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	// A broken URL argument must fail locally, never fall through to NOAA.
	// Keep the real HTTP request/body path for owned loopback fixtures.
	http.DefaultClient = &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(address)
			ip := net.ParseIP(host)
			if err != nil || ip == nil || !ip.IsLoopback() {
				return nil, fmt.Errorf("fixture refuses non-loopback address %q", address)
			}
			var dialer net.Dialer
			return dialer.DialContext(ctx, network, address)
		},
	}}
	if mode == "closed-output" {
		if err := os.Stdout.Close(); err != nil {
			os.Exit(91)
		}
	}
	main()
	os.Exit(0)
}

func watcherMainCommand(t *testing.T, args []string, closedOutput bool) (*exec.Cmd, *bytes.Buffer, *bytes.Buffer, context.Context, context.CancelFunc) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, exe, append([]string{"-test.run=^TestWatcherMainHelper$", "--"}, args...)...)
	cmd.Dir = t.TempDir()
	mode := "run"
	if closedOutput {
		mode = "closed-output"
	}
	cmd.Env = append(os.Environ(), "DXC_WATCHER_MAIN_HELPER="+mode)
	cmd.WaitDelay = 2 * time.Second
	stdout, stderr := new(bytes.Buffer), new(bytes.Buffer)
	cmd.Stdout, cmd.Stderr = stdout, stderr
	return cmd, stdout, stderr, ctx, cancel
}

func watcherExitCode(t *testing.T, err error) int {
	t.Helper()
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("child failed to launch or join: %v", err)
	}
	return exitErr.ExitCode()
}
