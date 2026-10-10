package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestProbeMainHelper(t *testing.T) {
	if os.Getenv("DXC_PROBE_MAIN_HELPER") != "eof" {
		t.Skip("actual-main subprocess only")
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("missing helper separator")
	}
	args := os.Args[separator+1:]
	host := slices.Index(args, "-host")
	if host < 0 || host+1 >= len(args) || args[host+1] != "127.0.0.1" {
		t.Fatal("actual-main fixture refuses external address")
	}
	os.Args = append([]string{"peerpoc"}, args...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	main()
	os.Exit(0)
}

func TestProbeActualMainEOFExitsUnsuccessfully(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), probeFixtureBound)
	defer cancel()
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverResult := make(chan error, 1)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			serverResult <- err
			return
		}
		defer conn.Close()
		if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			serverResult <- err
			return
		}
		line, err := bufio.NewReader(conn).ReadString('\n')
		if err == nil && line != "N2WQ-33\r\n" {
			err = fmt.Errorf("actual-main login = %q", line)
		}
		// A literal login proves the actual child connected and ran main before
		// this deliberate EOF; launch failure is never a successful witness.
		serverResult <- err
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case <-serverDone:
		case <-time.After(probeFixtureBound):
			t.Error("actual-main server owner did not join")
		}
	})
	port := listener.Addr().(*net.TCPAddr).Port
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestProbeMainHelper$", "--",
		"-host", "127.0.0.1", "-port", strconv.Itoa(port), "-reconnect=false", "-keepalive=0")
	cmd.Env = append(os.Environ(), "DXC_PROBE_MAIN_HELPER=eof")
	cmd.WaitDelay = 2 * time.Second
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	err = cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("actual main exceeded parent bound: %v", ctx.Err())
	}
	if serverErr := awaitProbe(ctx, t, serverResult, "actual-main connection/login witness"); serverErr != nil {
		t.Fatalf("actual-main fixture failed: %v", serverErr)
	}
	awaitProbe(ctx, t, serverDone, "actual-main server completion")
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("actual-main status = %v; output = %s", err, output.String())
	}
	if !strings.Contains(output.String(), "session ended: EOF") {
		t.Fatalf("actual main did not retain EOF cause: %s", output.String())
	}
}

func TestProbeCommandExplicitLocalCancelReturnsSuccess(t *testing.T) {
	guard, endGuard := context.WithTimeout(context.Background(), probeFixtureBound)
	defer endGuard()
	ctx, cancel := context.WithCancel(guard)
	defer cancel()
	client, remote := net.Pipe()
	defer remote.Close()
	defer client.Close()
	conn := newFixtureProbeConn(client)
	runtime := probeRuntime{dial: func(context.Context, string, string) (net.Conn, error) {
		return conn, nil
	}, newTicker: newProbeTicker}
	result := launchProbeCommand(ctx, t, fixtureProbeConfig(), runtime, func() {
		cancel()
		_ = remote.Close()
		_ = conn.Close()
	})
	awaitProbe(guard, t, conn.readEntered, "actual command session read entry")
	cancel()
	if err := awaitProbe(guard, t, result, "explicit local command cancellation"); err != nil {
		t.Fatalf("local cancellation returned unsuccessful status: %v", err)
	}
	awaitProbe(guard, t, conn.closed, "locally canceled command socket retirement")
}
