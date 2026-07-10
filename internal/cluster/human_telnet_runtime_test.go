package cluster

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestTwoHumanTelnetFeedsDeliverDistinctSourceNodes(t *testing.T) {
	listeners := make([]net.Listener, 2)
	entries := make(config.HumanTelnetRegistry, 2)
	for i := range listeners {
		listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen upstream %d: %v", i, err)
		}
		listeners[i] = listener
		defer listener.Close()
		host, port := splitRuntimeTestAddress(t, listener.Addr().String())
		entries[i] = config.RBNConfig{
			Enabled:         true,
			Name:            fmt.Sprintf("FEED-%d", i+1),
			Host:            host,
			Port:            port,
			Callsign:        fmt.Sprintf("N0CALL-%d", i+1),
			TelnetTransport: config.TelnetTransportNative,
			SlotBuffer:      4,
			KeepSSIDSuffix:  true,
			KeepaliveSec:    0,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ingest := make(chan *spot.Spot, 4)
	runtime := &clusterRuntime{
		cfg:         &config.Config{HumanTelnet: entries},
		ctx:         ctx,
		ingestInput: ingest,
	}
	runtime.connectHumanTelnetFeeds()
	t.Cleanup(runtime.stopHumanTelnetFeeds)

	for i, listener := range listeners {
		conn := acceptRuntimeTestConnection(t, listener, i)
		defer conn.Close()
		dxCall := fmt.Sprintf("W%dAW", i+1)
		line := fmt.Sprintf("DX de K%dABC: 14074.0 %s FT8 -10 dB CQ %s\r\n", i+1, dxCall, time.Now().UTC().Format("1504Z"))
		if _, err := conn.Write([]byte(line)); err != nil {
			t.Fatalf("write upstream %d spot: %v", i, err)
		}
	}

	seen := make(map[string]string, 2)
	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()
	for len(seen) < 2 {
		select {
		case got := <-ingest:
			if got == nil {
				t.Fatal("received nil human upstream spot")
			}
			seen[got.SourceNode] = got.DXCall
		case <-deadline.C:
			t.Fatalf("timed out waiting for two upstream spots; seen=%v", seen)
		}
	}
	if seen["FEED-1"] != "W1AW" || seen["FEED-2"] != "W2AW" {
		t.Fatalf("per-feed SourceNode/DX values = %v", seen)
	}
	for i := range runtime.humanTelnetFeeds {
		if !runtime.humanTelnetFeeds[i].client.HealthSnapshot().Connected {
			t.Fatalf("%s disconnected after delivering its spot", runtime.humanTelnetFeeds[i].label)
		}
	}
}

func acceptRuntimeTestConnection(t *testing.T, listener net.Listener, index int) net.Conn {
	t.Helper()
	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()
	select {
	case conn := <-accepted:
		return conn
	case err := <-acceptErr:
		t.Fatalf("accept upstream %d: %v", index, err)
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out accepting upstream %d", index)
	}
	return nil
}

func TestHumanTelnetFeedsStartIndependentlyAndStopJoined(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen healthy upstream: %v", err)
	}
	defer listener.Close()
	healthyHost, healthyPort := splitRuntimeTestAddress(t, listener.Addr().String())

	failedListener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed-upstream placeholder: %v", err)
	}
	failedHost, failedPort := splitRuntimeTestAddress(t, failedListener.Addr().String())
	_ = failedListener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ingest := make(chan *spot.Spot, 8)
	runtime := &clusterRuntime{
		cfg: &config.Config{
			HumanTelnet: config.HumanTelnetRegistry{
				{Enabled: true, Name: "PRIMARY", Host: failedHost, Port: failedPort, Callsign: "N0CALL-1", TelnetTransport: config.TelnetTransportNative, SlotBuffer: 4, KeepSSIDSuffix: true},
				{Enabled: false, Name: "DISABLED", Host: "disabled.invalid", Port: 7300, Callsign: "N0CALL-2", TelnetTransport: config.TelnetTransportNative, SlotBuffer: 4, KeepSSIDSuffix: true},
				{Enabled: true, Name: "BACKUP", Host: healthyHost, Port: healthyPort, Callsign: "N0CALL-3", TelnetTransport: config.TelnetTransportNative, SlotBuffer: 4, KeepSSIDSuffix: true},
			},
		},
		ctx:         ctx,
		ingestInput: ingest,
	}

	runtime.connectHumanTelnetFeeds()
	if got := len(runtime.humanTelnetFeeds); got != 2 {
		t.Fatalf("live human feed count = %d, want 2", got)
	}
	if runtime.humanTelnetFeeds[0].label != "HUMAN/PRIMARY" || runtime.humanTelnetFeeds[1].label != "HUMAN/BACKUP" {
		t.Fatalf("human feed order/labels = %q, %q", runtime.humanTelnetFeeds[0].label, runtime.humanTelnetFeeds[1].label)
	}

	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()
	var upstream net.Conn
	select {
	case upstream = <-accepted:
		defer upstream.Close()
	case err := <-acceptErr:
		t.Fatalf("accept healthy upstream: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("healthy human feed did not dial while the first feed was unavailable")
	}

	deadline := time.Now().Add(2 * time.Second)
	for !runtime.humanTelnetFeeds[1].client.HealthSnapshot().Connected && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	sources := dashboardHumanIngestSources(runtime.humanTelnetFeeds)
	if len(sources) != 2 || sources[0].Connected || !sources[1].Connected {
		t.Fatalf("human dashboard states = %#v, want failed red and healthy green", sources)
	}
	lines := formatIngestSourceLines(sources)
	if len(lines) == 0 || lines[0] != "[yellow]Ingest[-]: 1 / 2 connected" {
		t.Fatalf("human dashboard aggregate = %q, want 1 / 2 connected", lines)
	}
	joined := strings.Join(lines, "\n")
	if !strings.Contains(joined, "[red]HUMAN/PRIMARY[-]") || !strings.Contains(joined, "[green]HUMAN/BACKUP[-]") {
		t.Fatalf("human dashboard colors = %q, want PRIMARY red and BACKUP green", joined)
	}

	started := time.Now()
	runtime.stopHumanTelnetFeeds()
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("joined human-feed shutdown took %s", elapsed)
	}
	runtime.stopHumanTelnetFeeds()
	if _, ok := <-runtime.humanTelnetRaw; ok {
		t.Fatal("shared raw channel remained open after joined shutdown")
	}
}

func TestHumanTelnetFeedsWithNoEnabledEntriesStartNoRawWorker(t *testing.T) {
	runtime := &clusterRuntime{
		cfg: &config.Config{HumanTelnet: config.HumanTelnetRegistry{
			{Enabled: false, Name: "DISABLED", Host: "disabled.invalid", Port: 7300, Callsign: "N0CALL-1", TelnetTransport: config.TelnetTransportNative, SlotBuffer: 4, KeepSSIDSuffix: true},
		}},
		ctx: context.Background(),
	}
	runtime.connectHumanTelnetFeeds()
	runtime.stopHumanTelnetFeeds()
	if runtime.humanTelnetRaw != nil || len(runtime.humanTelnetFeeds) != 0 {
		t.Fatalf("disabled registry started resources: raw=%v feeds=%d", runtime.humanTelnetRaw != nil, len(runtime.humanTelnetFeeds))
	}
}

func splitRuntimeTestAddress(t *testing.T, address string) (string, int) {
	t.Helper()
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("split address %q: %v", address, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse port %q: %v", portText, err)
	}
	return host, port
}
