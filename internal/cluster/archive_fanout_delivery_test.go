//go:build !qualification

// File role: Exercises the immutable final handoff with a running archive,
// real TCP consumers and a registered peer. This is an ownership regression;
// its confidence is assigned before fanout, without changing scoring rules.
package cluster

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/peer"
	"dxcluster/spot"
	"dxcluster/telnet"
)

func TestArchiveImmutableFanoutDelivered(t *testing.T) {
	t.Chdir(t.TempDir())
	layout := spot.CurrentDXClusterLayout()
	if err := spot.SetDXClusterLineLength(78); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = spot.SetDXClusterLineLength(layout.LineLength) })
	ring := buffer.NewRingBuffer(32)
	cfg := config.ArchiveConfig{DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 32, BatchSize: 1, BatchIntervalMS: 1}
	writer, err := archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	writer.Start()
	t.Cleanup(writer.Stop)
	port := runtimeQualificationPort(t)
	srv := telnet.NewServer(telnet.ServerOptions{
		Port: port, LoginPrompt: "login: ", LoginGreeting: "FANOUT_READY\n",
		MaxConnections: 2, BroadcastWorkers: 1, BroadcastQueue: 32, WorkerQueue: 32, ClientBuffer: 32,
		BroadcastBatchIntervalSet: true,
	}, nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	// Login helpers close sockets first (cleanup is LIFO). Wait for their
	// unregister/profile writes on both successful and failing assertion paths
	// before t.Chdir restores the package cwd; Server.Stop does not join them.
	t.Cleanup(func() {
		deadline := time.Now().Add(3 * time.Second)
		for len(srv.ListClientCallsigns()) != 0 {
			if time.Now().After(deadline) {
				t.Error("fanout clients did not retire")
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
	normal, normalReader := archiveFanoutLogin(t, port, "DL1ABC", "OFF")
	diag, diagReader := archiveFanoutLogin(t, port, "DL2ABC", "CONF")
	manager, recipient := archiveFanoutPeer(t)
	pipeline := newDeliveryTestPipeline(ring, writer, srv)
	pipeline.peerManager = manager
	at := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 16; i++ {
		s := newDeliveryTestSpot("K1ABC", "N0AAA", at.Add(time.Duration(i)*time.Millisecond))
		s.SourceType, s.Confidence, s.ConfidencePercent, s.ConfidencePercentOK = spot.SourceManual, "V", 64, true
		pipeline.deliverSpot(&outputSpotContext{spot: s})
		archiveFanoutReadDX(t, normal, normalReader, "CW", at)
		archiveFanoutReadDX(t, diag, diagReader, "CW 64%", at)
		frame := peerAdmissionReadSpot(t, recipient)
		fields := strings.Split(frame, "^")
		if len(fields) < 9 || fields[0] != "PC61" || fields[1] != "14020.0" || fields[2] != "K1ABC" ||
			fields[3] != at.Format("02-Jan-2006") || fields[4] != at.Format("1504Z") || fields[6] != "N0AAA" || fields[7] != "N0CALL-1" {
			t.Fatalf("actual peer receipt: %q", frame)
		}
	}
	rows := ring.GetRecent(32)
	if len(rows) != 16 {
		t.Fatalf("ring count=%d want 16", len(rows))
	}
	for _, s := range rows {
		archiveFanoutFields(t, s)
		if s.SnapshotForAsync() != s || !s.ConfidencePercentOK || s.ConfidencePercent != 64 {
			t.Fatal("ring must retain sealed identity and its percentage")
		}
	}
	writer.Stop() // Join insertion and drain accepted work before reopening.
	reopened, err := archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	rows, err = reopened.Recent(32)
	if err != nil || len(rows) != 16 {
		t.Fatalf("reopened rows=%d err=%v", len(rows), err)
	}
	for _, s := range rows {
		archiveFanoutFields(t, s) // Archive stores the glyph, not the percentage.
	}
}

func archiveFanoutFields(t *testing.T, s *spot.Spot) {
	t.Helper()
	if s.DXCall != "K1ABC" || s.DXCallNorm != "K1ABC" || s.DECallNorm != "N0AAA" ||
		s.Frequency != 14020 || s.ModeNorm != "CW" || s.Confidence != "V" {
		t.Fatalf("delivered identity/glyph changed: %+v", s)
	}
}

func archiveFanoutLogin(t *testing.T, port int, call, diag string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, reader := runtimeQualificationLogin(t, fmt.Sprintf("127.0.0.1:%d", port), "", call)
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		if strings.Contains(customSCPDeliveryReadLine(t, reader), "FANOUT_READY") {
			if _, err := io.WriteString(conn, "SET DIAG "+diag+"\r\n"); err != nil {
				t.Fatal(err)
			}
			for j := 0; j < 16; j++ {
				if strings.Contains(customSCPDeliveryReadLine(t, reader), "Diagnostic comments: "+diag) {
					return conn, reader
				}
			}
		}
	}
	t.Fatal("fanout login/diagnostic acknowledgement missing")
	return nil, nil
}

func archiveFanoutReadDX(t *testing.T, conn net.Conn, reader *bufio.Reader, comment string, at time.Time) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	line := []byte(strings.Repeat(" ", 78))
	copy(line, "DX de N0AAA:     14020.00")
	copy(line[27:], "K1ABC")
	copy(line[39:], comment)
	line[71] = 'V'
	copy(line[73:], at.Format("1504Z"))
	want := string(line) + "\r\n"
	for i := 0; i < 16; i++ {
		got := customSCPDeliveryReadLine(t, reader)
		if start := strings.Index(got, "DX de "); start >= 0 {
			if got[start:] != want {
				t.Fatalf("actual TCP receipt: got %q want %q", got[start:], want)
			}
			return
		}
	}
	t.Fatal("fanout DX receipt missing")
}

func archiveFanoutPeer(t *testing.T) (*peer.Manager, *peerAdmissionSocket) {
	t.Helper()
	cfg := peerRuntimeTestConfig().Peering
	cfg.MaxPeers, cfg.ListenPort, cfg.ForwardSpots = 1, runtimeQualificationPort(t), true
	cfg.TelnetTransport = config.TelnetTransportNative
	cfg.Timeouts = config.PeeringTimeouts{LoginSeconds: 5, InitSeconds: 5, IdleSeconds: 30}
	cfg.Peers = []config.PeeringPeer{{Enabled: true, Direction: config.PeeringPeerDirectionInbound,
		Family: config.PeeringPeerFamilyDXSpider, RemoteCallsign: "N2DST", PreferPC9x: true}}
	manager, err := peer.NewManager(cfg, cfg.LocalCallsign, make(chan *spot.Spot, 1), 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(manager.Stop)
	if err := manager.SetBuildIdentity("test", "", "", "", ""); err != nil {
		t.Fatal(err)
	}
	manager.SetMembershipProvider(func() peer.LocalMembership { return peer.LocalMembership{Complete: true} })
	if err := manager.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	recipient := peerAdmissionLogin(t, fmt.Sprintf("127.0.0.1:%d", cfg.ListenPort), "N2DST", true)
	peerAdmissionSendAndAck(t, recipient, "")
	return manager, recipient
}
