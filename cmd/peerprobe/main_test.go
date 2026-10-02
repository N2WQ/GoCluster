package main

import (
	"bufio"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"dxcluster/peer"
)

func TestPeerProbeUsesParsedPayload(t *testing.T) {
	frame, err := peer.ParseFrame("PC51^N0LOCAL^H1ABC^1^H9^")
	if err != nil {
		t.Fatal(err)
	}
	want := append([]string(nil), frame.Fields...)
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	done := make(chan struct{})
	go func() { defer close(done); handlePeerPing(frame, &sync.Mutex{}, local, "N0LOCAL") }()
	_ = remote.SetReadDeadline(time.Now().Add(time.Second))
	line, err := bufio.NewReader(remote).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if line != "PC51^H1ABC^N0LOCAL^0^\r\n" {
		t.Fatalf("ping response=%q", line)
	}
	<-done
	if !reflect.DeepEqual(frame.Fields, want) {
		t.Fatal("probe changed parsed payload")
	}
}
