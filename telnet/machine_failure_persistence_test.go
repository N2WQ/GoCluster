package telnet

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/filter"
)

func TestMachineTerminalRejectionPreservesDivergentSavedPreferences(t *testing.T) {
	oversized := "value: " + strings.Repeat("a", maxYAMLBytes+1-len("value: ")-len("\r\nRESUME\r\n")) + "\r\nRESUME\r\n"
	for _, transport := range []string{"native", "ziutek"} {
		for _, tc := range []struct {
			name        string
			input       string
			headerError bool
		}{
			{name: "argument error", input: "PUT YAML CONFIG EXTRA\r\n---\r\nRESUME\r\n...\r\n", headerError: true},
			{name: "early reader error", input: "PATCH YAML SETTINGS _\r\n---\r\nRESUME\r\n...\r\n", headerError: true},
			{name: "framing error", input: "PUT YAML FILTER\r\n--- EXTRA\r\nRESUME\r\n...\r\n"},
			{name: "oversized body", input: "PUT YAML FILTER\r\n---\r\n" + oversized + "...\r\n"},
			{name: "ordinary disconnect"},
		} {
			t.Run(transport+"/"+tc.name, func(t *testing.T) {
				s, conn, reader, done := startMachineTestSession(t, transport)
				defer closeHandshakeTranscriptSession(t, conn, done)
				var failSave atomic.Bool
				s.saveConfigurationFn = func(call string, cfg filter.Configuration, ref *filter.PresetReference, ips []string) error {
					if failSave.Load() {
						return os.ErrPermission
					}
					return filter.SaveConfiguration(call, cfg, ref, ips)
				}
				for _, command := range []string{"SET NOISE QUIET", "SAVE PRESET BEFORE"} {
					if _, err := io.WriteString(conn, command+"\r\n"); err != nil {
						t.Fatal(err)
					}
					line, err := reader.ReadString('\n')
					if err != nil || strings.Contains(line, "failed") {
						t.Fatalf("setup %s: %q, %v", command, line, err)
					}
				}
				path := filepath.Join(filter.UserDataDir, "W1ABC-1.yaml")
				before := presetDiskBytes(t, path)
				if !bytes.Contains(before, []byte("noise_class: QUIET")) || !bytes.Contains(before, []byte("name: BEFORE")) {
					t.Fatalf("setup did not save literal preferences/reference: %q", before)
				}
				failSave.Store(true)
				if _, err := io.WriteString(conn, "SET NOISE URBAN\r\n"); err != nil {
					t.Fatal(err)
				}
				line, err := reader.ReadString('\n')
				if err != nil || !strings.Contains(line, "warning: failed to persist") {
					t.Fatalf("failed human save: %q, %v", line, err)
				}
				failSave.Store(false)
				if !bytes.Equal(before, presetDiskBytes(t, path)) {
					t.Fatal("failed human save changed the durable record")
				}
				if _, err := io.WriteString(conn, "GET YAML SETTINGS ID diverged-1\r\n"); err != nil {
					t.Fatal(err)
				}
				if response := readMachineTestFrame(t, reader); !strings.Contains(response, "noise_class: URBAN\r\n") || !strings.Contains(response, "modified: true\r\n") {
					t.Fatalf("fixture did not establish divergent live preferences: %q", response)
				}
				if tc.input == "" {
					_ = conn.Close()
				} else {
					writeDone := make(chan struct{})
					rejectedConn := conn
					go func() { _, _ = io.WriteString(rejectedConn, tc.input); close(writeDone) }()
					t.Cleanup(func() {
						_ = rejectedConn.Close()
						waitConfigurationTest(t, writeDone)
					})
					if tc.headerError {
						if response := readMachineTestFrame(t, reader); !strings.Contains(response, "code: invalid_header\r\n") {
							t.Fatal(response)
						}
					}
					if _, err := reader.ReadByte(); err == nil {
						t.Fatal("terminal rejection kept the connection open")
					}
					waitConfigurationTest(t, writeDone)
				}
				waitConfigurationTest(t, done)
				if s.GetClientCount() != 0 {
					t.Fatal("terminal session retained registry membership")
				}
				after := presetDiskBytes(t, path)
				wantNoise := "QUIET"
				wantModified := "false"
				if tc.input == "" {
					wantNoise = "URBAN"
					wantModified = "true"
					if !bytes.Contains(after, []byte("noise_class: URBAN")) {
						t.Fatalf("ordinary disconnect failed to save current preferences: %q", after)
					}
				} else if !bytes.Equal(before, after) {
					t.Fatal("terminal rejection changed saved bytes after the failed human save")
				}
				conn, reader, nextDone := reconnectMachineTestSession(t, s)
				defer closeHandshakeTranscriptSession(t, conn, nextDone)
				if _, err := io.WriteString(conn, "GET YAML SETTINGS ID restored-1\r\n"); err != nil {
					t.Fatal(err)
				}
				response := readMachineTestFrame(t, reader)
				if !strings.Contains(response, "noise_class: "+wantNoise+"\r\n") || !strings.Contains(response, "name: BEFORE\r\n") || !strings.Contains(response, "associated: true\r\n") {
					t.Fatalf("reconnect lost preserved preferences/reference: %q", response)
				}
				if !strings.Contains(response, "modified: "+wantModified+"\r\n") {
					t.Fatalf("reconnect lost preserved baseline: %q", response)
				}
			})
		}
	}
}

func reconnectMachineTestSession(t *testing.T, s *Server) (net.Conn, *bufio.Reader, <-chan struct{}) {
	t.Helper()
	_, conn, done := startHandshakeTranscriptSession(t, s)
	t.Cleanup(func() { closeHandshakeTranscriptSession(t, conn, done) })
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	prompt := make([]byte, len("login: "))
	if _, err := io.ReadFull(reader, prompt); err != nil || string(prompt) != "login: " {
		t.Fatalf("reconnect prompt=%q, %v", prompt, err)
	}
	if _, err := io.WriteString(conn, "W1ABC-1\r\n"); err != nil {
		t.Fatal(err)
	}
	if line, err := reader.ReadString('\n'); err != nil || line != "ready\r\n" {
		t.Fatalf("reconnect greeting=%q, %v", line, err)
	}
	return conn, reader, done
}
