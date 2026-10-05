package telnet

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"dxcluster/pathreliability"
	"gopkg.in/yaml.v3"
)

func startMachineTestSession(t *testing.T, transport string) (*Server, net.Conn, *bufio.Reader, <-chan struct{}) {
	t.Helper()
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.Transport, opts.EchoMode = transport, "off"
		opts.LoginGreeting = "ready\n"
		opts.NoiseModel = pathreliability.DefaultConfig().NoiseModel()
		opts.DefaultDedupePolicy = "FAST"
		opts.DedupeFastEnabled, opts.DedupeMedEnabled, opts.DedupeSlowEnabled = true, true, true
	})
	_, conn, done := startHandshakeTranscriptSession(t, s)
	reader := bufio.NewReader(conn)
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	prompt := make([]byte, len("login: "))
	if _, err := io.ReadFull(reader, prompt); err != nil || string(prompt) != "login: " {
		t.Fatalf("prompt=%q, %v", prompt, err)
	}
	if _, err := io.WriteString(conn, "W1ABC-1\r\n"); err != nil {
		t.Fatal(err)
	}
	line, err := reader.ReadString('\n')
	if err != nil || line != "ready\r\n" {
		t.Fatalf("greeting=%q, %v", line, err)
	}
	return s, conn, reader, done
}

func readMachineTestFrame(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	var response strings.Builder
	for response.Len() <= maxYAMLBytes {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read YAML frame: %v, partial=%q", err, response.String())
		}
		response.WriteString(line)
		if line == "...\r\n" {
			break
		}
	}
	result := response.String()
	if !strings.HasPrefix(result, "---\r\n") || !strings.HasSuffix(result, "...\r\n") || len(result) > maxYAMLBytes {
		t.Fatalf("invalid complete frame=%q", result)
	}
	if strings.Contains(result, "Type RESUME") || strings.Contains(result, "Missed spots") {
		t.Fatal("machine reply has a human pause footer")
	}
	return result
}

func machineTestRevision(t *testing.T, response string) string {
	t.Helper()
	var values struct {
		Revision string `yaml:"revision"`
	}
	if err := yaml.Unmarshal([]byte(response), &values); err != nil || values.Revision == "" {
		t.Fatalf("revision frame=%q, %v", response, err)
	}
	return values.Revision
}

func TestMachineFullSessionNativeAndZiutek(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s, conn, reader, done := startMachineTestSession(t, transport)
			defer closeHandshakeTranscriptSession(t, conn, done)
			if _, err := io.WriteString(conn, "get yaml settings id Noise-1\r\n"); err != nil {
				t.Fatal(err)
			}
			response := readMachineTestFrame(t, reader)
			if !strings.Contains(response, "request_id: Noise-1\r\n") {
				t.Fatalf("correlation case changed: %q", response)
			}
			revision := machineTestRevision(t, response)
			if _, err := io.WriteString(conn, "PAUSE 120\r\n"); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if _, err := reader.ReadString('\n'); err != nil {
					t.Fatal(err)
				}
			}
			s.clientsMutex.RLock()
			client := s.clients["W1ABC-1"]
			s.clientsMutex.RUnlock()
			beforePause := client.readPauseUntilUnixNano.Load()
			body := fmt.Sprintf("PATCH YAML SETTINGS\r\n---\r\nschema_version: 1\r\nrequest_id: edit-1\r\nif_revision: %s\r\nconfiguration:\r\n  noise_class: URBAN\r\n...\r\n", revision)
			if _, err := io.WriteString(conn, body); err != nil {
				t.Fatal(err)
			}
			response = readMachineTestFrame(t, reader)
			if !strings.Contains(response, "operation: PATCH\r\n") || !strings.Contains(response, "persisted: true\r\n") {
				t.Fatalf("PATCH response=%q", response)
			}
			newRevision := machineTestRevision(t, response)
			if newRevision == revision || client.readPauseUntilUnixNano.Load() != beforePause {
				t.Fatal("write failed revision or pause contract")
			}
			// A received invalid document is recoverable and still framed YAML.
			if _, err := io.WriteString(conn, "PATCH YAML SETTINGS\r\n---\r\nschema_version: 1\r\nrequest_id: invalid-1\r\nif_revision: rev-1\r\nconfiguration: {noise_class: null}\r\n...\r\n"); err != nil {
				t.Fatal(err)
			}
			response = readMachineTestFrame(t, reader)
			if !strings.Contains(response, "code: invalid_document\r\n") || client.readPauseUntilUnixNano.Load() != beforePause {
				t.Fatalf("invalid YAML response=%q", response)
			}
			if _, err := io.WriteString(conn, "GET YAML SETTINGS ID After-1\r\n"); err != nil {
				t.Fatal(err)
			}
			response = readMachineTestFrame(t, reader)
			if !strings.Contains(response, "request_id: After-1\r\n") || !strings.Contains(response, "noise_class: URBAN\r\n") || machineTestRevision(t, response) != newRevision {
				t.Fatalf("recovered GET=%q", response)
			}
		})
	}
}

func TestMachineRejectedHeaderNeverDispatchesPayload(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		for _, header := range []string{"PUT YAML CONFIG EXTRA", "PUT YAML CONFIG _"} {
			t.Run(transport+header, func(t *testing.T) {
				s, conn, reader, done := startMachineTestSession(t, transport)
				defer closeHandshakeTranscriptSession(t, conn, done)
				if _, err := io.WriteString(conn, "PAUSE 120\r\n"); err != nil {
					t.Fatal(err)
				}
				for range 2 {
					if _, err := reader.ReadString('\n'); err != nil {
						t.Fatal(err)
					}
				}
				s.clientsMutex.RLock()
				client := s.clients["W1ABC-1"]
				s.clientsMutex.RUnlock()
				before := client.readPauseUntilUnixNano.Load()
				writeDone := make(chan struct{})
				go func() { _, _ = io.WriteString(conn, header+"\r\n---\r\nRESUME\r\n...\r\n"); close(writeDone) }()
				response := readMachineTestFrame(t, reader)
				if !strings.Contains(response, "code: invalid_header\r\n") {
					t.Fatalf("header rejection=%q", response)
				}
				if _, err := reader.ReadByte(); err == nil {
					t.Fatal("terminal header returned to dispatch")
				}
				waitConfigurationTest(t, done)
				waitConfigurationTest(t, writeDone)
				if client.readPauseUntilUnixNano.Load() != before {
					t.Fatal("RESUME payload was executed")
				}
			})
		}
	}
}

func TestMachineCompleteGETHeaderErrorIsRecoverable(t *testing.T) {
	_, conn, reader, done := startMachineTestSession(t, "native")
	defer closeHandshakeTranscriptSession(t, conn, done)
	if _, err := io.WriteString(conn, "GET YAML SETTINGS EXTRA\r\n"); err != nil {
		t.Fatal(err)
	}
	if response := readMachineTestFrame(t, reader); !strings.Contains(response, "code: invalid_header") {
		t.Fatal(response)
	}
	if _, err := io.WriteString(conn, "GET YAML SETTINGS ID retry-1\r\n"); err != nil {
		t.Fatal(err)
	}
	if response := readMachineTestFrame(t, reader); !strings.Contains(response, "request_id: retry-1") {
		t.Fatal(response)
	}
}
