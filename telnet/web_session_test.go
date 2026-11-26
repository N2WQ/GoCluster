package telnet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/commands"
	"github.com/gorilla/websocket"
	"gopkg.in/yaml.v3"
)

func browserWirePair(t *testing.T, serve func(*websocket.Conn)) (*websocket.Conn, <-chan struct{}) {
	t.Helper()
	return browserWirePairWithListener(t, serve, nil)
}

func browserWirePairWithListener(t *testing.T, serve func(*websocket.Conn), wrap func(net.Listener) net.Listener) (*websocket.Conn, <-chan struct{}) {
	t.Helper()
	done := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		upgrader := websocket.Upgrader{ReadBufferSize: 256, WriteBufferSize: 256}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer ws.Close()
		serve(ws)
	}))
	if wrap != nil {
		server.Listener = wrap(server.Listener)
	}
	server.Start()
	t.Cleanup(server.Close)
	client, response, err := websocket.DefaultDialer.DialContext(t.Context(), "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if response != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.Close()
		select {
		case <-done:
		case <-time.After(4 * time.Second):
			t.Error("browser server did not retire")
		}
	})
	return client, done
}

func browserSend(t *testing.T, ws *websocket.Conn, msg string) {
	t.Helper()
	if err := ws.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
		t.Fatal(err)
	}
}
func browserReceive(t *testing.T, ws *websocket.Conn) string {
	t.Helper()
	ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	kind, p, err := ws.ReadMessage()
	if err != nil || kind != websocket.TextMessage {
		t.Fatalf("browser read kind %d err %v", kind, err)
	}
	return string(p)
}

func browserLogin(t *testing.T, s *Server, call string, heartbeat time.Duration) (*websocket.Conn, <-chan struct{}, string) {
	t.Helper()
	ws, done := browserWirePair(t, func(ws *websocket.Conn) {
		s.serveWebSocketSession(ws, &net.TCPAddr{IP: net.ParseIP("192.0.2.57")}, nil, time.Now(), heartbeat)
	})
	if prompt := browserReceive(t, ws); prompt != "login: " {
		t.Fatalf("prompt %q", prompt)
	}
	browserSend(t, ws, call)
	var greeting strings.Builder
	for !strings.Contains(greeting.String(), "READY\r\n") {
		greeting.WriteString(browserReceive(t, ws))
	}
	enableBrowserJSON(t, ws)
	return ws, done, greeting.String()
}

func enableBrowserJSON(t *testing.T, ws *websocket.Conn) {
	t.Helper()
	browserSend(t, ws, "SET JSON ON VERSION 1")
	if ack := browserReceive(t, ws); ack != "JSON ON 1\r\n" {
		t.Fatalf("ack %q", ack)
	}
	count := 0
	for count < 3 {
		for _, row := range strings.Split(strings.TrimSuffix(browserReceive(t, ws), "\n"), "\n") {
			var fields []json.RawMessage
			if json.Unmarshal([]byte(row), &fields) != nil || len(fields) < 2 {
				t.Fatalf("invalid startup record %q", row)
			}
			want := "\"schema\""
			if count == 0 {
				want = "\"hello\""
			}
			if string(fields[0]) != want {
				t.Fatalf("startup record %s want %s", fields[0], want)
			}
			count++
		}
	}
}
func browserYAML(t *testing.T, ws *websocket.Conn, msg string) map[string]any {
	t.Helper()
	browserSend(t, ws, msg)
	var result map[string]any
	for result == nil {
		for _, line := range strings.Split(strings.TrimSuffix(browserReceive(t, ws), "\n"), "\n") {
			var row []string
			if err := json.Unmarshal([]byte(line), &row); err != nil || len(row) < 2 {
				t.Fatalf("invalid JSON reply: %.150s", line)
			}
			if len(row) == 4 && row[0] == "notice" {
				switch row[1] {
				case "config-changed", "presence-changed", "go-selected", "presets-changed", "save-status-changed":
					continue // A Phase 3 advisory may arrive before or beside the reply.
				}
			}
			if len(row) != 2 || row[0] != "yaml" || result != nil {
				t.Fatalf("expected one YAML record: %.150s", line)
			}
			if err := yaml.Unmarshal([]byte(row[1]), &result); err != nil {
				t.Fatal(err)
			}
		}
	}
	return result
}

func TestWebSessionProfileBoundariesAndPrivacy(t *testing.T) {
	s := newHandshakeTranscriptServerWithOptions(t, func(o *ServerOptions) {
		o.Transport, o.HandshakeMode, o.EchoMode = "ziutek", telnetHandshakeMinimal, "server"
		o.LoginGreeting = "<LOGIN_SUMMARY>\nPrevious address: <LAST_IP>\nREADY\n"
		o.CommandRatePerSecond, o.CommandBurst = 1000, 1000
	})
	s.dedupeMedEnabled = true
	const call = "N0CALL-57"
	ws, done, _ := browserLogin(t, s, call, time.Second)
	const get = "GET YAML SETTINGS SCHEMA 7 ID check"
	before := browserYAML(t, ws, get)
	rev := fmt.Sprint(before["revision"])
	if rev == "" || rev == "<nil>" {
		t.Fatal(before)
	}
	patch := func(minutes, revision string, size int) string {
		doc := "schema_version: 7\nrequest_id: update\nif_revision: " + revision + "\nconfiguration:\n  solar_summary_minutes: " + minutes + "\n"
		if size > 0 {
			doc += "#" + strings.Repeat("a", size-len(doc)-2) + "\n"
		}
		return "PATCH YAML SETTINGS SCHEMA 7\n---\n" + doc + "..."
	}
	applied := browserYAML(t, ws, patch("15", rev, maxV7YAMLBytes))
	if applied["error"] != nil || applied["result"] == nil {
		t.Fatalf("valid full-size PATCH: %v", applied)
	}
	next := fmt.Sprint(applied["revision"])
	if next == rev {
		t.Fatal("successful patch did not change revision")
	}
	conflict := browserYAML(t, ws, patch("30", rev, 0))
	if conflict["error"] == nil {
		t.Fatalf("stale revision accepted: %v", conflict)
	}
	for _, test := range []struct {
		size int
		tail string
	}{
		{0, "\nSET GRID FN32"}, {maxV7YAMLBytes + 1, ""}, {0, "\n"},
	} {
		bad := patch("30", next, test.size) + test.tail
		browserSend(t, ws, bad)
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("malformed request left session running")
		}
		ws.Close()
		var greeting string
		ws, done, greeting = browserLogin(t, s, call, time.Second)
		if !strings.Contains(greeting, "Last login:") || strings.Contains(greeting, "192.0.2.57") || strings.Contains(greeting, " from ") {
			t.Fatalf("returning greeting: %s", greeting)
		}
		check := browserYAML(t, ws, get)
		next = fmt.Sprint(check["revision"])
		if !strings.Contains(fmt.Sprint(check["configuration"]), "solar_summary_minutes:15") {
			t.Fatalf("bad input changed profile: %v", check)
		}
	}
	browserSend(t, ws, "SET JSON OFF")
	if row := browserReceive(t, ws); row != "[\"notice\",\"json-off\",\"\"]\n" {
		t.Fatalf("OFF %q", row)
	}
	browserSend(t, ws, "SET JSON OFF")
	if row := browserReceive(t, ws); row != "JSON already off.\r\n" {
		t.Fatalf("text %q", row)
	}
}

func TestWebSessionSplitJSONRecord(t *testing.T) {
	expected := "[\"text\",\"" + strings.Repeat("æ¸¬", 32768) + "\"]\n"
	ws, _ := browserWirePair(t, func(ws *websocket.Conn) {
		w := webSessionConn{ws: ws}
		data := []byte(expected)
		for len(data) > 0 {
			n := min(4093, len(data))
			if _, err := w.writeJSON(data[:n]); err != nil {
				t.Error(err)
				return
			}
			data = data[n:]
		}
	})
	if got := browserReceive(t, ws); got != expected {
		t.Fatalf("split record %d bytes, want %d", len(got), len(expected))
	}
}

func TestWebSessionUploadBudgetBeforeBody(t *testing.T) {
	// Exhaust real reservations, then transmit only the header in a nonfinal
	// fragment. A bounded refusal has to arrive without any YAML body or EOF.
	s := newHandshakeTranscriptServerWithOptions(t, func(o *ServerOptions) { o.LoginGreeting = "READY\n" })
	ws, _, _ := browserLogin(t, s, "N0CALL-58", time.Second)
	c := &Client{sessionProfile: &sessionProfile{}, done: make(chan struct{})}
	for i := 0; i < configurationInputBudget/configurationInputReservation; i++ {
		release, err := acquireConfigurationInput(c)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
	}
	// Gorilla flushes complete wire fragments when its limited write buffer fills.
	writer, err := ws.NextWriter(websocket.TextMessage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.Copy(writer, bytes.NewReader([]byte("PATCH YAML SETTINGS SCHEMA 7\n"+strings.Repeat("x", 16384)))); err != nil {
		t.Fatal(err)
	}
	if got := browserReceive(t, ws); !strings.Contains(got, "busy") {
		t.Fatalf("budget refusal %s", got)
	}
}

func TestWebSessionMissingPongRetires(t *testing.T) {
	s := newHandshakeTranscriptServerWithOptions(t, nil)
	ws, done := browserWirePair(t, func(ws *websocket.Conn) {
		s.serveWebSocketSession(ws, &net.TCPAddr{IP: net.ParseIP("192.0.2.1")}, nil, time.Now(), 50*time.Millisecond)
	})
	if prompt := browserReceive(t, ws); prompt != "login: " {
		t.Fatal(prompt)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("missing pong failed to release prelogin")
	}
}

func TestWebSessionQuietWithPongs(t *testing.T) {
	s := newHandshakeTranscriptServerWithOptions(t, func(o *ServerOptions) { o.LoginGreeting = "READY\n" })
	ws, _ := browserWirePair(t, func(ws *websocket.Conn) {
		s.serveWebSocketSession(ws, &net.TCPAddr{IP: net.ParseIP("192.0.2.57")}, nil, time.Now(), 100*time.Millisecond)
	})
	if got := browserReceive(t, ws); got != "login: " {
		t.Fatal(got)
	}
	browserSend(t, ws, "N0CALL-59")
	for !strings.Contains(browserReceive(t, ws), "READY\r\n") {
	}
	pingCount := 0
	ws.SetPingHandler(func(payload string) error {
		if err := ws.WriteControl(websocket.PongMessage, []byte(payload), time.Now().Add(time.Second)); err != nil {
			return err
		}
		pingCount++
		if pingCount == 5 {
			return ws.WriteMessage(websocket.TextMessage, []byte("SET JSON OFF"))
		}
		return nil
	})
	if got := browserReceive(t, ws); got != "JSON already off.\r\n" || pingCount < 5 {
		t.Fatalf("quiet probe after %d pings: %q", pingCount, got)
	}
}

func TestWebSessionRejectsUnframedCommands(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind int
		body string
	}{
		{"binary", websocket.BinaryMessage, "SET JSON OFF"},
		{"ordinary multi-command", websocket.TextMessage, "SET JSON OFF\nSET JSON ON"},
		{"carriage return", websocket.TextMessage, "SET JSON OFF\r"},
		{"input editing", websocket.TextMessage, "SET JSON OX\bFF"},
		{"extra login", websocket.TextMessage, "N0CALL-1\nN0CALL-2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := make(chan error, 1)
			ws, _ := browserWirePair(t, func(ws *websocket.Conn) {
				w := webSessionConn{ws: ws, headerLimit: 128, allowUpload: true}
				header := make([]byte, 128)
				n, err := w.Read(header)
				if n != 0 || err == nil {
					result <- fmt.Errorf("invalid message delivered %q (refused=%t)", header[:n], err != nil)
					return
				}
				result <- nil
			})
			if err := ws.WriteMessage(tc.kind, []byte(tc.body)); err != nil {
				t.Fatal(err)
			}
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWebSessionHistoryCanOutlastHeartbeat(t *testing.T) {
	const heartbeat = 75 * time.Millisecond
	s := newHandshakeTranscriptServerWithOptions(t, func(o *ServerOptions) { o.LoginGreeting = "READY\n" })
	scanned := make(chan struct{})
	s.processor = commands.NewProcessor(nil, historyReaderFunc(func(archive.HistoryRequest) (archive.HistoryPage, error) {
		close(scanned)
		time.Sleep(6 * heartbeat)
		return archive.HistoryPage{End: archive.HistoryExhausted}, nil
	}), nil, nil, nil, nil)
	ws, _, _ := browserLogin(t, s, "N0CALL-60", heartbeat)
	browserSend(t, ws, "SHOW DX 1")
	select {
	case <-scanned:
	case <-time.After(2 * time.Second):
		t.Fatal("history scan did not start")
	}
	var result strings.Builder
	for !strings.Contains(result.String(), "\"he\",\"exhausted\"") {
		result.WriteString(browserReceive(t, ws))
	}

	browserSend(t, ws, "SET JSON OFF")
	if got := browserReceive(t, ws); !strings.Contains(got, "json-off") {
		t.Fatalf("connection did not survive history: %s", got)
	}
}
