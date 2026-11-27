package telnet

import (
	"crypto/sha256"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dxcluster/config"

	"github.com/gorilla/websocket"
)

const (
	testWebHost   = "pilot.example.com"
	testWebSecret = "unit-test-private-origin-credential"
)

func testWebHeaders() http.Header {
	h := make(http.Header)
	h.Set("Origin", "https://"+testWebHost)
	h.Set("X-GoCluster-Edge-Secret", testWebSecret)
	h.Set("CF-Connecting-IP", "192.0.2.7")
	h.Set("X-GoCluster-Visitor-IP", "192.0.2.7")
	return h
}

func TestWebListenerRejectsUntrustedRequests(t *testing.T) {
	s := newPreloginTestServer(nil)
	w := &WebListener{cluster: s, cfg: config.WebConfig{Hostname: testWebHost}, secret: sha256.Sum256([]byte(testWebSecret))}
	for name, mutate := range map[string]func(*http.Request){
		"peer":            func(r *http.Request) { r.RemoteAddr = "192.0.2.1:4444" },
		"host":            func(r *http.Request) { r.Host += ":443" },
		"origin":          func(r *http.Request) { r.Header.Set("Origin", "http://"+testWebHost) },
		"two origins":     func(r *http.Request) { r.Header.Add("Origin", "https://"+testWebHost) },
		"secret":          func(r *http.Request) { r.Header.Set("X-GoCluster-Edge-Secret", "wrong") },
		"two secrets":     func(r *http.Request) { r.Header.Add("X-GoCluster-Edge-Secret", testWebSecret) },
		"forwarded only":  func(r *http.Request) { r.Header.Del("CF-Connecting-IP"); r.Header.Set("X-Forwarded-For", "192.0.2.7") },
		"two addresses":   func(r *http.Request) { r.Header.Add("CF-Connecting-IP", "192.0.2.7") },
		"address list":    func(r *http.Request) { r.Header.Set("CF-Connecting-IP", "192.0.2.7, 192.0.2.8") },
		"missing stamp":   func(r *http.Request) { r.Header.Del("X-GoCluster-Visitor-IP") },
		"two stamps":      func(r *http.Request) { r.Header.Add("X-GoCluster-Visitor-IP", "192.0.2.7") },
		"different stamp": func(r *http.Request) { r.Header.Set("X-GoCluster-Visitor-IP", "192.0.2.8") },
		"zone": func(r *http.Request) {
			r.Header.Set("CF-Connecting-IP", "fe80::1%wifi")
			r.Header.Set("X-GoCluster-Visitor-IP", "fe80::1%wifi")
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/ws", nil)
			r.Host = testWebHost
			r.RemoteAddr = "127.0.0.1:1234"
			r.Header = testWebHeaders()
			mutate(r)
			result := httptest.NewRecorder()
			w.handleUpgrade(result, r)
			if result.Code != http.StatusForbidden || len(s.preloginByIP) != 0 {
				t.Fatalf("status=%d attributed IPs=%d", result.Code, len(s.preloginByIP))
			}
		})
	}
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/ws", nil)
	r.Host = testWebHost
	r.RemoteAddr = "[::1]:1234"
	r.Header = testWebHeaders()
	r.Header.Set("CF-Connecting-IP", "2001:db8::1")
	r.Header.Set("X-GoCluster-Visitor-IP", "2001:0db8::1")
	if addr, ok := w.visitor(r); !ok || addr.IP.String() != "2001:db8::1" {
		t.Fatalf("IPv6 identity: %v", addr)
	}
}

func TestWebListenerSecretValidation(t *testing.T) {
	var s Server
	c := config.WebConfig{ListenAddress: "127.0.0.1:8080", Hostname: testWebHost, HeartbeatSeconds: 30, MaxConnections: 1}
	if w, err := s.StartWebListener(t.Context(), c, ""); w != nil || err == nil {
		t.Fatalf("disabled: %v, %v", w, err)
	}
	c.Enabled = true
	for _, secret := range []string{"", strings.Repeat("x", 31), strings.Repeat("x", 257), testWebSecret + " ", testWebSecret + "\n"} {
		if _, err := s.StartWebListener(t.Context(), c, secret); err == nil || !strings.Contains(err.Error(), "GOCLUSTER_WEB_EDGE_SECRET") {
			t.Fatalf("bad secret length %d: %v", len(secret), err)
		}
	}
}

func TestWebListenerSharesPreloginAllowanceWithTelnet(t *testing.T) {
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.Transport, opts.EchoMode = "native", "off"
		opts.AcceptRateGlobal, opts.AcceptBurstGlobal = 0.001, 4
	})
	telnetAddr := startSessionCeilingListener(t, s)
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	w := s.serveWebListener(listener, config.WebConfig{Hostname: testWebHost, HeartbeatSeconds: 30, MaxConnections: 2}, testWebSecret)
	t.Cleanup(w.Stop)
	url := "ws://" + listener.Addr().String() + "/ws"
	headers := testWebHeaders()
	headers.Set("Host", testWebHost)
	dial := websocket.Dialer{HandshakeTimeout: 2 * time.Second}

	for range 2 {
		conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(t.Context(), "tcp", telnetAddr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		ceilingReadThrough(t, conn, "login: ")

		ws, response, err := dial.DialContext(t.Context(), url, headers)
		if response != nil {
			_ = response.Body.Close()
		}
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ws.Close() })
		if err := ws.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, prompt, err := ws.ReadMessage(); err != nil || !strings.Contains(string(prompt), "login:") {
			t.Fatalf("web login prompt: %q %v", prompt, err)
		}
		_ = conn.Close()
		waitSessionCeiling(t, func() bool { return s.PreloginAdmissionMetricSnapshot().Active == 1 })
		_ = ws.Close()
		waitSessionCeiling(t, func() bool { return s.PreloginAdmissionMetricSnapshot().Active == 0 })
	}

	ws, response, err := dial.DialContext(t.Context(), url, headers)
	if response != nil {
		_ = response.Body.Close()
	}
	if ws != nil {
		_ = ws.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected shared allowance refusal, response=%v err=%v", response, err)
	}
	if got := s.PreloginAdmissionMetricSnapshot(); got.RejectGlobalRate != 1 || got.Active != 0 {
		t.Fatalf("expected one shared rate rejection and no held tickets, got %+v", got)
	}
}

func TestWebListenerTransportAdmissionAndStop(t *testing.T) {
	s := newHandshakeTranscriptServer(t)
	s.preloginConcPerIP = 1
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	w := s.serveWebListener(listener, config.WebConfig{Hostname: testWebHost, HeartbeatSeconds: 30, MaxConnections: 2}, testWebSecret)
	t.Cleanup(w.Stop)
	url := "ws://" + listener.Addr().String() + "/ws"
	headers := testWebHeaders()
	headers.Set("Host", testWebHost)
	d := websocket.Dialer{HandshakeTimeout: time.Second}
	ws, resp, err := d.DialContext(t.Context(), url, headers)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := ws.ReadMessage()
	if err != nil || !strings.Contains(string(msg), "login:") {
		t.Fatalf("login: %q %v", msg, err)
	}
	denied, resp, err := d.DialContext(t.Context(), url, headers)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if denied != nil {
		denied.Close()
	}
	if resp == nil || resp.StatusCode != http.StatusTooManyRequests || err == nil {
		t.Fatalf("shared per-IP quota: response=%v err=%v", resp, err)
	}
	headers.Set("CF-Connecting-IP", "192.0.2.8")
	headers.Set("X-GoCluster-Visitor-IP", "192.0.2.8")
	other, resp, err := d.DialContext(t.Context(), url, headers)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("independent visitor admission: %v", err)
	}
	defer other.Close()
	// A hijacked session retains an origin slot. The next complete request
	// can reach the kernel backlog, but HTTP must wait for a release.
	dialer := net.Dialer{Timeout: time.Second}
	queued, err := dialer.DialContext(t.Context(), "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer queued.Close()
	if _, err = queued.Write([]byte("GET / HTTP/1.1\r\nHost: pilot.example.com\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	if err = queued.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	_, err = queued.Read(buf)
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("full origin accepted a request: %v", err)
	}
	_ = other.Close()
	if err = queued.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	n, err := queued.Read(buf)
	if err != nil || !strings.HasPrefix(string(buf[:n]), "HTTP/1.1 404") {
		t.Fatalf("request after release: %q, %v", buf[:n], err)
	}
	_ = queued.Close()
	partial, err := dialer.DialContext(t.Context(), "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer partial.Close()
	if _, err = partial.Write([]byte("GET /ws HTTP/1.1\r\n")); err != nil {
		t.Fatal(err)
	}
	w.Stop()
	s.preloginMu.Lock()
	active := s.preloginActive
	s.preloginMu.Unlock()
	if active != 0 {
		t.Fatalf("shutdown retained %d pending logins", active)
	}
	if _, _, err = ws.ReadMessage(); err == nil {
		t.Fatal("upgraded connection remained open")
	}
	if err = partial.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = partial.Read(make([]byte, 1)); err == nil {
		t.Fatal("incomplete HTTP request remained open")
	}
	if errors.As(err, &timeout) && timeout.Timeout() {
		t.Fatal("slow handshake only exited on client deadline")
	}
}
