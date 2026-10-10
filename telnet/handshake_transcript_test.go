package telnet

import (
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/uls"
)

const hybridLoginGreeting = "" +
	"Hello <CALL>, this is <CLUSTER>\n" +
	"Cluster: Users <USER_COUNT>  Uptime <UPTIME>\n" +
	"Last login: <LAST_LOGIN> from <LAST_IP>\n" +
	"Current commands: <DIALECT> (<DIALECT_SOURCE>).\n" +
	"Use DIALECT LIST or DIALECT <DIALECT_DEFAULT> to change.\n" +
	"Grid: <GRID> (SET GRID <grid>)\n" +
	"Noise: <NOISE> (SET NOISE QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL)\n" +
	"Dedupe policy: <DEDUPE> (SET DEDUPE FAST|MED|SLOW)\n" +
	"Type HELP for all commands.\n\n" +
	"<CALL> de <CLUSTER> <DATETIME>>"

func TestHandleClientPromptOnlyPreloginTranscript(t *testing.T) {
	s := newHandshakeTranscriptServer(t)
	serverConn, clientConn, done := startHandshakeTranscriptSession(t, s)
	defer closeHandshakeTranscriptSession(t, clientConn, done)
	defer serverConn.Close()

	got := readUntilContains(t, clientConn, "login: ", 2*time.Second)
	if got != "login: " {
		t.Fatalf("expected only login prompt, got %q", got)
	}
}

func TestHandleClientSuccessfulLoginTranscript(t *testing.T) {
	s := newHandshakeTranscriptServer(t)
	serverConn, clientConn, done := startHandshakeTranscriptSession(t, s)
	defer closeHandshakeTranscriptSession(t, clientConn, done)
	defer serverConn.Close()

	initial := readUntilContains(t, clientConn, "login: ", 2*time.Second)
	if initial != "login: " {
		t.Fatalf("expected only login prompt, got %q", initial)
	}
	if _, err := io.WriteString(clientConn, "n0call\r\n"); err != nil {
		t.Fatalf("write callsign: %v", err)
	}

	got := readUntilContains(t, clientConn, "UTC>", 2*time.Second)
	for _, want := range []string{
		"Hello N0CALL, this is N2WQ-2",
		"Cluster: Users 1  Uptime ",
		"Last login: (first login) from (unknown)",
		"Current commands: GO (default).",
		"Grid: unset (SET GRID <grid>)",
		"Noise: QUIET (SET NOISE QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL)",
		"Dedupe policy: FAST (SET DEDUPE FAST|MED|SLOW)",
		"Type HELP for all commands.",
		"N0CALL de N2WQ-2 ",
		"UTC>",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("login transcript missing %q:\n%s", want, got)
		}
	}
}

func TestHandleClientRePromptsAfterEmptyAndInvalidLogin(t *testing.T) {
	s := newHandshakeTranscriptServer(t)
	serverConn, clientConn, done := startHandshakeTranscriptSession(t, s)
	defer closeHandshakeTranscriptSession(t, clientConn, done)
	defer serverConn.Close()

	initial := readUntilContains(t, clientConn, "login: ", 2*time.Second)
	if initial != "login: " {
		t.Fatalf("expected only login prompt, got %q", initial)
	}
	if _, err := io.WriteString(clientConn, "\r\n"); err != nil {
		t.Fatalf("write blank login: %v", err)
	}
	emptyResp := readUntilContains(t, clientConn, "login: ", 2*time.Second)
	if !strings.Contains(emptyResp, normalizeOutboundLine("Callsign cannot be empty. Please try again.\n")) || !strings.HasSuffix(emptyResp, "login: ") {
		t.Fatalf("unexpected empty-login response: %q", emptyResp)
	}

	if _, err := io.WriteString(clientConn, "AA\r\n"); err != nil {
		t.Fatalf("write invalid login: %v", err)
	}
	invalidResp := readUntilContains(t, clientConn, "login: ", 2*time.Second)
	if !strings.Contains(invalidResp, normalizeOutboundLine("Invalid call. Please try again.\n")) || !strings.HasSuffix(invalidResp, "login: ") {
		t.Fatalf("unexpected invalid-login response: %q", invalidResp)
	}
}

func TestHandleClientRejectsNonCallsignLoginTokens(t *testing.T) {
	cases := []string{
		"8300",
		"9600",
		"at",
		"ABC",
		"N0@",
		"N0CALL-#",
		"SET/NOFT8",
		"SET/FT8",
		"FT8",
		"NOFT8",
		"PSK31",
	}
	for _, input := range cases {
		t.Run(input, func(t *testing.T) {
			s := newHandshakeTranscriptServer(t)
			serverConn, clientConn, done := startHandshakeTranscriptSession(t, s)
			defer closeHandshakeTranscriptSession(t, clientConn, done)
			defer serverConn.Close()

			initial := readUntilContains(t, clientConn, "login: ", 2*time.Second)
			if initial != "login: " {
				t.Fatalf("expected only login prompt, got %q", initial)
			}
			if _, err := io.WriteString(clientConn, input+"\r\n"); err != nil {
				t.Fatalf("write invalid login %q: %v", input, err)
			}
			resp := readUntilContains(t, clientConn, "login: ", 2*time.Second)
			if !strings.Contains(resp, normalizeOutboundLine("Invalid call. Please try again.\n")) || !strings.HasSuffix(resp, "login: ") {
				t.Fatalf("unexpected invalid-login response for %q: %q", input, resp)
			}
		})
	}
}

func TestValidateLoginCallsignAuthorityChecks(t *testing.T) {
	ctyDB := loadTestCTY(t)
	cases := []struct {
		name         string
		call         string
		ctyLookup    func() *cty.CTYDatabase
		licenseCheck func(string) bool
		wantValid    bool
		wantFailOpen bool
		wantReason   loginValidationReason
	}{
		{
			name:         "CTY known non US",
			call:         "K1ABC",
			ctyLookup:    func() *cty.CTYDatabase { return ctyDB },
			licenseCheck: func(string) bool { return false },
			wantValid:    true,
		},
		{
			name:         "CTY known US licensed",
			call:         "W6ABC",
			ctyLookup:    func() *cty.CTYDatabase { return ctyDB },
			licenseCheck: func(string) bool { return true },
			wantValid:    true,
		},
		{
			name:         "CTY known US unlicensed",
			call:         "W6ABC",
			ctyLookup:    func() *cty.CTYDatabase { return ctyDB },
			licenseCheck: func(string) bool { return false },
			wantReason:   loginValidationReasonUSUnlicensed,
		},
		{
			name:         "CTY unknown",
			call:         "ZZ9ABC",
			ctyLookup:    func() *cty.CTYDatabase { return ctyDB },
			licenseCheck: func(string) bool { return true },
			wantReason:   loginValidationReasonCTYUnknown,
		},
		{
			name:         "CTY unavailable fail open",
			call:         "ZZ9ABC",
			ctyLookup:    func() *cty.CTYDatabase { return nil },
			licenseCheck: func(string) bool { return false },
			wantValid:    true,
			wantFailOpen: true,
			wantReason:   loginValidationReasonCTYUnavailable,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
				opts.CTYLookup = tc.ctyLookup
				opts.USLicenseCheck = tc.licenseCheck
			})
			got := s.validateLoginCallsign(tc.call)
			if got.valid != tc.wantValid || got.failOpen != tc.wantFailOpen || got.reason != tc.wantReason {
				t.Fatalf("validateLoginCallsign(%q) = %+v, want valid=%t failOpen=%t reason=%q", tc.call, got, tc.wantValid, tc.wantFailOpen, tc.wantReason)
			}
		})
	}
}

func TestValidateLoginCallsignAllowsUSAllowlist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "allowlist.txt")
	if err := os.WriteFile(path, []byte("US: W6ALLOW\n"), 0o644); err != nil {
		t.Fatalf("write allowlist: %v", err)
	}
	uls.SetAllowlistPath(path)
	t.Cleanup(func() { uls.SetAllowlistPath("") })

	ctyDB := loadTestCTY(t)
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.CTYLookup = func() *cty.CTYDatabase { return ctyDB }
		opts.USLicenseCheck = func(string) bool { return false }
	})
	got := s.validateLoginCallsign("W6ALLOW")
	if !got.valid {
		t.Fatalf("expected allowlisted US call to pass, got %+v", got)
	}
}

func TestValidateLoginCallsignTestCallBypassesUSLicense(t *testing.T) {
	ctyDB := loadTestCTY(t)
	var licenseCalls atomic.Int64
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.CTYLookup = func() *cty.CTYDatabase { return ctyDB }
		opts.USLicenseCheck = func(string) bool {
			licenseCalls.Add(1)
			return false
		}
	})

	got := s.validateLoginCallsign("W6TEST-1")
	if !got.valid {
		t.Fatalf("expected CTY-valid US TEST call to pass, got %+v", got)
	}
	if calls := licenseCalls.Load(); calls != 0 {
		t.Fatalf("expected TEST call to bypass US license check, got %d calls", calls)
	}

	got = s.validateLoginCallsign("Z9TEST-1")
	if got.valid || got.reason != loginValidationReasonCTYUnknown {
		t.Fatalf("expected CTY-unknown TEST call rejection, got %+v", got)
	}
	if base, ok := loginTestCallBase("W6/K1TEST"); ok || base != "" {
		t.Fatalf("expected slash TEST form not to be treated as local TEST call, got base=%q ok=%t", base, ok)
	}
}

func newHandshakeTranscriptServer(t *testing.T) *Server {
	return newHandshakeTranscriptServerWithOptions(t, nil)
}

func newHandshakeTranscriptServerWithOptions(t *testing.T, configure func(*ServerOptions)) *Server {
	t.Helper()
	tmp := t.TempDir()
	orig := filter.UserDataDir
	filter.UserDataDir = tmp
	t.Cleanup(func() { filter.UserDataDir = orig })

	opts := ServerOptions{
		HandshakeMode:          telnetHandshakeNone,
		WelcomeMessage:         "",
		LoginPrompt:            "login: ",
		LoginGreeting:          hybridLoginGreeting,
		LoginEmptyMessage:      "Callsign cannot be empty. Please try again.\n",
		LoginInvalidMessage:    "Invalid call. Please try again.\n",
		DialectSourceDefault:   "default",
		DialectSourcePersisted: "persisted",
		ClusterCall:            "N2WQ-2",
		LoginLineLimit:         32,
		CommandLineLimit:       128,
	}
	if configure != nil {
		configure(&opts)
	}
	return NewServer(opts, nil)
}

func startHandshakeTranscriptSession(t *testing.T, s *Server) (net.Conn, net.Conn, <-chan struct{}) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		s.handleClient(serverConn, nil)
		close(done)
	}()
	return serverConn, clientConn, done
}

func closeHandshakeTranscriptSession(t *testing.T, clientConn net.Conn, done <-chan struct{}) {
	t.Helper()
	if clientConn != nil {
		_ = clientConn.Close()
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleClient did not exit after test connection close")
	}
}

func readUntilContains(t *testing.T, conn net.Conn, want string, timeout time.Duration) string {
	t.Helper()
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	var out strings.Builder
	buf := make([]byte, 256)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		n, err := conn.Read(buf)
		if n > 0 {
			out.Write(buf[:n])
			if strings.Contains(out.String(), want) {
				return out.String()
			}
		}
		if err == nil {
			continue
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			continue
		}
		if errors.Is(err, io.EOF) {
			break
		}
		t.Fatalf("read transcript: %v", err)
	}
	t.Fatalf("timed out waiting for %q in transcript %q", want, out.String())
	return ""
}
