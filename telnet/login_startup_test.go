package telnet

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"dxcluster/bulletinhistory"
	"dxcluster/filter"
	"dxcluster/pathreliability"
)

// Read beyond the prompt: a test that stops at the prompt misses later startup
// messages and cannot detect the ordering regression.
func readStartupTranscript(t *testing.T, conn net.Conn, marker string) string {
	t.Helper()
	got := readUntilContains(t, conn, marker, 5*time.Second)
	buf := make([]byte, 4096)
	for len(got) < 100000 {
		if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		n, err := conn.Read(buf)
		got += string(buf[:n])
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return got
			}
			t.Fatalf("read startup transcript: %v", err)
		}
	}
	t.Fatal("startup transcript exceeds bound")
	return ""
}

func assertSafeArchivedWire(t *testing.T, text string) {
	t.Helper()
	if !utf8.ValidString(text) {
		t.Fatalf("invalid UTF-8 in archived output: %q", text)
	}
	for i, r := range text {
		if r == '\r' && i+1 < len(text) && text[i+1] == '\n' {
			continue
		}
		if r == '\n' && i > 0 && text[i-1] == '\r' {
			continue
		}
		if unicode.IsControl(r) {
			t.Fatalf("unsafe control U+%04X in archived output: %q", r, text)
		}
	}
}

func openStartupSession(t *testing.T, s *Server, marker string) (net.Conn, string) {
	t.Helper()
	serverConn, conn, done := startHandshakeTranscriptSession(t, s)
	t.Cleanup(func() { closeHandshakeTranscriptSession(t, conn, done); _ = serverConn.Close() })
	readUntilContains(t, conn, "login: ", 5*time.Second)
	if _, err := io.WriteString(conn, "K1BUL\r\n"); err != nil {
		t.Fatal(err)
	}
	return conn, readStartupTranscript(t, conn, marker)
}

func TestShippedLoginStartupBeforePrompt(t *testing.T) {
	greeting := shippedBooleanGreeting(t)
	if strings.Count(greeting, "<STARTUP_INFO>") != 1 {
		t.Fatal("shipped greeting must place the startup marker exactly once")
	}
	for _, transport := range []string{"native", "ziutek"} {
		for _, tc := range []struct {
			name    string
			records int
		}{{"nil-store", -1}, {"empty", 0}, {"wwv-only", 1}, {"both", 2}} {
			t.Run(transport+"/"+tc.name, func(t *testing.T) {
				records := tc.records
				var store *bulletinhistory.Store
				if records >= 0 {
					store = bulletinhistory.Open(filepath.Join(t.TempDir(), "history.json"), 0, 0)
					t.Cleanup(func() { _ = store.Close() })
					if records >= 1 {
						store.Record("WWV", "WWV test data", time.Now())
					}
					if records >= 2 {
						store.Record("WCY", "WCY test data", time.Now())
					}
				}
				s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
					opts.Transport, opts.EchoMode, opts.LoginGreeting = transport, "off", greeting
					opts.BulletinHistory = store
					opts.DialectWelcomeMessage = "Dialect notice"
					opts.PathPredictor = &pathreliability.Predictor{}
					opts.PathDisplayEnabled = true
					opts.PathStatusMessage = "Path notice"
				})
				_, got := openStartupSession(t, s, "UTC>")
				welcome, prompt := strings.Index(got, "Hello K1BUL"), strings.LastIndex(got, "K1BUL de N2WQ-2")
				if welcome < 0 || prompt <= welcome || !strings.HasSuffix(strings.TrimSpace(got), "UTC>") || strings.Count(got, "UTC>") != 1 {
					t.Fatalf("incorrect welcome/prompt transcript: %q", got)
				}
				for _, notice := range []string{"Dialect notice", "Path notice"} {
					if pos := strings.Index(got, notice); pos <= welcome || pos >= prompt {
						t.Fatalf("%s not between welcome and prompt: %q", notice, got)
					}
				}
				if records >= 0 {
					for _, notice := range []string{"WWV history may be incomplete.", "WCY history may be incomplete."} {
						if pos := strings.Index(got, notice); pos <= welcome || pos >= prompt {
							t.Fatalf("%s not between welcome and prompt: %q", notice, got)
						}
					}
				}
				if strings.Contains(got, "<STARTUP_INFO>") || (strings.Contains(got, "WWV test data") != (records >= 1)) || (strings.Contains(got, "WCY test data") != (records >= 2)) {
					t.Fatalf("wrong bulletin display: %q", got)
				}
			})
		}
	}
}

func TestCustomLoginStartupInsertion(t *testing.T) {
	for _, tc := range []struct {
		name, greeting, notice, marker, want string
	}{
		{"unmarked", "Custom <CALL>\nEnd", "Dialect notice", "Dialect notice", "Dialect notice\r\nCustom K1BUL\r\nEnd"},
		{"empty", "", "Dialect notice", "Dialect notice", "Dialect notice\r\n"},
		{"embedded-and-repeated", "Welcome<STARTUP_INFO>End<STARTUP_INFO>", "Dialect notice", "Dialect notice", "Welcome\r\nDialect notice\r\nEnd"},
		{"no-notices", "A<STARTUP_INFO>B<STARTUP_INFO>", "", "AB", "AB"},
	} {
		for _, transport := range []string{"native", "ziutek"} {
			t.Run(tc.name+"/"+transport, func(t *testing.T) {
				s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
					opts.Transport, opts.EchoMode, opts.LoginGreeting = transport, "off", tc.greeting
					opts.DialectWelcomeMessage = tc.notice
				})
				_, got := openStartupSession(t, s, tc.marker)
				if got != tc.want {
					t.Fatalf("custom greeting changed: got %q want %q", got, tc.want)
				}
			})
		}
	}
}

func TestLoginBulletinTemplateTextIsLiteral(t *testing.T) {
	store := bulletinhistory.Open(filepath.Join(t.TempDir(), "history.json"), 0, 0)
	t.Cleanup(func() { _ = store.Close() })
	store.Record("WWV", "literal <CALL> <SETTINGS_FILTERS> <STARTUP_INFO>", time.Now())
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.EchoMode = "off"
		opts.LoginGreeting = "Welcome\n<STARTUP_INFO>Prompt\n"
		opts.BulletinHistory = store
	})
	_, got := openStartupSession(t, s, "Prompt")
	if !strings.Contains(got, "literal <CALL> <SETTINGS_FILTERS> <STARTUP_INFO>") || !strings.HasSuffix(got, "Prompt\r\n") {
		t.Fatalf("bulletin template-like text changed: %q", got)
	}
}

func TestNearbyRuleStartupHasNoGlobalWarning(t *testing.T) {
	for _, greeting := range []string{shippedBooleanGreeting(t), "Welcome\n<STARTUP_INFO>Prompt\n"} {
		s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
			opts.EchoMode, opts.LoginGreeting = "off", greeting
		})
		f := passRuleTestFilter(t, "NEARBY")
		if err := filter.SaveUserRecord("K1BUL", &filter.UserRecord{Filter: *f, Dialect: "go"}); err != nil {
			t.Fatal(err)
		}
		marker := "UTC>"
		if strings.Contains(greeting, "Prompt") {
			marker = "Prompt"
		}
		_, got := openStartupSession(t, s, marker)
		warning, prompt := strings.Index(got, "NEARBY is ON"), strings.LastIndex(got, marker)
		if warning >= 0 || prompt < 0 {
			t.Fatalf("retired global warning or missing prompt: %q", got)
		}
	}
}

func TestArchivedControlsAreSafeOnLoginAndShowWire(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	now := time.Now().UTC().Truncate(time.Second)
	// Construct a valid prior-version snapshot independently of the writer.
	old := "old\x1b[2J\rline\nnext\x00\x7f\u009bC1"
	snapshot := map[string]any{
		"version": 1, "sequence": 2, "watermark": now,
		"wwv": []any{map[string]any{"time": now.Add(-time.Minute), "text": "WWV " + old, "sequence": 1}},
		"wcy": []any{map[string]any{"time": now.Add(-time.Minute), "text": "WCY " + old, "sequence": 2}},
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	store := bulletinhistory.Open(path, 0, 0)
	t.Cleanup(func() { _ = store.Close() })
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.EchoMode, opts.LoginGreeting, opts.BulletinHistory = "off", "Welcome\n<STARTUP_INFO>Prompt\n", store
	})
	oldConn, oldLogin := openStartupSession(t, s, "Prompt")
	assertSafeArchivedWire(t, oldLogin)
	if !strings.Contains(oldLogin, "WWV old") || !strings.Contains(oldLogin, "WCY old") {
		t.Fatalf("login omitted old snapshot entries: %q", oldLogin)
	}
	_ = oldConn.Close()
	for _, kind := range []string{"WWV", "WCY"} {
		store.Record(kind, kind+" new\x1b[31m\rcr\nlf\x00\x7f\u009b"+string([]byte{0xff})+"END", now)
	}
	conn, login := openStartupSession(t, s, "Prompt")
	assertSafeArchivedWire(t, login)
	if !strings.Contains(login, "WWV new") || !strings.Contains(login, "WCY new") {
		t.Fatalf("login omitted new entries: %q", login)
	}
	for _, kind := range []string{"WWV", "WCY"} {
		for _, suffix := range []string{"", " FULL"} {
			if _, err := io.WriteString(conn, "SHOW "+kind+suffix+"\r\n"); err != nil {
				t.Fatal(err)
			}
			got := readStartupTranscript(t, conn, kind+" history may be incomplete.")
			assertSafeArchivedWire(t, got)
			if !strings.Contains(got, kind+" old") || !strings.Contains(got, kind+" new") {
				t.Fatalf("SHOW %s%s omitted archived entries: %q", kind, suffix, got)
			}
		}
	}
	// The approved change affects archive playback, not the live path.
	s.BroadcastWWV("WWV", "WWV live\x1b[2J LIVEEND")
	live := readStartupTranscript(t, conn, "LIVEEND")
	if !strings.Contains(live, "\x1b[2J") {
		t.Fatalf("live bulletin was changed: %q", live)
	}
}
