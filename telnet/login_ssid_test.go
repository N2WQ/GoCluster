package telnet

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/spot"
)

func TestLoginSSIDGrammar(t *testing.T) {
	for _, tc := range []struct {
		call string
		want bool
	}{
		{"N0CALL", true}, {"N0CALL-0", true}, {"N0CALL-9", true},
		{"N0CALL-10", true}, {"N0CALL-99", true}, {"N0CALL-00", true},
		{"N0CALL-01", true}, {"VE3/N0CALL-1", true}, {"N0CALL/P-1", true},
		{"N0CALL-100", false}, {"N0CALL-001", false}, {"N0CALL-A", false},
		{"N0CALL-1A", false}, {"N0CALL-", false}, {"N0CALL--1", false},
		{"N0CALL-1-2", false}, {"N0CALL-1/VE", false}, {"N0CALL-١", false},
		{"N0CALL-#", false}, {"FT8", false},
	} {
		t.Run(tc.call, func(t *testing.T) {
			if got := isValidLoginCallsign(tc.call); got != tc.want {
				t.Fatalf("isValidLoginCallsign(%q) = %t, want %t", tc.call, got, tc.want)
			}
		})
	}
}

func TestInvalidSSIDStopsBeforeReferenceLookups(t *testing.T) {
	lookups := 0
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.CTYLookup = func() *cty.CTYDatabase { lookups++; return nil }
		opts.USLicenseCheck = func(string) bool { t.Error("unexpected US license lookup"); return true }
		opts.CanadianLicenseCheck = func(string) bool { t.Error("unexpected Canadian license lookup"); return true }
	})
	for _, call := range []string{"N0CALL-100", "N0CALL-A", "N0CALL-1-2", "N0CALL-1/VE"} {
		got := s.validateLoginCallsign(call)
		if got.valid || got.failOpen || got.reason != loginValidationReasonSyntaxInvalid {
			t.Fatalf("invalid SSID %q: %+v", call, got)
		}
	}
	if lookups != 0 {
		t.Fatalf("invalid SSIDs invoked CTY lookup %d times", lookups)
	}
	if got := s.validateLoginCallsign("N0CALL-01"); !got.valid || !got.failOpen || lookups != 1 {
		t.Fatalf("valid SSID did not reach ordinary fail-open path: %+v, lookups=%d", got, lookups)
	}
}

func TestSSIDLoginRetryAndProfileIdentity(t *testing.T) {
	s := newHandshakeTranscriptServer(t)
	for _, tc := range []struct{ call, grid string }{
		{"N0CALL-100", "FN42"}, {"N0CALL-01", "FN31"}, {"N0CALL-1", "EM12"},
	} {
		if err := filter.SaveUserRecord(tc.call, &filter.UserRecord{Filter: *filter.NewFilter(), Grid: tc.grid}); err != nil {
			t.Fatalf("save %s: %v", tc.call, err)
		}
	}
	legacyPath := filepath.Join(filter.UserDataDir, "N0CALL-100.yaml")
	before, err := os.ReadFile(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	filesBefore, err := os.ReadDir(filter.UserDataDir)
	if err != nil {
		t.Fatal(err)
	}
	serverConn, clientConn, done := startHandshakeTranscriptSession(t, s)
	defer serverConn.Close()
	defer closeHandshakeTranscriptSession(t, clientConn, done)
	readUntilContains(t, clientConn, "login: ", 2*time.Second)
	if _, err := io.WriteString(clientConn, "N0CALL-100\r\n"); err != nil {
		t.Fatal(err)
	}
	reply := readUntilContains(t, clientConn, "login: ", 2*time.Second)
	if !strings.Contains(reply, "Invalid call. Please try again.") {
		t.Fatalf("missing invalid response: %q", reply)
	}
	if calls := s.ListClientCallsigns(); len(calls) != 0 {
		t.Fatalf("invalid login registered %v", calls)
	}
	after, err := os.ReadFile(legacyPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("rejected profile changed: %v", err)
	}
	filesAfter, err := os.ReadDir(filter.UserDataDir)
	if err != nil || len(filesAfter) != len(filesBefore) {
		t.Fatalf("rejected login changed profile entries: %v", err)
	}
	for i := range filesBefore {
		if filesBefore[i].Name() != filesAfter[i].Name() {
			t.Fatalf("rejected login changed profile entries: %q != %q", filesBefore[i].Name(), filesAfter[i].Name())
		}
	}
	if _, err := io.WriteString(clientConn, "n0call-01/P\r\n"); err != nil {
		t.Fatal(err)
	}
	reply = readUntilContains(t, clientConn, "UTC>", 2*time.Second)
	if !strings.Contains(reply, "Hello N0CALL-01,") || !strings.Contains(reply, "Grid: FN31") {
		t.Fatalf("wrong normalized identity or profile: %q", reply)
	}
	closeHandshakeTranscriptSession(t, clientConn, done)
	serverConn2, clientConn2, done2 := startHandshakeTranscriptSession(t, s)
	defer serverConn2.Close()
	defer closeHandshakeTranscriptSession(t, clientConn2, done2)
	readUntilContains(t, clientConn2, "login: ", 2*time.Second)
	if _, err := io.WriteString(clientConn2, "N0CALL-1\r\n"); err != nil {
		t.Fatal(err)
	}
	reply = readUntilContains(t, clientConn2, "UTC>", 2*time.Second)
	if !strings.Contains(reply, "Hello N0CALL-1,") || !strings.Contains(reply, "Grid: EM12") {
		t.Fatalf("wrong unpadded identity or profile: %q", reply)
	}
}

func FuzzLoginSSID(f *testing.F) {
	for _, seed := range []string{"0", "01", "99", "100", "001", "A", "1A", "1-2", "1/VE", "1/P", "", "١"} {
		f.Add(seed)
	}
	oracle := regexp.MustCompile(`^N0CALL(?:-[0-9]{1,2})?$`)
	f.Fuzz(func(t *testing.T, suffix string) {
		if len(suffix) > 32 {
			t.Skip()
		}
		call := spot.NormalizeCallsign("N0CALL-" + suffix)
		want := oracle.MatchString(call)
		if got := isValidLoginCallsign(call); got != want {
			t.Fatalf("normalized %q: got %t, want %t", call, got, want)
		}
	})
}
