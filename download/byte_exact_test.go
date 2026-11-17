package download

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestByteExactDownload304(t *testing.T) {
	const strong = "\"v1\""
	for _, tc := range []struct {
		name          string
		seedTag       string
		responseTags  []string
		legacy        bool
		seedRedirect  bool
		checkRedirect bool
		corrupt       bool
		failRecovery  bool
		wantInitial   string
		wantRecovery  int32
		wantStatus    Status
	}{
		{name: "strong match", seedTag: strong, responseTags: []string{strong}, wantInitial: strong, wantStatus: StatusNotModified},
		{name: "weak seed", seedTag: "W/" + strong, wantStatus: StatusUpdated},
		{name: "timestamp only", wantStatus: StatusUpdated},
		{name: "legacy sidecar", seedTag: strong, legacy: true, wantStatus: StatusUpdated},
		{name: "redirected seed", seedTag: strong, seedRedirect: true, wantStatus: StatusUpdated},
		{name: "weak 304", seedTag: strong, responseTags: []string{"W/" + strong}, wantInitial: strong, wantRecovery: 1, wantStatus: StatusUpdated},
		{name: "different 304 tag", seedTag: strong, responseTags: []string{"\"v2\""}, wantInitial: strong, wantRecovery: 1, wantStatus: StatusUpdated},
		{name: "missing 304 tag", seedTag: strong, wantInitial: strong, wantRecovery: 1, wantStatus: StatusUpdated},
		{name: "multiple 304 tags", seedTag: strong, responseTags: []string{strong, strong}, wantInitial: strong, wantRecovery: 1, wantStatus: StatusUpdated},
		{name: "redirected 304", seedTag: strong, responseTags: []string{strong}, checkRedirect: true, wantInitial: strong, wantRecovery: 1, wantStatus: StatusUpdated},
		{name: "corrupt before 304", seedTag: strong, responseTags: []string{strong}, corrupt: true, wantInitial: strong, wantRecovery: 1, wantStatus: StatusUpdated},
		{name: "unsuccessful recovery", seedTag: strong, responseTags: []string{"W/" + strong}, failRecovery: true, wantInitial: strong, wantRecovery: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var checking atomic.Bool
			var conditional, unconditional atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				check := checking.Load()
				if r.URL.Path == "/entry" && ((!check && tc.seedRedirect) || (check && tc.checkRedirect)) {
					if r.Header.Get("If-None-Match") != "" {
						conditional.Add(1)
					}
					http.Redirect(w, r, "/other", http.StatusFound)
					return
				}
				w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
				if !check {
					if tc.seedTag != "" {
						w.Header().Set("ETag", tc.seedTag)
					}
					_, _ = w.Write([]byte("known"))
					return
				}
				if r.Header.Get("If-Modified-Since") != "" {
					t.Error("strict check sent a date validator")
				}
				if sent := r.Header.Get("If-None-Match"); sent != "" {
					conditional.Add(1)
					if sent != tc.wantInitial {
						t.Errorf("sent validator %q, want %q", sent, tc.wantInitial)
					}
					for _, tag := range tc.responseTags {
						w.Header().Add("ETag", tag)
					}
					w.WriteHeader(http.StatusNotModified)
					return
				}
				unconditional.Add(1)
				if tc.failRecovery {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				w.Header().Set("ETag", "\"v2\"")
				_, _ = w.Write([]byte("newer"))
			}))
			defer server.Close()
			dest := filepath.Join(t.TempDir(), "archive")
			request := Request{URL: server.URL + "/entry", Destination: dest, RequireByteExact304: true}
			initial, err := Download(testContext(t), request)
			if err != nil || initial.Status != StatusUpdated {
				t.Fatalf("seed: status=%q err=%v", initial.Status, err)
			}
			metaPath := MetadataPath(dest)
			if tc.legacy {
				initial.Meta.ByteExactETag = ""
				if err := WriteMetadata(metaPath, initial.Meta); err != nil {
					t.Fatal(err)
				}
			}
			beforeMetadata, err := os.ReadFile(metaPath)
			if err != nil {
				t.Fatal(err)
			}
			if tc.corrupt {
				if err := os.WriteFile(dest, []byte("wrong"), 0o600); err != nil {
					t.Fatal(err)
				}
			}

			checking.Store(true)
			result, err := Download(testContext(t), request)
			if tc.failRecovery {
				if err == nil || result.Status != "" {
					t.Fatalf("failed recovery: status=%q err=%v", result.Status, err)
				}
				requireBytes(t, dest, []byte("known"))
				requireBytes(t, metaPath, beforeMetadata)
			} else {
				if err != nil || result.Status != tc.wantStatus {
					t.Fatalf("check: status=%q err=%v, want %q", result.Status, err, tc.wantStatus)
				}
				if result.Status == StatusNotModified {
					requireBytes(t, dest, []byte("known"))
				} else {
					requireBytes(t, dest, []byte("newer"))
				}
			}
			var wantConditional int32
			wantUnconditional := tc.wantRecovery
			if tc.wantInitial != "" {
				wantConditional = 1
			} else {
				wantUnconditional++
			}
			if conditional.Load() != wantConditional || unconditional.Load() != wantUnconditional {
				t.Fatalf("requests conditional=%d unconditional=%d, want %d and %d", conditional.Load(), unconditional.Load(), wantConditional, wantUnconditional)
			}
		})
	}
}

func TestPartialResponsePreservesDestinationAndSidecar(t *testing.T) {
	for _, recovering := range []bool{false, true} {
		t.Run(fmt.Sprintf("after 304=%t", recovering), func(t *testing.T) {
			var checking atomic.Bool
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", "\"old\"")
				if !checking.Load() {
					_, _ = w.Write([]byte("complete"))
					return
				}
				n := requests.Add(1)
				if recovering && n == 1 {
					if r.Header.Get("If-None-Match") != "\"old\"" {
						t.Error("first request lacked the retained validator")
					}
					w.WriteHeader(http.StatusNotModified)
					return
				}
				if recovering && r.Header.Get("If-None-Match") != "" {
					t.Error("recovery request sent a validator")
				}
				w.Header().Set("Content-Range", "bytes 0-2/8")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("par"))
			}))
			defer server.Close()
			dest := filepath.Join(t.TempDir(), "archive")
			request := Request{URL: server.URL, Destination: dest}
			if _, err := Download(testContext(t), request); err != nil {
				t.Fatal(err)
			}
			metadata := MetadataPath(dest)
			previous, err := os.ReadFile(metadata)
			if err != nil {
				t.Fatal(err)
			}
			retained, wantRequests := []byte("complete"), int32(1)
			if recovering {
				retained, wantRequests = []byte("damaged!"), 2
				if err := os.WriteFile(dest, retained, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			checking.Store(true)
			result, err := Download(testContext(t), request)
			if err == nil || !strings.Contains(err.Error(), "206") || result.Status != "" || requests.Load() != wantRequests {
				t.Fatalf("206 was not rejected at the expected step: status=%q requests=%d err=%v", result.Status, requests.Load(), err)
			}
			requireBytes(t, dest, retained)
			requireBytes(t, metadata, previous)
		})
	}
}

func FuzzStrongETag(f *testing.F) {
	for _, seed := range []string{"\"\"", "\"abc\"", "W/\"abc\"", "\"a b\"", "\"a\",\"b\"", "*", "\"\x80\"", "\"\x7f\"", "\"a\\b\""} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		var allowed strings.Builder
		for n := '!'; n <= 0xff; n++ {
			if n != '"' && n != 0x7f {
				allowed.WriteByte(byte(n))
			}
		}
		body, left := strings.CutPrefix(input, "\"")
		body, right := strings.CutSuffix(body, "\"")
		expected := left && right
		for i := range len(body) {
			expected = expected && strings.IndexByte(allowed.String(), body[i]) >= 0
		}
		if isStrongETag(input) != expected {
			t.Fatalf("incorrect classification for %q", input)
		}
		valid := fmt.Sprintf("\"%x\"", sha256.Sum256([]byte(input)))
		if !isStrongETag(valid) || isStrongETag("W/"+valid) {
			t.Fatalf("invalid classification for a strong tag and its weak counterpart")
		}
	})
}
