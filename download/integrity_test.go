package download

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"dxcluster/internal/fsutil"
)

func TestDownloadRepairsDamagedRetainedDestination(t *testing.T) {
	for _, scenario := range []struct {
		name          string
		conditional   bool
		recoveryFails bool
		wantRequests  int32
	}{
		{name: "same body", wantRequests: 2},
		{name: "not modified", conditional: true, wantRequests: 3},
		{name: "unconditional 304", conditional: true, recoveryFails: true, wantRequests: 3},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				request := requests.Add(1)
				w.Header().Set("ETag", "\"version\"")
				w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
				if scenario.conditional && request == 2 {
					if r.Header.Get("If-None-Match") == "" {
						t.Error("expected conditional request")
					}
					w.WriteHeader(http.StatusNotModified)
					return
				}
				if request == 3 {
					if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
						t.Error("recovery request sent conditional headers")
					}
					if scenario.recoveryFails {
						w.WriteHeader(http.StatusNotModified)
						return
					}
				}
				_, _ = w.Write([]byte("good"))
			}))
			defer server.Close()
			request := Request{URL: server.URL, Destination: filepath.Join(t.TempDir(), "archive")}
			if first, err := Download(testContext(t), request); err != nil || first.Status != StatusUpdated {
				t.Fatalf("first download: %+v, %v", first, err)
			}
			metadataPath := MetadataPath(request.Destination)
			before, err := os.ReadFile(metadataPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(request.Destination, []byte("evil"), 0o600); err != nil {
				t.Fatal(err)
			}

			result, err := Download(testContext(t), request)
			if scenario.recoveryFails {
				if err == nil || result.Status != "" {
					t.Fatalf("failed recovery reported success: %+v, %v", result, err)
				}
				requireBytes(t, request.Destination, []byte("evil"))
				requireBytes(t, metadataPath, before)
			} else {
				if err != nil || result.Status != StatusUpdated {
					t.Fatalf("repair: %+v, %v", result, err)
				}
				requireBytes(t, request.Destination, []byte("good"))
			}
			if got := requests.Load(); got != scenario.wantRequests {
				t.Fatalf("requests=%d, want %d", got, scenario.wantRequests)
			}
		})
	}
}

func TestDownloadForcePublishesIdenticalBytes(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
			t.Error("forced publication used validators")
		}
		w.Header().Set("ETag", "version")
		_, _ = w.Write([]byte("same"))
	}))
	defer server.Close()

	request := Request{URL: server.URL, Destination: filepath.Join(t.TempDir(), "archive"), Force: true}
	for range 2 {
		result, err := Download(testContext(t), request)
		if err != nil || result.Status != StatusUpdated {
			t.Fatalf("forced download: %+v, %v", result, err)
		}
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests=%d, want 2", got)
	}
	requireBytes(t, request.Destination, []byte("same"))
}

func TestDownloadReplacementFailurePreservesPublication(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("candidate"))
	}))
	defer server.Close()
	request := Request{URL: server.URL, Destination: filepath.Join(t.TempDir(), "archive")}
	if err := os.WriteFile(request.Destination, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	metaPath := MetadataPath(request.Destination)
	if err := WriteMetadata(metaPath, Metadata{URL: server.URL, SHA256: "previous"}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	refusal := errors.New("replacement rejected")
	calls := 0
	result, err := download(testContext(t), request, func(temp, destination string) error {
		calls++
		requireBytes(t, destination, []byte("old"))
		requireBytes(t, temp, []byte("candidate"))
		return refusal
	})
	if !errors.Is(err, refusal) || result.Status != "" || calls != 1 {
		t.Fatalf("replacement failure: %+v, calls=%d, err=%v", result, calls, err)
	}
	requireBytes(t, request.Destination, []byte("old"))
	requireBytes(t, metaPath, before)
}

func requireBytes(t *testing.T, path string, expected []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, expected) {
		t.Fatalf("%s: bytes=%q, err=%v, want %q", path, got, err, expected)
	}
}

func TestDownloadValidatorFollowsIssuingResource(t *testing.T) {
	var target atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/entry" && target.Load() != 2 {
			path := "/a"
			if target.Load() == 1 {
				path = "/b"
			}
			http.Redirect(w, r, path, http.StatusFound)
			return
		}
		w.Header().Set("ETag", `"same-tag"`)
		if r.Header.Get("If-None-Match") == `"same-tag"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = w.Write([]byte(r.URL.Path))
	}))
	defer server.Close()
	req := Request{URL: server.URL + "/entry", Destination: filepath.Join(t.TempDir(), "archive")}
	for i, tc := range []struct {
		target int32
		status Status
		body   string
	}{
		{0, StatusUpdated, "/a"},
		{0, StatusNotModified, "/a"},
		{1, StatusUpdated, "/b"},
		{1, StatusNotModified, "/b"},
		{2, StatusUpdated, "/entry"},
		{0, StatusUpdated, "/a"},
	} {
		target.Store(tc.target)
		got, err := Download(testContext(t), req)
		if err != nil || got.Status != tc.status {
			t.Fatalf("request %d: status=%s error=%v, want %s", i, got.Status, err, tc.status)
		}
		requireBytes(t, req.Destination, []byte(tc.body))
	}
}

func TestDownloadRefusesNonCompleteSuccess(t *testing.T) {
	for _, status := range []int{http.StatusAccepted, http.StatusNonAuthoritativeInfo, http.StatusNoContent, http.StatusIMUsed} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			dest, prior := seedDownloadSnapshot(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				if status != http.StatusNoContent {
					_, _ = w.Write([]byte("unrecognized representation"))
				}
			}))
			defer server.Close()
			got, err := Download(testContext(t), Request{URL: server.URL, Destination: dest})
			if err == nil || got.Status != "" {
				t.Fatalf("HTTP %d reported success: %+v error=%v", status, got, err)
			}
			assertDownloadSnapshotRetained(t, dest, prior)
		})
	}
	t.Run("200 with Content-Range", func(t *testing.T) {
		dest, prior := seedDownloadSnapshot(t)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Range", "bytes 0-2/8")
			_, _ = w.Write([]byte("abc"))
		}))
		defer server.Close()
		if result, err := Download(testContext(t), Request{URL: server.URL, Destination: dest}); err == nil || result.Status != "" {
			t.Fatalf("ranged 200 reported success: %+v, %v", result, err)
		}
		assertDownloadSnapshotRetained(t, dest, prior)
	})
}

func TestDownloadTruncatedOKKeepsPublishedFiles(t *testing.T) {
	dest, prior := seedDownloadSnapshot(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "32")
		_, _ = w.Write([]byte("short"))
	}))
	defer server.Close()
	got, err := Download(testContext(t), Request{URL: server.URL, Destination: dest})
	if !errors.Is(err, io.ErrUnexpectedEOF) || got.Status != "" {
		t.Fatalf("truncated success: %+v, %v", got, err)
	}
	assertDownloadSnapshotRetained(t, dest, prior)
}

func TestDownloadLegacyValidatorMustBeSeededAgain(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		tag := r.Header.Get("If-None-Match")
		if n == 1 && (tag != "" || r.Header.Get("If-Modified-Since") != "") {
			t.Error("legacy sidecar sent a validator with unknown resource identity")
		}
		w.Header().Set("ETag", `"v1"`)
		if tag == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = w.Write([]byte("body"))
	}))
	defer server.Close()
	dest := filepath.Join(t.TempDir(), "archive")
	if err := os.WriteFile(dest, []byte("body"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := WriteMetadata(MetadataPath(dest), Metadata{
		URL: server.URL, SHA256: gotSHA256("body"), SizeBytes: 4, ETag: `"v1"`,
		LastModified: "Mon, 02 Jan 2006 15:04:05 GMT",
	})
	if err != nil {
		t.Fatal(err)
	}
	req := Request{URL: server.URL, Destination: dest}
	for _, status := range []Status{StatusSameContent, StatusNotModified} {
		got, err := Download(testContext(t), req)
		if got.Status != status || err != nil {
			t.Fatalf("got %s, %v, want %s", got.Status, err, status)
		}
	}
	if requests.Load() != 2 {
		t.Fatalf("expected one full download and one conditional check")
	}
}

func TestDownloadFailedSidecarReplacementKeepsPriorRecord(t *testing.T) {
	var fresh atomic.Bool
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("If-None-Match") == `"old"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if fresh.Load() {
			w.Header().Set("ETag", `"new"`)
			_, _ = w.Write([]byte("verified new bytes"))
			return
		}
		w.Header().Set("ETag", `"old"`)
		_, _ = w.Write([]byte("previous bytes"))
	}))
	defer server.Close()
	dest := filepath.Join(t.TempDir(), "archive")
	req := Request{URL: server.URL, Destination: dest}
	metaPath := MetadataPath(dest)
	if _, err := Download(testContext(t), req); err != nil {
		t.Fatal(err)
	}
	prior, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	fresh.Store(true)
	req.Force = true
	var metadataTemp string
	got, err := download(testContext(t), req, func(temp, destination string) error {
		if destination == metaPath {
			metadataTemp = temp
			requireBytes(t, metaPath, prior)
			if staged, _ := ReadMetadata(temp); staged == nil || staged.SHA256 != gotSHA256("verified new bytes") {
				t.Fatalf("incomplete sidecar at replacement: %+v", staged)
			}
			return errors.New("metadata replacement refused")
		}
		return fsutil.ReplaceFile(temp, destination)
	})
	if err != nil || got.Status != StatusUpdated || metadataTemp == "" {
		t.Fatalf("published data=%s error=%v metadataTemp=%q", got.Status, err, metadataTemp)
	}
	requireBytes(t, metaPath, prior)
	requireBytes(t, dest, []byte("verified new bytes"))
	if _, err := os.Stat(metadataTemp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("metadata temporary file remains: %v", err)
	}
	// The old record cannot verify the new bytes, and the next complete response
	// replaces it with metadata for the actual retained file.
	req.Force = false
	retry, err := Download(testContext(t), req)
	if err != nil || retry.Status != StatusUpdated || requests.Load() != 4 {
		t.Fatalf("retry status=%s error=%v total requests=%d", retry.Status, err, requests.Load())
	}
	if saved, _ := ReadMetadata(metaPath); saved == nil || saved.SHA256 != retry.Meta.SHA256 || saved.SHA256 != gotSHA256("verified new bytes") {
		t.Fatalf("retry did not persist metadata for the new bytes: %+v", saved)
	}
}

func gotSHA256(body string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
}

func TestDownloadValidatorsRejectWildcardAndLists(t *testing.T) {
	for _, tags := range [][]string{{"*"}, {`"old", "new"`}, {`"old"`, `"new"`}, {"unquoted"}} {
		t.Run(fmt.Sprint(tags), func(t *testing.T) {
			var updated atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !updated.Load() {
					for _, tag := range tags {
						w.Header().Add("ETag", tag)
					}
					_, _ = w.Write([]byte("before"))
					return
				}
				if tag := r.Header.Get("If-None-Match"); tag != "" {
					t.Errorf("sent ambiguous If-None-Match %q", tag)
					w.WriteHeader(http.StatusNotModified)
					return
				}
				_, _ = w.Write([]byte("after"))
			}))
			defer server.Close()
			req := Request{URL: server.URL, Destination: filepath.Join(t.TempDir(), "archive")}
			seed, err := Download(testContext(t), req)
			if err != nil || seed.Status != StatusUpdated || seed.Meta.ETag != "" {
				t.Fatalf("seed recorded an invalid validator: %+v, %v", seed, err)
			}
			// External or older sidecar metadata must also be validated on use.
			seed.Meta.ETag = tags[0]
			if len(tags) > 1 {
				seed.Meta.ETag = tags[0] + "," + tags[1]
			}
			if err := WriteMetadata(MetadataPath(req.Destination), seed.Meta); err != nil {
				t.Fatal(err)
			}
			updated.Store(true)
			result, err := Download(testContext(t), req)
			if err != nil || result.Status != StatusUpdated {
				t.Fatalf("changed resource was not retrieved: %+v, %v", result, err)
			}
			requireBytes(t, req.Destination, []byte("after"))
		})
	}
}

func TestDownloadOrdinaryConditionalValidators(t *testing.T) {
	for _, validator := range []struct{ response, request, value string }{
		{"ETag", "If-None-Match", `W/"semantic"`},
		{"Last-Modified", "If-Modified-Since", "Mon, 02 Jan 2006 15:04:05 GMT"},
	} {
		t.Run(validator.response, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(validator.response, validator.value)
				if r.Header.Get(validator.request) == validator.value {
					w.WriteHeader(http.StatusNotModified)
					return
				}
				_, _ = w.Write([]byte("body"))
			}))
			defer server.Close()
			req := Request{URL: server.URL, Destination: filepath.Join(t.TempDir(), "archive")}
			for _, want := range []Status{StatusUpdated, StatusNotModified} {
				result, err := Download(testContext(t), req)
				if err != nil || result.Status != want {
					t.Fatalf("got %s, %v, want %s", result.Status, err, want)
				}
			}
		})
	}
}
