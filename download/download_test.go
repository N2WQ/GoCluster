package download

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDownloadUpdated(t *testing.T) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello"))
	}))
	t.Cleanup(server.Close)

	dir := t.TempDir()
	dest := filepath.Join(dir, "cty.plist")

	res, err := Download(testContext(t), Request{
		URL:         server.URL,
		Destination: dest,
		Timeout:     5 * time.Second,
	})
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if res.Status != StatusUpdated {
		t.Fatalf("expected updated status, got %s", res.Status)
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("unexpected content: %q", string(data))
	}
	meta, _ := ReadMetadata(MetadataPath(dest))
	if meta == nil || meta.ETag != `"v1"` || meta.SHA256 == "" {
		t.Fatalf("metadata missing expected fields: %+v", meta)
	}
}

func TestDownloadNotModified(t *testing.T) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("same"))
	}))
	t.Cleanup(server.Close)

	dir := t.TempDir()
	dest := filepath.Join(dir, "scp.txt")

	first, err := Download(testContext(t), Request{
		URL:         server.URL,
		Destination: dest,
		Timeout:     5 * time.Second,
	})
	if err != nil {
		t.Fatalf("initial download: %v", err)
	}
	if first.Status != StatusUpdated {
		t.Fatalf("expected updated status, got %s", first.Status)
	}
	metaBefore, _ := ReadMetadata(MetadataPath(dest))
	if metaBefore == nil {
		t.Fatalf("missing metadata after first download")
	}

	time.Sleep(10 * time.Millisecond)
	second, err := Download(testContext(t), Request{
		URL:         server.URL,
		Destination: dest,
		Timeout:     5 * time.Second,
	})
	if err != nil {
		t.Fatalf("second download: %v", err)
	}
	if second.Status != StatusNotModified {
		t.Fatalf("expected not modified status, got %s", second.Status)
	}
	metaAfter, _ := ReadMetadata(MetadataPath(dest))
	if metaAfter == nil || metaAfter.DownloadedAt != metaBefore.DownloadedAt {
		t.Fatalf("expected DownloadedAt to remain unchanged")
	}
}

func TestDownloadSameContent(t *testing.T) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"v2"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("repeat"))
	}))
	t.Cleanup(server.Close)

	dir := t.TempDir()
	dest := filepath.Join(dir, "ipinfo.gz")

	first, err := Download(testContext(t), Request{
		URL:         server.URL,
		Destination: dest,
		Timeout:     5 * time.Second,
	})
	if err != nil {
		t.Fatalf("initial download: %v", err)
	}
	if first.Status != StatusUpdated {
		t.Fatalf("expected updated status, got %s", first.Status)
	}
	metaBefore, _ := ReadMetadata(MetadataPath(dest))
	if metaBefore == nil {
		t.Fatalf("missing metadata after first download")
	}

	time.Sleep(10 * time.Millisecond)
	second, err := Download(testContext(t), Request{
		URL:         server.URL,
		Destination: dest,
		Timeout:     5 * time.Second,
	})
	if err != nil {
		t.Fatalf("second download: %v", err)
	}
	if second.Status != StatusSameContent {
		t.Fatalf("expected same content status, got %s", second.Status)
	}
	metaAfter, _ := ReadMetadata(MetadataPath(dest))
	if metaAfter == nil || metaAfter.DownloadedAt != metaBefore.DownloadedAt {
		t.Fatalf("expected DownloadedAt to remain unchanged")
	}
}

func TestDownloadAllowMissingDestination(t *testing.T) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		t.Fatalf("expected conditional request")
	}))
	t.Cleanup(server.Close)

	dir := t.TempDir()
	dest := filepath.Join(dir, "l_amat.zip")
	metaPath := MetadataPath(dest)

	meta := Metadata{
		URL:  server.URL,
		ETag: `"v1"`,
	}
	if err := WriteMetadata(metaPath, meta); err != nil {
		t.Fatalf("write metadata: %v", err)
	}

	res, err := Download(testContext(t), Request{
		URL:                     server.URL,
		Destination:             dest,
		Timeout:                 5 * time.Second,
		AllowMissingDestination: true,
	})
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if res.Status != StatusNotModified {
		t.Fatalf("expected not modified status, got %s", res.Status)
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("expected destination to remain missing")
	}
}

func TestDownloadMaxBytes(t *testing.T) {
	const body = "bounded archive"
	for _, declared := range []bool{false, true} {
		for _, limit := range []int64{0, int64(len(body)), int64(len(body) - 1), 1<<63 - 1} {
			t.Run(strconv.FormatBool(declared)+"/limit="+strconv.FormatInt(limit, 10), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("ETag", `"new"`)
					if declared {
						w.Header().Set("Content-Length", strconv.Itoa(len(body)))
					} else {
						w.WriteHeader(http.StatusOK)
						w.(http.Flusher).Flush() // Force a chunked body with no declared length.
					}
					_, _ = w.Write([]byte(body))
				}))
				t.Cleanup(server.Close)
				dest, beforeMeta := seedDownloadSnapshot(t)
				res, err := Download(testContext(t), Request{URL: server.URL, Destination: dest, MaxBytes: limit})
				if limit > 0 && limit < int64(len(body)) {
					if err == nil || !strings.Contains(err.Error(), "exceeds MaxBytes") {
						t.Fatalf("Download error = %v, want size limit error", err)
					}
					assertDownloadSnapshotRetained(t, dest, beforeMeta)
					return
				}
				if err != nil || res.Status != StatusUpdated || res.Bytes != int64(len(body)) {
					t.Fatalf("Download = %#v, %v, want complete updated body", res, err)
				}
				data, err := os.ReadFile(dest)
				if err != nil || string(data) != body {
					t.Fatalf("destination = %q, %v, want %q", data, err, body)
				}
				hash := sha256.Sum256([]byte(body))
				meta, _ := ReadMetadata(MetadataPath(dest))
				if meta == nil || meta.SHA256 != hex.EncodeToString(hash[:]) || meta.SizeBytes != int64(len(body)) {
					t.Fatalf("metadata = %#v, want body hash and length", meta)
				}
				res, err = Download(testContext(t), Request{URL: server.URL, Destination: dest, MaxBytes: limit})
				if err != nil || res.Status != StatusSameContent {
					t.Fatalf("repeat Download = %#v, %v, want same-content decision", res, err)
				}
			})
		}
	}
}

func TestDownloadRejectsNegativeMaxBytesBeforeFetch(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte("unused"))
	}))
	t.Cleanup(server.Close)
	dest, beforeMeta := seedDownloadSnapshot(t)
	if _, err := Download(testContext(t), Request{URL: server.URL, Destination: dest, MaxBytes: -1}); err == nil {
		t.Fatal("Download accepted negative MaxBytes")
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("HTTP requests = %d, want 0", got)
	}
	assertDownloadSnapshotRetained(t, dest, beforeMeta)
}

func TestDownloadMaxBytesCancellationRetainsSnapshot(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("partial"))
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)
	dest, beforeMeta := seedDownloadSnapshot(t)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := Download(ctx, Request{URL: server.URL, Destination: dest, MaxBytes: 100})
		finished <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("download did not start")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Download error = %v, want cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("download did not stop after cancellation")
	}
	assertDownloadSnapshotRetained(t, dest, beforeMeta)
}

func seedDownloadSnapshot(t *testing.T) (string, []byte) {
	t.Helper()
	dest := filepath.Join(t.TempDir(), "snapshot.zip")
	if err := os.WriteFile(dest, []byte("previous archive"), 0o644); err != nil {
		t.Fatal(err)
	}
	meta := Metadata{ETag: `"old"`, SHA256: "previous checksum", SizeBytes: 16, ProcessedOK: true}
	if err := WriteMetadata(MetadataPath(dest), meta); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(MetadataPath(dest))
	if err != nil {
		t.Fatal(err)
	}
	return dest, data
}

func assertDownloadSnapshotRetained(t *testing.T, dest string, beforeMeta []byte) {
	t.Helper()
	data, err := os.ReadFile(dest)
	if err != nil || string(data) != "previous archive" {
		t.Fatalf("previous archive changed: %q, %v", data, err)
	}
	meta, err := os.ReadFile(MetadataPath(dest))
	if err != nil || string(meta) != string(beforeMeta) {
		t.Fatalf("metadata changed: %q, %v", meta, err)
	}
	temps, err := filepath.Glob(filepath.Join(filepath.Dir(dest), "download-*.tmp"))
	if err != nil || len(temps) != 0 {
		t.Fatalf("temporary download files = %v, %v, want none", temps, err)
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}
