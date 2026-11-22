package download

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCredentialURLKeepsByteExactValidators(t *testing.T) {
	const firstToken, secondToken = "test-first-credential", "test-second-credential"
	const tag = `"snapshot-v1"`
	type requestHeaders struct{ token, etag string }
	received := make(chan requestHeaders, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		etag := r.Header.Get("If-None-Match")
		received <- requestHeaders{r.URL.Query().Get("token"), etag}
		w.Header().Set("ETag", tag)
		if etag == tag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = w.Write([]byte("snapshot"))
	}))
	defer server.Close()

	req := Request{Destination: filepath.Join(t.TempDir(), "snapshot.gz"), RequireByteExact304: true}
	check := func(token, ifNoneMatch string, status Status) Metadata {
		t.Helper()
		req.URL = server.URL + "/snapshot?token=" + token
		req.URLSecret = token
		result, err := Download(t.Context(), req)
		if err != nil || result.Status != status {
			t.Fatalf("download status = %q, error = %v, want %q", result.Status, err, status)
		}
		select {
		case got := <-received:
			if got != (requestHeaders{token, ifNoneMatch}) {
				t.Fatal("request credential or conditional validator mismatch")
			}
		default:
			t.Fatal("download did not reach the server")
		}
		checkCredentialSidecar(t, req.Destination, firstToken, secondToken)
		if !strings.HasPrefix(result.Meta.URL, "sha256:") || result.Meta.ByteExactETag != tag {
			t.Fatal("protected request identity or strong validator is missing")
		}
		return result.Meta
	}

	initial := check(firstToken, "", StatusUpdated)
	check(firstToken, tag, StatusNotModified)
	initial.URL = req.URL
	if err := WriteMetadata(MetadataPath(req.Destination), initial); err != nil {
		t.Fatal(err)
	}
	check(firstToken, "", StatusUpdated)
	check(secondToken, "", StatusUpdated)
	check(secondToken, tag, StatusNotModified)
}

func TestCredentialURLRedirectOmitsRefererAndSecretTag(t *testing.T) {
	const token = "test-redirect-credential"
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Referer") != "" || r.Header.Get("If-None-Match") != "" {
			t.Error("protected redirect forwarded an original request header")
		}
		w.Header().Set("ETag", `"`+token+`"`)
		_, _ = w.Write([]byte("snapshot"))
	}))
	defer target.Close()
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/file?signature=fixture", http.StatusFound)
	}))
	defer entry.Close()

	req := Request{URL: entry.URL + "/archive?token=" + token, URLSecret: token,
		Destination: filepath.Join(t.TempDir(), "snapshot.gz"), RequireByteExact304: true}
	result, err := Download(t.Context(), req)
	if err != nil || result.Status != StatusUpdated || result.Meta.ETag != "" {
		t.Fatalf("protected redirect status = %q, error = %v, or secret ETag survived", result.Status, err)
	}
	checkCredentialSidecar(t, req.Destination, token)
}

func TestCredentialURLErrorsOmitSecret(t *testing.T) {
	const token = "test-error-credential"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, buffer, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_, _ = fmt.Fprintf(buffer, "HTTP/1.1 403 Rejected %s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", token)
		_ = buffer.Flush()
	}))
	defer server.Close()
	req := Request{URL: server.URL + "/archive?token=" + token, URLSecret: token,
		Destination: filepath.Join(t.TempDir(), "snapshot.gz")}
	if _, err := Download(t.Context(), req); err == nil || strings.Contains(err.Error(), token) || !strings.Contains(err.Error(), "403") {
		t.Fatal("credential-bearing HTTP status was not safely reported")
	}
	req.URL = "invalid://example.invalid/archive?token=" + token
	if _, err := Download(t.Context(), req); err == nil || strings.Contains(err.Error(), token) || !strings.Contains(err.Error(), "unsupported protocol") {
		t.Fatal("credential-bearing transport error was not safely reported")
	}
	if _, err := os.Stat(MetadataPath(req.Destination)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed request created metadata")
	}
}

func checkCredentialSidecar(t *testing.T, destination string, secrets ...string) {
	t.Helper()
	data, err := os.ReadFile(MetadataPath(destination))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range secrets {
		if strings.Contains(string(data), secret) {
			t.Fatal("status file contains a credential")
		}
	}
}
