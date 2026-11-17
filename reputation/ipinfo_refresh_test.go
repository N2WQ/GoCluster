package reputation

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/download"

	"github.com/cockroachdb/pebble"
)

func TestIPInfoFreshnessAcrossStoreAndCache(t *testing.T) {
	for _, tc := range []struct {
		name    string
		index   bool
		address string
	}{
		{name: "ipv4 index", index: true, address: "203.0.113.42"},
		{name: "ipv4 pebble", address: "203.0.113.42"},
		{name: "ipv6 pebble", index: true, address: "2001:db8::42"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ipinfoTestConfig(t, "")
			cfg.IPInfoPebbleLoadIPv4 = tc.index
			cfg.SnapshotMaxAgeSeconds = 60
			csv := cfg.IPInfoSnapshotPath
			if err := os.WriteFile(csv, snapshotCSV("64500"), 0o600); err != nil {
				t.Fatal(err)
			}
			path, err := buildIPInfoPebble(t.Context(), csv, cfg.IPInfoPebblePath, false)
			if err != nil {
				t.Fatal(err)
			}
			if err := updateIPInfoPebbleCurrent(t.Context(), cfg.IPInfoPebblePath, path); err != nil {
				t.Fatal(err)
			}
			g := openTestGate(t, cfg)
			if err := g.LoadStore(); err != nil {
				t.Fatal(err)
			}
			boundary := g.ipinfoView.Load().currentAt.Add(time.Minute)
			addr := netip.MustParseAddr(tc.address)
			cymruCalls, apiCalls := 0, 0
			g.lookupCymruFunc = func(_ netip.Addr, _ time.Time) (LookupResult, bool, bool) {
				cymruCalls++
				return LookupResult{}, true, true
			}
			g.lookupIPInfoFunc = func(_ netip.Addr, _ time.Time) LookupResult {
				apiCalls++
				return LookupResult{}
			}

			requireIPInfoASN(t, g, addr, boundary.Add(-time.Nanosecond), "64500")
			requireIPInfoASN(t, g, addr, boundary, "64500")
			if cymruCalls != 0 || apiCalls != 0 {
				t.Fatalf("current data started fallback: Cymru=%d API=%d", cymruCalls, apiCalls)
			}
			expired := boundary.Add(time.Nanosecond)
			requireIPInfoASN(t, g, addr, expired, "64500")
			if cymruCalls != 1 || apiCalls != 1 {
				t.Fatalf("warm expired local cache skipped fallback: Cymru=%d API=%d", cymruCalls, apiCalls)
			}
			// Beyond the cache TTL, an uncached local read must still try both
			// fallbacks before accepting the expired disk/index result.
			cold := expired.Add(48 * time.Hour)
			g.lookupIPInfoFunc = func(_ netip.Addr, now time.Time) LookupResult {
				apiCalls++
				return LookupResult{Source: ipinfoSource, ASN: "64501", FetchedAt: now}
			}
			requireIPInfoASN(t, g, addr, cold, "64501")
			if cymruCalls != 2 || apiCalls != 2 {
				t.Fatalf("cold expired lookup did not reach API: Cymru=%d API=%d", cymruCalls, apiCalls)
			}
			if result, ok := g.LookupIPForAdmission(addr.String(), cold); !ok || result.ASN != "64501" {
				t.Fatalf("admission cached metadata: %+v, %t", result, ok)
			}
			if cymruCalls != 2 || apiCalls != 2 {
				t.Fatal("admission started fallback")
			}
		})
	}
}

func TestIPInfoUnchangedVerificationAndFailedImportRecovery(t *testing.T) {
	type representation struct {
		tag  string
		body []byte
	}
	first := representation{tag: "\"v1\"", body: snapshotGzip(t, snapshotCSV("64500"))}
	second := representation{tag: "\"v2\"", body: snapshotGzip(t, snapshotCSV("64502"))}
	var offered atomic.Pointer[representation]
	offered.Store(&first)
	var notModified atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rep := offered.Load()
		w.Header().Set("ETag", rep.tag)
		if r.Header.Get("If-None-Match") == rep.tag {
			notModified.Add(1)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = w.Write(rep.body)
	}))
	defer server.Close()
	cfg := ipinfoTestConfig(t, server.URL)
	if err := os.WriteFile(cfg.IPInfoSnapshotPath, snapshotCSV("64500"), 0o600); err != nil {
		t.Fatal(err)
	}
	legacy, err := buildIPInfoPebble(t.Context(), cfg.IPInfoSnapshotPath, cfg.IPInfoPebblePath, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := updateIPInfoPebbleCurrent(t.Context(), cfg.IPInfoPebblePath, legacy); err != nil {
		t.Fatal(err)
	}
	g := openTestGate(t, cfg)
	if err := g.LoadStore(); err != nil {
		t.Fatal(err)
	}
	if err := g.downloadAndLoad(t.Context()); err != nil {
		t.Fatal(err)
	}
	initialPath := g.ipinfoView.Load().path
	if initialPath == legacy || g.ipinfoView.Load().hash == "" {
		t.Fatal("verified refresh did not establish provenance for the legacy snapshot")
	}
	g.Close()
	old := time.Now().UTC().Add(-72 * time.Hour).Truncate(time.Second)
	modifyIPInfoMetadata(t, initialPath, ipinfoPebbleMetaBuiltAt, strconv.FormatInt(old.Unix(), 10))
	root := cfg.IPInfoPebblePath
	receipt := filepath.Join(root, ipinfoVerificationFile)
	wrong := ipinfoVerification{Database: filepath.Base(initialPath), SHA256: fmt.Sprintf("%x", sha256.Sum256(second.body)), VerifiedAt: time.Now().UTC()}
	data, err := json.Marshal(wrong)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(receipt, data, 0o600); err != nil {
		t.Fatal(err)
	}

	g = openTestGate(t, cfg)
	if err := g.LoadStore(); err != nil {
		t.Fatal(err)
	}
	if source := g.ipinfoView.Load().currentAt; !source.Equal(old) {
		t.Fatalf("accepted a mismatched receipt: %v, want build time %v", source, old)
	}
	g.lookupCymruFunc = func(_ netip.Addr, now time.Time) (LookupResult, bool, bool) {
		return LookupResult{ASN: "64501", Source: cymruSource, FetchedAt: now}, true, false
	}
	addr := netip.MustParseAddr("2001:db8::42")
	requireIPInfoASN(t, g, addr, time.Now().UTC(), "64501")
	if err := g.downloadAndLoad(t.Context()); err != nil {
		t.Fatal(err)
	}
	updated := g.ipinfoView.Load()
	if updated.path != initialPath || !updated.currentAt.After(old) || notModified.Load() != 1 {
		t.Fatalf("304 did not verify the existing store: %+v, requests=%d", updated, notModified.Load())
	}
	requireIPInfoASN(t, g, addr, time.Now().UTC(), "64500")
	g.Close()

	g = openTestGate(t, cfg)
	if err := g.LoadStore(); err != nil {
		t.Fatal(err)
	}
	if got := g.ipinfoView.Load().currentAt; !got.Equal(updated.currentAt) {
		t.Fatalf("verification was not retained across restart: got %v, want %v", got, updated.currentAt)
	}

	// The download succeeds, but a directory at the CSV path denies extraction
	// publication. This leaves the old pointer, handles and verification intact.
	offered.Store(&second)
	if err := os.Mkdir(cfg.IPInfoSnapshotPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := g.downloadAndLoad(t.Context()); err == nil {
		t.Fatal("import unexpectedly succeeded")
	}
	current, err := resolveIPInfoPebblePath(root)
	if err != nil || current != initialPath || !g.ipinfoView.Load().currentAt.Equal(updated.currentAt) {
		t.Fatalf("failed import advanced publication: %s, %v", current, err)
	}
	requireIPInfoASN(t, g, addr, time.Now().UTC(), "64500")
	g.Close()
	if err := os.Remove(cfg.IPInfoSnapshotPath); err != nil {
		t.Fatal(err)
	}

	g = openTestGate(t, cfg)
	if err := g.LoadStore(); err != nil {
		t.Fatal(err)
	}
	if err := g.downloadAndLoad(t.Context()); err != nil {
		t.Fatal(err)
	}
	if notModified.Load() != 2 || g.ipinfoView.Load().path == initialPath {
		t.Fatal("unchanged archive did not complete the pending import across restart")
	}
	requireIPInfoASN(t, g, addr, time.Now().UTC(), "64502")
	if _, err := os.Stat(initialPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retired IPinfo directory remains: %v", err)
	}
}

func TestIPInfoWeak304RequiresCompleteArchiveToRenew(t *testing.T) {
	payload := snapshotGzip(t, snapshotCSV("64500"))
	const tag = "\"archive\""
	var failFull atomic.Bool
	var weakResponses, fullResponses atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == tag {
			weakResponses.Add(1)
			w.Header().Set("ETag", "W/"+tag)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		fullResponses.Add(1)
		if failFull.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("ETag", tag)
		_, _ = w.Write(payload)
	}))
	defer server.Close()
	cfg := ipinfoTestConfig(t, server.URL)
	g := openTestGate(t, cfg)
	if err := g.downloadAndLoad(t.Context()); err != nil {
		t.Fatal(err)
	}
	active := g.ipinfoView.Load()

	failFull.Store(true)
	if err := g.downloadAndLoad(t.Context()); err == nil {
		t.Fatal("weak 304 renewed freshness without a complete response")
	}
	if g.ipinfoView.Load() != active {
		t.Fatal("unsuccessful recovery changed the serving view")
	}
	receipt := filepath.Join(cfg.IPInfoPebblePath, ipinfoVerificationFile)
	if _, err := os.Stat(receipt); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsuccessful recovery wrote a verification record: %v", err)
	}

	failFull.Store(false)
	if err := g.downloadAndLoad(t.Context()); err != nil {
		t.Fatal(err)
	}
	renewed := g.ipinfoView.Load()
	if renewed.path != active.path || !renewed.currentAt.After(active.currentAt) {
		t.Fatalf("full matching archive did not renew the existing database: %+v", renewed)
	}
	if weakResponses.Load() != 2 || fullResponses.Load() != 3 {
		t.Fatalf("checks weak=%d full=%d, want 2 and 3", weakResponses.Load(), fullResponses.Load())
	}
}

func TestIPInfoFailedCandidatePreservesServingDatabase(t *testing.T) {
	const goodASN = "64500"
	for _, tc := range []struct {
		name          string
		csv           []byte
		failClose     bool
		expectMessage string
	}{
		{name: "header only", csv: []byte("network,country_code,asn\n"), expectMessage: "no usable IP ranges"},
		{name: "all ranges rejected", csv: []byte("network,country_code,asn\ninvalid,US,AS64502\n"), expectMessage: "no usable IP ranges"},
		{name: "native endpoint families differ", csv: []byte("start_ip,end_ip,asn\n192.0.2.1,2001:db8::1,AS64502\n2001:db8::1,192.0.2.1,AS64502\n"), expectMessage: "no usable IP ranges"},
		{name: "reversed endpoints", csv: []byte("start_ip,end_ip,asn\n192.0.2.5,192.0.2.1,AS64502\n"), expectMessage: "no usable IP ranges"},
		{name: "ranges lack enrichment", csv: []byte("network,country_code,asn\n192.0.2.0/24,,AS\n2001:db8::/32,,\n"), expectMessage: "no usable IP ranges"},
		{name: "candidate close fails", csv: snapshotCSV("64502"), failClose: true, expectMessage: "close IPinfo candidate database"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ipinfoTestConfig(t, "")
			g := openTestGate(t, cfg)
			writeArchive := func(csv []byte) string {
				t.Helper()
				payload := snapshotGzip(t, csv)
				if err := os.WriteFile(cfg.IPInfoDownloadPath, payload, 0o600); err != nil {
					t.Fatal(err)
				}
				return fmt.Sprintf("%x", sha256.Sum256(payload))
			}
			goodHash := writeArchive(snapshotCSV(goodASN))
			if err := g.importIPInfo(t.Context(), goodHash, buildIPInfoPebbleFromSource); err != nil {
				t.Fatal(err)
			}
			current := g.ipinfoView.Load()
			if err := g.renewIPInfo(t.Context(), current, current.currentAt.Add(time.Nanosecond)); err != nil {
				t.Fatal(err)
			}
			current = g.ipinfoView.Load()
			pointer := filepath.Join(cfg.IPInfoPebblePath, ipinfoPebbleCurrentFile)
			receipt := filepath.Join(cfg.IPInfoPebblePath, ipinfoVerificationFile)
			read := func(path string) []byte {
				t.Helper()
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				return data
			}
			oldPointer, oldReceipt := read(pointer), read(receipt)

			var closeCalls int
			refusal := errors.New("fixture close error after real close")
			build := func(ctx context.Context, source io.Reader, rootDir string, compact bool, hash string) (string, error) {
				return buildIPInfoPebbleCandidate(ctx, source, rootDir, compact, hash, func(db *pebble.DB) error {
					closeCalls++
					if err := db.Close(); err != nil {
						t.Fatalf("real candidate close failed: %v", err)
					}
					if tc.failClose {
						return refusal
					}
					return nil
				})
			}
			err := g.importIPInfo(t.Context(), writeArchive(tc.csv), build)
			if err == nil || !strings.Contains(err.Error(), tc.expectMessage) {
				t.Fatalf("unexpected import error: %v", err)
			}
			if closeCalls != 1 || (tc.failClose && !errors.Is(err, refusal)) {
				t.Fatalf("candidate closes=%d, import error=%v", closeCalls, err)
			}
			if g.ipinfoView.Load() != current || !bytes.Equal(read(pointer), oldPointer) || !bytes.Equal(read(receipt), oldReceipt) {
				t.Fatal("failed candidate changed the active database or verification")
			}
			requireIPInfoASN(t, g, netip.MustParseAddr("2001:db8::42"), time.Now().UTC(), goodASN)
			generations, err := filepath.Glob(filepath.Join(cfg.IPInfoPebblePath, "db-*"))
			if err != nil || len(generations) != 1 || generations[0] != current.path {
				t.Fatalf("failed candidate was not removed: %v err=%v", generations, err)
			}
		})
	}
}

func TestIPInfoAcceptsSingleAddressFamily(t *testing.T) {
	for _, network := range []string{"203.0.113.0/24", "2001:db8::/32"} {
		root := t.TempDir()
		csvPath := filepath.Join(root, "source.csv")
		content := "network,country_code,asn\n" + network + ",US,AS64500\n"
		if err := os.WriteFile(csvPath, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := buildIPInfoPebble(t.Context(), csvPath, root, false); err != nil {
			t.Fatalf("%s: %v", network, err)
		}
	}
}

func TestIPInfoRejectsMissingAndInvalidMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, key, value string
	}{
		{"missing version", ipinfoPebbleMetaVersion, ""},
		{"malformed version", ipinfoPebbleMetaVersion, "1garbage"},
		{"unsupported version", ipinfoPebbleMetaVersion, "2"},
		{"missing build time", ipinfoPebbleMetaBuiltAt, ""},
		{"malformed build time", ipinfoPebbleMetaBuiltAt, "1577836800garbage"},
		{"zero build time", ipinfoPebbleMetaBuiltAt, "0"},
		{"future build time", ipinfoPebbleMetaBuiltAt, "253402300799"},
		{"malformed source hash", ipinfoPebbleMetaSHA256, "invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ipinfoTestConfig(t, "")
			if err := os.WriteFile(cfg.IPInfoSnapshotPath, snapshotCSV("64500"), 0o600); err != nil {
				t.Fatal(err)
			}
			path, err := buildIPInfoPebble(t.Context(), cfg.IPInfoSnapshotPath, cfg.IPInfoPebblePath, false)
			if err != nil {
				t.Fatal(err)
			}
			modifyIPInfoMetadata(t, path, tc.key, tc.value)
			if err := updateIPInfoPebbleCurrent(t.Context(), cfg.IPInfoPebblePath, path); err != nil {
				t.Fatal(err)
			}
			g := openTestGate(t, cfg)
			if err := g.LoadStore(); err == nil {
				t.Fatal("published a database with bad provenance")
			}
			if _, ok := g.LookupIPForAdmission("203.0.113.1", time.Now().UTC()); ok {
				t.Fatal("rejected database leaked into admission")
			}
			// A writeable reopen verifies that the rejected handle was closed.
			db, err := pebble.Open(path, nil)
			if err != nil {
				t.Fatalf("rejected database remained open: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestIPInfoCloseFencesCandidateDuringConcurrentLookups(t *testing.T) {
	cfg := ipinfoTestConfig(t, "")
	csv := cfg.IPInfoSnapshotPath
	if err := os.WriteFile(csv, snapshotCSV("64500"), 0o600); err != nil {
		t.Fatal(err)
	}
	first, err := buildIPInfoPebble(t.Context(), csv, cfg.IPInfoPebblePath, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := updateIPInfoPebbleCurrent(t.Context(), cfg.IPInfoPebblePath, first); err != nil {
		t.Fatal(err)
	}
	g := openTestGate(t, cfg)
	if err := g.LoadStore(); err != nil {
		t.Fatal(err)
	}
	candidate, err := buildIPInfoPebble(t.Context(), csv, cfg.IPInfoPebblePath, false)
	if err != nil {
		t.Fatal(err)
	}
	store, view, err := g.prepareIPInfoStore(t.Context(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	candidatePublished := false
	defer func() {
		if !candidatePublished {
			store.Close()
		}
	}()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := g.publishIPInfoStore(ctx, store, view, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled candidate publication: %v", err)
	}

	var readers sync.WaitGroup
	started := make(chan struct{})
	stopped := make(chan struct{})
	readers.Go(func() {
		addr := netip.MustParseAddr("2001:db8::1")
		close(started)
		for {
			select {
			case <-stopped:
				return
			default:
				g.lookupIP(addr, time.Now().UTC())
			}
		}
	})
	<-started
	err = g.publishIPInfoStore(t.Context(), store, view, true)
	candidatePublished = err == nil
	g.Close()
	close(stopped)
	readers.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if err := g.publishIPInfoStore(t.Context(), store, view, true); err == nil {
		t.Fatal("candidate republished after close")
	}
	if path, err := resolveIPInfoPebblePath(cfg.IPInfoPebblePath); err != nil || path != candidate {
		t.Fatalf("close or cancellation changed CURRENT_DB: %q %v", path, err)
	}
	if _, ok := g.LookupIPForAdmission("2001:db8::1", time.Now().UTC()); ok {
		t.Fatal("closed generation is still visible")
	}
}

func TestIPInfoStageRejectsDifferentArchive(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "source.gz")
	archive := snapshotGzip(t, []byte("candidate"))
	if err := os.WriteFile(src, archive, 0o600); err != nil {
		t.Fatal(err)
	}
	if file, err := stageIPInfoCSV(t.Context(), src, dir, fmt.Sprintf("%x", sha256.Sum256(nil))); err == nil {
		file.Close()
		t.Fatal("different hash produced a usable CSV")
	}
	if matches, err := filepath.Glob(filepath.Join(dir, "ipinfo-*.tmp")); err != nil || len(matches) != 0 {
		t.Fatalf("failed staging left temporary files: %v, %v", matches, err)
	}
	file, err := stageIPInfoCSV(t.Context(), src, dir, fmt.Sprintf("%x", sha256.Sum256(archive)))
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if data, err := io.ReadAll(file); err != nil || string(data) != "candidate" {
		t.Fatalf("private staged CSV=%q, err=%v", data, err)
	}
}

func TestIPInfoBuildUsesPrivateExtraction(t *testing.T) {
	cfg := ipinfoTestConfig(t, "")
	g := openTestGate(t, cfg)
	payload := snapshotGzip(t, snapshotCSV("64502"))
	if err := os.WriteFile(cfg.IPInfoDownloadPath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	build := func(ctx context.Context, source io.Reader, root string, compact bool, hash string) (string, error) {
		// Simulate a diagnostic CSV being replaced between extract and build.
		if err := os.WriteFile(cfg.IPInfoSnapshotPath, snapshotCSV("64500"), 0o600); err != nil {
			t.Fatal(err)
		}
		return buildIPInfoPebbleFromSource(ctx, source, root, compact, hash)
	}
	if err := g.importIPInfo(t.Context(), fmt.Sprintf("%x", sha256.Sum256(payload)), build); err != nil {
		t.Fatal(err)
	}
	addr := netip.MustParseAddr("2001:db8::42")
	requireIPInfoASN(t, g, addr, time.Now().UTC(), "64502")
	g.Close()
	g = openTestGate(t, cfg)
	if err := g.LoadStore(); err != nil {
		t.Fatal(err)
	}
	requireIPInfoASN(t, g, addr, time.Now().UTC(), "64502")
}

func ipinfoTestConfig(t *testing.T, url string) config.ReputationConfig {
	t.Helper()
	root := t.TempDir()
	return config.ReputationConfig{
		Enabled: true, ReputationDir: filepath.Join(root, "users"),
		IPInfoDownloadEnabled: url != "", IPInfoDownloadURL: url, IPInfoDownloadToken: "fixture",
		IPInfoPebblePath: filepath.Join(root, "db"), IPInfoPebbleLoadIPv4: true, IPInfoPebbleCleanup: true,
		IPInfoSnapshotPath: filepath.Join(root, "snapshot.csv"), IPInfoDeleteCSVAfterImport: true,
		IPInfoDownloadPath: filepath.Join(root, "snapshot.gz"), IPInfoKeepGzip: true,
	}
}

func openTestGate(t *testing.T, cfg config.ReputationConfig) *Gate {
	t.Helper()
	g, err := NewGate(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.Close)
	return g
}

func requireIPInfoASN(t *testing.T, g *Gate, addr netip.Addr, now time.Time, asn string) {
	t.Helper()
	primary, fallback, _, _, known := g.lookupIP(addr, now)
	if primary.Source == "" {
		primary = fallback
	}
	if !known || primary.ASN != asn {
		t.Fatalf("%s at %s selected %+v, known=%t, want ASN %s", addr, now, primary, known, asn)
	}
}

func snapshotCSV(asn string) []byte {
	return []byte(fmt.Sprintf("network,country_code,asn\n203.0.113.0/24,US,AS%s\n2001:db8::/32,US,AS%s\n", asn, asn))
}

func snapshotGzip(t *testing.T, csv []byte) []byte {
	t.Helper()
	var data bytes.Buffer
	writer := gzip.NewWriter(&data)
	if _, err := writer.Write(csv); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return data.Bytes()
}

func modifyIPInfoMetadata(t *testing.T, path, key, value string) {
	t.Helper()
	db, err := pebble.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	if value == "" {
		err = db.Delete([]byte(key), pebble.Sync)
	} else {
		err = db.Set([]byte(key), []byte(value), pebble.Sync)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestIPInfoRefreshUsesOnePathIdentity(t *testing.T) {
	for _, leadingSpace := range []string{"archive", "csv", "root"} {
		t.Run(leadingSpace, func(t *testing.T) {
			t.Chdir(t.TempDir())
			payload := snapshotGzip(t, snapshotCSV("64502"))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write(payload)
			}))
			defer server.Close()
			cfg := ipinfoTestConfig(t, server.URL)
			cfg.IPInfoDownloadPath = "snapshot.gz"
			cfg.IPInfoSnapshotPath = "snapshot.csv"
			cfg.IPInfoPebblePath = "pebble"
			if err := os.WriteFile(cfg.IPInfoSnapshotPath, snapshotCSV("64500"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(cfg.IPInfoPebblePath, 0o700); err != nil {
				t.Fatal(err)
			}
			switch leadingSpace {
			case "archive":
				cfg.IPInfoDownloadPath = " snapshot.gz"
			case "csv":
				cfg.IPInfoSnapshotPath = " snapshot.csv"
			case "root":
				cfg.IPInfoPebblePath = " pebble"
			}
			g := openTestGate(t, cfg)
			if err := g.downloadAndLoad(t.Context()); err != nil {
				t.Fatal(err)
			}
			addr := netip.MustParseAddr("2001:db8::42")
			requireIPInfoASN(t, g, addr, time.Now().UTC(), "64502")
			if g.ipinfoView.Load().hash != fmt.Sprintf("%x", sha256.Sum256(payload)) {
				t.Fatal("published store hash differs from the downloaded archive")
			}
			g.Close()
			g = openTestGate(t, cfg)
			if err := g.LoadStore(); err != nil {
				t.Fatalf("restart after successful refresh: %v", err)
			}
			requireIPInfoASN(t, g, addr, time.Now().UTC(), "64502")
		})
	}
}

func TestIPInfoRefreshRefusesManagedPathCollisionBeforeFetch(t *testing.T) {
	payload := snapshotGzip(t, snapshotCSV("64502"))
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write(payload)
	}))
	defer server.Close()
	cfg := ipinfoTestConfig(t, server.URL)
	csv := cfg.IPInfoSnapshotPath
	if err := os.WriteFile(csv, snapshotCSV("64500"), 0o600); err != nil {
		t.Fatal(err)
	}
	dbPath, err := buildIPInfoPebble(t.Context(), csv, cfg.IPInfoPebblePath, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := updateIPInfoPebbleCurrent(t.Context(), cfg.IPInfoPebblePath, dbPath); err != nil {
		t.Fatal(err)
	}
	pointer := filepath.Join(cfg.IPInfoPebblePath, ipinfoPebbleCurrentFile)
	before, err := os.ReadFile(pointer)
	if err != nil {
		t.Fatal(err)
	}
	cfg.IPInfoSnapshotPath = pointer
	g := openTestGate(t, cfg)
	if err := g.LoadStore(); err != nil {
		t.Fatal(err)
	}
	err = g.downloadAndLoad(t.Context())
	if err == nil || requests.Load() != 0 {
		t.Fatalf("collision reached publication: requests=%d error=%v", requests.Load(), err)
	}
	if after, err := os.ReadFile(pointer); err != nil || !bytes.Equal(before, after) {
		t.Fatalf("collision changed current pointer: %q error=%v", after, err)
	}
	requireIPInfoASN(t, g, netip.MustParseAddr("2001:db8::42"), time.Now().UTC(), "64500")
}

func TestIPInfoManagedPathIdentity(t *testing.T) {
	cfg := ipinfoTestConfig(t, "http://unused.invalid")
	cfg.IPInfoSnapshotPath = filepath.Join(cfg.IPInfoPebblePath, "diagnostics", "snapshot.csv")
	base := normalizeGateConfig(cfg)
	if err := validateIPInfoPaths(base, nil); err != nil {
		t.Fatalf("distinct diagnostic directory: %v", err)
	}
	if err := os.MkdirAll(base.ipinfoPebblePath, 0o700); err != nil {
		t.Fatal(err)
	}
	receipt := filepath.Join(base.ipinfoPebblePath, ipinfoVerificationFile)
	if err := os.WriteFile(receipt, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	hardLink := filepath.Join(filepath.Dir(cfg.IPInfoDownloadPath), "alias")
	if err := os.Link(receipt, hardLink); err != nil {
		t.Fatal(err)
	}
	custom := filepath.Join(base.ipinfoPebblePath, "custom")
	if err := updateIPInfoPebbleCurrent(t.Context(), base.ipinfoPebblePath, custom); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		key, archive, csv string
	}{
		{"archive and CSV", base.ipinfoDownloadPath, base.ipinfoDownloadPath},
		{"metadata and CSV", base.ipinfoDownloadPath, download.MetadataPath(base.ipinfoDownloadPath)},
		{"reserved generation child", base.ipinfoDownloadPath, filepath.Join(base.ipinfoPebblePath, "db-retained", "archive.csv")},
		{"custom generation child", base.ipinfoDownloadPath, filepath.Join(custom, "archive.csv")},
		{"verification record hard link", hardLink, base.ipinfoSnapshotPath},
	} {
		t.Run(tc.key, func(t *testing.T) {
			gcfg := base
			gcfg.ipinfoDownloadPath, gcfg.ipinfoSnapshotPath = tc.archive, tc.csv
			if err := validateIPInfoPaths(gcfg, nil); err == nil || !strings.Contains(err.Error(), "IPinfo path collision") {
				t.Fatalf("managed path collision was not refused: %v", err)
			}
		})
	}
}

func TestIPInfoRefreshUsesValidatedPathAcrossSymlinkParent(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix symlink parent traversal")
	}
	payload := snapshotGzip(t, snapshotCSV("64502"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer server.Close()
	cfg := ipinfoTestConfig(t, server.URL)
	child := filepath.Join(cfg.IPInfoPebblePath, "child")
	if err := os.MkdirAll(child, 0o700); err != nil {
		t.Fatal(err)
	}
	root := filepath.Dir(cfg.IPInfoPebblePath)
	link := filepath.Join(root, "link")
	if err := os.Symlink(child, link); err != nil {
		t.Fatal(err)
	}
	// Joining with filepath.Join would erase the traversal before the gate
	// sees it. The OS interprets this raw path as the serving pointer.
	cfg.IPInfoSnapshotPath = link + "/../" + ipinfoPebbleCurrentFile
	g := openTestGate(t, cfg)
	if err := g.downloadAndLoad(t.Context()); err != nil {
		t.Fatal(err)
	}
	current, err := resolveIPInfoPebblePath(cfg.IPInfoPebblePath)
	if err != nil || current != g.ipinfoView.Load().path {
		t.Fatalf("serving pointer changed during CSV publication or cleanup: %q, %v", current, err)
	}
	if _, err := os.Stat(filepath.Join(root, ipinfoPebbleCurrentFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("normalized diagnostic path was not cleaned up: %v", err)
	}
	g.Close()
	g = openTestGate(t, cfg)
	if err := g.LoadStore(); err != nil {
		t.Fatal(err)
	}
	requireIPInfoASN(t, g, netip.MustParseAddr("2001:db8::42"), time.Now().UTC(), "64502")
}
