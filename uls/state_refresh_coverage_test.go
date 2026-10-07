package uls

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
)

// A context boundary pauses a real refresh after it suppresses factual lookups.
// Concurrent readers then span the actual extraction, import and file swap. A
// real result from the old SQLite owner is deliberately held until publication
// and passed through the existing cold-lookup publication barrier.
func TestRefreshChangesStateDuringConcurrentLookups(t *testing.T) {
	SetLicenseDBPath(fixtureDB(t, false))
	t.Cleanup(func() { SetLicenseDBPath("") })
	oldCache := licenseCache.Load()
	oldGeneration := fccSnapshot.generation.Load()
	oldDB, hasState := getLicenseDB()
	if oldDB == nil || !hasState {
		t.Fatal("state-capable old database unavailable")
	}
	oldResult := queryLicense(oldDB, "SELECT state FROM AM WHERE call_sign=? LIMIT 1;", "K1ABC")
	if oldResult != (LookupResult{Available: true, Found: true, State: "CA"}) {
		t.Fatalf("old SQLite fact=%+v", oldResult)
	}
	files := map[string]string{
		"HD.DAT": sourceRow("HD", 1, "K1ABC", "A", ""),
		"AM.DAT": sourceRow("AM", 1, "K1ABC", "", ""),
		"EN.DAT": strings.Repeat(sourceRow("EN", 1, "K1ABC", "L", "TX"), 1024),
	}
	payload := zippedSources(t, files)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(payload) }))
	defer server.Close()
	dir := t.TempDir()
	cfg := config.FCCULSConfig{URL: server.URL, Archive: filepath.Join(dir, "fresh.zip"), DBPath: fccSnapshot.path}
	base, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entered, release, published := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var gateUsed atomic.Bool
	ctx := &stateProgressContext{Context: base, progress: func() {
		if RefreshInProgress() && gateUsed.CompareAndSwap(false, true) {
			close(entered)
			select {
			case <-release:
			case <-base.Done():
			}
		}
	}}
	readers := startStateRefreshReaders(base.Done(), entered, published, oldResult)
	refreshDone := make(chan struct{})
	refreshStarted := false
	defer func() {
		cancel()
		<-readers.done
		if refreshStarted {
			<-refreshDone
		}
	}()
	for i := 0; i < stateRefreshReaderCount; i++ {
		awaitStateReader(base, t, readers.ready, readers.failures)
	}
	type refreshOutcome struct {
		updated bool
		err     error
	}
	result := make(chan refreshOutcome, 1)
	refreshStarted = true
	go func() {
		defer close(refreshDone)
		updated, err := Refresh(ctx, cfg, true)
		result <- refreshOutcome{updated, err}
	}()
	for i := 0; i < stateRefreshReaderCount; i++ {
		awaitStateReader(base, t, readers.observed, readers.failures)
	}
	close(release)
	select {
	case outcome := <-result:
		if outcome.err != nil || !outcome.updated {
			t.Fatalf("replacement updated=%v err=%v", outcome.updated, outcome.err)
		}
	case <-base.Done():
		t.Fatal("refresh did not finish within deadline")
	}
	if result := finishLookup(fccSnapshot, oldCache, licenseCacheKey{call: "K1ABC"}, oldGeneration, 0, oldResult, time.Now(), time.Now); result.Available {
		t.Fatalf("old CA fact returned after TX publication: %+v", result)
	}
	close(published)
	<-readers.done
	select {
	case err := <-readers.failures:
		t.Fatal(err)
	default:
	}
	if got := LookupUS("K1ABC"); got != (LookupResult{Available: true, Found: true, State: "TX"}) {
		t.Fatalf("published fact=%+v, want TX", got)
	}
	if oldGeneration == fccSnapshot.generation.Load() || RefreshInProgress() {
		t.Fatal("publication retained the old generation or refresh flag")
	}
}

const stateRefreshReaderCount = 4

type stateRefreshReaders struct {
	ready, observed chan struct{}
	failures        chan error
	done            chan struct{}
}

// The caller closes stop through context cancellation and joins done before
// releasing FCC globals or files. LookupUS owns its synchronous database query.
// Each reader can report at most one error; buffered channels cannot strand a
// reader if the controlling test exits early.
func startStateRefreshReaders(stop, entered, published <-chan struct{}, oldResult LookupResult) stateRefreshReaders {
	r := stateRefreshReaders{
		ready: make(chan struct{}, stateRefreshReaderCount), observed: make(chan struct{}, stateRefreshReaderCount),
		failures: make(chan error, stateRefreshReaderCount), done: make(chan struct{}),
	}
	var workers sync.WaitGroup
	for i := 0; i < stateRefreshReaderCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if got := LookupUS("K1ABC"); got != oldResult {
				r.failures <- fmt.Errorf("before refresh: %+v", got)
				return
			}
			r.ready <- struct{}{}
			select {
			case <-entered:
			case <-stop:
				return
			}
			if got := LookupUS("K1ABC"); got.Available {
				r.failures <- fmt.Errorf("refresh lookup remained factual: %+v", got)
				return
			}
			r.observed <- struct{}{}
			if err := sampleStateAcrossRefresh(stop, published); err != nil {
				r.failures <- err
			}
		}()
	}
	go func() {
		workers.Wait()
		close(r.done)
	}()
	return r
}

func awaitStateReader(ctx context.Context, t *testing.T, ready <-chan struct{}, failures <-chan error) {
	t.Helper()
	select {
	case <-ready:
	case err := <-failures:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("concurrent lookup stage did not finish within deadline")
	}
}

func sampleStateAcrossRefresh(stop, published <-chan struct{}) error {
	for {
		select {
		case <-stop:
			return context.Canceled
		case <-published:
			for i := 0; i < 100; i++ {
				if got := LookupUS("K1ABC"); got != (LookupResult{Available: true, Found: true, State: "TX"}) {
					return fmt.Errorf("post-publication lookup %d: %+v, want TX", i, got)
				}
			}
			return nil
		default:
			got := LookupUS("K1ABC")
			if got.Available && (!got.Found || (got.State != "CA" && got.State != "TX")) {
				return fmt.Errorf("lookup across replacement: %+v", got)
			}
			runtime.Gosched()
		}
	}
}
