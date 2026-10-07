package archive

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"

	"github.com/cockroachdb/pebble"
)

func historyTestWriter(tb testing.TB, retention int) *Writer {
	tb.Helper()
	w, err := NewWriter(config.ArchiveConfig{
		DBPath: filepath.Join(tb.TempDir(), "history"), Synchronous: "off",
		RetentionSeconds: retention,
	})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(w.Stop)
	return w
}

func putHistoryRow(tb testing.TB, w *Writer, key, raw []byte) {
	tb.Helper()
	if err := w.db.Set(key, raw, pebble.NoSync); err != nil {
		tb.Fatal(err)
	}
}

func historyRaw(call string) []byte {
	return encodeRecord(&spot.Spot{DXCall: call, DECall: "W1XYZ", Frequency: 14030, Mode: "CW"})
}

func TestHistoryPageBeyondProductionBudget(t *testing.T) {
	w := historyTestWriter(t, 86400)
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	base := now.Add(-time.Second).UnixNano()
	const rows = recentScanMax + 2
	batch := w.db.NewBatch()
	defer batch.Close()
	miss := historyRaw("W9ZZZ")
	for i := 0; i < rows; i++ {
		raw := miss
		if i == 0 {
			// Persist a genuine pre-normalization record, rather than constructing
			// a modern Spot that would already remove the numeric DX SSID.
			raw = encodeRecordV2ForTest(&spot.Spot{DXCall: "K1ABC-1", DECall: "W1XYZ", Frequency: 14030, Mode: "CW"})
		}
		if err := batch.Set(spotKeyBytes(base+int64(i), uint32(i)), raw, nil); err != nil {
			t.Fatal(err)
		}
		if (i+1)%2000 == 0 {
			if err := batch.Commit(pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			batch.Reset()
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	match := func(s *spot.Spot) bool { return s.DXCallNorm == "K1ABC" }
	request := HistoryRequest{Limit: 1, Now: now, Match: match}
	first, err := w.ReadHistoryPage(request)
	if err != nil {
		t.Fatal(err)
	}
	if first.End != HistoryBudgetReached || first.Examined != recentScanMax || len(first.Spots) != 0 {
		t.Fatalf("first page: end=%v examined=%d spots=%d", first.End, first.Examined, len(first.Spots))
	}
	wantBefore := spotKeyBytes(base+3, 3)
	if !bytes.Equal(first.Before, wantBefore) {
		t.Fatalf("cursor consumed lookahead: got %x want %x", first.Before, wantBefore)
	}
	request.Before = first.Before
	second, err := w.ReadHistoryPage(request)
	if err != nil {
		t.Fatal(err)
	}
	if second.End != HistoryExhausted || second.Examined != 3 || len(second.Spots) != 1 || second.Spots[0].DXCallNorm != "K1ABC" {
		t.Fatalf("second page: %+v", second)
	}
	if len(second.Before) != 0 {
		t.Fatal("exhausted page has continuation")
	}
	for _, test := range []struct {
		name  string
		match func(*spot.Spot) bool
	}{
		{"absent", func(s *spot.Spot) bool { return s.DXCallNorm == "N0ABS" }},
		{"rejected", func(*spot.Spot) bool { return false }},
	} {
		t.Run(test.name, func(t *testing.T) {
			page, err := w.ReadHistoryPage(HistoryRequest{Limit: 1, Now: now, Match: test.match})
			if err != nil || page.End != HistoryBudgetReached || page.Examined != recentScanMax || len(page.Spots) != 0 {
				t.Fatalf("page=%+v err=%v", page, err)
			}
		})
	}
}

func TestHistoryPageEqualTimestampDeletedCursorAndLookahead(t *testing.T) {
	w := historyTestWriter(t, 60)
	now := time.Now().UTC()
	for seq := uint32(1); seq <= 3; seq++ {
		putHistoryRow(t, w, spotKeyBytes(now.UnixNano(), seq), historyRaw("K1ABC"))
	}
	request := HistoryRequest{Limit: 1, Now: now}
	first, err := w.ReadHistoryPage(request)
	if err != nil || first.End != HistoryCountReached || first.Examined != 2 || !bytes.Equal(first.Before, spotKeyBytes(now.UnixNano(), 3)) {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	if err := w.db.Delete(first.Before, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	request.Before = first.Before
	second, err := w.ReadHistoryPage(request)
	if err != nil || second.End != HistoryCountReached || !bytes.Equal(second.Before, spotKeyBytes(now.UnixNano(), 2)) {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	request.Before = second.Before
	third, err := w.ReadHistoryPage(request)
	if err != nil || third.End != HistoryExhausted || third.Examined != 1 || len(third.Spots) != 1 {
		t.Fatalf("third=%+v err=%v", third, err)
	}
}

func TestHistoryPageRequestCutoffAndFreshContinuation(t *testing.T) {
	w := historyTestWriter(t, 10)
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	cutoff := now.Add(-10 * time.Second)
	for i, ts := range []time.Time{cutoff.Add(-time.Nanosecond), cutoff, cutoff.Add(time.Nanosecond), now} {
		putHistoryRow(t, w, spotKeyBytes(ts.UnixNano(), uint32(i)), historyRaw("K1ABC"))
	}
	page, err := w.ReadHistoryPage(HistoryRequest{Limit: 10, Now: now})
	if err != nil || page.End != HistoryExhausted || len(page.Spots) != 3 || page.Examined != 3 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	if !page.Spots[2].Time.Equal(cutoff) {
		t.Fatal("exact cutoff was excluded")
	}
	first, err := w.ReadHistoryPage(HistoryRequest{Limit: 1, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	second, err := w.ReadHistoryPage(HistoryRequest{Limit: 10, Now: now.Add(time.Nanosecond), Before: first.Before})
	if err != nil || len(second.Spots) != 1 || !second.Spots[0].Time.Equal(cutoff.Add(time.Nanosecond)) {
		t.Fatalf("fresh cutoff: %+v err=%v", second, err)
	}
}

func TestHistoryPageCorruptionAndMalformedBoundary(t *testing.T) {
	w := historyTestWriter(t, 60)
	now := time.Now().UTC()
	putHistoryRow(t, w, spotKeyBytes(now.UnixNano(), 3), []byte("bad record"))
	malformed := append(spotKeyBytes(now.UnixNano(), 2), 0)
	putHistoryRow(t, w, malformed, historyRaw("K1ABC"))
	putHistoryRow(t, w, spotKeyBytes(now.UnixNano(), 1), historyRaw("K1ABC"))
	page, err := w.ReadHistoryPage(HistoryRequest{Limit: 10, Now: now})
	if err != nil || page.Unreadable != 2 || page.Examined != 3 || len(page.Spots) != 1 || page.End != HistoryExhausted {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	// Place the malformed key exactly at the production consumption boundary.
	batch := w.db.NewBatch()
	defer batch.Close()
	raw := historyRaw("W9ZZZ")
	for i := 0; i < recentScanMax-2; i++ {
		if err := batch.Set(spotKeyBytes(now.UnixNano()+1+int64(i), 0), raw, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	// Remove the corrupt record above the malformed key, leaving the malformed
	// key as consumed row 199999 and a valid older row as lookahead.
	if err := w.db.Delete(spotKeyBytes(now.UnixNano(), 3), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	_, err = w.ReadHistoryPage(HistoryRequest{Limit: 1, Now: now, Match: func(*spot.Spot) bool { return false }})
	if err == nil {
		t.Fatal("unrepresentable boundary silently produced a continuation")
	}
}

func TestHistoryPageConcurrentCleanupAndInsert(t *testing.T) {
	w := historyTestWriter(t, 60)
	now := time.Now().UTC()
	for i := 0; i < 3; i++ {
		putHistoryRow(t, w, spotKeyBytes(now.Add(time.Duration(i)*time.Nanosecond).UnixNano(), 0), historyRaw("K1ABC"))
	}
	entered, release := make(chan struct{}), make(chan struct{})
	result := make(chan HistoryPage, 1)
	errors := make(chan error, 1)
	go func() {
		first := true
		page, err := w.ReadHistoryPage(HistoryRequest{Limit: 10, Now: now, Match: func(*spot.Spot) bool {
			if first {
				first = false
				close(entered)
				<-release
			}
			return true
		}})
		result <- page
		errors <- err
	}()
	<-entered
	w.cleanupOnceAt(now.Add(2 * time.Minute))
	w.flush([]*spot.Spot{{DXCall: "W9ZZZ", DECall: "W1XYZ", Frequency: 14030, Mode: "CW", Time: now.Add(time.Minute)}})
	close(release)
	page := <-result
	if err := <-errors; err != nil || len(page.Spots) != 3 {
		t.Fatalf("request view changed: %+v err=%v", page, err)
	}
	fresh, err := w.ReadHistoryPage(HistoryRequest{Limit: 10, Now: now.Add(2 * time.Minute)})
	if err != nil || len(fresh.Spots) != 1 || fresh.Spots[0].DXCallNorm != "W9ZZZ" {
		t.Fatalf("fresh view=%+v err=%v", fresh, err)
	}
}

func TestHistoryReadersStopWaitsAndCancels(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "paged"
		if legacy {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			w := historyTestWriter(t, 60)
			now := time.Now().UTC()
			for seq := uint32(0); seq < 2; seq++ {
				putHistoryRow(t, w, spotKeyBytes(now.UnixNano(), seq), historyRaw("K1ABC"))
			}
			entered, release := make(chan struct{}), make(chan struct{})
			queryDone := make(chan error, 1)
			match := func(*spot.Spot) bool { close(entered); <-release; return true }
			go func() {
				var err error
				if legacy {
					_, err = w.RecentFiltered(2, match)
				} else {
					_, err = w.ReadHistoryPage(HistoryRequest{Limit: 2, Now: now, Match: match})
				}
				queryDone <- err
			}()
			<-entered
			stopped := make(chan struct{})
			go func() { w.Stop(); close(stopped) }()
			<-w.stop
			select {
			case <-stopped:
				t.Fatal("Stop closed DB while a reader still owned its iterator")
			default:
			}
			close(release)
			if err := <-queryDone; err == nil {
				t.Fatal("reader ignored Stop cancellation")
			}
			<-stopped
			if _, err := w.ReadHistoryPage(HistoryRequest{Limit: 1}); err == nil {
				t.Fatal("query opened a stopped DB")
			}
		})
	}
}

func TestHistoryPageCancellationAndValidation(t *testing.T) {
	w := historyTestWriter(t, 60)
	now := time.Now().UTC()
	putHistoryRow(t, w, spotKeyBytes(now.UnixNano(), 0), historyRaw("K1ABC"))
	done := make(chan struct{})
	_, err := w.ReadHistoryPage(HistoryRequest{Limit: 1, Now: now, Done: done, Match: func(*spot.Spot) bool { close(done); return true }})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error=%v", err)
	}
	for _, request := range []HistoryRequest{{Limit: 0}, {Limit: 251}, {Limit: 1, Before: []byte("invalid")}} {
		if _, err := w.ReadHistoryPage(request); err == nil {
			t.Fatalf("accepted invalid request %+v", request)
		}
	}
}

func TestHistoryConcurrentStartStop(t *testing.T) {
	w := historyTestWriter(t, 60)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(w.Start)
		wg.Go(w.Stop)
	}
	wg.Wait()
	w.Start()
	w.Stop()
	if _, err := w.Recent(1); err == nil {
		t.Fatal("legacy reader opened a stopped DB")
	}
}

func BenchmarkReadHistoryPage(b *testing.B) {
	for _, shape := range []string{"dense", "sparse", "absent", "rejected", "legacy", "absent-budget"} {
		b.Run(shape, func(b *testing.B) {
			w := historyTestWriter(b, 86400)
			now := time.Now().UTC()
			base := now.Add(-time.Second).UnixNano()
			rows := 10000
			if shape == "absent-budget" {
				rows = recentScanMax + 2
			}
			batch := w.db.NewBatch()
			defer batch.Close()
			for i := 0; i < rows; i++ {
				call := "K1ABC"
				if shape == "sparse" && i%1000 != 0 || shape == "absent" || shape == "absent-budget" {
					call = "W9ZZZ"
				}
				raw := historyRaw(call)
				if shape == "legacy" {
					raw = encodeRecordV2ForTest(&spot.Spot{DXCall: "K1ABC-1", DECall: "W1XYZ", Frequency: 14030, Mode: "CW"})
				}
				if err := batch.Set(spotKeyBytes(base+int64(i), uint32(i)), raw, nil); err != nil {
					b.Fatal(err)
				}
			}
			if err := batch.Commit(pebble.NoSync); err != nil {
				b.Fatal(err)
			}
			request := HistoryRequest{Limit: 10, Now: now, Match: func(s *spot.Spot) bool { return shape != "rejected" && s.DXCallNorm == "K1ABC" }}
			want := 10
			wantExamined, wantEnd := 11, HistoryCountReached
			if shape == "sparse" {
				wantExamined, wantEnd = rows, HistoryExhausted
			}
			if shape == "absent" || shape == "rejected" || shape == "absent-budget" {
				want = 0
				wantExamined, wantEnd = rows, HistoryExhausted
			}
			if shape == "absent-budget" {
				wantExamined, wantEnd = recentScanMax, HistoryBudgetReached
			}
			b.ReportAllocs()
			b.ResetTimer()
			var examined int
			for i := 0; i < b.N; i++ {
				page, err := w.ReadHistoryPage(request)
				if err != nil || len(page.Spots) != want || page.Examined != wantExamined || page.End != wantEnd {
					b.Fatalf("page=%+v err=%v", page, err)
				}
				for _, s := range page.Spots {
					if s.DXCallNorm != "K1ABC" {
						b.Fatal("benchmark returned wrong station")
					}
				}
				examined = page.Examined
			}
			b.ReportMetric(float64(examined), "examined/op")
		})
	}
}
