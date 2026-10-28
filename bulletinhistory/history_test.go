package bulletinhistory

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T, window time.Duration, keys int) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "history.json")
	s := Open(path, window, keys)
	t.Cleanup(func() { _ = s.Close() })
	return s, path
}

func TestHistoryTruncationAndOriginalDedupe(t *testing.T) {
	s, _ := newTestStore(t, 10*time.Minute, 64)
	now := time.Now().UTC().Truncate(time.Second)
	prefix := strings.Repeat("🛰", 256)
	s.Record("WWV", prefix+" A", now)
	s.Record("WWV", prefix+" B", now.Add(time.Second))
	s.Record("WWV", prefix+" A", now.Add(2*time.Second))
	s.Record("WCY", strings.Repeat("x", 255), now)
	s.Record("WCY", strings.Repeat("y", 256), now.Add(time.Second))
	text := s.Render("WWV", true, now.Add(2*time.Second))
	if strings.Count(text, "[truncated]") != 2 || strings.Count(text, prefix) != 2 || strings.Contains(text, " A") || strings.Contains(text, " B") {
		t.Fatalf("expected two distinct truncated 256-character originals: %q", text)
	}
	if s.Stats().Duplicates != 1 {
		t.Fatalf("duplicate count = %d", s.Stats().Duplicates)
	}
	if got := s.Render("WCY", true, now.Add(2*time.Second)); strings.Contains(got, "[truncated]") {
		t.Fatalf("255/256 characters truncated: %q", got)
	}
}

func TestHistoryCapacityCutoffAndOrder(t *testing.T) {
	s, _ := newTestStore(t, 0, 0)
	now := time.Now().UTC().Add(time.Minute).Truncate(time.Second)
	for i := 0; i < 48; i++ {
		s.Record("WWV", fmt.Sprintf("item-%02d", i), now.Add(-24*time.Hour+time.Duration(i)*time.Minute))
		s.Record("WCY", fmt.Sprintf("other-%02d", i), now.Add(-24*time.Hour+time.Duration(i)*time.Minute))
	}
	if stats := s.Stats(); stats.WWVRecords != 48 || stats.WCYRecords != 48 || stats.WWVLosses != 0 || stats.WCYLosses != 0 {
		t.Fatalf("48 records per type should fit: %+v", stats)
	}
	s.Record("WWV", "item-48", now.Add(-24*time.Hour+48*time.Minute))
	wwv := s.Render("WWV", true, now)
	if strings.Count(wwv, "WWV received ") != 48 || strings.Contains(wwv, "item-00") || !strings.Contains(wwv, "item-01") || !strings.Contains(wwv, "item-48") || !strings.Contains(wwv, "history is incomplete") {
		t.Fatalf("unexpected oldest eviction/warning: %q", wwv)
	}
	if got := s.Render("WCY", true, now); strings.Count(got, "WCY received ") != 48 || !strings.Contains(got, "other-00") || strings.Contains(got, "history is incomplete:") {
		t.Fatalf("type isolation or inclusive cutoff: %q", got)
	}
	if got := s.Render("WCY", true, now.Add(time.Nanosecond)); strings.Contains(got, "other-00") || !strings.Contains(got, "other-01") {
		t.Fatalf("expired record retained: %q", got)
	}
	s.Record("WCY", "later", now.Add(2*time.Second))
	s.Record("WCY", "earlier", now.Add(time.Second))
	got := s.Render("WCY", true, now.Add(3*time.Second))
	if strings.Index(got, "earlier") > strings.Index(got, "later") || !strings.Contains(s.Render("WCY", false, now.Add(3*time.Second)), "later") {
		t.Fatalf("receipt order incorrect: %q", got)
	}
}

func TestRecentReportsAndLoginSelectDifferentCounts(t *testing.T) {
	s, _ := newTestStore(t, 0, 0)
	now := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 7; i++ {
		s.Record("WWV", fmt.Sprintf("report-%d", i), now.Add(time.Duration(i)*time.Second))
	}
	// Insert in reverse time order too; selection follows receipt timestamps,
	// not insertion order, and display remains oldest to newest.
	s.Record("WWV", "earlier", now.Add(-time.Second))
	when := now.Add(7 * time.Second)
	recent := s.RenderRecent("WWV", when)
	if strings.Count(recent, "WWV received ") != 5 || strings.Contains(recent, "report-0") || strings.Contains(recent, "report-1") || strings.Contains(recent, "earlier") {
		t.Fatalf("recent output should contain only the newest five: %q", recent)
	}
	last := -1
	for i := 2; i < 7; i++ {
		index := strings.Index(recent, fmt.Sprintf("report-%d", i))
		if index <= last {
			t.Fatalf("recent reports missing or out of order: %q", recent)
		}
		last = index
	}
	if login := s.Render("WWV", false, when); strings.Count(login, "WWV received ") != 1 || !strings.Contains(login, "report-6") {
		t.Fatalf("login should contain newest report: %q", login)
	}
	if full := s.Render("WWV", true, when); strings.Count(full, "WWV received ") != 8 || !strings.Contains(full, "earlier") {
		t.Fatalf("full should contain all reports: %q", full)
	}
}

func TestRecentReportsZeroThroughSix(t *testing.T) {
	cases := []struct {
		count int
		want  []int
	}{
		{0, nil},
		{1, []int{0}},
		{2, []int{0, 1}},
		{3, []int{0, 1, 2}},
		{4, []int{0, 1, 2, 3}},
		{5, []int{0, 1, 2, 3, 4}},
		{6, []int{1, 2, 3, 4, 5}},
	}
	for _, kind := range []string{"WWV", "WCY"} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%s/%d", kind, tc.count), func(t *testing.T) {
				s, _ := newTestStore(t, 0, 0)
				now := time.Now().UTC().Truncate(time.Second)
				for i := 0; i < tc.count; i++ {
					s.Record(kind, fmt.Sprintf("report-%d", i), now.Add(time.Duration(i)*time.Second))
				}
				got := s.RenderRecent(kind, now.Add(6*time.Second))
				if tc.count == 0 && !strings.Contains(got, "No "+kind+" bulletin in the last 24 hours.\n") {
					t.Fatalf("empty history did not report no data: %q", got)
				}
				var lines []string
				for _, line := range strings.Split(got, "\n") {
					if strings.HasPrefix(line, kind+" received ") {
						lines = append(lines, line)
					}
				}
				if len(lines) != len(tc.want) {
					t.Fatalf("got %d reports, want %d: %q", len(lines), len(tc.want), got)
				}
				for i, report := range tc.want {
					want := fmt.Sprintf("%s received %s UTC: report-%d", kind, now.Add(time.Duration(report)*time.Second).Format("2006-01-02 15:04:05"), report)
					if lines[i] != want {
						t.Fatalf("report %d = %q, want %q", i, lines[i], want)
					}
				}
			})
		}
	}
}

func TestEqualReceiptTimesUseInsertionOrder(t *testing.T) {
	for _, kind := range []string{"WWV", "WCY"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := newTestStore(t, 0, 0)
			now := time.Now().UTC().Truncate(time.Second)
			for i := 0; i < 6; i++ {
				s.Record(kind, fmt.Sprintf("report-%d", i), now)
			}
			for _, tc := range []struct {
				name string
				got  string
				want []int
			}{
				{"recent", s.RenderRecent(kind, now), []int{1, 2, 3, 4, 5}},
				{"full", s.Render(kind, true, now), []int{0, 1, 2, 3, 4, 5}},
				{"login", s.Render(kind, false, now), []int{5}},
			} {
				var lines []string
				for _, line := range strings.Split(tc.got, "\n") {
					if strings.HasPrefix(line, kind+" received ") {
						lines = append(lines, line)
					}
				}
				if len(lines) != len(tc.want) {
					t.Fatalf("%s: got %d reports, want %d: %q", tc.name, len(lines), len(tc.want), tc.got)
				}
				for i, report := range tc.want {
					want := fmt.Sprintf("%s received %s UTC: report-%d", kind, now.Format("2006-01-02 15:04:05"), report)
					if lines[i] != want {
						t.Fatalf("%s: report %d = %q, want %q", tc.name, i, lines[i], want)
					}
				}
			}
		})
	}
}

func TestV3SnapshotCompatibilityAnd48RecordRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	now := time.Now().UTC().Truncate(time.Second)
	var old strings.Builder
	fmt.Fprintf(&old, `{"version":1,"sequence":48,"watermark":%q,"wwv":[`, now.Format(time.RFC3339Nano))
	for i := 0; i < 24; i++ {
		if i > 0 {
			old.WriteByte(',')
		}
		fmt.Fprintf(&old, `{"time":%q,"text":%q,"sequence":%d}`, now.Add(time.Duration(i-48)*time.Minute).Format(time.RFC3339Nano), fmt.Sprintf("old-wwv-%02d", i), i+1)
	}
	old.WriteString(`],"wcy":[`)
	for i := 0; i < 24; i++ {
		if i > 0 {
			old.WriteByte(',')
		}
		fmt.Fprintf(&old, `{"time":%q,"text":%q,"sequence":%d}`, now.Add(time.Duration(i-48)*time.Minute).Format(time.RFC3339Nano), fmt.Sprintf("old-wcy-%02d", i), i+25)
	}
	old.WriteString(`],"clean":true}`)
	if err := os.WriteFile(path, []byte(old.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	s := Open(path, 0, 0)
	if stats := s.Stats(); stats.WWVRecords != 24 || stats.WCYRecords != 24 {
		t.Fatalf("old version 1 snapshot did not load: %+v", stats)
	}
	for i := 0; i < 24; i++ {
		at := now.Add(time.Duration(i-24) * time.Minute)
		s.Record("WWV", fmt.Sprintf("new-wwv-%02d", i), at)
		s.Record("WCY", fmt.Sprintf("new-wcy-%02d", i), at)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r := Open(path, 0, 0)
	defer r.Close()
	for _, kind := range []string{"WWV", "WCY"} {
		got := r.Render(kind, true, now)
		if strings.Count(got, kind+" received ") != 48 || !strings.Contains(got, "old-"+strings.ToLower(kind)+"-00") || !strings.Contains(got, "new-"+strings.ToLower(kind)+"-23") || strings.Contains(got, "history is incomplete:") {
			t.Fatalf("%s not preserved across upgrade and restart: %q", kind, got)
		}
	}
}

func TestMaximumEscapedSnapshotFitsStorageBound(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	saved := snapshot{Version: 1, Sequence: 96, Watermark: now, Clean: true}
	for i := 0; i < 48; i++ {
		saved.WWV = append(saved.WWV, Record{Time: now, Text: strings.Repeat("\x00", 256), Truncated: true, Sequence: uint64(i + 1)})
		saved.WCY = append(saved.WCY, Record{Time: now, Text: strings.Repeat("\x00", 256), Truncated: true, Sequence: uint64(i + 49)})
	}
	var hash [32]byte
	for i := range hash {
		hash[i] = 255
	}
	for i := 0; i < 4096; i++ {
		saved.Identities = append(saved.Identities, identity{Kind: "WWV", Hash: hash, Time: now})
	}
	encoded, err := json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) >= 2<<20 {
		t.Fatalf("maximum escaped snapshot has %d bytes", len(encoded))
	}
	decoded, err := decodeSnapshot(encoded)
	if err != nil || len(decoded.WWV) != 48 || len(decoded.WCY) != 48 || len(decoded.Identities) != 4096 {
		t.Fatalf("maximum escaped snapshot did not decode: %v", err)
	}
}

func TestHistoryDedupeWindowAndRestart(t *testing.T) {
	s, path := newTestStore(t, 48*time.Hour, 8)
	now := time.Now().UTC().Truncate(time.Second)
	s.Record("WWV", "same", now)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := Open(path, 48*time.Hour, 8)
	defer reopened.Close()
	reopened.Record("WWV", "same", now.Add(time.Hour))
	if got := reopened.Stats().Duplicates; got != 1 {
		t.Fatalf("clean restart lost dedupe identity: %d", got)
	}
	reopened.Record("WWV", "same", now.Add(48*time.Hour))
	if got := reopened.Stats().Duplicates; got != 2 {
		t.Fatalf("exact window must suppress: %d", got)
	}
	reopened.Record("WWV", "same", now.Add(48*time.Hour+time.Nanosecond))
	if got := reopened.Stats().Duplicates; got != 2 {
		t.Fatalf("beyond-window duplicate unexpectedly suppressed: %d", got)
	}
}

func TestHistoryCleanRestartAndCorruptSnapshot(t *testing.T) {
	s, path := newTestStore(t, 10*time.Minute, 8)
	now := time.Now().UTC().Truncate(time.Second)
	s.Record("WWV", "prior WWV", now)
	s.Record("WCY", "prior WCY", now.Add(time.Second))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r := Open(path, 10*time.Minute, 8)
	if got := r.Render("WWV", true, now.Add(2*time.Second)); !strings.Contains(got, "prior WWV") || strings.Contains(got, "prior WCY") {
		t.Fatalf("restart returned wrong history: %q", got)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"wwv":`), 0o600); err != nil {
		t.Fatal(err)
	}
	broken := Open(path, 10*time.Minute, 8)
	defer broken.Close()
	if got := broken.Render("WWV", true, now.Add(2*time.Second)); !strings.Contains(got, "may be incomplete") {
		t.Fatalf("corrupt file did not warn: %q", got)
	}
}

func TestHistoryFailureAndRecovery(t *testing.T) {
	dir := t.TempDir()
	blocked := filepath.Join(dir, "block")
	if err := os.WriteFile(blocked, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := Open(filepath.Join(blocked, "history.json"), time.Minute, 8)
	defer s.Close()
	now := time.Now().UTC()
	s.Record("WWV", "still available", now)
	if got := s.Render("WWV", false, now); !strings.Contains(got, "still available") || !strings.Contains(got, "incomplete") {
		t.Fatalf("storage fault impaired memory or warning: %q", got)
	}
	if err := os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(blocked, 0o700); err != nil {
		t.Fatal(err)
	}
	// A new dirty signal triggers immediate retry without waiting for the timer.
	s.Record("WCY", "retry", now)
	if err := s.Close(); err != nil {
		t.Fatalf("recovered final write: %v", err)
	}
	r := Open(filepath.Join(blocked, "history.json"), time.Minute, 8)
	defer r.Close()
	if got := r.Render("WWV", false, now); !strings.Contains(got, "still available") {
		t.Fatalf("failed recovery: %q", got)
	}
}

func TestHistoryConcurrentChurnBounded(t *testing.T) {
	s, _ := newTestStore(t, time.Hour, 12)
	now := time.Now().UTC()
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				s.Record("WWV", fmt.Sprintf("%d-%d", worker, i), now.Add(time.Duration(i)*time.Millisecond))
				_ = s.Render("WWV", true, now.Add(time.Second))
			}
		}(worker)
	}
	wg.Wait()
	stats := s.Stats()
	if stats.WWVRecords != 48 || stats.DedupeEntries > 12 || stats.WWVLosses == 0 || stats.DedupeEvictions == 0 {
		t.Fatalf("unbounded or missing diagnostics: %+v", stats)
	}
}

func TestDelayedDuplicatesAndRestart(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	cases := []struct {
		name    string
		window  time.Duration
		first   time.Time
		delayed time.Time
	}{
		{"ten_minute_window", 10 * time.Minute, now.Add(-11 * time.Minute), now.Add(-20 * time.Minute)},
		{"more_than_day_window", 48 * time.Hour, now.Add(-50 * time.Hour), now.Add(-23 * time.Hour)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "history.json")
			s := Open(path, tc.window, 8)
			s.Record("WWV", "identical", tc.first)
			// Move the watermark past the ordinary dedupe cutoff before the
			// older receipt arrives on the delayed feed.
			s.Record("WWV", "unrelated", now)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			r := Open(path, tc.window, 8)
			defer r.Close()
			r.Record("WWV", "identical", tc.delayed)
			if r.Stats().Duplicates != 1 {
				t.Fatalf("delayed duplicate not suppressed after restart: %+v", r.Stats())
			}
		})
	}
}

func TestCorruptFutureWatermarkAndLossDoNotBlockNewData(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	for _, tc := range []struct {
		name         string
		makeSnapshot func() snapshot
	}{
		{"watermark", func() snapshot { return snapshot{Version: 1, Watermark: now.Add(48 * time.Hour)} }},
		{"wwv_loss", func() snapshot { return snapshot{Version: 1, Watermark: now, WWVLossAt: now.Add(48 * time.Hour)} }},
		{"wcy_loss", func() snapshot { return snapshot{Version: 1, Watermark: now, WCYLossAt: now.Add(48 * time.Hour)} }},
		{"loss_after_watermark", func() snapshot { return snapshot{Version: 1, Watermark: now.Add(-time.Hour), WWVLossAt: now} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "history.json")
			encoded, err := json.Marshal(tc.makeSnapshot())
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, encoded, 0o600); err != nil {
				t.Fatal(err)
			}
			s := Open(path, time.Minute, 8)
			defer s.Close()
			s.Record("WWV", "fresh", now)
			got := s.Render("WWV", true, now)
			if !strings.Contains(got, "fresh") || !strings.Contains(got, "may be incomplete") {
				t.Fatalf("corrupt timestamp blocked data or concealed uncertainty: %q", got)
			}
		})
	}
}

func TestStatsExpiresIdleHistoryAndKeepsLossExpiredAfterClockRollback(t *testing.T) {
	s, path := newTestStore(t, 10*time.Minute, 8)
	now := time.Now().UTC().Truncate(time.Second)
	s.Record("WWV", "old bulletin", now.Add(-25*time.Hour))
	s.ReportLoss("WWV", now.Add(-25*time.Hour))
	s.mu.Lock()
	before := len(s.state.WWV)
	s.mu.Unlock()
	if before != 1 {
		t.Fatalf("fixture did not insert old record: %d", before)
	}
	if stats := s.Stats(); stats.WWVRecords != 0 {
		t.Fatalf("idle expired record still counted: %+v", stats)
	}
	// A backward clock must not revive a loss which the watermark expired.
	if got := s.Render("WWV", true, now.Add(-2*time.Hour)); strings.Contains(got, "history is incomplete:") {
		t.Fatalf("expired loss revived after clock rollback: %q", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r := Open(path, 10*time.Minute, 8)
	defer r.Close()
	if got := r.Render("WWV", true, now.Add(-2*time.Hour)); strings.Contains(got, "old bulletin") || strings.Contains(got, "history is incomplete:") {
		t.Fatalf("expired state returned after restart: %q", got)
	}
}
