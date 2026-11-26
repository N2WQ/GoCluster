package archive

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"dxcluster/spot"
)

// These fixtures are native value layouts. Their big-endian scalar headers
// are 28 bytes (v2) or 36 bytes followed by 13/13/14/17/19/21 uint16 lengths
// and concatenated field bytes, independent of today's archive writer.
func nativeHistoryFixture(t *testing.T, version int) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", fmt.Sprintf("history-v%d.bin", version)))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// Successful Match calls in the ordinary API may retain every pointer, even
// on rejection. Borrowing is independent of result collection and preserves
// every selected value after later candidates, additional scans and DB close.
func TestHistoryCandidateOwnership(t *testing.T) {
	w := historyTestWriter(t, 60)
	now := time.Now().UTC()
	base := now.Add(-time.Second).UnixNano()
	for i, version := range []int{3, 4, 5, 6, 2, 7} {
		putHistoryRow(t, w, spotKeyBytes(base+int64(i), 0), nativeHistoryFixture(t, version))
	}
	bad := append(nativeHistoryFixture(t, 7), 0)
	putHistoryRow(t, w, spotKeyBytes(base+4, 1), bad)
	selected := func(s *spot.Spot) bool { return (s.Time.UnixNano()-base)%2 == 0 }
	request := HistoryRequest{Limit: 10, Now: now}
	var retained []*spot.Spot
	request.Match = func(s *spot.Spot) bool {
		retained = append(retained, s)
		return selected(s)
	}
	owned, err := w.ReadHistoryPage(request)
	if err != nil || owned.End != HistoryExhausted || owned.Before != nil || owned.Examined != 7 ||
		owned.Selected != 3 || len(owned.Spots) != 3 || owned.Unreadable != 1 || len(retained) != 6 {
		t.Fatalf("owned page=%+v retained=%d err=%v", owned, len(retained), err)
	}
	request.BorrowMatch = true
	var firstRejected uintptr
	request.Match = func(s *spot.Spot) bool {
		address := reflect.ValueOf(s).Pointer()
		switch s.Time.UnixNano() - base {
		case 5:
			firstRejected = address
		case 4:
			if address != firstRejected {
				t.Fatal("malformed candidate discarded reusable storage")
			}
		}
		return selected(s)
	}
	borrowed, err := w.ReadHistoryPage(request)
	if err != nil || !reflect.DeepEqual(borrowed, owned) {
		t.Fatalf("borrowed page differs: got %+v want %+v err=%v", borrowed, owned, err)
	}
	var collected []*spot.Spot
	request.Collect = func(s *spot.Spot) error {
		collected = append(collected, s.Clone())
		return nil
	}
	page, err := w.ReadHistoryPage(request)
	if err != nil || page.Spots != nil || page.Selected != 3 || page.Examined != 7 ||
		page.End != HistoryExhausted || page.Before != nil || page.Unreadable != 1 || len(collected) != 3 {
		t.Fatalf("collected page=%+v rows=%d err=%v", page, len(collected), err)
	}
	w.Stop()
	for i, s := range retained {
		if s.Time.UnixNano() != base+int64(5-i) {
			t.Fatalf("ordinary matcher lost row %d after later reads", i)
		}
	}
	for i, s := range borrowed.Spots {
		if s.Time.UnixNano() != base+int64(4-2*i) || !reflect.DeepEqual(s, owned.Spots[i]) ||
			!reflect.DeepEqual(s.Clone(), collected[i]) {
			t.Fatalf("selected row %d changed after scan or database close", i)
		}
	}
}

func TestCollectHistoryNativeVersions(t *testing.T) {
	w := historyTestWriter(t, 60)
	now := time.Now().UTC()
	for v := 2; v <= 7; v++ {
		putHistoryRow(t, w, spotKeyBytes(now.UnixNano(), uint32(v)), nativeHistoryFixture(t, v))
	}
	calls := 0
	request := HistoryRequest{Limit: 50, Now: now, Match: func(s *spot.Spot) bool {
		return s.DXCall == "K1ABC" && s.Frequency == 14030.25
	}, Collect: func(s *spot.Spot) error {
		v := 7 - calls // same timestamp, newest sequence first
		calls++
		wantObserved := 14030.125
		if v == 2 {
			wantObserved = 14030.25
		}
		wantEvents := spot.EventSOTA
		if v <= 3 {
			wantEvents = spot.EventPOTA
		}
		if s.Events != wantEvents || s.ObservedFrequency != wantObserved || s.Comment != `POTA-1234 "trail"` ||
			s.SourceType != spot.SourceRBN || s.Report != -9 || !s.HasReport {
			t.Fatalf("v%d lost source fields: %+v", v, s)
		}
		if (s.DXMetadata.State == "CT" && s.DEMetadata.State == "MA") != (v >= 6) ||
			(s.ToxicityStatus == spot.ToxicitySafe) != (v >= 5) ||
			s.CWDisplayAvailable != (v == 7) {
			t.Fatalf("v%d wrong version availability: %+v", v, s)
		}
		if v == 7 {
			if s.CWDisplayComment != `CQ "trail"` || !slices.Equal(s.CWSpeeds, []string{"30 WPM", "34 WPM"}) {
				t.Fatalf("v7 CW projection = %q %q", s.CWDisplayComment, s.CWSpeeds)
			}
		} else if len(s.CWSpeeds) != 0 || s.CWDisplayComment != "" {
			t.Fatalf("v%d acquired CW metadata", v)
		}
		return nil
	}}
	page, err := w.ReadHistoryPage(request)
	if err != nil || page.Selected != 6 || calls != 6 || page.Spots != nil ||
		page.Before != nil || page.Examined != 6 || page.Unreadable != 0 || page.End != HistoryExhausted {
		t.Fatalf("page=%+v calls=%d err=%v", page, calls, err)
	}
}

func TestCollectHistoryBudgetLeavesStartingPosition(t *testing.T) {
	w := historyTestWriter(t, 60)
	now := time.Now().UTC()
	for i := 1; i <= 5; i++ {
		putHistoryRow(t, w, spotKeyBytes(now.UnixNano(), uint32(i)), historyRaw(fmt.Sprintf("K%dABC", i)))
	}
	before := spotKeyBytes(now.UnixNano(), 5)
	var received []string
	request := HistoryRequest{Limit: 2, Before: before, Now: now, Collect: func(s *spot.Spot) error {
		if len(received) == 1 {
			return ErrHistoryBusy
		}
		received = append(received, s.DXCall)
		return nil
	}}
	page, err := w.ReadHistoryPage(request)
	if !errors.Is(err, ErrHistoryBusy) || page.Selected != 0 || page.Before != nil || !slices.Equal(received, []string{"K4ABC"}) {
		t.Fatalf("refused page=%+v received=%q err=%v", page, received, err)
	}
	received = nil // the collector's caller discards the partial reservation
	request.Collect = func(s *spot.Spot) error {
		received = append(received, s.DXCall)
		return nil
	}
	page, err = w.ReadHistoryPage(request)
	if err != nil || page.Selected != 2 || page.Spots != nil || page.End != HistoryCountReached ||
		!bytes.Equal(page.Before, spotKeyBytes(now.UnixNano(), 3)) || !slices.Equal(received, []string{"K4ABC", "K3ABC"}) {
		t.Fatalf("retry page=%+v received=%q err=%v", page, received, err)
	}
}

func TestCollectHistoryScanAdmission(t *testing.T) {
	first := historyTestWriter(t, 60)
	second := historyTestWriter(t, 60)
	now := time.Now().UTC()
	for _, w := range []*Writer{first, second} {
		putHistoryRow(t, w, spotKeyBytes(now.UnixNano(), 1), historyRaw("K1ABC"))
	}
	entered := make(chan struct{}, MaxHistoryScans)
	release := make(chan struct{})
	results := make(chan error, MaxHistoryScans)
	unblock := func() {
		if release != nil {
			close(release)
			release = nil
		}
	}
	defer unblock()
	wait := release
	held := HistoryRequest{Limit: 1, Now: now, Collect: func(*spot.Spot) error {
		entered <- struct{}{}
		<-wait
		return nil
	}}
	for range MaxHistoryScans {
		go func() {
			_, err := first.ReadHistoryPage(held)
			results <- err
		}()
	}
	for range MaxHistoryScans {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("scan did not enter collector")
		}
	}
	r := HistoryRequest{Limit: 1, Now: now, Collect: func(*spot.Spot) error { return nil }}
	page, err := second.ReadHistoryPage(r)
	if !errors.Is(err, ErrHistoryBusy) || page.Selected != 0 || page.Before != nil {
		t.Fatalf("saturated second archive: page=%+v err=%v", page, err)
	}
	// A collecting request cannot consume the native text/CC11 scan path.
	page, err = second.ReadHistoryPage(HistoryRequest{Limit: 1, Now: now})
	if err != nil || len(page.Spots) != 1 {
		t.Fatalf("native history under pressure: page=%+v err=%v", page, err)
	}
	unblock()
	for range MaxHistoryScans {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	page, err = second.ReadHistoryPage(r)
	if err != nil || page.Selected != 1 {
		t.Fatalf("admission after release: page=%+v err=%v", page, err)
	}
}

func TestCollectHistoryReadableRecordCeiling(t *testing.T) {
	// Stress the structural reader envelope independently of normal feed fields.
	// A uint16 length of 65535, two actual state codes, and both 1024-byte CW
	// fields are legal together, including the padded decimal representation.
	raw := make([]byte, 78)
	raw[0] = 7
	for i := range 21 {
		field := strings.Repeat("Q", 65535)
		switch i {
		case 17, 18:
			field = "CT"
		case 19:
			field = strings.Repeat("q", 1024)
		case 20:
			field = strings.Repeat("0", 1020) + " WPM"
		}
		binary.BigEndian.PutUint16(raw[36+2*i:], uint16(len(field)))
		raw = append(raw, field...)
	}
	if len(raw) != MaxHistoryRecordBytes {
		t.Fatalf("fixture is %d bytes, claimed limit %d", len(raw), MaxHistoryRecordBytes)
	}
	if _, err := decodeRecord(raw); err != nil {
		t.Fatalf("maximal native value: %v", err)
	}
	w := historyTestWriter(t, 60)
	now := time.Now().UTC()
	putHistoryRow(t, w, spotKeyBytes(now.UnixNano(), 1), nativeHistoryFixture(t, 7))
	putHistoryRow(t, w, spotKeyBytes(now.UnixNano(), 2), append(raw, 0))
	page, err := w.ReadHistoryPage(HistoryRequest{Limit: 50, Now: now, Collect: func(s *spot.Spot) error {
		if s.DXCall != "K1ABC" {
			t.Fatalf("returned unreadable value")
		}
		return nil
	}})
	if err != nil || page.Selected != 1 || page.Unreadable != 1 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
}

// This benchmark deliberately combines independent maxima: all-field payload,
// per-space legacy EVENT tokenization, per-comma EVENT/toxicity headers, and
// Unicode normalization expansion. B/op measures allocation churn for this
// fixture; it is independent of the static simultaneous-backing bound.
func BenchmarkCollectedHistoryDecodeScratch(b *testing.B) {
	raw := make([]byte, 78)
	raw[0] = 7
	for i := range 21 {
		field := strings.Repeat("\xff", 65535)
		switch i {
		case 4:
			field = strings.Repeat("a ", 32767) + "b"
		case 13, 15:
			field = strings.Repeat(",", 65535)
		case 17, 18:
			field = "CT"
		case 19:
			field = strings.Repeat("q", 1024)
		case 20:
			field = strings.Repeat("0 WPM\x00", 169) + "000000 WPM"
		}
		binary.BigEndian.PutUint16(raw[36+i*2:], uint16(len(field)))
		raw = append(raw, field...)
	}
	b.ReportAllocs()
	if len(raw) != MaxHistoryRecordBytes {
		b.Fatalf("scratch fixture is %d bytes, want %d", len(raw), MaxHistoryRecordBytes)
	}
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		s, err := decodeSpot(1, raw)
		if err != nil || len(s.Comment) != 65535 || len(s.CWSpeeds) != 170 || s.DXMetadata.State != "CT" {
			b.Fatalf("lost a native field, err=%v", err)
		}
	}
}
