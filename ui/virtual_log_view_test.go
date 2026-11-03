package ui

import (
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/gdamore/tcell/v2"
)

func TestVirtualLogViewMaintainsBoundedHistory(t *testing.T) {
	v := newVirtualLogView("Events", 3, false)
	v.Append("one")
	v.Append("two")
	v.Append("three")
	v.Append("four")

	got := v.SnapshotText()
	if strings.Contains(got, "one") {
		t.Fatalf("expected oldest line to be evicted, got %q", got)
	}
	for _, line := range []string{"two", "three", "four", "... +1 more"} {
		if !strings.Contains(got, line) {
			t.Fatalf("expected %q in snapshot, got %q", line, got)
		}
	}
}

func TestVirtualLogViewScrollDeterministic(t *testing.T) {
	v := newVirtualLogView("Events", 8, false)
	v.SetRect(0, 0, 40, 5)
	for i := 0; i < 8; i++ {
		v.Append("line")
	}

	if !v.HandleScroll(tcell.NewEventKey(tcell.KeyHome, 0, tcell.ModNone)) {
		t.Fatalf("expected home key to be handled")
	}
	v.mu.Lock()
	homeOffset := v.offset
	v.mu.Unlock()
	if homeOffset != 0 {
		t.Fatalf("expected home offset 0, got %d", homeOffset)
	}

	if !v.HandleScroll(tcell.NewEventKey(tcell.KeyEnd, 0, tcell.ModNone)) {
		t.Fatalf("expected end key to be handled")
	}
	v.mu.Lock()
	endOffset := v.offset
	v.mu.Unlock()
	if endOffset == 0 {
		t.Fatalf("expected non-zero end offset with overflow")
	}

	if !v.HandleScroll(tcell.NewEventKey(tcell.KeyRune, 'k', tcell.ModNone)) {
		t.Fatalf("expected k-scroll to be handled")
	}
	v.mu.Lock()
	upOffset := v.offset
	v.mu.Unlock()
	if upOffset >= endOffset {
		t.Fatalf("expected k-scroll to move up, end=%d current=%d", endOffset, upOffset)
	}
}

func TestVirtualLogViewEnforcesMessageByteLimit(t *testing.T) {
	v := newVirtualLogViewWithOptions("Events", virtualLogOptions{
		MaxLines:         4,
		MaxBytes:         128,
		MaxMessageBytes:  16,
		EvictOnByteLimit: true,
	}, false)

	v.Append("abcdefghijklmnopqrstuvwxyz")

	got := v.SnapshotText()
	if strings.Contains(got, "abcdefghijklmnopqrstuvwxyz") {
		t.Fatalf("expected long message to be truncated, got %q", got)
	}
	v.mu.Lock()
	truncated := v.truncated
	bytes := v.bytes
	v.mu.Unlock()
	if truncated == 0 {
		t.Fatalf("expected truncation counter to advance")
	}
	if bytes > 16 {
		t.Fatalf("expected retained bytes <= 16, got %d", bytes)
	}
}

func TestVirtualLogViewTruncatedRowsDetachBacking(t *testing.T) {
	largeASCII := strings.Repeat("a", 8192)
	largeUnicode := "é" + strings.Repeat("b", 8192)
	for _, tc := range []struct {
		name  string
		input string
		limit int
		want  string
	}{
		{name: "one byte", input: largeASCII, limit: 1, want: "a"},
		{name: "suffix boundary", input: largeASCII, limit: 15, want: strings.Repeat("a", 15)},
		{name: "with suffix", input: largeASCII, limit: 16, want: "a... [truncated]"},
		{name: "default", input: largeASCII, limit: 4096, want: strings.Repeat("a", 4081) + "... [truncated]"},
		{name: "partial rune produces empty row", input: largeUnicode, limit: 1, want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := newVirtualLogViewWithOptions("Events", virtualLogOptions{
				MaxLines: 1, MaxMessageBytes: tc.limit,
			}, false)
			v.Append(tc.input)
			got := v.lines[v.head]
			if got != tc.want || v.bytes != len(tc.want) || v.truncated != 1 || v.count != 1 {
				t.Fatalf("row=%q bytes=%d truncated=%d count=%d", got, v.bytes, v.truncated, v.count)
			}
			if unsafe.StringData(got) == unsafe.StringData(tc.input) {
				t.Fatal("truncated row retains the input backing")
			}
		})
	}
}

func TestVirtualLogViewEvictAndResetClearEmptyBacking(t *testing.T) {
	input := strings.Repeat("x", 8192)
	v := newVirtualLogView("Events", 2, false)
	// An empty substring can still point at a much larger allocation.
	v.Append(input[:0])
	if unsafe.StringData(v.lines[0]) != unsafe.StringData(input) {
		t.Fatal("test setup did not retain the empty substring backing")
	}
	v.evictOldestLocked()
	if unsafe.StringData(v.lines[0]) == unsafe.StringData(input) || v.count != 0 || v.bytes != 0 {
		t.Fatal("eviction retained the empty substring backing")
	}
	v.Append(input[:0])
	v.Reset(nil)
	for _, row := range v.lines {
		if unsafe.StringData(row) == unsafe.StringData(input) {
			t.Fatal("reset retained the empty substring backing")
		}
	}
	if v.count != 0 || v.bytes != 0 || v.total != 0 || v.truncated != 0 {
		t.Fatalf("reset left state count=%d bytes=%d total=%d truncated=%d", v.count, v.bytes, v.total, v.truncated)
	}
}

func TestVirtualLogViewTruncatedRowChurn(t *testing.T) {
	v := newVirtualLogViewWithOptions("Events", virtualLogOptions{
		MaxLines: 2, MaxBytes: 2, MaxMessageBytes: 1, EvictOnByteLimit: true,
	}, false)
	for i := 0; i < 100; i++ {
		input := strings.Repeat("x", 8192)
		v.Append(input)
		row := v.lines[(v.head+v.count-1)%v.max]
		if row != "x" || unsafe.StringData(row) == unsafe.StringData(input) {
			t.Fatalf("iteration %d retained input backing or changed row", i)
		}
		if v.count > 2 || v.bytes > 2 {
			t.Fatalf("iteration %d: count=%d bytes=%d", i, v.count, v.bytes)
		}
	}
	if v.truncated != 100 || v.total != 100 || v.count != 2 || v.bytes != 2 {
		t.Fatalf("after churn: truncated=%d total=%d count=%d bytes=%d", v.truncated, v.total, v.count, v.bytes)
	}
}

func TestVirtualLogViewSequentialLimitsKeepBothTruncationCounts(t *testing.T) {
	v := newVirtualLogViewWithOptions("Events", virtualLogOptions{
		MaxLines: 1, MaxBytes: 1, MaxMessageBytes: 16, EvictOnByteLimit: true,
	}, false)
	v.Append(strings.Repeat("a", 8192))
	if got := v.lines[v.head]; got != "a" || v.bytes != 1 || v.truncated != 2 {
		t.Fatalf("row=%q bytes=%d truncated=%d", got, v.bytes, v.truncated)
	}
}

func TestVirtualLogViewConcurrentTruncationAndReset(t *testing.T) {
	v := newVirtualLogViewWithOptions("Events", virtualLogOptions{
		MaxLines: 4, MaxBytes: 4, MaxMessageBytes: 1, EvictOnByteLimit: true,
	}, false)
	input := strings.Repeat("x", 8192)
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 100; j++ {
				v.Append(input)
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		for i := 0; i < 100; i++ {
			v.Reset(nil)
		}
	}()
	workers.Wait()
	if v.count > 4 || v.bytes > 4 {
		t.Fatalf("after concurrent activity: count=%d bytes=%d", v.count, v.bytes)
	}
	for _, row := range v.lines {
		if row != "" && (row != "x" || unsafe.StringData(row) == unsafe.StringData(input)) {
			t.Fatal("retained row changed or holds the input backing")
		}
	}
}

func TestVirtualLogViewEnforcesByteLimitByEvictingOldest(t *testing.T) {
	v := newVirtualLogViewWithOptions("Events", virtualLogOptions{
		MaxLines:         10,
		MaxBytes:         len("bravo") + len("charlie"),
		EvictOnByteLimit: true,
	}, false)

	v.Append("alpha")
	v.Append("bravo")
	v.Append("charlie")

	got := v.SnapshotText()
	if strings.Contains(got, "alpha") {
		t.Fatalf("expected oldest line to be evicted by byte cap, got %q", got)
	}
	for _, want := range []string{"bravo", "charlie"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected retained line %q, got %q", want, got)
		}
	}
}

func TestVirtualLogViewDropsWhenByteLimitCannotEvict(t *testing.T) {
	v := newVirtualLogViewWithOptions("Events", virtualLogOptions{
		MaxLines:         10,
		MaxBytes:         len("alpha"),
		EvictOnByteLimit: false,
	}, false)

	v.Append("alpha")
	v.Append("bravo")

	got := v.SnapshotText()
	if !strings.Contains(got, "alpha") || strings.Contains(got, "bravo") {
		t.Fatalf("expected second line to be dropped without eviction, got %q", got)
	}
	v.mu.Lock()
	dropped := v.dropped
	v.mu.Unlock()
	if dropped == 0 {
		t.Fatalf("expected drop counter to advance")
	}
}
