package ui

import (
	"strings"
	"testing"

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
