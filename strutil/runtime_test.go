package strutil

import (
	"testing"
	"time"
)

func TestFormatAge(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	if got := FormatAge(now, time.Time{}); got != "never" {
		t.Fatalf("expected never, got %q", got)
	}
	if got := FormatAge(now, now.Add(500*time.Millisecond)); got != "0s" {
		t.Fatalf("expected 0s, got %q", got)
	}
	if got := FormatAge(now, now.Add(-1500*time.Millisecond)); got != "1s" {
		t.Fatalf("expected 1s, got %q", got)
	}
}

func TestIsAllDigitsASCII(t *testing.T) {
	if IsAllDigitsASCII("") {
		t.Fatal("expected empty string to be false")
	}
	if !IsAllDigitsASCII("0123456789") {
		t.Fatal("expected digits string to be true")
	}
	if IsAllDigitsASCII("12A") {
		t.Fatal("expected mixed string to be false")
	}
}
