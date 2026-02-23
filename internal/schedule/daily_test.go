package schedule

import (
	"testing"
	"time"
)

func TestParseUTCHourMinuteStrict(t *testing.T) {
	t.Parallel()

	hour, minute := ParseUTCHourMinute(" 09:17 ", 1, 2, ParseOptions{})
	if hour != 9 || minute != 17 {
		t.Fatalf("expected 09:17, got %02d:%02d", hour, minute)
	}

	fallbackHour, fallbackMinute := ParseUTCHourMinute("daily@09:17Z", 1, 2, ParseOptions{})
	if fallbackHour != 1 || fallbackMinute != 2 {
		t.Fatalf("expected fallback 01:02 for strict parse, got %02d:%02d", fallbackHour, fallbackMinute)
	}
}

func TestParseUTCHourMinuteDailyOptions(t *testing.T) {
	t.Parallel()

	opts := ParseOptions{AllowDailyPrefix: true, AllowTrailingZ: true}
	hour, minute := ParseUTCHourMinute("Daily@09:17z", 1, 2, opts)
	if hour != 9 || minute != 17 {
		t.Fatalf("expected 09:17, got %02d:%02d", hour, minute)
	}

	fallbackHour, fallbackMinute := ParseUTCHourMinute("invalid", 1, 2, opts)
	if fallbackHour != 1 || fallbackMinute != 2 {
		t.Fatalf("expected fallback 01:02 for invalid parse, got %02d:%02d", fallbackHour, fallbackMinute)
	}
}

func TestNextAtUTC(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	delay := NextAtUTC(now, 3, 5)
	if delay != 55*time.Second {
		t.Fatalf("expected 55s delay, got %s", delay)
	}

	wrapDelay := NextAtUTC(now, 3, 4)
	expected := 23*time.Hour + 59*time.Minute + 55*time.Second
	if wrapDelay != expected {
		t.Fatalf("expected wrapped delay %s, got %s", expected, wrapDelay)
	}
}

func TestNextDailyUTCUsesFallback(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 2, 1, 0, 0, 0, time.UTC)
	delay := NextDailyUTC("bad", now, 2, 15, ParseOptions{})
	expected := 1*time.Hour + 15*time.Minute
	if delay != expected {
		t.Fatalf("expected delay %s, got %s", expected, delay)
	}
}
