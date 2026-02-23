// Package schedule provides shared UTC daily scheduling helpers.
package schedule

import (
	"strings"
	"time"
)

// ParseOptions controls accepted refresh string variants.
type ParseOptions struct {
	AllowDailyPrefix bool
	AllowTrailingZ   bool
}

// ParseUTCHourMinute parses a refresh time in HH:MM UTC format.
// It returns fallbacks when the input is empty or invalid.
func ParseUTCHourMinute(value string, fallbackHour int, fallbackMinute int, opts ParseOptions) (int, int) {
	refresh := strings.TrimSpace(value)
	if refresh == "" {
		return fallbackHour, fallbackMinute
	}
	if opts.AllowDailyPrefix && strings.HasPrefix(strings.ToLower(refresh), "daily@") {
		refresh = refresh[len("daily@"):]
	}
	if opts.AllowTrailingZ {
		refresh = strings.TrimSuffix(strings.ToUpper(refresh), "Z")
	}
	parsed, err := time.Parse("15:04", refresh)
	if err != nil {
		return fallbackHour, fallbackMinute
	}
	return parsed.Hour(), parsed.Minute()
}

// NextAtUTC returns the delay until the next UTC occurrence of hour:minute.
func NextAtUTC(now time.Time, hour int, minute int) time.Duration {
	utcNow := now.UTC()
	target := time.Date(utcNow.Year(), utcNow.Month(), utcNow.Day(), hour, minute, 0, 0, time.UTC)
	if !target.After(utcNow) {
		target = target.Add(24 * time.Hour)
	}
	return target.Sub(utcNow)
}

// NextDailyUTC parses refreshUTC and returns the delay until the next run.
func NextDailyUTC(refreshUTC string, now time.Time, fallbackHour int, fallbackMinute int, opts ParseOptions) time.Duration {
	hour, minute := ParseUTCHourMinute(refreshUTC, fallbackHour, fallbackMinute, opts)
	return NextAtUTC(now, hour, minute)
}
