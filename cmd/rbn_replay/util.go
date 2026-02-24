package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	envConfigPath     = "DXC_CONFIG_PATH"
	defaultConfigPath = "data/config"
)

func must(err error) {
	if err != nil {
		fatalf("%v", err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func writeJSONAtomic(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func ensureDir(path string) error {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" {
		return errors.New("empty path")
	}
	return os.MkdirAll(path, 0o755)
}

func parseUTCDate(value string) (time.Time, string, error) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return time.Time{}, "", errors.New("date is empty")
	}
	var day time.Time
	var err error
	switch {
	case len(raw) == len("2006-01-02"):
		day, err = time.ParseInLocation("2006-01-02", raw, time.UTC)
	case len(raw) == len("20060102"):
		day, err = time.ParseInLocation("20060102", raw, time.UTC)
	default:
		return time.Time{}, "", fmt.Errorf("invalid date %q: expected YYYY-MM-DD or YYYYMMDD", raw)
	}
	if err != nil {
		return time.Time{}, "", fmt.Errorf("parse date %q: %w", raw, err)
	}
	day = time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
	return day, day.Format("20060102"), nil
}
