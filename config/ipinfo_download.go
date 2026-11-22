package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	ipinfoTokenFilePrefix = "file:"
	ipinfoTokenFileKey    = "IPINFO_DOWNLOAD_TOKEN"
	maxIPInfoTokenFile    = 4 << 10
)

var ipinfoFileTokenPattern = regexp.MustCompile(`^[A-Za-z0-9._~-]+$`)

// Keep the effective download setting boolean for the runtime. Auto opts in
// only after the configured local file supplies a usable credential.
func prepareIPInfoDownloadMode(raw map[string]any) bool {
	reputation, ok := raw["reputation"].(map[string]any)
	if !ok {
		return false
	}
	mode, ok := reputation["ipinfo_download_enabled"].(string)
	if !ok || mode != "auto" {
		return false
	}
	reputation["ipinfo_download_enabled"] = true
	return true
}

// Resolve the file at startup, after YAML validation, so diagnostics from the
// shared decoder never receive material read from the token file.
func resolveIPInfoDownloadToken(cfg *Config, automatic bool, diagnostics *LoadDiagnostics) {
	reputation := &cfg.Reputation
	value := strings.TrimSpace(reputation.IPInfoDownloadToken)
	name, fromFile := strings.CutPrefix(value, ipinfoTokenFilePrefix)
	if !reputation.Enabled || !reputation.IPInfoDownloadEnabled {
		if fromFile || automatic {
			reputation.IPInfoDownloadEnabled = false
			reputation.IPInfoDownloadToken = ""
		}
		return
	}
	if !automatic && !fromFile {
		return
	}

	reputation.IPInfoDownloadEnabled = false
	reputation.IPInfoDownloadToken = ""
	name = strings.TrimSpace(name)
	if !fromFile || name == "" {
		diagnostics.addWarning("IPinfo download requires a nonempty file: reference in ipinfo_download_token")
		return
	}
	if !filepath.IsAbs(name) {
		name = filepath.Join(cfg.LoadedFrom, name)
	}
	token, err := readIPInfoDownloadToken(name)
	if err != nil {
		if !automatic || !errors.Is(err, os.ErrNotExist) {
			diagnostics.addWarning(fmt.Sprintf("IPinfo download disabled for credential file %q: %s", name, err))
		}
		return
	}
	reputation.IPInfoDownloadEnabled = true
	reputation.IPInfoDownloadToken = token
}

// The optional file contains assignments for this feature only. Read a bounded
// regular file and keep its contents out of errors.
func readIPInfoDownloadToken(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", os.ErrNotExist
		}
		return "", errors.New("could not inspect the file")
	}
	if !info.Mode().IsRegular() || info.Size() > maxIPInfoTokenFile {
		return "", errors.New("expected a regular file no larger than 4 KiB")
	}
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", os.ErrNotExist
		}
		return "", errors.New("could not open the file")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxIPInfoTokenFile+1))
	if err != nil || len(data) > maxIPInfoTokenFile {
		return "", errors.New("could not read the file within the 4 KiB limit")
	}

	var token string
	for line := range strings.SplitSeq(strings.TrimPrefix(string(data), "\ufeff"), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		value = strings.TrimSpace(value)
		if !ok || strings.TrimSpace(key) != ipinfoTokenFileKey ||
			token != "" || !ipinfoFileTokenPattern.MatchString(value) {
			return "", errors.New("expected one unquoted IPINFO_DOWNLOAD_TOKEN assignment")
		}
		token = value
	}
	if token == "" {
		return "", errors.New("IPINFO_DOWNLOAD_TOKEN is missing")
	}
	return token, nil
}
