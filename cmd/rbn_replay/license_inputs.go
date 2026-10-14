// File role: Owns frozen, local-only license inputs for one replay run.
// Source files are stream-copied and hashed before consumers open them. Copies,
// SQL handles, allowlists, and cached authority have the same run lifetime;
// cleanup clears global consumers before removing the owned directory.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dxcluster/uls"
)

type replayLicenseInput struct {
	SourcePath         string `json:"source_path,omitempty"`
	SHA256             string `json:"sha256,omitempty"`
	Bytes              int64  `json:"bytes,omitempty"`
	EnforcementEnabled bool   `json:"enforcement_enabled"`
	// Availability describes the actual frozen authority, independently of
	// enforcement. Confirmed absence is a per-call lookup result, never this
	// source-wide readiness status.
	Availability string `json:"availability"`
}

type replayLicenseInputs struct {
	FCC             replayLicenseInput `json:"fcc"`
	ISED            replayLicenseInput `json:"ised"`
	Allowlist       replayLicenseInput `json:"allowlist"`
	EligibilityTime string             `json:"eligibility_time"`
	CacheTime       string             `json:"cache_time"`
}

// resetReplayLicenseAuthority runs only after offline consumers have stopped.
// Empty paths prevent reopen; generation resets detach late/cache facts, and
// clearing policy prevents a following same-process run from inheriting it.
func resetReplayLicenseAuthority() {
	uls.CloseLicenseDatabases()
	uls.SetAllowlistPath("")
	uls.SetLicenseChecksEnabled(false)
	uls.SetCanadianLicenseChecksEnabled(false)
	uls.SetRefreshInProgress(false)
	uls.SetCanadianRefreshInProgress(false)
	uls.SetLicenseCacheTTL(0)
}

func (r *replayRunner) closeLicenseInputs() error {
	if !r.ownsLicenseInputs {
		return nil
	}
	resetReplayLicenseAuthority()
	dir := r.licenseInputsDir
	if dir != "" {
		// Verify the exact owned directory before recursive Windows cleanup.
		// A failed deletion retains ownership so a later close can retry.
		root := r.outDir
		if root == "" {
			root = os.TempDir()
		}
		rootAbs, rootErr := filepath.Abs(root)
		dirAbs, dirErr := filepath.Abs(dir)
		if err := errors.Join(rootErr, dirErr); err != nil {
			return err
		}
		if !strings.EqualFold(filepath.Dir(dirAbs), rootAbs) || !strings.HasPrefix(filepath.Base(dirAbs), ".license-inputs-") {
			return fmt.Errorf("refuse cleanup outside frozen license input owner: %s", dirAbs)
		}
		if err := os.RemoveAll(dirAbs); err != nil {
			return fmt.Errorf("remove frozen license inputs %s: %w", dir, err)
		}
	}
	r.ownsLicenseInputs = false
	r.licenseInputsDir = ""
	return nil
}

// freezeLicenseInputs preserves existing enabled/missing setup errors. A
// disabled missing database stays unavailable; a present disabled database is
// still frozen because factual lookup is independent of enforcement.
func (r *replayRunner) freezeLicenseInputs() error {
	if err := r.closeLicenseInputs(); err != nil {
		return err
	}
	r.ownsLicenseInputs = true
	resetReplayLicenseAuthority()
	r.licenseInputs = replayLicenseInputs{
		EligibilityTime: "historical_utc_at_each_check_including_release",
		CacheTime:       "process_time",
	}
	var err error
	r.licenseInputsDir, err = os.MkdirTemp(r.outDir, ".license-inputs-")
	if err != nil {
		return fmt.Errorf("create frozen license inputs: %w", err)
	}

	fcc, fccPath, err := r.freezeLicenseFile(r.cfg.FCCULS.DBPath, "fcc.db", "fcc_uls.db_path", r.cfg.FCCULS.Enabled)
	if err != nil {
		return err
	}
	fcc.EnforcementEnabled = r.cfg.FCCULS.Enabled
	r.licenseInputs.FCC = fcc
	ised, isedPath, err := r.freezeLicenseFile(r.cfg.ISED.DBPath, "ised.db", "ised.db_path", r.cfg.ISED.Enabled)
	if err != nil {
		return err
	}
	ised.EnforcementEnabled = r.cfg.ISED.Enabled
	r.licenseInputs.ISED = ised
	allowlistPath := strings.TrimSpace(r.cfg.FCCULS.AllowlistPath)
	al, frozenAllowlist, err := r.freezeLicenseFile(allowlistPath, "allowlist.txt", "fcc_uls.allowlist_path", allowlistPath != "")
	if err != nil {
		return err
	}
	if frozenAllowlist != "" {
		// Preserve runtime warning/fail-open parsing: malformed bytes supply no
		// exceptions. Their identity is still recorded, with unavailable parsed
		// authority rather than claiming that configured exceptions were loaded.
		if _, err := uls.LoadAllowlistFromFile(frozenAllowlist); err != nil {
			al.Availability = "unavailable"
		}
	}
	r.licenseInputs.Allowlist = al
	uls.SetLicenseCacheTTL(time.Duration(r.cfg.FCCULS.CacheTTLSeconds) * time.Second)
	uls.SetLicenseChecksEnabled(r.cfg.FCCULS.Enabled)
	uls.SetCanadianLicenseChecksEnabled(r.cfg.ISED.Enabled)
	uls.SetLicenseDBPath(fccPath)
	uls.SetCanadianLicenseDBPath(isedPath)
	uls.SetAllowlistPath(frozenAllowlist)
	if fccPath != "" {
		r.licenseInputs.FCC.Availability = "unavailable"
		if uls.LicenseDatabaseAvailableForADIF(291) {
			r.licenseInputs.FCC.Availability = "available"
		}
	}
	if isedPath != "" {
		r.licenseInputs.ISED.Availability = "unavailable"
		if uls.LicenseDatabaseAvailableForADIF(1) {
			r.licenseInputs.ISED.Availability = "available"
		}
	}
	return nil
}

func (r *replayRunner) freezeLicenseFile(path, name, setting string, required bool) (replayLicenseInput, string, error) {
	path = strings.TrimSpace(path)
	input := replayLicenseInput{Availability: "not_configured"}
	if path == "" {
		if required {
			return input, "", fmt.Errorf("%s is empty for enabled replay (config=%s)", setting, r.cfg.LoadedFrom)
		}
		return input, "", nil
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return input, "", fmt.Errorf("resolve %s %s: %w", setting, path, err)
	}
	input.SourcePath = abs
	source, err := os.Open(abs)
	if err != nil {
		input.Availability = "unavailable"
		if !required && errors.Is(err, os.ErrNotExist) {
			return input, "", nil
		}
		return input, "", fmt.Errorf("%s missing/unreadable %s: %w", setting, abs, err)
	}
	destination := filepath.Join(r.licenseInputsDir, name)
	copyErr := copyFrozenLicenseFile(source, destination, &input)
	closeErr := source.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return input, "", fmt.Errorf("freeze %s %s: %w", setting, abs, err)
	}
	input.Availability = "available"
	return input, destination, nil
}

// No whole-database byte slice is retained. The closed snapshot's bytes are the
// hashed bytes; replacement of the original pathname cannot change the copy.
func copyFrozenLicenseFile(source *os.File, destination string, input *replayLicenseInput) error {
	info, err := source.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("license input is not a regular file")
	}
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(out, hash), source)
	closeErr := out.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return err
	}
	input.Bytes, input.SHA256 = n, hex.EncodeToString(hash.Sum(nil))
	return nil
}
