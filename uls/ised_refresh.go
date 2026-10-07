// File role: Publishes the assigned-call and event archives as one Canadian
// snapshot. The manifest inside the database is authoritative; HTTP sidecars
// never decide whether a partially downloaded pair has already been built.
package uls

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"dxcluster/config"
	"dxcluster/download"
)

// canadianRefreshOwner bounds source workers to one, independently of FCC.
// Waiters are cancellable; the startup owner joins every refresh before done.
var canadianRefreshOwner = make(chan struct{}, 1)

// StartCanadianBackground reconciles pending archive pairs on startup, then
// owns the shared daily timer. The caller cancels and joins done at shutdown;
// enforcement being disabled does not disable province lookup or refresh.
func StartCanadianBackground(ctx context.Context, cfg config.ISEDConfig) <-chan struct{} {
	done := make(chan struct{})
	if ctx == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		ready, _, _ := canadianPublishedPair(ctx, cfg.DBPath)
		if updated, err := RefreshCanadian(ctx, cfg, !ready); err != nil {
			log.Printf("Warning: ISED startup refresh failed: %v", err)
		} else if updated {
			log.Printf("ISED database updated")
		}
		runDailyRefresh(ctx, "ISED", cfg.RefreshUTC, func(ctx context.Context, force bool) (bool, error) { return RefreshCanadian(ctx, cfg, force) })
	}()
	return done
}

// RefreshCanadian retains fixed archive paths so both-304 retry and process
// restart can rebuild any pair not recorded in the last successful database.
// Readers retain the last-good generation during import; only the cancellable
// handle-close/rename publication interval is unavailable and fails open.
// Runtime callers supply loader-validated config, which additionally rejects
// aliases with FCC files; this direct API checks all five Canadian owned files.
func RefreshCanadian(ctx context.Context, cfg config.ISEDConfig, force bool) (updated bool, err error) {
	if ctx == nil {
		return false, errors.New("ised: nil context")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	select {
	case canadianRefreshOwner <- struct{}{}:
		defer func() { <-canadianRefreshOwner }()
	case <-ctx.Done():
		return false, ctx.Err()
	}
	if err := validateCanadianRefreshPaths(cfg); err != nil {
		return false, err
	}
	if err := downloadCanadianArchive(ctx, cfg.URL, cfg.Archive, maxISEDMainBytes, force); err != nil {
		return false, err
	}
	if err := downloadCanadianArchive(ctx, cfg.SpecialURL, cfg.SpecialArchive, maxISEDSpecialBytes, force); err != nil {
		return false, err
	}
	mainSHA, err := canadianArchiveSHA(ctx, cfg.Archive, maxISEDMainBytes)
	if err != nil {
		return false, err
	}
	specialSHA, err := canadianArchiveSHA(ctx, cfg.SpecialArchive, maxISEDSpecialBytes)
	if err != nil {
		return false, err
	}
	ready, publishedMain, publishedSpecial := canadianPublishedPair(ctx, cfg.DBPath)
	if ready && !force && mainSHA == publishedMain && specialSHA == publishedSpecial {
		return false, nil
	}
	defer func() {
		if err != nil {
			markCanadianProcessed(cfg, false)
		}
	}()
	mainDir, err := extractNamedArchiveContext(ctx, cfg.Archive, "amateur_delim.txt", cfg.TempDir, maxISEDMainBytes)
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(mainDir)
	specialDir, err := extractNamedArchiveContext(ctx, cfg.SpecialArchive, "special_callsign.txt", cfg.TempDir, maxISEDSpecialBytes)
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(specialDir)
	tmpPath, err := buildCanadianDatabase(ctx, filepath.Join(mainDir, "amateur_delim.txt"), filepath.Join(specialDir, "special_callsign.txt"), cfg.DBPath, mainSHA, specialSHA)
	if err != nil {
		return false, err
	}
	defer os.Remove(tmpPath)
	SetCanadianRefreshInProgress(true)
	defer SetCanadianRefreshInProgress(false)
	ResetCanadianLicenseDB()
	if err := replaceDBWithRetryContext(ctx, cfg.DBPath, tmpPath); err != nil {
		return false, err
	}
	SetCanadianLicenseDBPath(cfg.DBPath)
	markCanadianProcessed(cfg, true)
	stats := CanadianLookupStats()
	log.Printf("ISED published: schema=%d cache_generation=%d cache_entries=%d cache_cap=%d", CanadianSchemaVersion, stats.Generation, stats.Entries, stats.Capacity)
	return true, nil
}

func downloadCanadianArchive(ctx context.Context, url, destination string, maxBytes int64, force bool) error {
	// A download may have replaced the archive while sidecar persistence failed.
	// Verify the retained bytes before trusting validators or the downloader's
	// same-content shortcut; otherwise an upstream rollback could keep the newer
	// local archive because it happens to match the stale metadata's old hash.
	if exists, err := fileExists(destination); err != nil {
		return err
	} else if exists {
		actualSHA, hashErr := canadianArchiveSHA(ctx, destination, maxBytes)
		if err := ctx.Err(); err != nil {
			return err
		}
		meta, _ := download.ReadMetadata(download.MetadataPath(destination))
		if hashErr != nil || meta == nil || meta.SHA256 != actualSHA || meta.URL != strings.TrimSpace(url) {
			force = true
		}
	}
	_, err := download.Download(ctx, download.Request{URL: strings.TrimSpace(url), Destination: strings.TrimSpace(destination), Timeout: downloadTimeout, Force: force, MetadataPath: download.MetadataPath(destination), MaxBytes: maxBytes})
	if err != nil {
		return fmt.Errorf("ised: download %s: %w", filepath.Base(destination), err)
	}
	return nil
}

// canadianArchiveSHA hashes actual retained bytes, even if metadata persistence
// failed or inherited a stale ProcessedOK flag. This makes retries restart-safe.
func canadianArchiveSHA(ctx context.Context, path string, maxBytes int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("ised: open archive: %w", err)
	}
	defer f.Close()
	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(&contextReader{ctx: ctx, reader: f}, maxBytes+1))
	if err != nil {
		return "", err
	}
	if written == 0 || written > maxBytes {
		return "", errors.New("ised: archive byte limit or empty archive")
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func canadianPublishedPair(ctx context.Context, path string) (ready bool, mainSHA, specialSHA string) {
	exists, err := fileExists(path)
	if err != nil || !exists {
		return false, "", ""
	}
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=ro&_pragma=query_only(1)&_pragma=immutable(1)", path))
	if err != nil {
		return false, "", ""
	}
	defer db.Close()
	if err := probeCanadianDatabase(ctx, db); err != nil {
		return false, "", ""
	}
	mainSHA, specialSHA, err = canadianSourcePair(ctx, db)
	return err == nil, mainSHA, specialSHA
}

func markCanadianProcessed(cfg config.ISEDConfig, ok bool) {
	for _, path := range []string{cfg.Archive, cfg.SpecialArchive} {
		if err := download.UpdateProcessedStatus(download.MetadataPath(path), ok); err != nil {
			log.Printf("Warning: ISED processing metadata unavailable: %v", err)
		}
	}
}

func validateCanadianRefreshPaths(cfg config.ISEDConfig) error {
	if strings.TrimSpace(cfg.URL) == "" || strings.TrimSpace(cfg.SpecialURL) == "" {
		return errors.New("ised: both source URLs are required")
	}
	type managedPath struct {
		absolute, resolved string
		info               os.FileInfo
	}
	paths := make([]managedPath, 0, 5)
	for _, path := range []string{cfg.Archive, download.MetadataPath(cfg.Archive), cfg.SpecialArchive, download.MetadataPath(cfg.SpecialArchive), cfg.DBPath} {
		if strings.TrimSpace(path) == "" {
			return errors.New("ised: both archive paths and db_path are required")
		}
		absolute, err := filepath.Abs(strings.TrimSpace(path))
		if err != nil {
			return err
		}
		resolved, err := canadianManagedPathIdentity(absolute)
		if err != nil {
			return err
		}
		info, err := os.Stat(absolute)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		current := managedPath{absolute: absolute, resolved: resolved, info: info}
		for _, prior := range paths {
			if canadianPathsOverlap(current.absolute, prior.absolute) || canadianPathsOverlap(current.resolved, prior.resolved) || current.info != nil && prior.info != nil && os.SameFile(current.info, prior.info) {
				return errors.New("ised: colliding source, metadata or database paths")
			}
		}
		paths = append(paths, current)
	}
	return nil
}

func canadianPathsOverlap(a, b string) bool {
	a, b = strings.ToLower(filepath.Clean(a)), strings.ToLower(filepath.Clean(b))
	return a == b || strings.HasPrefix(a, b+string(filepath.Separator)) || strings.HasPrefix(b, a+string(filepath.Separator))
}

// Existing ancestors are resolved even before first download creates the final
// files. This closes aliases through a symlinked data directory on cold start.
func canadianManagedPathIdentity(path string) (string, error) {
	ancestor := path
	var suffix []string
	for {
		resolved, err := filepath.EvalSymlinks(ancestor)
		if err == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return resolved, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		// ENOENT can mean a present symlink has a missing target. Walking past
		// that link would invent a different identity, allowing metadata writes
		// to create and overwrite a future database through the unresolved alias.
		info, lstatErr := os.Lstat(ancestor)
		if lstatErr == nil && info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("ised: managed path contains an unresolved symbolic link")
		}
		if lstatErr != nil && !errors.Is(lstatErr, os.ErrNotExist) {
			return "", lstatErr
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return "", err
		}
		suffix = append(suffix, filepath.Base(ancestor))
		ancestor = parent
	}
}
