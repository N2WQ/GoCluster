// Package uls downloads and refreshes the FCC ULS amateur archive, rebuilding a
// slim SQLite database used for call metadata enrichment.
package uls

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/download"
)

const (
	downloadTimeout     = 30 * time.Minute
	legacyMetaSuffix    = ".meta.json"
	tempCleanupMaxFiles = 10
	tempCleanupMinAge   = 30 * time.Minute
)

// StartBackground starts a background refresh loop for the FCC ULS database.
// Key aspects: Kicks off an immediate refresh and then schedules daily updates.
// Upstream: main.go startup when ULS is enabled.
// Downstream: Refresh, startScheduler.
func StartBackground(ctx context.Context, cfg config.FCCULSConfig) {
	if !cfg.Enabled {
		return
	}
	// Run refresh/scheduler without blocking the caller.
	go func() {
		archiveDir := filepath.Dir(strings.TrimSpace(cfg.Archive))
		cleanupDownloadTemps(archiveDir, time.Now().UTC())
		dbExists, err := fileExists(cfg.DBPath)
		if err != nil {
			log.Printf("Warning: FCC ULS db stat failed: %v", err)
		}
		if err != nil || !dbExists {
			if updated, err := Refresh(ctx, cfg, true); err != nil {
				log.Printf("Warning: FCC ULS refresh failed: %v", err)
			} else if updated {
				log.Printf("FCC ULS database updated")
			} else {
				log.Printf("FCC ULS archive/database already up to date (db=%s)", cfg.DBPath)
			}
		}
		startScheduler(ctx, cfg)
	}()
}

// Refresh downloads, extracts, and rebuilds the FCC ULS SQLite database.
// Key aspects: Uses conditional HTTP headers unless forced; rebuilds only when needed.
// Upstream: StartBackground, BuildOnce/manual refresh triggers.
// Downstream: downloadArchive, extractArchive, buildDatabase, ResetLicenseDB.
func Refresh(ctx context.Context, cfg config.FCCULSConfig, force bool) (bool, error) {
	url := strings.TrimSpace(cfg.URL)
	dest := strings.TrimSpace(cfg.Archive)
	dbPath := strings.TrimSpace(cfg.DBPath)
	metaPath := download.MetadataPath(dest)
	if url == "" {
		return false, errors.New("fcc uls: URL is empty")
	}
	if dest == "" {
		return false, errors.New("fcc uls: archive_path is empty")
	}
	if dbPath == "" {
		return false, errors.New("fcc uls: db_path is empty")
	}

	dir := filepath.Dir(dest)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return false, fmt.Errorf("fcc uls: create directory: %w", err)
		}
	}

	dbExists, err := fileExists(dbPath)
	if err != nil {
		return false, fmt.Errorf("fcc uls: stat db: %w", err)
	}
	if !dbExists {
		force = true
	}

	archiveUpdated, err := downloadArchive(ctx, url, dest, metaPath, force, dbExists)
	if err != nil {
		return false, err
	}

	needBuild := archiveUpdated || force || !dbExists

	if !needBuild && !force {
		return false, nil
	}

	// Prevent readers from reopening the DB during build/swap.
	SetRefreshInProgress(true)
	defer SetRefreshInProgress(false)

	extractDir, err := extractArchive(dest)
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(extractDir)

	ResetLicenseDB()
	if err := buildDatabase(extractDir, dbPath, cfg.TempDir); err != nil {
		if metaErr := download.UpdateProcessedStatus(metaPath, false); metaErr != nil {
			log.Printf("Warning: unable to update FCC ULS metadata %s: %v", metaPath, metaErr)
		}
		return false, err
	}
	if err := download.UpdateProcessedStatus(metaPath, true); err != nil {
		log.Printf("Warning: unable to update FCC ULS metadata %s: %v", metaPath, err)
	}

	// Clean up archive on success to save space
	if err := os.Remove(dest); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("Warning: could not remove archive %s: %v", dest, err)
	}
	cleanupDownloadTemps(filepath.Dir(dest), time.Now().UTC())

	return true, nil
}

// Purpose: Run the daily refresh schedule until ctx is canceled.
// Key aspects: Uses reusable timers and honors ctx cancellation.
// Upstream: StartBackground goroutine.
// Downstream: nextRefreshDelay, Refresh.
func startScheduler(ctx context.Context, cfg config.FCCULSConfig) {
	for {
		delay := nextRefreshDelay(cfg, time.Now().UTC())
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if updated, err := Refresh(ctx, cfg, false); err != nil {
			log.Printf("Warning: scheduled FCC ULS download failed: %v", err)
		} else if updated {
			log.Printf("FCC ULS database updated")
		} else {
			log.Printf("Scheduled FCC ULS download: up to date (%s)", cfg.Archive)
		}
	}
}

// Purpose: Compute the delay until the next scheduled refresh.
// Key aspects: Uses configured UTC hour/minute; rolls to next day if needed.
// Upstream: startScheduler.
// Downstream: refreshHourMinute.
func nextRefreshDelay(cfg config.FCCULSConfig, now time.Time) time.Duration {
	hour, minute := refreshHourMinute(cfg)
	target := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, time.UTC)
	if !target.After(now) {
		target = target.Add(24 * time.Hour)
	}
	return target.Sub(now)
}

// Purpose: Parse the configured refresh time into hour/minute.
// Key aspects: Defaults to 02:15 UTC on parse errors or empty config.
// Upstream: nextRefreshDelay.
// Downstream: time.Parse.
func refreshHourMinute(cfg config.FCCULSConfig) (int, int) {
	refresh := strings.TrimSpace(cfg.RefreshUTC)
	if refresh == "" {
		refresh = "02:15"
	}
	if parsed, err := time.Parse("15:04", refresh); err == nil {
		return parsed.Hour(), parsed.Minute()
	}
	return 2, 15
}

// Purpose: Fetch the FCC ULS archive to disk, honoring cached metadata.
// Key aspects: Uses conditional HTTP download and sidecar metadata.
// Upstream: Refresh.
// Downstream: download.Download.
func downloadArchive(ctx context.Context, url, destination, metaPath string, force bool, dbExists bool) (bool, error) {
	if err := ensureDir(destination); err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, downloadTimeout)
	defer cancel()
	res, err := download.Download(ctx, download.Request{
		URL:                     url,
		Destination:             destination,
		Timeout:                 downloadTimeout,
		Force:                   force,
		AllowMissingDestination: dbExists && !force,
		MetadataPath:            metaPath,
		LegacyMetadataPaths:     []string{destination + legacyMetaSuffix},
	})
	if err != nil {
		return false, fmt.Errorf("fcc uls: %w", err)
	}
	return res.Status == download.StatusUpdated, nil
}

func ensureDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("fcc uls: create directory: %w", err)
	}
	return nil
}

func fileExists(path string) (bool, error) {
	if strings.TrimSpace(path) == "" {
		return false, nil
	}
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func cleanupDownloadTemps(dir string, now time.Time) {
	if strings.TrimSpace(dir) == "" || dir == "." {
		return
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	cutoff := now.Add(-tempCleanupMinAge)
	removed := 0
	for _, entry := range entries {
		if removed >= tempCleanupMaxFiles {
			break
		}
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "download-") || !strings.HasSuffix(name, ".tmp") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.ModTime().After(cutoff) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("Warning: FCC ULS temp cleanup failed for %s: %v", name, err)
			continue
		}
		removed++
	}
	if removed > 0 {
		log.Printf("FCC ULS temp cleanup removed %d file(s) in %s", removed, dir)
	}
}
