// Package uls downloads and refreshes the FCC ULS amateur archive, rebuilding a
// slim SQLite database used for call metadata enrichment.
package uls

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/download"
	"dxcluster/internal/fsutil"
	"dxcluster/internal/schedule"
)

const (
	downloadTimeout     = 30 * time.Minute
	legacyMetaSuffix    = ".meta.json"
	tempCleanupMaxFiles = 10
	tempCleanupMinAge   = 30 * time.Minute
)

// StartBackground starts a background refresh loop for the FCC ULS database.
// Key aspects: Rebuilds missing/old schemas immediately, then schedules daily updates.
// The caller cancels ctx and joins the returned channel before releasing runtime owners.
// Upstream: runtime startup; download/enrichment are independent of enforcement.
// Downstream: Refresh, startScheduler.
func StartBackground(ctx context.Context, cfg config.FCCULSConfig) <-chan struct{} {
	done := make(chan struct{})
	if ctx == nil {
		close(done)
		return done
	}
	// Run refresh/scheduler without blocking the caller.
	go func() {
		defer close(done)
		archiveDir := filepath.Dir(strings.TrimSpace(cfg.Archive))
		cleanupDownloadTemps(archiveDir, time.Now().UTC())
		dbExists, err := fileExists(cfg.DBPath)
		if err != nil {
			log.Printf("Warning: FCC ULS db stat failed: %v", err)
		}
		ready := dbExists && databaseStateReady(ctx, cfg.DBPath)
		if err != nil || !ready {
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
	return done
}

// refreshOwner serializes builders, independently of lookup/diagnostic locks.
var refreshOwner = make(chan struct{}, 1)

// Refresh downloads, extracts, and rebuilds the FCC ULS SQLite database.
// Key aspects: Uses conditional HTTP headers unless forced; rebuilds only when needed.
// Upstream: StartBackground, BuildOnce/manual refresh triggers.
// Downstream: downloadArchive, extractArchive, buildDatabase, ResetLicenseDB.
func Refresh(ctx context.Context, cfg config.FCCULSConfig, force bool) (updated bool, err error) {
	if ctx == nil {
		return false, errors.New("fcc uls: nil context")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	select {
	case refreshOwner <- struct{}{}:
		defer func() { <-refreshOwner }()
	case <-ctx.Done():
		return false, ctx.Err()
	}
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
	ready := dbExists && databaseStateReady(ctx, dbPath)
	meta, _ := download.ReadMetadata(metaPath, dest+legacyMetaSuffix)
	processingFailed := meta != nil && !meta.ProcessedAt.IsZero() && !meta.ProcessedOK
	if !ready || processingFailed {
		force = true
	}

	archiveUpdated, err := downloadArchive(ctx, url, dest, metaPath, force, dbExists)
	if err != nil {
		return false, err
	}

	needBuild := archiveUpdated || force || !ready || processingFailed

	if !needBuild && !force {
		return false, nil
	}

	// Prevent readers from reopening the DB during build/swap.
	SetRefreshInProgress(true)
	defer SetRefreshInProgress(false)
	defer func() {
		if err != nil {
			if metaErr := download.UpdateProcessedStatus(metaPath, false); metaErr != nil {
				log.Printf("Warning: unable to mark FCC ULS build failure: %v", metaErr)
			}
		}
	}()

	extractDir, err := extractArchiveContext(ctx, dest)
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(extractDir)

	ResetLicenseDB()
	if err = buildDatabase(ctx, extractDir, dbPath, cfg.TempDir); err != nil {
		return false, err
	}
	SetLicenseDBPath(dbPath)
	snapshot := LookupStats()
	log.Printf("FCC ULS published: schema=%d cache_generation=%d cache_entries=%d cache_cap=%d", CurrentSchemaVersion, snapshot.Generation, snapshot.Entries, snapshot.Capacity)

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
// Downstream: internal/schedule helpers.
func nextRefreshDelay(cfg config.FCCULSConfig, now time.Time) time.Duration {
	return schedule.NextDailyUTC(cfg.RefreshUTC, now, 2, 15, schedule.ParseOptions{})
}

// Purpose: Fetch the FCC ULS archive to disk, honoring cached metadata.
// Key aspects: Uses conditional HTTP download and sidecar metadata.
// Upstream: Refresh.
// Downstream: download.Download.
func downloadArchive(ctx context.Context, url, destination, metaPath string, force bool, dbExists bool) (bool, error) {
	if err := fsutil.EnsureParentDir(destination, "fcc uls: create directory"); err != nil {
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

// databaseStateReady is a cold lifecycle probe, never a per-spot operation.
// The schema marker and actual column both matter; an unusable projection must
// retry on unchanged upstream data rather than wait for a new FCC archive.
func databaseStateReady(ctx context.Context, path string) bool {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=ro&_pragma=query_only(1)&_pragma=immutable(1)", path))
	if err != nil {
		return false
	}
	defer db.Close()
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version;").Scan(&version); err != nil || version != CurrentSchemaVersion {
		return false
	}
	rows, err := db.QueryContext(ctx, "SELECT call_sign, state FROM AM LIMIT 0;")
	if err != nil {
		return false
	}
	return rows.Close() == nil
}
