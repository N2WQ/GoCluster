// Package uls downloads and refreshes the FCC ULS amateur archive, rebuilding a
// slim SQLite database used for call metadata enrichment.
package uls

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dxcluster/config"
)

const (
	downloadTimeout  = 30 * time.Minute
	metadataSuffix   = ".status.json"
	legacyMetaSuffix = ".meta.json"
)

type metadata struct {
	LastModified string    `json:"last_modified,omitempty"`
	ETag         string    `json:"etag,omitempty"`
	DownloadedAt time.Time `json:"downloaded_at"`
	SizeBytes    int64     `json:"size_bytes"`
	CheckedAt    time.Time `json:"checked_at,omitempty"`
	UpToDate     bool      `json:"up_to_date,omitempty"`
}

// Purpose: Start a background refresh loop for the FCC ULS database.
// Key aspects: Kicks off an immediate refresh and then schedules daily updates.
// Upstream: main.go startup when ULS is enabled.
// Downstream: Refresh, startScheduler.
func StartBackground(ctx context.Context, cfg config.FCCULSConfig) {
	if !cfg.Enabled {
		return
	}
	// Run refresh/scheduler without blocking the caller.
	go func() {
		if updated, err := Refresh(cfg, false); err != nil {
			log.Printf("Warning: FCC ULS refresh failed: %v", err)
		} else if updated {
			log.Printf("FCC ULS database updated")
		} else {
			log.Printf("FCC ULS archive/database already up to date (db=%s)", cfg.DBPath)
		}
		startScheduler(ctx, cfg)
	}()
}

// Purpose: Download, extract, and rebuild the FCC ULS SQLite database.
// Key aspects: Uses conditional HTTP headers unless forced; rebuilds only when needed.
// Upstream: StartBackground, BuildOnce/manual refresh triggers.
// Downstream: downloadArchive, extractArchive, buildDatabase, ResetLicenseDB.
func Refresh(cfg config.FCCULSConfig, force bool) (bool, error) {
	url := strings.TrimSpace(cfg.URL)
	dest := strings.TrimSpace(cfg.Archive)
	dbPath := strings.TrimSpace(cfg.DBPath)
	metaPath := dest + metadataSuffix
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

	archiveUpdated, err := downloadArchive(url, dest, force)
	if err != nil {
		return false, err
	}

	needBuild := archiveUpdated
	if !needBuild {
		if _, err := os.Stat(dbPath); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				needBuild = true
			} else {
				return false, fmt.Errorf("fcc uls: stat db: %w", err)
			}
		}
	}

	if !needBuild && !force {
		return false, nil
	}

	extractDir, err := extractArchive(dest)
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(extractDir)

	ResetLicenseDB()
	if err := buildDatabase(extractDir, dbPath, cfg.TempDir); err != nil {
		markMetadataBuildStatus(metaPath, false)
		return false, err
	}
	markMetadataBuildStatus(metaPath, true)

	// Clean up archive on success to save space
	if err := os.Remove(dest); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("Warning: could not remove archive %s: %v", dest, err)
	}

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
		if updated, err := Refresh(cfg, false); err != nil {
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
// Key aspects: Uses conditional headers, atomic rename, and metadata tracking.
// Upstream: Refresh.
// Downstream: readMetadata, writeMetadata, cleanupLegacyMeta, HTTP client.
func downloadArchive(url, destination string, force bool) (bool, error) {
	metaPath := destination + metadataSuffix
	legacyMetaPath := destination + legacyMetaSuffix
	prevMeta, metaSource := readMetadata(metaPath, legacyMetaPath)
	if prevMeta == nil {
		if info, err := os.Stat(destination); err == nil {
			prevMeta = &metadata{
				LastModified: info.ModTime().UTC().Format(http.TimeFormat),
				SizeBytes:    info.Size(),
			}
		}
	}

	client := &http.Client{Timeout: downloadTimeout}
	ctx, cancel := context.WithTimeout(context.Background(), downloadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, fmt.Errorf("fcc uls: build request: %w", err)
	}
	if !force && prevMeta != nil {
		if prevMeta.ETag != "" {
			req.Header.Set("If-None-Match", prevMeta.ETag)
		}
		if prevMeta.LastModified != "" {
			req.Header.Set("If-Modified-Since", prevMeta.LastModified)
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("fcc uls: fetch failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		// Record that we checked and nothing changed so operators can see the scheduler ran.
		now := time.Now().UTC()
		meta := metadata{
			LastModified: prevMeta.LastModified,
			ETag:         prevMeta.ETag,
			DownloadedAt: prevMeta.DownloadedAt,
			SizeBytes:    prevMeta.SizeBytes,
			CheckedAt:    now,
			UpToDate:     true,
		}
		if err := writeMetadata(metaPath, meta); err != nil {
			log.Printf("Warning: unable to write FCC ULS metadata: %v", err)
		}
		cleanupLegacyMeta(metaSource, metaPath, legacyMetaPath)
		return false, nil
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return false, fmt.Errorf("fcc uls: fetch failed: status %s", resp.Status)
	}

	tmpDir := filepath.Dir(destination)
	if tmpDir == "" || tmpDir == "." {
		tmpDir = "."
	}
	tmpFile, err := os.CreateTemp(tmpDir, "fcc-uls-*.tmp")
	if err != nil {
		return false, fmt.Errorf("fcc uls: create temp file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	bytesWritten, err := io.Copy(tmpFile, resp.Body)
	if err != nil {
		tmpFile.Close()
		return false, fmt.Errorf("fcc uls: copy body: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return false, fmt.Errorf("fcc uls: finalize temp file: %w", err)
	}
	if bytesWritten == 0 {
		return false, errors.New("fcc uls: empty download")
	}

	if err := os.Remove(destination); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("fcc uls: remove old file: %w", err)
	}
	if err := os.Rename(tmpName, destination); err != nil {
		return false, fmt.Errorf("fcc uls: replace file: %w", err)
	}

	meta := metadata{
		LastModified: resp.Header.Get("Last-Modified"),
		ETag:         resp.Header.Get("ETag"),
		DownloadedAt: time.Now().UTC(),
		SizeBytes:    bytesWritten,
		CheckedAt:    time.Now().UTC(),
		UpToDate:     true,
	}
	if meta.LastModified == "" && prevMeta != nil {
		meta.LastModified = prevMeta.LastModified
	}
	if meta.ETag == "" && prevMeta != nil {
		meta.ETag = prevMeta.ETag
	}
	if err := writeMetadata(metaPath, meta); err != nil {
		log.Printf("Warning: unable to write FCC ULS metadata: %v", err)
	}
	cleanupLegacyMeta(metaSource, metaPath, legacyMetaPath)

	return true, nil
}

// Purpose: Read metadata JSON from the first readable path.
// Key aspects: Supports legacy path migration.
// Upstream: downloadArchive, markMetadataBuildStatus.
// Downstream: os.ReadFile, json.Unmarshal.
func readMetadata(paths ...string) (*metadata, string) {
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var meta metadata
		if err := json.Unmarshal(data, &meta); err != nil {
			continue
		}
		return &meta, path
	}
	return nil, ""
}

// Purpose: Persist metadata JSON to disk.
// Key aspects: Writes a human-readable indented JSON file.
// Upstream: downloadArchive, markMetadataBuildStatus.
// Downstream: json.MarshalIndent, os.WriteFile.
func writeMetadata(path string, meta metadata) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// Purpose: Remove legacy metadata files after a successful write.
// Key aspects: Deletes only when the legacy path was the source.
// Upstream: downloadArchive.
// Downstream: os.Remove.
func cleanupLegacyMeta(metaSource, newPath, legacyPath string) {
	if legacyPath == "" || legacyPath == newPath {
		return
	}
	if metaSource == legacyPath {
		if err := os.Remove(legacyPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("Warning: unable to remove legacy FCC ULS metadata %s: %v", legacyPath, err)
		}
	}
}

// Purpose: Update metadata to reflect whether the DB build succeeded.
// Key aspects: Avoids stale "up to date" markers after a failed build.
// Upstream: Refresh.
// Downstream: readMetadata, writeMetadata.
func markMetadataBuildStatus(metaPath string, success bool) {
	if strings.TrimSpace(metaPath) == "" {
		return
	}
	meta, _ := readMetadata(metaPath)
	if meta == nil {
		return
	}
	meta.CheckedAt = time.Now().UTC()
	meta.UpToDate = success
	if err := writeMetadata(metaPath, *meta); err != nil {
		log.Printf("Warning: unable to update FCC ULS metadata %s: %v", metaPath, err)
	}
}
