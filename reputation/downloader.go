package reputation

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dxcluster/download"
	"dxcluster/internal/schedule"
)

func (g *Gate) downloadLoop(ctx context.Context) {
	if g == nil || !g.cfg.enabled || !g.cfg.ipinfoDownloadEnabled {
		return
	}
	if err := g.downloadAndLoad(ctx); err != nil {
		log.Printf("Warning: ipinfo download failed: %v", err)
	}
	for {
		delay := nextRefreshDelay(g.cfg.ipinfoRefreshUTC, time.Now().UTC())
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if err := g.downloadAndLoad(ctx); err != nil {
			log.Printf("Warning: scheduled ipinfo download failed: %v", err)
		}
	}
}

func (g *Gate) downloadAndLoad(ctx context.Context) error {
	if g == nil || !g.cfg.ipinfoDownloadEnabled {
		return nil
	}
	updated, err := downloadSnapshot(ctx, g.cfg)
	if err != nil {
		return err
	}
	if !updated && g.ipinfoStoreLoaded() {
		return nil
	}
	if err := g.LoadStore(); err != nil {
		return err
	}
	log.Printf("IPinfo store loaded from %s", g.cfg.ipinfoPebblePath)
	return nil
}

func downloadSnapshot(ctx context.Context, cfg gateConfig) (bool, error) {
	url := strings.TrimSpace(cfg.ipinfoDownloadURL)
	token := strings.TrimSpace(cfg.ipinfoDownloadToken)
	if url == "" || token == "" {
		return false, fmt.Errorf("ipinfo download url/token missing")
	}
	url = strings.ReplaceAll(url, "$TOKEN", token)
	gzPath := strings.TrimSpace(cfg.ipinfoDownloadPath)
	csvPath := strings.TrimSpace(cfg.ipinfoSnapshotPath)
	if gzPath == "" || csvPath == "" {
		return false, fmt.Errorf("ipinfo download paths missing")
	}

	if err := os.MkdirAll(filepath.Dir(gzPath), 0o755); err != nil {
		return false, err
	}
	if err := os.MkdirAll(filepath.Dir(csvPath), 0o755); err != nil {
		return false, err
	}

	timeout := cfg.ipinfoDownloadTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	downloadCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	res, err := download.Download(downloadCtx, download.Request{
		URL:         url,
		Destination: gzPath,
		Timeout:     timeout,
	})
	if err != nil {
		return false, err
	}
	if res.Status != download.StatusUpdated {
		log.Printf("IPinfo download: up to date (%s)", gzPath)
		return false, nil
	}
	importTimeout := cfg.ipinfoImportTimeout
	if importTimeout <= 0 {
		importTimeout = 10 * time.Minute
	}
	importCtx, importCancel := context.WithTimeout(ctx, importTimeout)
	defer importCancel()

	if err := extractGzip(importCtx, gzPath, csvPath); err != nil {
		return false, err
	}
	dbPath, err := buildIPInfoPebble(importCtx, csvPath, cfg.ipinfoPebblePath, cfg.ipinfoPebbleCompact)
	if err != nil {
		return false, err
	}
	if err := updateIPInfoPebbleCurrent(cfg.ipinfoPebblePath, dbPath); err != nil {
		return false, err
	}
	if cfg.ipinfoDeleteCSV {
		if err := os.Remove(csvPath); err != nil && !os.IsNotExist(err) {
			return false, err
		}
	}
	if !cfg.ipinfoKeepGzip {
		if err := os.Remove(gzPath); err != nil && !os.IsNotExist(err) {
			return false, err
		}
	}
	if cfg.ipinfoPebbleCleanup {
		if err := cleanupIPInfoPebbleDirs(cfg.ipinfoPebblePath, dbPath); err != nil {
			log.Printf("Warning: ipinfo pebble cleanup failed: %v", err)
		}
	}
	log.Printf("IPinfo pebble store updated at %s", dbPath)
	return true, nil
}

func (g *Gate) ipinfoStoreLoaded() bool {
	if g == nil {
		return false
	}
	g.ipinfoStoreMu.RLock()
	loaded := g.ipinfoStore != nil
	g.ipinfoStoreMu.RUnlock()
	return loaded
}

func extractGzip(ctx context.Context, src, dest string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	reader, err := gzip.NewReader(in)
	if err != nil {
		return err
	}
	defer reader.Close()

	dir := filepath.Dir(dest)
	tmp, err := os.CreateTemp(dir, "ipinfo-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	buf := make([]byte, 128*1024)
	for {
		if err := ctx.Err(); err != nil {
			tmp.Close()
			return err
		}
		n, err := reader.Read(buf)
		if n > 0 {
			if _, werr := tmp.Write(buf[:n]); werr != nil {
				tmp.Close()
				return werr
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			tmp.Close()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Remove(dest); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(tmpPath, dest)
}

func nextRefreshDelay(refreshUTC string, now time.Time) time.Duration {
	return schedule.NextDailyUTC(refreshUTC, now, 3, 0, schedule.ParseOptions{
		AllowDailyPrefix: true,
		AllowTrailingZ:   true,
	})
}
