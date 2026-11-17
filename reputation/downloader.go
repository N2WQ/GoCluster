package reputation

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dxcluster/download"
	"dxcluster/internal/fsutil"
	"dxcluster/internal/schedule"
)

// downloadLoop refreshes the local IPinfo store on the configured UTC cadence.
// Runtime admission never waits on this loop; failed refreshes leave the last
// loaded store in place and surface as warnings.
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

// Compare the verified archive to the serving database, not to the previous
// download. That makes an unchanged HTTP response eligible to retry a failed
// import and prevents download success from refreshing a different database.
func (g *Gate) downloadAndLoad(ctx context.Context) error {
	if g == nil || !g.cfg.ipinfoDownloadEnabled {
		return nil
	}
	g.ipinfoRefreshMu.Lock()
	defer g.ipinfoRefreshMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	cfg := g.cfg
	endpoint := strings.TrimSpace(cfg.ipinfoDownloadURL)
	token := strings.TrimSpace(cfg.ipinfoDownloadToken)
	if endpoint == "" || token == "" {
		return errors.New("ipinfo download URL or token missing")
	}
	gzPath := cfg.ipinfoDownloadPath
	if err := validateIPInfoPaths(cfg, g.ipinfoView.Load()); err != nil {
		return err
	}

	result, err := download.Download(ctx, download.Request{
		URL:                 strings.ReplaceAll(endpoint, "$TOKEN", token),
		Destination:         gzPath,
		Timeout:             cfg.ipinfoDownloadTimeout,
		RequireByteExact304: true,
	})
	if err != nil {
		var requestErr *url.Error
		if errors.As(err, &requestErr) {
			// The URL can contain the private download token.
			return fmt.Errorf("IPinfo download request: %w", requestErr.Err)
		}
		return err
	}
	hash := result.Meta.SHA256
	if !validIPInfoSourceHash(hash) {
		return errors.New("ipinfo download returned no verified source hash")
	}
	if active := g.ipinfoView.Load(); active != nil && active.hash == hash {
		if err := g.renewIPInfo(ctx, active, result.Meta.CheckedAt); err != nil {
			return err
		}
		g.cleanupIPInfoSources(false)
		log.Printf("IPinfo snapshot verified unchanged")
		return nil
	}
	return g.importIPInfo(ctx, hash, buildIPInfoPebbleFromSource)
}

func (g *Gate) importIPInfo(ctx context.Context, hash string, build func(context.Context, io.Reader, string, bool, string) (string, error)) error {
	cfg := g.cfg
	ctx, cancel := context.WithTimeout(ctx, cfg.ipinfoImportTimeout)
	defer cancel()
	if err := fsutil.EnsureParentDir(cfg.ipinfoSnapshotPath, "create IPinfo CSV directory"); err != nil {
		return err
	}
	csvFile, err := stageIPInfoCSV(ctx, cfg.ipinfoDownloadPath, filepath.Dir(cfg.ipinfoSnapshotPath), hash)
	if err != nil {
		return fmt.Errorf("extract IPinfo snapshot: %w", err)
	}
	defer os.Remove(csvFile.Name())

	// The builder consumes the same private file that extraction filled. The
	// configured CSV is a diagnostic artifact and can change independently.
	dbPath, buildErr := build(ctx, csvFile, cfg.ipinfoPebblePath, cfg.ipinfoPebbleCompact, hash)
	if err := errors.Join(csvFile.Close(), ctx.Err()); err != nil {
		return discardIPInfoCandidate(dbPath, errors.Join(buildErr, err))
	}
	if err := fsutil.ReplaceFile(csvFile.Name(), cfg.ipinfoSnapshotPath); err != nil {
		return discardIPInfoCandidate(dbPath, errors.Join(buildErr, fmt.Errorf("publish IPinfo CSV: %w", err)))
	}
	if buildErr != nil {
		return discardIPInfoCandidate(dbPath, buildErr)
	}
	if err := g.loadIPInfoCandidate(ctx, dbPath); err != nil {
		return discardIPInfoCandidate(dbPath, err)
	}
	g.cleanupIPInfoSources(true)
	if cfg.ipinfoPebbleCleanup {
		if err := cleanupIPInfoPebbleDirs(cfg.ipinfoPebblePath, dbPath); err != nil {
			log.Printf("Warning: ipinfo pebble cleanup failed: %v", err)
		}
	}
	log.Printf("IPinfo pebble store updated at %s", dbPath)
	return nil
}

func discardIPInfoCandidate(path string, cause error) error {
	if path != "" {
		if err := os.RemoveAll(path); err != nil {
			return errors.Join(cause, fmt.Errorf("cleanup uncommitted IPinfo database: %w", err))
		}
	}
	return cause
}

func (g *Gate) loadIPInfoCandidate(ctx context.Context, dbPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store, view, err := g.prepareIPInfoStore(ctx, dbPath)
	if err != nil {
		return err
	}
	if err := g.publishIPInfoStore(ctx, store, view, true); err != nil {
		store.Close()
		return err
	}
	return nil
}

func (g *Gate) cleanupIPInfoSources(imported bool) {
	if imported && g.cfg.ipinfoDeleteCSV {
		removeIPInfoSource(g.cfg.ipinfoSnapshotPath)
	}
	if !g.cfg.ipinfoKeepGzip {
		removeIPInfoSource(g.cfg.ipinfoDownloadPath)
	}
}

func removeIPInfoSource(path string) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("Warning: IPinfo source cleanup failed: %v", err)
	}
}

// stageIPInfoCSV verifies the compressed bytes while extracting into a private
// file. On success the caller owns the open, rewound file and its cleanup.
func stageIPInfoCSV(ctx context.Context, src, dir, expectedHash string) (_ *os.File, err error) {
	in, err := os.Open(src)
	if err != nil {
		return nil, err
	}
	defer in.Close()
	hash := sha256.New()
	reader, err := gzip.NewReader(bufio.NewReader(io.TeeReader(in, hash)))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	tmp, err := os.CreateTemp(dir, "ipinfo-*.tmp")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()

	buf := make([]byte, 128*1024)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, readErr := reader.Read(buf)
		if n > 0 {
			if _, err := tmp.Write(buf[:n]); err != nil {
				return nil, err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if hex.EncodeToString(hash.Sum(nil)) != expectedHash {
		return nil, errors.New("IPinfo archive changed after download verification")
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return tmp, nil
}

func nextRefreshDelay(refreshUTC string, now time.Time) time.Duration {
	return schedule.NextDailyUTC(refreshUTC, now, 3, 0, schedule.ParseOptions{
		AllowDailyPrefix: true,
		AllowTrailingZ:   true,
	})
}
