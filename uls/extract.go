package uls

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const maxULSExtractFileBytes int64 = 1 << 30 // 1 GiB safety cap per archive member

// Purpose: Extract the FCC ULS DAT files needed for building the SQLite DB.
// Key aspects: Filters to a small set of tables; returns a temp dir for caller cleanup.
// Upstream: Refresh in uls/downloader.go.
// Downstream: extractFile, os.MkdirTemp, zip reader.
func extractArchiveContext(ctx context.Context, archivePath string) (string, error) {
	r, err := zip.OpenReader(archivePath)
	if err != nil {
		return "", fmt.Errorf("fcc uls: open zip: %w", err)
	}
	defer r.Close()

	tmpDir, err := os.MkdirTemp(filepath.Dir(archivePath), "fcc-uls-extract-*")
	if err != nil {
		return "", fmt.Errorf("fcc uls: create temp dir: %w", err)
	}

	success := false
	defer func() {
		if !success {
			_ = os.RemoveAll(tmpDir)
		}
	}()

	wanted := map[string]bool{
		"AM.DAT": true,
		"EN.DAT": true,
		"HD.DAT": true,
	}

	seen := make(map[string]bool, len(wanted))
	for _, f := range r.File {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		name := strings.ToUpper(filepath.Base(f.Name))
		if !wanted[name] {
			continue
		}
		if seen[name] {
			return "", fmt.Errorf("fcc uls: duplicate archive member %s", name)
		}
		seen[name] = true
		if err := extractFileContext(ctx, f, filepath.Join(tmpDir, name)); err != nil {
			return "", err
		}
	}

	success = true
	return tmpDir, nil
}

// Purpose: Extract a single file from the zip archive to disk.
// Key aspects: Streams file contents and preserves caller-chosen filename.
// Upstream: extractArchive.
// Downstream: f.Open, io.Copy, os.Create.
func extractFileContext(ctx context.Context, f *zip.File, dest string) error {
	if f.UncompressedSize64 > uint64(maxULSExtractFileBytes) {
		return fmt.Errorf("fcc uls: %s exceeds extraction limit (%d bytes)", f.Name, f.UncompressedSize64)
	}

	rc, err := f.Open()
	if err != nil {
		return fmt.Errorf("fcc uls: open %s: %w", f.Name, err)
	}
	defer rc.Close()

	out, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("fcc uls: create %s: %w", dest, err)
	}
	defer out.Close()

	written, err := io.Copy(out, io.LimitReader(&contextReader{ctx: ctx, reader: rc}, maxULSExtractFileBytes+1))
	if err != nil {
		return fmt.Errorf("fcc uls: copy %s: %w", dest, err)
	}
	if written > maxULSExtractFileBytes {
		return fmt.Errorf("fcc uls: %s exceeds extraction limit", f.Name)
	}
	return nil
}

// contextReader checks between streamed ZIP reads; cancellation does not need
// another worker and partial extraction remains owned by the failed build.
type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
