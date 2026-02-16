package uls

import (
	"archive/zip"
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
func extractArchive(archivePath string) (string, error) {
	r, err := zip.OpenReader(archivePath)
	if err != nil {
		return "", fmt.Errorf("fcc uls: open zip: %w", err)
	}
	defer r.Close()

	tmpDir, err := os.MkdirTemp(filepath.Dir(archivePath), "fcc-uls-extract-*")
	if err != nil {
		return "", fmt.Errorf("fcc uls: create temp dir: %w", err)
	}

	wanted := map[string]bool{
		"AM.DAT": true,
		"EN.DAT": true,
		"HD.DAT": true,
	}

	for _, f := range r.File {
		name := strings.ToUpper(filepath.Base(f.Name))
		if !wanted[name] {
			continue
		}
		if err := extractFile(f, filepath.Join(tmpDir, name)); err != nil {
			return "", err
		}
	}

	return tmpDir, nil
}

// Purpose: Extract a single file from the zip archive to disk.
// Key aspects: Streams file contents and preserves caller-chosen filename.
// Upstream: extractArchive.
// Downstream: f.Open, io.Copy, os.Create.
func extractFile(f *zip.File, dest string) error {
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

	written, err := io.Copy(out, io.LimitReader(rc, maxULSExtractFileBytes+1))
	if err != nil {
		return fmt.Errorf("fcc uls: copy %s: %w", dest, err)
	}
	if written > maxULSExtractFileBytes {
		return fmt.Errorf("fcc uls: %s exceeds extraction limit", f.Name)
	}
	return nil
}
