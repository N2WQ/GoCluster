// Package download provides HTTP download helpers with conditional requests and
// sidecar metadata to avoid unnecessary reloads.
package download

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	"dxcluster/internal/fsutil"
)

const MetadataSuffix = ".status.json"

// Status indicates whether the remote content changed.
type Status string

const (
	StatusUpdated     Status = "updated"
	StatusNotModified Status = "not_modified"
	StatusSameContent Status = "same_content"
)

// Metadata tracks the last successful download/check and any post-processing status.
type Metadata struct {
	URL          string    `json:"url,omitempty"`
	ETag         string    `json:"etag,omitempty"`
	LastModified string    `json:"last_modified,omitempty"`
	DownloadedAt time.Time `json:"downloaded_at,omitempty"`
	CheckedAt    time.Time `json:"checked_at,omitempty"`
	SizeBytes    int64     `json:"size_bytes,omitempty"`
	SHA256       string    `json:"sha256,omitempty"`
	UpToDate     bool      `json:"up_to_date,omitempty"`
	ProcessedAt  time.Time `json:"processed_at,omitempty"`
	ProcessedOK  bool      `json:"processed_ok,omitempty"`
}

// Request configures a download with optional metadata and legacy fallbacks.
type Request struct {
	URL                     string
	Destination             string
	Timeout                 time.Duration
	Force                   bool
	AllowMissingDestination bool
	MetadataPath            string
	LegacyMetadataPaths     []string
	UserAgent               string
	DisableLegacyCleanup    bool
	// MaxBytes bounds both declared and streamed body size before publication.
	// Zero preserves unlimited downloads for existing callers.
	MaxBytes int64
}

// Result summarizes the download outcome.
type Result struct {
	Status Status
	Meta   Metadata
	Bytes  int64
}

// MetadataPath returns the default metadata sidecar path for a destination.
func MetadataPath(dest string) string {
	if strings.TrimSpace(dest) == "" {
		return ""
	}
	return dest + MetadataSuffix
}

// Download downloads a file with conditional headers and metadata sidecar.
// Key aspects: Uses ETag/Last-Modified, computes SHA256, and caps opted-in bodies
// before publication. Oversized or interrupted bodies preserve existing data.
// Upstream: CTY/SCP/IPinfo/ULS/ISED refreshers.
// Downstream: HTTP client, metadata read/write helpers.
func Download(ctx context.Context, req Request) (Result, error) {
	var result Result
	url := strings.TrimSpace(req.URL)
	dest := strings.TrimSpace(req.Destination)
	if url == "" {
		return result, errors.New("download: URL is empty")
	}
	if dest == "" {
		return result, errors.New("download: destination is empty")
	}
	if req.MaxBytes < 0 {
		return result, errors.New("download: MaxBytes must be >= 0")
	}

	metaPath := strings.TrimSpace(req.MetadataPath)
	if metaPath == "" {
		metaPath = MetadataPath(dest)
	}

	destInfo, err := os.Stat(dest)
	destExists := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, fmt.Errorf("download: stat destination: %w", err)
	}

	prevMeta, metaSource := ReadMetadata(append([]string{metaPath}, req.LegacyMetadataPaths...)...)
	if prevMeta == nil && destExists {
		prevMeta = &Metadata{
			LastModified: destInfo.ModTime().UTC().Format(http.TimeFormat),
			SizeBytes:    destInfo.Size(),
		}
	}

	force := req.Force || (!destExists && !req.AllowMissingDestination)

	client := &http.Client{}
	if req.Timeout > 0 {
		client.Timeout = req.Timeout
	}
	reqCtx := ctx
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}

	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return result, fmt.Errorf("download: build request: %w", err)
	}
	if !force && prevMeta != nil {
		if prevMeta.ETag != "" {
			httpReq.Header.Set("If-None-Match", prevMeta.ETag)
		}
		if prevMeta.LastModified != "" {
			httpReq.Header.Set("If-Modified-Since", prevMeta.LastModified)
		}
	}
	if req.UserAgent != "" {
		httpReq.Header.Set("User-Agent", req.UserAgent)
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return result, fmt.Errorf("download: fetch failed: %w", err)
	}
	defer resp.Body.Close()

	now := time.Now().UTC()
	if resp.StatusCode == http.StatusNotModified {
		result.Status = StatusNotModified
		meta := mergeMetadata(prevMeta, url, resp, now, false, "")
		meta.CheckedAt = now
		meta.UpToDate = true
		if err := WriteMetadata(metaPath, meta); err != nil {
			log.Printf("Warning: unable to write metadata %s: %v", metaPath, err)
		} else if !req.DisableLegacyCleanup {
			cleanupLegacyMetadata(metaSource, metaPath)
		}
		result.Meta = meta
		return result, nil
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return result, fmt.Errorf("download: fetch failed: status %s", resp.Status)
	}
	if req.MaxBytes > 0 && resp.ContentLength > req.MaxBytes {
		return result, fmt.Errorf("download: body exceeds MaxBytes (%d)", req.MaxBytes)
	}

	if err := fsutil.EnsureParentDir(dest, "download: create directory"); err != nil {
		return result, err
	}
	tmpFile, err := os.CreateTemp(filepath.Dir(dest), "download-*.tmp")
	if err != nil {
		return result, fmt.Errorf("download: create temp file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	hasher := sha256.New()
	written, err := copyDownloadBody(io.MultiWriter(tmpFile, hasher), resp.Body, req.MaxBytes)
	if err != nil {
		tmpFile.Close()
		return result, fmt.Errorf("download: copy body: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return result, fmt.Errorf("download: finalize temp file: %w", err)
	}
	if written <= 0 {
		return result, errors.New("download: empty response body")
	}
	if err := reqCtx.Err(); err != nil {
		return result, fmt.Errorf("download: canceled before publication: %w", err)
	}

	hashHex := hex.EncodeToString(hasher.Sum(nil))
	result.Bytes = written

	sameContent := prevMeta != nil && destExists && prevMeta.SHA256 != "" && prevMeta.SHA256 == hashHex
	if sameContent && !force {
		result.Status = StatusSameContent
		meta := mergeMetadata(prevMeta, url, resp, now, false, hashHex)
		meta.CheckedAt = now
		meta.UpToDate = true
		if err := WriteMetadata(metaPath, meta); err != nil {
			log.Printf("Warning: unable to write metadata %s: %v", metaPath, err)
		} else if !req.DisableLegacyCleanup {
			cleanupLegacyMetadata(metaSource, metaPath)
		}
		result.Meta = meta
		return result, nil
	}

	if err := os.Remove(dest); err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, fmt.Errorf("download: remove old file: %w", err)
	}
	if err := os.Rename(tmpName, dest); err != nil {
		return result, fmt.Errorf("download: replace file: %w", err)
	}

	result.Status = StatusUpdated
	meta := mergeMetadata(prevMeta, url, resp, now, true, hashHex)
	meta.CheckedAt = now
	meta.DownloadedAt = now
	meta.SizeBytes = written
	meta.UpToDate = true
	if err := WriteMetadata(metaPath, meta); err != nil {
		log.Printf("Warning: unable to write metadata %s: %v", metaPath, err)
	} else if !req.DisableLegacyCleanup {
		cleanupLegacyMetadata(metaSource, metaPath)
	}
	result.Meta = meta
	return result, nil
}

// copyDownloadBody consumes at most the configured limit plus one overflow byte.
// Overflow is checked even without Content-Length, before any destination or
// metadata changes. Limiting before Copy avoids integer overflow at MaxInt64.
func copyDownloadBody(dst io.Writer, body io.Reader, maxBytes int64) (int64, error) {
	if maxBytes == 0 {
		return io.Copy(dst, body)
	}
	written, err := io.Copy(dst, io.LimitReader(body, maxBytes))
	if err != nil || written < maxBytes {
		return written, err
	}
	overflow, err := io.CopyN(io.Discard, body, 1)
	if overflow != 0 {
		return written, fmt.Errorf("body exceeds MaxBytes (%d)", maxBytes)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return written, err
	}
	return written, nil
}

// ReadMetadata reads a metadata JSON file from the first readable path.
// Key aspects: Ignores parse errors to allow legacy fallbacks.
// Upstream: Download, UpdateProcessedStatus.
// Downstream: os.ReadFile, json.Unmarshal.
func ReadMetadata(paths ...string) (*Metadata, string) {
	for _, path := range paths {
		if strings.TrimSpace(path) == "" {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var meta Metadata
		if err := json.Unmarshal(data, &meta); err != nil {
			continue
		}
		return &meta, path
	}
	return nil, ""
}

// WriteMetadata persists metadata JSON to disk.
// Key aspects: Writes an indented JSON file for operator readability.
// Upstream: Download, UpdateProcessedStatus.
// Downstream: json.MarshalIndent, os.WriteFile.
func WriteMetadata(path string, meta Metadata) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("download: metadata path is empty")
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// UpdateProcessedStatus updates metadata with post-processing status (e.g., DB build).
// Key aspects: Leaves download fields intact and only patches processing flags.
// Upstream: Callers that build derived artifacts.
// Downstream: ReadMetadata, WriteMetadata.
func UpdateProcessedStatus(metaPath string, ok bool) error {
	meta, source := ReadMetadata(metaPath)
	if meta == nil {
		return nil
	}
	meta.ProcessedAt = time.Now().UTC()
	meta.ProcessedOK = ok
	meta.UpToDate = ok
	if err := WriteMetadata(metaPath, *meta); err != nil {
		return err
	}
	if source != "" && source != metaPath {
		if err := os.Remove(source); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("Warning: unable to remove legacy metadata %s: %v", source, err)
		}
	}
	return nil
}

func mergeMetadata(prev *Metadata, url string, resp *http.Response, now time.Time, updated bool, hash string) Metadata {
	meta := Metadata{}
	if prev != nil {
		meta = *prev
	}
	meta.URL = url
	if resp != nil {
		if etag := strings.TrimSpace(resp.Header.Get("ETag")); etag != "" {
			meta.ETag = etag
		}
		if last := strings.TrimSpace(resp.Header.Get("Last-Modified")); last != "" {
			meta.LastModified = last
		}
	}
	if updated {
		meta.DownloadedAt = now
	}
	if hash != "" {
		meta.SHA256 = hash
	}
	return meta
}

func cleanupLegacyMetadata(source, metaPath string) {
	if source == "" || source == metaPath {
		return
	}
	if err := os.Remove(source); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("Warning: unable to remove legacy metadata %s: %v", source, err)
	}
}
