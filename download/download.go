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
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dxcluster/internal/fsutil"
)

const MetadataSuffix = ".status.json"

// Status indicates whether the local file was published or verified unchanged.
type Status string

const (
	StatusUpdated     Status = "updated"
	StatusNotModified Status = "not_modified"
	StatusSameContent Status = "same_content"
)

// Metadata tracks the last successful download/check and any post-processing status.
type Metadata struct {
	// URL identifies the original request. Credential-bearing URLs use a digest.
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
	// ByteExactETag belongs to SHA256 and URL from a complete, direct response.
	ByteExactETag string `json:"byte_exact_etag,omitempty"`
	// ValidatorResource identifies the effective URL of a complete response.
	// Hash it so signed redirect URLs need no additional plaintext storage.
	ValidatorResource string `json:"validator_resource,omitempty"`
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
	// RequireByteExact304 allows a 304 only for the same strong ETag as a
	// previously verified, complete response from the requested resource.
	RequireByteExact304 bool
	// MaxBytes bounds both declared and streamed body size before publication.
	// Zero preserves unlimited downloads for existing callers.
	MaxBytes int64
	// URLSecret is the credential already included in URL. When set, persist
	// only a digest of the request URL, omit redirect Referer, and suppress
	// credential-bearing validators and error text.
	URLSecret string
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
// Upstream: CTY, IPinfo, FCC, ISED and RBN replay.
// Downstream: HTTP client, metadata read/write helpers.
func Download(ctx context.Context, req Request) (Result, error) {
	result, err := download(ctx, req, fsutil.ReplaceFile)
	if err != nil && req.URLSecret != "" {
		err = redactDownloadError(err, req.URLSecret)
	}
	return result, err
}

func download(ctx context.Context, req Request, replace func(string, string) error) (Result, error) {
	var result Result
	endpoint := strings.TrimSpace(req.URL)
	dest := strings.TrimSpace(req.Destination)
	if endpoint == "" {
		return result, errors.New("download: URL is empty")
	}
	if dest == "" {
		return result, errors.New("download: destination is empty")
	}
	if req.MaxBytes < 0 {
		return result, errors.New("download: MaxBytes must be >= 0")
	}
	identity := endpoint
	if req.URLSecret != "" {
		identity = fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(endpoint)))
	}
	metaPath := strings.TrimSpace(req.MetadataPath)
	if metaPath == "" {
		metaPath = MetadataPath(dest)
	}

	_, err := os.Stat(dest)
	destExists := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, fmt.Errorf("download: stat destination: %w", err)
	}
	prevMeta, metaSource := ReadMetadata(append([]string{metaPath}, req.LegacyMetadataPaths...)...)
	force := req.Force || (!destExists && !req.AllowMissingDestination)
	validatorMeta := prevMeta
	if prevMeta != nil {
		clearSecretValidators(prevMeta, req.URLSecret)
		if prevMeta.URL != identity {
			force = force || prevMeta.URL != ""
			validatorMeta = nil
		}
	}

	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}
	resp, evidence, err := fetch(ctx, endpoint, req, validatorMeta, force)
	if err != nil {
		return result, err
	}
	if resp.StatusCode == http.StatusNotModified {
		allowMissing := req.AllowMissingDestination && !req.RequireByteExact304
		validatorOK := evidence.conditional
		if req.RequireByteExact304 {
			validatorOK = validatorOK && evidence.direct && prevMeta != nil &&
				prevMeta.ByteExactETag != "" && responseStrongETag(resp) == prevMeta.ByteExactETag
		}
		verified := validatorOK && ((!destExists && allowMissing) ||
			(destExists && prevMeta != nil && retainedFileMatches(ctx, dest, prevMeta.SHA256, prevMeta.SizeBytes, req.MaxBytes)))
		if err := ctx.Err(); err != nil {
			resp.Body.Close()
			return result, err
		}
		if verified {
			defer resp.Body.Close()
			result.Status = StatusNotModified
			result.Meta = mergeMetadata(prevMeta, identity, resp, time.Now().UTC(), false, "")
			if !req.RequireByteExact304 {
				result.Meta.ByteExactETag = ""
			}
			saveDownloadMetadata(req, metaPath, metaSource, result.Meta, replace)
			return result, nil
		}
		resp.Body.Close()
		if !evidence.conditional {
			return result, errors.New("download: unexpected 304 without a conditional request")
		}
		// Neither the validator nor the retained file proved this response.
		// Fetch once without validators under the original deadline.
		resp, evidence, err = fetch(ctx, endpoint, req, nil, true)
		if err != nil {
			return result, err
		}
		force = true
	}
	defer resp.Body.Close()
	result, err = publishResponse(ctx, resp, dest, req.MaxBytes, prevMeta, !force && destExists, replace)
	if err != nil {
		return result, err
	}
	now := time.Now().UTC()
	result.Meta = mergeMetadata(prevMeta, identity, resp, now, result.Status == StatusUpdated, result.Meta.SHA256)
	result.Meta.SizeBytes = result.Bytes
	result.Meta.ValidatorResource = evidence.resource
	result.Meta.ByteExactETag = ""
	if req.RequireByteExact304 && evidence.direct {
		result.Meta.ByteExactETag = responseStrongETag(resp)
	}
	clearSecretValidators(&result.Meta, req.URLSecret)
	saveDownloadMetadata(req, metaPath, metaSource, result.Meta, replace)
	return result, nil
}

type fetchEvidence struct {
	conditional bool
	direct      bool
	resource    string
}

func fetch(ctx context.Context, endpoint string, opts Request, previous *Metadata, force bool) (*http.Response, fetchEvidence, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fetchEvidence{}, fmt.Errorf("download: build request: %w", err)
	}
	if force {
		previous = nil
	}
	setDownloadValidators(req, opts.RequireByteExact304, previous, true)
	if opts.UserAgent != "" {
		req.Header.Set("User-Agent", opts.UserAgent)
	}
	client := &http.Client{
		CheckRedirect: func(next *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			// Go copies the initial headers before this hook. Recompute them
			// for this hop so matching tag text at a different URL is irrelevant.
			setDownloadValidators(next, opts.RequireByteExact304, previous, false)
			if opts.URLSecret != "" {
				next.Header.Del("Referer")
			}
			return nil
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fetchEvidence{}, fmt.Errorf("download: fetch failed: %w", err)
	}
	final := resp.Request
	if final == nil {
		return resp, fetchEvidence{}, nil
	}
	return resp, fetchEvidence{
		conditional: final.Header.Get("If-None-Match") != "" || final.Header.Get("If-Modified-Since") != "",
		direct:      final.Response == nil,
		resource:    validatorResource(final.URL),
	}, nil
}

func setDownloadValidators(req *http.Request, exact bool, previous *Metadata, direct bool) {
	req.Header.Del("If-None-Match")
	req.Header.Del("If-Modified-Since")
	if previous == nil {
		return
	}
	if exact {
		if direct && isStrongETag(previous.ByteExactETag) {
			req.Header.Set("If-None-Match", previous.ByteExactETag)
		}
		return
	}
	if previous.ValidatorResource == "" || previous.ValidatorResource != validatorResource(req.URL) {
		return
	}
	if isEntityTag(previous.ETag) {
		req.Header.Set("If-None-Match", previous.ETag)
	}
	if isHTTPDate(previous.LastModified) {
		req.Header.Set("If-Modified-Since", previous.LastModified)
	}
}

func validatorResource(resource *url.URL) string {
	identity := *resource
	identity.Fragment, identity.RawFragment = "", ""
	hash := sha256.Sum256([]byte(identity.String()))
	return hex.EncodeToString(hash[:])
}

func singleResponseHeader(resp *http.Response, name string) string {
	values := resp.Header.Values(name)
	if len(values) != 1 {
		return ""
	}
	return strings.Trim(values[0], " \t")
}

func responseStrongETag(resp *http.Response) string {
	value := singleResponseHeader(resp, "ETag")
	if !isStrongETag(value) {
		return ""
	}
	return value
}

func isEntityTag(value string) bool {
	return isStrongETag(strings.TrimPrefix(value, "W/"))
}

func isHTTPDate(value string) bool {
	_, err := http.ParseTime(value)
	return err == nil
}

// RFC 9110 entity-tag syntax for a single strong tag. The quoted contents are
// opaque, including any backslash. ASCII space, controls and quotes are excluded.
func isStrongETag(value string) bool {
	if len(value) < 2 || value[0] != '"' || value[len(value)-1] != '"' {
		return false
	}
	for i := 1; i < len(value)-1; i++ {
		if b := value[i]; b < '!' || b == '"' || b == 0x7f {
			return false
		}
	}
	return true
}

func publishResponse(ctx context.Context, resp *http.Response, dest string, maxBytes int64, previous *Metadata, compare bool, replace func(string, string) error) (Result, error) {
	var result Result
	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("download: expected a complete HTTP 200 response: %s", resp.Status)
	}
	if len(resp.Header.Values("Content-Range")) != 0 {
		return result, errors.New("download: HTTP 200 carried a Content-Range header")
	}
	if maxBytes > 0 && resp.ContentLength > maxBytes {
		return result, fmt.Errorf("download: body exceeds MaxBytes (%d)", maxBytes)
	}
	if err := fsutil.EnsureParentDir(dest, "download: create directory"); err != nil {
		return result, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dest), "download-*.tmp")
	if err != nil {
		return result, fmt.Errorf("download: create temp file: %w", err)
	}
	defer os.Remove(tmp.Name())

	hasher := sha256.New()
	written, copyErr := copyDownloadBody(io.MultiWriter(tmp, hasher), resp.Body, maxBytes)
	closeErr := tmp.Close()
	if copyErr != nil {
		return result, fmt.Errorf("download: copy body: %w", copyErr)
	}
	if closeErr != nil {
		return result, fmt.Errorf("download: finalize temp file: %w", closeErr)
	}
	if written <= 0 {
		return result, errors.New("download: empty response body")
	}

	hash := hex.EncodeToString(hasher.Sum(nil))
	same := compare && previous != nil && previous.SHA256 == hash &&
		retainedFileMatches(ctx, dest, hash, written, maxBytes)
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("download: canceled before publication: %w", err)
	}
	result.Bytes = written
	result.Meta.SHA256 = hash
	if same {
		result.Status = StatusSameContent
		return result, nil
	}
	if err := replace(tmp.Name(), dest); err != nil {
		return result, fmt.Errorf("download: replace file: %w", err)
	}
	result.Status = StatusUpdated
	return result, nil
}

// Read only a regular file of the expected size. The size observed on the open
// handle limits verification work even if the file changes during the read.
func retainedFileMatches(ctx context.Context, path, expectedHash string, expectedSize, maxBytes int64) bool {
	if len(expectedHash) != sha256.Size*2 || ctx.Err() != nil {
		return false
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return false
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	info, err = f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 ||
		(expectedSize > 0 && info.Size() != expectedSize) || (maxBytes > 0 && info.Size() > maxBytes) {
		return false
	}

	hash := sha256.New()
	size, err := copyDownloadBody(hash, cancelableReader{ctx, f}, info.Size())
	return err == nil && size == info.Size() && hex.EncodeToString(hash.Sum(nil)) == expectedHash
}

type cancelableReader struct {
	ctx context.Context
	src io.Reader
}

func (r cancelableReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.src.Read(p)
}

func saveDownloadMetadata(req Request, path, previousPath string, meta Metadata, replace func(string, string) error) {
	if err := writeMetadata(path, meta, replace); err != nil {
		log.Printf("Warning: unable to write metadata %s: %v", path, err)
	} else if !req.DisableLegacyCleanup {
		cleanupLegacyMetadata(previousPath, path)
	}
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
// Downstream: same-directory staging and replacement.
func WriteMetadata(path string, meta Metadata) error {
	return writeMetadata(path, meta, fsutil.ReplaceFile)
}

func writeMetadata(path string, meta Metadata, replace func(string, string) error) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("download: metadata path is empty")
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".download-metadata-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	_, writeErr := temp.Write(data)
	if err := errors.Join(writeErr, temp.Close()); err != nil {
		return err
	}
	return replace(temp.Name(), path)
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
	meta.CheckedAt = now
	meta.UpToDate = true
	if resp.StatusCode == http.StatusOK {
		meta.ETag, meta.LastModified = "", ""
		if tag := singleResponseHeader(resp, "ETag"); isEntityTag(tag) {
			meta.ETag = tag
		}
		if date := singleResponseHeader(resp, "Last-Modified"); isHTTPDate(date) {
			meta.LastModified = date
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
