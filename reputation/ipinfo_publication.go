package reputation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"dxcluster/internal/fsutil"
)

const ipinfoVerificationFile = "VERIFIED_SNAPSHOT.json"
const maxIPInfoRecordBytes = 4096

// ipinfoSnapshot is an immutable view for cache hits and IPv4 index reads. The
// Pebble handle stays separately owned under ipinfoStoreMu and is never pinned
// by a cached result. A new epoch makes old cache entries ineligible on read.
type ipinfoSnapshot struct {
	index     *ipinfoIndex
	path      string
	hash      string
	currentAt time.Time
	epoch     uint64
}

func (s *ipinfoSnapshot) current(now time.Time, maxAge time.Duration) bool {
	return maxAge == 0 || now.Sub(s.currentAt) <= maxAge
}

func (s *ipinfoSnapshot) lookupEpoch() uint64 {
	if s == nil {
		return 0
	}
	return s.epoch
}

type ipinfoVerification struct {
	Database   string    `json:"database"`
	SHA256     string    `json:"sha256"`
	VerifiedAt time.Time `json:"verified_at"`
}

// Build time remains available for legacy and manually supplied databases. A
// verification record only speaks for the database and exact archive it names.
func ipinfoCurrentAt(root string, store *ipinfoStore, now time.Time) time.Time {
	data, err := readIPInfoRecord(filepath.Join(root, ipinfoVerificationFile))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("Warning: ignoring IPinfo verification record: %v", err)
		}
		return store.builtAt
	}
	var record ipinfoVerification
	if err := json.Unmarshal(data, &record); err != nil {
		log.Printf("Warning: ignoring invalid IPinfo verification record: %v", err)
		return store.builtAt
	}
	database, err := filepath.Rel(root, store.path)
	if err != nil || record.Database != database || store.sourceHash == "" ||
		record.SHA256 != store.sourceHash || record.VerifiedAt.Before(store.builtAt) ||
		record.VerifiedAt.After(now) {
		return store.builtAt
	}
	return record.VerifiedAt
}

func readIPInfoRecord(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxIPInfoRecordBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxIPInfoRecordBytes {
		return nil, errors.New("IPinfo record exceeds maximum length")
	}
	return data, nil
}

func writeIPInfoRecord(ctx context.Context, path string, data []byte) error {
	if len(data) > maxIPInfoRecordBytes {
		return errors.New("IPinfo record exceeds maximum length")
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".ipinfo-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, writeErr := f.Write(data)
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fsutil.ReplaceFile(f.Name(), path)
}

func (g *Gate) prepareIPInfoStore(ctx context.Context, path string) (*ipinfoStore, *ipinfoSnapshot, error) {
	store, err := openIPInfoStore(path, g.cfg.ipinfoPebbleCacheBytes)
	if err != nil {
		return nil, nil, err
	}
	view := &ipinfoSnapshot{
		path:      store.path,
		hash:      store.sourceHash,
		currentAt: ipinfoCurrentAt(g.cfg.ipinfoPebblePath, store, time.Now().UTC()),
	}
	if g.cfg.ipinfoPebbleLoadIPv4 {
		view.index, err = loadIPv4IndexFromStore(ctx, store)
		if err != nil {
			store.Close()
			return nil, nil, err
		}
	}
	return store, view, nil
}

// The refresh mutex serializes preparation and cleanup. The separate commit
// mutex fences Close against publication without making it wait for download,
// import or index preparation. Lookup locks never cover filesystem writes.
func (g *Gate) publishIPInfoStore(ctx context.Context, store *ipinfoStore, view *ipinfoSnapshot, setCurrent bool) error {
	g.ipinfoCommitMu.Lock()
	defer g.ipinfoCommitMu.Unlock()
	if err := g.canPublishIPInfo(ctx); err != nil {
		return err
	}
	if setCurrent {
		if err := updateIPInfoPebbleCurrent(ctx, g.cfg.ipinfoPebblePath, store.path); err != nil {
			return fmt.Errorf("publish IPinfo current database: %w", err)
		}
	}
	g.ipinfoEpoch++
	view.epoch = g.ipinfoEpoch
	g.ipinfoStoreMu.Lock()
	old := g.ipinfoStore
	g.ipinfoStore = store
	g.ipinfoView.Store(view)
	g.ipinfoStoreMu.Unlock()
	if old != nil {
		old.Close()
	}
	return nil
}

func (g *Gate) canPublishIPInfo(ctx context.Context) error {
	if g.ipinfoClosed {
		return errors.New("IPinfo store is closed")
	}
	return ctx.Err()
}

func (g *Gate) renewIPInfo(ctx context.Context, active *ipinfoSnapshot, checkedAt time.Time) error {
	g.ipinfoCommitMu.Lock()
	defer g.ipinfoCommitMu.Unlock()
	if err := g.canPublishIPInfo(ctx); err != nil {
		return err
	}
	if !checkedAt.After(active.currentAt) {
		return nil
	}
	root := g.cfg.ipinfoPebblePath
	database, err := filepath.Rel(root, active.path)
	if err != nil {
		return err
	}
	record, err := json.Marshal(ipinfoVerification{Database: database, SHA256: active.hash, VerifiedAt: checkedAt})
	if err != nil {
		return err
	}
	if err := writeIPInfoRecord(ctx, filepath.Join(root, ipinfoVerificationFile), record); err != nil {
		return fmt.Errorf("publish IPinfo verification record: %w", err)
	}
	next := *active
	next.currentAt = checkedAt
	// A lookup can start using fallback while a verification record is being
	// written. Retire the old epoch even if the old view was current at fetch.
	g.ipinfoEpoch++
	next.epoch = g.ipinfoEpoch
	g.ipinfoView.Store(&next)
	return nil
}
