package gridstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
)

// IntegrityStats reports the outcome of a full key/value scan.
type IntegrityStats struct {
	Records        int64
	Duration       time.Duration
	CountMeta      int64
	CountMetaValid bool
	CountMetaErr   error
}

// Checkpoint creates a Pebble checkpoint on disk with a flushed WAL.
// Key aspects: Requires a non-empty destination path and uses Pebble's checkpoint.
// Upstream: Scheduled gridstore checkpointing.
// Downstream: Pebble DB.Checkpoint.
func (s *Store) Checkpoint(dest string) error {
	if s == nil || s.db == nil {
		return errors.New("gridstore: store is not initialized")
	}
	if strings.TrimSpace(dest) == "" {
		return errors.New("gridstore: checkpoint destination is empty")
	}
	if err := s.db.Checkpoint(dest, pebble.WithFlushedWAL()); err != nil {
		return fmt.Errorf("gridstore: checkpoint %s: %w", dest, err)
	}
	return nil
}

// VerifyCheckpoint verifies checkpoint integrity by opening it read-only and scanning entries.
// Key aspects: Honors context cancellation and maxDuration for bounded scans.
// Upstream: Corruption recovery on startup.
// Downstream: Pebble iterator and decodeRecordValue.
func VerifyCheckpoint(ctx context.Context, path string, maxDuration time.Duration) (IntegrityStats, error) {
	if strings.TrimSpace(path) == "" {
		return IntegrityStats{}, errors.New("gridstore: checkpoint path is empty")
	}
	info, err := os.Stat(path)
	if err != nil {
		return IntegrityStats{}, fmt.Errorf("gridstore: checkpoint stat %s: %w", path, err)
	}
	if !info.IsDir() {
		return IntegrityStats{}, fmt.Errorf("gridstore: checkpoint %s is not a directory", path)
	}
	db, err := pebble.Open(path, &pebble.Options{ReadOnly: true})
	if err != nil {
		return IntegrityStats{}, fmt.Errorf("gridstore: checkpoint open %s: %w", path, err)
	}
	defer db.Close()
	stats, err := verifyDB(ctx, db, maxDuration)
	if err != nil {
		return stats, fmt.Errorf("gridstore: checkpoint verify %s: %w", path, err)
	}
	return stats, nil
}

// Verify verifies the active store via a bounded full-table scan.
// Key aspects: Decodes each record and returns stats for logging.
// Upstream: Daily integrity scan scheduler.
// Downstream: Pebble iterator and decodeRecordValue.
func (s *Store) Verify(ctx context.Context, maxDuration time.Duration) (IntegrityStats, error) {
	if s == nil || s.db == nil {
		return IntegrityStats{}, errors.New("gridstore: store is not initialized")
	}
	return verifyDB(ctx, s.db, maxDuration)
}

func verifyDB(ctx context.Context, db *pebble.DB, maxDuration time.Duration) (IntegrityStats, error) {
	start := time.Now().UTC()
	deadline := time.Time{}
	if maxDuration > 0 {
		deadline = start.Add(maxDuration)
	}
	stats := IntegrityStats{}
	if db == nil {
		return stats, errors.New("gridstore: database is nil")
	}
	if count, err := readCountMeta(db); err == nil {
		stats.CountMeta = count
		stats.CountMetaValid = true
	} else if !errors.Is(err, pebble.ErrNotFound) && !errors.Is(err, errInvalidCount) {
		stats.CountMetaErr = err
	} else {
		stats.CountMetaErr = err
	}

	iter, err := db.NewIter(iterOptionsForPrefix(callPrefix))
	if err != nil {
		return stats, fmt.Errorf("gridstore: verify iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return stats, ctx.Err()
			default:
			}
		}
		if !deadline.IsZero() && time.Now().UTC().After(deadline) {
			return stats, errors.New("gridstore: integrity scan timed out")
		}
		if _, err := decodeRecordValue(iter.Value()); err != nil {
			return stats, fmt.Errorf("gridstore: verify decode: %w", err)
		}
		stats.Records++
	}
	if err := iter.Error(); err != nil {
		return stats, fmt.Errorf("gridstore: verify iterate: %w", err)
	}
	stats.Duration = time.Since(start)
	return stats, nil
}

