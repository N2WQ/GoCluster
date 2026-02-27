package pebbleresilience

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// CheckpointEntry describes one on-disk checkpoint directory.
type CheckpointEntry struct {
	Path string
	At   time.Time
}

// CheckpointRoot returns the checkpoint root directory under a DB path.
func CheckpointRoot(dbPath string, checkpointDir string) string {
	return filepath.Join(dbPath, checkpointDir)
}

// ListCheckpoints returns checkpoint directories sorted newest-first.
func ListCheckpoints(root string, nameLayout string) ([]CheckpointEntry, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var checkpoints []CheckpointEntry
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".tmp-") {
			continue
		}
		at, err := time.Parse(nameLayout, name)
		if err != nil {
			continue
		}
		checkpoints = append(checkpoints, CheckpointEntry{
			Path: filepath.Join(root, name),
			At:   at,
		})
	}
	sort.Slice(checkpoints, func(i, j int) bool {
		return checkpoints[i].At.After(checkpoints[j].At)
	})
	return checkpoints, nil
}

// CleanupCheckpoints removes checkpoints older than retention.
func CleanupCheckpoints(root string, nameLayout string, retention time.Duration, now time.Time) (int, error) {
	if root == "" || retention <= 0 {
		return 0, nil
	}
	checkpoints, err := ListCheckpoints(root, nameLayout)
	if err != nil {
		return 0, err
	}
	cutoff := now.Add(-retention)
	removed := 0
	for _, checkpoint := range checkpoints {
		if checkpoint.At.Before(cutoff) {
			if err := os.RemoveAll(checkpoint.Path); err != nil {
				return removed, err
			}
			removed++
		}
	}
	return removed, nil
}

// RestoreFromCheckpointDir restores dbPath from checkpointPath using an atomic swap.
// It preserves checkpoint history by carrying forward the checkpoint directory from
// the previous DB when the restored DB does not already have one.
func RestoreFromCheckpointDir(
	ctx context.Context,
	dbPath string,
	checkpointPath string,
	nameLayout string,
	checkpointDir string,
) error {
	if strings.TrimSpace(dbPath) == "" {
		return errors.New("pebbleresilience: db path is empty")
	}
	if strings.TrimSpace(checkpointPath) == "" {
		return errors.New("pebbleresilience: checkpoint path is empty")
	}
	if ctx == nil {
		return errors.New("pebbleresilience: nil context")
	}
	parent := filepath.Dir(dbPath)
	base := filepath.Base(dbPath)
	if strings.TrimSpace(base) == "" || strings.TrimSpace(parent) == "" {
		return fmt.Errorf("pebbleresilience: invalid db path %q", dbPath)
	}

	ts := time.Now().UTC().Format(nameLayout)
	tempDir := filepath.Join(parent, base+".restore-"+ts)
	backupDir := filepath.Join(parent, base+".backup-"+ts)
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return fmt.Errorf("pebbleresilience: create restore dir: %w", err)
	}
	if err := CopyDirCtx(ctx, checkpointPath, tempDir); err != nil {
		_ = os.RemoveAll(tempDir)
		return err
	}
	if ctx.Err() != nil {
		_ = os.RemoveAll(tempDir)
		return ctx.Err()
	}

	hadExisting := false
	if info, err := os.Stat(dbPath); err == nil {
		if !info.IsDir() {
			_ = os.RemoveAll(tempDir)
			return fmt.Errorf("pebbleresilience: db path %s is not a directory", dbPath)
		}
		hadExisting = true
	} else if !os.IsNotExist(err) {
		_ = os.RemoveAll(tempDir)
		return fmt.Errorf("pebbleresilience: stat db path: %w", err)
	}

	if hadExisting {
		if err := os.Rename(dbPath, backupDir); err != nil {
			_ = os.RemoveAll(tempDir)
			return fmt.Errorf("pebbleresilience: backup existing db: %w", err)
		}
	}
	if err := os.Rename(tempDir, dbPath); err != nil {
		if hadExisting {
			_ = os.Rename(backupDir, dbPath)
		}
		_ = os.RemoveAll(tempDir)
		return fmt.Errorf("pebbleresilience: finalize restore dir: %w", err)
	}

	if hadExisting {
		backupCheckpoint := filepath.Join(backupDir, checkpointDir)
		newCheckpoint := filepath.Join(dbPath, checkpointDir)
		if _, err := os.Stat(backupCheckpoint); err == nil {
			if _, err := os.Stat(newCheckpoint); os.IsNotExist(err) {
				_ = os.Rename(backupCheckpoint, newCheckpoint)
			}
		}
		_ = os.RemoveAll(backupDir)
	}
	return nil
}

// CopyDirCtx recursively copies one directory into another and honors cancellation.
func CopyDirCtx(ctx context.Context, src string, dst string) error {
	return filepath.WalkDir(src, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		target := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		return copyFileCtx(ctx, path, target, info.Mode())
	})
}

func copyFileCtx(ctx context.Context, src string, dst string, mode os.FileMode) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer func() {
		closeErr := out.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			_ = os.Remove(dst)
		}
	}()

	buf := make([]byte, 128*1024)
	for {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		n, readErr := in.Read(buf)
		if n > 0 {
			if _, writeErr := out.Write(buf[:n]); writeErr != nil {
				return writeErr
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return readErr
		}
	}
	return out.Sync()
}
