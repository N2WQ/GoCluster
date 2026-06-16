package pebbleutil

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/pebble"
)

// ErrEmptyPath identifies a Pebble directory path that is empty after
// whitespace trimming.
var ErrEmptyPath = errors.New("pebble db path is empty")

// ErrNotDirectory identifies an existing Pebble path that is not a directory.
var ErrNotDirectory = errors.New("pebble db path exists and is not a directory")

// DirOp names the filesystem operation that failed while preparing a Pebble
// directory.
type DirOp string

const (
	// DirOpStat means os.Stat failed for a reason other than non-existence.
	DirOpStat DirOp = "stat"
	// DirOpMkdir means os.MkdirAll failed while creating the directory.
	DirOpMkdir DirOp = "mkdir"
)

// DirOpError wraps filesystem failures from PrepareDir so callers can preserve
// component-specific error wording while sharing the same path checks.
type DirOpError struct {
	Op   DirOp
	Path string
	Err  error
}

func (e *DirOpError) Error() string {
	switch e.Op {
	case DirOpStat:
		return fmt.Sprintf("stat path: %v", e.Err)
	case DirOpMkdir:
		return fmt.Sprintf("mkdir: %v", e.Err)
	default:
		return e.Err.Error()
	}
}

func (e *DirOpError) Unwrap() error {
	return e.Err
}

// NotDirectoryError carries the path that blocked Pebble directory creation.
type NotDirectoryError struct {
	Path string
}

func (e *NotDirectoryError) Error() string {
	return fmt.Sprintf("%s exists and is not a directory", e.Path)
}

func (e *NotDirectoryError) Is(target error) bool {
	return target == ErrNotDirectory
}

// PrepareDir trims, validates, and creates a directory suitable for opening a
// Pebble database. It deliberately does not delete paths or recover corruption;
// those policies belong to the owning store.
func PrepareDir(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", ErrEmptyPath
	}
	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			return "", &NotDirectoryError{Path: path}
		}
	} else if !os.IsNotExist(err) {
		return "", &DirOpError{Op: DirOpStat, Path: path, Err: err}
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return "", &DirOpError{Op: DirOpMkdir, Path: path, Err: err}
	}
	return path, nil
}

// Open forwards to pebble.Open after defaulting nil options. Callers retain
// ownership of any resources held by opts, including Pebble caches.
func Open(path string, opts *pebble.Options) (*pebble.DB, error) {
	if opts == nil {
		opts = &pebble.Options{}
	}
	return pebble.Open(path, opts)
}
