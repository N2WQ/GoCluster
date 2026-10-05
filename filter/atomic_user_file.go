package filter

import (
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
)

// The callbacks are a private fault-injection seam, not mutable global hooks.
// The target remains untouched until a synced, closed temporary file replaces it.
func writeAtomicUserFile(path string, data []byte) error {
	return writeAtomicUserFileWith(path, data, writeUserFileBytes, replaceUserFile)
}

func writeUserFileBytes(file *os.File, data []byte) error {
	n, err := file.Write(data)
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}
	return err
}

func writeAtomicUserFileWith(path string, data []byte, write func(*os.File, []byte) error, replace func(string, string) error) error {
	return writeAtomicUserFileWithStages(path, data, atomicUserFileStages{
		write: write, sync: (*os.File).Sync, close: (*os.File).Close, replace: replace,
	})
}

// Stages are per-call fault seams for the four commit boundaries. They do not
// retain filesystem handles or install mutable process-wide hooks.
type atomicUserFileStages struct {
	write   func(*os.File, []byte) error
	sync    func(*os.File) error
	close   func(*os.File) error
	replace func(string, string) error
}

func writeAtomicUserFileWithStages(path string, data []byte, stages atomicUserFileStages) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".preset-*.tmp")
	if err != nil {
		return err
	}
	temporary := file.Name()
	closed, committed := false, false
	defer func() {
		if !closed {
			_ = file.Close()
		}
		if !committed {
			if err := os.Remove(temporary); err != nil && !errors.Is(err, os.ErrNotExist) {
				log.Printf("Warning: failed to remove preset temporary file: %v", err)
			}
		}
	}()
	if err := file.Chmod(0o644); err != nil {
		return err
	}
	if err := stages.write(file, data); err != nil {
		return err
	}
	if err := stages.sync(file); err != nil {
		return err
	}
	if err := stages.close(file); err != nil {
		return err
	}
	closed = true
	if err := stages.replace(temporary, path); err != nil {
		return err
	}
	committed = true
	return nil
}
