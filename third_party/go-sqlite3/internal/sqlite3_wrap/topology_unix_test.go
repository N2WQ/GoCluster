//go:build unix

package sqlite3_wrap

import (
	"errors"
	"github.com/ncruces/go-sqlite3/internal/errutil"
	"golang.org/x/sys/unix"
	"testing"
	"unsafe"
)

func TestV15SQLiteNativeAllocationOOMOwnership(t *testing.T) {
	originalAllocate, originalCommit := allocateNativeMemory, commitNativeMemory
	defer func() { allocateNativeMemory, commitNativeMemory = originalAllocate, originalCommit }()
	for _, phase := range []string{"reserve", "commit"} {
		m := &Memory{Max: 128}
		allocateNativeMemory, commitNativeMemory = originalAllocate, originalCommit
		if phase == "reserve" {
			allocateNativeMemory = func(int, int64, int, int, int) ([]byte, error) { return nil, unix.ENOMEM }
		} else {
			commitNativeMemory = func([]byte, int) error { return unix.ENOMEM }
		}
		var failure any
		func() { defer func() { failure = recover() }(); m.Grow(5, 128) }()
		if failure != errutil.OOMErr {
			t.Fatalf("phase %s returned wrong allocation failure: %v", phase, failure)
		}
		if phase == "commit" && (m.Buf == nil || m.Accounting.EngineNative.Load() != 8<<20) {
			t.Fatal("commit failure lost native reservation")
		}
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		if m.Accounting.EngineBacking.Load() != 0 {
			t.Fatal("failed allocation did not retire")
		}
	}
}

func TestV15SQLiteEngineBackingBound(t *testing.T) {
	m := &Memory{Max: 128}
	if m.Grow(5, 128) != 0 {
		t.Fatal("initial growth")
	}
	defer m.Close()
	base := unsafe.SliceData(m.Buf)
	for pages := int64(5); pages < 128; pages++ {
		if got := m.Grow(1, 128); got != pages {
			t.Fatalf("growth=%d want %d", got, pages)
		}
		if cap(m.Buf) != 8<<20 || unsafe.SliceData(m.Buf) != base {
			t.Fatal("backing moved or exceeded allowance")
		}
	}
	if m.Grow(1, 128) != -1 {
		t.Fatal("over-limit growth admitted")
	}
}

func TestV15SQLiteNativeReleaseOwnership(t *testing.T) {
	m := &Memory{Max: 128}
	m.Grow(5, 128)
	base := unsafe.SliceData(m.Buf)
	original := releaseNativeMemory
	releaseNativeMemory = func([]byte) error { return errors.New("injected unmap failure") }
	defer func() { releaseNativeMemory = original; m.Close() }()
	if err := m.Close(); err == nil || unsafe.SliceData(m.Buf) != base || m.Accounting.EngineNative.Load() != 8<<20 {
		t.Fatal("failed release forgot real extent or charge")
	}
	m.Buf[0] = 37
	if *base != 37 {
		t.Fatal("retained extent was not real")
	}
	releaseNativeMemory = original
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if m.Buf != nil || m.Accounting.EngineNative.Load() != 0 {
		t.Fatal("retired owner remains charged")
	}
}
