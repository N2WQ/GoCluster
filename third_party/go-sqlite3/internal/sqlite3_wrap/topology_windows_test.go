package sqlite3_wrap

import (
	"errors"
	"github.com/ncruces/go-sqlite3/internal/errutil"
	"os"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

const memFree = 0x10000 // MEMORY_BASIC_INFORMATION.State: MEM_FREE.

func TestV15SQLiteNativeAllocationOOMOwnership(t *testing.T) {
	original := allocateNativeMemory
	defer func() { allocateNativeMemory = original }()
	for _, failAt := range []int{1, 2} {
		m := &Memory{Max: 128}
		calls := 0
		allocateNativeMemory = func(address, size uintptr, kind, prot uint32) (uintptr, error) {
			calls++
			if calls == failAt {
				return 0, windows.ERROR_NOT_ENOUGH_MEMORY
			}
			return original(address, size, kind, prot)
		}
		var failure any
		func() { defer func() { failure = recover() }(); m.Grow(5, 128) }()
		if failure != errutil.OOMErr {
			t.Fatalf("phase %d returned wrong allocation failure: %v", failAt, failure)
		}
		if failAt == 2 && (m.ptr == 0 || m.Accounting.EngineNative.Load() != 8<<20) {
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
	for _, fallback := range []bool{false, true} {
		m := &Memory{Max: 128, ForceFallback: fallback}
		if m.Grow(5, 128) != 0 {
			t.Fatal("initial growth")
		}
		base := unsafe.SliceData(m.Buf)
		for pages := int64(5); pages < 128; pages++ {
			if got := m.Grow(1, 128); got != pages {
				t.Fatalf("growth=%d want%d", got, pages)
			}
			if cap(m.Buf) != 8<<20 || unsafe.SliceData(m.Buf) != base {
				t.Fatal("backing moved or exceeded allowance")
			}
		}
		if m.Grow(1, 128) != -1 {
			t.Fatal("over-limit growth admitted")
		}
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestV15SQLiteNativeReleaseOwnership(t *testing.T) {
	m := &Memory{Max: 128}
	m.Grow(5, 128)
	if m.ptr == 0 {
		t.Fatal("native fixture did not reserve memory")
	}
	base := m.ptr
	original := releaseNativeMemory
	releaseNativeMemory = func(uintptr, uintptr, uint32) error { return errors.New("injected free failure") }
	defer func() { releaseNativeMemory = original }()
	if err := m.Close(); err == nil || m.ptr != base || m.Buf == nil {
		t.Fatal("failed release forgot extent")
	}
	var info windows.MemoryBasicInformation
	if err := windows.VirtualQuery(base, &info, unsafe.Sizeof(info)); err != nil || info.State == memFree {
		t.Fatal("failed release did not leave real occupied extent")
	}
	releaseNativeMemory = original
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := windows.VirtualQuery(base, &info, unsafe.Sizeof(info)); err != nil || info.State != memFree {
		t.Fatal("retired native extent remains occupied")
	}
}

func TestV15SQLiteFallbackMappingAlignmentAndGlobalBudget(t *testing.T) {
	w := &Wrapper{Memory: &Memory{Max: 128, ForceFallback: true}}
	w.Grow(5, 128)
	defer w.Close()
	a, err := os.CreateTemp(t.TempDir(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := os.CreateTemp(t.TempDir(), "b")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for i := 0; i < 64; i++ {
		f := a
		if i&1 != 0 {
			f = b
		}
		r, err := w.MapFallback(f, int64(i)*FallbackPageBytes)
		if err != nil || r == nil {
			t.Fatalf("region%d: %v", i, err)
		}
		if r.addr%allocationGranularity != 0 || len(r.Data) != FallbackPageBytes {
			t.Fatal("bad mapping alignment")
		}
		r.Data[0] = byte(i + 1)
		var got [1]byte
		if _, err = f.ReadAt(got[:], int64(i)*FallbackPageBytes); err != nil || got[0] != byte(i+1) {
			t.Fatalf("mapped wrong region%d: %v", i, err)
		}
	}
	if r, err := w.MapFallback(a, 64*FallbackPageBytes); r != nil || err != nil {
		t.Fatalf("global capacity admitted: %v %v", r, err)
	}
	if err = w.fallback[7].Close(); err != nil {
		t.Fatal(err)
	}
	if r, err := w.MapFallback(b, 64*FallbackPageBytes); r == nil || err != nil {
		t.Fatalf("released slot not reusable: %v", err)
	}
}

func TestV15SQLiteFallbackFailedReleaseRetainsCharge(t *testing.T) {
	w := &Wrapper{Memory: &Memory{Max: 128, ForceFallback: true}}
	w.Grow(5, 128)
	f, err := os.CreateTemp(t.TempDir(), "mapped")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	originalUnmap, originalClose := unmapFallbackView, closeFallbackHandle
	defer func() { unmapFallbackView, closeFallbackHandle = originalUnmap, originalClose; w.Close() }()
	r, err := w.MapFallback(f, 0)
	if err != nil {
		t.Fatal(err)
	}
	unmapFallbackView = func(uintptr) error { return errors.New("injected unmap failure") }
	if err = r.Close(); err == nil || r.addr == 0 || w.Accounting.WALViews.Load() != allocationGranularity || w.Accounting.WALShadows.Load() != FallbackPageBytes || w.Accounting.WALSlots.Load() != 1 {
		t.Fatal("failed unmap lost ownership or charge")
	}
	r.Data[0] = 31
	var got [1]byte
	if _, err = f.ReadAt(got[:], 0); err != nil || got[0] != 31 {
		t.Fatal("retained mapping not real", err)
	}
	unmapFallbackView = originalUnmap
	closeFallbackHandle = func(windows.Handle) error { return errors.New("injected mapping handle close failure") }
	if err = r.Close(); err == nil || r.addr != 0 || r.handle == 0 || w.Accounting.WALViews.Load() != 0 || w.Accounting.WALShadows.Load() != FallbackPageBytes || w.Accounting.WALSlots.Load() != 1 {
		t.Fatal("partial release forgot mapping handle/shadow owner")
	}
	closeFallbackHandle = originalClose
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if w.Accounting.WALSlots.Load() != 0 || w.Accounting.WALShadows.Load() != 0 {
		t.Fatal("confirmed release remains charged")
	}
}

func TestV15SQLiteNativeMappingHandleReleaseOwnership(t *testing.T) {
	w := &Wrapper{Memory: &Memory{Max: 128}}
	w.Grow(5, 128)
	f, err := os.CreateTemp(t.TempDir(), "mapped")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	original := closeMappedHandle
	defer func() { closeMappedHandle = original; w.Close() }()
	closeMappedHandle = func(windows.Handle) error { return errors.New("injected handle close failure") }
	if _, err = w.MapRegion(f, 0, FallbackPageBytes, false); err == nil {
		t.Fatal("fault was not reached")
	}
	if len(w.regions) != 1 || w.regions[0].handle == 0 || !w.regions[0].file {
		t.Fatal("failed acquisition lost mapped file owner")
	}
	if err = w.Close(); err == nil || w.Buf == nil || w.regions[0].handle == 0 {
		t.Fatal("failed retirement lost backing or pending handle")
	}
	closeMappedHandle = original
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
}
