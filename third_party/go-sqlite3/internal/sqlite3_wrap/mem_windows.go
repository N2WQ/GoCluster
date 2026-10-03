package sqlite3_wrap

import (
	"math"
	"unsafe"

	"github.com/ncruces/go-sqlite3/internal/errutil"
	"golang.org/x/sys/windows"
)

type Memory struct {
	Accounting    *MemoryAccounting
	Buf           []byte
	Max           int64
	pieces        []uintptr
	pieceStorage  [129]uintptr
	pieceReleased [129]bool
	regions       []*MappedRegion
	regionStorage [128]*MappedRegion
	regionValues  [128]MappedRegion
	ptr           uintptr
	ForceFallback bool
	fallback      [64]FallbackRegion
}

func (m *Memory) Slice() *[]byte {
	return &m.Buf
}

func (m *Memory) Grow(delta, max int64) int64 {
	if delta < 0 || int64(len(m.Buf))>>16+delta > min(max, m.Max) {
		return -1
	}
	if m.Buf == nil {
		if m.Accounting == nil {
			m.Accounting = new(MemoryAccounting)
		}
		if m.Max < 0 || m.Max > 128 {
			panic(errutil.OOMErr)
		}
		m.pieces = m.pieceStorage[:0]
		m.regions = m.regionStorage[:0]
		m.allocate(uint64(m.Max) << 16)
	}

	len := int64(len(m.Buf))
	old := len >> 16
	if delta == 0 {
		return old
	}
	new := old + delta
	max = min(max, m.Max, int64(math.MaxInt)>>16)
	if new > max || new < old {
		return -1
	}
	m.commit(uint64(new) << 16)
	return old
}

func (m *Memory) allocate(max uint64) {
	if m.ForceFallback || !placeholdersSupported() {
		// Allocate the full backing once. append-based geometric growth can
		// exceed the logical limit and retain overlapping generations.
		m.Buf = make([]byte, int(max))[:0]
		m.Accounting.EngineBacking.Store(int64(max))
		return
	}

	if max > math.MaxInt {
		// This ensures uintptr(max) overflows to a large value,
		// and VirtualAlloc2 returns an error.
		max = math.MaxUint64
	}

	// Reserve max bytes of address space, to ensure we won't need to move it.
	// Use virtual memory placeholders so we can later map files.
	// https://devblogs.microsoft.com/oldnewthing/?p=109346
	r, err := allocateNativeMemory(0, uintptr(max),
		windows.MEM_RESERVE|_MEM_RESERVE_PLACEHOLDER, windows.PAGE_NOACCESS)
	if err != nil {
		panicMemoryAllocation(err)
	}
	m.pieces = append(m.pieces, 0)
	m.ptr = r
	m.Accounting.EngineBacking.Store(int64(max))
	m.Accounting.EngineNative.Store(int64(max))

	ptr := *(*unsafe.Pointer)(unsafe.Pointer(&m.ptr))
	m.Buf = unsafe.Slice((*byte)(ptr), max)[:0]
}

func (m *Memory) commit(size uint64) {
	if m.ptr == 0 {
		m.Buf = m.Buf[:size]
		return
	}

	com := uint64(len(m.Buf))
	res := uint64(cap(m.Buf))
	if com < size && size <= res {
		// Split the trailing placeholder.
		if size < res {
			err := windows.VirtualFree(m.ptr+uintptr(com), uintptr(size-com),
				windows.MEM_RELEASE|_MEM_PRESERVE_PLACEHOLDER)
			if err != nil {
				panic(err)
			}
			m.pieces = append(m.pieces, uintptr(size))
		}
		// Replace the placeholder with committed memory.
		_, err := allocateNativeMemory(m.ptr+uintptr(com), uintptr(size-com),
			windows.MEM_COMMIT|windows.MEM_RESERVE|_MEM_REPLACE_PLACEHOLDER, windows.PAGE_READWRITE)
		if err != nil {
			panicMemoryAllocation(err)
		}
	}
	m.Buf = m.Buf[:size]
}

func (m *Memory) reserve(size int64) int64 {
	if m.ptr == 0 || size <= 0 {
		return 0
	}

	com := int64(len(m.Buf))
	res := int64(cap(m.Buf))
	new := com + size

	if new > res || new < com {
		return 0
	}

	// Split the trailing placeholder.
	if new < res {
		err := windows.VirtualFree(m.ptr+uintptr(com), uintptr(new-com),
			windows.MEM_RELEASE|_MEM_PRESERVE_PLACEHOLDER)
		if err != nil {
			panic(err)
		}
		m.pieces = append(m.pieces, uintptr(new))
	}
	m.Buf = m.Buf[:new]
	return com
}

func (m *Memory) Close() (err error) {
	for i := range m.fallback {
		if e := m.fallback[i].Close(); e != nil && err == nil {
			err = e
		}
	}
	if err != nil {
		return err
	}
	if m.ptr == 0 {
		m.Buf = nil
		if m.Accounting != nil {
			m.Accounting.EngineBacking.Store(0)
		}
		return nil
	}

	for _, r := range m.regions {
		e := r.Close()
		if err == nil {
			err = e
		}
	}
	if err != nil {
		return err
	}

	for i, off := range m.pieces {
		if m.pieceReleased[i] {
			continue
		}
		e := releaseNativeMemory(m.ptr+off, 0, windows.MEM_RELEASE)
		if e == nil {
			m.pieceReleased[i] = true
			end := uintptr(cap(m.Buf))
			if i+1 < len(m.pieces) {
				end = m.pieces[i+1]
			}
			m.Accounting.EngineBacking.Add(-int64(end - off))
			m.Accounting.EngineNative.Add(-int64(end - off))
		} else if err == nil {
			err = e
		}
	}
	if err != nil {
		return err
	}
	m.Buf = nil
	m.pieces = nil
	m.regions = nil
	m.ptr = 0
	clear(m.pieceReleased[:])
	return err
}

// Kept package-private so fault tests can prove partial release ownership.
var releaseNativeMemory = windows.VirtualFree
var allocateNativeMemory = virtualAlloc2

func panicMemoryAllocation(err error) {
	if err == windows.ERROR_NOT_ENOUGH_MEMORY || err == windows.ERROR_OUTOFMEMORY || err == windows.ERROR_COMMITMENT_LIMIT {
		panic(errutil.OOMErr)
	}
	panic(err)
}

// CanMapFiles reports whether file views can be mapped into this memory.
func (m *Memory) CanMapFiles() bool { return m.ptr != 0 }
