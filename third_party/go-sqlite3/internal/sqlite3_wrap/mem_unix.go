//go:build unix

package sqlite3_wrap

import (
	"math"
	"unsafe"

	"github.com/ncruces/go-sqlite3/internal/errutil"
	"golang.org/x/sys/unix"
)

type Memory struct {
	Accounting    *MemoryAccounting
	Buf           []byte
	Max           int64
	regions       []*MappedRegion
	regionStorage [128]*MappedRegion
	regionValues  [128]MappedRegion
	committed     int
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
	// Round up to the page size.
	rnd := uint64(unix.Getpagesize() - 1)
	res := (max + rnd) &^ rnd

	if res > math.MaxInt {
		// This ensures int(res) overflows to a negative value,
		// and mapNativeMemory returns EINVAL.
		res = math.MaxUint64
	}

	// Reserve res bytes of address space, to ensure we won't need to move it.
	// A protected, private, anonymous mapping should not commit memory.
	b, err := allocateNativeMemory(-1, 0, int(res), unix.PROT_NONE, unix.MAP_PRIVATE|unix.MAP_ANON)
	if err != nil {
		panicMemoryAllocation(err)
	}
	m.Buf = b[:0]
	m.Accounting.EngineBacking.Store(int64(res))
	m.Accounting.EngineNative.Store(int64(res))
}

func (m *Memory) commit(size uint64) {
	com := uint64(m.committed)
	res := uint64(cap(m.Buf))
	if com < size && size <= res {
		// Grow geometrically, round up to the page size.
		rnd := uint64(unix.Getpagesize() - 1)
		new := com + com>>3
		new = min(max(size, new), res)
		new = (new + rnd) &^ rnd

		// Commit additional memory up to new bytes.
		err := commitNativeMemory(m.Buf[m.committed:new], unix.PROT_READ|unix.PROT_WRITE)
		if err != nil {
			panicMemoryAllocation(err)
		}
		m.committed = int(new)
	}
	m.Buf = m.Buf[:size]
}

func (m *Memory) Close() error {
	if m.Buf == nil {
		return nil
	}
	err := releaseNativeMemory(m.Buf[:cap(m.Buf)])
	if err != nil {
		return err
	}
	m.Accounting.EngineBacking.Store(0)
	m.Accounting.EngineNative.Store(0)
	m.Buf = nil
	m.regions = nil
	m.committed = 0
	return err
}

// The engine owner already retains the exact extent through failed releases.
// Use the pointer syscalls: unix.Mmap's package-global active map would add
// retained backing shared with unrelated mappings, outside this owner's bound.
func mapNativeMemory(fd int, offset int64, length, prot, flags int) ([]byte, error) {
	if length <= 0 {
		return nil, unix.EINVAL
	}
	p, err := unix.MmapPtr(fd, offset, nil, uintptr(length), prot, flags)
	if err != nil {
		return nil, err
	}
	return unsafe.Slice((*byte)(p), length), nil
}

func unmapNativeMemory(b []byte) error {
	if len(b) == 0 || len(b) != cap(b) {
		return unix.EINVAL
	}
	return unix.MunmapPtr(unsafe.Pointer(unsafe.SliceData(b)), uintptr(len(b)))
}

var releaseNativeMemory = unmapNativeMemory
var allocateNativeMemory = mapNativeMemory
var commitNativeMemory = unix.Mprotect

func panicMemoryAllocation(err error) {
	if err == unix.ENOMEM {
		panic(errutil.OOMErr)
	}
	panic(err)
}
