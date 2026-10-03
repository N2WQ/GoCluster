package sqlite3_wrap

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

const FallbackPageBytes = 32768

// FallbackRegion owns one 64 KiB native mapping and one 32 KiB Go shadow.
// All attached files compete for the same 64 slots in their engine owner.
// Private SQLite copies live inside the separately bounded engine allocation.
type FallbackRegion struct {
	Data       []byte
	Shadow     *[FallbackPageBytes]byte
	Private    Ptr_t
	addr       uintptr
	handle     windows.Handle
	used       bool
	accounting *MemoryAccounting
}

func (w *Wrapper) MapFallback(f *os.File, offset int64) (*FallbackRegion, error) {
	var r *FallbackRegion
	for i := range w.fallback {
		if !w.fallback[i].used {
			r = &w.fallback[i]
			break
		}
	}
	if r == nil {
		return nil, nil
	}
	r.used = true // Own every subsequent acquisition before it can fail.
	r.accounting = w.Accounting
	r.accounting.WALSlots.Add(1)
	align := offset & (allocationGranularity - 1)
	base := offset - align
	end := base + allocationGranularity
	h, err := windows.CreateFileMapping(windows.Handle(f.Fd()), nil, windows.PAGE_READWRITE, uint32(end>>32), uint32(end), nil)
	if err != nil {
		r.used = false
		r.accounting.WALSlots.Add(-1)
		return nil, err
	}
	r.handle = h
	addr, err := windows.MapViewOfFile(h, windows.FILE_MAP_WRITE|windows.FILE_MAP_READ, uint32(base>>32), uint32(base), allocationGranularity)
	if err != nil {
		if closeErr := r.Close(); closeErr != nil {
			return r, closeErr
		}
		return nil, err
	}
	r.addr = addr
	r.accounting.WALViews.Add(allocationGranularity)
	// MapViewOfFile returned an OS-owned address, never a Go heap pointer
	// round-tripped through uintptr. Use the pinned Memory implementation's
	// native-address conversion; this owner retains the mapping until Close
	// confirms UnmapViewOfFile, and only then clears every Data borrow.
	ptr := *(*unsafe.Pointer)(unsafe.Pointer(&r.addr))
	view := unsafe.Slice((*byte)(ptr), allocationGranularity)
	r.Data = view[align : align+FallbackPageBytes : align+FallbackPageBytes]
	r.Shadow = new([FallbackPageBytes]byte)
	r.accounting.WALShadows.Add(FallbackPageBytes)
	return r, nil
}

// Close never discards a failed native owner. In particular a successfully
// unmapped view and an unreleased mapping handle are separate retirement states.
func (r *FallbackRegion) Close() error {
	if r.addr != 0 {
		if err := unmapFallbackView(r.addr); err != nil {
			return err
		}
		r.addr = 0
		r.accounting.WALViews.Add(-allocationGranularity)
		r.Data = nil
	}
	if r.handle != 0 {
		if err := closeFallbackHandle(r.handle); err != nil {
			return err
		}
		r.handle = 0
	}
	if r.Shadow != nil {
		r.Shadow = nil
		r.accounting.WALShadows.Add(-FallbackPageBytes)
	}
	if r.used {
		r.used = false
		r.accounting.WALSlots.Add(-1)
	}
	return nil
}

var (
	unmapFallbackView   = windows.UnmapViewOfFile
	closeFallbackHandle = windows.CloseHandle
)
