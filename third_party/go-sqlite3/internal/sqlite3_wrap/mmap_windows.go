package sqlite3_wrap

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func (w *Wrapper) MapRegion(f *os.File, offset int64, size int32, readOnly bool) (*MappedRegion, error) {
	align := offset & (allocationGranularity - 1)
	size += int32(align + allocationGranularity - 1)
	size &^= int32(allocationGranularity - 1)

	r := w.newRegion(size)
	if r == nil {
		return nil, nil
	}
	if err := r.mmap(f, offset-align, readOnly); err != nil {
		return nil, err
	}
	r.Ptr = r.base + Ptr_t(align)
	return r, nil
}

func (w *Wrapper) newRegion(size int32) *MappedRegion {
	// Find unused region.
	for _, r := range w.regions {
		if !r.file && r.size == size {
			return r
		}
	}

	// Reserve page aligned memmory.
	if len(w.regions) == len(w.regionValues) {
		return nil
	}
	ptr := Ptr_t(w.reserve(int64(size)))
	if ptr == 0 {
		return nil
	}

	// Save the newly reserved region.
	ret := &w.regionValues[len(w.regions)]
	*ret = MappedRegion{
		base: ptr,
		size: size,
		addr: uintptr(unsafe.Pointer(&w.Buf[ptr])),
	}
	w.regions = append(w.regions, ret)
	return ret
}

type MappedRegion struct {
	addr   uintptr
	base   Ptr_t
	Ptr    Ptr_t
	size   int32
	file   bool
	zero   bool
	handle windows.Handle
}

func (r *MappedRegion) Close() error {
	if r.file || r.zero {
		// Convert the file view back to a placeholder.
		if err := unmapViewOfFile2(r.addr, _MEM_PRESERVE_PLACEHOLDER); err != nil {
			return err
		}
		r.file, r.zero = false, false
	}
	if r.handle != 0 {
		if err := closeMappedHandle(r.handle); err != nil {
			return err
		}
		r.handle = 0
	}
	return nil
}

func (r *MappedRegion) Unmap() error {
	err := r.Close()
	if err != nil {
		return err
	}

	return r.mapHandle(^windows.Handle(0), 0, windows.PAGE_READONLY, true)
}

func (r *MappedRegion) mmap(f *os.File, offset int64, readOnly bool) error {
	err := r.Close()
	if err != nil {
		return err
	}

	prot := uint32(windows.PAGE_READWRITE)
	if readOnly {
		prot = windows.PAGE_READONLY
	}
	return r.mapHandle(windows.Handle(f.Fd()), offset, prot, false)
}

func (r *MappedRegion) mapHandle(file windows.Handle, offset int64, prot uint32, zero bool) error {
	maxSize := offset + int64(r.size)

	h, err := windows.CreateFileMapping(
		file, nil, prot,
		uint32(maxSize>>32), uint32(maxSize), nil)
	if h == 0 {
		return err
	}
	r.handle = h
	_, err = mapViewOfFile3(h, r.addr, uint64(offset), uintptr(r.size),
		_MEM_REPLACE_PLACEHOLDER, prot)
	if err == nil {
		r.file, r.zero = !zero, zero
	}
	if closeErr := closeMappedHandle(h); closeErr == nil {
		r.handle = 0
	} else if err == nil {
		err = closeErr
	}
	return err
}

var closeMappedHandle = windows.CloseHandle
