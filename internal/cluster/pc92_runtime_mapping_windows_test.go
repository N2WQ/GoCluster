//go:build qualification && windows

package cluster

import (
	"fmt"
	"math"
	"os"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// The mapped input ledger contains scalars only. Its exact size is fixed by
// the selected profile before either process starts. Each entry is written
// once; started.Store publishes all preceding immutable fields to the child.
type qualificationSharedInputs struct {
	inputs []qualificationInput
	file   *os.File
	handle windows.Handle
	view   uintptr
	bytes  int
}

func openQualificationSharedInputs(path string, count int, create bool) (*qualificationSharedInputs, error) {
	if count < 1 || count > 456000 {
		return nil, fmt.Errorf("invalid mapped input capacity %d", count)
	}
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE | os.O_TRUNC
	}
	file, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, err
	}
	m := &qualificationSharedInputs{file: file, bytes: count * int(unsafe.Sizeof(qualificationInput{}))}
	if create {
		err = file.Truncate(int64(m.bytes))
	} else {
		var info os.FileInfo
		info, err = file.Stat()
		if err == nil && info.Size() != int64(m.bytes) {
			err = fmt.Errorf("mapped input file size=%d want=%d", info.Size(), m.bytes)
		}
	}
	if err == nil {
		m.handle, err = windows.CreateFileMapping(windows.Handle(file.Fd()), nil, windows.PAGE_READWRITE, 0, uint32(m.bytes), nil)
	}
	if err == nil {
		m.view, err = windows.MapViewOfFile(m.handle, windows.FILE_MAP_READ|windows.FILE_MAP_WRITE, 0, 0, uintptr(m.bytes))
	}
	if err != nil {
		_ = m.close()
		return nil, err
	}
	// MapViewOfFile returns page-aligned native memory, not a Go heap pointer
	// round-tripped through uintptr. Reinterpret its stored address as in the
	// pinned native SQLite wrapper. This owner retains the live view/handle;
	// inputs contains only scalar records and is borrowed until close unmaps it.
	ptr := *(*unsafe.Pointer)(unsafe.Pointer(&m.view))
	m.inputs = unsafe.Slice((*qualificationInput)(ptr), count)
	return m, nil
}

func (m *qualificationSharedInputs) close() error {
	var first error
	if m.view != 0 {
		first = windows.UnmapViewOfFile(m.view)
		m.view, m.inputs = 0, nil
	}
	if m.handle != 0 {
		if err := windows.CloseHandle(m.handle); first == nil {
			first = err
		}
		m.handle = 0
	}
	if m.file != nil {
		if err := m.file.Close(); first == nil {
			first = err
		}
		m.file = nil
	}
	return first
}

// QPC is a common monotonic counter for processes on this modern Windows host.
// time.Time carries raw ticks internally, not UTC: only tickDuration converts
// intervals, once, using integer arithmetic and a conservative one-tick margin.
// https://learn.microsoft.com/en-us/windows/win32/sysinfo/acquiring-high-resolution-time-stamps
func qualificationCounterClock() (func() time.Time, int64, error) {
	dll := windows.NewLazySystemDLL("kernel32.dll")
	frequencyProc := dll.NewProc("QueryPerformanceFrequency")
	counterProc := dll.NewProc("QueryPerformanceCounter")
	var frequency int64
	if ok, _, err := frequencyProc.Call(uintptr(unsafe.Pointer(&frequency))); ok == 0 {
		return nil, 0, fmt.Errorf("QPC frequency: %w", err)
	}
	if frequency < 1_000_000 || frequency > math.MaxInt64/int64(time.Second)-1 {
		return nil, 0, fmt.Errorf("unsupported QPC frequency %d", frequency)
	}
	now := func() time.Time {
		var ticks int64
		if ok, _, err := counterProc.Call(uintptr(unsafe.Pointer(&ticks))); ok == 0 || ticks <= 0 {
			panic(fmt.Sprintf("QPC failed: %v ticks=%d", err, ticks))
		}
		return time.Unix(0, ticks)
	}
	return now, frequency, nil
}
