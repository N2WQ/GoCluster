//go:build windows

package testsupport

import (
	"fmt"
	"math"
	"math/bits"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Clock measures test intervals in nanoseconds using QueryPerformanceCounter.
// NewClock instances have one goroutine owner: their WinAPI output storage is
// reused on every read. RunFixedBenchmark instead supplies one immutable clock
// shared by every owner in that live benchmark. These two domains must not mix.
// A Clock's zero value is not usable; construct it before starting timed work.
type Clock struct {
	benchmark   *testing.B
	counter     int64
	counterProc uintptr
	frequency   int64
}

type clockSource struct {
	counterProc uintptr
	frequency   int64
}

// The system DLL, procedure addresses and frequency have process lifetime. The
// once function runs during NewClock, never during a measured read.
var loadClockSource = sync.OnceValues(func() (clockSource, error) {
	dll := windows.NewLazySystemDLL("kernel32.dll")
	counter := dll.NewProc("QueryPerformanceCounter")
	frequency := dll.NewProc("QueryPerformanceFrequency")
	if err := counter.Find(); err != nil {
		return clockSource{}, fmt.Errorf("resolve QueryPerformanceCounter: %w", err)
	}
	if err := frequency.Find(); err != nil {
		return clockSource{}, fmt.Errorf("resolve QueryPerformanceFrequency: %w", err)
	}
	var hz int64
	ok, _, errno := syscall.SyscallN(frequency.Addr(), uintptr(unsafe.Pointer(&hz)))
	if ok == 0 {
		return clockSource{}, fmt.Errorf("QueryPerformanceFrequency failed: %w", errno)
	}
	if hz <= 0 {
		return clockSource{}, fmt.Errorf("QueryPerformanceFrequency returned %d", hz)
	}
	return clockSource{counterProc: counter.Addr(), frequency: hz}, nil
})

// NewClock resolves the clock source during fixture setup. Failure is fatal:
// silently using a coarse replacement would invalidate short-interval tails.
func NewClock(t testing.TB) *Clock {
	t.Helper()
	source, err := loadClockSource()
	if err != nil {
		t.Fatalf("initialize high-resolution test clock: %v", err)
	}
	clock := &Clock{counterProc: source.counterProc, frequency: source.frequency}
	clock.NowNS()
	return clock
}

// NowNS returns nanoseconds in this clock's domain: absolute QPC for NewClock,
// or elapsed time in RunFixedBenchmark's shared live benchmark. Equal readings
// remain equal; no duration is clipped or replaced. A failed or unrepresentable
// direct QPC reading panics rather than supplying misleading measurements.
func (c *Clock) NowNS() int64 {
	if c.benchmark != nil {
		return int64(c.benchmark.Elapsed())
	}
	if c.counterProc == 0 || c.frequency <= 0 {
		panic("testsupport.Clock requires NewClock")
	}
	// The output belongs to the heap-allocated Clock, avoiding a new escaping
	// output variable on each read. SyscallN also avoids boxing a successful
	// thread's unrelated last-error value into the error interface of Proc.Call.
	ok, _, errno := syscall.SyscallN(c.counterProc, uintptr(unsafe.Pointer(&c.counter)))
	runtime.KeepAlive(c)
	if ok == 0 {
		panic(fmt.Sprintf("QueryPerformanceCounter failed: %v", errno))
	}
	if c.counter < 0 {
		panic("QueryPerformanceCounter returned a negative reading")
	}
	const nsPerSecond = uint64(1_000_000_000)
	ticks, hz := uint64(c.counter), uint64(c.frequency)
	seconds, remainder := ticks/hz, ticks%hz
	// Multiplying absolute ticks by 1e9 can overflow long before the clock
	// does. Split whole seconds first and use a wide product for the fraction.
	hi, lo := bits.Mul64(remainder, nsPerSecond)
	fraction, _ := bits.Div64(hi, lo, hz)
	if seconds > uint64(math.MaxInt64)/nsPerSecond {
		panic("QueryPerformanceCounter nanoseconds overflow int64")
	}
	ns := seconds*nsPerSecond + fraction
	if ns > uint64(math.MaxInt64) {
		panic("QueryPerformanceCounter nanoseconds overflow int64")
	}
	return int64(ns)
}

// Source identifies the timer used in captured test artifacts.
func (c *Clock) Source() string {
	if c.benchmark != nil {
		return "testing.B.Elapsed(shared live benchmark)"
	}
	return "windows.QueryPerformanceCounter"
}

// FrequencyHz returns the actual QPC frequency, resolved during fixture setup.
func (c *Clock) FrequencyHz() int64 { return c.frequency }
