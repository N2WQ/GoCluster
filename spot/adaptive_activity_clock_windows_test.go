package spot

import (
	"syscall"
	"unsafe"
)

const activityLatencyClock = "Windows QueryPerformanceCounter"

var activityCounterDLL = syscall.NewLazyDLL("kernel32.dll")
var activityCounterProc = activityCounterDLL.NewProc("QueryPerformanceCounter")
var activityCounterFrequency = func() int64 {
	var frequency int64
	result, _, err := activityCounterDLL.NewProc("QueryPerformanceFrequency").Call(uintptr(unsafe.Pointer(&frequency)))
	if result == 0 || frequency <= 0 {
		panic(err)
	}
	return frequency
}()

// This host's Go time.Now samples quantize sub-microsecond operations to zero.
// Use QPC only for test instrumentation; production clock ownership is unchanged.
func activityLatencyNow() int64 {
	var ticks int64
	result, _, err := activityCounterProc.Call(uintptr(unsafe.Pointer(&ticks)))
	if result == 0 {
		panic(err)
	}
	return (ticks/activityCounterFrequency)*1000000000 + (ticks%activityCounterFrequency)*1000000000/activityCounterFrequency
}
