package peer

// Allocation charges describe owned Go backing storage, not RSS. These are the
// Go 1.26 small-object classes (internal/runtime/gc/sizeclasses.go); larger
// objects occupy whole 8 KiB runtime pages. Qualification checks these bounds
// against actual allocations when the Go toolchain changes.
var peerAllocationClasses = [...]int{
	8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224,
	240, 256, 288, 320, 352, 384, 416, 448, 480, 512, 576, 640, 704, 768,
	896, 1024, 1152, 1280, 1408, 1536, 1792, 2048, 2304, 2688, 3072,
	3200, 3456, 4096, 4864, 5376, 6144, 6528, 6784, 6912, 8192, 9472,
	9728, 10240, 10880, 12288, 13568, 14336, 16384, 18432, 19072, 20480,
	21760, 24576, 27264, 28672, 32768,
}

func allocationBytes(size int) int {
	if size <= 0 {
		return 0
	}
	if size > 32768 {
		return (size + 8191) &^ 8191
	}
	low, high := 0, len(peerAllocationClasses)
	for low < high {
		mid := low + (high-low)/2
		if peerAllocationClasses[mid] < size {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return peerAllocationClasses[low]
}

// Pointer-bearing allocations may need an inline type header. Always reserve
// it, even for classes where this runtime keeps the type information elsewhere.
func pointerAllocationBytes(size int) int {
	if size <= 0 {
		return 0
	}
	return allocationBytes(size + 8)
}

func ingressEntryBytes(origin, ingress string) int {
	return allocationBytes(len(origin)) + allocationBytes(len(ingress))
}

func queuedLineBytes(line string) int { return allocationBytes(len(line)) + 2 }

// The channel object itself is smaller than 256 bytes on the qualified Go
// runtime. Pointer-bearing element backing is allocated separately. Reserve
// both before make so an extreme configured count cannot request a huge array.
func channelAllocationBytes(count, elementBytes int) int {
	return 256 + pointerAllocationBytes(count*elementBytes)
}

func boundedDataQueueCount(requested int) int {
	count := min(max(requested, 0), (peerQueueBytes-256)/16)
	for channelAllocationBytes(count, 16)+queuedLineBytes("") > peerQueueBytes {
		count--
	}
	return count
}
