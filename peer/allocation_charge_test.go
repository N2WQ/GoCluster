package peer

import (
	"net"
	"runtime"
	"runtime/debug"
	"testing"
)

func TestSessionQueueBackingBoundsExtremeConfiguredCount(t *testing.T) {
	for _, requested := range []int{1, 128, int(^uint(0) >> 1)} {
		local, remote := net.Pipe()
		s := newSession(local, dirInbound, nil, PeerEndpoint{}, sessionSettings{writeQueue: requested})
		if s.writeCh != nil || s.dataBytes != 0 {
			t.Fatal("pending handshake allocated the configured normal-lane backing")
		}
		if err := s.activateNormalQueue(); err != nil {
			t.Fatal(err)
		}
		_ = local.Close()
		_ = remote.Close()
		if cap(s.writeCh) > requested || s.dataBytes > 1<<20 || s.controlBytes > 1<<20 {
			t.Fatalf("requested=%d effective=%d data=%d control=%d", requested, cap(s.writeCh), s.dataBytes, s.controlBytes)
		}
		if requested <= 128 && cap(s.writeCh) != requested {
			t.Fatalf("ordinary configured count changed: requested=%d effective=%d", requested, cap(s.writeCh))
		}
		if requested == 128 && (s.dataBytes != 2560 || s.controlBytes != 6016) {
			t.Fatalf("independent default backing oracle: data=%d control=%d", s.dataBytes, s.controlBytes)
		}
		if requested > 128 && channelAllocationBytes(cap(s.writeCh)+1, 16)+2 <= 1<<20 {
			t.Fatal("effective count was reduced below the byte-derived boundary")
		}
	}
}

func TestAllocationChargeBoundaries(t *testing.T) {
	for _, tc := range []struct{ size, want int }{
		{0, 0}, {1, 8}, {17, 24}, {2016, 2048}, {2049, 2304},
		{8193, 9472}, {32768, 32768}, {32769, 40960}, {65536, 65536},
	} {
		if got := allocationBytes(tc.size); got != tc.want {
			t.Errorf("allocationBytes(%d)=%d, want %d", tc.size, got, tc.want)
		}
	}
	if got := pointerAllocationBytes(256 * 16); got < 4104 {
		t.Fatalf("256-string staging backing omits its allocation header: %d", got)
	}
}

func TestAllocationChargeCoversRuntimeBacking(t *testing.T) {
	// Observe actual runtime allocation independently of the reservation table.
	// This check deliberately covers the large-object page boundary and the
	// two staging/transport counterexamples, not only small ordinary messages.
	previous := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previous)
	for _, size := range []int{17, 2016, 2049, 8193, 32769, 65536} {
		const count = 128
		owned := make([][]byte, count)
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for i := range owned {
			owned[i] = make([]byte, size)
		}
		runtime.ReadMemStats(&after)
		allocated := after.TotalAlloc - before.TotalAlloc
		if limit := uint64(count*allocationBytes(size) + 4096); allocated > limit {
			t.Fatalf("runtime backing for %d-byte objects=%d exceeds reservation=%d", size, allocated, limit)
		}
		runtime.KeepAlive(owned)
	}
}
