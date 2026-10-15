package cluster

import (
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Snapshot IDs rather than total goroutine counts, so unrelated pre-existing
// owners do not become this fixture's retirement obligation. Source paths in
// actual stack frames retain the module name even when symbol dots are escaped.
func pskrZeroMQTTOwners(t *testing.T) map[uint64]bool {
	t.Helper()
	buffer := make([]byte, 1<<20)
	n := runtime.Stack(buffer, true)
	if n == len(buffer) {
		t.Fatal("bounded MQTT owner snapshot overflowed")
	}
	owners := make(map[uint64]bool)
	for _, stack := range strings.Split(string(buffer[:n]), "\n\n") {
		if !strings.Contains(stack, "paho.mqtt.golang") {
			continue
		}
		header := strings.Fields(stack)
		if len(header) < 2 || header[0] != "goroutine" {
			t.Fatal("malformed MQTT owner stack header")
		}
		id, err := strconv.ParseUint(header[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		owners[id] = true
	}
	return owners
}

func pskrZeroRequireLiveMQTT(t *testing.T, baseline map[uint64]bool) {
	t.Helper()
	for id := range pskrZeroMQTTOwners(t) {
		if !baseline[id] {
			return
		}
	}
	t.Fatal("connected MQTT fixture has no observed owned goroutine")
}

func pskrZeroAwaitMQTTRetirement(t *testing.T, baseline map[uint64]bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var remaining []uint64
		for id := range pskrZeroMQTTOwners(t) {
			if !baseline[id] {
				remaining = append(remaining, id)
			}
		}
		if len(remaining) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("owned MQTT goroutines did not retire: %v", remaining)
		}
		time.Sleep(time.Millisecond)
	}
}
