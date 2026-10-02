package peer

import (
	"bufio"
	"context"
	"net"
	"reflect"
	"testing"
	"unsafe"

	"dxcluster/config"

	ztelnet "github.com/ziutek/telnet"
)

// This guards the source-derived per-owner inventory, not a sampled heap delta.
// Buffer payloads have their own transport reservation. Canceled contexts can
// remain reachable through stale session references, as can TCP wrapper structs.
func TestPC92MetadataOwnerLayout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcpType := reflect.TypeOf(net.TCPConn{})
	connField, ok := tcpType.FieldByName("conn")
	if !ok {
		t.Fatal("TCP wrapper layout changed; re-audit ownership")
	}
	fdField, ok := connField.Type.FieldByName("fd")
	if !ok || fdField.Type.Kind() != reflect.Pointer {
		t.Fatal("TCP descriptor layout changed; re-audit ownership")
	}
	rows := []struct {
		name  string
		bytes int
	}{
		{"session including both queue-age rings", int(unsafe.Sizeof(session{}))},
		{"retained reader wrapper", int(unsafe.Sizeof(lineReader{}))},
		{"TCP wrapper", int(tcpType.Size())},
		{"TCP descriptor including poll fields", int(fdField.Type.Elem().Size())},
		{"local TCP address", int(unsafe.Sizeof(net.TCPAddr{}))},
		{"remote TCP address", int(unsafe.Sizeof(net.TCPAddr{}))},
		{"cancel context", int(reflect.TypeOf(ctx).Elem().Size())},
	}
	owned := 0
	for _, row := range rows {
		charged := dedupeOracleAllocation(row.bytes + 8)
		owned += charged
		t.Logf("%s: layout=%d rounded=%d", row.name, row.bytes, charged)
	}
	// Source-owned fixed extras: normal-ready and cancellation channels; one
	// lifecycle reply channel; cancel closure; two IP addresses; compact remote
	// numeric metadata; and actual-address/identity strings. Configuration-derived
	// endpoint/ACL/password backing is separately reported unchanged config.
	owned += 3*256 + 64 + 2*16 + 2*16 + 1024
	if owned > 8<<10 {
		t.Fatalf("per live/stale owner exceeds8KiB metadata envelope: %d", owned)
	}
	t.Logf("per-owner fixed payload inventory=%d, reservation=%d", owned, 8<<10)

	// Active-only work: all transport wrappers/closures, at most six simultaneous
	// timer/ticker objects including their channels, one waiting parse context,
	// one semaphore-list waiter and two small overlapped I/O control objects.
	active := dedupeOracleAllocation(int(unsafe.Sizeof(bufio.Writer{}))+8) +
		dedupeOracleAllocation(int(unsafe.Sizeof(bufio.Reader{}))+8) +
		dedupeOracleAllocation(int(unsafe.Sizeof(ztelnet.Conn{}))+8) +
		dedupeOracleAllocation(int(unsafe.Sizeof(telnetParser{}))+8) +
		8*64 + 6*256 + 512 + 256 + 2*128
	if active > 4<<10 {
		t.Fatalf("active fixed work exceeds4KiB metadata envelope: %d", active)
	}
	t.Logf("active-only fixed inventory=%d, reservation=%d", active, 4<<10)
}

// The v11 changes fit existing local/global reservations. This inventory does
// not turn the still-open context backing and outer retirement proofs into a
// complete metadata or SQLite bound.
func TestPC92V11ChangedOwnershipEnvelope(t *testing.T) {
	// Existing deliberately overcombined work plus one maximum immutable wire
	// per recovery, and an extra transient baseline/member decode generation.
	const priorLocalWork = 5783936
	const recoveryWires = 64 * 65536
	const baselineScratch = 512 << 10
	local := priorLocalWork + recoveryWires + baselineScratch
	if local > localPublicationReservedBytes {
		t.Fatalf("local publication overlap %d exceeds %d", local, localPublicationReservedBytes)
	}
	// Candidate states move to replay ownership, never copy their staged wires.
	// Closed requests and active callers may coexist with replay replies; all
	// message/timer/attempt allowances are deliberately combined, not subtracted
	// because two paths usually share a channel.
	const requestOwners = 128 + 192 + 64 + 2
	request := dedupeOracleAllocation(int(unsafe.Sizeof(protocolRequest{}))+8) +
		dedupeOracleAllocation(int(unsafe.Sizeof(establishmentAttempt{}))+8) + 256 + 256
	global := requestOwners*request +
		128*int(unsafe.Sizeof(protocolRequest{})) +
		192*dedupeOracleAllocation(int(unsafe.Sizeof(candidateState{}))+8) +
		dedupeOracleAllocation(64*int(unsafe.Sizeof(config.PeeringPeer{}))+8) + 129*48 +
		dedupeOracleAllocation(int(unsafe.Sizeof(Manager{}))+8) +
		dedupeOracleAllocation(int(unsafe.Sizeof(protocolController{}))+8) + 64<<10
	if global > 1<<20 {
		t.Fatalf("changed fixed global ownership exceeds existing1MiB allowance: %d", global)
	}
	t.Logf("local overlap bound=%d; global fixed owners=%d; request allowance=%d", local, global, request)
}
