package peer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"
	"unsafe"

	"dxcluster/config"
	"dxcluster/spot"
)

type peerSpotResourceDestination struct {
	session   *session
	saturated bool
}

type peerSpotResourceFixture struct {
	manager      *Manager
	source       *session
	destinations []peerSpotResourceDestination
	ingest       chan *spot.Spot
}

// Exercise the production registry iterator and queue admission with 64 owned,
// established identities. Socket workers are unnecessary for these synchronous
// handler scopes; each admitted queue payload still uses sendLine's real clone.
func newPeerSpotResourceFixture(ctx context.Context, t testing.TB, saturate bool) *peerSpotResourceFixture {
	t.Helper()
	fixture := &peerSpotResourceFixture{ingest: make(chan *spot.Spot, 1)}
	m := &Manager{cfg: config.PeeringConfig{MaxPeers: 64, ForwardSpots: true}, ctx: ctx,
		parseBudget: newFrameParseBudget(), dedupe: newBoundedDedupe(time.Minute, 8, 4096),
		ingest: fixture.ingest, sessions: newFixedIndex[string, *session](64)}
	fixture.manager = m
	fixture.source = &session{id: "N1SRC", remoteCall: "N1SRC", established: true,
		ctx: ctx, manager: m, pc9x: true, writeCh: make(chan string, 1)}
	m.sessions.Set(fixture.source.id, fixture.source)
	for i := range 63 {
		call := fmt.Sprintf("N2RSC%d", i)
		destination := &session{id: call, remoteCall: call, established: true,
			ctx: ctx, manager: m, pc9x: i%2 == 0, writeCh: make(chan string, 1)}
		full := saturate && i%3 == 0
		if full {
			if err := destination.sendLine("occupied"); err != nil {
				t.Fatal(err)
			}
		}
		m.sessions.Set(call, destination)
		fixture.destinations = append(fixture.destinations, peerSpotResourceDestination{destination, full})
	}
	if m.sessions.Len() != 64 {
		t.Fatal("resource fixture did not reach the existing established-peer cap")
	}
	return fixture
}

func peerSpotResourceWire(kind, shape string) string {
	date := "01-Oct-2026"
	if strings.HasPrefix(shape, "space_padded_") {
		date = " 1-Oct-2026"
		shape = strings.TrimPrefix(shape, "space_padded_")
	}
	prefix := kind + "^14074.019^K1RSC" + kind[2:] + "-123^" + date + "^1200Z^"
	suffix := "^W1RSC" + kind[2:] + "-#^N1RSC"
	switch kind {
	case "PC61":
		suffix += "^192.0.2.1"
	case "PC26":
		suffix += "^ "
	}
	suffix += "^H99^"
	comment := "FT8 -10 dB CQ DX"
	if shape != "short" {
		// Leave one byte for the required relay ~. Hop99 -> Hop98 retains
		// the hop width and both modern and legacy variants can be admitted.
		size := MaxPeerFrameBytes - 1 - len(prefix) - len(suffix)
		comment = strings.Repeat("x", size)
		if shape == "many_tokens" {
			comment = strings.Repeat("x ", size/2) + strings.Repeat("x", size%2)
		}
	}
	return prefix + comment + suffix
}

func handleLeasedPeerSpot(fixture *peerSpotResourceFixture, wire string) error {
	_, err := fixture.source.withParsedFrame(wire, time.Time{}, func(frame *Frame) (bool, error) {
		fixture.manager.HandleFrame(frame, fixture.source)
		return true, nil
	})
	return err
}

func peerSpotResourceExpected(wire string, modern bool) string {
	line := strings.TrimSuffix(wire, "^H99^")
	if strings.HasPrefix(wire, "PC61^") && !modern {
		line = "PC11" + strings.TrimSuffix(line[4:], "^192.0.2.1")
	}
	return line + "^H98^~"
}

func assertPeerSpotStringOwned(t testing.TB, raw, value, owner string) {
	t.Helper()
	if value == "" {
		return
	}
	start := uintptr(unsafe.Pointer(unsafe.StringData(raw)))
	data := uintptr(unsafe.Pointer(unsafe.StringData(value)))
	if data >= start && data < start+uintptr(len(raw)) {
		t.Fatalf("%s retained a substring of the incoming raw line", owner)
	}
	runtime.KeepAlive(raw)
	runtime.KeepAlive(value)
}

func TestPeerSpotFullLeasedHandlerBoundedFanoutAndOwnedStorage(t *testing.T) {
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		for _, shape := range []string{"short", "one_token", "many_tokens", "space_padded_short", "space_padded_one_token", "space_padded_many_tokens"} {
			t.Run(kind+"/"+shape, func(t *testing.T) {
				fixture := newPeerSpotResourceFixture(t.Context(), t, true)
				wire := peerSpotResourceWire(kind, shape)
				charge, err := frameParseCharge(wire)
				if err != nil || charge+readerScratchBytes > peerParseScratchBytes {
					t.Fatalf("existing charge cannot leave reader headroom: charge=%d err=%v", charge, err)
				}
				if err := handleLeasedPeerSpot(fixture, wire); err != nil {
					t.Fatal(err)
				}
				if used, peak := fixture.manager.parseBudget.usage(); used != 0 || peak != charge {
					t.Fatalf("full handler lease ownership: used=%d peak=%d charge=%d", used, peak, charge)
				}
				if len(fixture.ingest) != 1 || len(fixture.source.writeCh) != 0 {
					t.Fatal("full handler lost local admission or violated source exclusion")
				}
				local := <-fixture.ingest
				for owner, value := range map[string]string{"DX": local.DXCall, "DE": local.DECall,
					"comment": local.Comment, "origin": local.SourceNode, "IP": local.SpotterIP,
					"DX normalization cache": spot.NormalizeCallsign("K1RSC" + kind[2:] + "-123")} {
					assertPeerSpotStringOwned(t, wire, value, owner)
				}
				for key := range fixture.manager.dedupe.items.All() {
					assertPeerSpotStringOwned(t, wire, key, "peer dedupe key")
					if len(key) > 65 {
						t.Fatalf("valid original fields exceeded the reachable key bound: %d", len(key))
					}
				}
				if entries, _, refused := fixture.manager.dedupe.occupancy(); entries != 1 || refused != 0 {
					t.Fatalf("handler did not reach normal peer dedupe admission: entries=%d refused=%d", entries, refused)
				}
				owned := make(map[*byte]bool)
				for _, destination := range fixture.destinations {
					s := destination.session
					eligible := kind != "PC26" || s.pc9x
					wantCount := 0
					if destination.saturated || eligible {
						wantCount = 1
					}
					if len(s.writeCh) != wantCount || s.dataBytes > peerQueueBytes {
						t.Fatalf("destination %s count=%d want=%d bytes=%d", s.id, len(s.writeCh), wantCount, s.dataBytes)
					}
					if wantCount == 0 {
						continue
					}
					line := <-s.writeCh
					if destination.saturated {
						if line != "occupied" {
							t.Fatal("overload replaced existing queue payload")
						}
						continue
					}
					if line != peerSpotResourceExpected(wire, s.pc9x) {
						t.Fatalf("destination %s changed original payload", s.id)
					}
					assertPeerSpotStringOwned(t, wire, line, "destination queue")
					data := unsafe.StringData(line)
					if owned[data] {
						t.Fatal("destinations shared one unowned relay backing")
					}
					owned[data] = true
					if s.dataBytes-s.dataFixedBytes != dedupeOracleAllocation(len(line))+2 {
						t.Fatal("queue payload did not receive its separate rounded backing and CRLF charge")
					}
				}
				runtime.KeepAlive(owned)
			})
		}
	}
}

type peerSpotHeadroomParser struct {
	t      *testing.T
	budget *frameParseBudget
	charge int64
	seen   bool
	parser telnetParser
}

func (p *peerSpotHeadroomParser) Feed(data []byte) ([]byte, [][]byte) {
	if used, _ := p.budget.usage(); used != p.charge+readerScratchBytes {
		p.t.Errorf("reader and real spot handler were not leased together: used=%d", used)
	}
	p.seen = true
	return p.parser.Feed(data)
}

func TestPeerSpotFullHandlerReaderHeadroomAndStop(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	fixture := newPeerSpotResourceFixture(ctx, t, true)
	m := fixture.manager
	m.cancel, m.protocol = cancel, newProtocolController(m)
	wire := peerSpotResourceWire("PC61", "many_tokens")
	charge, err := frameParseCharge(wire)
	if err != nil || 2*charge <= peerParseScratchBytes || charge+readerScratchBytes > peerParseScratchBytes {
		t.Fatalf("fixture must queue a second parser and permit a reader: charge=%d err=%v", charge, err)
	}
	// The real handler is held after local handoff, keeping its parse scope
	// live while another native reader and parser waiter use the same manager.
	m.dedupe.mu.Lock()
	locked := true
	defer func() {
		cancel()
		if locked {
			m.dedupe.mu.Unlock()
		}
		m.wg.Wait()
	}()
	results := make(chan error, 2)
	m.wg.Go(func() {
		_, err := fixture.source.withParsedFrame(wire, time.Time{}, func(frame *Frame) (bool, error) {
			m.HandleFrame(frame, fixture.source)
			return false, ctx.Err()
		})
		results <- err
	})
	select {
	case <-fixture.ingest:
	case <-time.After(5 * time.Second):
		t.Fatal("full handler never reached local handoff")
	}
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()
	parser := &peerSpotHeadroomParser{t: t, budget: m.parseBudget, charge: charge}
	reader := newLineReaderWithTransport(local, MaxPeerFrameBytes, MaxPeerFrameBytes, nil, parser, nil)
	defer reader.release()
	reader.acquireScratch = func(deadline time.Time) (frameParseLease, error) {
		return m.parseBudget.acquireCharge(ctx, deadline, readerScratchBytes)
	}
	writeDone := make(chan error, 1)
	go func() { _, err := remote.Write([]byte("PC51^N1READ^N2READ^0^~\r\n")); writeDone <- err }()
	if line, err := reader.ReadLine(time.Now().Add(3 * time.Second)); err != nil || line != "PC51^N1READ^N2READ^0^" || !parser.seen {
		t.Fatalf("native reader could not progress beside the full handler: line=%q err=%v", line, err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if used, _ := m.parseBudget.usage(); used != charge {
		t.Fatalf("reader return leaked its lease: %d", used)
	}
	if _, err := fixture.source.withParsedFrame(wire, time.Now().Add(20*time.Millisecond), func(*Frame) (bool, error) {
		return false, errors.New("deadline waiter unexpectedly acquired scratch")
	}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued real parser did not retain its fixed deadline: %v", err)
	}
	started := make(chan struct{})
	m.wg.Go(func() {
		close(started)
		_, err := fixture.source.withParsedFrame(wire, time.Time{}, func(*Frame) (bool, error) {
			return false, errors.New("Stop waiter unexpectedly acquired scratch")
		})
		results <- err
	})
	<-started
	m.wg.Go(func() {
		select {
		case request := <-m.protocol.lifecycle:
			request.done <- nil
		case <-ctx.Done():
		}
	})
	stopped := make(chan struct{})
	go func() { m.Stop(); close(stopped) }()
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not cancel handler/parser owners")
	}
	m.dedupe.mu.Unlock()
	locked = false
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not join the real handler/parser scopes")
	}
	for range 2 {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Errorf("Stop scope result: %v", err)
		}
	}
	if used, peak := m.parseBudget.usage(); used != 0 || peak != charge+readerScratchBytes {
		t.Fatalf("full handler/read/Stop lease accounting: used=%d peak=%d", used, peak)
	}
}

// This inventory bounds additions to the existing parser, not destination
// copies retained under their separate queue quotas. <=8 compact field strings
// use ceil(5L/4)+56 allocator bytes; one header array and escaped Frame use 224.
// Accepted date syntax limits Unix text to12 bytes, so even a15-byte DX and DE
// and largest admitted frequency make a <=65-byte key (80-byte class). The
// 1024-byte fixed allowance covers date/formatting intermediates. Both encoders
// are exact-growth, each at most L+1 and independently capped at64KiB.
func peerSpotAddedScratchEnvelope(length int) int {
	return (5*length+3)/4 + 56 + 224 + 80 + 1024 +
		2*dedupeOracleAllocation(min(length+1, MaxPeerFrameBytes))
}

func TestPeerSpotAddedScratchInventoryWithinUnchangedCharge(t *testing.T) {
	for length := 1; length <= MaxPeerFrameBytes; length++ {
		// Leave the remainder of Q's per-wire/comment/token terms to the
		// existing parser. This arithmetic is source-allocation inventory;
		// actual full-handler measurements below are separate evidence.
		if added := peerSpotAddedScratchEnvelope(length); added > 65536+8*length {
			t.Fatalf("new relay storage exceeds unchanged charge allowance: L=%d added=%d", length, added)
		}
	}
	t.Logf("maximum newly added scratch inventory=%d bytes", peerSpotAddedScratchEnvelope(MaxPeerFrameBytes))
}

func TestPeerSpotReachableKeyIncludesCalendarExtremes(t *testing.T) {
	for _, date := range []string{"01-Jan-0000", " 1-Jan-0000", "31-Dec-9999", " 9-Jan-9999"} {
		fixture := newPeerSpotResourceFixture(t.Context(), t, false)
		wire := "PC61^4294967295.99^K0ABCDEFGHIJKLM^" + date + "^1200Z^CQ^W1ABCDEFGHIJKLM^N1RSC^192.0.2.1^H99^"
		if err := handleLeasedPeerSpot(fixture, wire); err != nil {
			t.Fatal(err)
		}
		if len(fixture.ingest) != 1 || fixture.manager.dedupe.items.Len() != 1 {
			t.Fatalf("agreed calendar grammar refused the date %q", date)
		}
		for key := range fixture.manager.dedupe.items.All() {
			if len(key) != 65 {
				t.Fatalf("full calendar domain key %q has %d bytes, want65", key, len(key))
			}
		}
	}
}

func resetPeerSpotResourceFixture(fixture *peerSpotResourceFixture) {
	select {
	case <-fixture.ingest:
	default:
	}
	for _, destination := range fixture.destinations {
		s := destination.session
		select {
		case line := <-s.writeCh:
			s.dataBytes -= queuedLineBytes(line)
		default:
		}
	}
	fixture.manager.dedupe.prune(time.Now().Add(2 * time.Minute))
}

func TestPeerSpotFullHandlerActualAllocationWithinScratchAndQueues(t *testing.T) {
	previous := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previous)
	for _, shape := range []string{"short", "one_token", "many_tokens", "space_padded_short", "space_padded_one_token", "space_padded_many_tokens"} {
		t.Run(shape, func(t *testing.T) {
			fixture := newPeerSpotResourceFixture(t.Context(), t, false)
			wire := peerSpotResourceWire("PC61", shape)
			charge, err := frameParseCharge(wire)
			if err != nil {
				t.Fatal(err)
			}
			runtime.GC()
			// Warm immutable taxonomy, regex and formatting caches after GC.
			// This does not alter measured per-spot owned field/encoder work.
			if err := handleLeasedPeerSpot(fixture, wire); err != nil {
				t.Fatal(err)
			}
			resetPeerSpotResourceFixture(fixture)
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			if err := handleLeasedPeerSpot(fixture, wire); err != nil {
				t.Fatal(err)
			}
			runtime.ReadMemStats(&after)
			allocated := after.TotalAlloc - before.TotalAlloc
			queuedBacking := 0
			for _, destination := range fixture.destinations {
				line := <-destination.session.writeCh
				queuedBacking += dedupeOracleAllocation(len(line))
			}
			if len(fixture.ingest) != 1 || fixture.manager.dedupe.items.Len() != 1 {
				t.Fatal("allocation sample bypassed normal admission/relay work")
			}
			if allocated > uint64(charge)+uint64(queuedBacking) {
				t.Fatalf("actual full-handler allocation exceeds scratch plus separate queues: total=%d Q=%d queues=%d", allocated, charge, queuedBacking)
			}
			t.Logf("full leased handler TotalAlloc=%d Q=%d separately owned queue backing=%d residual=%d", allocated, charge, queuedBacking, allocated-uint64(queuedBacking))
			runtime.KeepAlive(fixture)
		})
	}
}

func BenchmarkPeerSpotFullLeasedHandler(b *testing.B) {
	for _, shape := range []string{"short", "one_token", "many_tokens"} {
		b.Run(shape, func(b *testing.B) {
			fixture := newPeerSpotResourceFixture(b.Context(), b, false)
			wire := peerSpotResourceWire("PC61", shape)
			charge, err := frameParseCharge(wire)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(wire)))
			b.ResetTimer()
			for range b.N {
				if err := handleLeasedPeerSpot(fixture, wire); err != nil {
					b.Fatal(err)
				}
				// Nonallocating consumer/expiry work enables repeated accepted
				// admissions, instead of timing only the duplicate fast path.
				resetPeerSpotResourceFixture(fixture)
			}
			b.StopTimer()
			b.ReportMetric(float64(charge), "lease-B/op")
		})
	}
}
