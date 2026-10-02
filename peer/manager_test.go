package peer

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestHandleFramePC92QueueFailureClosesWithoutAuthority(t *testing.T) {
	m := newProtocolTestManager(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &session{id: "N1SRC", remoteCall: "N1SRC", pc9x: true, ctx: ctx, cancel: cancel}
	m.sessions.Set(src.id, src)
	frame, _ := ParseFrame(fmt.Sprintf("PC92^N1SRC^%d^C^5N1SRC^H2^", utcSecond(time.Now())))
	for i := 0; i < 192; i++ {
		if !m.protocol.enqueue(frame, src, time.Now()) {
			t.Fatalf("premature refusal %d", i)
		}
	}
	start := time.Now()
	m.HandleFrame(frame, src)
	if time.Since(start) > time.Second {
		t.Fatal("reader blocked on full control queue")
	}
	if ctx.Err() == nil {
		t.Fatal("authoritative admission failure did not close source")
	}
	if m.protocol.graph.freshness.Len() != 0 || m.protocol.graph.nodes.Len() != 0 {
		t.Fatal("refused input changed authority")
	}
}
func newProtocolTestManager(t *testing.T) *Manager {
	t.Helper()
	m, err := NewManager(completeProtocolTestConfig(config.PeeringConfig{NodeVersion: "5457", NodeBuild: "633", PC92Bitmap: 5, HopCount: 99, MaxLineLength: 65536, PC92MaxBytes: 65536}, "N0LOCAL"), "N0LOCAL", nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	return m
}
func utcSecond(now time.Time) int { u := now.UTC(); return u.Hour()*3600 + u.Minute()*60 + u.Second() }

func TestActiveSessionSSIDsSortedUnique(t *testing.T) {
	m := &Manager{
		sessions: sessionTestIndex(map[string]*session{
			"a": {remoteCall: "n2wq-73"},
			"b": {remoteCall: " KM3T-44 "},
			"c": {remoteCall: "km3t-44"},
			"d": {remoteCall: "*"},
			"e": {remoteCall: ""},
			"f": nil,
		}),
	}

	got := m.ActiveSessionSSIDs()
	want := []string{"KM3T-44", "N2WQ-73"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestHandleFrameRelaysInboundPC11AndPC61ToOtherPeers(t *testing.T) {
	tests := []struct {
		name         string
		line         string
		targetPC9x   bool
		wantPrefix   string
		wantHopToken string
	}{
		{
			name:         "pc11 relays to legacy peer as pc11",
			line:         "PC11^14074.0^K1ABC^23-Dec-2025^2001Z^CQ TEST^W1XYZ^ORIGIN^H3^",
			targetPC9x:   false,
			wantPrefix:   "PC11^",
			wantHopToken: "^H2^",
		},
		{
			name:         "pc61 relays to pc9x peer as pc61",
			line:         "PC61^14074.0^K1ABC^23-Dec-2025^2001Z^CQ TEST^W1XYZ^ORIGIN^203.0.113.7^H3^",
			targetPC9x:   true,
			wantPrefix:   "PC61^",
			wantHopToken: "^H2^",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := &session{
				id:         "src",
				remoteCall: "SRC",
				ctx:        context.Background(),
				writeCh:    make(chan string, 1),
			}
			dst := &session{
				id:         "dst",
				remoteCall: "DST",
				pc9x:       tc.targetPC9x,
				ctx:        context.Background(),
				writeCh:    make(chan string, 1),
			}
			ingest := make(chan *spot.Spot, 1)
			m := &Manager{
				cfg:      config.PeeringConfig{ForwardSpots: true},
				dedupe:   newDedupeCache(time.Minute),
				ingest:   ingest,
				sessions: sessionTestIndex(map[string]*session{"src": src, "dst": dst}),
			}

			frame, err := ParseFrame(tc.line)
			if err != nil {
				t.Fatalf("ParseFrame: %v", err)
			}

			m.HandleFrame(frame, src)

			select {
			case got := <-ingest:
				if got == nil {
					t.Fatal("expected inbound spot to be ingested locally")
				}
			default:
				t.Fatal("expected inbound spot to be ingested locally")
			}

			select {
			case got := <-dst.writeCh:
				if !strings.HasPrefix(got, tc.wantPrefix) {
					t.Fatalf("expected relayed frame prefix %q, got %q", tc.wantPrefix, got)
				}
				if !strings.Contains(got, tc.wantHopToken) {
					t.Fatalf("expected relayed frame hop token %q, got %q", tc.wantHopToken, got)
				}
			default:
				t.Fatal("expected relay to destination peer")
			}

			select {
			case got := <-src.writeCh:
				t.Fatalf("expected source peer to be excluded from relay, got %q", got)
			default:
			}
		})
	}
}

func TestInboundSpotNotRelayedWhenLocalIngestQueueFull(t *testing.T) {
	src := &session{
		id:         "src",
		remoteCall: "SRC",
		ctx:        context.Background(),
		writeCh:    make(chan string, 1),
	}
	dst := &session{
		id:         "dst",
		remoteCall: "DST",
		pc9x:       true,
		ctx:        context.Background(),
		writeCh:    make(chan string, 1),
	}
	ingest := make(chan *spot.Spot, 1)
	ingest <- spot.NewSpot("BUSY1", "LOCAL", 14074.0, "FT8")
	m := &Manager{
		cfg:      config.PeeringConfig{ForwardSpots: true},
		dedupe:   newDedupeCache(time.Minute),
		ingest:   ingest,
		sessions: sessionTestIndex(map[string]*session{"src": src, "dst": dst}),
	}

	frame, err := ParseFrame("PC61^14074.0^K1ABC^23-Dec-2025^2001Z^CQ TEST^W1XYZ^ORIGIN^203.0.113.7^H3^")
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}

	m.HandleFrame(frame, src)

	if got := len(ingest); got != 1 {
		t.Fatalf("expected ingest queue to stay full after local drop, got len=%d", got)
	}
	select {
	case got := <-dst.writeCh:
		t.Fatalf("expected relay suppression when local ingest drops, got %q", got)
	default:
	}
}

func TestInboundSpotRelayedWhenLocallyAccepted(t *testing.T) {
	src := &session{
		id:         "src",
		remoteCall: "SRC",
		ctx:        context.Background(),
		writeCh:    make(chan string, 1),
	}
	dst := &session{
		id:         "dst",
		remoteCall: "DST",
		pc9x:       true,
		ctx:        context.Background(),
		writeCh:    make(chan string, 1),
	}
	ingest := make(chan *spot.Spot, 1)
	m := &Manager{
		cfg:      config.PeeringConfig{ForwardSpots: true},
		dedupe:   newDedupeCache(time.Minute),
		ingest:   ingest,
		sessions: sessionTestIndex(map[string]*session{"src": src, "dst": dst}),
	}

	frame, err := ParseFrame("PC61^14074.0^K1ABC^23-Dec-2025^2001Z^CQ TEST^W1XYZ^ORIGIN^203.0.113.7^H3^")
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}

	m.HandleFrame(frame, src)

	select {
	case got := <-ingest:
		if got == nil {
			t.Fatal("expected local ingest acceptance")
		}
	default:
		t.Fatal("expected local ingest acceptance")
	}

	select {
	case got := <-dst.writeCh:
		if !strings.HasPrefix(got, "PC61^") {
			t.Fatalf("expected relayed PC61 frame, got %q", got)
		}
	default:
		t.Fatal("expected relay after local acceptance")
	}
}

func TestHandleFramePC92DuplicateDoesNotReapplyOrRelay(t *testing.T) {
	m := newProtocolTestManager(t)
	src := &session{id: "N1SRC", remoteCall: "N1SRC", pc9x: true, ctx: context.Background()}
	dst := &session{id: "N2DST", remoteCall: "N2DST", pc9x: true, ctx: context.Background(), priorityLineCh: make(chan string, 4)}
	m.sessions.Set(src.id, src)
	m.sessions.Set(dst.id, dst)
	now := time.Now()
	stamp := utcSecond(now)
	first, _ := ParseFrame(fmt.Sprintf("PC92^N1SRC^%d^C^5N1SRC^1K1USER^H2^", stamp))
	second, _ := ParseFrame(fmt.Sprintf("PC92^N1SRC^%d^C^5N1SRC^1K1USER^H1^", stamp))
	m.protocol.receive(first, src, now)
	m.protocol.receive(second, src, now.Add(time.Second))
	if m.protocol.graph.edges != 1 || len(dst.priorityLineCh) != 1 {
		t.Fatalf("edges=%d relays=%d", m.protocol.graph.edges, len(dst.priorityLineCh))
	}
	if m.protocol.graph.nodes.Value("N1SRC").Seen != now {
		t.Fatal("duplicate refreshed liveness")
	}
}

func TestHandleFrameWWVDuplicateHopVariantSuppressed(t *testing.T) {
	m := newProtocolTestManager(t)
	var delivered int
	m.SetWWVBroadcast(func(kind, line string) {
		delivered++
	})

	first, err := ParseFrame("PC23^19-Apr-2026^1200Z^120^5^1^No storms^W1AW^NODE^H95^")
	if err != nil {
		t.Fatalf("ParseFrame(first): %v", err)
	}
	second, err := ParseFrame("PC23^19-Apr-2026^1200Z^120^5^1^No storms^W1AW^NODE^H94^")
	if err != nil {
		t.Fatalf("ParseFrame(second): %v", err)
	}

	m.HandleFrame(first, &session{remoteCall: "SRC"})
	m.HandleFrame(second, &session{remoteCall: "SRC"})

	if delivered != 1 {
		t.Fatalf("expected one WWV delivery after duplicate suppression, got %d", delivered)
	}
}

func TestHandleFramePC93AnnouncementDuplicateHopVariantSuppressed(t *testing.T) {
	m := newProtocolTestManager(t)
	src := &session{id: "N1SRC", remoteCall: "N1SRC", pc9x: true}
	m.sessions.Set(src.id, src)
	delivered := 0
	m.SetAnnouncementBroadcast(func(string) { delivered++ })
	now := time.Now()
	stamp := utcSecond(now)
	first, _ := ParseFrame(fmt.Sprintf("PC93^N1SRC^%d^*^N1SRC^*^hello^H97^", stamp))
	second, _ := ParseFrame(fmt.Sprintf("PC93^N1SRC^%d^*^N1SRC^*^hello^H96^", stamp))
	m.protocol.receive(first, src, now)
	m.protocol.receive(second, src, now)
	if delivered != 1 {
		t.Fatalf("deliveries=%d", delivered)
	}
}

func TestPublishDXReceiveOnlyStillPublishesManualSpot(t *testing.T) {
	dst := &session{
		id:         "dst",
		remoteCall: "DST",
		pc9x:       true,
		ctx:        context.Background(),
		writeCh:    make(chan string, 1),
	}
	m := &Manager{
		cfg:       config.PeeringConfig{ForwardSpots: false, HopCount: 3},
		localCall: "LOCAL",
		sessions:  sessionTestIndex(map[string]*session{"dst": dst}),
	}

	sp := spot.NewSpot("K1ABC", "W1XYZ", 14074.0, "FT8")
	if !m.PublishDX(sp) {
		t.Fatal("expected manual DX spot to publish in receive-only mode")
	}

	select {
	case got := <-dst.writeCh:
		if !strings.HasPrefix(got, "PC61^") {
			t.Fatalf("expected PC61 publish to pc9x peer, got %q", got)
		}
		if !strings.Contains(got, "^H3^") {
			t.Fatalf("expected configured hop token in publish, got %q", got)
		}
	default:
		t.Fatal("expected DX publish to destination peer")
	}
}

func TestPublishDXWithCommentFallsBackToManualMode(t *testing.T) {
	dst := &session{
		id:         "dst",
		remoteCall: "DST",
		pc9x:       true,
		ctx:        context.Background(),
		writeCh:    make(chan string, 1),
	}
	m := &Manager{
		cfg:       config.PeeringConfig{ForwardSpots: true, HopCount: 2},
		localCall: "LOCAL",
		sessions:  sessionTestIndex(map[string]*session{"dst": dst}),
	}

	sp := spot.NewSpot("K1ABC", "W1XYZ", 14074.0, "FT8")
	sp.Comment = ""
	if !m.PublishDXWithComment(sp, "FT8") {
		t.Fatal("expected manual DX spot to publish with override comment")
	}

	select {
	case got := <-dst.writeCh:
		if !strings.Contains(got, "^FT8^") {
			t.Fatalf("expected override comment in PC61 publish, got %q", got)
		}
	default:
		t.Fatal("expected DX publish to destination peer")
	}
}

func TestHandleFrameSpotRelaySuppressedWhenForwardSpotsDisabled(t *testing.T) {
	tests := []struct {
		name string
		line string
	}{
		{
			name: "pc11 ingest only",
			line: "PC11^14074.0^K1ABC^23-Dec-2025^2001Z^CQ TEST^W1XYZ^ORIGIN^H3^",
		},
		{
			name: "pc26 ingest only",
			line: "PC26^14074.0^K1ABC^23-Dec-2025^2001Z^CQ TEST^W1XYZ^ORIGIN^ ^H3^",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := &session{
				id:         "src",
				remoteCall: "SRC",
				ctx:        context.Background(),
				writeCh:    make(chan string, 1),
			}
			dst := &session{
				id:         "dst",
				remoteCall: "DST",
				pc9x:       true,
				ctx:        context.Background(),
				writeCh:    make(chan string, 1),
			}
			ingest := make(chan *spot.Spot, 1)
			m := &Manager{
				cfg:      config.PeeringConfig{ForwardSpots: false},
				dedupe:   newDedupeCache(time.Minute),
				ingest:   ingest,
				sessions: sessionTestIndex(map[string]*session{"src": src, "dst": dst}),
			}

			frame, err := ParseFrame(tc.line)
			if err != nil {
				t.Fatalf("ParseFrame: %v", err)
			}

			m.HandleFrame(frame, src)

			select {
			case got := <-ingest:
				if got == nil {
					t.Fatal("expected ingested spot, got nil")
				}
			default:
				t.Fatal("expected inbound spot to be ingested locally")
			}

			select {
			case got := <-dst.writeCh:
				t.Fatalf("expected relay suppression, got %q", got)
			default:
			}
		})
	}
}
