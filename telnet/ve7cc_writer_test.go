package telnet

import (
	"bufio"
	"bytes"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/spot"
)

// These literals are wire oracles, not output of the serializer under test.
// The manual live origin is deliberately absent from the historical record.
const ve7ccWireSpot = "CC11^14000.0^K1ABC^ 1-Jan-2000^0000Z^CW hello^VE7XYZ-12^291^1^N2WQ-2^8^5^2^3^MA^BC^United States^Canada^FN42^cn89\r\n"

const ve7ccHistoryWireSpot = "CC11^14000.0^K1ABC^ 1-Jan-2000^0000Z^CW hello^VE7XYZ-12^291^1^^8^5^2^3^MA^BC^United States^Canada^FN42^cn89\r\n"

// Adapt Unit1's canonical vector to production fan-out's normalized fields.
func ve7ccWireFixture() *spot.Spot {
	base := ve7ccTestSpot()
	sp := spot.NewSpot(base.DXCall, base.DECall, base.Frequency, base.Mode)
	sp.Time, sp.Comment, sp.SourceType = base.Time, base.Comment, base.SourceType
	sp.DXMetadata, sp.DEMetadata = base.DXMetadata, base.DEMetadata
	spot.ApplySourceHumanFlag(sp)
	return sp
}

// Only the writer owns captured writes. Bounded channels and a close-aware
// first-write gate let cleanup join it even when a byte assertion fails.
type ve7ccCaptureConn struct {
	discardConn
	writes      chan []byte
	entered     chan struct{}
	release     chan struct{}
	closed      chan struct{}
	first       sync.Once
	releaseOnce sync.Once
	closeOnce   sync.Once
}

func ve7ccNewCaptureConn(gated bool) *ve7ccCaptureConn {
	c := &ve7ccCaptureConn{writes: make(chan []byte, 32), release: make(chan struct{}), closed: make(chan struct{})}
	if gated {
		c.entered = make(chan struct{})
	}
	return c
}

func (c *ve7ccCaptureConn) Write(data []byte) (int, error) {
	c.first.Do(func() {
		if c.entered != nil {
			close(c.entered)
			select {
			case <-c.release:
			case <-c.closed:
			}
		}
	})
	select {
	case c.writes <- bytes.Clone(data):
		return len(data), nil
	case <-c.closed:
		return 0, net.ErrClosed
	}
}

func (c *ve7ccCaptureConn) unblock() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func (c *ve7ccCaptureConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func ve7ccAttachWriter(c *Client, conn *ve7ccCaptureConn) {
	c.conn, c.writer = conn, bufio.NewWriterSize(conn, 32768)
	c.spotChan = make(chan *spotEnvelope, 8)
	if c.controlChan == nil {
		c.controlChan = make(chan controlMessage, 8)
	}
	if c.done == nil {
		c.done = make(chan struct{})
	}
}

func ve7ccStartWriter(t testing.TB, c *Client, conn *ve7ccCaptureConn) <-chan struct{} {
	t.Helper()
	done := writerV15Start(c)
	t.Cleanup(func() {
		conn.unblock()
		c.close("")
		writerV15Wait(t, done, "VE7CC cleanup")
		select {
		case extra := <-conn.writes:
			t.Errorf("unexpected trailing writer bytes: %q", extra)
		default:
		}
	})
	return done
}

func ve7ccReadWrites(t testing.TB, conn *ve7ccCaptureConn, expected string) {
	t.Helper()
	var received []byte
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for len(received) < len(expected) {
		select {
		case data := <-conn.writes:
			received = append(received, data...)
		case <-timer.C:
			t.Fatalf("writer stalled: got %q; want %q", received, expected)
		}
	}
	if string(received) != expected {
		t.Fatalf("wire mismatch:\n got %q\nwant %q", received, expected)
	}
}

func TestVE7CCWriterAcknowledgementBoundary(t *testing.T) {
	s, c := v7MachineTestClient(t)
	s.clusterCall, s.writerBatchMaxBytes, s.writerBatchWait = "N2WQ-2", 1, time.Millisecond
	conn := ve7ccNewCaptureConn(true)
	ve7ccAttachWriter(c, conn)
	input, output := configurationTransferBudgetStats()
	t.Cleanup(func() { assertConfigurationTransferStats(t, input, output) })
	c.spotChan <- &spotEnvelope{spot: ve7ccWireFixture()}
	ve7ccStartWriter(t, c, conn)
	writerV15Wait(t, conn.entered, "prepared text write")
	oldText := writerV15Oracle(ve7ccWireFixture().FormatDXCluster() + "\n")

	reservation, err := c.reserveConfigurationOutput()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.enqueueConfigurationResponse(reservation, []byte("earlier history\r\n"), false); err != nil {
		t.Fatal(err) // Enqueue consumes ownership on failure too.
	}
	// This unsent spot is allowed to use the new mode; selectors are not a
	// strict admission/filter barrier and must not flush the old spot backlog.
	c.spotChan <- &spotEnvelope{spot: ve7ccWireFixture()}
	if handled, proceed := s.handleVE7CCCommand(c, "SET/VE7CC"); !handled || !proceed {
		t.Fatal("selection failed behind the prepared text write")
	}
	c.spotChan <- &spotEnvelope{spot: ve7ccWireFixture()}
	conn.unblock()
	ve7ccReadWrites(t, conn, oldText+"earlier history\r\nVE7CC enabled for this session.\r\n"+ve7ccWireSpot+ve7ccWireSpot)

	if handled, proceed := s.handleVE7CCCommand(c, "UNSET/VE7CC"); !handled || !proceed {
		t.Fatal("disable selection failed")
	}
	c.spotChan <- &spotEnvelope{spot: ve7ccWireFixture()}
	ve7ccReadWrites(t, conn, "VE7CC disabled for this session.\r\n"+oldText)
}

func TestVE7CCWriterPipelinedTransitions(t *testing.T) {
	for _, finalNative := range []bool{false, true} {
		t.Run(map[bool]string{false: "text", true: "native"}[finalNative], func(t *testing.T) {
			s := &Server{clusterCall: "N2WQ-2", writerBatchMaxBytes: 16384, writerBatchWait: time.Millisecond}
			c := &Client{server: s, callsign: "W1PIPE"}
			conn := ve7ccNewCaptureConn(false)
			ve7ccAttachWriter(c, conn)
			c.spotChan <- &spotEnvelope{spot: ve7ccWireFixture()}
			lines := []string{"SET/VE7CC", "UNSET/VE7CC"}
			want := "VE7CC enabled for this session.\r\nVE7CC disabled for this session.\r\n"
			if finalNative {
				lines = append(lines, "SET/VE7CC")
				want += "VE7CC enabled for this session.\r\n" + ve7ccWireSpot
			} else {
				want += writerV15Oracle(ve7ccWireFixture().FormatDXCluster() + "\n")
			}
			for _, line := range lines {
				if handled, proceed := s.handleVE7CCCommand(c, line); !handled || !proceed {
					t.Fatal("pipelined selector failed:", line)
				}
			}
			ve7ccStartWriter(t, c, conn)
			ve7ccReadWrites(t, conn, want)
		})
	}
}

func TestVE7CCWriterRefusalIsExplicitAndThrottled(t *testing.T) {
	for _, kind := range []string{"invalid", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			now := time.Unix(1800000000, 0).UTC()
			s := &Server{clusterCall: "N2WQ-2", writerBatchMaxBytes: 1, writerBatchWait: time.Millisecond, nowFn: func() time.Time { return now }}
			c := &Client{server: s, callsign: "W1REFUSE"}
			conn := ve7ccNewCaptureConn(false)
			ve7ccAttachWriter(c, conn)
			if handled, proceed := s.handleVE7CCCommand(c, "SET/VE7CC"); !handled || !proceed {
				t.Fatal("selection failed")
			}
			bad := ve7ccWireFixture()
			if kind == "invalid" {
				bad.Frequency = 0
			} else {
				bad.Comment = strings.Repeat("X", maxVE7CCRecordBytes)
			}
			c.spotChan <- &spotEnvelope{spot: bad}
			c.spotChan <- &spotEnvelope{spot: bad}
			c.spotChan <- &spotEnvelope{spot: ve7ccWireFixture()}
			ve7ccStartWriter(t, c, conn)
			ve7ccReadWrites(t, conn, "VE7CC enabled for this session.\r\n"+
				"VE7CC spot omitted: invalid or oversized native record.\r\n"+ve7ccWireSpot)
			if c.ve7ccDiagnostics.refused.Load() != 2 || c.ve7ccDiagnostics.lastNotice.Load() != now.UnixNano() {
				t.Fatal("refusal count or fixed-clock notice cadence lost records")
			}
			// Exact bytes above exclude DX-de fallback and a second notice. The
			// final good spot is a FIFO witness that both refusals were consumed.
		})
	}
}

func TestVE7CCDiagnosticZeroClock(t *testing.T) {
	c := &Client{}
	epoch := time.Unix(0, 0)
	if !c.allowVE7CCRefusalNotice(epoch) || c.allowVE7CCRefusalNotice(epoch) ||
		c.allowVE7CCRefusalNotice(epoch.Add(29*time.Second)) ||
		!c.allowVE7CCRefusalNotice(epoch.Add(30*time.Second+time.Nanosecond)) {
		t.Fatal("zero-valued clock bypassed or permanently blocked native notice throttling")
	}
}

func TestVE7CCWriterSharedSpotCWAndDiagnostics(t *testing.T) {
	const native = "CC11^14025.0^K1ABC^ 2-Jan-2026^0304Z^CW 0 dB 22 WPM 30 WPM POTA^W1AW^291^1^^8^5^2^3^MA^BC^United States^Canada^FN42^cn89\r\n"
	for _, speed := range []bool{false, true} {
		for _, mode := range []struct {
			name, tag string
			mode      diagMode
		}{
			{"off", "", diagModeOff}, {"source", "RBN", diagModeSource},
			{"confidence", "37%", diagModeConfidence}, {"path", "n0|none", diagModePath},
			{"mode", "CW|SRC", diagModeMode}, {"dedupe", "02@14024900|1|CN|S|F", diagModeDedupe},
		} {
			t.Run(mode.name+map[bool]string{false: "/speed-off", true: "/speed-on"}[speed], func(t *testing.T) {
				s := &Server{clusterCall: "N2WQ-2", writerBatchWait: time.Millisecond, dedupeFastEnabled: true}
				sp, meta := cwSpeedFixture(), ve7ccTestSpot()
				sp.DXMetadata, sp.DEMetadata = meta.DXMetadata, meta.DEMetadata
				sp.HasReport, sp.Report, sp.Confidence = true, 0, "V"
				sp.ConfidencePercentOK, sp.ConfidencePercent = true, 37
				sp.ModeProvenance = spot.ModeProvenanceSourceExplicit
				sp.SecondaryDedupe[0] = spot.SecondaryDedupeEvidence{AnchorHz: 14024900, ModeID: 2, Enabled: true, HasAnchor: true}
				before := ve7ccSnapshot(sp)
				expected := sp.Clone()
				textLine := expected.FormatDXClusterCW(speed)
				if mode.mode != diagModeOff {
					textLine = expected.FormatDXClusterWithComment(mode.tag)
				}
				text, cc := &Client{server: s, callsign: "W1TEXT"}, &Client{server: s, callsign: "W1NATIVE"}
				text.configuredSettings.CWSpeed, cc.configuredSettings.CWSpeed = speed, speed
				text.setDiagMode(mode.mode)
				cc.setDiagMode(mode.mode)
				textConn, nativeConn := ve7ccNewCaptureConn(false), ve7ccNewCaptureConn(false)
				ve7ccAttachWriter(text, textConn)
				ve7ccAttachWriter(cc, nativeConn)
				if handled, proceed := s.handleVE7CCCommand(cc, "SET/VE7CC"); !handled || !proceed {
					t.Fatal("native selection failed")
				}
				textDone, nativeDone := ve7ccStartWriter(t, text, textConn), ve7ccStartWriter(t, cc, nativeConn)
				// Separate writer owners concurrently consume the very same pointer.
				text.spotChan <- &spotEnvelope{spot: sp}
				cc.spotChan <- &spotEnvelope{spot: sp}
				ve7ccReadWrites(t, textConn, writerV15Oracle(textLine+"\n"))
				ve7ccReadWrites(t, nativeConn, "VE7CC enabled for this session.\r\n"+native)
				text.close("")
				cc.close("")
				writerV15Wait(t, textDone, "shared text shutdown")
				writerV15Wait(t, nativeDone, "shared native shutdown")
				if !reflect.DeepEqual(before, ve7ccSnapshot(sp)) || sp.ModeProvenance != spot.ModeProvenanceSourceExplicit ||
					!sp.ConfidencePercentOK || sp.ConfidencePercent != 37 {
					t.Fatal("writers mutated canonical spot, CW projection or dedupe evidence")
				}
			})
		}
	}
}

func TestVE7CCWriterNativeSkipsGlyphProjection(t *testing.T) {
	s, text, sp, _ := newPathPredictionReuseFixture(t, 1, true, true)
	s.clusterCall, s.writerBatchWait = "N2WQ-2", time.Millisecond
	text.server, text.callsign = s, "W1TEXT"
	sp.Time, sp.Comment, sp.SourceType = s.now(), "hello", spot.SourceManual
	meta := ve7ccTestSpot()
	sp.DXMetadata, sp.DEMetadata = meta.DXMetadata, meta.DEMetadata
	sp.DXMetadata.Grid = "FN32"
	before := ve7ccSnapshot(sp)
	prediction, ok := s.pathPredictionForClient(text, sp)
	if !ok {
		t.Fatal("existing path-display fixture produced no prediction")
	}
	glyph := s.pathGlyphFromPrediction(prediction)
	base := sp.Clone().FormatDXCluster()
	textLine := injectGlyphs(base, glyph)
	if glyph == "" || textLine == base {
		t.Fatal("text witness did not actually exercise glyph injection")
	}
	cc := &Client{server: s, callsign: "W1NATIVE"}
	textConn, nativeConn := ve7ccNewCaptureConn(false), ve7ccNewCaptureConn(false)
	ve7ccAttachWriter(text, textConn)
	ve7ccAttachWriter(cc, nativeConn)
	if handled, proceed := s.handleVE7CCCommand(cc, "SET/VE7CC"); !handled || !proceed {
		t.Fatal("native selection failed")
	}
	textDone, nativeDone := ve7ccStartWriter(t, text, textConn), ve7ccStartWriter(t, cc, nativeConn)
	text.spotChan <- &spotEnvelope{spot: sp}
	cc.spotChan <- &spotEnvelope{spot: sp}
	const native = "CC11^14074.0^DX1AA^11-Jun-2026^1800Z^FT8 hello^DE1AA^291^1^N2WQ-2^8^5^2^3^MA^BC^United States^Canada^FN32^cn89\r\n"
	ve7ccReadWrites(t, textConn, writerV15Oracle(textLine+"\n"))
	ve7ccReadWrites(t, nativeConn, "VE7CC enabled for this session.\r\n"+native)
	text.close("")
	cc.close("")
	writerV15Wait(t, textDone, "glyph text shutdown")
	writerV15Wait(t, nativeDone, "glyph native shutdown")
	if !reflect.DeepEqual(before, ve7ccSnapshot(sp)) {
		t.Fatal("glyph/native writers mutated the shared spot")
	}
}
