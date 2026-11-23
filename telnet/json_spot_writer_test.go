// The qualification workload uses the real connection writer, a loopback TCP
// socket, and a receiving client comparing every record to frozen wire bytes.
package telnet

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/spot"
)

func mixedJSONWriterSpots() []spotEnvelope {
	cases := []struct {
		source              spot.SourceType
		frequency           float64
		mode, node, comment string
		report              int
		hasReport           bool
	}{
		{spot.SourceManual, 14175.25, "USB", "K1ABC", "Thanks José / POTA", 0, false},
		{spot.SourceRBN, 14030.25, "CW", "RBN", "22 WPM CQ 34 WPM", 19, true},
		{spot.SourceFT8, 14074.25, "FT8", "RBN-DIGITAL", "", -14, true},
		{spot.SourceFT4, 14080.25, "FT4", "RBN-DIGITAL", "", 2, true},
		{spot.SourcePSKReporter, 14070.25, "PSK31", "", "", -21, true},
		{spot.SourceUpstream, 14199.25, "LSB", "UPSTREAM-1", `QSL via "bureau" \ direct`, 0, false},
		{spot.SourcePeer, 14034.25, "CW", "K2NODE", "SOTA-1234 POTA", 0, false},
	}
	out := make([]spotEnvelope, len(cases))
	for i, tc := range cases {
		s := spot.NewSpot("K1ABC", "VE1XYZ", tc.frequency, tc.mode)
		s.ID, s.Time = uint64(i+1), time.Unix(0, 1234567890123456789)
		s.SourceType, s.SourceNode, s.Comment, s.Report, s.HasReport = tc.source, tc.node, tc.comment, tc.report, tc.hasReport
		s.ModeProvenance = spot.ModeProvenanceSourceExplicit
		s.DXMetadata = spot.CallMetadata{ADIF: 291, Country: "United States", Continent: "NA", CQZone: 5, ITUZone: 8, State: "MA", Grid: "FN31", IARURegion: spot.IARURegion2}
		s.DEMetadata = spot.CallMetadata{ADIF: 1, Country: "Canada", Continent: "NA", CQZone: 5, ITUZone: 9, State: "ON", Grid: "FN25", IARURegion: spot.IARURegion2}
		spot.ApplySourceHumanFlag(s)
		switch tc.source {
		case spot.SourceManual:
			s.Events, s.ModeProvenance = spot.EventPOTA, spot.ModeProvenanceCommentExplicit
		case spot.SourceRBN:
			s.CWDisplayAvailable, s.CWDisplayComment, s.CWSpeeds = true, "CQ", []string{"22 WPM", "34 WPM"}
			s.Confidence, s.ConfidencePercent, s.ConfidencePercentOK = "P", 89, true
		case spot.SourceFT8:
			s.ObservedFrequency = 14075.125
		case spot.SourcePSKReporter:
			s.DXMetadata.Grid, s.DEMetadata.Grid = "FN31PR12", "EM10AA34"
		case spot.SourcePeer:
			s.Events, s.ModeProvenance = spot.EventPOTA|spot.EventSOTA, spot.ModeProvenanceUnknown
		case spot.SourceUpstream:
			s.ModeProvenance = spot.ModeProvenanceCommentExplicit
		}
		s.EnsureNormalized()
		out[i] = spotEnvelope{spot: s, enqueueAt: time.Now()}
	}
	return out
}

// One writer owns all counters. The qualification reader sees the counters
// only after the writer joins. Connecting, negotiating and closing are outside
// the measured writes. The optional gate holds one actual socket batch until
// the retained-memory fixture has filled the bounded queue.
type jsonQualificationConn struct {
	net.Conn
	bins                      [6]int64 // <=511, <=2047, <=8191, <=16383, <=32767, >=32768
	written, minimum, maximum int64
	active                    atomic.Bool
	gate                      atomic.Bool
	entered, release          chan struct{}
}

func (c *jsonQualificationConn) Write(data []byte) (int, error) {
	if c.active.Load() {
		if c.gate.Swap(false) {
			close(c.entered)
			<-c.release
		}
		n := int64(len(data))
		i := 0
		for _, limit := range [...]int64{512, 2048, 8192, 16384, 32768} {
			if n < limit {
				break
			}
			i++
		}
		c.bins[i]++
		if c.minimum == 0 || n < c.minimum {
			c.minimum = n
		}
		c.maximum = max(c.maximum, n)
		c.written += n
	}
	return c.Conn.Write(data)
}

type jsonQualificationSession struct {
	c      *Client
	conn   *jsonQualificationConn
	peer   net.Conn
	reader *bufio.Reader
	joined <-chan struct{}
}

func openJSONQualificationSession(tb testing.TB) *jsonQualificationSession {
	tb.Helper()
	var lc net.ListenConfig
	listener, err := lc.Listen(tb.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	defer listener.Close()
	dialer := net.Dialer{Timeout: 3 * time.Second}
	peer, err := dialer.DialContext(tb.Context(), "tcp4", listener.Addr().String())
	if err != nil {
		tb.Fatal(err)
	}
	socket, err := listener.Accept()
	if err != nil {
		peer.Close()
		tb.Fatal(err)
	}
	conn := &jsonQualificationConn{Conn: socket, entered: make(chan struct{}), release: make(chan struct{})}
	server := &Server{clusterCall: "W1BENCH", writerBatchMaxBytes: 16384, writerBatchWait: 5 * time.Millisecond,
		jsonLimits: newJSONSpotLimits(128, 1<<20, spot.CurrentTaxonomy()), latency: newLatencyMetrics()}
	c := &Client{server: server, callsign: "K1BENCH", conn: conn, writer: bufio.NewWriterSize(conn, 32768),
		done: make(chan struct{}), controlChan: make(chan controlMessage, 8), spotChan: make(chan *spotEnvelope, 256)}
	c.configuredSettings.CWSpeed = true
	session := &jsonQualificationSession{c: c, conn: conn, peer: peer, reader: bufio.NewReaderSize(peer, 32768), joined: writerV15Start(c)}
	tb.Cleanup(func() { session.close(tb) })
	if handled, ok := server.handleJSONCommand(c, "SET JSON ON"); !handled || !ok {
		tb.Fatal("JSON switch failed")
	}
	if err := peer.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		tb.Fatal(err)
	}
	for i, prefix := range []string{"JSON ON 1\r\n", `["hello",1,`, jsonSpotSchemaLine, jsonStationSchemaLine} {
		line, err := session.reader.ReadString('\n')
		if err != nil || !strings.HasPrefix(line, prefix) {
			tb.Fatalf("activation %d: %q err=%v", i, line, err)
		}
	}
	if err := peer.SetReadDeadline(time.Time{}); err != nil {
		tb.Fatal(err)
	}
	conn.active.Store(true)
	return session
}

func (q *jsonQualificationSession) close(tb testing.TB) {
	tb.Helper()
	if q.c == nil {
		return
	}
	select {
	case <-q.conn.release:
	default:
		close(q.conn.release)
	}
	q.c.close("")
	q.peer.Close()
	writerV15Wait(tb, q.joined, "qualified socket close")
	q.c = nil
	q.reader = nil
	q.peer = nil
}

func mixedJSONWire(tb testing.TB) [][]byte {
	tb.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "json-v1-writer.ndjson"))
	if err != nil {
		tb.Fatal(err)
	}
	// Preserve the required LF, with one final LF even on CRLF checkouts.
	raw = bytes.ReplaceAll(raw, []byte("\r\n"), []byte("\n"))
	rows := bytes.SplitAfter(append(bytes.TrimRight(raw, "\n"), '\n'), []byte{'\n'})
	if len(rows) != 8 || len(rows[7]) != 0 {
		tb.Fatal("expected seven wire fixtures")
	}
	return rows[:7]
}

func (q *jsonQualificationSession) verify(count int, expected [][]byte) <-chan error {
	result := make(chan error, 1)
	go func() {
		for i := 0; i < count; i++ {
			data, err := q.reader.ReadSlice('\n')
			if err != nil {
				result <- err
				return
			}
			if !bytes.Equal(data, expected[i%len(expected)]) {
				result <- fmt.Errorf("record %d mismatch: got %s want %s", i, data, expected[i%len(expected)])
				return
			}
		}
		result <- nil
	}()
	return result
}

func (q *jsonQualificationSession) send(tb testing.TB, env *spotEnvelope, done <-chan error, deadline <-chan time.Time) {
	select {
	case q.c.spotChan <- env:
	case err := <-done:
		tb.Fatalf("reader finished before admission: %v", err)
	case <-q.joined:
		tb.Fatal("writer exited before admission")
	case <-deadline:
		tb.Fatal("admission timed out")
	}
}

func checkJSONQualificationDrain(tb testing.TB, done <-chan error, deadline <-chan time.Time) {
	tb.Helper()
	select {
	case err := <-done:
		if err != nil {
			tb.Fatal(err)
		}
	case <-deadline:
		tb.Fatal("reader did not receive every spot")
	}
}

func BenchmarkJSONSpotSocketMixed(b *testing.B) {
	envs, expected := mixedJSONWriterSpots(), mixedJSONWire(b)
	session := openJSONQualificationSession(b)
	latency := session.c.server.latency.firstByte
	drained := session.verify(b.N, expected)
	deadline := time.NewTimer(60 * time.Second)
	defer deadline.Stop()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Each admission owns an immutable envelope and a fresh timestamp,
		// just as broadcast does. It can still share the normalized spot.
		session.send(b, &spotEnvelope{spot: envs[i%len(envs)].spot, enqueueAt: time.Now()}, drained, deadline.C)
	}
	checkJSONQualificationDrain(b, drained, deadline.C)
	b.StopTimer()
	session.close(b)
	if got := atomic.LoadUint64(&latency.next); got != uint64(b.N) {
		b.Fatalf("first-byte samples=%d, want %d", got, b.N)
	}
	var bytesExpected int64
	for i, row := range expected {
		count := b.N / len(expected)
		if i < b.N%len(expected) {
			count++
		}
		bytesExpected += int64(count * len(row))
	}
	if session.conn.written != bytesExpected {
		b.Fatalf("wire bytes=%d expected=%d", session.conn.written, bytesExpected)
	}
	b.ReportMetric(float64(bytesExpected)/float64(b.N), "wire-B/spot")
	for i, name := range [...]string{"writes<512", "writes<2Ki", "writes<8Ki", "writes<16Ki", "writes<32Ki", "writes>=32Ki"} {
		b.ReportMetric(float64(session.conn.bins[i]), name)
	}
	b.ReportMetric(float64(session.conn.minimum), "min-write-B")
	b.ReportMetric(float64(session.conn.maximum), "max-write-B")
	b.ReportMetric(float64(b.N), "verified-spots")
}

func sampledLiveHeap() uint64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

func TestJSONSpotSocketRetained(t *testing.T) {
	envs, expected := mixedJSONWriterSpots(), mixedJSONWire(t)
	base := sampledLiveHeap()
	q := openJSONQualificationSession(t)
	idle := sampledLiveHeap()
	q.conn.gate.Store(true)
	queueSize := cap(q.c.spotChan)
	drained := q.verify(queueSize+1, expected)
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	q.send(t, &envs[0], drained, deadline.C)
	writerV15Wait(t, q.conn.entered, "blocked socket batch")
	for i := 1; i <= queueSize; i++ {
		sp := envs[i%len(envs)].spot.Clone()
		sp.Comment = strings.Clone(sp.Comment)
		q.send(t, &spotEnvelope{spot: sp, enqueueAt: time.Now()}, drained, deadline.C)
	}
	if len(q.c.spotChan) != queueSize {
		t.Fatal("writer consumed rows during the retained snapshot")
	}
	occupied := sampledLiveHeap()
	close(q.conn.release)
	checkJSONQualificationDrain(t, drained, deadline.C)
	empty := sampledLiveHeap()
	q.close(t)
	released := sampledLiveHeap()
	t.Logf("process live heap bytes baseline=%d idle=%d full-queue=%d drained=%d closed=%d records=%d max-sampled=%d", base, idle, occupied, empty, released, queueSize+1, max(base, idle, occupied, empty, released))
	runtime.KeepAlive(expected)
	runtime.KeepAlive(envs)
}
