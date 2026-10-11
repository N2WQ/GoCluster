//go:build !qualification

package cluster

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/cty"
	"dxcluster/spot"

	"github.com/cockroachdb/pebble"
)

type b32CallSurface struct {
	captureSurface
	mu    sync.Mutex
	lines []string
}

func (s *b32CallSurface) AppendCall(line string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lines = append(s.lines, line)
}

func (s *b32CallSurface) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.lines...)
}

type b32LogWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *b32LogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *b32LogWriter) decisions() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var result []string
	for _, line := range strings.Split(w.buf.String(), "\n") {
		if strings.HasPrefix(line, "Call decision:") || strings.HasPrefix(line, "Call corrected:") {
			result = append(result, line)
		}
	}
	return result
}

func b32CaptureDecisions(t *testing.T, p *outputPipeline, destination string) func() []string {
	t.Helper()
	if destination == "ui" {
		s := &b32CallSurface{}
		p.dash = s
		return s.snapshot
	}
	w := &b32LogWriter{}
	previousWriter, previousFlags, previousPrefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetOutput(w)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
		log.SetPrefix(previousPrefix)
	})
	p.dash = nil
	return w.decisions
}

func b32AssertDecision(t *testing.T, got []string, want string) {
	t.Helper()
	var expected []string
	if want != "" {
		expected = []string{want}
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("complete evaluation event(s):\n got %q\nwant %q", got, expected)
	}
}

func b32OriginalSpot(call string, at time.Time) *spot.Spot {
	s := newDeliveryTestSpot(call, "N0AAA", at)
	s.SourceType, s.SourceNode = spot.SourceManual, "N0SOURCE"
	return s
}

func b32RejectCTY(t *testing.T, p *outputPipeline) {
	t.Helper()
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(`<?xml version="1.0"?><plist version="1.0"><dict></dict></plist>`))
	if err != nil {
		t.Fatal(err)
	}
	p.ctyLookup = func() *cty.CTYDatabase { return db }
}

func b32AssertDelivered(t *testing.T, f ambiguityDeliveryFixture, original, call, glyph string, percent int, available bool, at time.Time) {
	t.Helper()
	diag := "CW --%"
	if available {
		diag = fmt.Sprintf("CW %d%%", percent)
	}
	for i, comment := range []string{"CW", diag} {
		conn, reader := f.normalConn, f.normalReader
		if i == 1 {
			conn, reader = f.diagConn, f.diagReader
		}
		if got, want := correctionEvidenceReadDX(t, conn, reader), correctionEvidenceWire(call, glyph, comment, at); got != want {
			t.Errorf("actual local TCP %d:\n got %q\nwant %q", i, got, want)
		}
	}
	correctionEvidenceAssertOriginalPeer(t, f.peer, original, at)
	rows := f.ring.GetRecent(2)
	if len(rows) != 1 {
		t.Errorf("ring rows=%d, want exactly one local delivered spot", len(rows))
	} else {
		want := correctionEvidenceDeliveryCase{wantCall: call, wantGlyph: glyph}
		correctionEvidenceAssertSpot(t, "ring", rows[0], want, at, false)
		if rows[0].ConfidencePercent != percent || rows[0].ConfidencePercentOK != available || rows[0].SourceType != spot.SourceManual || rows[0].SourceNode != "N0SOURCE" || rows[0].SpotterIP != "192.0.2.1" || rows[0].SnapshotForAsync() != rows[0] {
			t.Errorf("ring availability/source/ownership contract: %+v", rows[0])
		}
	}
	b32AssertArchive(t, f, call, glyph, 14020, "N0SOURCE", "", at)
}

// The current archive contract has no confidence percentage or added correction
// provenance. Existing ingress source fields must nevertheless survive reopen.
func b32AssertArchive(t *testing.T, f ambiguityDeliveryFixture, call, glyph string, frequency float64, sourceNode, comment string, at time.Time) {
	t.Helper()
	f.writer.Stop()
	w, err := archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	rows, err := w.Recent(4)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Errorf("reopened archive has %d rows, want only %s", len(rows), call)
		return
	}
	s := rows[0]
	if s.DXCallNorm != call || s.DECallNorm != "N0AAA" || s.Frequency != frequency || s.ModeNorm != "CW" || s.Confidence != glyph || !s.Time.Equal(at) ||
		s.ConfidencePercent != 0 || s.ConfidencePercentOK || s.SourceType != spot.SourceManual || s.SourceNode != sourceNode || s.Comment != comment {
		t.Errorf("reopened archive identity/glyph and existing-source contract: %+v", s)
	}
}

func b32AssertSuppressionBarrier(t *testing.T, f ambiguityDeliveryFixture, at time.Time) {
	t.Helper()
	s := newDeliveryTestSpot("K1XYZ", "N0AAA", at)
	s.SourceType, s.Frequency, s.ToxicityStatus = spot.SourceManual, 14030, spot.ToxicitySafe
	f.pipeline.processSpot(s, nil)
	for i, comment := range []string{"CW", "CW --%"} {
		conn, reader := f.normalConn, f.normalReader
		if i == 1 {
			conn, reader = f.diagConn, f.diagReader
		}
		want := strings.Replace(correctionEvidenceWire("K1XYZ", "?", comment, at), "14020.00", "14030.00", 1)
		if got := correctionEvidenceReadDX(t, conn, reader); got != want {
			t.Errorf("actual positive FIFO barrier %d:\n got %q\nwant %q", i, got, want)
		}
	}
	peerOriginalAssertFields(t, f.peer, []string{"14030", "K1XYZ", at.Format("02-Jan-2006"), at.Format("1504Z"), " ", "N0AAA", "N0CALL-1", "192.0.2.1"})
	if rows := f.ring.GetRecent(4); len(rows) != 1 || rows[0].DXCallNorm != "K1XYZ" || rows[0].Confidence != "?" || rows[0].ConfidencePercentOK || rows[0].ConfidencePercent != 0 {
		t.Errorf("only successful barrier belongs in ring: %+v", rows)
	}
	b32AssertArchive(t, f, "K1XYZ", "?", 14030, "", "", at)
}

func b32RecognitionStore(t *testing.T, source string, at time.Time) *spot.CustomSCPStore {
	t.Helper()
	raw := make(map[string][]byte)
	if source == "static_only" {
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, uint64(at.Add(-time.Hour).Unix()))
		raw["m|K1ABC"] = value
	} else {
		for i, receiver := range []string{"H0AAA", "H0BBB", "H0CCC"} {
			value := make([]byte, 10)
			binary.BigEndian.PutUint64(value[:8], uint64(at.Add(-time.Minute).Unix()))
			binary.BigEndian.PutUint16(value[8:], uint16(101*(i+1)))
			raw["o|K1ABC|20m|cw|"+receiver] = value
		}
	}
	path := filepath.Join(t.TempDir(), "independent-recognition")
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range raw {
		if err := db.Set([]byte(name), value, pebble.Sync); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	customSCPFreshnessAssertBytes(t, path, raw)
	store := customSCPFreshnessOpen(t, spot.CustomSCPOptions{
		Path: path, HorizonDays: 60, StaticHorizonDays: 395, MaxKeys: 8, MaxSpottersPerKey: 8,
		CoreMinScore: 3, CoreMinH3Cells: 2, SFloorMinScore: 3, SFloorExactMinH3Cells: 2, SFloorFamilyMinH3Cells: 2,
	})
	if store.StaticContains("K1ABC") != (source == "static_only") || store.HasSFloorSupportExact("K1ABC", "20m", "CW", 3, at) != (source == "history_only") {
		t.Fatal("independent static and history populations were not isolated")
	}
	return store
}
