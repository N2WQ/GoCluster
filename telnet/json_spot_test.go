package telnet

import (
	"bytes"
	"encoding/binary"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
	"github.com/cockroachdb/pebble"
)

func decodeJSONSpot(t testing.TB, wire []byte) []any {
	t.Helper()
	if bytes.Count(wire, []byte{'\n'}) != 1 || len(wire) == 0 || wire[len(wire)-1] != '\n' {
		t.Fatalf("invalid line framing (%d bytes)", len(wire))
	}
	var record []any
	if err := json.Unmarshal(wire, &record); err != nil || len(record) != 23 {
		t.Fatalf("invalid v1 record: fields=%d err=%v", len(record), err)
	}
	for _, index := range []int{18, 19} {
		if station, ok := record[index].([]any); !ok || len(station) != 11 {
			t.Fatalf("invalid station at position %d: %v", index, record[index])
		}
	}
	return record
}

func writeTestJSONLive(t testing.TB, c *Client, sp *spot.Spot) []any {
	t.Helper()
	var encoder jsonSpotEncoder
	var wire bytes.Buffer
	if err := encoder.writeLive(&wire, c, &spotEnvelope{spot: sp}); err != nil {
		t.Fatal(err)
	}
	return decodeJSONSpot(t, wire.Bytes())
}

func TestJSONSpotPublicRecords(t *testing.T) {
	c := &Client{sessionProfile: &sessionProfile{}, server: &Server{}}
	s := &spot.Spot{
		ID: 1<<53 - 1, Time: time.Unix(0, 1234567890123456789), Frequency: 14116.19, ObservedFrequency: 14116.125,
		DXCall: "N2SZ", DECall: "VE9JWW-7", DECallStripped: "VE9JWW-#", Band: "20m", Mode: "USB",
		ModeProvenance: spot.ModeProvenanceCommentExplicit, Comment: "雪\n\"trail\"\\path", SourceType: spot.SourcePeer,
		SourceNode: "W3LPL-2", IsHuman: true, Confidence: "S", ConfidencePercent: 42, ConfidencePercentOK: true,
		DXMetadata: spot.CallMetadata{ADIF: 291, Country: "United States", Continent: "NA", CQZone: 3, ITUZone: 7, State: "WA", Grid: "CN87", GridDerived: true, IARURegion: spot.IARURegion2},
		DEMetadata: spot.CallMetadata{ADIF: 1, Country: "Canada", Continent: "NA", CQZone: 5, ITUZone: 11, State: "ON", Grid: "FN25", IARURegion: spot.IARURegion2},
		Events:     spot.EventPOTA, ToxicityStatus: spot.ToxicityToxic,
		SpotterIP: "NEVER-SEND-IP", TTL: 199, ToxicityCategories: []string{"NEVER-SEND-CATEGORY"}, ToxicityModel: "NEVER-SEND-MODEL",
		IsTestSpotter: true, DXCellID: 238, RBNCQRawKHz: 198765.4321, CWBurstFocal: spot.CWBurstFocal{Original: "NEVER-SEND-BURST"},
	}
	s.EnsureNormalized()
	s.RBNCQReceivedAt = time.Unix(0, 1888812345678)
	s.SecondaryDedupe[0] = spot.SecondaryDedupeEvidence{AnchorHz: 95555533, ModeID: 221, HasAnchor: true, Enabled: true}
	s.BandNorm, s.DECallNormStripped = "NEVER-SEND-BAND-CACHE", "NEVER-SEND-DE-CACHE"
	var buf bytes.Buffer
	var e jsonSpotEncoder
	if err := e.writeLive(&buf, c, &spotEnvelope{spot: s}); err != nil {
		t.Fatal(err)
	}
	const want = `["s",9007199254740991,"1234567890123456789",14116.19,14116.125,"20m","USB","comment_explicit",null,"雪\n\"trail\"\\path",[],["POTA"],"PEER","W3LPL-2",true,false,"S",42,["N2SZ",291,"United States","NA",3,7,"WA","CN87",true,"CN",2],["VE9JWW-#",1,"Canada","NA",5,11,"ON","FN25",false,"FN",2],true,null,null]` + "\n"
	if buf.String() != want {
		t.Fatalf("live record:\n got %s want %s", buf.String(), want)
	}
	decodeJSONSpot(t, buf.Bytes())
	s.ID, s.Time = 1<<53, time.Time{}
	s.SourceType, s.Mode, s.Report, s.HasReport = spot.SourceFT8, "FT8", -14, true
	r := writeTestJSONLive(t, c, s)
	if r[1] != nil || r[2] != nil || r[8] != float64(-14) {
		t.Fatal(r)
	}
	s.IsBeacon, s.Comment, s.CWDisplayAvailable, s.CWDisplayComment, s.CWSpeeds = true, "30 WPM", true, "", []string{"30 WPM", "34 WPM"}
	s.SourceType, s.Mode = spot.SourceRBN, "CW"
	for _, show := range []bool{false, true} {
		c.configuredSettings.CWSpeed = show
		r = writeTestJSONLive(t, c, s)
		if r[9] != "" || (len(r[10].([]any)) == 2) != show || r[15] != true {
			t.Fatal(r)
		}
	}
	s.CWDisplayAvailable = false
	s.Comment = ""
	s.DXMetadata = spot.CallMetadata{}
	s.DEMetadata = spot.CallMetadata{}
	s.DXGridNorm, s.DEGridNorm, s.DXContinentNorm, s.DEContinentNorm = "", "", "", ""
	r = writeTestJSONLive(t, c, s)
	if r[9] != "" || !reflect.DeepEqual(r[18], []any{"N2SZ", nil, nil, nil, nil, nil, nil, nil, false, nil, nil}) {
		t.Fatal(r)
	}
}

func TestJSONSpotIndependentOptionalFields(t *testing.T) {
	limit := strings.Repeat("é", 128)
	for i := 0; i < 4; i++ {
		server := &Server{}
		s := spot.NewSpot("N2SZ", "W1XYZ", 14030, "CW")
		s.Comment = "full"
		s.SourceNode, s.DXMetadata.Country, s.DEMetadata.Country = limit, limit, limit
		want := []any{limit, limit, limit}
		counts := []uint64{0, 0, 0}
		if i < 3 {
			*[]*string{&s.SourceNode, &s.DXMetadata.Country, &s.DEMetadata.Country}[i] += "x"
			want[i], counts[i] = nil, 1
		}
		record := writeTestJSONLive(t, &Client{sessionProfile: &sessionProfile{}, server: server}, s)
		got := []any{record[13], record[18].([]any)[2], record[19].([]any)[2]}
		stats := server.JSONSpotStats()
		if !reflect.DeepEqual(got, want) || record[9] != "full" || !reflect.DeepEqual(counts, []uint64{stats.NodeOmissions, stats.DXCountryOmissions, stats.DECountryOmissions}) {
			t.Fatalf("fields=%v stats=%+v", got, stats)
		}
	}
}

func TestJSONSpotExtendedGridArchiveReopen(t *testing.T) {
	cfg := config.ArchiveConfig{DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 4, BatchSize: 1, BatchIntervalMS: 1}
	s := spot.NewSpot("K1ABC", "N0CALL", 14074, "FT8")
	s.SourceType = spot.SourcePSKReporter
	s.DXMetadata.Grid, s.DEMetadata.Grid = "fn31pr12", "em10aa34"
	s.EnsureNormalized()
	c := &Client{sessionProfile: &sessionProfile{}, server: &Server{}}
	check := func(r []any) {
		t.Helper()
		dx, de := r[18].([]any), r[19].([]any)
		if dx[7] != "FN31PR" || dx[9] != "FN" || de[7] != "EM10AA" || de[9] != "EM" {
			t.Fatalf("dx=%v de=%v", dx, de)
		}
	}
	check(writeTestJSONLive(t, c, s))
	w, err := archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	w.Enqueue(s)
	w.Stop()
	w, err = archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	var rows jsonHistoryRows
	defer rows.release()
	p, err := w.ReadHistoryPage(archive.HistoryRequest{Limit: 1, Now: time.Now(), Collect: func(sp *spot.Spot) error {
		if sp.DXGridNorm != "FN31PR12" || sp.DEGridNorm != "EM10AA34" {
			t.Fatal("archive lost precision:", sp.DXGridNorm, sp.DEGridNorm)
		}
		return rows.collect(c.server, sp, false)
	}})
	if err != nil || p.Selected != 1 || rows.count != 1 {
		t.Fatalf("page=%+v err=%v", p, err)
	}
	var encoder jsonSpotEncoder
	var wire bytes.Buffer
	if err = encoder.writeHistory(&wire, c, rows.first); err != nil {
		t.Fatal(err)
	}
	check(decodeJSONSpot(t, wire.Bytes()))
	if s.DXMetadata.Grid != "fn31pr12" || s.DEMetadata.Grid != "em10aa34" {
		t.Fatal("encoder mutated original spot")
	}
}

func TestJSONSpotPartialStationFields(t *testing.T) {
	sp := spot.NewSpot("W1BEN", "K1ABC", 14030, "CW")
	sp.DXMetadata.Grid, sp.DEMetadata.Grid = "fn", "zz"
	sp.DXMetadata.State, sp.DEMetadata.State = " wa ", strings.Repeat("AA", 65535)
	sp.EnsureNormalized()
	row := jsonProjectSpot(&Server{}, sp, false, false)
	if row.dx.grid != "" || row.dx.grid2 != "FN" || row.de.grid2 != "" ||
		row.dx.state != "" || row.de.state != "" {
		t.Fatalf("DX %+v, DE %+v", row.dx, row.de)
	}
}

func TestJSONSpotNativeArchiveRecords(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "native")
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for v := 2; v <= 7; v++ {
		raw, err := os.ReadFile(filepath.Join("..", "archive", "testdata", fmt.Sprintf("history-v%d.bin", v)))
		if err != nil {
			t.Fatal(err)
		}
		key := binary.BigEndian.AppendUint64([]byte("s|"), uint64(now.UnixNano()))
		key = binary.BigEndian.AppendUint32(key, uint32(v))
		if err = db.Set(key, raw, pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := archive.NewWriter(config.ArchiveConfig{DBPath: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	ctyDB := ve7ccTestCTY(t)
	c := &Client{sessionProfile: &sessionProfile{}, server: &Server{ctyLookup: func() *cty.CTYDatabase { return ctyDB }}}
	var rows jsonHistoryRows
	defer rows.release()
	p, err := w.ReadHistoryPage(archive.HistoryRequest{Limit: 50, Now: now, Collect: func(s *spot.Spot) error { return rows.collect(c.server, s, true) }})
	if err != nil || p.Selected != 6 || rows.count != 6 {
		t.Fatalf("page=%+v err=%v", p, err)
	}
	var encoder jsonSpotEncoder
	v := 2
	for row := rows.first; row != nil; row = row.next {
		var buf bytes.Buffer
		if err := encoder.writeHistory(&buf, c, row); err != nil {
			t.Fatal(err)
		}
		r := decodeJSONSpot(t, buf.Bytes())
		event, comment := "SOTA", `POTA-1234 "trail"`
		if v < 4 {
			event = "POTA"
		}
		if v == 7 {
			comment = `CQ "trail"`
		}
		dx, de := r[18].([]any), r[19].([]any)
		if r[0] != "h" || r[1] != nil || r[7] != nil || r[17] != nil || r[21] != nil || r[22] != nil || r[9] != comment ||
			!reflect.DeepEqual(r[11], []any{event}) || (len(r[10].([]any)) == 2) != (v == 7) || (r[4] == nil) != (v == 2) ||
			dx[2] != "Current USA" || dx[5] != float64(8) || dx[10] != float64(2) || (dx[6] == "CT") != (v >= 6) || (de[6] == "MA") != (v >= 6) {
			t.Fatalf("v%d: %s", v, buf.String())
		}
		v++
	}
}

func TestJSONSpotRecipientPathMatchesText(t *testing.T) {
	s, c, sp, _ := newPathPredictionReuseFixture(t, 1, true, true)
	c.server = s
	c.filter = testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "MODE=FT8 AND PATH=HIGH"})
	env := deliverPathPredictionReuseSpot(t, s, c, sp)
	if env.pathPrediction == nil {
		t.Fatal("missing admission prediction")
	}
	other := &Client{sessionProfile: &sessionProfile{grid: "EM10",
		gridCell:   pathreliability.CellID(88),
		noiseClass: "QUIET",
	}, server: s}
	check := func(client *Client) string {
		t.Helper()
		textLine := s.formatSpotEnvelopeForClient(client, env)
		var encoder jsonSpotEncoder
		var wire bytes.Buffer
		if err := encoder.writeLive(&wire, client, env); err != nil {
			t.Fatal(err)
		}
		r := decodeJSONSpot(t, wire.Bytes())
		if r[21] != s.pathClassForClient(client, sp) {
			t.Fatalf("grid=%q row=%s", client.grid, wire.String())
		}
		glyph, ok := r[22].(string)
		column := spot.CurrentDXClusterLayout().GlyphColumn - 1
		if !ok || len(glyph) != 1 || len(textLine) <= column || glyph[0] != textLine[column] {
			t.Fatalf("glyph=%v text=%q", r[22], textLine)
		}
		return r[21].(string)
	}
	before := check(c)
	originalGlyph := env.pathPrediction.result.Glyph
	for _, glyph := range []string{"HIGH", "雪"} {
		env.pathPrediction.result.Glyph = glyph
		check(c)
	}
	env.pathPrediction.result.Glyph = originalGlyph
	after := check(other)
	if before == after {
		t.Fatalf("two grids produced the same class %s", before)
	}
	c.pathMu.Lock()
	c.grid, c.gridCell = other.grid, other.gridCell
	c.pathMu.Unlock()
	if check(c) != after {
		t.Fatal("stale queued grid reused")
	}
}

type jsonTestWriteSink struct {
	largest int
	calls   int
	bytes   int64
	fail    int
}

func (s *jsonTestWriteSink) Write(p []byte) (int, error) {
	s.calls++
	s.largest = max(s.largest, len(p))
	if s.calls == s.fail {
		return len(p) - 1, nil
	}
	s.bytes += int64(len(p))
	return len(p), nil
}

func TestJSONSpotBoundedWritesAndAllocations(t *testing.T) {
	s := spot.NewSpot("K1ABC", "N0CALL", 14030, "CW")
	s.Confidence, s.CWDisplayAvailable, s.CWDisplayComment, s.CWSpeeds = "S", true, "snow 雪", []string{"0 WPM", "34 WPM"}
	s.Events = spot.EventPOTA | spot.EventSOTA
	s.DXMetadata.Grid = "fn31pr12"
	s.EnsureNormalized()
	client := &Client{sessionProfile: &sessionProfile{}, server: &Server{}}
	client.configuredSettings.CWSpeed = true
	env := &spotEnvelope{spot: s}
	var e jsonSpotEncoder
	sink := new(jsonTestWriteSink)
	if err := e.writeLive(sink, client, env); err != nil || sink.calls != 1 {
		t.Fatalf("ordinary writes=%d err=%v", sink.calls, err)
	}
	if n := testing.AllocsPerRun(100, func() {
		if e.writeLive(io.Discard, client, env) != nil {
			panic("write")
		}
	}); n != 0 {
		t.Fatalf("warm allocations %.1f", n)
	}
	s.CWDisplayComment = strings.Repeat("\x01", 65535) + "\xff"
	sink = new(jsonTestWriteSink)
	if err := e.writeLive(sink, client, env); err != nil || sink.bytes < 65535*6 || sink.largest > len(e.buf) {
		t.Fatalf("sink=%+v err=%v", sink, err)
	}
	if client.server.JSONSpotStats().InvalidUTF8Bytes != 1 {
		t.Fatal("invalid byte not counted")
	}
	for _, fail := range []int{1, 2, sink.calls} {
		broken := &jsonTestWriteSink{fail: fail}
		if !errors.Is(e.writeLive(broken, client, env), io.ErrShortWrite) || broken.calls != fail {
			t.Fatalf("write failure %d: %+v", fail, broken)
		}
	}
	for _, freqs := range [][2]float64{{0, 1}, {-1, 0}, {math.NaN(), 0}, {math.Inf(1), 0}, {1, math.NaN()}, {1, -1}, {1, math.Inf(1)}} {
		s.Frequency, s.ObservedFrequency = freqs[0], freqs[1]
		sink = new(jsonTestWriteSink)
		if !errors.Is(e.writeLive(sink, client, env), errJSONInvalidSpot) || sink.calls != 0 {
			t.Fatal("invalid numeric field wrote bytes", freqs)
		}
	}
}

func FuzzJSONSpotComments(f *testing.F) {
	for i, s := range []string{"", "雪", "\r\n\x00\\\"", "\xff", strings.Repeat("x", 1023) + "🛰"} {
		f.Add(s, []int64{0, -1, 1234567890123456789, math.MaxInt64, math.MinInt64}[i])
	}
	f.Fuzz(func(t *testing.T, comment string, nanos int64) {
		if len(comment) > 65535 {
			t.Skip()
		}
		row := writeTestJSONLive(t, &Client{sessionProfile: &sessionProfile{}}, &spot.Spot{Time: time.Unix(0, nanos), Frequency: 14030, Comment: comment, SourceType: spot.SourceManual})
		if row[9] != string([]rune(comment)) || row[2] != strconv.FormatInt(nanos, 10) {
			t.Fatalf("source (%d,%q), decoded (%v,%q)", nanos, comment, row[2], row[9])
		}
	})
}

func BenchmarkJSONSpotMixedSources(b *testing.B) {
	envs, want := mixedJSONWriterSpots(), mixedJSONWire(b)
	c := &Client{sessionProfile: &sessionProfile{}, server: &Server{}}
	c.configuredSettings.CWSpeed = true
	var e jsonSpotEncoder
	var bytesPerSet int
	for i := range envs {
		var wire bytes.Buffer
		if err := e.writeLive(&wire, c, &envs[i]); err != nil || !bytes.Equal(wire.Bytes(), want[i]) {
			b.Fatalf("row %d: %s want %s err=%v", i, wire.Bytes(), want[i], err)
		}
		bytesPerSet += wire.Len()
	}
	b.ReportAllocs()
	b.SetBytes(int64(bytesPerSet / len(envs)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := e.writeLive(io.Discard, c, &envs[i%len(envs)]); err != nil {
			b.Fatal(err)
		}
	}
}
