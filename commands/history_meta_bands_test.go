package commands

import (
	"bytes"
	"encoding/binary"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/config"
	"dxcluster/spot"

	"github.com/cockroachdb/pebble"
)

func TestMetaBandHistoryParser(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, prefix := range []string{"SHOW DX", "SHOW MYDX", "SHOW/DX"} {
		command, handled, text := p.ParseHistoryCommand(prefix+" 1 BAND warc,6m,WARC MODE CW COMMENT BAND=UHF", "cc")
		if !handled || text != "" || !command.Query.metaBands || !reflect.DeepEqual(command.Query.bands, []string{"30m", "17m", "12m", "6m"}) || command.Query.comment != "BAND=UHF" {
			t.Fatalf("%s meta parse = %+v, %v, %q", prefix, command, handled, text)
		}
	}
	for _, args := range []string{"BAND WARC VHF", "BAND VHF,BOGUS", "BAND UHF BAND WARC", "MODE WARC", "BAND VHF-CW"} {
		command, handled, text := p.ParseHistoryCommand("SHOW DX "+args, "go")
		if !handled || text == "" || !reflect.DeepEqual(command.Query, HistoryQuery{}) {
			t.Fatalf("invalid meta history accepted: %s", args)
		}
	}
	command, _, text := p.ParseHistoryCommand("SHOW DX BAND "+strings.Repeat("HF,VHFUP,", 100)+"HF", "go")
	if text != "" || len(command.Query.bands) != 18 {
		t.Fatalf("retained meta union not bounded/deduplicated: %+v %q", command.Query, text)
	}
}

func TestMetaBandHistoryQueryLocalView(t *testing.T) {
	old := spot.NewSpot("K1ABC", "W1AAA", 70100, "CW")
	old.Band, old.BandNorm = "???", "???"
	p := NewProcessor(nil, &fakeArchive{spots: []*spot.Spot{old}}, nil, nil, nil, nil)
	command, _, text := p.ParseHistoryCommand("SHOW DX BAND VHF", "go")
	if text != "" {
		t.Fatal(text)
	}
	for _, borrow := range []bool{false, true} {
		read := p.CollectHistoryPage
		if borrow {
			read = p.CollectBorrowedHistoryPage
		}
		seen := false
		page, err := read(command.Query, nil, func(s *spot.Spot) bool {
			seen = true
			return s != old && s.Band == "4m" && s.BandNorm == "4m"
		}, time.Now(), nil, nil)
		if err != nil || !seen || len(page.Spots) != 1 || page.Spots[0] != old || old.Band != "???" || old.BandNorm != "???" {
			t.Fatalf("query view changed owned archive object or callback: %+v %v", page, err)
		}
	}
	ordinary, _, _ := p.ParseHistoryCommand("SHOW DX BAND 4m", "go")
	page, err := p.ReadHistoryPage(ordinary.Query, nil, func(*spot.Spot) bool { return true }, time.Now(), nil)
	if err != nil || len(page.Spots) != 0 {
		t.Fatal("ordinary physical history silently reclassified old archive labels")
	}
}

// Hand-built v2 archive bytes deliberately retain nonempty unknown band labels.
// Current Spot constructors/encoders must not establish the expected fixture.
func metaHistoryLegacyRecord(frequency float64, mode, comment string) []byte {
	fields := []string{"K1ABC", "W1AAA", "", mode, comment, "", "", "", "???", "", "", "", ""}
	raw := make([]byte, 54)
	raw[0] = 2
	binary.BigEndian.PutUint64(raw[4:], math.Float64bits(frequency))
	for i, value := range fields {
		binary.BigEndian.PutUint16(raw[28+i*2:], uint16(len(value)))
	}
	for _, value := range fields {
		raw = append(raw, value...)
	}
	return raw
}

func TestMetaBandHistoryRealArchivePagination(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta-history")
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	var keys, records [][]byte
	for i, row := range []struct {
		frequency     float64
		mode, comment string
	}{
		{14030, "CW", "POTA"}, {70000, "CW", "POTA"}, {70650, "CW", "POTA"},
		{math.Nextafter(70650, math.Inf(1)), "CW", "POTA"},
		{50313, "FT8", "POTA"}, {70100, "CW", "SOTA"}, {144100, "CW", "POTA"},
		{14030, "CW", "POTA"}, // An older nonmember establishes a final continuation page.
	} {
		key := make([]byte, 14)
		copy(key, "s|")
		binary.BigEndian.PutUint64(key[2:], uint64(now.Add(-time.Duration(i)*time.Second).UnixNano()))
		binary.BigEndian.PutUint32(key[10:], uint32(i))
		raw := metaHistoryLegacyRecord(row.frequency, row.mode, row.comment)
		if err := db.Set(key, raw, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		keys, records = append(keys, key), append(records, raw)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := archive.NewWriter(config.ArchiveConfig{DBPath: path, RetentionSeconds: 3600, Synchronous: "off"})
	if err != nil {
		t.Fatal(err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			w.Stop()
		}
	})
	p := NewProcessor(nil, w, nil, nil, nil, nil)
	command, _, text := p.ParseHistoryCommand("SHOW DX 1 BAND VHF MODE CW COMMENT POTA", "go")
	if text != "" {
		t.Fatal(text)
	}
	var before []byte
	for _, want := range []float64{70000, 70650, 144100} {
		page, err := p.ReadHistoryPage(command.Query, before, func(s *spot.Spot) bool {
			return s.Band != "???" && s.Band == s.BandNorm
		}, now, nil)
		if err != nil || len(page.Spots) != 1 || page.Spots[0].Frequency != want || page.Spots[0].Band != "???" {
			t.Fatalf("expected frequency %v before %x, got %+v %v", want, before, page, err)
		}
		if bytes.Equal(before, page.Before) {
			t.Fatal("meta history continuation did not advance")
		}
		before = page.Before
	}
	page, err := p.ReadHistoryPage(command.Query, before, func(*spot.Spot) bool { return true }, now, nil)
	if err != nil || len(page.Spots) != 0 || page.End != archive.HistoryExhausted {
		t.Fatalf("unexpected meta history tail: %+v %v", page, err)
	}
	w.Stop()
	stopped = true
	db, err = pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i, key := range keys {
		raw, closer, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		unchanged := bytes.Equal(raw, records[i])
		if err := closer.Close(); err != nil {
			t.Fatal(err)
		}
		if !unchanged {
			t.Fatalf("meta query rewrote archive key %x", key)
		}
	}
}
