package archive

import (
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestCWSpeedArchiveReopen(t *testing.T) {
	cfg := config.ArchiveConfig{DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 8, BatchSize: 1, BatchIntervalMS: 1}
	w, err := NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	s := spot.NewSpot("K1ABC", "W1XYZ", 14030, "CW")
	s.Time = time.Now().UTC()
	s.Comment = "0 WPM 30 WPM POTA"
	s.CWDisplayAvailable, s.CWDisplayComment = true, "POTA"
	s.CWSpeeds = []string{"0 WPM", "30 WPM"}
	w.Enqueue(s)
	w.Stop()

	reopened, err := NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	rows, err := reopened.Recent(10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	got := rows[0]
	if !got.CWDisplayAvailable || got.CWDisplayComment != "POTA" || got.Comment != s.Comment || !reflect.DeepEqual(got.CWSpeeds, s.CWSpeeds) {
		t.Fatalf("archive projection changed: %+v", got)
	}
}

func FuzzCWSpeedArchiveDecode(f *testing.F) {
	sp := spot.NewSpot("K1ABC", "W1XYZ", 14030, "CW")
	sp.CWDisplayAvailable, sp.CWDisplayComment = true, "POTA"
	sp.CWSpeeds = []string{"0 WPM", "200 WPM"}
	f.Add(encodeRecord(sp))
	f.Add([]byte{7, 0, 0, 0})
	f.Fuzz(func(t *testing.T, raw []byte) {
		record, err := decodeRecord(raw)
		if err != nil || record.cwSpeeds == "" {
			return
		}
		for _, label := range strings.Split(record.cwSpeeds, "\x00") {
			value, ok := strings.CutSuffix(label, " WPM")
			n, err := strconv.Atoi(value)
			if !ok || err != nil || n < 0 || n > 200 || value == "" || strings.Trim(value, "0123456789") != "" {
				t.Fatalf("decoder admitted invalid speed %q", label)
			}
		}
	})
}

func TestCWSpeedArchiveAbsenceAndZeroDiffer(t *testing.T) {
	for _, speeds := range [][]string{nil, {"0 WPM"}} {
		s := spot.NewSpot("K1ABC", "W1XYZ", 14030, "CW")
		s.CWDisplayAvailable, s.CWSpeeds = true, speeds
		got, err := decodeSpot(time.Now().UnixNano(), encodeRecord(s))
		if err != nil || !got.CWDisplayAvailable || !reflect.DeepEqual(got.CWSpeeds, speeds) {
			t.Fatalf("speeds=%q got=%+v err=%v", speeds, got, err)
		}
	}
}

func TestCWSpeedArchiveReadsVersionSix(t *testing.T) {
	s := spot.NewSpot("K1ABC", "W1XYZ", 14030, "CW")
	s.ObservedFrequency = 14030.25
	s.Comment = "POTA"
	raw := encodeRecord(s)
	old := append([]byte(nil), raw[:recordHeaderSizeV6]...)
	old = append(old, raw[recordHeaderSize:]...)
	old[0] = recordVersionV6
	got, err := decodeSpot(time.Now().UnixNano(), old)
	if err != nil || got.CWDisplayAvailable || got.Comment != "POTA" || got.ObservedFrequency != s.ObservedFrequency {
		t.Fatalf("version-six record got=%+v err=%v", got, err)
	}
}

func TestCWSpeedArchiveRejectsMalformedLabels(t *testing.T) {
	for _, label := range []string{"-1 WPM", "201 WPM", "2 WPM\x00", "30 BPS"} {
		s := spot.NewSpot("K1ABC", "W1XYZ", 14030, "CW")
		s.CWDisplayAvailable, s.CWSpeeds = true, []string{label}
		if _, err := decodeRecord(encodeRecord(s)); err == nil {
			t.Fatalf("accepted malformed label %q", label)
		}
	}
}
