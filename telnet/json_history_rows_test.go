package telnet

import (
	"errors"
	"strings"
	"testing"

	"dxcluster/archive"
	"dxcluster/spot"
)

func TestJSONHistoryMaximumPageAndRelease(t *testing.T) {
	// Use independent upper bounds from all stored versions. Four bytes per
	// DX input byte is deliberately more than UTF-8 case mapping requires.
	field := strings.Repeat("x", 65535)
	dx := strings.Repeat("X", 4*65535)
	country := strings.Repeat("c", 256)
	labels := strings.Split(strings.TrimSuffix(strings.Repeat("0 WPM\x00", 170), "\x00"), "\x00")
	p := new(jsonHistoryRows)
	base := processJSONHistoryBytes.Load()
	defer p.release()
	for i := range 250 {
		row := &jsonHistoryRow{band: field, mode: field, source: field, comment: field,
			node: country, speeds: labels, timeNS: int64(250 - i),
			dx: jsonHistoryStation{call: dx, country: country, continent: "NA", state: "MA", grid: "FN42AB", grid2: "FN"},
			de: jsonHistoryStation{call: field, country: country, continent: "EU", grid: "JO21AB", grid2: "JO"},
		}
		if err := p.retain(row); err != nil {
			t.Fatalf("row %d: %v", i+1, err)
		}
	}
	if p.count != 250 || p.charged != processJSONHistoryBytes.Load()-base || p.charged > jsonHistoryBudgetBytes {
		t.Fatalf("count %d, charge %d, global %d", p.count, p.charged, processJSONHistoryBytes.Load())
	}
	t.Logf("250 maximal projection rows: %d charged bytes of %d", p.charged, jsonHistoryBudgetBytes)
	for i, row := 1, p.first; row != nil; i, row = i+1, row.next {
		if row.timeNS != int64(i) || len(row.comment) != len(field) || len(row.speeds) != 170 || row.speeds[169] != "0 WPM" {
			t.Fatalf("damaged row at %d", i)
		}
	}
	p.release()
	if used := processJSONHistoryBytes.Load(); used != base {
		t.Fatalf("charge after release %d, want %d", used, base)
	}
}

func TestJSONHistoryBusyDiscardsPartialProjection(t *testing.T) {
	var p jsonHistoryRows
	base := processJSONHistoryBytes.Load()
	defer p.release()
	row := &spot.Spot{DXCall: "K1ABC", DECall: "W1XYZ", Frequency: 14030, Comment: strings.Repeat("b", 35)}
	if err := p.collect(nil, row, false); err != nil {
		t.Fatal(err)
	}
	external := jsonHistoryBudgetBytes - processJSONHistoryBytes.Load()
	processJSONHistoryBytes.Add(external)
	defer processJSONHistoryBytes.Add(-external)
	if err := p.collect(nil, row, false); !errors.Is(err, archive.ErrHistoryBusy) || p.count != 1 {
		t.Fatalf("after refused row: count %d, err %v", p.count, err)
	}
	p.release()
	if used := processJSONHistoryBytes.Load(); used != base+external {
		t.Fatalf("partial charge retained: %d", used-base-external)
	}
}
