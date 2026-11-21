package correctionflow

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"dxcluster/spot"
)

func burstSpot(call, de string, at time.Time, wpm string) *spot.Spot {
	s := spot.NewSpot(call, de, 7034, "CW")
	s.SourceType = spot.SourceRBN
	s.IsHuman = false
	s.RBNCQRawKHz = 7034
	s.RBNCQReceivedAt = at
	if wpm != "" {
		s.CWSpeeds = []string{wpm + " WPM"}
	}
	return s
}

func TestCWBurstCausalEvidence(t *testing.T) {
	base := time.Unix(1800000000, 0)
	for _, tc := range []struct {
		name            string
		offset, width   time.Duration
		receiver, speed string
		want            bool
	}{
		{"clear", 6 * time.Second, 3 * time.Second, "K1AAE", "24", true},
		{"exact separation", 3 * time.Second, 3 * time.Second, "K1AAE", "24", false},
		{"one nanosecond apart", 3*time.Second + 1, 3 * time.Second, "K1AAE", "24", true},
		{"pair wider than limit", 6 * time.Second, 3*time.Second + 1, "K1AAE", "24", false},
		{"batched arrivals", 0, 0, "K1AAE", "24", false},
		{"alias echoes", 6 * time.Second, 3 * time.Second, "K1AAD-2", "24", false},
		{"missing speed", 6 * time.Second, 3 * time.Second, "K1AAE", "", false},
		{"wrong speed", 6 * time.Second, 3 * time.Second, "K1AAE", "20", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := NewCWBurstHistory()
			h.Observe(burstSpot("W1OLD", "K1AAA", base, "19"))
			h.Observe(burstSpot("W1OLD", "K1AAC", base.Add(time.Second), "19"))
			start := base.Add(time.Second + tc.offset)
			h.Observe(burstSpot("W1NEW", "K1AAD", start, "24"))
			h.Observe(burstSpot("W1NEW", tc.receiver, start.Add(tc.width), tc.speed))
			f := burstSpot("W1NEW", "K1AAG", start.Add(4*time.Second), "24")
			h.Observe(f)
			if got := h.Retain(f.CWBurstFocal, "W1OLD"); got != tc.want {
				t.Fatalf("retained %v want %v", got, tc.want)
			}
			// Even a late delivery with an earlier read clock cannot change its focal's history.
			h.Observe(burstSpot("W1OLD", "K1AAJ", start.Add(2*time.Second), "19"))
			h.Observe(burstSpot("W1OLD", "K1AAK", start.Add(3*time.Second), "19"))
			if got := h.Retain(f.Clone().CWBurstFocal, "W1OLD"); got != tc.want {
				t.Fatalf("changed after admission: %v", got)
			}
		})
	}
}

func TestCWBurstBoundAndSpeed(t *testing.T) {
	h := NewCWBurstHistory()
	now := time.Unix(1800000000, 0)
	for i := 0; i < cwBurstBucketSize+20; i++ {
		h.Observe(burstSpot("W1OLD", "K1AAA", now.Add(time.Duration(i)*time.Millisecond), "20"))
	}
	rows, lost := h.Stats()
	if rows != cwBurstBucketSize || lost != 20 {
		t.Fatalf("rows/drops=%d/%d", rows, lost)
	}
	h.Observe(burstSpot("W1OLD", "K1AAC", now.Add(time.Second), "20"))
	h.Observe(burstSpot("W1NEW", "K1AAD", now.Add(9*time.Second), "25"))
	h.Observe(burstSpot("W1NEW", "K1AAE", now.Add(10*time.Second), "25"))
	f := burstSpot("W1NEW", "K1AAG", now.Add(12*time.Second), "25")
	h.Observe(f)
	if h.Retain(f.CWBurstFocal, "W1OLD") {
		t.Fatal("incomplete retained bucket permitted a decision")
	}
	if liveCWSpeed([]string{"20 WPM", "20 WPM"}) != 0 {
		t.Fatal("multi-label speed accepted")
	}
	if CWReceiverFamily("DL/K1ABC-2") != "K1ABC" {
		t.Fatal("receiver family mismatch")
	}
}

// Verify the live pair selector on every receipt in the packaged study. Inject
// archive seconds as receive times; this checks implementation agreement without
// assuming actual network arrival times reproduce the archive.
func TestCWBurstPackagedCorpus(t *testing.T) {
	type report struct {
		sec     int64
		ordinal int
		hz      int64
		family  string
		speed   int
	}
	read := func(data []json.RawMessage) (r report) {
		t.Helper()
		if len(data) != 5 {
			t.Fatal("bad report length")
		}
		for i, v := range []any{&r.sec, &r.ordinal, &r.hz, &r.family, &r.speed} {
			if err := json.Unmarshal(data[i], v); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	f, err := os.Open("../../scripts/research/call-correction-v16/burst-review.jsonl.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	scan := bufio.NewScanner(z)
	h := NewCWBurstHistory()
	hash := sha256.New()
	var total, matched int
	for scan.Scan() {
		var c struct {
			Date, Original, Replacement string
			Focal                       []json.RawMessage
			A                           [][]json.RawMessage `json:"original_history"`
			B                           [][]json.RawMessage `json:"replacement_history"`
		}
		if err := json.Unmarshal(scan.Bytes(), &c); err != nil {
			t.Fatal(err)
		}
		// Each fixture contains its own focal-excluded predictor histories.
		ob, wb := h.bucket(c.Original), h.bucket(c.Replacement)
		ob.count, ob.next, ob.lostThrough = 0, 0, 0
		wb.count, wb.next, wb.lostThrough = 0, 0, 0
		type event struct {
			report
			call string
		}
		var events []event
		for _, r := range c.A {
			events = append(events, event{read(r), c.Original})
		}
		for _, r := range c.B {
			events = append(events, event{read(r), c.Replacement})
		}
		slices.SortFunc(events, func(a, b event) int { return a.ordinal - b.ordinal })
		observe := func(call string, r report) spot.CWBurstFocal {
			s := burstSpot(call, r.family, time.Unix(1800000000+r.sec, 0), strconv.Itoa(r.speed))
			s.RBNCQRawKHz = float64(r.hz) / 1000
			h.Observe(s)
			return s.CWBurstFocal
		}
		for _, row := range events {
			observe(row.call, row.report)
		}
		focal := read(c.Focal)
		if h.Retain(observe(c.Original, focal), c.Replacement) {
			matched++
			fmt.Fprintf(hash, "%s:%d\n", c.Date, focal.ordinal)
		}
		total++
	}
	if err := scan.Err(); err != nil {
		t.Fatal(err)
	}
	const identityDigest = "008ae5fb291f303d051427770c4b105c07021966831e0852bb4a5717ecbd3eb5"
	if total != 4270 || matched != 658 || fmt.Sprintf("%x", hash.Sum(nil)) != identityDigest {
		t.Fatalf("%d cases, %d matches, selected IDs hash %x", total, matched, hash.Sum(nil))
	}
}

func BenchmarkCWBurstHistory(b *testing.B) {
	h := NewCWBurstHistory()
	base := time.Unix(1800000000, 0)
	s := burstSpot("W1NEW", "K1AAA", base, "24")
	b.Run("observe", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			s.CWBurstFocal = spot.CWBurstFocal{}
			h.Observe(s)
		}
	})
	h = NewCWBurstHistory()
	for i := 0; i < cwBurstBucketSize/2; i++ {
		h.Observe(burstSpot("W1OLD", []string{"K1AAC", "K1AAD"}[i%2], base.Add(time.Duration(i)*time.Millisecond), "16"))
		h.Observe(burstSpot("W1NEW", []string{"K1AAC", "K1AAD"}[i%2], base.Add(time.Minute+time.Duration(i)*time.Millisecond), "24"))
	}
	s = burstSpot("W1NEW", "K1AAA", base.Add(70*time.Second), "24")
	h.Observe(s)
	b.Run("dense", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if !h.Retain(s.CWBurstFocal, "W1OLD") {
				b.Fatal("dense gate failed")
			}
		}
	})
}
