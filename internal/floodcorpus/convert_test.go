package floodcorpus

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

func TestNativeCaptureTimingAndOriginalBytes(t *testing.T) {
	first := testWire("PC11", "K1AAA", "  CQ\t~literal\xfe  ")
	second := testWire("PC61", "K1BBB", "CQ")
	third := testWire("PC26", "K1CCC", "CQ")
	// More than one native reader buffer: A's first complete sentence must not
	// borrow B's timestamp. One journal record may be returned in several Reads.
	noise := bytes.Repeat([]byte{'\r'}, 4090)
	a := string(noise) + first + second[:12]
	b := second[12:] + third
	input := writeFixture(t, encodeJournal(t, journalRecords([]capturedBlock{{"s2c", 10, a}, {"s2c", 20, b}})))
	out := filepath.Join(t.TempDir(), "corpus")
	m, err := Convert(input, out, "s2c", "peer-A")
	if err != nil || !m.Complete || m.Events != 3 {
		t.Fatalf("import: %+v %v", m, err)
	}
	var events []Event
	if err := readEvents(out, m, func(e Event) error { events = append(events, e); return nil }); err != nil {
		t.Fatal(err)
	}
	for i, want := range []int64{10, 20, 20} {
		if events[i].NS != want {
			t.Fatalf("event%d ns%d want%d", i, events[i].NS, want)
		}
	}
	data, err := base64.StdEncoding.DecodeString(events[0].SentenceBase64)
	if err != nil || !bytes.Equal(data, []byte(first[:len(first)-3])) {
		t.Fatalf("decoded sentence bytes changed: %v", err)
	}
	if events[0].Truth != nil {
		t.Fatal("equality invented truth")
	}
	if _, err := Convert(input, out, "s2c", "peer-A"); err == nil {
		t.Fatal("overwrote corpus")
	}
}

func TestCapturePartialTailAndUnsentPrefix(t *testing.T) {
	for _, tail := range []string{"PC11^14020^unfinished", string([]byte{255, 251})} {
		t.Run("pending", func(t *testing.T) {
			wire := testWire("PC11", "K1AAA", "CQ")
			input := writeFixture(t, encodeJournal(t, journalRecords([]capturedBlock{{"s2c", 1, wire + tail}})))
			out := filepath.Join(t.TempDir(), "out")
			m, err := Convert(input, out, "s2c", "peer-A")
			if err != nil || m.Complete || !m.CaptureComplete || !m.PendingTail || m.Events != 1 {
				t.Fatalf("tail unqualified: %+v %v", m, err)
			}
			o := Options{Threshold: 2, WindowSeconds: 10, ActorCap: 4}
			if _, err := Compare(out, o); err == nil {
				t.Fatal("implicit partial comparison")
			}
			o.AllowPartial = true
			r, err := Compare(out, o)
			if err != nil || !r.Partial {
				t.Fatalf("sensitivity: %+v %v", r, err)
			}
		})
	}
	wire := testWire("PC11", "K1AAA", "CQ")
	records := journalRecords([]capturedBlock{{"s2c", 1, wire + testWire("PC61", "K1BBB", "CQ")}})
	footer := &records[len(records)-1]
	footer.Reason = pointer("TimeoutError: session deadline reached")
	footer.Sent["s2c"] = pointer(int64(len(wire)))
	out := filepath.Join(t.TempDir(), "out")
	m, err := Convert(writeFixture(t, encodeJournal(t, records)), out, "s2c", "peer-A")
	if err != nil || m.Complete || m.Events != 1 || m.Unsent == 0 || m.PendingTail {
		t.Fatalf("unsent complete frame replayed: %+v %v", m, err)
	}
}

func TestCaptureMutationCannotPublishManifest(t *testing.T) {
	input := writeFixture(t, encodeJournal(t, journalRecords([]capturedBlock{{"s2c", 1, testWire("PC11", "K1AAA", "CQ")}})))
	out := filepath.Join(t.TempDir(), "out")
	_, err := convert(input, out, "s2c", "peer-A", func() {
		raw, readErr := os.ReadFile(input)
		if readErr != nil {
			t.Fatal(readErr)
		}
		raw = bytes.ReplaceAll(raw, []byte(`"ns":1,`), []byte(`"ns":2,`))
		if writeErr := os.WriteFile(input, raw, 0600); writeErr != nil {
			t.Fatal(writeErr)
		}
	})
	if err == nil {
		t.Fatal("mutated source accepted")
	}
	if _, err := os.Stat(filepath.Join(out, "manifest.json")); !os.IsNotExist(err) {
		t.Fatal("failed import published manifest")
	}
}
