package floodcorpus

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func pointer[T any](value T) *T { return &value }

type capturedBlock struct {
	direction string
	ns        int64
	data      string
}

func journalRecords(blocks []capturedBlock) []captureRecord {
	records := []captureRecord{{Kind: "start", Seq: pointer(int64(0)), NS: pointer(int64(0)), Version: pointer(1), UTC: "2026-11-15T12:00:00+00:00", ToolSHA: RecorderSHA256, MaxSeconds: pointer(int64(600)), MaxBytes: pointer(int64(32 << 20))}}
	var offsets [2]int64
	last := int64(0)
	for _, b := range blocks {
		i := directionIndex(b.direction)
		records = append(records, captureRecord{Kind: "data", Seq: pointer(int64(len(records))), NS: pointer(b.ns), Direction: b.direction, Offset: pointer(offsets[i]), Data: base64.StdEncoding.EncodeToString([]byte(b.data))})
		offsets[i] += int64(len(b.data))
		last = b.ns
	}
	for _, direction := range []string{"c2s", "s2c"} {
		last++
		records = append(records, captureRecord{Kind: "eof", Seq: pointer(int64(len(records))), NS: pointer(last), Direction: direction})
	}
	counts := func() map[string]*int64 {
		return map[string]*int64{"c2s": pointer(offsets[0]), "s2c": pointer(offsets[1])}
	}
	records = append(records, captureRecord{Kind: "end", Seq: pointer(int64(len(records))), NS: pointer(last + 1), Reason: pointer("eof"), Received: counts(), Recorded: counts(), Sent: counts()})
	return records
}

func encodeJournal(t testing.TB, records []captureRecord) []byte {
	t.Helper()
	var b bytes.Buffer
	for _, r := range records {
		if err := json.NewEncoder(&b).Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	return b.Bytes()
}

func writeFixture(t testing.TB, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "capture.jsonl")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func testWire(kind, dx, comment string) string {
	wire := kind + "^14020^" + dx + "^15-Nov-2026^1200Z^" + comment + "^W1XYZ^K2ORG"
	if kind == "PC61" {
		wire += "^2001:db8::1"
	}
	if kind == "PC26" {
		wire += "^ "
	}
	return wire + "^H5^~\r\n"
}

func TestCaptureStrictJournalContract(t *testing.T) {
	blocks := []capturedBlock{{"c2s", 1, "LAB_ONLY\r\n"}, {"s2c", 2, testWire("PC11", "K1AAA", "CQ")}}
	for _, tc := range []struct {
		name   string
		change func([]captureRecord) []captureRecord
	}{
		{"valid", func(r []captureRecord) []captureRecord { return r }},
		{"missing_footer", func(r []captureRecord) []captureRecord { return r[:len(r)-1] }},
		{"duplicate_sequence", func(r []captureRecord) []captureRecord { r[1].Seq = pointer(int64(0)); return r }},
		{"decreasing_time", func(r []captureRecord) []captureRecord { r[2].NS = pointer(int64(0)); return r }},
		{"negative_time", func(r []captureRecord) []captureRecord { r[1].NS = pointer(int64(-1)); return r }},
		{"missing_time", func(r []captureRecord) []captureRecord { r[1].NS = nil; return r }},
		{"offset_gap", func(r []captureRecord) []captureRecord { r[2].Offset = pointer(int64(1)); return r }},
		{"invalid_base64", func(r []captureRecord) []captureRecord { r[2].Data = "!"; return r }},
		{"oversized_payload", func(r []captureRecord) []captureRecord {
			r[2].Data = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'x'}, 16385))
			return r
		}},
		{"null_counter", func(r []captureRecord) []captureRecord { r[len(r)-1].Sent["s2c"] = nil; return r }},
		{"impossible_counter", func(r []captureRecord) []captureRecord { r[len(r)-1].Sent["s2c"] = pointer(int64(10000)); return r }},
		{"wrong_recorded", func(r []captureRecord) []captureRecord { r[len(r)-1].Recorded["s2c"] = pointer(int64(0)); return r }},
		{"false_eof", func(r []captureRecord) []captureRecord { r[len(r)-1].Sent["s2c"] = pointer(int64(0)); return r }},
		{"duplicate_eof", func(r []captureRecord) []captureRecord { r[len(r)-2].Direction = "c2s"; return r }},
		{"trailing_record", func(r []captureRecord) []captureRecord { return append(r, r[1]) }},
		{"wrong_dependency", func(r []captureRecord) []captureRecord { r[0].ToolSHA = strings.Repeat("0", 64); return r }},
		{"missing_UTC", func(r []captureRecord) []captureRecord { r[0].UTC = ""; return r }},
		{"invalid_UTC", func(r []captureRecord) []captureRecord { r[0].UTC = "invalid"; return r }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info, err := ScanCapture(writeFixture(t, encodeJournal(t, tc.change(journalRecords(blocks)))))
			if tc.name == "valid" {
				if err != nil || !info.Complete || info.Recorded[1] != int64(len(blocks[1].data)) {
					t.Fatalf("valid journal: %+v %v", info, err)
				}
			} else if err == nil {
				t.Fatal("invalid journal accepted")
			}
		})
	}
	valid := encodeJournal(t, journalRecords(blocks))
	for name, data := range map[string][]byte{
		"duplicate_key": bytes.Replace(valid, []byte(`"seq":0`), []byte(`"seq":0,"seq":0`), 1),
		"float_time":    bytes.Replace(valid, []byte(`"ns":0`), []byte(`"ns":0.0`), 1),
		"null_time":     bytes.Replace(valid, []byte(`"ns":0`), []byte(`"ns":null`), 1),
		"truncated":     valid[:len(valid)-1],
		"large_record":  append(bytes.Repeat([]byte{'x'}, captureRecordBytes+1), '\n'),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ScanCapture(writeFixture(t, data)); err == nil {
				t.Fatal("invalid bytes accepted")
			}
		})
	}
}

func TestCaptureFileLimit(t *testing.T) {
	p := filepath.Join(t.TempDir(), "large.jsonl")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(MaxInputBytes + 1); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()
	if _, err := ScanCapture(p); err == nil || err.Error() != "capture must be a bounded regular file" {
		t.Fatalf("oversized file did not hit file guard: %v", err)
	}
}

func FuzzCaptureRecord(f *testing.F) {
	for _, seed := range []string{`{"kind":"data","seq":1,"ns":2,"direction":"s2c","offset":0,"data":"QQ=="}`, `{"seq":0,"seq":1}`, `{"ns":null}`, `{"ns":1.0}`, `[]`, `{"ns":9223372036854775808}`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 4096 {
			t.Skip()
		}
		var r captureRecord
		if err := strictJSON([]byte(raw), &r); err == nil {
			if !json.Valid([]byte(raw)) {
				t.Fatal("accepted invalid JSON")
			}
			encoded, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			var other captureRecord
			if strictJSON(encoded, &other) != nil {
				t.Fatal("accepted record cannot roundtrip")
			}
		}
	})
}
