package telnet

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"dxcluster/filter"
)

func TestJSONYAMLMaximumSchema7Reply(t *testing.T) {
	cfg, rules := v7PreparedFixture(t)
	cfg.Filters.CC = v7CCCapacityCandidate(cfg.Filters.CC, 6118)
	// Persisted settings retain their exact string. Use the real CONFIG
	// renderer with its two copies of that setting and widest status.
	cfg.Settings.Dialect = `NN\Q`
	status := v7WidestCapacityStatus()
	status.Effective.Dialect = "go"
	presets := map[string]readbackPresetStatus{
		"go": {Name: strings.Repeat("1", 32)}, "cc": {Name: strings.Repeat("1", 32)},
	}
	requestID, revision := strings.Repeat("1", 31), strings.Repeat("\"", 128)
	render := func() string {
		t.Helper()
		frame, err := renderV7YAMLReadback(cfg, rules, status, presets, "CONFIG", requestID, revision)
		if err != nil {
			t.Fatal(err)
		}
		return frame
	}
	spare := maxV7YAMLBytes - len(render())
	cfg.Settings.Dialect += strings.Repeat("N", spare/2)
	if spare%2 != 0 {
		requestID += "1"
	}
	s, client := v7MachineTestClient(t)
	record := &filter.V7UserRecord{ConfigurationVersion: filter.V7ConfigurationVersion, Configuration: cfg}
	if err := s.admitV7LoginRecord(client, record, time.Now()); err != nil {
		t.Fatalf("configuration failed normal login admission: %v", err)
	}
	raw := render()
	if len(raw) != maxV7YAMLBytes || !strings.HasPrefix(raw, "---\r\n") || !strings.HasSuffix(raw, "...\r\n") {
		t.Fatalf("fixture failed to reach the complete schema 7 frame boundary: bytes=%d", len(raw))
	}
	var wire bytes.Buffer
	conn := ve7ccNewCaptureConn(false)
	client.conn = conn
	owner, err := client.reserveConfigurationOutput()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.Release)
	if err := client.enqueueConfigurationResponse(owner, []byte(raw), false); err != nil {
		t.Fatal(err)
	}
	w := jsonSessionWriter{out: jsonWriterOutput{client: client, buf: make([]byte, 0, 16<<10)}}
	if _, _, err := w.control(<-client.controlChan); err != nil {
		t.Fatal(err)
	}
	if len(conn.writes) < 2 || !owner.released.Load() {
		t.Fatal("maximum reply did not stream across ordinary batches or release its owner")
	}
	for len(conn.writes) != 0 {
		wire.Write(<-conn.writes)
	}
	if wire.Len() <= len(raw) || wire.Len() > maxJSONYAMLRecordBytes || bytes.Count(wire.Bytes(), []byte{'\n'}) != 1 || wire.Bytes()[wire.Len()-1] != '\n' {
		t.Fatalf("invalid JSON wire size or framing: raw=%d wire=%d", len(raw), wire.Len())
	}
	var fields []string
	if err := json.Unmarshal(wire.Bytes(), &fields); err != nil || len(fields) != 2 || fields[0] != "yaml" || fields[1] != raw {
		t.Fatalf("decoded CONFIG differs from the raw CRLF document: %v", err)
	}
	t.Logf("schema 7 CONFIG raw=%d JSON=%d scratch=%d bytes", len(raw), wire.Len(), len(w.spots.buf))
	wire.Reset()
	if _, err := writeJSONYAML(&wire, &w.spots.buf, []byte(raw+"x")); !errors.Is(err, errReadbackTooLarge) || wire.Len() != 0 {
		t.Fatalf("oversize raw document wrote %d bytes, err=%v", wire.Len(), err)
	}
}

func TestJSONStringFixedScratch(t *testing.T) {
	// An ordinary raw C0 byte needs six encoded bytes. Also exercise the
	// 2x bound for printable YAML content at the complete raw-frame cap.
	scratch := new(jsonQuoteScratch)
	control := strings.Repeat("\x01", jsonQuoteChunkBytes)
	quoted, count := appendJSONString(scratch[:0], control)
	if count != 0 || len(quoted) != len(scratch) || &quoted[0] != &scratch[0] {
		t.Fatal("a worst-case string chunk exceeds writer scratch")
	}
	raw := bytes.Repeat([]byte{'\\'}, maxV7YAMLBytes)
	var wire bytes.Buffer
	if _, err := writeJSONYAML(&wire, scratch, raw); err != nil || wire.Len() != maxJSONYAMLRecordBytes {
		t.Fatalf("escaped size stress: wire=%d want=%d err=%v", wire.Len(), maxJSONYAMLRecordBytes, err)
	}
	allocs := testing.AllocsPerRun(5, func() {
		if _, err := writeJSONYAML(io.Discard, scratch, raw); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("streaming a valid maximum raw frame allocated %.1f times", allocs)
	}
}

func FuzzJSONStringChunks(f *testing.F) {
	f.Add("")
	f.Add("CW 株\t\r\n\x00\"\\<&\ufffd")
	f.Add(strings.Repeat("\x01", 2*jsonQuoteChunkBytes))
	f.Add(strings.Repeat("\x80", 2*jsonQuoteChunkBytes))
	for i := 1; i < utf8.UTFMax; i++ {
		f.Add(strings.Repeat("a", jsonQuoteChunkBytes-i) + "🌐\xfe\xff" + strings.Repeat("b", jsonQuoteChunkBytes))
		f.Add(strings.Repeat("\x80", jsonQuoteChunkBytes-i) + "🌐" + strings.Repeat("\x80", i))
	}
	f.Fuzz(func(t *testing.T, source string) {
		if len(source) > maxV7YAMLBytes {
			t.Skip()
		}
		var scratch jsonQuoteScratch
		var wire bytes.Buffer
		count, err := writeJSONString(&wire, &scratch, []byte(source))
		if err != nil {
			t.Fatal(err)
		}
		var decoded string
		if err := json.Unmarshal(wire.Bytes(), &decoded); err != nil || decoded != string([]rune(source)) {
			t.Fatalf("string round trip failed: bytes=%d err=%v", len(source), err)
		}
		invalid := 0
		for i, r := range source {
			if r == utf8.RuneError && !strings.HasPrefix(source[i:], "\ufffd") {
				invalid++
			}
		}
		ordinary, ordinaryCount := appendJSONString(nil, source)
		if count != invalid || ordinaryCount != invalid || !bytes.Equal(ordinary, wire.Bytes()) {
			t.Fatalf("streaming and ordinary encoding differ: bytes=%d invalid=%d counts=%d,%d", len(source), invalid, count, ordinaryCount)
		}
	})
}

type jsonShortWriter struct {
	writes int
	failAt int
}

func (w *jsonShortWriter) Write(b []byte) (int, error) {
	w.writes++
	if w.writes == w.failAt {
		return len(b) - 1, nil
	}
	return len(b), nil
}

func TestJSONYAMLShortWrite(t *testing.T) {
	input := bytes.Repeat([]byte{'x'}, 2*jsonQuoteChunkBytes)
	for _, failAt := range []int{1, 2, 3, 4} {
		w := &jsonShortWriter{failAt: failAt}
		var scratch jsonQuoteScratch
		if _, err := writeJSONYAML(w, &scratch, input); !errors.Is(err, io.ErrShortWrite) || w.writes != failAt {
			t.Fatalf("short write %d, writes=%d err=%v", failAt, w.writes, err)
		}
	}
}
