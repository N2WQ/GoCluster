package peer

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// These literal expectations pin the parser boundary separately from the
// encoder: changing both to erase protected payload must not make a test pass.
func TestFramePayloadRoundTripGoldens(t *testing.T) {
	for _, tc := range []struct {
		name, input, frameType string
		fields                 []string
		hop                    int
		wire                   string
	}{
		{"minimized_failure", "PC00^H0^^H0^^H0", "PC00", []string{"H0", "", "H0", ""}, 0, "PC00^H0^^H0^^H0^"},
		{"empty_gap", "PC00^DATA^H7^^H3^", "PC00", []string{"DATA", "H7", ""}, 3, "PC00^DATA^H7^^H3^"},
		{"multiple_empty_gap", "PC00^DATA^H7^^^H3^", "PC00", []string{"DATA", "H7", "", ""}, 3, "PC00^DATA^H7^^^H3^"},
		{"space_gap", "PC00^DATA^H7^ ^H3^", "PC00", []string{"DATA", "H7", " "}, 3, "PC00^DATA^H7^ ^H3^"},
		{"tab_gap", "PC00^DATA^H7^\t^H3^", "PC00", []string{"DATA", "H7", "\t"}, 3, "PC00^DATA^H7^\t^H3^"},
		{"unicode_space_gap", "PC00^DATA^H7^\u2003^H3^", "PC00", []string{"DATA", "H7", "\u2003"}, 3, "PC00^DATA^H7^\u2003^H3^"},
		{"malformed_hop_like_payload", "PC00^DATA^H9x^^H3^", "PC00", []string{"DATA", "H9x", ""}, 3, "PC00^DATA^H9x^^H3^"},
		{"lowercase_transport", "pc00^DATA^H7^^h03^~\r\n", "PC00", []string{"DATA", "H7", ""}, 3, "PC00^DATA^H7^^H3^"},
		{"contiguous_stack", "PC00^DATA^H7^H3^", "PC00", []string{"DATA"}, 3, "PC00^DATA^H3^"},
		{"contiguous_malformed_stack", "PC00^DATA^H9x^H3^", "PC00", []string{"DATA"}, 3, "PC00^DATA^H3^"},
		{"zero_fields_h0", "PC00^H0^", "PC00", nil, 0, "PC00^H0^"},
		{"zero_fields_h7", "PC00^H7^", "PC00", nil, 7, "PC00^H7^"},
		{"one_empty_field", "PC00^^H3^", "PC00", []string{""}, 3, "PC00^^H3^"},
		{"bare_header", "PC00^", "PC00", []string{""}, 0, "PC00^^H0^"},
		{"default_h0", "PC00^DATA", "PC00", []string{"DATA"}, 0, "PC00^DATA^H0^"},
		{"unsuffixed_empty_fields", "PC00^DATA^^", "PC00", []string{"DATA", "", ""}, 0, "PC00^DATA^^^H0^"},
		{"pc92_stack", "PC92^NODE^123^A^^9CALL:ver^H95^H94^H93^", "PC92", []string{"NODE", "123", "A", "", "9CALL:ver"}, 93, "PC92^NODE^123^A^^9CALL:ver^H93^"},
		{"pc92_keepalive_extension", "PC92^N1NODE^123^K^5N1NODE^3^21^^H98^H99^", "PC92", []string{"N1NODE", "123", "K", "5N1NODE", "3", "21", "", "H98"}, 99, "PC92^N1NODE^123^K^5N1NODE^3^21^^H98^H99^"},
		{"pc92_large_hop_like_extension", "PC92^N1NODE^123^K^5N1NODE^3^21^H123^H99^", "PC92", []string{"N1NODE", "123", "K", "5N1NODE", "3", "21", "H123"}, 99, "PC92^N1NODE^123^K^5N1NODE^3^21^H123^H99^"},
		{"pc93_text_and_metadata", "PC93^N1NODE^123^LOGGER^K1ABC^*^H9x^H1ABC^^H99^", "PC93", []string{"N1NODE", "123", "LOGGER", "K1ABC", "*", "H9x", "H1ABC", ""}, 99, "PC93^N1NODE^123^LOGGER^K1ABC^*^H9x^H1ABC^^H99^"},
		{"pc93_h0_empty_metadata", "PC93^NODE^123^K1ABC^W1XYZ^*^H123^^^H0^", "PC93", []string{"NODE", "123", "K1ABC", "W1XYZ", "*", "H123", "", ""}, 0, "PC93^NODE^123^K1ABC^W1XYZ^*^H123^^^H0^"},
		{"pc11_hop_like_origin", "PC11^14074^K1ABC^04-Oct-2026^1200Z^CQ^W1XYZ^H1ABC^H3^~", "PC11", []string{"14074", "K1ABC", "04-Oct-2026", "1200Z", "CQ", "W1XYZ", "H1ABC"}, 3, "PC11^14074^K1ABC^04-Oct-2026^1200Z^CQ^W1XYZ^H1ABC^H3^"},
		{"pc61_original_ip", "PC61^14074^K1ABC^04-Oct-2026^1200Z^CQ^W1XYZ^N0CALL^2001:0DB8:0000::1^H3^", "PC61", []string{"14074", "K1ABC", "04-Oct-2026", "1200Z", "CQ", "W1XYZ", "N0CALL", "2001:0DB8:0000::1"}, 3, "PC61^14074^K1ABC^04-Oct-2026^1200Z^CQ^W1XYZ^N0CALL^2001:0DB8:0000::1^H3^"},
		{"pc26_requested_payload", "PC26^14074^K1ABC^04-Oct-2026^1200Z^CQ^W1XYZ^N0CALL^H1ABC^H3^", "PC26", []string{"14074", "K1ABC", "04-Oct-2026", "1200Z", "CQ", "W1XYZ", "N0CALL", "H1ABC"}, 3, "PC26^14074^K1ABC^04-Oct-2026^1200Z^CQ^W1XYZ^N0CALL^H1ABC^H3^"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame, err := ParseFrame(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			if frame.Type != tc.frameType || frame.Hop != tc.hop || !reflect.DeepEqual(frame.Fields, tc.fields) || frame.Raw != tc.input {
				t.Fatalf("parsed=%+v expected type=%q fields=%q hop=%d raw=%q", frame, tc.frameType, tc.fields, tc.hop, tc.input)
			}
			before := snapshotProtocolFrame(frame)
			if got := frame.Encode(tc.hop); got != tc.wire {
				t.Fatalf("encoded=%q expected=%q", got, tc.wire)
			}
			if !reflect.DeepEqual(*frame, before) {
				t.Fatalf("Encode mutated frame: before=%+v after=%+v", before, frame)
			}
			reparsed, err := ParseFrame(tc.wire)
			if err != nil || reparsed.Type != tc.frameType || reparsed.Hop != tc.hop || !reflect.DeepEqual(reparsed.Fields, tc.fields) {
				t.Fatalf("reparsed=%+v error=%v", reparsed, err)
			}
			if got := reparsed.Encode(reparsed.Hop); got != tc.wire {
				t.Fatalf("second encoding=%q expected=%q", got, tc.wire)
			}
		})
	}
}

func TestFrameEncodeFormatterConventions(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame *Frame
		hop   int
		wire  string
	}{
		{"nil_receiver", nil, 0, ""},
		{"nil_payload_h0", &Frame{Type: "PC00"}, 0, "PC00^H0^"},
		{"empty_slice_h3", &Frame{Type: "PC00", Fields: []string{}}, 3, "PC00^H3^"},
		{"empty_field_h0", &Frame{Type: "PC00", Fields: []string{""}}, 0, "PC00^^H0^"},
		{"nil_payload_negative", &Frame{Type: "PC00"}, -1, "PC00^"},
		{"empty_field_negative", &Frame{Type: "PC00", Fields: []string{""}}, -1, "PC00^"},
		{"gap_negative", &Frame{Type: "PC00", Fields: []string{"DATA", "H7", ""}}, -1, "PC00^DATA^H7^"},
		{"gap_hop_override", &Frame{Type: "PC00", Fields: []string{"DATA", "H7", ""}, Hop: 3, Raw: "unchanged"}, 2, "PC00^DATA^H7^^H2^"},
		// Manual payload ending H7 has no gap. The formatter preserves it, but
		// the generic parser may interpret the resulting contiguous hop stack.
		{"manual_hop_like_payload", &Frame{Type: "PC00", Fields: []string{"DATA", "H7"}, Hop: 9, Raw: "unparsed"}, 3, "PC00^DATA^H7^H3^"},
		{"pc26_no_hop", &Frame{Type: "PC26", Fields: []string{"14074", "K1ABC", "04-Oct-2026", "1200Z", "CQ", "W1XYZ", "N0CALL"}}, -1, "PC26^14074^K1ABC^04-Oct-2026^1200Z^CQ^W1XYZ^N0CALL^"},
		{"pc26_empty_requested_no_hop", &Frame{Type: "PC26", Fields: []string{"14074", "K1ABC", "04-Oct-2026", "1200Z", "CQ", "W1XYZ", "N0CALL", ""}}, -1, "PC26^14074^K1ABC^04-Oct-2026^1200Z^CQ^W1XYZ^N0CALL^^"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var before Frame
			if tc.frame != nil {
				before = snapshotProtocolFrame(tc.frame)
			}
			if got := tc.frame.Encode(tc.hop); got != tc.wire {
				t.Fatalf("encoded=%q expected=%q", got, tc.wire)
			}
			if tc.frame != nil && !reflect.DeepEqual(*tc.frame, before) {
				t.Fatalf("Encode mutated frame: before=%+v after=%+v", before, tc.frame)
			}
		})
	}
}

func TestFrameEncodeParsedHopOverride(t *testing.T) {
	frame, err := ParseFrame("PC00^DATA^H7^^H3^")
	if err != nil {
		t.Fatal(err)
	}
	before := snapshotProtocolFrame(frame)
	if got := frame.Encode(2); got != "PC00^DATA^H7^^H2^" {
		t.Fatalf("hop override changed payload: %q", got)
	}
	if !reflect.DeepEqual(*frame, before) {
		t.Fatalf("hop override mutated source: before=%+v after=%+v", before, frame)
	}
}

func TestFrameEncodeSizeRefusalBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name              string
		inputBytes        int
		encodedBytes      int
		inputOK, outputOK bool
	}{
		{"exact_output_limit", MaxPeerFrameBytes - 4, MaxPeerFrameBytes, true, true},
		{"one_over_output_limit", MaxPeerFrameBytes - 3, MaxPeerFrameBytes + 1, true, false},
		{"largest_input", MaxPeerFrameBytes, MaxPeerFrameBytes + 4, true, false},
		{"overlimit_input", MaxPeerFrameBytes + 1, 0, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := "PC00^" + strings.Repeat("A", tc.inputBytes-5)
			frame, err := ParseFrame(input)
			if !tc.inputOK {
				if err == nil {
					t.Fatal("accepted overlimit input")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			before := snapshotProtocolFrame(frame)
			encoded := frame.Encode(frame.Hop)
			// Even an oversized sentence is complete. Size refusal belongs to
			// the parser and output queue, not an empty-string Encode sentinel.
			if encoded != input+"^H0^" || len(encoded) != tc.encodedBytes {
				t.Fatalf("encoded length=%d expected=%d or payload changed", len(encoded), tc.encodedBytes)
			}
			if !reflect.DeepEqual(*frame, before) {
				t.Fatal("Encode mutated source")
			}
			reparsed, parseErr := ParseFrame(encoded)
			if (parseErr == nil) != tc.outputOK {
				t.Fatalf("output parse error=%v expected acceptance=%v", parseErr, tc.outputOK)
			}
			if tc.outputOK && (reparsed.Type != frame.Type || reparsed.Hop != frame.Hop || !reflect.DeepEqual(reparsed.Fields, frame.Fields)) {
				t.Fatal("exact-limit parser changed payload")
			}
			assertEncodedDataQueueLimit(t, encoded, tc.outputOK)
		})
	}
}

func assertEncodedDataQueueLimit(t *testing.T, encoded string, accepted bool) {
	t.Helper()
	s, _ := newTransportTestSession(t)
	// Initialize with a real admission, leaving live context, spare record
	// slots and byte headroom. A refusal cannot be caused by an unset or full
	// queue, and its contents and accounting must remain unchanged.
	if err := s.sendLine("existing"); err != nil {
		t.Fatal(err)
	}
	s.queueMu.Lock()
	beforeCount, beforeBytes := len(s.writeCh), s.dataBytes
	if s.dataFixedBytes == 0 || cap(s.writeCh)-beforeCount < 1 || peerQueueBytes-beforeBytes < queuedLineBytes(encoded) {
		s.queueMu.Unlock()
		t.Fatal("output fixture lacks initialized capacity or byte headroom")
	}
	s.queueMu.Unlock()
	err := s.sendLine(encoded)
	if accepted && err != nil || !accepted && !errors.Is(err, errSessionWriteQueueFull) {
		t.Fatalf("output admission error=%v expected acceptance=%v", err, accepted)
	}
	if s.ctx.Err() != nil {
		t.Fatalf("data size refusal closed session: %v", s.ctx.Err())
	}
	s.queueMu.Lock()
	wantCount, wantBytes := beforeCount, beforeBytes
	if accepted {
		wantCount++
		wantBytes += queuedLineBytes(encoded)
	}
	count, bytes := len(s.writeCh), s.dataBytes
	s.queueMu.Unlock()
	if count != wantCount || bytes != wantBytes {
		t.Fatalf("queue count=%d/%d bytes=%d/%d", count, wantCount, bytes, wantBytes)
	}
	// Observe retained order without starting the writer or changing accounting.
	// No producer or consumer runs in this fixture; restore each observed item.
	if got := <-s.writeCh; got != "existing" {
		t.Fatalf("retained queue entry=%q", got)
	}
	if accepted {
		if got := <-s.writeCh; got != encoded {
			t.Fatal("queue changed accepted encoded sentence")
		}
	}
	s.writeCh <- "existing"
	if accepted {
		s.writeCh <- encoded
	}
}

func snapshotProtocolFrame(frame *Frame) Frame {
	copyFrame := *frame
	if frame.Fields != nil {
		copyFrame.Fields = make([]string, len(frame.Fields))
		copy(copyFrame.Fields, frame.Fields)
	}
	return copyFrame
}
