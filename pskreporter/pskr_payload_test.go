package pskreporter

import (
	"fmt"
	"testing"
	"time"
)

func TestParsePSKRPayloadFastReorderedSkipsUnusedFields(t *testing.T) {
	payload := []byte(`{
		"ra":291,
		"rl":"em10",
		"sc":"k1abc",
		"sq":99,
		"md":"ft8",
		"x":{"nested":[true,null,{"a":"b"}]},
		"t":1700000000,
		"rp":-12,
		"sl":"fn42",
		"f":14074000,
		"b":"20m",
		"rc":"n0call"
	}`)

	got, ok := parsePSKRPayloadFast(payload)
	if !ok {
		t.Fatal("expected fast parser to accept payload")
	}
	if got.frequency != 14074000 || got.timestamp != 1700000000 || got.report != -12 || !got.hasReport {
		t.Fatalf("unexpected numeric fields: %+v", got)
	}
	if mode, ok := got.modeInfo(); !ok || mode.canonical != "FT8" || mode.variant != "FT8" {
		t.Fatalf("unexpected mode info: %+v ok=%v", mode, ok)
	}
	if got.senderCallString() != "k1abc" || got.receiverCallString() != "n0call" {
		t.Fatalf("unexpected calls sender=%q receiver=%q", got.senderCallString(), got.receiverCallString())
	}
	if got.senderGridString() != "FN42" || got.receiverGridString() != "EM10" {
		t.Fatalf("unexpected grids sender=%q receiver=%q", got.senderGridString(), got.receiverGridString())
	}
}

func TestParsePSKRPayloadFastDuplicateFieldsLastWins(t *testing.T) {
	payload := []byte(`{"f":14074000,"md":"PACKET","md":"PSK31","rp":0,"rp":5,"t":1700000000,"sc":"K1ABC","rc":"N0CALL"}`)

	got, ok := parsePSKRPayloadFast(payload)
	if !ok {
		t.Fatal("expected fast parser to accept payload")
	}
	if got.report != 5 || !got.hasReport {
		t.Fatalf("expected duplicate report to use last value, got report=%d has=%v", got.report, got.hasReport)
	}
	mode, ok := got.modeInfo()
	if !ok || mode.canonical != "PSK" || mode.variant != "PSK31" {
		t.Fatalf("expected duplicate mode to use last value, got %+v ok=%v", mode, ok)
	}
}

func TestParsePSKRPayloadFastEscapedStringFallsBack(t *testing.T) {
	payload := []byte(`{"f":14074000,"md":"FT8","rp":5,"t":1700000000,"sc":"K1\u0041BC","rc":"N0CALL"}`)
	if _, ok := parsePSKRPayloadFast(payload); ok {
		t.Fatal("expected escaped string to require compatibility fallback")
	}
}

func TestParsePSKRPayloadFastKnownCaseVariantFallsBack(t *testing.T) {
	payload := []byte(`{"rC":0,"00":"","00":"0000","00":"","00":"0000"}`)
	if _, ok := parsePSKRPayloadFast(payload); ok {
		t.Fatal("expected case-variant known key to require compatibility fallback")
	}
}

func TestParsePSKRPayloadFastSkippedKnownFieldWrongTypeFallsBack(t *testing.T) {
	payload := []byte(`{"B":0,"0":0,"0":0,"00":"","0":""}`)
	if _, ok := parsePSKRPayloadFast(payload); ok {
		t.Fatal("expected case-variant skipped field to require compatibility fallback")
	}
	payload = []byte(`{"b":0,"f":14074000,"md":"FT8","rp":5,"t":1700000000,"sc":"K1ABC","rc":"N0CALL"}`)
	if _, ok := parsePSKRPayloadFast(payload); ok {
		t.Fatal("expected wrong-type skipped field to require compatibility fallback")
	}
}

func TestHandlePayloadUsesCompatForEscapedString(t *testing.T) {
	client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 2, 0)
	payload := []byte(fmt.Sprintf(`{"f":14074000,"md":"FT8","rp":5,"t":%d,"sc":"K1\u0041BC","rc":"N0CALL","sl":"FN42","rl":"EM10"}`, time.Now().Add(-time.Minute).Unix()))

	client.handlePayload(payload)

	select {
	case spot := <-client.spotChan:
		if spot.DXCall != "K1ABC" {
			t.Fatalf("expected escaped sender call to decode through fallback, got %q", spot.DXCall)
		}
	default:
		t.Fatalf("expected escaped-string payload to enqueue through compatibility fallback")
	}
}

func FuzzParsePSKRPayloadFastMatchesCompat(f *testing.F) {
	f.Add([]byte(`{"f":14074000,"md":"FT8","rp":5,"t":1700000000,"sc":"K1ABC","sl":"FN42","rc":"N0CALL","rl":"EM10"}`))
	f.Add([]byte(`{"rc":"N0CALL","rl":"EM10","rp":-7,"md":"WSPR","f":14097000,"t":1700000000,"sc":"K1ABC","sl":"FN42","sa":291,"ra":291}`))
	f.Add([]byte(`{"f":14074000,"md":"PSK31","rp":5,"rp":6,"t":1700000000,"sc":"K1ABC","rc":"N0CALL","b":"20m"}`))

	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > 1024 {
			t.Skip()
		}
		fast, ok := parsePSKRPayloadFast(payload)
		if !ok {
			return
		}
		compat, err := parsePSKRPayloadCompat(payload)
		if err != nil {
			t.Fatalf("fast parser accepted payload rejected by compat parser: %v", err)
		}
		assertPSKRPayloadsEqual(t, fast, compat)
	})
}

func assertPSKRPayloadsEqual(t *testing.T, fast, compat pskrPayload) {
	t.Helper()
	if fast.frequency != compat.frequency || fast.timestamp != compat.timestamp || fast.report != compat.report || fast.hasReport != compat.hasReport {
		t.Fatalf("numeric mismatch fast=%+v compat=%+v", fast, compat)
	}
	if fast.modeString() != compat.modeString() {
		t.Fatalf("mode mismatch fast=%q compat=%q", fast.modeString(), compat.modeString())
	}
	if fast.senderCallString() != compat.senderCallString() || fast.receiverCallString() != compat.receiverCallString() {
		t.Fatalf("call mismatch fast sender=%q receiver=%q compat sender=%q receiver=%q", fast.senderCallString(), fast.receiverCallString(), compat.senderCallString(), compat.receiverCallString())
	}
	if fast.senderGridString() != compat.senderGridString() || fast.receiverGridString() != compat.receiverGridString() {
		t.Fatalf("grid mismatch fast sender=%q receiver=%q compat sender=%q receiver=%q", fast.senderGridString(), fast.receiverGridString(), compat.senderGridString(), compat.receiverGridString())
	}
}

func BenchmarkParsePSKRPayloadFastFT8(b *testing.B) {
	payload := []byte(`{"f":14074000,"md":"FT8","rp":5,"t":1700000000,"sc":"K1ABC","sl":"FN42","rc":"N0CALL","rl":"EM10","sq":1,"sa":291,"ra":291,"b":"20m"}`)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		got, ok := parsePSKRPayloadFast(payload)
		if !ok || got.frequency != 14074000 || got.report != 5 {
			b.Fatalf("unexpected parse result: %+v ok=%v", got, ok)
		}
	}
}

func BenchmarkParsePSKRPayloadCompatFT8(b *testing.B) {
	payload := []byte(`{"f":14074000,"md":"FT8","rp":5,"t":1700000000,"sc":"K1ABC","sl":"FN42","rc":"N0CALL","rl":"EM10","sq":1,"sa":291,"ra":291,"b":"20m"}`)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		got, err := parsePSKRPayloadCompat(payload)
		if err != nil || got.frequency != 14074000 || got.report != 5 {
			b.Fatalf("unexpected parse result: %+v err=%v", got, err)
		}
	}
}
