package pskreporter

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"dxcluster/spot"
)

type reportPresenceRoute uint8

const (
	reportPresenceIgnored reportPresenceRoute = iota
	reportPresenceNormal
	reportPresencePathOnly
)

// These literal routes are an oracle for the shipped taxonomy. Deriving them
// through the production route helpers would conceal a broken destination.
var reportPresenceModes = []struct {
	name, input, display, canonical string
	frequency                       int64
	route                           reportPresenceRoute
}{
	{"FT8", "FT8", "FT8", "FT8", 14074000, reportPresenceNormal},
	{"WSPR", "WSPR", "WSPR", "WSPR", 14097000, reportPresencePathOnly},
	{"PACKET", "PACKET", "PACKET", "PACKET", 14074000, reportPresenceIgnored},
	{"PSK31", "psk31", "PSK31", "PSK", 14070000, reportPresenceNormal},
}

// Escaped values force successful compatibility decoding. Escaped keys retain
// the existing ConfigFastest boundary: raw key bytes are ignored without an
// error, so parsePSKRPayloadCompat does not retry its second decoder.
var reportPresenceCodecs = []string{"fast", "compat", "escaped_key", "case_key"}

type reportPresenceState struct {
	name, fields string
	present      bool
	report       int
	fast         bool
	truncated    bool
}

var reportPresenceStates = []reportPresenceState{
	{"zero", `"rp":0`, true, 0, true, false},
	{"negative_zero", `"rp":-0`, true, 0, true, false},
	{"positive", `"rp":5`, true, 5, true, false},
	{"negative", `"rp":-7`, true, -7, true, false},
	{"omitted", "", false, 0, true, false},
	{"null", `"rp":null`, false, 0, false, false},
	{"wrong_string", `"rp":"0"`, false, 0, false, false},
	{"wrong_bool", `"rp":true`, false, 0, false, false},
	{"wrong_object", `"rp":{}`, false, 0, false, false},
	{"truncated", `"rp":0`, false, 0, false, true},
	{"duplicate_final_zero", `"rp":-7,"rp":0`, true, 0, true, false},
	{"duplicate_final_null", `"rp":0,"rp":null`, false, 0, false, false},
	{"duplicate_null_then_zero", `"rp":null,"rp":0`, true, 0, false, false},
}

func reportPresencePayload(mode string, frequency, observed int64, codec string, state reportPresenceState) []byte {
	senderKey, sender := `"sc"`, `"K1ABC"`
	fields := state.fields
	switch codec {
	case "compat":
		sender = `"K1\u0041BC"`
	case "escaped_key":
		senderKey = `"s\u0063"`
		fields = strings.ReplaceAll(fields, `"rp"`, `"r\u0070"`)
	case "case_key":
		senderKey = `"Sc"`
		fields = strings.ReplaceAll(fields, `"rp"`, `"Rp"`)
	}
	if fields != "" {
		fields += ","
	}
	payload := []byte(fmt.Sprintf(`{"f":%d,"md":%q,%s"t":%d,%s:%s,"sl":"fn42","rc":"N0CALL","rl":"em10"}`,
		frequency, mode, fields, observed, senderKey, sender))
	if state.truncated {
		payload = payload[:len(payload)-1]
	}
	return payload
}

func TestHandlePayloadReportPresenceRoutes(t *testing.T) {
	observed := time.Now().Add(-time.Minute).Unix()
	for _, mode := range reportPresenceModes {
		for _, codec := range reportPresenceCodecs {
			for _, state := range reportPresenceStates {
				t.Run(mode.name+"/"+codec+"/"+state.name, func(t *testing.T) {
					payload := reportPresencePayload(mode.input, mode.frequency, observed, codec, state)
					_, fast := parsePSKRPayloadFast(payload)
					if wantFast := codec == "fast" && state.fast; fast != wantFast {
						t.Fatalf("parser path fast=%t, want %t; payload=%s", fast, wantFast, payload)
					}
					client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 2, 0)
					client.handlePayload(payload)
					route := mode.route
					if !state.present || codec == "escaped_key" {
						route = reportPresenceIgnored
					}
					assertReportPresenceOutput(t, client, route, mode.display, mode.canonical, mode.frequency, observed, state.report)
				})
			}
		}
	}
}

func TestHandlePayloadEscapedKnownKeyCompatibilityBoundary(t *testing.T) {
	observed := time.Now().Add(-time.Minute).Unix()
	for _, test := range []struct {
		name, reportKey, senderKey string
		wantReport                 bool
		wantSender                 string
	}{
		{"report_key_nonzero", `"r\u0070"`, `"sc"`, false, "K1ABC"},
		{"sender_key_nonzero", `"rp"`, `"s\u0063"`, true, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			// A nonzero report isolates the pre-existing key-decoding boundary
			// from the removed zero-value admission check. Escape only one key.
			payload := []byte(fmt.Sprintf(`{"f":14074000,"md":"FT8",%s:5,"t":%d,%s:"K1ABC","sl":"FN42","rc":"N0CALL","rl":"EM10"}`,
				test.reportKey, observed, test.senderKey))
			if _, fast := parsePSKRPayloadFast(payload); fast {
				t.Fatal("escaped known key unexpectedly used the fast parser")
			}
			decoded, err := parsePSKRPayloadCompat(payload)
			if err != nil || decoded.hasReport != test.wantReport || decoded.senderCallString() != test.wantSender ||
				decoded.frequency != 14074000 || decoded.timestamp != observed || decoded.receiverCallString() != "N0CALL" ||
				(test.wantReport && decoded.report != 5) {
				t.Fatalf("existing raw-key compatibility boundary changed: decoded=%+v err=%v", decoded, err)
			}
			client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 2, 0)
			client.handlePayload(payload)
			assertReportPresenceOutput(t, client, reportPresenceIgnored, "FT8", "FT8", 14074000, observed, 5)
		})
	}
}

// handlePayload is synchronous here: no Connect, workers or sockets are started.
// Draining one expected tuple then checking both bounded queues proves exact
// cardinality as well as exclusion from the opposite route.
func assertReportPresenceOutput(t *testing.T, client *Client, route reportPresenceRoute, display, canonical string, frequency, observed int64, report int) {
	t.Helper()
	var output <-chan *spot.Spot
	switch route {
	case reportPresenceNormal:
		output = client.spotChan
	case reportPresencePathOnly:
		output = client.pathOnlyChan
	}
	if route != reportPresenceIgnored && output == nil {
		t.Fatalf("intended route %d has no output channel", route)
	}
	if output != nil {
		var got *spot.Spot
		select {
		case got = <-output:
		default:
			t.Fatalf("missing intended route %d tuple", route)
		}
		if got == nil || got.DXCall != "K1ABC" || got.DECall != "N0CALL" ||
			got.Mode != display || got.ModeNorm != canonical || !got.HasReport || got.Report != report ||
			got.Frequency != float64(frequency)/1000 || got.ObservedFrequency != 0 ||
			!got.Time.Equal(time.Unix(observed, 0)) || got.DXMetadata.Grid != "FN42" || got.DEMetadata.Grid != "EM10" ||
			got.SourceType != spot.SourcePSKReporter || got.SourceNode != "PSKREPORTER" || got.IsHuman ||
			got.ModeProvenance != spot.ModeProvenanceSourceExplicit || got.Comment != "" {
			t.Fatalf("report tuple changed: %+v; mode=%s/%s frequency=%d observed=%d report=%d", got, display, canonical, frequency, observed, report)
		}
	}
	if len(client.spotChan) != 0 || len(client.pathOnlyChan) != 0 {
		t.Fatalf("extra or wrongly routed output: normal=%d path-only=%d want route=%d", len(client.spotChan), len(client.pathOnlyChan), route)
	}
	if client.spotDropCounter.Total() != 0 || client.pathOnlyDropCounter.Total() != 0 {
		t.Fatal("bounded output queues dropped a report")
	}
}

// Structured scalar inputs keep every fuzz invocation bounded and meaningful.
// Report presence and destinations come from literal test states, independently
// of either parser or the production taxonomy helpers.
func FuzzPSKRReportPresenceRoutes(f *testing.F) {
	for mode := range reportPresenceModes {
		for codec := range reportPresenceCodecs {
			for _, state := range []uint8{0, 1, 4, 5, 10, 11, 12} {
				f.Add(state, int16(0), uint8(mode), uint8(codec))
			}
		}
	}
	f.Add(uint8(0), int16(-17), uint8(0), uint8(0))
	f.Add(uint8(0), int16(9), uint8(1), uint8(1))
	f.Fuzz(func(t *testing.T, stateIndex uint8, value int16, modeIndex, codecIndex uint8) {
		state := reportPresenceStates[int(stateIndex)%len(reportPresenceStates)]
		if stateIndex%uint8(len(reportPresenceStates)) == 0 {
			state.fields = fmt.Sprintf(`"rp":%d`, value)
			state.report = int(value)
		}
		mode := reportPresenceModes[int(modeIndex)%len(reportPresenceModes)]
		codec := reportPresenceCodecs[int(codecIndex)%len(reportPresenceCodecs)]
		observed := time.Now().Add(-time.Minute).Unix()
		payload := reportPresencePayload(mode.input, mode.frequency, observed, codec, state)
		if len(payload) > 512 {
			t.Fatal("structured payload exceeded its fixed resource bound")
		}
		_, fast := parsePSKRPayloadFast(payload)
		if wantFast := codec == "fast" && state.fast; fast != wantFast {
			t.Fatalf("parser path fast=%t, want %t; payload=%s", fast, wantFast, payload)
		}
		client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 2, 0)
		client.handlePayload(payload)
		route := mode.route
		if !state.present || codec == "escaped_key" {
			route = reportPresenceIgnored
		}
		assertReportPresenceOutput(t, client, route, mode.display, mode.canonical, mode.frequency, observed, state.report)
	})
}
