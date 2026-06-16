package pskreporter

import (
	"encoding/json"
	"runtime"
	"strings"
	"testing"
	"time"

	"dxcluster/spot"
)

func intPtr(v int) *int {
	return &v
}

func TestDecorateSpotterCall(t *testing.T) {
	with := &Client{appendSSID: true}

	if got := with.decorateSpotterCall("K1ABC"); got != "K1ABC-#" {
		t.Fatalf("expected -# suffix for bare call, got %s", got)
	}
	if got := with.decorateSpotterCall("K1ABC-1"); got != "K1ABC-1" {
		t.Fatalf("expected existing SSID to remain untouched, got %s", got)
	}
	// 10-character calls can be expanded when still within the validation limit.
	longCall := "AB2CDEFGHI" // length 10
	if got := with.decorateSpotterCall(longCall); got != longCall+"-#" {
		t.Fatalf("expected long call to include SSID suffix, got %s", got)
	}

	without := &Client{appendSSID: false}
	if got := without.decorateSpotterCall("K1ABC"); got != "K1ABC" {
		t.Fatalf("expected disabled flag to leave call unchanged, got %s", got)
	}
}

func TestConvertToSpotOmitsCommentAndCarriesGrids(t *testing.T) {
	client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 16, 0)

	msg := &PSKRMessage{
		SequenceNumber:  1,
		Frequency:       14074000,
		Mode:            "FT8",
		Report:          intPtr(10),
		Timestamp:       time.Now().Add(-time.Minute).Unix(),
		SenderCall:      "K1ABC",
		SenderLocator:   "fn42",
		ReceiverCall:    "N0CALL",
		ReceiverLocator: "em10",
	}

	modeInfo, ok := parseModeInfo(msg.Mode)
	if !ok {
		t.Fatalf("expected valid mode info for %q", msg.Mode)
	}
	spot := client.convertToSpot(msg, modeInfo)
	if spot == nil {
		t.Fatalf("expected spot, got nil")
	}
	if trimmed := strings.TrimSpace(spot.Comment); trimmed != "" {
		t.Fatalf("expected empty comment, got %q", trimmed)
	}
	if spot.DXMetadata.Grid != "FN42" {
		t.Fatalf("expected DX grid FN42, got %q", spot.DXMetadata.Grid)
	}
	if spot.DEMetadata.Grid != "EM10" {
		t.Fatalf("expected DE grid EM10, got %q", spot.DEMetadata.Grid)
	}
}

func TestConvertToSpotCanonicalizesSenderNumericSSID(t *testing.T) {
	client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 16, 0)
	msg := &PSKRMessage{
		SequenceNumber:  1,
		Frequency:       14074000,
		Mode:            "FT8",
		Report:          intPtr(10),
		Timestamp:       time.Now().Add(-time.Minute).Unix(),
		SenderCall:      "K1ABC-2",
		SenderLocator:   "fn42",
		ReceiverCall:    "N0CALL",
		ReceiverLocator: "em10",
	}

	modeInfo, ok := parseModeInfo(msg.Mode)
	if !ok {
		t.Fatalf("expected valid mode info for %q", msg.Mode)
	}
	spotEntry := client.convertToSpot(msg, modeInfo)
	if spotEntry == nil {
		t.Fatalf("expected spot, got nil")
	}
	if spotEntry.DXCall != "K1ABC" || spotEntry.DXCallNorm != "K1ABC" {
		t.Fatalf("expected canonical DX K1ABC, got DXCall=%q DXCallNorm=%q", spotEntry.DXCall, spotEntry.DXCallNorm)
	}
}

func TestConvertToSpotCanonicalizesFTFrequencyAndPreservesObserved(t *testing.T) {
	registry := spot.NewFTDialRegistry([]spot.ModeSeed{
		{FrequencyKHz: 14074, Mode: "FT8"},
	})
	client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, registry, false, 16, 0)

	msg := &PSKRMessage{
		SequenceNumber:  1,
		Frequency:       14076110,
		Mode:            "FT8",
		Report:          intPtr(10),
		Timestamp:       time.Now().Add(-time.Minute).Unix(),
		SenderCall:      "K1ABC",
		ReceiverCall:    "N0CALL",
		SenderLocator:   "fn42",
		ReceiverLocator: "em10",
	}

	modeInfo, ok := parseModeInfo(msg.Mode)
	if !ok {
		t.Fatalf("expected valid mode info for %q", msg.Mode)
	}
	got := client.convertToSpot(msg, modeInfo)
	if got == nil {
		t.Fatal("expected spot")
	}
	if got.Frequency != 14074.0 {
		t.Fatalf("expected canonical FT8 dial 14074.0, got %.2f", got.Frequency)
	}
	if got.ObservedFrequency != 14076.11 {
		t.Fatalf("expected observed FT8 frequency 14076.11, got %.2f", got.ObservedFrequency)
	}
}

func TestConvertToSpotUsesObservationTimestampAndMachineSource(t *testing.T) {
	client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 16, 0)
	observed := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	msg := &PSKRMessage{
		SequenceNumber:  1,
		Frequency:       14074000,
		Mode:            "FT8",
		Report:          intPtr(10),
		Timestamp:       observed.Unix(),
		SenderCall:      "K1ABC",
		SenderLocator:   "FN42",
		ReceiverCall:    "N0CALL",
		ReceiverLocator: "EM10",
	}

	modeInfo, ok := parseModeInfo(msg.Mode)
	if !ok {
		t.Fatalf("expected valid mode info for %q", msg.Mode)
	}
	got := client.convertToSpot(msg, modeInfo)
	if got == nil {
		t.Fatal("expected spot")
	}
	if !got.Time.Equal(observed) || got.Time.Location() != time.UTC {
		t.Fatalf("expected observation timestamp in UTC, got %s (%s)", got.Time, got.Time.Location())
	}
	if got.SourceType != spot.SourcePSKReporter || got.SourceNode != "PSKREPORTER" || got.IsHuman {
		t.Fatalf("unexpected source fields: source=%q node=%q isHuman=%v", got.SourceType, got.SourceNode, got.IsHuman)
	}
	if got.ModeProvenance != spot.ModeProvenanceSourceExplicit {
		t.Fatalf("expected source-explicit mode provenance, got %q", got.ModeProvenance)
	}
}

func TestConvertToSpotFTFrequencyFailsOpenWithoutMatchingDial(t *testing.T) {
	registry := spot.NewFTDialRegistry([]spot.ModeSeed{
		{FrequencyKHz: 14074, Mode: "FT8"},
	})
	client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, registry, false, 16, 0)

	msg := &PSKRMessage{
		SequenceNumber: 1,
		Frequency:      14079000,
		Mode:           "FT8",
		Report:         intPtr(10),
		Timestamp:      time.Now().Add(-time.Minute).Unix(),
		SenderCall:     "K1ABC",
		ReceiverCall:   "N0CALL",
	}

	modeInfo, ok := parseModeInfo(msg.Mode)
	if !ok {
		t.Fatalf("expected valid mode info for %q", msg.Mode)
	}
	got := client.convertToSpot(msg, modeInfo)
	if got == nil {
		t.Fatal("expected spot")
	}
	if got.Frequency != 14079.0 {
		t.Fatalf("expected unmapped FT8 frequency to remain raw, got %.2f", got.Frequency)
	}
	if got.ObservedFrequency != 0 {
		t.Fatalf("expected no separate observed frequency when canonicalization fails open, got %.2f", got.ObservedFrequency)
	}
}

func TestConvertToSpotReportsInvalidSenderCall(t *testing.T) {
	client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 16, 0)
	var got struct {
		source string
		role   string
		reason string
		call   string
		de     string
		dx     string
		mode   string
		detail string
	}
	client.SetBadCallReporter(func(source, role, reason, call, deCall, dxCall, mode, detail string) {
		got.source = source
		got.role = role
		got.reason = reason
		got.call = call
		got.de = deCall
		got.dx = dxCall
		got.mode = mode
		got.detail = detail
	})

	modeInfo, ok := parseModeInfo("FT8")
	if !ok {
		t.Fatal("expected FT8 mode info")
	}
	msg := &PSKRMessage{
		Frequency:    14074000,
		Mode:         "FT8",
		Timestamp:    time.Now().Add(-time.Minute).Unix(),
		SenderCall:   "BAD!",
		ReceiverCall: "N0CALL",
	}
	if spot := client.convertToSpot(msg, modeInfo); spot != nil {
		t.Fatalf("expected invalid sender to drop, got %+v", spot)
	}
	if got.source != "PSKREPORTER" || got.role != "DX" || got.reason != "invalid_callsign" || got.call != "BAD!" || got.mode != "FT8" || got.detail != "source_parser" {
		t.Fatalf("unexpected bad-call report: %+v", got)
	}
}

type testMessage struct {
	payload []byte
}

func (m testMessage) Duplicate() bool { return false }
func (m testMessage) Qos() byte       { return 0 }
func (m testMessage) Retained() bool  { return false }
func (m testMessage) Topic() string   { return "" }
func (m testMessage) MessageID() uint16 {
	return 0
}
func (m testMessage) Payload() []byte { return m.payload }
func (m testMessage) Ack()            {}

func TestMessageHandlerDropsOversizePayload(t *testing.T) {
	client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 16, 4)
	client.processingMu.Lock()
	client.processing = make(chan []byte, 1)
	client.processingMu.Unlock()

	client.messageHandler(nil, testMessage{payload: make([]byte, 10)})

	select {
	case <-client.processing:
		t.Fatalf("expected oversized payload to be dropped")
	default:
	}
}

func TestDefaultPSKReporterWorkersFollowsGOMAXPROCS(t *testing.T) {
	old := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(old)

	runtime.GOMAXPROCS(1)
	if got := defaultPSKReporterWorkers(); got != 1 {
		t.Fatalf("GOMAXPROCS=1 default workers = %d, want 1", got)
	}

	runtime.GOMAXPROCS(2)
	if got := defaultPSKReporterWorkers(); got != 2 {
		t.Fatalf("GOMAXPROCS=2 default workers = %d, want 2", got)
	}
}

func TestHandlePayloadFiltersModesByTaxonomyRoute(t *testing.T) {
	client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 2, 0)
	allowed := PSKRMessage{
		Frequency:    14074000,
		Mode:         "FT8",
		Report:       intPtr(5),
		Timestamp:    time.Now().Unix(),
		SenderCall:   "K1ABC",
		ReceiverCall: "N0CALL",
	}
	blocked := allowed
	blocked.Mode = "PACKET"

	allowedPayload, _ := json.Marshal(allowed)
	blockedPayload, _ := json.Marshal(blocked)

	// Ignored taxonomy route should not reach spotChan.
	client.handlePayload(blockedPayload)
	select {
	case <-client.spotChan:
		t.Fatalf("expected blocked mode to be dropped")
	default:
	}

	// Normal taxonomy route should enqueue a spot.
	client.handlePayload(allowedPayload)
	select {
	case <-client.spotChan:
		// ok
	default:
		t.Fatalf("expected allowed mode to enqueue a spot")
	}
}

func TestHandlePayloadDropsZeroSNR(t *testing.T) {
	client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 2, 0)
	msg := PSKRMessage{
		Frequency:    14074000,
		Mode:         "FT8",
		Report:       intPtr(0),
		Timestamp:    time.Now().Unix(),
		SenderCall:   "K1ABC",
		ReceiverCall: "N0CALL",
	}
	payload, _ := json.Marshal(msg)

	client.handlePayload(payload)
	select {
	case spot := <-client.spotChan:
		t.Fatalf("expected zero-SNR spot to be dropped, got %+v", spot)
	default:
	}
}

func TestHandlePayloadDropsMissingReport(t *testing.T) {
	client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 2, 0)
	msg := PSKRMessage{
		Frequency:    14074000,
		Mode:         "FT8",
		Timestamp:    time.Now().Unix(),
		SenderCall:   "K1ABC",
		ReceiverCall: "N0CALL",
	}
	payload, _ := json.Marshal(msg)

	client.handlePayload(payload)
	select {
	case spot := <-client.spotChan:
		t.Fatalf("expected missing-report spot to be dropped, got %+v", spot)
	default:
	}
}

func TestHandlePayloadAllowsPSKVariantsWithCanonicalTaxonomyRoute(t *testing.T) {
	client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 2, 0)
	psk31 := PSKRMessage{
		Frequency:    14074000,
		Mode:         "PSK31",
		Report:       intPtr(5),
		Timestamp:    time.Now().Unix(),
		SenderCall:   "K1ABC",
		ReceiverCall: "N0CALL",
	}
	payload, _ := json.Marshal(psk31)

	client.handlePayload(payload)
	select {
	case spot := <-client.spotChan:
		if spot.Mode != "PSK31" || spot.ModeNorm != "PSK" {
			t.Fatalf("expected PSK31 variant with canonical PSK ModeNorm, got Mode=%q ModeNorm=%q", spot.Mode, spot.ModeNorm)
		}
	default:
		t.Fatalf("expected PSK31 to be accepted when PSK is allowed")
	}
}

func TestHandlePayloadAllowsFT2WhenTaxonomyRouted(t *testing.T) {
	client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 2, 0)
	msg := PSKRMessage{
		Frequency:    14080000,
		Mode:         "FT2",
		Report:       intPtr(5),
		Timestamp:    time.Now().Unix(),
		SenderCall:   "K1ABC",
		ReceiverCall: "N0CALL",
	}
	payload, _ := json.Marshal(msg)

	client.handlePayload(payload)
	select {
	case spot := <-client.spotChan:
		if spot.Mode != "FT2" || spot.ModeNorm != "FT2" {
			t.Fatalf("expected FT2 spot, got Mode=%q ModeNorm=%q", spot.Mode, spot.ModeNorm)
		}
	default:
		t.Fatalf("expected FT2 to be accepted when taxonomy routes FT2")
	}
}

func TestHandlePayloadRoutesPathOnlyMode(t *testing.T) {
	client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 2, 0)
	msg := PSKRMessage{
		Frequency:       14097000,
		Mode:            "WSPR",
		Report:          intPtr(5),
		Timestamp:       time.Now().Unix(),
		SenderCall:      "K1ABC",
		ReceiverCall:    "N0CALL",
		SenderLocator:   "FN42",
		ReceiverLocator: "EM10",
	}
	payload, _ := json.Marshal(msg)

	client.handlePayload(payload)

	select {
	case spot := <-client.GetPathOnlyChannel():
		if spot.Mode != "WSPR" {
			t.Fatalf("expected WSPR path-only spot, got %q", spot.Mode)
		}
	default:
		t.Fatalf("expected WSPR to be routed to path-only channel")
	}

	select {
	case <-client.spotChan:
		t.Fatalf("expected path-only WSPR to bypass spot channel")
	default:
	}
}

func BenchmarkConvertToSpotFTCanonicalized(b *testing.B) {
	registry := spot.NewFTDialRegistry([]spot.ModeSeed{
		{FrequencyKHz: 14074, Mode: "FT8"},
	})
	client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, registry, false, 16, 0)
	msg := &PSKRMessage{
		SequenceNumber:  1,
		Frequency:       14076110,
		Mode:            "FT8",
		Report:          intPtr(10),
		Timestamp:       time.Now().Add(-time.Minute).Unix(),
		SenderCall:      "K1ABC",
		SenderLocator:   "fn42",
		ReceiverCall:    "N0CALL",
		ReceiverLocator: "em10",
	}
	modeInfo, ok := parseModeInfo(msg.Mode)
	if !ok {
		b.Fatalf("expected valid mode info for %q", msg.Mode)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if got := client.convertToSpot(msg, modeInfo); got == nil {
			b.Fatal("expected spot")
		}
	}
}

func BenchmarkHandlePayloadFT8(b *testing.B) {
	client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 2, 0)
	msg := PSKRMessage{
		Frequency:       14074000,
		Mode:            "FT8",
		Report:          intPtr(5),
		Timestamp:       time.Now().Add(-time.Minute).Unix(),
		SenderCall:      "K1ABC",
		SenderLocator:   "FN42",
		ReceiverCall:    "N0CALL",
		ReceiverLocator: "EM10",
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		b.Fatalf("marshal payload: %v", err)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		client.handlePayload(payload)
		select {
		case <-client.spotChan:
		default:
			b.Fatal("expected enqueued spot")
		}
	}
}
