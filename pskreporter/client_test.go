package pskreporter

import "testing"

func TestDecorateSpotterCall(t *testing.T) {
	with := &Client{appendSSID: true}

	if got := with.decorateSpotterCall("K1ABC"); got != "K1ABC-#" {
		t.Fatalf("expected -# suffix for bare call, got %s", got)
	}
	if got := with.decorateSpotterCall("K1ABC-1"); got != "K1ABC-1" {
		t.Fatalf("expected existing SSID to remain untouched, got %s", got)
	}
	// 10-character calls shouldn't be expanded when it would exceed the validation limit.
	longCall := "AB2CDEFGHI" // length 10
	if got := with.decorateSpotterCall(longCall); got != longCall {
		t.Fatalf("expected long call to remain unchanged, got %s", got)
	}

	without := &Client{appendSSID: false}
	if got := without.decorateSpotterCall("K1ABC"); got != "K1ABC" {
		t.Fatalf("expected disabled flag to leave call unchanged, got %s", got)
	}
}
