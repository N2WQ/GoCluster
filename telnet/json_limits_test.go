package telnet

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"dxcluster/spot"
)

func TestJSONSpotCeilingIncludesReadableArchive(t *testing.T) {
	// Archive readers verify field lengths but accept arbitrary bytes in the
	// comment, mode and band. Encode independently with the standard library.
	worst := string(bytes.Repeat([]byte{1}, math.MaxUint16))
	record := []any{"h", nil, "-9223372036854775808", math.MaxFloat64, math.SmallestNonzeroFloat64, worst, worst, nil, nil, worst}
	wire, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	limits := newJSONSpotLimits(128, 0, spot.CurrentTaxonomy())
	if jsonTestSpotBound(t, limits).Cmp(big.NewInt(int64(len(wire)+1))) < 0 {
		t.Fatalf("archive prefix needs %d bytes, advertised %s", len(wire)+1, limits.spotBytes)
	}
}

func TestJSONSpotCeilingFollowsEnabledFeedAndManualLimit(t *testing.T) {
	taxonomy := spot.CurrentTaxonomy()
	noHTTP := newJSONSpotLimits(128, 0, taxonomy)
	shipped := newJSONSpotLimits(128, 1<<20, taxonomy)
	if jsonTestSpotBound(t, shipped).Cmp(jsonTestSpotBound(t, noHTTP)) <= 0 {
		t.Fatalf("shipped DXSummit limit did not change ceiling: %s and %s", noHTTP.spotBytes, shipped.spotBytes)
	}
	// The decoder repairs raw invalid bytes to three-byte U+FFFD before the
	// human parser retains the comment. Size a complete line against that
	// actual decoded representation, rather than assuming len(raw) is enough.
	input := append([]byte{'"'}, bytes.Repeat([]byte{0xff}, 1<<20-2)...)
	input = append(input, '"')
	var comment string
	if err := json.Unmarshal(input, &comment); err != nil {
		t.Fatal(err)
	}
	actual, err := jsontext.AppendQuote([]byte(`["s",`), comment)
	if err != nil {
		t.Fatal(err)
	}
	actual = append(actual, ']', '\n')
	if len(actual) <= len(input) || jsonTestSpotBound(t, shipped).Cmp(big.NewInt(int64(len(actual)))) < 0 {
		t.Fatalf("raw=%d, quoted=%d, advertised=%s", len(input), len(actual), shipped.spotBytes)
	}
	largeManual := newJSONSpotLimits(12<<20, 0, taxonomy)
	if jsonTestSpotBound(t, largeManual).Cmp(big.NewInt(12<<20)) < 0 {
		t.Fatalf("a long manual DX comment exceeds advertised %s", largeManual.spotBytes)
	}
}

func TestJSONSpotCeilingUsesConfiguredModesAndEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "taxonomy.yaml")
	data := []byte("modes:\n  - name: CUSTOM\n    variants: [LONG_CUSTOM_VARIANT]\n    comment_tokens: [X]\nevents:\n  - name: LONG_EVENT_NAME\n    standalone_tokens: [Y]\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	taxonomy, err := spot.LoadTaxonomyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := taxonomy.MaxModeLabelBytes(); got != len("LONG_CUSTOM_VARIANT") {
		t.Fatalf("non-filterable mode variant: got %d bytes", got)
	}
	if names := taxonomy.EventNames(^spot.EventMask(0)); len(names) != 1 || names[0] != "LONG_EVENT_NAME" {
		t.Fatalf("event labels=%v", names)
	}
	withLabels := newJSONSpotLimits(128, 1<<20, taxonomy)
	if err := os.WriteFile(path, []byte("modes: [{name: X}]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	short, err := spot.LoadTaxonomyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	withoutLabels := newJSONSpotLimits(128, 1<<20, short)
	delta := new(big.Int).Sub(jsonTestSpotBound(t, withLabels), jsonTestSpotBound(t, withoutLabels))
	if delta.Cmp(big.NewInt(int64(len("LONG_EVENT_NAME")+len("LONG_CUSTOM_VARIANT")-1))) < 0 {
		t.Fatalf("bounds differ by %s bytes", delta)
	}
}

func TestJSONSpotCeilingExtremeLimitIsExactOnTheWire(t *testing.T) {
	limits := newJSONSpotLimits(math.MaxInt, math.MaxInt64, spot.CurrentTaxonomy())
	bound := jsonTestSpotBound(t, limits)
	if bound.Cmp(new(big.Int).Mul(big.NewInt(6), big.NewInt(math.MaxInt64))) < 0 {
		t.Fatalf("escaped source ceiling wrapped: %s", bound)
	}
	f, accuracy := new(big.Float).SetInt(bound).Float64()
	if accuracy != big.Exact || math.IsInf(f, 0) {
		t.Fatalf("advertised integer cannot be represented by a browser number: %s", bound)
	}
}

func jsonTestSpotBound(t *testing.T, l jsonSpotLimits) *big.Int {
	t.Helper()
	var fields struct {
		SpotBytes      json.Number
		NodeBytes      int
		DXCountryBytes int
		DECountryBytes int
	}
	if err := json.Unmarshal(l.appendHelloLimits(nil), &fields); err != nil {
		t.Fatal(err)
	}
	if fields.NodeBytes != 256 || fields.DXCountryBytes != 256 || fields.DECountryBytes != 256 {
		t.Fatalf("wrong optional field ceilings: %+v", fields)
	}
	bound, ok := new(big.Int).SetString(fields.SpotBytes.String(), 10)
	if !ok || bound.Sign() <= 0 {
		t.Fatalf("invalid advertised spot ceiling %q", fields.SpotBytes)
	}
	return bound
}
