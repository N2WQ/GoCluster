package voacap

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFT8EquivalentSNRDB(t *testing.T) {
	tests := []struct {
		name   string
		dbHz   int
		wantDB int
	}{
		{name: "ten", dbHz: 10, wantDB: -24},
		{name: "negative thirty four", dbHz: -34, wantDB: -68},
		{name: "sixteen", dbHz: 16, wantDB: -18},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FT8EquivalentSNRDB(tt.dbHz); got != tt.wantDB {
				t.Fatalf("FT8EquivalentSNRDB(%d) = %d, want %d", tt.dbHz, got, tt.wantDB)
			}
		})
	}
}

func TestParseMethod30Predictions(t *testing.T) {
	body := readTestdata(t, "voacap_output_method30.txt")
	records, err := ParseMethod30Predictions(body)
	if err != nil {
		t.Fatalf("ParseMethod30Predictions() error: %v", err)
	}
	if len(records) != 6 {
		t.Fatalf("len(records) = %d, want 6", len(records))
	}

	assertPrediction(t, records[0], PredictionRecord{
		HourUTC:        1,
		FrequencyMHz:   3.6,
		VOACAPSNRDBHz:  -8,
		FT8SNRDB:       -42,
		Reliability:    0.02,
		HasReliability: true,
	})
	assertPrediction(t, records[1], PredictionRecord{
		HourUTC:        1,
		FrequencyMHz:   7.0,
		VOACAPSNRDBHz:  20,
		FT8SNRDB:       -14,
		Reliability:    0.72,
		HasReliability: true,
	})
	assertPrediction(t, records[3], PredictionRecord{
		HourUTC:        2,
		FrequencyMHz:   3.6,
		VOACAPSNRDBHz:  -11,
		FT8SNRDB:       -45,
		Reliability:    0.01,
		HasReliability: true,
	})
}

func TestParseMethod30PredictionsAllowsMissingReliability(t *testing.T) {
	body := []byte(`
      1.0 17.2  3.6  7.0  0.0 FREQ
            16   -8   20    -  SNR
`)
	records, err := ParseMethod30Predictions(body)
	if err != nil {
		t.Fatalf("ParseMethod30Predictions() error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("len(records) = %d, want 2", len(records))
	}
	if records[0].HasReliability {
		t.Fatalf("records[0].HasReliability = true, want false")
	}
}

func TestParseMethod30PredictionsRequiresSNR(t *testing.T) {
	_, err := ParseMethod30Predictions([]byte(`
      1.0 17.2  3.6  7.0  0.0 FREQ
`))
	if err == nil || !strings.Contains(err.Error(), "no matching SNR row") {
		t.Fatalf("ParseMethod30Predictions() error = %v, want missing SNR", err)
	}
}

func TestParseMethod30PredictionsRejectsMismatchedRows(t *testing.T) {
	_, err := ParseMethod30Predictions([]byte(`
      1.0 17.2  3.6  7.0 10.1  0.0 FREQ
            16   -8   20    -    -  SNR
`))
	if err == nil || !strings.Contains(err.Error(), "want at least 4") {
		t.Fatalf("ParseMethod30Predictions() error = %v, want mismatched SNR cells", err)
	}
}

func FuzzParseMethod30Predictions(f *testing.F) {
	f.Add(string(readTestdata(f, "voacap_output_method30.txt")))
	f.Add("")
	f.Add("1.0 17.2 3.6 7.0 0.0 FREQ\n16 -8 20 SNR\n")
	f.Fuzz(func(t *testing.T, body string) {
		_, _ = ParseMethod30Predictions([]byte(body))
	})
}

func assertPrediction(t *testing.T, got PredictionRecord, want PredictionRecord) {
	t.Helper()
	if got.HourUTC != want.HourUTC ||
		got.FrequencyMHz != want.FrequencyMHz ||
		got.VOACAPSNRDBHz != want.VOACAPSNRDBHz ||
		got.FT8SNRDB != want.FT8SNRDB ||
		got.HasReliability != want.HasReliability ||
		got.Reliability != want.Reliability {
		t.Fatalf("prediction = %#v, want %#v", got, want)
	}
}

type testdataReader interface {
	Helper()
	Fatalf(format string, args ...any)
}

func readTestdata(t testdataReader, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read testdata %s: %v", name, err)
	}
	return body
}
