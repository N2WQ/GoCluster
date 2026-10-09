package commands

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/reputation"
	"dxcluster/spot"
)

const invalidDXFrequencyResponse = "Invalid frequency. Use a kHz value like 7001.0.\n"

func invalidManualFrequencies() []string {
	return []string{"0", "0.004", "135.694", "2450000.005", "NaN", "Inf", "+Inf", "-Inf",
		"1" + strings.Repeat("0", 308), strings.Repeat("9", 400)}
}

func TestManualDXCanonicalFrequencyEnvelope(t *testing.T) {
	cases := []struct {
		token string
		want  float64
		band  string
	}{
		{"135.70", 135.7, "2200m"},
		{"135.695", 135.7, "2200m"},
		{strconv.FormatFloat(math.Nextafter(135.7, math.Inf(-1)), 'f', -1, 64), 135.7, "2200m"},
		{"2450000", 2450000, "13cm"},
		{"2450000.004", 2450000, "13cm"},
		{strconv.FormatFloat(math.Nextafter(2450000, math.Inf(1)), 'f', -1, 64), 2450000, "13cm"},
		{"1000", 1000, "???"},
		{"2350000", 2350000, "???"},
		{"13999.999", 14000, "20m"},
	}
	for _, dialect := range []string{"go", "cc"} {
		for _, loggerOrder := range []bool{false, true} {
			for _, tc := range cases {
				command := "DX " + tc.token + " W6ABC CW TEST"
				if loggerOrder {
					command = "DX W6ABC " + tc.token + " CW TEST"
				}
				input := make(chan *spot.Spot, 1)
				processor := NewProcessor(nil, nil, input, nil, nil, nil)
				if response := processor.ProcessCommandForClient(command, "K1ABC-1", "", nil, dialect); response != "Spot queued.\n" || len(input) != 1 {
					t.Fatalf("%s/%s: response=%q queued=%d", dialect, command, response, len(input))
				}
				queued := <-input
				if queued.Frequency != tc.want || queued.Band != tc.band || queued.BandNorm != tc.band {
					t.Fatalf("%s/%s: frequency=%v band=%q/%q, want %v/%q", dialect, command, queued.Frequency, queued.Band, queued.BandNorm, tc.want, tc.band)
				}
			}
			for _, token := range invalidManualFrequencies() {
				command := "DX " + token + " W6ABC CW TEST"
				if loggerOrder {
					command = "DX W6ABC " + token + " CW TEST"
				}
				input := make(chan *spot.Spot, 1)
				processor := NewProcessor(nil, nil, input, nil, nil, nil)
				if response := processor.ProcessCommandForClient(command, "K1ABC-1", "", nil, dialect); response != invalidDXFrequencyResponse || len(input) != 0 {
					t.Fatalf("%s/invalid frequency %q: response=%q queued=%d", dialect, token, response, len(input))
				}
			}
		}
	}
}

func TestManualDXInvalidFrequencyPreservesAdmissionBudget(t *testing.T) {
	// One total allowance also detects spending if the prefix token refills.
	gate, err := reputation.NewGate(config.ReputationConfig{
		Enabled: true, ReputationDir: t.TempDir(), RampWindowSeconds: 3600,
		PerBandStart: 1, PerBandCap: 1, TotalCapStart: 1, TotalCapPostRamp: 1,
		IPv4BucketSize: 1, IPv4BucketRefillPerSec: 1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gate.Close)
	gate.RecordLogin("K1ABC-1", "203.0.113.1", time.Now().UTC())
	input := make(chan *spot.Spot, 1)
	reports := 0
	processor := NewProcessor(nil, nil, input, nil, gate, func(reputation.DropEvent) { reports++ })
	for _, token := range invalidManualFrequencies() {
		if response := processor.ProcessCommandForClient("DX "+token+" W6ABC CW TEST", "K1ABC-2", "203.0.113.1", nil, "go"); response != invalidDXFrequencyResponse || len(input) != 0 || reports != 0 {
			t.Fatalf("invalid frequency consumed effects: response=%q queued=%d reports=%d", response, len(input), reports)
		}
	}
	if response := processor.ProcessCommandForClient("DX 14074 W6ABC CW TEST", "K1ABC-2", "203.0.113.1", nil, "go"); response != "Spot queued.\n" || len(input) != 1 || reports != 0 {
		t.Fatalf("invalid values spent remaining allowance: response=%q queued=%d reports=%d", response, len(input), reports)
	}
}

func TestManualDXAdmissionUsesCanonicalBand(t *testing.T) {
	gate := manualDXGate(t, config.ReputationConfig{})
	gate.RecordLogin("K1ABC-1", "203.0.113.1", time.Now().UTC())
	input := make(chan *spot.Spot, 1)
	var reason reputation.DropReason
	processor := NewProcessor(nil, nil, input, nil, gate, func(event reputation.DropEvent) { reason = event.Reason })
	if response := processor.ProcessCommandForClient("DX 13999.999 W6ABC CW TEST", "K1ABC-1", "203.0.113.1", nil, "go"); response != "Spot queued.\n" || len(input) != 1 {
		t.Fatalf("canonical boundary control: response=%q queued=%d", response, len(input))
	}
	queued := <-input
	if queued.Frequency != 14000 || queued.Band != "20m" {
		t.Fatalf("wrong queued canonical frequency/band: %+v", queued)
	}
	if response := processor.ProcessCommandForClient("DX 14074 W6ABC CW TEST", "K1ABC-2", "203.0.113.1", nil, "go"); response != "Spot rejected.\n" || len(input) != 0 || reason != reputation.DropBandCap {
		t.Fatalf("admission used raw band: response=%q queued=%d reason=%q", response, len(input), reason)
	}
}

func TestManualDXFrequencyHelpMatchesCurrentEnvelope(t *testing.T) {
	processor := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		text := processor.ProcessCommandForClient("HELP DX", "K1ABC", "", nil, dialect)
		for _, required := range []string{"After 10 Hz rounding: 135.70 through 2450000.00 kHz.", "Values in gaps between bands remain accepted."} {
			if !strings.Contains(text, required) {
				t.Fatalf("%s HELP DX missing %q", dialect, required)
			}
		}
		assertHelpWidth(t, text)
	}
}

func FuzzManualDXFrequencyAdmission(f *testing.F) {
	for _, token := range append(invalidManualFrequencies(), "135.695", "2450000.004", "13999.999", "1000", "5E7", "") {
		f.Add(token)
	}
	f.Fuzz(func(t *testing.T, token string) {
		if len(token) > 1024 {
			return // Test-work bound; no production input rule is introduced.
		}
		input := make(chan *spot.Spot, 1)
		processor := NewProcessor(nil, nil, input, nil, nil, nil)
		response := processor.ProcessCommandForClient("DX W6ABC "+token+" CW", "K1ABC", "", nil, "go")
		if response != "Spot queued.\n" {
			if len(input) != 0 {
				t.Fatal("refused frequency reached queue")
			}
			return
		}
		if len(input) != 1 {
			t.Fatal("queue acknowledgement lacks queued spot")
		}
		queued := <-input
		frequency := queued.Frequency
		if math.IsNaN(frequency) || math.IsInf(frequency, 0) || frequency < 135.7 || frequency > 2450000 ||
			math.Abs(frequency*100-math.Round(frequency*100)) > 1e-6 {
			t.Fatalf("admitted frequency outside canonical domain: %v", frequency)
		}
	})
}
