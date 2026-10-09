package commands

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/reputation"
	"dxcluster/spot"
)

func manualDXGate(t *testing.T, cfg config.ReputationConfig) *reputation.Gate {
	t.Helper()
	cfg.Enabled = true
	cfg.ReputationDir = t.TempDir()
	cfg.RampWindowSeconds = 3600 // Longer than the whole test timeout.
	cfg.PerBandStart, cfg.PerBandCap = 1, 1
	cfg.TotalCapStart, cfg.TotalCapPostRamp = 2, 2
	if cfg.IPv4BucketSize == 0 {
		cfg.IPv4BucketSize = 10000
		cfg.IPv4BucketRefillPerSec = 10000
	}
	gate, err := reputation.NewGate(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gate.Close)
	return gate // No enrichment or maintenance workers are started.
}

func TestManualDXGateRejectionIsGenericAndNeverQueued(t *testing.T) {
	for _, dialect := range []string{"go", "cc"} {
		for _, reason := range []reputation.DropReason{
			reputation.DropProbation, reputation.DropBandCap,
			reputation.DropTotalCap, reputation.DropPrefixCap,
		} {
			t.Run(dialect+"/"+string(reason), func(t *testing.T) {
				cfg := config.ReputationConfig{}
				if reason == reputation.DropProbation {
					cfg.InitialWaitSeconds = 3600
				}
				if reason == reputation.DropPrefixCap {
					cfg.IPv4BucketSize, cfg.IPv4BucketRefillPerSec = 1, 1
				}
				gate := manualDXGate(t, cfg)
				now := time.Now().UTC()
				gate.RecordLogin("K1ABC-1", "203.0.113.1", now)
				command := "DX 14074 W6ABC CW TEST"
				prime := func(band string, when time.Time) {
					t.Helper()
					if decision := gate.Check(reputation.Request{Call: "K1ABC", Band: band, IP: "203.0.113.1", Now: when}); !decision.Allow {
						t.Fatalf("eligible priming control failed: %+v", decision)
					}
				}
				switch reason {
				case reputation.DropBandCap:
					prime("20m", now)
				case reputation.DropTotalCap:
					prime("20m", now)
					prime("40m", now)
					command = "DX 3504 W6ABC CW TEST"
				case reputation.DropPrefixCap:
					// A later reference prevents refill before this single post;
					// the reported cause must still prove a prefix refusal.
					prime("20m", now.Add(time.Hour))
					command = "DX 7074 W6ABC CW TEST"
				}
				input := make(chan *spot.Spot, 1)
				var reported []reputation.DropEvent
				processor := NewProcessor(nil, nil, input, nil, gate, func(event reputation.DropEvent) {
					reported = append(reported, event)
				})
				if response := processor.ProcessCommandForClient(command, "K1ABC-2", "203.0.113.1", nil, dialect); response != "Spot rejected.\n" {
					t.Fatalf("rejection must be exact and generic: %q", response)
				}
				if len(input) != 0 || len(reported) != 1 || reported[0].Reason != reason || reported[0].Call != "K1ABC" {
					t.Fatalf("wrong queue/diagnostic effects: queued=%d reports=%+v", len(input), reported)
				}
			})
		}
	}
}

func TestManualDXGateFeedbackDoesNotRequireDiagnosticCallback(t *testing.T) {
	gate := manualDXGate(t, config.ReputationConfig{InitialWaitSeconds: 3600})
	input := make(chan *spot.Spot, 1)
	processor := NewProcessor(nil, nil, input, nil, gate, nil)
	if response := processor.ProcessCommandForClient("DX 14074 W6ABC CW TEST", "K1ABC-1", "203.0.113.1", nil, "go"); response != "Spot rejected.\n" || len(input) != 0 {
		t.Fatalf("callback-free refusal: response=%q queued=%d", response, len(input))
	}
}

func TestManualDXGateAcceptedSpotRetainsBaselineDisplay(t *testing.T) {
	for _, dialect := range []string{"go", "cc"} {
		t.Run(dialect, func(t *testing.T) {
			gate := manualDXGate(t, config.ReputationConfig{})
			gate.RecordLogin("K1ABC-1", "203.0.113.1", time.Now().UTC())
			input := make(chan *spot.Spot, 1)
			processor := NewProcessor(nil, nil, input, nil, gate, nil)
			if response := processor.ProcessCommandForClient("DX 14074 W6ABC CW TEST", "K1ABC-2", "203.0.113.1", nil, dialect); response != "Spot queued.\n" {
				t.Fatalf("accepted control: %q", response)
			}
			select {
			case queued := <-input:
				if queued.DECall != "K1ABC" || queued.SourceNode != "K1ABC" || queued.DXCall != "W6ABC" || queued.Frequency != 14074 {
					t.Fatalf("accepted spot lost baseline display or content: %+v", queued)
				}
			default:
				t.Fatal("accepted control was not queued")
			}
		})
	}
}
