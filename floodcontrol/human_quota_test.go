package floodcontrol

import (
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestNativeSourcesBypassReporterQuotaOnly(t *testing.T) {
	for _, source := range []spot.SourceType{spot.SourceRBN, spot.SourceFT4, spot.SourceFT8, spot.SourcePSKReporter} {
		for _, action := range []string{config.FloodActionObserve, config.FloodActionSuppress, config.FloodActionDrop} {
			t.Run(string(source)+"/"+action, func(t *testing.T) {
				cfg := testFloodConfig(action, 8)
				cfg.Rails.DECall.ThresholdsPerSourceType[string(source)] = 1
				output := make(chan *spot.Spot, 4)
				c := New(cfg, output, nil, nil)
				for range 3 {
					s := testSpot("W1AAA", spot.SourceType(" "+strings.ToLower(string(source))+" "))
					s.IsHuman, s.Mode = true, "CW" // Source type, not mutable flags or mode, controls exemption.
					c.process(time.Unix(1, 0), s)
				}
				p, o, s, d, overflow, actors := c.GetStats()
				if p != 3 || o != 0 || s != 0 || d != 0 || overflow != 0 || actors != 0 || len(output) != 3 {
					t.Fatalf("native quota charged: processed=%d observe=%d suppress=%d drop=%d overflow=%d actors=%d output=%d", p, o, s, d, overflow, actors, len(output))
				}
			})
		}
		for _, name := range []string{"source_node", "spotter_ip", "dxcall"} {
			t.Run(string(source)+"/retains_"+name, func(t *testing.T) {
				cfg := testFloodConfig(config.FloodActionSuppress, 8)
				cfg.Rails.DECall.ThresholdsPerSourceType[string(source)] = 1
				rail := cfg.Rails.DECall
				switch name {
				case "source_node":
					cfg.Rails.SourceNode = rail
				case "spotter_ip":
					cfg.Rails.SpotterIP = rail
				case "dxcall":
					rail.ActiveMode = "conservative"
					rail.ThresholdsByMode.Conservative = rail.ThresholdsPerSourceType
					cfg.Rails.DXCall = rail
				}
				output := make(chan *spot.Spot, 4)
				c := New(cfg, output, nil, nil)
				for range 3 {
					s := testSpot("W1AAA", source)
					s.SourceNode, s.SpotterIP = "N1NODE", "192.0.2.1"
					c.process(time.Unix(1, 0), s)
				}
				_, _, suppressed, _, _, actors := c.GetStats()
				if suppressed != 2 || actors != 1 || len(output) != 1 || len(c.rails[0].partitions) != 0 {
					t.Fatalf("other rail bypassed or reporter charged: suppressed=%d actors=%d output=%d reporter=%v", suppressed, actors, len(output), c.rails[0].partitions)
				}
			})
		}
	}
}

func TestHumanReporterAllowancesRemainSeparate(t *testing.T) {
	sources := []spot.SourceType{spot.SourceManual, spot.SourcePeer, spot.SourceUpstream}
	for _, exhausted := range sources {
		t.Run(string(exhausted), func(t *testing.T) {
			cfg := testFloodConfig(config.FloodActionSuppress, 8)
			for _, source := range sources {
				cfg.Rails.DECall.ThresholdsPerSourceType[string(source)] = 2
			}
			output := make(chan *spot.Spot, 8)
			c := New(cfg, output, nil, nil)
			arrivals := 0
			send := func(source spot.SourceType, accepted bool) {
				t.Helper()
				s := testSpot("W1AAA", source)
				s.SourceNode = []string{"N1AAA", "N2BBB", "N3CCC"}[arrivals%3]
				arrivals++ // Changing reported origin cannot mint another station allowance.
				s.IsHuman, s.Mode, s.Comment = false, "FT8", "RBN skimmer -#"
				c.process(time.Unix(1, 0), s)
				if (len(output) == 1) != accepted {
					t.Fatalf("%s accepted=%v want %v", source, len(output) == 1, accepted)
				}
				if accepted && <-output != s {
					t.Fatal("forwarded another report")
				}
			}
			send(exhausted, true)
			send(exhausted, true)
			send(exhausted, false)
			for _, source := range sources {
				if source != exhausted {
					send(source, true)
					send(source, true)
				}
			}
			p, _, suppressed, _, _, actors := c.GetStats()
			if p != 7 || suppressed != 1 || actors != 3 {
				t.Fatalf("isolation accounting: processed=%d suppressed=%d actors=%d", p, suppressed, actors)
			}
		})
	}
}

func TestReporterQuotaRecoversWithoutOperator(t *testing.T) {
	for _, source := range []spot.SourceType{spot.SourceManual, spot.SourcePeer, spot.SourceUpstream} {
		for _, scenario := range []string{"quiet", "slower"} {
			t.Run(string(source)+"/"+scenario, func(t *testing.T) {
				cfg := testFloodConfig(config.FloodActionSuppress, 8)
				cfg.Rails.DECall.ThresholdsPerSourceType[string(source)] = 3
				output := make(chan *spot.Spot, 1)
				c := New(cfg, output, nil, nil)
				base := time.Unix(1, 0)
				send := func(elapsed time.Duration, accepted bool) {
					t.Helper()
					c.process(base.Add(elapsed), testSpot("W1AAA", source))
					if (len(output) == 1) != accepted {
						t.Fatalf("at %v accepted=%v want %v", elapsed, len(output) == 1, accepted)
					}
					if accepted {
						<-output
					}
				}
				for range 3 {
					send(0, true)
				}
				send(0, false)
				if scenario == "quiet" {
					send(time.Minute, false) // Exact window cutoff remains inclusive.
					send(time.Minute+time.Nanosecond, true)
				} else {
					send(25*time.Second, false)
					send(50*time.Second, false)
					send(75*time.Second, true)
					send(100*time.Second, true)
					send(125*time.Second, true)
				}
				// No cleanup call, worker restart, reset or operator action supplied recovery.
			})
		}
	}
}
