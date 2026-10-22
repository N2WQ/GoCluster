package telnet

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func recentConsumerConfig(t *testing.T, mode string) pathreliability.Config {
	t.Helper()
	cfg, err := pathreliability.LoadFile(filepath.Join("..", "data", "config", "path_reliability.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinObservationCount != 21 || cfg.BeaconMinObservationCount != 11 || cfg.ReceiverMaxEffectiveCount != 8 {
		t.Fatal("shipped 21/11/8 support policy changed")
	}
	cfg.ReceiverContributionMode = mode
	cfg.BandHalfLifeSec = map[string]int{"20m": 180, "160m": 180}
	cfg.MaxPredictionAgeHalfLifeMultiplier = 3
	cfg.VOACAPFallback.Enabled, cfg.Native160Fallback.Enabled = false, false
	return cfg
}

func recentConsumerFixture(t *testing.T, cfg pathreliability.Config, at time.Time, band, dxGrid string, beacon bool) (*Server, *Client, *spot.Spot) {
	t.Helper()
	requireH3Mappings(t)
	rx, tx := pathreliability.EncodeCell("FN31"), pathreliability.EncodeCell(dxGrid)
	rc, tc := pathreliability.EncodeCoarseCell("FN31"), pathreliability.EncodeCoarseCell(dxGrid)
	if rx == 0 || tx == 0 || rc == 0 || tc == 0 {
		t.Fatal("fixture requires real mapped cells")
	}
	if dxGrid == "IO91" && (rx == tx || rc == tc) {
		t.Fatal("intercontinental fixture requires distinct directions")
	}
	p := pathreliability.NewPredictor(cfg, []string{band})
	s := &Server{pathPredictor: p, pathDisplay: true, noiseModel: cfg.NoiseModel(), nowFn: func() time.Time { return at }}
	c := &Client{filter: passRuleTestFilter(t, "MODE=FT8"), spotChan: make(chan *spotEnvelope, 1), grid: "FN31", gridCell: rx, gridCoarseCell: rc, noiseClass: "QUIET"}
	frequency := 14074.0
	if band == "160m" {
		frequency = 1840
	}
	sp := spot.NewSpot("K1S7", "N0S7", frequency, "FT8")
	sp.BandNorm, sp.DXCellID, sp.DXMetadata.Grid, sp.IsBeacon, sp.Time = band, uint16(tx), dxGrid, beacon, at
	return s, c, sp
}

func recentConsumerReports(t *testing.T, s *Server, c *Client, sp *spot.Spot, reports, receivers int, weight, db float64, observed, arrived time.Time, reverse bool) {
	t.Helper()
	rx, tx := c.gridCoarseCell, pathreliability.EncodeCoarseCell(sp.DXMetadata.Grid)
	if reverse {
		rx, tx = tx, rx
	}
	for i := 0; i < reports; i++ {
		admission := s.pathPredictor.UpdateWithReceiverHash(pathreliability.BucketCombined, 0, 0, rx, tx, sp.BandNorm, db, weight,
			pathreliability.ObservationTimes{ObservedAt: observed, ArrivedAt: arrived}, false, uint64(1+i%receivers))
		if !admission.Accepted() {
			t.Fatalf("report %d rejected: %+v", i, admission)
		}
	}
}

func recentConsumerPrediction(t *testing.T, s *Server, c *Client, sp *spot.Spot) pathPrediction {
	t.Helper()
	got, ok := s.computePathPredictionForClient(c, sp)
	if !ok {
		t.Fatal("connected prediction unavailable")
	}
	return got
}

func recentConsumerWantSupport(t *testing.T, got pathreliability.ReportSupport, raw, admitted float64, mode string) {
	t.Helper()
	want := pathreliability.ReportSupport{RawUsable: raw, Admitted: admitted, AdmittedAvailable: mode != pathreliability.ReceiverContributionOff, Enforced: mode == pathreliability.ReceiverContributionEnforce}
	active := raw
	if mode == pathreliability.ReceiverContributionOff {
		want.Admitted = 0
	}
	if want.Enforced {
		active = admitted
	}
	units, ok := got.ActiveUnits()
	if got != want || !ok || units != active {
		t.Fatalf("recent support=%+v active=%g/%v, want %+v active=%g", got, units, ok, want, active)
	}
}

func TestPathRecentSupportPolicy(t *testing.T) {
	for _, mode := range []string{pathreliability.ReceiverContributionOff, pathreliability.ReceiverContributionShadow, pathreliability.ReceiverContributionEnforce} {
		for _, beacon := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/beacon=%t", mode, beacon), func(t *testing.T) {
				cfg, at := recentConsumerConfig(t, mode), time.Date(2026, time.June, 18, 4, 0, 0, 0, time.UTC)
				s, c, sp := recentConsumerFixture(t, cfg, at, "20m", "IO91", beacon)
				reports, receivers, required := 21, 6, uint32(3)
				if beacon {
					reports, receivers, required = 11, 2, 2
				}
				recentConsumerReports(t, s, c, sp, reports, receivers, 1, -10, at, at, false)
				c.filter = passRuleTestFilter(t, "MODE=FT8 AND PATH=HIGH")
				c.setDiagMode(diagModePath)
				env := deliverPathPredictionReuseSpot(t, s, c, sp)
				if env.pathPrediction == nil {
					t.Fatal("PATH admission did not carry prediction")
				}
				fresh := env.pathPrediction.result
				recentConsumerWantSupport(t, fresh.ObservedRecentSupport, float64(reports), float64(reports), mode)
				if fresh.Source != pathreliability.SourceCombined || fresh.Class != "HIGH" || fresh.P50DB != -9.5 || !fresh.HasP50 {
					t.Fatalf("fresh reports must qualify: %+v", fresh)
				}
				line := s.formatSpotEnvelopeForClient(c, env)
				if len(strings.TrimRight(line, "\r\n")) != 78 || !strings.Contains(line, fmt.Sprintf("/u%d|", reports)) {
					t.Fatalf("fresh support/78-column DIAG: %q", line)
				}
				s.nowFn = func() time.Time { return at.Add(180 * time.Second) }
				aged := recentConsumerPrediction(t, s, c, sp).result
				recentConsumerWantSupport(t, aged.ObservedRecentSupport, float64(reports)/2, float64(reports)/2, mode)
				if aged.Source != pathreliability.SourceInsufficient || aged.InsufficientReason != pathreliability.InsufficientLowCount || !aged.HasP50 || aged.P50DB != -9.5 || aged.ObservationCount != uint32(reports) || aged.RawCount != uint32(reports) {
					t.Fatalf("aging must lower support while preserving SNR/lifetime counts: %+v", aged)
				}
				if mode == pathreliability.ReceiverContributionOff {
					required = 0
				}
				if aged.ReceiverRequired != required || aged.CapWouldBlock != (mode == pathreliability.ReceiverContributionShadow) {
					t.Fatalf("receiver baseline/shadow gate changed: %+v", aged)
				}
				s.deliverJob(broadcastJob{spot: sp, clients: []*Client{c}, allowFast: true, allowMed: true, allowSlow: true})
				select {
				case <-c.spotChan:
					t.Fatal("PATH HIGH admitted insufficient aged support")
				default:
				}
				c.filter = passRuleTestFilter(t, "MODE=FT8 AND PATH=INSUFFICIENT")
				env = deliverPathPredictionReuseSpot(t, s, c, sp)
				line = s.formatSpotEnvelopeForClient(c, env)
				wantUnits := "/u10.5|"
				if beacon {
					wantUnits = "/u5.5|"
				}
				if env.pathPrediction == nil || env.pathPrediction.result.Source != pathreliability.SourceInsufficient || len(strings.TrimRight(line, "\r\n")) != 78 || !strings.Contains(line, wantUnits) {
					t.Fatalf("aged support DIAG/admission: %q", line)
				}
			})
		}
	}
}

func TestPathRecentSupportUserOverride(t *testing.T) {
	for _, mode := range []string{pathreliability.ReceiverContributionOff, pathreliability.ReceiverContributionShadow, pathreliability.ReceiverContributionEnforce} {
		for _, beacon := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/beacon=%t", mode, beacon), func(t *testing.T) {
				cfg, at := recentConsumerConfig(t, mode), time.Date(2026, time.June, 18, 4, 0, 0, 0, time.UTC)
				s, c, sp := recentConsumerFixture(t, cfg, at.Add(180*time.Second), "20m", "IO91", beacon)
				recentConsumerReports(t, s, c, sp, 63, 12, 1, -10, at, at, false)
				before := recentConsumerPrediction(t, s, c, sp).result
				if before.Source != pathreliability.SourceCombined || before.CapWouldBlock {
					t.Fatalf("31.5 units must meet default floor without shadow blocking: %+v", before)
				}
				response, handled := s.handlePathSettingsCommand(c, "SET PATHSAMPLES 32")
				if !handled || c.pathSnapshot().pathMinObservationCount != 32 || !strings.Contains(response, "set to 32") {
					t.Fatalf("real PATHSAMPLES update failed: %q", response)
				}
				got := recentConsumerPrediction(t, s, c, sp).result
				recentConsumerWantSupport(t, got.ObservedRecentSupport, 31.5, 31.5, mode)
				if got.Source != pathreliability.SourceInsufficient || got.InsufficientReason != pathreliability.InsufficientLowCount || got.ObservationCount != 63 || !got.HasP50 || got.P50DB != -9.5 {
					t.Fatalf("31.5 must not round up or use lifetime63 at floor32: %+v", got)
				}
				required := uint32(3)
				if beacon {
					required = 2
				}
				if mode == pathreliability.ReceiverContributionOff {
					required = 0
				}
				if got.ReceiverRequired != required || got.CapWouldBlock != (mode == pathreliability.ReceiverContributionShadow) {
					t.Fatalf("override changed receiver baseline/shadow comparison: %+v", got)
				}
				response, handled = s.handlePathSettingsCommand(c, "SET PATHSAMPLES 20")
				if !handled || c.pathSnapshot().pathMinObservationCount != 32 || !strings.Contains(response, "greater than cluster default 21") {
					t.Fatalf("user weakened configured floor: %q", response)
				}
				response, handled = s.handlePathSettingsCommand(c, "SET PATHSAMPLES DEFAULT")
				if !handled || c.pathSnapshot().pathMinObservationCount != 0 || !strings.Contains(response, "reset to cluster default 21") {
					t.Fatalf("default reset failed: %q", response)
				}
				if reset := recentConsumerPrediction(t, s, c, sp).result; reset.Source != pathreliability.SourceCombined || reset.ObservedRecentSupport != before.ObservedRecentSupport {
					t.Fatalf("default reset lost the same qualifying recent evidence: %+v", reset)
				}
			})
		}
	}
}

func TestPathRecentSupportFractionalAdmission(t *testing.T) {
	for _, mode := range []string{pathreliability.ReceiverContributionOff, pathreliability.ReceiverContributionShadow, pathreliability.ReceiverContributionEnforce} {
		for _, beacon := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/beacon=%t", mode, beacon), func(t *testing.T) {
				cfg, at := recentConsumerConfig(t, mode), time.Date(2026, time.June, 18, 4, 0, 0, 0, time.UTC)
				cfg.ReceiverMaxEffectiveWeight = 14.5
				reports, admitted := 24, 21.75
				if beacon {
					cfg.ReceiverMaxEffectiveWeight, reports, admitted = 7.25, 12, 10.875
				}
				s, c, sp := recentConsumerFixture(t, cfg, at, "20m", "IO91", beacon)
				recentConsumerReports(t, s, c, sp, reports, 3, 2, -10, at, at, false)
				if !beacon {
					if before := recentConsumerPrediction(t, s, c, sp).result; before.Source != pathreliability.SourceCombined || before.CapWouldBlock {
						t.Fatalf("21.75 must meet cluster21 before override without shadow blocking: %+v", before)
					}
					response, handled := s.handlePathSettingsCommand(c, "SET PATHSAMPLES 22")
					if !handled || !strings.Contains(response, "set to 22") {
						t.Fatalf("override failed: %q", response)
					}
				}
				got := recentConsumerPrediction(t, s, c, sp).result
				recentConsumerWantSupport(t, got.ObservedRecentSupport, float64(reports), admitted, mode)
				wantSource := pathreliability.SourceCombined
				if mode == pathreliability.ReceiverContributionEnforce {
					wantSource = pathreliability.SourceInsufficient
				}
				if got.Source != wantSource || !got.HasP50 || got.P50DB != -9.5 || got.ObservationCount != uint32(reports) {
					t.Fatalf("fractional support was confused with report volume/signal mass: %+v", got)
				}
				if mode == pathreliability.ReceiverContributionEnforce && got.InsufficientReason != pathreliability.InsufficientLowCount {
					t.Fatalf("fractional admission failed a different gate: %+v", got)
				}
				if got.CapWouldBlock != (mode == pathreliability.ReceiverContributionShadow) {
					t.Fatalf("shadow ignored effective sample floor: %+v", got)
				}
			})
		}
	}
}

func TestPathRecentSupportConfiguredReceiverBaseline(t *testing.T) {
	cfg, at := recentConsumerConfig(t, pathreliability.ReceiverContributionEnforce), time.Date(2026, time.June, 18, 4, 0, 0, 0, time.UTC)
	s, c, sp := recentConsumerFixture(t, cfg, at, "20m", "IO91", false)
	for _, reverse := range []bool{false, true} {
		recentConsumerReports(t, s, c, sp, 32, 4, 1, -10, at, at, reverse)
	}
	response, handled := s.handlePathSettingsCommand(c, "SET PATHSAMPLES 40")
	if !handled || !strings.Contains(response, "set to 40") {
		t.Fatalf("override failed: %q", response)
	}
	got := recentConsumerPrediction(t, s, c, sp).result
	recentConsumerWantSupport(t, got.ObservedRecentSupport, 64, 64, pathreliability.ReceiverContributionEnforce)
	if got.Source != pathreliability.SourceCombined || got.ReceiverCount != 4 || got.ReceiverRequired != 3 || got.Weight != 32 || got.P50DB != -9.5 {
		t.Fatalf("user40 must retain configured21 receiver baseline (three, not five): %+v", got)
	}
}

func TestPathRecentSupportDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result pathreliability.Result
		want   string
	}{
		{"cap_off", pathreliability.Result{ObservationCount: 21, ObservedRecentSupport: pathreliability.ReportSupport{RawUsable: 10.5}}, "n21/u10.5"},
		{"explicit_zero_not_lifetime", pathreliability.Result{ObservationCount: 99, ObservedRecentSupport: pathreliability.ReportSupport{}}, "n99/u0"},
		{"unavailable_enforced_no_raw_substitution", pathreliability.Result{ObservationCount: 99, ObservedRecentSupport: pathreliability.ReportSupport{RawUsable: 99, Enforced: true}}, "n99/u?"},
		{"admitted_fraction", pathreliability.Result{ObservationCount: 24, CappedCount: 21, CapLimited: true, ReceiverCount: 3, ReceiverRequired: 3, ObservedRecentSupport: pathreliability.ReportSupport{RawUsable: 24, Admitted: 21.75, AdmittedAvailable: true, Enforced: true}}, "n24/u21.8/c21/rx3"},
		{"shadow_active_raw", pathreliability.Result{ObservationCount: 24, CappedCount: 21, CapLimited: true, ReceiverCount: 3, ReceiverRequired: 3, ObservedRecentSupport: pathreliability.ReportSupport{RawUsable: 24, Admitted: 21.75, AdmittedAvailable: true}}, "n24/u24/c21/rx3"},
		{"tenths_down", pathreliability.Result{ObservationCount: 2, ObservedRecentSupport: pathreliability.ReportSupport{RawUsable: 1.24}}, "n2/u1.2"},
		{"tenths_up", pathreliability.Result{ObservationCount: 2, ObservedRecentSupport: pathreliability.ReportSupport{RawUsable: 1.26}}, "n2/u1.3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := diagPathCountToken(tc.result) + diagPathCapToken(tc.result); got != tc.want {
				t.Fatalf("count token=%q, want %q", got, tc.want)
			}
		})
	}
}

func TestPathRecentSupportReportPresentWidth(t *testing.T) {
	for _, beacon := range []bool{false, true} {
		t.Run(fmt.Sprintf("beacon=%t", beacon), func(t *testing.T) {
			cfg := recentConsumerConfig(t, pathreliability.ReceiverContributionEnforce)
			at := time.Date(2026, time.June, 18, 4, 0, 0, 0, time.UTC)
			s, c, sp := recentConsumerFixture(t, cfg, at.Add(180*time.Second), "20m", "IO91", beacon)
			sp.HasReport, sp.Report = true, -10
			reports := 21
			if beacon {
				reports = 11
			}
			recentConsumerReports(t, s, c, sp, reports, 6, 1, -10, at, at, false)
			c.setDiagMode(diagModePath)
			c.filter = passRuleTestFilter(t, "MODE=FT8 AND PATH=INSUFFICIENT")
			env := deliverPathPredictionReuseSpot(t, s, c, sp)
			line := strings.TrimRight(s.formatSpotEnvelopeForClient(c, env), "\r\n")
			want := "DX de N0S7:      14074.00  K1S7        FT8 -10 dB n21/u10.5|lo    IO91   0403Z"
			if beacon {
				want = "DX de N0S7:      14074.00  K1S7        FT8 -10 dB brx|n11/u5.5    IO91   0403Z"
			}
			// A present report consumes comment space: the ordinary reason is
			// partial and the beacon reason is clipped. Preserve the wire budget.
			if line != want || len(line) != 78 || env.pathPrediction == nil || env.pathPrediction.result.InsufficientReason != pathreliability.InsufficientLowCount {
				t.Fatalf("report-present width/decision changed: %q", line)
			}
		})
	}
}
