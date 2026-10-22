package telnet

import (
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestHandleDiagCommandOnRejected(t *testing.T) {
	server := NewServer(ServerOptions{}, nil)
	client := &Client{}

	resp, handled := server.handleDiagCommand(client, "SET DIAG ON")
	if !handled {
		t.Fatalf("expected SET DIAG ON to be handled as usage")
	}
	if !strings.Contains(resp, "Usage: SET DIAG <OFF|DEDUPE|SOURCE|CONF|PATH|MODE>") {
		t.Fatalf("expected usage response, got %q", resp)
	}
	if client.getDiagMode() != diagModeOff {
		t.Fatalf("expected diag mode OFF")
	}
}

func TestFormatSpotForClientDedupeDiagComment(t *testing.T) {
	server := NewServer(ServerOptions{}, nil)
	client := &Client{}
	client.setDedupePolicy(dedupePolicySlow)
	client.setDiagMode(diagModeDedupe)

	sp := spot.NewSpot("LZ2BE", "M9PSY-#", 3524.6, "CW")
	sp.Report = 26
	sp.HasReport = true
	sp.Time = time.Date(2025, time.January, 7, 4, 9, 0, 0, time.UTC)
	sp.SourceType = spot.SourceRBN
	sp.DEMetadata.ADIF = 291
	sp.DEMetadata.CQZone = 5
	sp.DEMetadata.Grid = "KN33"
	sp.DXMetadata.Grid = "KN33"
	sp.Confidence = "S"
	sp.Comment = "ORIG"

	line := server.formatSpotForClient(client, sp)
	if strings.Contains(line, "ORIG") {
		t.Fatalf("expected diagnostic comment to replace original, got %q", line)
	}
	if !strings.Contains(line, "291|05|S|S") {
		t.Fatalf("expected diagnostic tag in output, got %q", line)
	}
	if !strings.HasSuffix(strings.TrimRight(line, "\r\n "), "KN33 S 0409Z") {
		t.Fatalf("expected tail preserved, got %q", line)
	}
}

func TestFormatSpotForClientDedupeDiagUsesNearbyEffectivePolicy(t *testing.T) {
	requireH3Mappings(t)
	server := NewServer(ServerOptions{
		DedupeFastEnabled: true,
		DedupeMedEnabled:  true,
		DedupeSlowEnabled: true,
	}, nil)
	client := &Client{filter: testRuleFilter(t)}
	client.setDedupePolicy(dedupePolicySlow)
	client.setDiagMode(diagModeDedupe)
	if err := client.filter.EnableNearby(pathreliability.EncodeCell("FN31"), pathreliability.EncodeCoarseCell("FN31")); err != nil {
		t.Fatalf("EnableNearby failed: %v", err)
	}

	sp := spot.NewSpot("LZ2BE", "M9PSY-#", 14024.6, "CW")
	sp.Time = time.Date(2025, time.January, 7, 4, 9, 0, 0, time.UTC)
	sp.SourceType = spot.SourceRBN
	sp.DEMetadata.ADIF = 291
	sp.DEMetadata.CQZone = 5
	sp.DEMetadata.Grid = "FN31"
	sp.DXMetadata.Grid = "FN31"
	sp.Comment = "ORIG"

	line := server.formatSpotForClient(client, sp)
	if !strings.Contains(line, "291|FN|S|F") {
		t.Fatalf("expected NEARBY effective FAST diagnostic tag, got %q", line)
	}
	if strings.Contains(line, "291|05|S|S") {
		t.Fatalf("did not expect saved SLOW diagnostic tag while NEARBY effective policy is FAST, got %q", line)
	}
}

func TestFormatSpotForClientSourceDiagPeerNode(t *testing.T) {
	server := NewServer(ServerOptions{}, nil)
	client := &Client{}
	client.setDiagMode(diagModeSource)

	sp := spot.NewSpot("K1ABC", "W1AW", 14025.0, "CW")
	sp.Time = time.Date(2025, time.January, 7, 4, 9, 0, 0, time.UTC)
	sp.SourceType = spot.SourcePeer
	sp.SourceNode = "n0call-15-extra"
	sp.DXMetadata.Grid = "FN31"

	line := server.formatSpotForClient(client, sp)
	if !strings.Contains(line, "P:N0CAL") {
		t.Fatalf("expected capped peer source diagnostic in output, got %q", line)
	}
}

func TestFormatSpotForClientConfidenceDiagComment(t *testing.T) {
	server := NewServer(ServerOptions{}, nil)
	client := &Client{}
	client.setDiagMode(diagModeConfidence)

	sp := spot.NewSpot("K1ABC", "W1AW", 14025.0, "CW")
	sp.Time = time.Date(2025, time.January, 7, 4, 9, 0, 0, time.UTC)
	sp.Confidence = "P"
	sp.ConfidencePercent = 37
	sp.ConfidencePercentOK = true
	sp.DXMetadata.Grid = "FN31"

	line := server.formatSpotForClient(client, sp)
	if !strings.Contains(line, "37%") {
		t.Fatalf("expected confidence diagnostic in output, got %q", line)
	}
}

func TestFormatSpotForClientPathDiagCommentIncludesCount(t *testing.T) {
	now := time.Date(2025, time.January, 7, 4, 9, 0, 0, time.UTC)
	cfg := pathreliability.DefaultConfig()
	cfg.MinObservationCount = 1
	cfg.MinEffectiveWeight = 0.1
	cfg.ReceiverContributionMode = pathreliability.ReceiverContributionOff
	predictor := pathreliability.NewPredictor(cfg, []string{"20m"})
	server := NewServer(ServerOptions{
		PathPredictor:      predictor,
		PathDisplayEnabled: true,
	}, nil)
	server.nowFn = func() time.Time { return now }
	client := &Client{grid: "FN31"}
	client.setDiagMode(diagModePath)

	userCell, dxCell, userCoarse, dxCoarse := requireDistinctPathCells(t)
	receiver := pathreliability.ReceiverIdentityHash("W1AW")
	predictor.UpdateWithReceiverHash(pathreliability.BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -12, 1, pathreliability.ObservationTimes{ObservedAt: now.Add(-10 * time.Second), ArrivedAt: now.Add(-10 * time.Second)}, false, receiver)
	predictor.UpdateWithReceiverHash(pathreliability.BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -12, 1, pathreliability.ObservationTimes{ObservedAt: now.Add(-5 * time.Second), ArrivedAt: now.Add(-5 * time.Second)}, false, receiver)

	sp := spot.NewSpot("K1ABC", "W1AW", 14074.0, "FT8")
	sp.Time = now
	sp.Band = "20m"
	sp.DXMetadata.Grid = "IO91"
	sp.Confidence = "V"

	line := server.formatSpotForClient(client, sp)
	if !strings.Contains(line, "n") || !strings.Contains(line, "|w") || !strings.Contains(line, "|a") {
		t.Fatalf("expected path diagnostic in output, got %q", line)
	}
	if !strings.Contains(line, "n2/u2|") {
		t.Fatalf("expected path observation count without fine/coarse double count, got %q", line)
	}
	if strings.Contains(line, "P:") {
		t.Fatalf("expected path diagnostic without type marker or glyph, got %q", line)
	}
}

func TestFormatSpotForClientPathDiagUsesUnionWeight(t *testing.T) {
	now := time.Date(2025, time.January, 7, 4, 9, 0, 0, time.UTC)
	cfg := pathreliability.DefaultConfig()
	cfg.MinObservationCount = 1
	cfg.ReceiverContributionMode = pathreliability.ReceiverContributionOff
	predictor := pathreliability.NewPredictor(cfg, []string{"20m"})
	server := NewServer(ServerOptions{
		PathPredictor:      predictor,
		PathDisplayEnabled: true,
	}, nil)
	server.nowFn = func() time.Time { return now }
	client := &Client{grid: "FN31"}
	client.setDiagMode(diagModePath)

	userCell, dxCell, userCoarse, dxCoarse := requireDistinctPathCells(t)
	for i := 0; i < 6; i++ {
		predictor.UpdateWithReceiverHash(pathreliability.BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -12, 1, pathreliability.ObservationTimes{ObservedAt: now.Add(-5 * time.Second), ArrivedAt: now.Add(-5 * time.Second)}, false, pathreliability.ReceiverIdentityHash("W1AW"))
	}

	sp := spot.NewSpot("K1ABC", "W1AW", 14074.0, "FT8")
	sp.Time = now
	sp.Band = "20m"
	sp.DXMetadata.Grid = "IO91"
	sp.Confidence = "V"

	line := server.formatSpotForClient(client, sp)
	if !strings.Contains(line, "n6/u5.9|w3|") {
		t.Fatalf("expected one-way union diagnostic weight n6|w3, got %q", line)
	}
}

func TestDiagPathInsufficientLowCountReason(t *testing.T) {
	if got := diagPathInsufficientReason(pathreliability.InsufficientLowCount); got != "lown" {
		t.Fatalf("expected low-count path diagnostic reason lown, got %q", got)
	}
	if got := diagPathInsufficientReason(pathreliability.InsufficientLowReceiver); got != "lowr" {
		t.Fatalf("expected low-receiver path diagnostic reason lowr, got %q", got)
	}
}

func TestDiagPathTagLiteralFormatting(t *testing.T) {
	for _, tc := range []struct {
		name       string
		prediction pathPrediction
		want       string
	}{
		{"combined capped counts", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceCombined, Weight: 19, Count: 19, RawCount: 19, CappedCount: 5, CappedWeight: 5, AgeSec: 12, CapLimited: true, CapWouldBlock: true, ObservedRecentSupport: pathreliability.ReportSupport{RawUsable: 19, Admitted: 5, AdmittedAvailable: true, Enforced: true}}}, "n19/u5/c5|w5|a12"},
		{"VOACAP selected hour", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceVOACAPClosed, VOACAPFT8SNRDB: -34, VOACAPHourUTC: 3, VOACAPSSN: 147, VOACAPAgeSec: 9}}, "vcap|-34|h03|s147"},
		{"VOACAP aligned sparse p50", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceVOACAPAligned, P50DB: -14.5, VOACAPFT8SNRDB: -15, VOACAPHourUTC: 20, VOACAPSSN: 112}}, "valn|-15/-15h20s112"},
		{"VOACAP sparse upgrade", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceVOACAPSparseUpgrade, P50DB: -19, VOACAPFT8SNRDB: -15, VOACAPHourUTC: 20, VOACAPSSN: 112, VOACAPReqSNRReliability: 0.84, VOACAPHasReqSNRReliability: true}}, "vup|-19/-15r84s112"},
		{"VOACAP open without p50", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceVOACAPOpen, VOACAPFT8SNRDB: -19, VOACAPHourUTC: 20, VOACAPSSN: 112, VOACAPReqSNRReliability: 0.75, VOACAPHasReqSNRReliability: true}}, "vop|-19r75h20s112"},
		{"native 160 closed", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceNative160, Class: filter.PathClassClosed, Native160CivilDarkFraction: 0.12}}, "n160c|d12"},
		{"beacon native 160 closed", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceNative160, Class: filter.PathClassClosed, BeaconRX: true, Native160CivilDarkFraction: 0.05}}, "bn160c|d05"},
		{"user daylight closed", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceNative160, Class: filter.PathClassClosed, Native160UserDaylight: true, Native160CivilDarkFraction: 0.82}}, "n160c|uD|d82"},
		{"DX twilight unlikely", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceNative160, Class: filter.PathClassUnlikely, Native160DXTwilight: true, Native160CivilDarkFraction: 0.54}}, "n160|xT|d54"},
		{"beacon both daylight closed", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceNative160, Class: filter.PathClassClosed, BeaconRX: true, Native160UserDaylight: true, Native160DXDaylight: true, Native160CivilDarkFraction: 0.05}}, "bn160c|bD|d05"},
		{"sparse delayed", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceInsufficient, InsufficientReason: pathreliability.InsufficientNoSample}, sparseVOACAPTrace: sparseP50VOACAPTrace{Active: true, NoP50: true, Status: pathreliability.VOACAPForecastCheckDelayWait, CacheMiss: true}}, "n0/u0|none|vdly"},
		{"sparse staleHour", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceInsufficient, InsufficientReason: pathreliability.InsufficientNoSample}, sparseVOACAPTrace: sparseP50VOACAPTrace{Active: true, NoP50: true, Status: pathreliability.VOACAPForecastCheckDelayWait, CacheMiss: true, NoCurrentHour: true}}, "n0/u0|none|vcur"},
		{"sparse unsupported band", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceInsufficient, InsufficientReason: pathreliability.InsufficientNoSample}, sparseVOACAPTrace: sparseP50VOACAPTrace{Active: true, NoP50: true, Status: pathreliability.VOACAPForecastCheckInvalidRequest, InvalidReason: pathreliability.VOACAPInvalidUnsupportedBand}}, "n0/u0|none|vband"},
		{"sparse empty unknown band", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceInsufficient, InsufficientReason: pathreliability.InsufficientNoSample}, sparseVOACAPTrace: sparseP50VOACAPTrace{Active: true, NoP50: true, Status: pathreliability.VOACAPForecastCheckInvalidRequest, InvalidReason: pathreliability.VOACAPInvalidEmptyUnknownBand}}, "n0/u0|none|vnbnd"},
		{"sparse user grid", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceInsufficient, InsufficientReason: pathreliability.InsufficientNoSample}, sparseVOACAPTrace: sparseP50VOACAPTrace{Active: true, NoP50: true, Status: pathreliability.VOACAPForecastCheckInvalidRequest, InvalidReason: pathreliability.VOACAPInvalidUserGrid}}, "n0/u0|none|vugrd"},
		{"sparse DX grid", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceInsufficient, InsufficientReason: pathreliability.InsufficientNoSample}, sparseVOACAPTrace: sparseP50VOACAPTrace{Active: true, NoP50: true, Status: pathreliability.VOACAPForecastCheckInvalidRequest, InvalidReason: pathreliability.VOACAPInvalidDXGrid}}, "n0/u0|none|vdgrd"},
		{"sparse user cell", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceInsufficient, InsufficientReason: pathreliability.InsufficientNoSample}, sparseVOACAPTrace: sparseP50VOACAPTrace{Active: true, NoP50: true, Status: pathreliability.VOACAPForecastCheckInvalidRequest, InvalidReason: pathreliability.VOACAPInvalidUserCell}}, "n0/u0|none|vucel"},
		{"sparse DX cell", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceInsufficient, InsufficientReason: pathreliability.InsufficientNoSample}, sparseVOACAPTrace: sparseP50VOACAPTrace{Active: true, NoP50: true, Status: pathreliability.VOACAPForecastCheckInvalidRequest, InvalidReason: pathreliability.VOACAPInvalidDXCell}}, "n0/u0|none|vdcel"},
		{"sparse relFail", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceInsufficient}, sparseVOACAPTrace: sparseP50VOACAPTrace{Active: true, NoP50: true, Status: pathreliability.VOACAPForecastCheckReady, Outcome: sparseP50VOACAPOutcomeOpenRELFail}}, "n0/u0|none|vrel"},
		{"beacon p50", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceCombined, BeaconRX: true, Weight: 11, Count: 11, AgeSec: 12, ObservedRecentSupport: pathreliability.ReportSupport{RawUsable: 11}}}, "brx|n11/u11|w11|a12"},
		{"beacon insufficient", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceInsufficient, BeaconRX: true, Count: 10, InsufficientReason: pathreliability.InsufficientLowCount, ObservedRecentSupport: pathreliability.ReportSupport{RawUsable: 10}}}, "brx|n10/u10|lown"},
		{"beacon closed", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceVOACAPClosed, BeaconRX: true, VOACAPFT8SNRDB: -34, VOACAPHourUTC: 3, VOACAPSSN: 147}}, "bvcap|-34|h03|s147"},
		{"beacon aligned", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceVOACAPAligned, BeaconRX: true, P50DB: -14.5, VOACAPFT8SNRDB: -15, VOACAPHourUTC: 20, VOACAPSSN: 112}}, "bvaln|-15/-15h20s112"},
		{"beacon sparseUpgrade", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceVOACAPSparseUpgrade, BeaconRX: true, P50DB: -19, VOACAPFT8SNRDB: -15, VOACAPHourUTC: 20, VOACAPSSN: 112, VOACAPReqSNRReliability: 0.84, VOACAPHasReqSNRReliability: true}}, "bvup|-19/-15r84s112"},
		{"beacon open", pathPrediction{result: pathreliability.Result{Source: pathreliability.SourceVOACAPOpen, BeaconRX: true, VOACAPFT8SNRDB: -19, VOACAPHourUTC: 20, VOACAPSSN: 112, VOACAPReqSNRReliability: 0.75, VOACAPHasReqSNRReliability: true}}, "bvop|-19r75h20s112"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := diagPathTag(tc.prediction, true); got != tc.want {
				t.Fatalf("path diagnostic: got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFormatSpotForClientModeDiagComment(t *testing.T) {
	server := NewServer(ServerOptions{}, nil)
	client := &Client{}
	client.setDiagMode(diagModeMode)

	sp := spot.NewSpot("K1ABC", "W1AW", 14025.0, "RTTY")
	sp.Time = time.Date(2025, time.January, 7, 4, 9, 0, 0, time.UTC)
	sp.ModeProvenance = spot.ModeProvenanceCommentExplicit
	sp.DXMetadata.Grid = "FN31"
	sp.Comment = "ORIG"

	line := server.formatSpotForClient(client, sp)
	if strings.Contains(line, "ORIG") {
		t.Fatalf("expected mode diagnostic to replace original comment, got %q", line)
	}
	if !strings.Contains(line, "RTTY|CMT") {
		t.Fatalf("expected mode provenance diagnostic in output, got %q", line)
	}
	if !strings.Contains(line, "FN31") || !strings.Contains(line, "0409Z") {
		t.Fatalf("expected tail preserved, got %q", line)
	}
}

func TestFormatSpotForClientModeDiagBlankRegional(t *testing.T) {
	server := NewServer(ServerOptions{}, nil)
	client := &Client{}
	client.setDiagMode(diagModeMode)

	sp := spot.NewSpot("K1ABC", "W1AW", 14025.0, "")
	sp.Time = time.Date(2025, time.January, 7, 4, 9, 0, 0, time.UTC)
	sp.ModeProvenance = spot.ModeProvenanceRegionalMixedBlank
	sp.DXMetadata.Grid = "FN31"

	line := server.formatSpotForClient(client, sp)
	if !strings.Contains(line, "--|RMIX") {
		t.Fatalf("expected blank mode provenance diagnostic in output, got %q", line)
	}
}
