package telnet

import (
	"strings"
	"testing"
	"time"

	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestHandleDiagCommandToggle(t *testing.T) {
	server := NewServer(ServerOptions{}, nil)
	client := &Client{}

	resp, handled := server.handleDiagCommand(client, "SET DIAG DEDUPE")
	if !handled {
		t.Fatalf("expected SET DIAG DEDUPE to be handled")
	}
	if !strings.Contains(resp, "DEDUPE") {
		t.Fatalf("expected DEDUPE response, got %q", resp)
	}
	if client.getDiagMode() != diagModeDedupe {
		t.Fatalf("expected diag mode DEDUPE")
	}

	resp, handled = server.handleDiagCommand(client, "SET DIAG OFF")
	if !handled {
		t.Fatalf("expected SET DIAG OFF to be handled")
	}
	if !strings.Contains(resp, "OFF") {
		t.Fatalf("expected OFF response, got %q", resp)
	}
	if client.getDiagMode() != diagModeOff {
		t.Fatalf("expected diag mode OFF")
	}
}

func TestHandleDiagCommandOnRejected(t *testing.T) {
	server := NewServer(ServerOptions{}, nil)
	client := &Client{}

	resp, handled := server.handleDiagCommand(client, "SET DIAG ON")
	if !handled {
		t.Fatalf("expected SET DIAG ON to be handled as usage")
	}
	if !strings.Contains(resp, "Usage: SET DIAG <OFF|DEDUPE|SOURCE|CONF|PATH|PATHP50|MODE>") {
		t.Fatalf("expected usage response, got %q", resp)
	}
	if client.getDiagMode() != diagModeOff {
		t.Fatalf("expected diag mode OFF")
	}
}

func TestHandleDiagCommandMode(t *testing.T) {
	server := NewServer(ServerOptions{}, nil)
	client := &Client{}

	resp, handled := server.handleDiagCommand(client, "SET DIAG MODE")
	if !handled {
		t.Fatalf("expected SET DIAG MODE to be handled")
	}
	if !strings.Contains(resp, "MODE") {
		t.Fatalf("expected MODE response, got %q", resp)
	}
	if client.getDiagMode() != diagModeMode {
		t.Fatalf("expected diag mode MODE")
	}
}

func TestHandleDiagCommandPathP50(t *testing.T) {
	server := NewServer(ServerOptions{}, nil)
	client := &Client{}

	resp, handled := server.handleDiagCommand(client, "SET DIAG PATHP50")
	if !handled {
		t.Fatalf("expected SET DIAG PATHP50 to be handled")
	}
	if !strings.Contains(resp, "PATHP50") {
		t.Fatalf("expected PATHP50 response, got %q", resp)
	}
	if client.getDiagMode() != diagModePathP50 {
		t.Fatalf("expected diag mode PATHP50")
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
	requireH3Mappings(t)
	now := time.Date(2025, time.January, 7, 4, 9, 0, 0, time.UTC)
	predictor := newTestPathPredictor()
	server := NewServer(ServerOptions{
		PathPredictor:      predictor,
		PathDisplayEnabled: true,
	}, nil)
	server.nowFn = func() time.Time { return now }
	client := &Client{grid: "FN31"}
	client.setDiagMode(diagModePath)

	userCell := pathreliability.EncodeCell("FN31")
	dxCell := pathreliability.EncodeCell("FN32")
	userCoarse := pathreliability.EncodeCoarseCell("FN31")
	dxCoarse := pathreliability.EncodeCoarseCell("FN32")
	receiver := pathreliability.ReceiverIdentityHash("W1AW")
	predictor.UpdateWithReceiverHash(pathreliability.BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -12, 1, now.Add(-10*time.Second), false, receiver)
	predictor.UpdateWithReceiverHash(pathreliability.BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -12, 1, now.Add(-5*time.Second), false, receiver)

	sp := spot.NewSpot("K1ABC", "W1AW", 14074.0, "FT8")
	sp.Time = now
	sp.Band = "20m"
	sp.DXMetadata.Grid = "FN32"
	sp.Confidence = "V"

	line := server.formatSpotForClient(client, sp)
	if !strings.Contains(line, "n") || !strings.Contains(line, "|w") || !strings.Contains(line, "|a") {
		t.Fatalf("expected path diagnostic in output, got %q", line)
	}
	if !strings.Contains(line, "n2|") {
		t.Fatalf("expected path observation count without fine/coarse double count, got %q", line)
	}
	if strings.Contains(line, "P:") {
		t.Fatalf("expected path diagnostic without type marker or glyph, got %q", line)
	}
}

func TestDiagPathInsufficientLowCountReason(t *testing.T) {
	if got := diagPathInsufficientReason(pathreliability.InsufficientLowCount); got != "lown" {
		t.Fatalf("expected low-count path diagnostic reason lown, got %q", got)
	}
}

func TestDiagPathTagShowsCappedAndRawCountsWhenLimited(t *testing.T) {
	prediction := pathPrediction{
		result: pathreliability.Result{
			Source:        pathreliability.SourceCombined,
			Weight:        19,
			Count:         19,
			RawCount:      19,
			CappedCount:   5,
			CappedWeight:  5,
			AgeSec:        12,
			CapLimited:    true,
			CapWouldBlock: true,
		},
	}
	got := diagPathTag(prediction, true)
	if got != "n5/r19|w5|a12" {
		t.Fatalf("unexpected capped path diagnostic: %q", got)
	}
}

func TestDiagPathP50TagShowsP50MeanAndCounts(t *testing.T) {
	prediction := pathPrediction{
		result: pathreliability.Result{
			Source:       pathreliability.SourceCombined,
			P50DB:        -15,
			HasP50:       true,
			MeanDB:       -11,
			HasMeanDB:    true,
			Glyph:        ">",
			Count:        19,
			RawCount:     19,
			CappedCount:  5,
			CappedWeight: 5,
			CapLimited:   true,
		},
	}
	got := diagPathP50Tag(prediction, true)
	if got != "p-15d4n19" {
		t.Fatalf("unexpected PATHP50 diagnostic: %q", got)
	}
	prediction.result = pathreliability.Result{
		Source:    pathreliability.SourceCombined,
		P50DB:     3,
		HasP50:    true,
		MeanDB:    1,
		HasMeanDB: true,
		Glyph:     "=",
		Count:     42,
	}
	if got := diagPathP50Tag(prediction, true); got != "p3d-2n42" {
		t.Fatalf("unexpected positive PATHP50 diagnostic: %q", got)
	}
	prediction.result = pathreliability.Result{
		Source:    pathreliability.SourceCombined,
		MeanDB:    -11,
		HasMeanDB: true,
		Glyph:     "-",
		Count:     7,
	}
	if got := diagPathP50Tag(prediction, true); got != "p?d?n7" {
		t.Fatalf("unexpected missing p50 PATHP50 diagnostic: %q", got)
	}
	if got := diagPathP50Tag(pathPrediction{}, false); got != "p?d?n0" {
		t.Fatalf("unexpected no-prediction PATHP50 diagnostic: %q", got)
	}
}

func TestDiagPathP50TagFitsReportedModeCommentSpace(t *testing.T) {
	prediction := pathPrediction{
		result: pathreliability.Result{
			Source:    pathreliability.SourceCombined,
			P50DB:     -24,
			HasP50:    true,
			MeanDB:    -1,
			HasMeanDB: true,
			Count:     1355,
		},
	}
	tag := diagPathP50Tag(prediction, true)
	if tag != "p-24d23n1355" {
		t.Fatalf("unexpected compact PATHP50 diagnostic: %q", tag)
	}

	sp := spot.NewSpot("N3QE", "AA4PA-#", 3585.60, "RTTY")
	sp.Report = 24
	sp.HasReport = true
	sp.Time = time.Date(2025, time.January, 7, 4, 9, 0, 0, time.UTC)
	sp.DXMetadata.Grid = "FM19"
	sp.Confidence = "S"

	line := sp.FormatDXClusterWithComment(tag)
	if !strings.Contains(line, tag) {
		t.Fatalf("expected compact PATHP50 diagnostic to fit, got %q", line)
	}
	if len(strings.TrimRight(line, "\r\n")) != spot.CurrentDXClusterLayout().LineLength {
		t.Fatalf("expected fixed-width spot line, got len=%d line=%q", len(strings.TrimRight(line, "\r\n")), line)
	}
}

func TestFormatSpotForClientPathP50DiagComment(t *testing.T) {
	requireH3Mappings(t)
	now := time.Date(2025, time.January, 7, 4, 9, 0, 0, time.UTC)
	predictor := newTestPathPredictor()
	server := NewServer(ServerOptions{
		PathPredictor:      predictor,
		PathDisplayEnabled: true,
	}, nil)
	server.nowFn = func() time.Time { return now }
	client := &Client{grid: "FN31"}
	client.setDiagMode(diagModePathP50)

	userCell := pathreliability.EncodeCell("FN31")
	dxCell := pathreliability.EncodeCell("FN32")
	userCoarse := pathreliability.EncodeCoarseCell("FN31")
	dxCoarse := pathreliability.EncodeCoarseCell("FN32")
	receiver := pathreliability.ReceiverIdentityHash("W1AW")
	predictor.UpdateWithReceiverHash(pathreliability.BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -15, 1, now.Add(-10*time.Second), false, receiver)
	predictor.UpdateWithReceiverHash(pathreliability.BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -11, 1, now.Add(-5*time.Second), false, receiver)

	sp := spot.NewSpot("K1ABC", "W1AW", 14074.0, "FT8")
	sp.Time = now
	sp.Band = "20m"
	sp.DXMetadata.Grid = "FN32"
	sp.Confidence = "V"

	line := server.formatSpotForClient(client, sp)
	if !strings.Contains(line, "p-15d") || !strings.Contains(line, "n2") {
		t.Fatalf("expected PATHP50 diagnostic in output, got %q", line)
	}
	if strings.Contains(line, "p+") || strings.Contains(line, "d+") {
		t.Fatalf("expected positive PATHP50 values to omit plus signs, got %q", line)
	}
	if strings.Contains(line, "|") {
		t.Fatalf("expected compact PATHP50 diagnostic without separators, got %q", line)
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
