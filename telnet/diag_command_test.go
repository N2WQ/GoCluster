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
	if !strings.Contains(resp, "Usage: SET DIAG <OFF|DEDUPE|SOURCE|CONF|PATH|MODE>") {
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
