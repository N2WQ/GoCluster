package cluster

import (
	"strings"
	"testing"
	"time"

	"dxcluster/config"
)

type categoryTestEmitter struct {
	streams []uint8
	lines   []string
}

func (e *categoryTestEmitter) Emit(stream uint8, _ time.Time, p []byte) bool {
	e.streams = append(e.streams, stream)
	e.lines = append(e.lines, string(p))
	return true
}

func TestUnifiedLogCategoriesRouteBeforeDedupe(t *testing.T) {
	e := &categoryTestEmitter{}
	cfg := config.LoggingConfig{
		DroppedCalls:  config.DroppedCallLoggingConfig{Enabled: true, BadDEDX: true, DedupeWindowSeconds: 60},
		LoginAttempts: config.EventFileLoggingConfig{Enabled: true, DedupeWindowSeconds: 60},
	}
	dropped := newUnifiedDroppedCallLogger(cfg.DroppedCalls, e)
	events := newUnifiedEventFileLogger(cfg, e)
	for range 2 {
		dropped.LogBadCall("rbn", "DX", "bad", "N0CALL", "AA1AA", "N0CALL", "CW", "none")
		events.LogLoginAttempt(eventLogField{key: "peer", value: "N0CALL"})
	}
	if len(e.streams) != 4 || e.streams[0] != streamDroppedBadCall || e.streams[1] != streamLoginAttempts || e.streams[2] != streamDroppedBadCall || e.streams[3] != streamLoginAttempts {
		t.Fatalf("unexpected category routing: %v", e.streams)
	}
	if !strings.Contains(e.lines[0], "source=rbn role=DX reason=bad call=N0CALL") || e.lines[1] != "peer=N0CALL" {
		t.Fatalf("category format changed: %v", e.lines)
	}
	if dropped.noLicense != nil || dropped.harmonics != nil || events.ingestConnections != nil {
		t.Fatal("disabled category was enabled")
	}
}
