package peer

import (
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	"dxcluster/config"
)

func TestPC92ColonFloodRejectsBeforeTokenAllocation(t *testing.T) {
	raw := "5W1AAA" + strings.Repeat(":", 65000)
	previous := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previous)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := DecodePC92Entry(raw)
	runtime.ReadMemStats(&after)
	if err == nil {
		t.Fatal("invalid entry shape accepted")
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 4096 {
		t.Fatalf("colon flood allocated%d bytes before rejection", allocated)
	}
}

func TestPC18WordFloodWithinFrameScratchCharge(t *testing.T) {
	banner := strings.Repeat("X ", 32000) + " Build: 0.633"
	frame, err := ParseFrame("PC18^" + banner + "^5457^")
	if err != nil {
		t.Fatal(err)
	}
	charge, err := frameParseCharge(frame.Raw)
	if err != nil {
		t.Fatal(err)
	}
	s := &session{peer: PeerEndpoint{family: config.PeeringPeerFamilyDXSpider}}
	previous := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previous)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err = s.acceptPC18(frame)
	runtime.ReadMemStats(&after)
	if err != nil || s.remoteVersion != "5457" || s.remoteBuild != "633" {
		t.Fatalf("PC18 result=%v version=%q", err, s.remoteVersion)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > uint64(charge) {
		t.Fatalf("PC18 word flood allocated%d beyond reserved%d", allocated, charge)
	}
}

func TestPC18MetadataReplacementPreservesBoundedOversizeMarkers(t *testing.T) {
	s := &session{peer: PeerEndpoint{family: config.PeeringPeerFamilyDXSpider}}
	for _, wire := range []string{
		"PC18^DXSpider Build: " + strings.Repeat("1", 65000) + "^5457^",
		"PC18^DXSpider^" + strings.Repeat("2", 65000) + "^",
	} {
		frame, err := ParseFrame(wire)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.acceptPC18(frame); err != nil {
			t.Fatal(err)
		}
	}
	if s.remoteVersion != "" || s.remoteBuild != "" || !s.remoteVersionTooLong || !s.remoteBuildTooLong || s.remotePublicationMetadataOK() {
		t.Fatal("repeated PC18 lost oversized presence or retained large numeric strings")
	}
	if err := s.acceptPC18(&Frame{Type: "PC18", Fields: []string{"DXSpider", "5457"}}); err != nil {
		t.Fatal(err)
	}
	if s.remoteVersion != "5457" || s.remoteVersionTooLong || !s.remoteBuildTooLong || s.remotePublicationMetadataOK() {
		t.Fatal("version replacement erased an absent oversized Build or failed to clear its own marker")
	}
	if err := s.acceptPC18(&Frame{Type: "PC18", Fields: []string{"DXSpider Build: 633", "5457"}}); err != nil {
		t.Fatal(err)
	}
	if s.remoteBuild != "633" || !s.remotePublicationMetadataOK() {
		t.Fatal("later valid metadata did not restore publication eligibility")
	}
}

func TestPC18StreamingFamilyNormalization(t *testing.T) {
	for _, banner := range []string{" CC\tCluster\nVersion: 1.0 ", "CCCluster\u00a0Version:1.0", "myCC\tcluster\u2003version:1.0"} {
		if !isCCClusterBanner(&Frame{Type: "PC18", Fields: []string{banner}}) {
			t.Errorf("family normalization changed for%q", banner)
		}
	}
}
