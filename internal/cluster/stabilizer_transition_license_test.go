//go:build !qualification

package cluster

import (
	"strings"
	"testing"
	"time"

	"dxcluster/buffer"
	"dxcluster/cty"
	"dxcluster/stats"
)

func TestStabilizerTransitionExcludesEarlierLicenseAbort(t *testing.T) {
	replacementLicenseFixture(t, false)
	db := loadIngestCTY(t)
	p := newDeliveryTestPipeline(buffer.NewRingBuffer(4), nil, nil)
	p.tracker = stats.NewTracker()
	p.ctyLookup = func() *cty.CTYDatabase { return db }
	s := newDeliveryTestSpot("K1ABD", "N0AAA", time.Now().UTC())
	if !applyLicenseGate(s, db, nil, nil) {
		t.Fatal("fixture did not exercise a real license rejection")
	}
	p.handleStabilizerRelease(&telnetStabilizerEnvelope{spot: s, entryGlyph: "S", checksCompleted: 2})
	assertStabilizerTransition(t, p.tracker, 1, 0, 0, 0)
	if p.buf.GetCount() != 0 || p.tracker.StabilizerReleasedDelayed() != 0 || p.tracker.StabilizerSuppressedTimeout() != 0 {
		t.Fatal("earlier license abort was treated as a terminal stabilizer outcome")
	}
}

func TestStabilizerTransitionRejectedCorrectionEndsAsB(t *testing.T) {
	cfg := replacementLicenseConfig(t)
	at := time.Now().UTC().Truncate(time.Second).Add(-time.Second)
	r, key := replacementLicensePopulation(t, cfg, at)
	// The original and receiver resolve, but the proposed winner K1ABD does not.
	// ADIF 230 keeps unrelated national license availability out of this fixture.
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(`<plist><dict>
<key>K1ABC</key><dict><key>Country</key><string>Germany</string><key>Prefix</key><string>K1ABC</string><key>ADIF</key><integer>230</integer><key>Continent</key><string>EU</string></dict>
<key>N0AAA</key><dict><key>Country</key><string>Germany</string><key>Prefix</key><string>N0AAA</string><key>ADIF</key><integer>230</integer><key>Continent</key><string>EU</string></dict>
</dict></plist>`))
	if err != nil {
		t.Fatal(err)
	}
	p := newDeliveryTestPipeline(buffer.NewRingBuffer(4), nil, nil)
	p.tracker = stats.NewTracker()
	p.correctionCfg = cfg
	p.correctionCfg.InvalidAction = "broadcast"
	p.ctyLookup = func() *cty.CTYDatabase { return db }
	p.stabilizerEnabled = true
	p.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 4)
	t.Cleanup(p.telnetStabilizer.Stop)
	s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
	s.Confidence = "S"
	if _, deliver := p.resolveDeliveryPlan(&outputSpotContext{spot: s, stabilizerResolverKey: key, hasStabilizerResolverKey: true}); deliver {
		t.Fatal("original S fixture did not enter stabilizer")
	}
	p.telnetStabilizer.Start()
	envelope := receiveStabilizerMatrixEnvelope(t, p.telnetStabilizer)
	p.signalResolver = r
	p.handleStabilizerRelease(envelope)
	if envelope.spot.Confidence != "B" || envelope.spot.DXCallNorm != "K1ABC" ||
		p.tracker.CorrectionDecisionReasons()[resolverDecisionCTYMiss] != 1 || p.tracker.CallCorrections() != 0 || p.buf.GetCount() != 1 {
		t.Fatalf("fixture did not reject proposed correction and deliver original as B: glyph=%s call=%s reasons=%v",
			envelope.spot.Confidence, envelope.spot.DXCallNorm, p.tracker.CorrectionDecisionReasons())
	}
	assertStabilizerTransition(t, p.tracker, 1, 5, 1, 1)
}
