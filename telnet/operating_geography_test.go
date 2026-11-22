package telnet

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/spot"
)

func TestOperatingGeographyAcrossLoginLiveHistoryAndPreset(t *testing.T) {
	fixture := func(portableEntity int) *cty.CTYDatabase {
		t.Helper()
		var text strings.Builder
		text.WriteString("<plist><dict>")
		for _, row := range []struct {
			prefix, continent string
			entity            int
		}{{"N", "NA", 291}, {"VE3", "NA", portableEntity}, {"DL", "EU", 230}} {
			fmt.Fprintf(&text, "<key>%s</key><dict><key>Country</key><string>Fixture</string><key>Prefix</key><string>%s</string><key>Continent</key><string>%s</string><key>ADIF</key><integer>%d</integer></dict>",
				row.prefix, row.prefix, row.continent, row.entity)
		}
		text.WriteString("</dict></plist>")
		db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(text.String()))
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	var current atomic.Pointer[cty.CTYDatabase]
	current.Store(fixture(1))
	var lookups atomic.Int64
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.CTYLookup = func() *cty.CTYDatabase { lookups.Add(1); return current.Load() }
		opts.USLicenseCheck = func(call string) bool { return call == "N2WQ" }
	})
	command := func(conn net.Conn, line, want string) string {
		t.Helper()
		if _, err := io.WriteString(conn, line+"\r\n"); err != nil {
			t.Fatal(err)
		}
		return readUntilContains(t, conn, want, 2*time.Second)
	}
	login := func(call string) (net.Conn, *Client, func()) {
		t.Helper()
		_, conn, done := startHandshakeTranscriptSession(t, s)
		closed := false
		closeSession := func() {
			if !closed {
				closed = true
				closeHandshakeTranscriptSession(t, conn, done)
			}
		}
		t.Cleanup(closeSession)
		readUntilContains(t, conn, "login: ", 2*time.Second)
		command(conn, call, "UTC>")
		s.clientsMutex.RLock()
		client := s.clients[call]
		s.clientsMutex.RUnlock()
		if client == nil {
			t.Fatalf("login %s was not registered", call)
		}
		return conn, client, closeSession
	}
	const expression = "DECONT=MYCONT AND DXDXCC=MYDXCC"
	canada := spot.NewSpot("VE3AAA", "VE3BBB", 14020, "CW")
	canada.DXMetadata.ADIF, canada.DEMetadata.Continent = 1, "NA"
	us := spot.NewSpot("W1AAA", "W1BBB", 14021, "CW")
	us.DXMetadata.ADIF, us.DEMetadata.Continent = 291, "NA"
	germany := spot.NewSpot("DL1AAA", "DL1BBB", 14022, "CW")
	germany.DXMetadata.ADIF, germany.DEMetadata.Continent = 230, "EU"
	captureHistory := func(client *Client) historyFilterSnapshot {
		t.Helper()
		release, err := s.acquireConfiguration(client, false, false, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		return client.captureHistoryFilter()
	}
	assertMatches := func(conn net.Conn, client *Client, accepts, rejects *spot.Spot) {
		t.Helper()
		before := lookups.Load()
		history := captureHistory(client)
		if !history.matches(s, accepts) || history.matches(s, rejects) {
			t.Fatalf("history for %s should select %s and exclude %s", client.callsign, accepts.DXCall, rejects.DXCall)
		}
		for _, value := range []*spot.Spot{rejects, accepts} {
			s.deliverJob(broadcastJob{spot: value, clients: []*Client{client}, allowFast: true, allowMed: true, allowSlow: true})
		}
		if output := readUntilContains(t, conn, accepts.DXCall, 2*time.Second); strings.Contains(output, rejects.DXCall) {
			t.Fatalf("live delivery leaked excluded spot: %q", output)
		}
		if got := lookups.Load(); got != before {
			t.Fatalf("spot matching changed CTY lookup count from %d to %d", before, got)
		}
	}

	conn, client, closeSession := login("N2WQ/VE3")
	command(conn, "PASS RULE 0 "+expression, "Saved and active.")
	command(conn, "SAVE PRESET LOCAL", "Saved preset LOCAL (GO)")
	command(conn, "RESET FILTER", "Personal rules cleared")
	command(conn, "LOAD PRESET LOCAL", "Loaded preset LOCAL (GO)")
	current.Store(fixture(291))
	assertMatches(conn, client, canada, us)
	closeSession()

	conn, client, closeSession = login("N2WQ/VE3")
	assertMatches(conn, client, us, canada)
	command(conn, "SHOW FILTER RULE", expression)
	closeSession()

	conn, client, closeSession = login("DL/N2WQ")
	command(conn, "PASS RULE 0 "+expression, "Saved and active.")
	assertMatches(conn, client, germany, us)
	// A spot with the right DXCC and a report from the wrong continent must fail.
	wrongContinent := spot.NewSpot("DL1CCC", "W1CCC", 14023, "CW")
	wrongContinent.DXMetadata.ADIF, wrongContinent.DEMetadata.Continent = 230, "NA"
	if captureHistory(client).matches(s, wrongContinent) {
		t.Fatal("portable login did not use its operating continent")
	}
	closeSession()

	current.Store(fixture(1))
	conn, client, closeSession = login("N2WQ/VE3-01")
	command(conn, "LOAD PRESET LOCAL", "Loaded preset LOCAL (GO)")
	assertMatches(conn, client, canada, us)
	closeSession()

	current.Store(nil)
	conn, client, _ = login("N2WQ/VE3")
	if client.operatingGeography != (filter.OperatingGeography{}) || captureHistory(client).matches(s, us) {
		t.Fatal("CTY outage did not leave both operands unknown")
	}
	command(conn, "SHOW FILTER RULE", expression)
}
