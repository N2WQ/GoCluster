package telnet

import (
	"fmt"
	"testing"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestNearbyEndpointsLiveAndCapturedHistory(t *testing.T) {
	requireH3Mappings(t)
	for _, frequency := range []float64{14020, 1820, 3520, 5355} {
		for _, test := range []struct {
			dx, de    string
			blockMode bool
			want      bool
		}{
			{"FN31", "", false, true}, {"", "FN31", false, true},
			{"FN31", "BAD", false, true}, {"BAD", "FN31", false, true},
			{"", "", false, false}, {"EM10", "", false, false},
			{"", "EM10", false, false}, {"EM10", "EM10", false, false},
			{"FN31", "", true, false}, {"", "FN31", true, false},
		} {
			t.Run(fmt.Sprintf("%.0f/%s/%s/blockMode=%v", frequency, test.dx, test.de, test.blockMode), func(t *testing.T) {
				server := &Server{}
				client := &Client{callsign: "VE3ABC-7", filter: filter.NewFilter(),
					spotChan: make(chan *spotEnvelope, 1), done: make(chan struct{})}
				client.filter.SetDXContinent("EU", true)
				if err := client.filter.EnableNearby(pathreliability.EncodeCell("FN31"), pathreliability.EncodeCoarseCell("FN31")); err != nil {
					t.Fatal(err)
				}
				if test.blockMode {
					client.filter.SetMode("CW", false)
				}
				s := spot.NewSpotNormalized("W6ABC", "N0XYZ", frequency, "CW")
				s.DXMetadata = spot.CallMetadata{Grid: test.dx, Continent: "NA"}
				s.DEMetadata = spot.CallMetadata{Grid: test.de, Continent: "NA"}
				if isSelfMatch(s, client.callsign) {
					t.Fatal("consumer control accidentally uses the own-call exemption")
				}
				snapshot := client.captureHistoryFilter()
				if got := snapshot.matches(server, s); got != test.want {
					t.Fatalf("captured history = %v, want %v", got, test.want)
				}
				server.deliverJob(broadcastJob{spot: s, clients: []*Client{client},
					allowFast: true, allowMed: true, allowSlow: true})
				select {
				case envelope := <-client.spotChan:
					if !test.want || envelope.spot != s {
						t.Fatal("live delivery queued a refused or different spot")
					}
				default:
					if test.want {
						t.Fatal("live delivery did not queue the matching endpoint")
					}
				}
				// The history request retains its own rules after a later command.
				client.filter.SetMode("CW", test.blockMode)
				if test.want && client.filter.Matches(s) {
					t.Fatal("live mode edit did not change the matching control")
				}
				if test.blockMode && !client.filter.Matches(s) {
					t.Fatal("live mode edit did not re-enable the refused matching control")
				}
				if got := snapshot.matches(server, s); got != test.want {
					t.Fatal("live mode edit changed the captured history predicate")
				}
			})
		}
	}
}
