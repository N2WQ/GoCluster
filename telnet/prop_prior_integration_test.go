package telnet

import (
	"strings"
	"testing"
	"time"

	"dxcluster/internal/propprior"
	"dxcluster/pathreliability"
)

func TestShowPropWithPropPriorFallbackInstant24hAndHeader(t *testing.T) {
	requireH3Mappings(t)
	model, err := propprior.LoadEmbedded()
	if err != nil {
		t.Fatalf("propprior.LoadEmbedded: %v", err)
	}
	cfg := pathreliability.DefaultConfig()
	cfg.VOACAPFallback.Enabled = false
	fbCfg := pathreliability.DefaultPropPriorFallbackConfig()
	cfg.PropPriorFallback = &fbCfg
	fb, err := pathreliability.NewPropPriorFallback(cfg, model, propprior.NewSpaceWeatherStore())
	if err != nil {
		t.Fatalf("NewPropPriorFallback: %v", err)
	}

	now := time.Date(2026, 3, 21, 16, 15, 0, 0, time.UTC)
	server := &Server{
		pathPredictor:      pathreliability.NewPredictor(cfg, []string{"80m", "40m", "20m", "15m", "10m"}),
		pathClosedFallback: fb,
		noiseModel:         cfg.NoiseModel(),
		nowFn:              func() time.Time { return now },
	}
	client := testShowPropClient("FN20", "QUIET")

	resp, handled := server.handleShowPropCommand(client, "SHOW PROP JO22 15m CW")
	if !handled {
		t.Fatalf("SHOW PROP not handled")
	}
	if !strings.Contains(resp, "engine=propprior") {
		t.Fatalf("expected engine=propprior in header:\n%s", resp)
	}
	if strings.Contains(resp, "ssn=") {
		t.Fatalf("propprior SHOW PROP header must omit ssn=:\n%s", resp)
	}
	if strings.Contains(resp, "Still computing") {
		t.Fatalf("propprior SHOW PROP must not delay:\n%s", resp)
	}
	if !strings.Contains(resp, "hours=24") {
		t.Fatalf("expected hours=24 in header:\n%s", resp)
	}

	// All-band outlook returns immediately across all configured HF bands.
	respAll, handled := server.handleShowPropCommand(client, "SHOW PROP JO22 CW")
	if !handled {
		t.Fatalf("SHOW PROP all-band not handled")
	}
	if !strings.Contains(respAll, "engine=propprior") || strings.Contains(respAll, "Still computing") {
		t.Fatalf("unexpected all-band propprior SHOW PROP response:\n%s", respAll)
	}
	// Unsupported 160m band reports SHOW PROP supported HF bands cleanly.
	resp160, handled := server.handleShowPropCommand(client, "SHOW PROP JO22 160m CW")
	if !handled || !strings.Contains(resp160, "Unsupported SHOW PROP band: 160m") {
		t.Fatalf("unexpected 160m SHOW PROP response:\n%s", resp160)
	}
}

func TestDiagPathTagsPropPriorDistinctFromVOACAP(t *testing.T) {
	cases := []struct {
		name string
		res  pathreliability.Result
		want string
	}{
		{
			name: "propprior_closed",
			res:  pathreliability.Result{Source: pathreliability.SourceVOACAPClosed, PropPrior: true, VOACAPFT8SNRDB: -31, VOACAPHourUTC: 16},
			want: "ppcap|-31|h16",
		},
		{
			name: "propprior_beacon_closed",
			res:  pathreliability.Result{Source: pathreliability.SourceVOACAPClosed, PropPrior: true, BeaconRX: true, VOACAPFT8SNRDB: -31, VOACAPHourUTC: 16},
			want: "bppcap|-31|h16",
		},
		{
			name: "propprior_aligned",
			res:  pathreliability.Result{Source: pathreliability.SourceVOACAPAligned, PropPrior: true, P50DB: -12, VOACAPFT8SNRDB: -13, VOACAPHourUTC: 18},
			want: "ppaln|-12/-13h18",
		},
		{
			name: "propprior_beacon_aligned",
			res:  pathreliability.Result{Source: pathreliability.SourceVOACAPAligned, PropPrior: true, BeaconRX: true, P50DB: -12, VOACAPFT8SNRDB: -13, VOACAPHourUTC: 18},
			want: "bppaln|-12/-13h18",
		},
		{
			name: "propprior_sparse_upgrade",
			res:  pathreliability.Result{Source: pathreliability.SourceVOACAPSparseUpgrade, PropPrior: true, P50DB: -15, VOACAPFT8SNRDB: -9, VOACAPReqSNRReliability: 0.78, VOACAPHasReqSNRReliability: true},
			want: "ppup|-15/-9r78",
		},
		{
			name: "propprior_beacon_sparse_upgrade",
			res:  pathreliability.Result{Source: pathreliability.SourceVOACAPSparseUpgrade, PropPrior: true, BeaconRX: true, P50DB: -15, VOACAPFT8SNRDB: -9, VOACAPReqSNRReliability: 0.78, VOACAPHasReqSNRReliability: true},
			want: "bppup|-15/-9r78",
		},
		{
			name: "propprior_open",
			res:  pathreliability.Result{Source: pathreliability.SourceVOACAPOpen, PropPrior: true, VOACAPFT8SNRDB: -8, VOACAPReqSNRReliability: 0.86, VOACAPHasReqSNRReliability: true, VOACAPHourUTC: 14},
			want: "ppop|-8r86h14",
		},
		{
			name: "propprior_beacon_open",
			res:  pathreliability.Result{Source: pathreliability.SourceVOACAPOpen, PropPrior: true, BeaconRX: true, VOACAPFT8SNRDB: -8, VOACAPReqSNRReliability: 0.86, VOACAPHasReqSNRReliability: true, VOACAPHourUTC: 14},
			want: "bppop|-8r86h14",
		},
		{
			name: "voacap_closed_unchanged",
			res:  pathreliability.Result{Source: pathreliability.SourceVOACAPClosed, PropPrior: false, VOACAPFT8SNRDB: -31, VOACAPHourUTC: 16, VOACAPSSN: 112},
			want: "vcap|-31|h16|s112",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := diagPathTag(pathPrediction{result: tc.res}, true)
			if got != tc.want {
				t.Fatalf("diagPathTag = %q, want %q", got, tc.want)
			}
		})
	}
}
