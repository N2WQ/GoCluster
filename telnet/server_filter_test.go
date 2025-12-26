package telnet

import (
	"strings"
	"testing"

	"dxcluster/filter"
	"dxcluster/spot"
)

func newTestClient() *Client {
	return &Client{
		filter: filter.NewFilter(),
	}
}

func TestPassCommands(t *testing.T) {
	tests := []struct {
		name  string
		cmd   string
		setup func(*Client)
		check func(*testing.T, *filter.Filter)
	}{
		{
			name: "pass band list",
			cmd:  "PASS BAND 20M,40M",
			check: func(t *testing.T, f *filter.Filter) {
				b20 := spot.NormalizeBand("20M")
				b40 := spot.NormalizeBand("40M")
				if !f.Bands[b20] || !f.Bands[b40] {
					t.Fatalf("expected bands 20M and 40M to be enabled")
				}
				if f.AllBands {
					t.Fatalf("AllBands should be false when specific bands are set")
				}
			},
		},
		{
			name: "pass band all",
			cmd:  "PASS BAND ALL",
			check: func(t *testing.T, f *filter.Filter) {
				if !f.AllBands || f.BlockAllBands {
					t.Fatalf("expected AllBands=true and BlockAllBands=false after PASS BAND ALL")
				}
			},
		},
		{
			name: "pass mode list",
			cmd:  "PASS MODE FT8",
			check: func(t *testing.T, f *filter.Filter) {
				if !f.Modes["FT8"] {
					t.Fatalf("expected FT8 mode to be enabled")
				}
				if f.AllModes {
					t.Fatalf("AllModes should be false when a specific mode is set")
				}
			},
		},
		{
			name: "pass source human",
			cmd:  "PASS SOURCE HUMAN",
			check: func(t *testing.T, f *filter.Filter) {
				if !f.Sources["HUMAN"] {
					t.Fatalf("expected HUMAN source to be enabled")
				}
				if f.AllSources {
					t.Fatalf("AllSources should be false when a specific source is set")
				}
			},
		},
		{
			name: "pass source all resets",
			cmd:  "PASS SOURCE ALL",
			setup: func(c *Client) {
				c.filter.SetSource("HUMAN", true)
				c.filter.SetSource("SKIMMER", false)
			},
			check: func(t *testing.T, f *filter.Filter) {
				if !f.AllSources || f.BlockAllSources {
					t.Fatalf("expected AllSources=true and BlockAllSources=false after PASS SOURCE ALL")
				}
				if len(f.Sources) != 0 || len(f.BlockSources) != 0 {
					t.Fatalf("expected PASS SOURCE ALL to clear allow and block sets")
				}
			},
		},
		{
			name: "pass dxcall pattern",
			cmd:  "PASS DXCALL K1*",
			check: func(t *testing.T, f *filter.Filter) {
				if len(f.DXCallsigns) != 1 || f.DXCallsigns[0] != "K1*" {
					t.Fatalf("expected DX callsign pattern K1* to be stored")
				}
			},
		},
		{
			name: "pass decall pattern",
			cmd:  "PASS DECALL W1*",
			check: func(t *testing.T, f *filter.Filter) {
				if len(f.DECallsigns) != 1 || f.DECallsigns[0] != "W1*" {
					t.Fatalf("expected DE callsign pattern W1* to be stored")
				}
			},
		},
		{
			name: "pass confidence list",
			cmd:  "PASS CONFIDENCE P,V",
			check: func(t *testing.T, f *filter.Filter) {
				if f.AllConfidence {
					t.Fatalf("AllConfidence should be false when specific symbols are set")
				}
				if !f.Confidence["P"] || !f.Confidence["V"] {
					t.Fatalf("expected confidence symbols P and V to be enabled")
				}
			},
		},
		{
			name: "pass dx continent",
			cmd:  "PASS DXCONT EU",
			check: func(t *testing.T, f *filter.Filter) {
				if !f.DXContinents["EU"] {
					t.Fatalf("expected DX continent EU to be enabled")
				}
			},
		},
		{
			name: "pass de zone",
			cmd:  "PASS DEZONE 5",
			check: func(t *testing.T, f *filter.Filter) {
				if !f.DEZones[5] {
					t.Fatalf("expected DE zone 5 to be enabled")
				}
			},
		},
		{
			name: "pass dx dxcc",
			cmd:  "PASS DXDXCC 291",
			check: func(t *testing.T, f *filter.Filter) {
				if !f.DXDXCC[291] {
					t.Fatalf("expected DX DXCC 291 to be enabled")
				}
			},
		},
		{
			name: "pass dx grid2 truncates",
			cmd:  "PASS DXGRID2 FN05",
			check: func(t *testing.T, f *filter.Filter) {
				if !f.DXGrid2Prefixes["FN"] {
					t.Fatalf("expected DX grid2 prefix FN to be enabled")
				}
			},
		},
		{
			name: "pass beacon enables",
			cmd:  "PASS BEACON",
			setup: func(c *Client) {
				c.filter.SetBeaconEnabled(false)
			},
			check: func(t *testing.T, f *filter.Filter) {
				if !f.BeaconsEnabled() {
					t.Fatalf("expected beacon delivery to be enabled")
				}
			},
		},
		{
			name: "pass wwv enables",
			cmd:  "PASS WWV",
			setup: func(c *Client) {
				c.filter.SetWWVEnabled(false)
			},
			check: func(t *testing.T, f *filter.Filter) {
				if !f.WWVEnabled() {
					t.Fatalf("expected WWV bulletins to be enabled")
				}
			},
		},
		{
			name: "pass wcy enables",
			cmd:  "PASS WCY",
			setup: func(c *Client) {
				c.filter.SetWCYEnabled(false)
			},
			check: func(t *testing.T, f *filter.Filter) {
				if !f.WCYEnabled() {
					t.Fatalf("expected WCY bulletins to be enabled")
				}
			},
		},
		{
			name: "pass announce enables",
			cmd:  "PASS ANNOUNCE",
			setup: func(c *Client) {
				c.filter.SetAnnounceEnabled(false)
			},
			check: func(t *testing.T, f *filter.Filter) {
				if !f.AnnounceEnabled() {
					t.Fatalf("expected announcements to be enabled")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newTestClient()
			if tt.setup != nil {
				tt.setup(client)
			}
			resp := client.handleFilterCommand(tt.cmd)
			if resp == "" {
				t.Fatalf("expected response for command %q", tt.cmd)
			}
			tt.check(t, client.filter)
		})
	}
}

func TestRejectCommands(t *testing.T) {
	tests := []struct {
		name  string
		cmd   string
		setup func(*Client)
		check func(*testing.T, *filter.Filter)
	}{
		{
			name: "reject band all blocks all bands",
			cmd:  "REJECT BAND ALL",
			check: func(t *testing.T, f *filter.Filter) {
				if !f.BlockAllBands || f.AllBands {
					t.Fatalf("expected BlockAllBands=true and AllBands=false after REJECT BAND ALL")
				}
			},
		},
		{
			name: "reject mode list",
			cmd:  "REJECT MODE FT8",
			check: func(t *testing.T, f *filter.Filter) {
				if !f.BlockModes["FT8"] {
					t.Fatalf("expected FT8 mode to be blocked")
				}
			},
		},
		{
			name: "reject source human",
			cmd:  "REJECT SOURCE HUMAN",
			check: func(t *testing.T, f *filter.Filter) {
				if !f.BlockSources["HUMAN"] {
					t.Fatalf("expected HUMAN source to be blocked")
				}
			},
		},
		{
			name: "reject source skimmer",
			cmd:  "REJECT SOURCE SKIMMER",
			check: func(t *testing.T, f *filter.Filter) {
				if !f.BlockSources["SKIMMER"] {
					t.Fatalf("expected SKIMMER source to be blocked")
				}
			},
		},
		{
			name: "reject confidence all",
			cmd:  "REJECT CONFIDENCE ALL",
			check: func(t *testing.T, f *filter.Filter) {
				if !f.BlockAllConfidence || f.AllConfidence {
					t.Fatalf("expected BlockAllConfidence=true and AllConfidence=false after REJECT CONFIDENCE ALL")
				}
			},
		},
		{
			name: "reject degrid2 list",
			cmd:  "REJECT DEGRID2 FN",
			check: func(t *testing.T, f *filter.Filter) {
				if !f.BlockDEGrid2["FN"] {
					t.Fatalf("expected DE grid2 prefix FN to be blocked")
				}
			},
		},
		{
			name: "reject beacon disables",
			cmd:  "REJECT BEACON",
			check: func(t *testing.T, f *filter.Filter) {
				if f.BeaconsEnabled() {
					t.Fatalf("expected beacon delivery to be disabled")
				}
			},
		},
		{
			name: "reject wwv disables",
			cmd:  "REJECT WWV",
			check: func(t *testing.T, f *filter.Filter) {
				if f.WWVEnabled() {
					t.Fatalf("expected WWV bulletins to be disabled")
				}
			},
		},
		{
			name: "reject wcy disables",
			cmd:  "REJECT WCY",
			check: func(t *testing.T, f *filter.Filter) {
				if f.WCYEnabled() {
					t.Fatalf("expected WCY bulletins to be disabled")
				}
			},
		},
		{
			name: "reject announce disables",
			cmd:  "REJECT ANNOUNCE",
			check: func(t *testing.T, f *filter.Filter) {
				if f.AnnounceEnabled() {
					t.Fatalf("expected announcements to be disabled")
				}
			},
		},
		{
			name: "reject dxcall clears patterns",
			cmd:  "REJECT DXCALL",
			setup: func(c *Client) {
				c.filter.AddDXCallsignPattern("K1*")
			},
			check: func(t *testing.T, f *filter.Filter) {
				if len(f.DXCallsigns) != 0 {
					t.Fatalf("expected DX callsign patterns cleared")
				}
			},
		},
		{
			name: "reject decall clears patterns",
			cmd:  "REJECT DECALL",
			setup: func(c *Client) {
				c.filter.AddDECallsignPattern("W1*")
			},
			check: func(t *testing.T, f *filter.Filter) {
				if len(f.DECallsigns) != 0 {
					t.Fatalf("expected DE callsign patterns cleared")
				}
			},
		},
		{
			name: "reject all resets filters",
			cmd:  "REJECT ALL",
			check: func(t *testing.T, f *filter.Filter) {
				if !f.AllBands || !f.AllModes || !f.AllConfidence || !f.AllSources {
					t.Fatalf("expected REJECT ALL to reset filter to defaults")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newTestClient()
			if tt.setup != nil {
				tt.setup(client)
			}
			resp := client.handleFilterCommand(tt.cmd)
			if resp == "" {
				t.Fatalf("expected response for command %q", tt.cmd)
			}
			tt.check(t, client.filter)
		})
	}
}

func TestLegacySyntaxReturnsHint(t *testing.T) {
	client := newTestClient()
	resp := client.handleFilterCommand("SET/FILTER BAND 20M")
	if !strings.Contains(resp, "syntax changed") {
		t.Fatalf("expected legacy syntax hint, got: %q", resp)
	}
}

func TestInvalidFilterCommandShowsHelpHint(t *testing.T) {
	client := newTestClient()
	resp := client.handleFilterCommand("PASS")
	if !strings.Contains(strings.ToLower(resp), "help") {
		t.Fatalf("expected help hint in response, got: %q", resp)
	}
}

func TestBroadcastWWVRespectsFilter(t *testing.T) {
	server := &Server{
		clients: make(map[string]*Client),
	}

	allow := &Client{
		callsign:     "ALLOW",
		bulletinChan: make(chan bulletin, 1),
		filter:       filter.NewFilter(),
	}
	block := &Client{
		callsign:     "BLOCK",
		bulletinChan: make(chan bulletin, 1),
		filter:       filter.NewFilter(),
	}
	block.filter.SetWWVEnabled(false)

	server.clients["ALLOW"] = allow
	server.clients["BLOCK"] = block

	server.BroadcastWWV("WWV", "WWV de TEST <00> : SFI=1 A=1 K=1")

	select {
	case <-allow.bulletinChan:
	default:
		t.Fatalf("expected bulletin delivered to allowed client")
	}
	select {
	case <-block.bulletinChan:
		t.Fatalf("did not expect bulletin delivered to blocked client")
	default:
	}
}

func TestBroadcastAnnouncementRespectsFilter(t *testing.T) {
	server := &Server{
		clients: make(map[string]*Client),
	}

	allow := &Client{
		callsign:     "ALLOW",
		bulletinChan: make(chan bulletin, 1),
		filter:       filter.NewFilter(),
	}
	block := &Client{
		callsign:     "BLOCK",
		bulletinChan: make(chan bulletin, 1),
		filter:       filter.NewFilter(),
	}
	block.filter.SetAnnounceEnabled(false)

	server.clients["ALLOW"] = allow
	server.clients["BLOCK"] = block

	server.BroadcastAnnouncement("To ALL de TEST: hello")

	select {
	case <-allow.bulletinChan:
	default:
		t.Fatalf("expected announcement delivered to allowed client")
	}
	select {
	case <-block.bulletinChan:
		t.Fatalf("did not expect announcement delivered to blocked client")
	default:
	}
}
