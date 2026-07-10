package config

import (
	"fmt"
	"strings"
	"testing"
)

func TestHumanTelnetLoadsLegacyAndOrderedSequence(t *testing.T) {
	t.Run("legacy mapping", func(t *testing.T) {
		dir := testConfigDir(t)
		writeTestConfigOverlay(t, dir, "ingest.yaml", `
human_telnet:
  enabled: true
  host: " legacy.example.invalid "
  port: 7300
  callsign: " n0call-9 "
  name: "  MiXeD-1  "
  telnet_transport: " ZIUTEK "
  keep_ssid_suffix: true
  slot_buffer: 1
  keepalive_seconds: 0
`)

		cfg, _, err := LoadWithDiagnostics(dir)
		if err != nil {
			t.Fatalf("LoadWithDiagnostics() error: %v", err)
		}
		if len(cfg.HumanTelnet) != 1 {
			t.Fatalf("human_telnet entries = %d, want 1", len(cfg.HumanTelnet))
		}
		got := cfg.HumanTelnet[0]
		if got.Name != "MiXeD-1" {
			t.Fatalf("name = %q, want case-preserved trimmed name", got.Name)
		}
		if got.Host != "legacy.example.invalid" || got.Callsign != "N0CALL-9" {
			t.Fatalf("endpoint/login = %q/%q, want trimmed normalized values", got.Host, got.Callsign)
		}
		if got.TelnetTransport != "ziutek" {
			t.Fatalf("telnet_transport = %q, want ziutek", got.TelnetTransport)
		}
		if got.KeepaliveSec != 0 || got.SlotBuffer != 1 {
			t.Fatalf("sentinels = keepalive:%d slot:%d, want 0/1", got.KeepaliveSec, got.SlotBuffer)
		}
	})

	t.Run("ordered sequence", func(t *testing.T) {
		dir := testConfigDir(t)
		writeHumanTelnetList(t, dir,
			renderHumanTelnetEntry("Zulu", "zulu.example.invalid", 7300, "N0CALL-1", " NATIVE ", false, 100, 0),
			renderHumanTelnetEntry("Alpha", "alpha.example.invalid", 7301, "N0CALL-2", " ZIUTEK ", true, 200, 10),
			renderHumanTelnetEntry("Mike", "mike.example.invalid", 7302, "N0CALL-3", " Native ", false, 300, 20),
		)

		cfg, _, err := LoadWithDiagnostics(dir)
		if err != nil {
			t.Fatalf("LoadWithDiagnostics() error: %v", err)
		}
		got := make([]string, 0, len(cfg.HumanTelnet))
		for _, entry := range cfg.HumanTelnet {
			got = append(got, entry.Name)
		}
		if strings.Join(got, ",") != "Zulu,Alpha,Mike" {
			t.Fatalf("human_telnet order = %v, want YAML order", got)
		}
		transports := []string{
			cfg.HumanTelnet[0].TelnetTransport,
			cfg.HumanTelnet[1].TelnetTransport,
			cfg.HumanTelnet[2].TelnetTransport,
		}
		if strings.Join(transports, ",") != "native,ziutek,native" {
			t.Fatalf("human_telnet transports = %v, want every entry normalized", transports)
		}
	})
}

func TestHumanTelnetRegistryShapeValidation(t *testing.T) {
	tests := []struct {
		name string
		edit func(*testing.T, string)
		want string
	}{
		{
			name: "missing",
			edit: func(t *testing.T, dir string) {
				removeTestConfigKey(t, dir, "ingest.yaml", "human_telnet")
			},
			want: `required YAML setting "human_telnet" is missing`,
		},
		{
			name: "null",
			edit: func(t *testing.T, dir string) {
				writeTestConfigOverlay(t, dir, "ingest.yaml", "human_telnet: null\n")
			},
			want: `required YAML setting "human_telnet" must not be null`,
		},
		{
			name: "empty mapping",
			edit: func(t *testing.T, dir string) {
				writeTestConfigOverlay(t, dir, "ingest.yaml", "human_telnet: {}\n")
			},
			want: `required YAML setting "human_telnet[0].enabled" is missing`,
		},
		{
			name: "scalar",
			edit: func(t *testing.T, dir string) {
				writeTestConfigOverlay(t, dir, "ingest.yaml", "human_telnet: invalid\n")
			},
			want: `required YAML setting "human_telnet" must be a mapping or sequence`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := testConfigDir(t)
			tt.edit(t, dir)
			_, _, err := LoadWithDiagnostics(dir)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("LoadWithDiagnostics() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestHumanTelnetRequiresCompleteEntriesIncludingDisabled(t *testing.T) {
	t.Run("missing field", func(t *testing.T) {
		dir := testConfigDir(t)
		writeTestConfigOverlay(t, dir, "ingest.yaml", `
human_telnet:
  - enabled: false
    host: "one.example.invalid"
    port: 7300
    callsign: "N0CALL-1"
    name: "ONE"
    telnet_transport: "native"
    keep_ssid_suffix: true
    slot_buffer: 1000
    keepalive_seconds: 0
  - enabled: false
    host: "two.example.invalid"
    port: 7300
    callsign: "N0CALL-2"
    name: "TWO"
    telnet_transport: "native"
    slot_buffer: 1000
    keepalive_seconds: 0
`)

		_, _, err := LoadWithDiagnostics(dir)
		want := `required YAML setting "human_telnet[1].keep_ssid_suffix" is missing`
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("LoadWithDiagnostics() error = %v, want containing %q", err, want)
		}
	})

	t.Run("null field", func(t *testing.T) {
		dir := testConfigDir(t)
		writeTestConfigOverlay(t, dir, "ingest.yaml", `
human_telnet:
  - enabled: false
    host: null
    port: 7300
    callsign: "N0CALL-1"
    name: "ONE"
    telnet_transport: "native"
    keep_ssid_suffix: true
    slot_buffer: 1000
    keepalive_seconds: 0
`)

		_, _, err := LoadWithDiagnostics(dir)
		want := `required YAML setting "human_telnet[0].host" must not be null`
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("LoadWithDiagnostics() error = %v, want containing %q", err, want)
		}
	})
}

func TestHumanTelnetUnknownKeysRemainWarnings(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "legacy mapping",
			body: `
human_telnet:
  enabled: false
  host: "legacy.example.invalid"
  port: 7300
  callsign: "N0CALL-1"
  name: "LEGACY"
  telnet_transport: "native"
  keep_ssid_suffix: true
  slot_buffer: 1000
  keepalive_seconds: 0
  mystery: 1
`,
			want: "human_telnet[0].mystery",
		},
		{
			name: "sequence",
			body: `
human_telnet:
  - enabled: false
    host: "one.example.invalid"
    port: 7300
    callsign: "N0CALL-1"
    name: "ONE"
    telnet_transport: "native"
    keep_ssid_suffix: true
    slot_buffer: 1000
    keepalive_seconds: 0
  - enabled: false
    host: "two.example.invalid"
    port: 7300
    callsign: "N0CALL-2"
    name: "TWO"
    telnet_transport: "native"
    keep_ssid_suffix: true
    slot_buffer: 1000
    keepalive_seconds: 0
    mystery: 1
`,
			want: "human_telnet[1].mystery",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := testConfigDir(t)
			writeTestConfigOverlay(t, dir, "ingest.yaml", tt.body)
			_, diagnostics, err := LoadWithDiagnostics(dir)
			if err != nil {
				t.Fatalf("LoadWithDiagnostics() error: %v", err)
			}
			if !containsDiagnostic(diagnostics.Warnings, tt.want) {
				t.Fatalf("warnings = %#v, want indexed path %q", diagnostics.Warnings, tt.want)
			}
			if containsDiagnostic(diagnostics.Errors, tt.want) {
				t.Fatalf("unknown key unexpectedly fatal: %#v", diagnostics.Errors)
			}
		})
	}
}

func TestHumanTelnetEntryCountBounds(t *testing.T) {
	for _, count := range []int{0, 1, 64} {
		t.Run(fmt.Sprintf("accepts %d", count), func(t *testing.T) {
			dir := testConfigDir(t)
			entries := make([]string, 0, count)
			for i := 0; i < count; i++ {
				entries = append(entries, renderHumanTelnetEntry(
					fmt.Sprintf("SOURCE-%d", i),
					fmt.Sprintf("source%d.example.invalid", i),
					7300,
					fmt.Sprintf("N0CALL-%d", i),
					"native",
					false,
					1,
					0,
				))
			}
			writeHumanTelnetList(t, dir, entries...)
			cfg, _, err := LoadWithDiagnostics(dir)
			if err != nil {
				t.Fatalf("LoadWithDiagnostics() error: %v", err)
			}
			if len(cfg.HumanTelnet) != count {
				t.Fatalf("human_telnet entries = %d, want %d", len(cfg.HumanTelnet), count)
			}
		})
	}

	t.Run("rejects 65", func(t *testing.T) {
		dir := testConfigDir(t)
		entries := make([]string, 0, 65)
		for i := 0; i < 65; i++ {
			entries = append(entries, renderHumanTelnetEntry(
				fmt.Sprintf("SOURCE-%d", i),
				fmt.Sprintf("source%d.example.invalid", i),
				7300,
				fmt.Sprintf("N0CALL-%d", i),
				"native",
				false,
				1,
				0,
			))
		}
		writeHumanTelnetList(t, dir, entries...)
		_, _, err := LoadWithDiagnostics(dir)
		if err == nil || !strings.Contains(err.Error(), "65 entries exceeds maximum 64") {
			t.Fatalf("LoadWithDiagnostics() error = %v, want 65-entry limit", err)
		}
	})
}

func TestHumanTelnetNameContract(t *testing.T) {
	valid := []string{
		"A",
		"a.B_c-9",
		strings.Repeat("Z", 32),
		"  Case-Preserved  ",
	}
	for _, name := range valid {
		t.Run("valid "+name, func(t *testing.T) {
			dir := testConfigDir(t)
			writeHumanTelnetList(t, dir, renderHumanTelnetEntry(name, "one.example.invalid", 7300, "N0CALL-1", "native", false, 1, 0))
			cfg, _, err := LoadWithDiagnostics(dir)
			if err != nil {
				t.Fatalf("LoadWithDiagnostics() error: %v", err)
			}
			if cfg.HumanTelnet[0].Name != strings.TrimSpace(name) {
				t.Fatalf("name = %q, want %q", cfg.HumanTelnet[0].Name, strings.TrimSpace(name))
			}
		})
	}

	invalid := []string{
		"",
		"_starts-wrong",
		"space inside",
		"slash/name",
		"Náme",
		strings.Repeat("Z", 33),
	}
	for _, name := range invalid {
		t.Run("invalid "+name, func(t *testing.T) {
			dir := testConfigDir(t)
			writeHumanTelnetList(t, dir, renderHumanTelnetEntry(name, "one.example.invalid", 7300, "N0CALL-1", "native", false, 1, 0))
			_, _, err := LoadWithDiagnostics(dir)
			if err == nil || !strings.Contains(err.Error(), "human_telnet[0].name") {
				t.Fatalf("LoadWithDiagnostics() error = %v, want name rejection", err)
			}
		})
	}

	t.Run("case insensitive duplicate", func(t *testing.T) {
		dir := testConfigDir(t)
		writeHumanTelnetList(t, dir,
			renderHumanTelnetEntry("Feed", "one.example.invalid", 7300, "N0CALL-1", "native", false, 1, 0),
			renderHumanTelnetEntry(" fEEd ", "two.example.invalid", 7300, "N0CALL-2", "native", false, 1, 0),
		)
		_, _, err := LoadWithDiagnostics(dir)
		if err == nil || !strings.Contains(err.Error(), "case-insensitive duplicate") {
			t.Fatalf("LoadWithDiagnostics() error = %v, want duplicate-name rejection", err)
		}
	})
}

func TestHumanTelnetDuplicateIdentityAndNearMisses(t *testing.T) {
	t.Run("normalized duplicate", func(t *testing.T) {
		dir := testConfigDir(t)
		writeHumanTelnetList(t, dir,
			renderHumanTelnetEntry("ONE", " Example.COM ", 7300, " n0call-1 ", "native", false, 1, 0),
			renderHumanTelnetEntry("TWO", "example.com", 7300, "N0CALL-1", "native", false, 1, 0),
		)
		_, _, err := LoadWithDiagnostics(dir)
		if err == nil || !strings.Contains(err.Error(), "duplicate host/port/callsign identity") {
			t.Fatalf("LoadWithDiagnostics() error = %v, want identity rejection", err)
		}
	})

	t.Run("near misses remain distinct", func(t *testing.T) {
		dir := testConfigDir(t)
		writeHumanTelnetList(t, dir,
			renderHumanTelnetEntry("ONE", "example.com", 7300, "N0CALL-1", "native", false, 1, 0),
			renderHumanTelnetEntry("TWO", "EXAMPLE.COM", 7300, "N0CALL-2", "native", false, 1, 0),
			renderHumanTelnetEntry("THREE", "example.com.", 7300, "N0CALL-1", "native", false, 1, 0),
			renderHumanTelnetEntry("FOUR", "example.com", 7301, "N0CALL-1", "native", false, 1, 0),
		)
		cfg, _, err := LoadWithDiagnostics(dir)
		if err != nil {
			t.Fatalf("LoadWithDiagnostics() error: %v", err)
		}
		if len(cfg.HumanTelnet) != 4 {
			t.Fatalf("human_telnet entries = %d, want 4", len(cfg.HumanTelnet))
		}
	})
}

func TestHumanTelnetPreservesExistingFieldValidation(t *testing.T) {
	tests := []struct {
		name      string
		host      string
		port      int
		callsign  string
		transport string
		keepalive int
		want      string
	}{
		{name: "blank host", host: " ", port: 7300, callsign: "N0CALL-1", transport: "native", want: "human_telnet[0].host"},
		{name: "zero port", host: "one.example.invalid", port: 0, callsign: "N0CALL-1", transport: "native", want: "human_telnet[0].port"},
		{name: "blank callsign", host: "one.example.invalid", port: 7300, callsign: " ", transport: "native", want: "human_telnet[0].callsign"},
		{name: "blank transport", host: "one.example.invalid", port: 7300, callsign: "N0CALL-1", transport: " ", want: "human_telnet[0].telnet_transport"},
		{name: "invalid transport", host: "one.example.invalid", port: 7300, callsign: "N0CALL-1", transport: "invalid", want: "human_telnet[0].telnet_transport"},
		{name: "negative keepalive", host: "one.example.invalid", port: 7300, callsign: "N0CALL-1", transport: "native", keepalive: -1, want: "human_telnet[0].keepalive_seconds"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := testConfigDir(t)
			writeHumanTelnetList(t, dir, renderHumanTelnetEntry("ONE", tt.host, tt.port, tt.callsign, tt.transport, false, 1, tt.keepalive))
			_, _, err := LoadWithDiagnostics(dir)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("LoadWithDiagnostics() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestHumanTelnetSlotBufferBounds(t *testing.T) {
	tests := []struct {
		name    string
		entries []string
		wantErr string
	}{
		{
			name:    "minimum",
			entries: []string{renderHumanTelnetEntry("ONE", "one.example.invalid", 7300, "N0CALL-1", "native", true, 1, 0)},
		},
		{
			name:    "per entry maximum",
			entries: []string{renderHumanTelnetEntry("ONE", "one.example.invalid", 7300, "N0CALL-1", "native", true, 64000, 0)},
		},
		{
			name:    "zero",
			entries: []string{renderHumanTelnetEntry("ONE", "one.example.invalid", 7300, "N0CALL-1", "native", false, 0, 0)},
			wantErr: "must be between 1 and 64000",
		},
		{
			name:    "over per entry maximum",
			entries: []string{renderHumanTelnetEntry("ONE", "one.example.invalid", 7300, "N0CALL-1", "native", false, 64001, 0)},
			wantErr: "must be between 1 and 64000",
		},
		{
			name:    "extreme integer",
			entries: []string{renderHumanTelnetEntry("ONE", "one.example.invalid", 7300, "N0CALL-1", "native", false, int(^uint(0)>>1), 0)},
			wantErr: "must be between 1 and 64000",
		},
		{
			name: "exact aggregate",
			entries: []string{
				renderHumanTelnetEntry("ONE", "one.example.invalid", 7300, "N0CALL-1", "native", true, 32000, 0),
				renderHumanTelnetEntry("TWO", "two.example.invalid", 7300, "N0CALL-2", "native", true, 32000, 0),
			},
		},
		{
			name: "aggregate overflow",
			entries: []string{
				renderHumanTelnetEntry("ONE", "one.example.invalid", 7300, "N0CALL-1", "native", true, 32000, 0),
				renderHumanTelnetEntry("TWO", "two.example.invalid", 7300, "N0CALL-2", "native", true, 32001, 0),
			},
			wantErr: "enabled slot_buffer aggregate exceeds 64000",
		},
		{
			name: "disabled excluded from aggregate",
			entries: []string{
				renderHumanTelnetEntry("ONE", "one.example.invalid", 7300, "N0CALL-1", "native", true, 64000, 0),
				renderHumanTelnetEntry("TWO", "two.example.invalid", 7300, "N0CALL-2", "native", false, 64000, 0),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := testConfigDir(t)
			writeHumanTelnetList(t, dir, tt.entries...)
			_, _, err := LoadWithDiagnostics(dir)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("LoadWithDiagnostics() error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("LoadWithDiagnostics() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestHumanTelnetSequenceReplacementUsesMergedConfigSemantics(t *testing.T) {
	dir := testConfigDir(t)
	writeHumanTelnetList(t, dir,
		renderHumanTelnetEntry("INGEST", "ingest.example.invalid", 7300, "N0CALL-1", "native", false, 1, 0),
	)
	writeTestConfigOverlay(t, dir, "runtime.yaml", "human_telnet:\n"+
		renderHumanTelnetEntry("REPLACEMENT", "replacement.example.invalid", 7301, "N0CALL-2", "ziutek", false, 2, 0))

	cfg, _, err := LoadWithDiagnostics(dir)
	if err != nil {
		t.Fatalf("LoadWithDiagnostics() error: %v", err)
	}
	if len(cfg.HumanTelnet) != 1 || cfg.HumanTelnet[0].Name != "REPLACEMENT" {
		t.Fatalf("human_telnet = %#v, want later sequence replacement", cfg.HumanTelnet)
	}
}

func writeHumanTelnetList(t *testing.T, dir string, entries ...string) {
	t.Helper()
	body := "human_telnet: []\n"
	if len(entries) > 0 {
		body = "human_telnet:\n" + strings.Join(entries, "")
	}
	writeTestConfigOverlay(t, dir, "ingest.yaml", body)
}

func renderHumanTelnetEntry(name, host string, port int, callsign, transport string, enabled bool, slotBuffer, keepalive int) string {
	return fmt.Sprintf(`  - enabled: %t
    host: %q
    port: %d
    callsign: %q
    name: %q
    telnet_transport: %q
    keep_ssid_suffix: true
    slot_buffer: %d
    keepalive_seconds: %d
`, enabled, host, port, callsign, name, transport, slotBuffer, keepalive)
}
