package telnet

import (
	"strings"
	"testing"
	"time"
)

func TestApplyTemplateTokens_ReplacesAll(t *testing.T) {
	now := time.Date(2026, time.January, 6, 20, 12, 18, 0, time.UTC)
	start := now.Add(-(3*24*time.Hour + 4*time.Hour + 18*time.Minute + 22*time.Second))
	lastLogin := time.Date(2026, time.January, 5, 18, 0, 11, 0, time.UTC)
	data := templateData{
		now:            now,
		startTime:      start,
		userCount:      12,
		callsign:       "N2WQ-2",
		cluster:        "LZ13ZZ",
		lastLogin:      lastLogin,
		lastIP:         "203.0.113.9",
		dialect:        "GO",
		dialectSource:  "default",
		dialectDefault: "GO",
		dedupePolicy:   "MED",
		grid:           "FN31",
		noiseClass:     "QUIET",
	}

	msg := "\nLast login: <LAST_LOGIN> from <LAST_IP>\n\n<CALL> de <CLUSTER> <DATETIME>> \nUptime: <UPTIME> | Users: <USER_COUNT>\nDialect: <DIALECT> (<DIALECT_SOURCE>/<DIALECT_DEFAULT>) Dedupe: <DEDUPE> Grid: <GRID> Noise: <NOISE>\n"
	out := applyTemplateTokens(msg, data)

	for _, want := range []string{
		"Last login: 05-Jan-2026 18:00:11 UTC from 203.0.113.9",
		"N2WQ-2 de LZ13ZZ 06-Jan-2026 20:12:18 UTC>",
		"Uptime: 3d 04:18:22 | Users: 12",
		"Dialect: GO (default/GO) Dedupe: MED Grid: FN31 Noise: QUIET",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q:\n%s", want, out)
		}
	}
}

func TestApplyTemplateTokens_FirstLoginFallbacks(t *testing.T) {
	now := time.Date(2026, time.January, 6, 20, 12, 18, 0, time.UTC)
	data := templateData{
		now:       now,
		startTime: now,
		userCount: 0,
		callsign:  "N0CALL",
		cluster:   "NODE",
	}
	out := applyTemplateTokens("Last: <LAST_LOGIN> IP: <LAST_IP> Count: <USER_COUNT>", data)
	for _, want := range []string{"(first login)", "(unknown)", "Count: 0"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q: %s", want, out)
		}
	}
}

func TestPreLoginTemplateDataIncludesCluster(t *testing.T) {
	server := &Server{clusterCall: "N0CALL"}
	now := time.Date(2026, time.January, 6, 20, 12, 18, 0, time.UTC)
	data := server.preLoginTemplateData(now)
	out := applyTemplateTokens("<CLUSTER>", data)
	if out != "N0CALL" {
		t.Fatalf("expected cluster in pre-login template data, got %q", out)
	}
}

func TestApplyInputTemplateTokens(t *testing.T) {
	msg := "<CONTEXT> input is too long (maximum <MAX_LEN> characters). Allowed: <ALLOWED>."
	out := applyInputTemplateTokens(msg, inputTemplateData{
		context: "Login",
		maxLen:  32,
		allowed: "letters, numbers",
	})
	if !strings.Contains(out, "Login input is too long") || !strings.Contains(out, "32") || !strings.Contains(out, "letters, numbers") {
		t.Fatalf("unexpected output: %s", out)
	}
}

func TestPostLoginTemplateDataLowercasesDerivedGrid(t *testing.T) {
	now := time.Date(2026, time.January, 6, 20, 12, 18, 0, time.UTC)
	server := &Server{startTime: now, clusterCall: "DXC"}
	client := &Client{sessionProfile: &sessionProfile{grid: "FN31",
		gridDerived: true,
	}, callsign: "N0CALL"}
	data := server.postLoginTemplateData(now, client, time.Time{}, "", "default", "GO")
	if data.grid != "fn31" {
		t.Fatalf("expected derived grid to be lowercase, got %q", data.grid)
	}
}
