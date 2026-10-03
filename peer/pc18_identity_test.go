package peer

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestPC18HonestBuildIdentityAndReferenceRegexes(t *testing.T) {
	for _, version := range []string{"261003", "dev", "v1.2.3", "2026.10.01-g91abc-dirty", "release DXSpider Version: 1.55", "CCCluster pc9x 91"} {
		banner, err := BuildPC18Banner(version, "91abc", "2026-10-01T00:00:00Z", "true", "go1.25.0")
		if err != nil {
			t.Fatal(err)
		}
		wire, err := FormatPC18(banner, "5457", true)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(wire, "PC18^GoCluster Version: ") || !strings.HasSuffix(wire, " pc9x^5457^") {
			t.Fatalf("untruthful product/numeric identity: %s", wire)
		}
		// These are the actual matching rules in pinned handle_18, rather
		// than a parser copied from our banner implementation.
		for _, pattern := range []string{`(?i)(DXSpider|CC\s*Cluster)\s+Version: (\d+(?:\.\d+)?)`, `(?i)CC\s*Cluster`, `\b91`} {
			if regexp.MustCompile(pattern).MatchString(banner) {
				t.Fatalf("false reference claim %q: %s", pattern, banner)
			}
		}
		legacy, err := FormatPC18(banner, "5457", false)
		if err != nil {
			t.Fatal(err)
		}
		if regexp.MustCompile(`(?i)\bpc9x`).MatchString(legacy) {
			t.Fatalf("metadata enables pc9x: %s", legacy)
		}
	}
}

func TestPC18DateOnlyVersionPreservesMetadataAndCompatibility(t *testing.T) {
	banner, err := BuildPC18Banner("261003", "abcdef123456", "2026-10-03T12:34:56Z", "true", "go1.26.4")
	if err != nil {
		t.Fatal(err)
	}
	for _, pc9x := range []bool{false, true} {
		wire, err := FormatPC18(banner, "5457", pc9x)
		if err != nil {
			t.Fatal(err)
		}
		want := "PC18^GoCluster Version: 261003 Commit: abcdef123456 Built: 2026-10-03T12:34:56Z Modified: true Go: go1.26.4"
		if pc9x {
			want += " pc9x"
		}
		want += "^5457^"
		if wire != want {
			t.Fatalf("PC18 identity = %q, want %q", wire, want)
		}
	}
}

func TestPC18MetadataEscapesAreReversibleAndBounded(t *testing.T) {
	for _, value := range []string{"abc^~\r\n", "éİſ", "\\x91", "CC Cluster Version: 5 pc9x", "91abcdef", "x-91abcd"} {
		encoded := encodePC18Metadata(value)
		decoded, err := strconv.Unquote(`"` + strings.ReplaceAll(encoded, `"`, `\"`) + `"`)
		if err != nil || decoded != value {
			t.Fatalf("%q => %q => %q: %v", value, encoded, decoded, err)
		}
		banner, err := BuildPC18Banner(value, value, "", "", "")
		if err != nil {
			t.Fatal(err)
		}
		wire, err := FormatPC18(banner, "5457", true)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(wire, "^") != 3 || strings.ContainsAny(wire, "\r\n~") {
			t.Fatalf("wire injection: %q", wire)
		}
	}
	if _, err := BuildPC18Banner(strings.Repeat("x", 1024), "", "", "", ""); err == nil {
		t.Fatal("oversized banner accepted")
	}
	if _, err := FormatPC18("GoCluster Version: v1^PC20", "5457", true); err == nil {
		t.Fatal("unsafe prebuilt banner accepted")
	}
	if _, err := FormatPC18("GoCluster Version: dev", "54.57", true); err == nil {
		t.Fatal("nonnumeric compatibility version accepted")
	}
}

func FuzzPC18MetadataIdentity(f *testing.F) {
	f.Add("v1.2.3", "91abcdef")
	f.Add("éİſ^~\r\n", "CCCluster pc9x 91")
	f.Fuzz(func(t *testing.T, version, commit string) {
		banner, err := BuildPC18Banner(version, commit, "", "", "")
		if err != nil {
			return
		}
		line, err := FormatPC18(banner, "5457", false)
		if err != nil {
			return
		} // complete-frame overhead can exceed its bound
		if strings.Count(line, "^") != 3 || strings.ContainsAny(line, "\r\n~") {
			t.Fatalf("wire boundary escaped: %q", line)
		}
		for _, pattern := range []string{`(?i)CC\s*Cluster`, `(?i)\bpc9x`, `\b91`} {
			if regexp.MustCompile(pattern).MatchString(banner) {
				t.Fatalf("false capability: %q", banner)
			}
		}
	})
}
