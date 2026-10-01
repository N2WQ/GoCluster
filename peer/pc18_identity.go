package peer

import (
	"errors"
	"fmt"
	"strings"
)

const maxPC18BannerBytes = 1024

// BuildPC18Banner uses the actual startup build identity, independently of the
// numeric compatibility version/build in PC92. Values use reversible \xHH byte
// escapes where DXSpider's unanchored software/capability regexes could mistake
// metadata for a claim. No value is truncated or replaced by a fabricated one.
// The caller must fail startup with peering enabled when this envelope fails.
func BuildPC18Banner(version, commit, buildTime, vcsModified, goVersion string) (string, error) {
	values := []string{version, commit, buildTime, vcsModified, goVersion}
	for i, value := range values {
		if len(value) > maxPC18BannerBytes {
			return "", errors.New("PC18 build identity exceeds 1024 bytes")
		}
		values[i] = encodePC18Metadata(value)
	}
	banner := fmt.Sprintf("GoCluster Version: %s Commit: %s Built: %s Modified: %s Go: %s", values[0], values[1], values[2], values[3], values[4])
	if len(banner) > maxPC18BannerBytes {
		return "", errors.New("PC18 build identity exceeds 1024 bytes after safe encoding")
	}
	return banner, nil
}

func encodePC18Metadata(value string) string {
	var out strings.Builder
	for i := 0; i < len(value); i++ {
		b := value[i]
		escape := b <= 32 || b >= 127 || b == '^' || b == '~' || b == '\\'
		// handle_18 searches for CCCluster anywhere, and pc9x or 91 after
		// any word boundary, even inside a hash, dirty marker, or version.
		escape = escape || pc18PrefixFold(value[i:], "cccluster") || pc18PrefixFold(value[i:], "pc9x")
		if b == '9' && i+1 < len(value) && value[i+1] == '1' && (i == 0 || !pc18WordByte(value[i-1])) {
			escape = true
		}
		if escape {
			fmt.Fprintf(&out, "\\x%02x", b)
		} else {
			out.WriteByte(b)
		}
	}
	return out.String()
}

func pc18PrefixFold(value, prefix string) bool {
	return len(value) >= len(prefix) && strings.EqualFold(value[:len(prefix)], prefix)
}

func pc18WordByte(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_'
}

// FormatPC18 appends capability only when the negotiated local policy permits
// it. The second field remains numeric compatibility metadata; GoCluster's
// product version never enters DXSpider's numeric version decoder.
func FormatPC18(banner, numericVersion string, pc9x bool) (string, error) {
	if !strings.HasPrefix(banner, "GoCluster Version: ") || len(banner) > maxPC18BannerBytes {
		return "", errors.New("invalid PC18 build identity")
	}
	for i := range banner {
		if banner[i] < 32 || banner[i] > 126 || banner[i] == '^' || banner[i] == '~' {
			return "", errors.New("unsafe PC18 build identity")
		}
	}
	if numericVersion == "" || len(numericVersion) > 10 {
		return "", errors.New("invalid PC18 compatibility version")
	}
	for i := range numericVersion {
		if numericVersion[i] < '0' || numericVersion[i] > '9' {
			return "", errors.New("invalid PC18 compatibility version")
		}
	}
	if pc9x {
		banner += " pc9x"
	}
	line := "PC18^" + banner + "^" + numericVersion + "^"
	if len(line) > maxPC18BannerBytes {
		return "", errors.New("complete PC18 frame exceeds 1024 bytes")
	}
	return line, nil
}
