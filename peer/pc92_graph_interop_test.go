package peer

import (
	"fmt"
	"testing"
	"time"
)

func referenceHasMember(t *testing.T, parent map[string]any, field string, want bool) {
	t.Helper()
	const call = "K1DUAL"
	found := false
	if values, ok := parent[field].([]any); ok {
		for _, value := range values {
			if fmt.Sprint(value) == call {
				found = true
			}
		}
	}
	if found != want {
		t.Fatalf("receiver %s contains %s=%v, want %v: %v", field, call, found, want, parent)
	}
}

func TestDXSpiderReferenceTypedMembership(t *testing.T) {
	r, _ := startDXReference(t, false, "N0CALL")
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	result := r.frameAt("PC92^N0CALL^43200^C^5N0CALL^1K1DUAL:192.0.2.1^5K1DUAL:5457:633:192.0.2.2^H99^", now, "K1DUAL")
	referenceHasMember(t, result.RouteNodes["N0CALL"], "users", true)
	referenceHasMember(t, result.RouteNodes["N0CALL"], "nodes", true)
	referenceField(t, result.RouteUsers["K1DUAL"], "ip", "192.0.2.1")
	referenceField(t, result.RouteNodes["K1DUAL"], "ip", "192.0.2.2")
	result = r.frameAt("PC92^N0CALL^43201^D^^5K1DUAL^H99^", now.Add(time.Second), "K1DUAL")
	referenceHasMember(t, result.RouteNodes["N0CALL"], "users", true)
	referenceHasMember(t, result.RouteNodes["N0CALL"], "nodes", false)
	referenceField(t, result.RouteUsers["K1DUAL"], "ip", "192.0.2.1")
	result = r.frameAt("PC92^N0CALL^43202^C^5N0CALL^5K1DUAL^H99^", now.Add(2*time.Second), "K1DUAL")
	referenceHasMember(t, result.RouteNodes["N0CALL"], "users", false)
	referenceHasMember(t, result.RouteNodes["N0CALL"], "nodes", true)
	result = r.frameAt("PC92^N0CALL^43203^C^5N0CALL^1K1DUAL^H99^", now.Add(3*time.Second), "K1DUAL")
	referenceHasMember(t, result.RouteNodes["N0CALL"], "users", true)
	referenceHasMember(t, result.RouteNodes["N0CALL"], "nodes", false)
}

func TestDXSpiderReferenceCMetadataSelection(t *testing.T) {
	for _, tc := range []struct{ name, input, want string }{
		{"stable", "1K1DUAL:192.0.2.2^", "192.0.2.1"},
		{"repeated", "1K1DUAL:192.0.2.2^0K1DUAL^", "192.0.2.2"},
		{"second-kind", "1K1DUAL:192.0.2.2^5K1DUAL:5457:633:192.0.2.3^", "192.0.2.2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := startDXReference(t, false, "N0CALL")
			now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
			result := r.frameAt("PC92^N0CALL^43200^A^^1K1DUAL:192.0.2.1^H99^", now, "K1DUAL")
			referenceField(t, result.RouteUsers["K1DUAL"], "ip", "192.0.2.1")
			result = r.frameAt("PC92^N0CALL^43201^C^5N0CALL^"+tc.input+"H99^", now.Add(time.Second), "K1DUAL")
			referenceField(t, result.RouteUsers["K1DUAL"], "ip", tc.want)
		})
	}
}

func TestDXSpiderReferenceRepeatedMetadata(t *testing.T) {
	r, _ := startDXReference(t, false, "N0CALL")
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	result := r.frameAt("PC92^N0CALL^43200^A^^1K1USER:10:20:192.0.2.1^0K1USER:11:21^H99^", now, "K1USER")
	referenceField(t, result.RouteUsers["K1USER"], "ip", "192.0.2.1")
	if _, exists := result.RouteUsers["K1USER"]["version"]; exists {
		t.Fatal("user numeric version became receiver metadata")
	}
	if _, exists := result.RouteUsers["K1USER"]["build"]; exists {
		t.Fatal("user numeric build became receiver metadata")
	}
	// Route's internal Here mask is bit 2, distinct from the wire bitmap.
	referenceField(t, result.RouteUsers["K1USER"], "flags", "2")
}

func TestDXSpiderReferenceMemberHereAndExplicitSubject(t *testing.T) {
	r, _ := startDXReference(t, false, "N0CALL")
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	result := r.frameAt("PC92^N0CALL^43200^A^^0K1USER^4N3NODE^H99^", now, "K1USER", "N3NODE")
	referenceField(t, result.RouteUsers["K1USER"], "flags", "2")
	referenceField(t, result.RouteNodes["N3NODE"], "flags", "2")
	result = r.frameAt("PC92^N3NODE^43201^K^4N3NODE^0^0^H99^", now.Add(time.Second), "N3NODE")
	referenceField(t, result.RouteNodes["N3NODE"], "flags", "0")
	result = r.frameAt("PC92^N0CALL^43202^A^^5N3NODE^H99^", now.Add(2*time.Second), "N3NODE")
	referenceField(t, result.RouteNodes["N3NODE"], "flags", "0")
}

func TestDXSpiderReferenceMemberNodeNumericAuthority(t *testing.T) {
	r, _ := startDXReference(t, false, "N0CALL")
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	result := r.frameAt("PC92^N0CALL^43200^A^^4N3NODE::633:192.0.2.1^5N3NODE:5458^H99^", now, "N3NODE")
	referenceField(t, result.RouteNodes["N3NODE"], "version", "5401")
	if _, exists := result.RouteNodes["N3NODE"]["build"]; exists {
		t.Fatal("member build unexpectedly became receiver node authority")
	}
	result = r.frameAt("PC92^N3NODE^43201^K^4N3NODE:5459:635^0^0^H99^", now.Add(time.Second), "N3NODE")
	referenceField(t, result.RouteNodes["N3NODE"], "version", "5459")
	referenceField(t, result.RouteNodes["N3NODE"], "build", "635")
	result = r.frameAt("PC92^N0CALL^43202^A^^5N3NODE:5460:636^H99^", now.Add(2*time.Second), "N3NODE")
	referenceField(t, result.RouteNodes["N3NODE"], "version", "5459")
	referenceField(t, result.RouteNodes["N3NODE"], "build", "635")
}
