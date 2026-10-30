package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// This oracle uses the ordinary encoder and an independent whole-string CRLF
// conversion. It never uses boundedResponse or its size accounting.
func v7ReferenceYAMLFrame(t *testing.T, document any) string {
	t.Helper()
	var body bytes.Buffer
	encoder := yaml.NewEncoder(&body)
	encoder.SetIndent(2)
	if err := encoder.Encode(document); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	return "---\r\n" + strings.ReplaceAll(body.String(), "\n", "\r\n") + "...\r\n"
}

func TestV7EncoderFinalFrameExactAndPlusOne(t *testing.T) {
	for _, limit := range []int{65_536, 262_144} {
		for _, escaped := range []bool{false, true} {
			t.Run(fmt.Sprintf("limit=%d/escaped=%t", limit, escaped), func(t *testing.T) {
				document := struct {
					Version int    `yaml:"schema_version"`
					Payload string `yaml:"payload"`
				}{Version: 7, Payload: "prefix"}
				if escaped {
					document.Payload += strings.Repeat("\x00\"\\\t", 1_000)
				}
				base := v7ReferenceYAMLFrame(t, document)
				if escaped && len(base) <= len(document.Payload)*2 {
					t.Fatal("fixture did not exercise quoted-scalar expansion")
				}
				document.Payload += strings.Repeat("x", limit-len(base))
				want := v7ReferenceYAMLFrame(t, document)
				if len(want) != limit {
					t.Fatalf("independent fixture has %d bytes, want %d", len(want), limit)
				}
				got, err := encodeBoundedYAMLLimit(document, limit)
				if err != nil || got != want {
					t.Fatalf("exact final frame differs from ordinary encoder: length=%d err=%v", len(got), err)
				}
				document.Payload += "x"
				if got := len(v7ReferenceYAMLFrame(t, document)); got != limit+1 {
					t.Fatalf("independent overflow fixture length=%d", got)
				}
				if got, err := encodeBoundedYAMLLimit(document, limit); got != "" || !errors.Is(err, errReadbackTooLarge) {
					t.Fatalf("one-byte overflow returned partial/successful frame: length=%d err=%v", len(got), err)
				}
			})
		}
	}
}
