package main

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestOfflineCLIExplicitExperimentAndSanitizedOutput(t *testing.T) {
	var out bytes.Buffer
	dir := filepath.Join(t.TempDir(), "generated")
	if err := run([]string{"synthetic", "-output", dir}, &out); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"compare", "-input", dir}, &out); err == nil {
		t.Fatal("implicit settings accepted")
	}
	out.Reset()
	if err := run([]string{"compare", "-input", dir, "-threshold", "2", "-window-seconds", "10", "-actor-cap", "8"}, &out); err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"W1AAA", "K1AAA", "CQ", "N0ORG", "known_post"} {
		if bytes.Contains(out.Bytes(), []byte(secret)) {
			t.Fatal("source field leaked")
		}
	}
	if !bytes.Contains(out.Bytes(), []byte("hypothetical_parameters")) || !bytes.Contains(out.Bytes(), []byte("revision")) {
		t.Fatal("missing experiment/provenance boundary")
	}
}
