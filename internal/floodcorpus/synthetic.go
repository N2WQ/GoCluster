package floodcorpus

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Generate writes a fixed independently labeled fixture. Its schedules expose
// burst/sustained/cutoff/recovery and copy ambiguity; they are not traffic data
// or recommended settings. The hand-derived examples use T=2 and W=10 seconds.
func Generate(output string) (Manifest, error) {
	m := Manifest{Version: Schema, Stage: corpusStage, Synthetic: true, Transport: "generated", Complete: true, CaptureComplete: true, Timing: "generated receipt schedule, not measured traffic", Eligibility: "independently labeled generated inputs; not a contest or operational threshold oracle", Implementation: buildIdentity()}
	if !localPath(output) {
		return m, errors.New("local output required")
	}
	if err := os.Mkdir(output, 0700); err != nil {
		return m, errors.New("output directory must be new")
	}
	f, err := os.OpenFile(filepath.Join(output, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return m, errors.New("cannot create generated events")
	}
	defer f.Close()
	h := sha256.New()
	w := &boundedWriter{writer: io.MultiWriter(f, h), remaining: MaxInputBytes}
	scenario := "burst"
	emit := func(ns int64, source, de, dx, id, kind string) error {
		m.Events++
		m.LastNS = ns
		e := Event{Version: Schema, Arrival: m.Events, NS: ns, Source: source, Transport: "generated", Truth: &TruthLabel{Kind: kind, LogicalID: id, Provenance: "synthetic-fixture"}}
		e.Scenario = scenario
		if source == "PEER" {
			wire := fmt.Sprintf("PC11^14020^%s^15-Nov-2026^1200Z^CQ^%s^N0ORG^H5^", dx, de)
			e.SentenceBase64 = base64.StdEncoding.EncodeToString([]byte(wire))
		} else {
			e.Spot = &SpotRecord{DX: dx, DE: de, Node: "N0ORG", Mode: "CW", Frequency: 14020, Time: "2026-11-15T12:00:00Z"}
		}
		return writeEvent(w, e)
	}
	send := func(seconds int64, source, de string) error {
		return emit(seconds*int64(time.Second), source, de, "K1AAA", fmt.Sprintf("post-%d", m.Events+1), "known_post")
	}
	for _, at := range []int64{0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9} {
		if at > 0 {
			scenario = "sustained"
		}
		if err := send(at, "MANUAL", "W1AAA"); err != nil {
			return m, err
		}
	}
	scenario = "cutoff"
	for _, at := range []int64{20, 20, 30} {
		if err := send(at, "UPSTREAM", "W2BBB"); err != nil {
			return m, err
		}
	}
	if err := emit(30*int64(time.Second)+1, "UPSTREAM", "W2BBB", "K1AAA", "cutoff-after", "known_post"); err != nil {
		return m, err
	}
	scenario = "idle_recovery"
	if err := send(60, "MANUAL", "W1AAA"); err != nil {
		return m, err
	}
	scenario = "slower_recovery"
	for _, at := range []int64{80, 80, 80, 86, 92, 98, 104} {
		if err := send(at, "UPSTREAM", "W3CCC"); err != nil {
			return m, err
		}
	}
	for _, e := range []struct {
		ns               int64
		de, dx, id, kind string
	}{
		{120e9, "W4DDD", "K1AAA", "copy-post", "known_post"},
		{120e9 + 1, "W4DDD", "K1AAA", "copy-post", "known_copy"},
		{120e9 + 2, "W4DDD", "K1BBB", "later-post", "known_post"},
		{140e9, "W5EEE", "K1AAA", "genuine-one", "known_post"},
		{140e9 + 1, "W5EEE", "K1AAA", "genuine-two", "known_post"},
	} {
		scenario = "known_copies"
		if e.ns >= 140e9 {
			scenario = "genuine_repeats"
		}
		if err := emit(e.ns, "PEER", e.de, e.dx, e.id, e.kind); err != nil {
			return m, err
		}
	}
	scenario = "native"
	if err := send(160, "RBN", "W1AAA"); err != nil {
		return m, err
	}
	m.EventsSHA256 = hex.EncodeToString(h.Sum(nil))
	if err := f.Close(); err != nil {
		return m, errors.New("generated event close failed")
	}
	if err := writeExclusiveJSON(filepath.Join(output, "manifest.json"), m); err != nil {
		return m, err
	}
	return m, nil
}
