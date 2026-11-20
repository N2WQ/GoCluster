package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestStudyWeightReplayResolver(t *testing.T) {
	dir := t.TempDir()
	global := filepath.Join(dir, "global.txt")
	control := filepath.Join(dir, "cw.txt")
	candidate := filepath.Join(dir, "cw-private.txt")
	for name, text := range map[string]string{
		global:    "N0BBB 0.983\n",
		control:   "N0CCC 0.94\n",
		candidate: "N0CCC 0.94\nN0AAA 0.950\n",
	} {
		if err := os.WriteFile(name, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var settings [2]*config.Config
	for i, path := range []string{control, candidate} {
		settings[i] = &config.Config{}
		cfg := &settings[i].CallCorrection
		cfg.SpotterReliabilityFile = global
		cfg.SpotterReliabilityFileCW = path
		cfg.MinSpotterReliability = .95
		known, cw, _, err := loadSpotterReliability(*cfg)
		if err != nil {
			t.Fatal(err)
		}
		r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 16, HysteresisWindows: 1,
			EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond,
			SpotterReliability: known, SpotterReliabilityCW: cw, MinSpotterReliability: .95})
		driver, err := spot.NewSignalResolverDriver(r)
		if err != nil {
			t.Fatal(err)
		}
		at := time.Unix(10000, 0)
		key := spot.NewResolverSignalKey(7000, "40m", "CW", 400)
		for _, reporter := range []string{"N0AAA", "N0BBB", "N0CCC"} {
			if !r.Enqueue(spot.ResolverEvidence{Key: key, ObservedAt: at, FrequencyKHz: 7000, Spotter: reporter, DXCall: "K1ABC", RecencyWindow: time.Minute}) {
				t.Fatal("failed to queue fixture")
			}
		}
		driver.Drain()
		driver.Sweep(at.Add(time.Millisecond))
		got, ok := r.Lookup(key)
		if !ok || got.WinnerSupport != 2 || r.MetricsSnapshot().DropReliability != 1 {
			t.Fatalf("floor or admission changed: %+v", got)
		}
		seen := make(map[string]int)
		got.ReceiverEvidence().Range(func(_ string, row spot.ResolverReceiverObservation) bool {
			seen[row.Receiver] = row.WeightMilli
			return true
		})
		if seen["N0AAA"] != []int{1000, 950}[i] || seen["N0BBB"] != 983 || len(seen) != 2 {
			t.Fatalf("arm %d receiver weights = %v", i, seen)
		}
	}
	known, added, err := studyLoadWeightTables(settings)
	if err != nil || known["N0BBB"] != .983 || known["N0CCC"] != .94 || len(added) != 1 || added["N0AAA"] != .95 {
		t.Fatalf("asset did not resolve to precisely one fallback: %v, %v, %v", known, added, err)
	}
	if err := os.WriteFile(candidate, []byte("N0CCC 0.94\nN0AAA 0.950\nN0AAA 0.950\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := studyLoadWeightTables(settings); err == nil {
		t.Fatal("accepted duplicate private fallback line")
	}
}

func TestStudyWeightParityRejectsAnotherAsset(t *testing.T) {
	dir := t.TempDir()
	oldCW, newCW, ctyPath := filepath.Join(dir, "cw.txt"), filepath.Join(dir, "private.txt"), filepath.Join(dir, "cty")
	left := studyArm{Assets: []studyFile{{Path: oldCW, SHA256: "cw"}, {Path: ctyPath, SHA256: "cty"}}, ConfigFiles: map[string]string{"pipeline.yaml": "original", "runtime.yaml": "same"}}
	right := studyArm{Assets: []studyFile{{Path: newCW, SHA256: "private"}, {Path: ctyPath, SHA256: "cty"}}, ConfigFiles: map[string]string{"pipeline.yaml": "private", "runtime.yaml": "same"}}
	if !studyEqualWeightInputs(left, right, oldCW, newCW) {
		t.Fatal("rejected sole intended delta")
	}
	right.Assets[1].SHA256 = "mismatch"
	if studyEqualWeightInputs(left, right, oldCW, newCW) {
		t.Fatal("accepted a change in CTY evidence")
	}
	right.Assets[1] = left.Assets[1]
	right.ConfigFiles["runtime.yaml"] = "mismatch"
	if studyEqualWeightInputs(left, right, oldCW, newCW) {
		t.Fatal("accepted a change in runtime settings")
	}
}
