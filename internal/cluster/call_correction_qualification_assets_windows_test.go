//go:build windows && !qualification

package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/spot"
)

type ccQualAssets struct {
	cfg          *config.Config
	cty          *cty.CTYDatabase
	model        *spot.ConfusionModel
	global       spot.SpotterReliability
	cw           spot.SpotterReliability
	rtty         spot.SpotterReliability
	fingerprints map[string]string
	caps         map[string]int
}

// Load every deployment-owned input before a child t.Chdir. Missing or invalid
// configured assets fail this run; no literal model silently substitutes for it.
func ccQualLoadAssets(t *testing.T, opts ccQualOptions) *ccQualAssets {
	t.Helper()
	cfg, err := config.Load(filepath.Join(opts.Root, "data", "config"))
	if err != nil {
		t.Fatalf("qualification config: %v", err)
	}
	if !cfg.CallCorrection.Enabled || !cfg.CallCorrection.ConfusionModelEnabled || !cfg.Archive.Enabled {
		t.Fatal("qualification requires the configured correction, confusion-model and archive owners enabled")
	}
	a := &ccQualAssets{cfg: cfg, fingerprints: make(map[string]string), caps: map[string]int{
		"output_queue": cfg.Dedup.OutputBufferSize, "ring": cfg.Buffer.Capacity,
		"resolver_queue": shadowResolverQueueSize, "resolver_keys": shadowResolverMaxActiveKeys,
		"resolver_raw_spellings_per_key":      shadowResolverMaxCandidatesPerKey,
		"resolver_receivers_per_raw_spelling": shadowResolverMaxReportersPerCand,
		"temporal_pending":                    cfg.CallCorrection.TemporalDecoder.MaxPending,
		"stabilizer_pending":                  cfg.CallCorrection.StabilizerMaxPending,
		"archive_queue":                       cfg.Archive.QueueSize, "archive_batch": cfg.Archive.BatchSize,
		"telnet_broadcast_queue": cfg.Telnet.BroadcastQueue, "telnet_worker_queue": cfg.Telnet.WorkerQueue,
		"telnet_client_buffer": cfg.Telnet.ClientBuffer, "telnet_workers": cfg.Telnet.BroadcastWorkers,
		"loopback_clients": 2,
	}}
	a.fingerprints["production_go"] = ccQualProductionHash(t, opts.Root)
	a.fingerprints["config_yaml"] = ccQualTreeHash(t, opts.Root, []string{"data/config"}, func(path string) bool {
		return filepath.Ext(path) == ".yaml" || filepath.Ext(path) == ".yml"
	})
	a.fingerprints["loaded_correction_config"] = ccQualHashJSON(t, cfg.CallCorrection)
	a.fingerprints["fixture_go"] = ccQualTreeHash(t, opts.Root, []string{"internal/cluster"}, func(path string) bool {
		return strings.HasPrefix(filepath.Base(path), "call_correction_qualification_") && strings.HasSuffix(path, "_test.go")
	})
	modelPath := ccQualAssetPath(opts.Root, cfg.CallCorrection.ConfusionModelFile)
	a.fingerprints["confusion_model"] = ccQualFileHash(t, modelPath)
	a.model, err = spot.LoadConfusionModel(modelPath)
	if err != nil {
		t.Fatalf("qualification confusion model: %v", err)
	}
	if ccQualFileHash(t, modelPath) != a.fingerprints["confusion_model"] {
		t.Fatal("confusion model changed while loading")
	}
	tables := []struct {
		name, path string
		dest       *spot.SpotterReliability
	}{
		{"reliability_global", cfg.CallCorrection.SpotterReliabilityFile, &a.global},
		{"reliability_cw", cfg.CallCorrection.SpotterReliabilityFileCW, &a.cw},
		{"reliability_rtty", cfg.CallCorrection.SpotterReliabilityFileRTTY, &a.rtty},
	}
	for _, table := range tables {
		if strings.TrimSpace(table.path) == "" {
			a.fingerprints[table.name] = "disabled_by_config"
			continue
		}
		path := ccQualAssetPath(opts.Root, table.path)
		a.fingerprints[table.name] = ccQualFileHash(t, path)
		*table.dest, _, err = spot.LoadSpotterReliability(path)
		if err != nil {
			t.Fatalf("qualification %s: %v", table.name, err)
		}
		if ccQualFileHash(t, path) != a.fingerprints[table.name] {
			t.Fatalf("%s changed while loading", table.name)
		}
	}
	ctyPath := ccQualAssetPath(opts.Root, cfg.CTY.File)
	a.fingerprints["cty"] = ccQualFileHash(t, ctyPath)
	a.cty, err = cty.LoadCTYDatabase(ctyPath)
	if err != nil {
		t.Fatalf("qualification CTY: %v", err)
	}
	if ccQualFileHash(t, ctyPath) != a.fingerprints["cty"] {
		t.Fatal("CTY changed while loading")
	}
	return a
}

func ccQualAssetPath(root, path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(root, path)
}

func ccQualFileHash(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("fingerprint %s: %v", path, err)
	}
	h := sha256.New()
	_, copyErr := io.Copy(h, f)
	closeErr := f.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("fingerprint %s: read=%v close=%v", path, copyErr, closeErr)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func ccQualProductionHash(t *testing.T, root string) string {
	t.Helper()
	return ccQualTreeHash(t, root, []string{"spot", "internal/cluster", "internal/correctionflow", "archive", "buffer", "config", "cty", "dedup", "filter", "stats", "telnet", "go.mod", "go.sum"}, func(path string) bool {
		return (strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go")) || path == "go.mod" || path == "go.sum"
	})
}

// The hash includes sorted relative paths and content digests, with fixed file
// and traversal limits. It retains filenames only, never a source-content copy.
func ccQualTreeHash(t *testing.T, root string, scopes []string, include func(string) bool) string {
	t.Helper()
	var paths []string
	visited := 0
	for _, scope := range scopes {
		err := filepath.WalkDir(filepath.Join(root, scope), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			visited++
			if visited > 10000 {
				return fmt.Errorf("fingerprint traversal exceeded 10000 entries")
			}
			if entry.IsDir() {
				return nil
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("fingerprint refuses symlink %s", path)
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			if include(rel) {
				paths = append(paths, rel)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("fingerprint %s: %v", scope, err)
		}
	}
	if len(paths) == 0 {
		t.Fatal("fingerprint selected no files")
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, path := range paths {
		fmt.Fprintf(h, "%s\x00%s\n", path, ccQualFileHash(t, filepath.Join(root, path)))
	}
	return hex.EncodeToString(h.Sum(nil))
}

type ccQualInput struct {
	Call      string  `json:"call"`
	Receiver  string  `json:"receiver"`
	Mode      string  `json:"mode"`
	Frequency float64 `json:"frequency_khz"`
	Report    int     `json:"report_db"`
}

func ccQualWorkload(t *testing.T, a *ccQualAssets, name string) ([]ccQualInput, string) {
	t.Helper()
	var inputs []ccQualInput
	if name == "dense_canonical" {
		// Eight permutations per semantic identity, sixteen raw entries total.
		// Every raw entry has 64 separate receivers. The two 512-receiver unions
		// are disjoint and have opposing frequency witnesses within each bucket;
		// a shared-receiver shortcut cannot discharge the frequency guard work.
		for _, mode := range []string{"CW", "RTTY"} {
			center := 14020.0
			if mode == "RTTY" {
				center = 14084
			}
			step := a.cfg.CallCorrection.FrequencyToleranceHz / 1000
			if step < 0.4 {
				t.Fatal("dense_canonical requires configured tolerance >=400 Hz for disjoint frequency witnesses")
			}
			center = float64(spot.NewResolverSignalKey(center, "20m", mode, step*1000).Bucket) * step
			for key := -2; key <= 2; key++ {
				for receiver := 0; receiver < 64; receiver++ {
					for raw := 0; raw < 16; raw++ {
						group, alias := raw/8, raw%8
						base := []string{"DL1AAA", "DL1AAB"}[group]
						aliases := []string{base + "/F/2/3", base + "/F/3/2", base + "/2/F/3", base + "/2/3/F", base + "/3/F/2", base + "/3/2/F", "F/" + base + "/2/3", "2/" + base + "/F/3"}
						idx := raw*64 + receiver
						rx := fmt.Sprintf("DL9%c%c%c", 'A'+rune(idx/676), 'A'+rune((idx/26)%26), 'A'+rune(idx%26))
						if _, known := a.global[rx]; known {
							t.Fatalf("dense receiver %s unexpectedly collides with configured global reliability", rx)
						}
						if _, known := a.cw[rx]; known {
							t.Fatalf("dense receiver %s unexpectedly collides with configured CW reliability", rx)
						}
						if _, known := a.rtty[rx]; known {
							t.Fatalf("dense receiver %s unexpectedly collides with configured RTTY reliability", rx)
						}
						frequency := center + float64(key)*step + []float64{-0.16, 0.16}[group]
						inputs = append(inputs, ccQualInput{aliases[alias], rx, mode, frequency, (receiver+raw)%41 - 5})
					}
				}
			}
		}
		return inputs, "CW/RTTY; five adjacent keys per mode; 16 raw slash spellings, two disjoint canonical groups of 512 synthetic default-weight receivers per key; +/-160 Hz frequency witnesses; 64 receivers per raw spelling"
	}
	for _, mode := range []string{"CW", "RTTY"} {
		receivers := ccQualReceivers(t, a, mode)
		for key := 0; key < 32; key++ {
			prefix := []string{"DL", "JA", "G", "VK", "OH", "EA"}[key%6]
			base := fmt.Sprintf("%s%dA%c", prefix, key%8+1, 'A'+rune(key/6))
			for row := 0; row < 8; row++ {
				tie := key%16 == 0 || name == "drain_recovery"
				variant, receiver := 0, row
				if row >= 5 || (tie && row >= 4) {
					variant = 1
				}
				if tie {
					receiver %= 4 // Equal receiver/weight sets form the small exact tie cohort.
				}
				frequency := 14020.0 + float64(key)*0.8 + float64(variant)*0.03
				if mode == "RTTY" {
					frequency += 60
				}
				inputs = append(inputs, ccQualInput{base + string(rune('A'+variant)), receivers[(receiver+key)%len(receivers)], mode, frequency, (row + key) % 36})
			}
		}
	}
	return inputs, "CW/RTTY; 32 refreshed keys per mode; valid non-US DX identities; configured admitted receiver weights; 5:3 ordinary support and 1/16 exact tied keys (all keys tied for drain_recovery)"
}

func ccQualReceivers(t *testing.T, a *ccQualAssets, mode string) []string {
	t.Helper()
	modeTable := a.cw
	if mode == "RTTY" {
		modeTable = a.rtty
	}
	known := make(map[string]float64, len(a.global)+len(modeTable))
	for receiver, weight := range a.global {
		known[receiver] = weight
	}
	for receiver, weight := range modeTable {
		known[receiver] = weight
	}
	var receivers []string
	for receiver, weight := range known {
		if weight >= a.cfg.CallCorrection.MinSpotterReliability && spot.IsValidCallsign(receiver) {
			receivers = append(receivers, receiver)
		}
	}
	sort.Strings(receivers)
	if len(receivers) < 8 {
		t.Fatalf("qualification %s has fewer than eight admitted configured receivers", mode)
	}
	return receivers
}

func ccQualValidateDense(t *testing.T, a *ccQualAssets, inputs []ccQualInput) {
	t.Helper()
	type group struct {
		raw       map[string]int
		receivers map[string]struct{}
		frequency float64
	}
	keys := make(map[spot.ResolverSignalKey]map[string]*group)
	for _, input := range inputs {
		s := ccQualSpot(a, input, 0)
		evidence, ok := buildResolverEvidenceSnapshot(s, a.cfg.CallCorrection, nil, time.Now().UTC())
		if !ok {
			t.Fatal("dense setup failed to build resolver evidence")
		}
		identity := spot.CorrectionVoteKey(s.DXCallNorm)
		if keys[evidence.Key] == nil {
			keys[evidence.Key] = make(map[string]*group)
		}
		if keys[evidence.Key][identity] == nil {
			keys[evidence.Key][identity] = &group{raw: make(map[string]int), receivers: make(map[string]struct{}), frequency: input.Frequency}
		}
		g := keys[evidence.Key][identity]
		g.raw[s.DXCallNorm]++
		g.receivers[s.DECallNorm] = struct{}{}
		if input.Frequency != g.frequency {
			t.Fatal("dense setup has inconsistent frequency witnesses within one canonical group")
		}
	}
	if len(keys) != 10 {
		t.Fatalf("dense setup produced %d resolver keys, want five adjacent keys in each mode", len(keys))
	}
	for key, groups := range keys {
		if len(groups) != 2 {
			t.Fatalf("dense key %v produced %d semantic identities, want 2", key, len(groups))
		}
		var previous *group
		for identity, g := range groups {
			if len(g.raw) != 8 || len(g.receivers) != 512 {
				t.Fatalf("dense key %v identity %s: raw variants=%d receiver union=%d, want 8 and 512", key, identity, len(g.raw), len(g.receivers))
			}
			for raw, reports := range g.raw {
				if reports != 64 {
					t.Fatalf("dense raw %s has %d reports, want 64", raw, reports)
				}
			}
			if previous != nil {
				for receiver := range g.receivers {
					if _, overlap := previous.receivers[receiver]; overlap {
						t.Fatalf("dense canonical groups overlap at receiver %s", receiver)
					}
				}
				if math.Abs(g.frequency-previous.frequency) < 0.3 {
					t.Fatal("dense canonical groups lack separated frequency witnesses")
				}
			}
			previous = g
		}
	}
}
