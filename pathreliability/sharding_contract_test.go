package pathreliability

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreShardSelectorLiteralVectors(t *testing.T) {
	store := NewStore(DefaultConfig(), []string{"20m"})
	// Expected indices were calculated separately with unsigned 64-bit Python
	// arithmetic. Do not derive them from the Go mixer or its packed-key helpers.
	for _, vector := range []struct {
		key   uint64
		index int
	}{
		{0x0000000100020000, 22},
		{0x0003000a00140000, 10},
		{0x0000000100020001, 1},
		{0x0000000000010002, 25},
		{0xffffffffffff0000, 53},
		{0x0001000100010000, 56},
		{0x0000000003e803e9, 1},
	} {
		t.Run(fmt.Sprintf("%016x", vector.key), func(t *testing.T) {
			if got := store.shardForKey(vector.key); got != &store.shards[vector.index] {
				t.Fatalf("key %016x selected shard %p, want index %d (%p)", vector.key, got, vector.index, &store.shards[vector.index])
			}
		})
	}
}

func TestStoreShardSelectorMappedPopulations(t *testing.T) {
	cfg := shardingConfiguredConfig(t)
	cells := shardingMappedCells(t)
	userFine, dxFine, userCoarse, dxCoarse := requireDistinctPathCells(t, "FN31", "DM79")
	store := NewStore(cfg, cfg.AllowedBands)
	indices := make(map[*shard]int, len(store.shards))
	for i := range store.shards {
		indices[&store.shards[i]] = i
	}
	for _, layer := range []string{"fine", "coarse"} {
		for _, shape := range []string{"fixed_receiver", "fixed_sender", "same_cell", "forward_reverse", "mixed_bands", "band_varied"} {
			t.Run(layer+"/"+shape, func(t *testing.T) {
				seen := make(map[uint64]struct{})
				var counts [64]int
				add := func(receiver, sender CellID, band uint16) {
					key := packKey(receiver, sender, band)
					if layer == "coarse" {
						key = packCoarseKey(receiver, sender, band)
					}
					if _, duplicate := seen[key]; duplicate {
						return
					}
					seen[key] = struct{}{}
					index, ok := indices[store.shardForKey(key)]
					if !ok {
						t.Fatal("selector returned a shard outside its store")
					}
					counts[index]++
				}
				receiver, sender := userFine, dxFine
				if layer == "coarse" {
					receiver, sender = userCoarse, dxCoarse
				}
				if shape == "band_varied" {
					for _, band := range cfg.AllowedBands {
						index, _ := store.bandIndex.Lookup(band)
						add(receiver, sender, index)
					}
				} else {
					for i, cell := range cells {
						id := cell.fine
						if layer == "coarse" {
							id = cell.coarse
						}
						index, _ := store.bandIndex.Lookup("20m")
						switch shape {
						case "fixed_receiver":
							add(receiver, id, index)
						case "fixed_sender":
							add(id, sender, index)
						case "same_cell":
							add(id, id, index)
						case "forward_reverse":
							add(receiver, id, index)
							add(id, receiver, index)
						case "mixed_bands":
							for _, band := range cfg.AllowedBands {
								index, _ := store.bandIndex.Lookup(band)
								other := cells[(i+1)%len(cells)].fine
								if layer == "coarse" {
									other = cells[(i+1)%len(cells)].coarse
								}
								add(id, other, index)
							}
						}
					}
				}
				occupied, maximum := 0, 0
				for _, count := range counts {
					if count > 0 {
						occupied++
					}
					maximum = max(maximum, count)
				}
				if shape == "band_varied" {
					if len(seen) != len(cfg.AllowedBands) || occupied < 6 {
						t.Fatalf("fixed path across configured bands: keys=%d occupied=%d counts=%v", len(seen), occupied, counts)
					}
				} else if len(seen) < 200 || occupied < 48 || maximum*8 > len(seen) {
					t.Fatalf("mapped population collapsed: keys=%d occupied=%d maximum=%d counts=%v", len(seen), occupied, maximum, counts)
				}
			})
		}
	}
}

func TestPredictorShardedReadAfterWrite(t *testing.T) {
	cfg := shardingConfiguredConfig(t)
	userFine, dxFine, userCoarse, dxCoarse := requireDistinctPathCells(t, "FN31", "DM79")
	now := time.Date(2026, time.October, 14, 20, 0, 0, 0, time.UTC)
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		for _, route := range []struct {
			name     string
			receive  bool
			transmit bool
			beacon   bool
			wantP50  float64
			wantRaw  uint32
		}{
			{"forward", true, false, false, -10.5, 24},
			{"reverse", false, true, false, -20.5, 24},
			{"bidirectional", true, true, false, -10.5, 48},
			{"beacon", true, true, true, -10.5, 24},
		} {
			t.Run(mode+"/"+route.name, func(t *testing.T) {
				modeConfig := cfg
				modeConfig.ReceiverContributionMode = mode
				modeConfig.ReceiverMaxEffectiveCount = 8
				modeConfig.ReceiverMaxEffectiveWeight = 8
				predictor := NewPredictor(modeConfig, cfg.AllowedBands)
				for _, band := range cfg.AllowedBands {
					for report := 0; report < 24; report++ {
						hash := uint64(report/8 + 1)
						if route.receive {
							admission := predictor.UpdateWithReceiverHash(BucketCombined, userFine, dxFine, userCoarse, dxCoarse, band, -10.5, 1, ObservationTimes{ObservedAt: now, ArrivedAt: now}, route.beacon, hash)
							if admission.Status != AdmissionAccepted {
								t.Fatalf("forward %s report was not admitted: %+v", band, admission)
							}
						}
						if route.transmit {
							admission := predictor.UpdateWithReceiverHash(BucketCombined, dxFine, userFine, dxCoarse, userCoarse, band, -20.5, 1, ObservationTimes{ObservedAt: now, ArrivedAt: now}, false, hash)
							if admission.Status != AdmissionAccepted {
								t.Fatalf("reverse %s report was not admitted: %+v", band, admission)
							}
						}
					}
					result := predictor.Predict(userFine, dxFine, userCoarse, dxCoarse, band, "FT8", 0, now)
					if route.beacon {
						result = predictor.PredictBeaconReceiveOnlyWithMinObservationCount(userFine, dxFine, userCoarse, dxCoarse, band, "FT8", 0, modeConfig.BeaconMinObservationCount, now)
					}
					if result.Source != SourceCombined || !result.HasP50 || result.P50DB != route.wantP50 || result.RawCount != route.wantRaw || result.BeaconRX != route.beacon || !result.ObservedDistributionAvailable || !result.HasObservedSamplingTime || result.ObservedSamplingUnix != now.Unix() {
						t.Fatalf("%s read-after-write mismatch: %+v; want p50=%v raw=%d beacon=%v", band, result, route.wantP50, route.wantRaw, route.beacon)
					}
				}
				if got := predictor.AdmissionStats(); got != (AdmissionStats{}) {
					t.Fatalf("valid routing corpus changed admission outcomes: %+v", got)
				}
			})
		}
	}
}

type shardingMappedCell struct{ fine, coarse CellID }

func shardingConfiguredConfig(t *testing.T) Config {
	t.Helper()
	cfg, err := LoadFile(filepath.Join("..", "data", "config", "path_reliability.yaml"))
	if err != nil {
		t.Fatalf("load configured routing bands: %v", err)
	}
	if len(cfg.AllowedBands) < 8 {
		t.Fatal("configured band-variation fixture requires at least eight bands")
	}
	return cfg
}

func shardingMappedCells(t *testing.T) []shardingMappedCell {
	t.Helper()
	requireH3Mappings(t)
	cells := make([]shardingMappedCell, 0, 324)
	for longitude := 0; longitude < 18; longitude++ {
		for latitude := 0; latitude < 18; latitude++ {
			grid := fmt.Sprintf("%c%c55", 'A'+longitude, 'A'+latitude)
			cell := shardingMappedCell{EncodeCell(grid), EncodeCoarseCell(grid)}
			if cell.fine == InvalidCell || cell.coarse == InvalidCell || fineMapper.ToH3[cell.fine] == 0 || coarseMapper.ToH3[cell.coarse] == 0 {
				t.Fatalf("global grid %s did not produce real fine/coarse H3 proxy IDs: %+v", grid, cell)
			}
			cells = append(cells, cell)
		}
	}
	return cells
}
