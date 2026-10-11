package spot

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func b21WriteReliability(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "reliability.txt")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestB21ReliabilityLoaderRejectsNonfiniteBeforeClamping(t *testing.T) {
	for _, token := range []string{"NaN", "+Inf", "-Inf", "1e999", "-1e999"} {
		t.Run(token, func(t *testing.T) {
			path := b21WriteReliability(t, "# valid prefix does not excuse an invalid asset\nK1GOOD 0.25\nK1BAD "+token+"\nK1LATER 0.75\n")
			weights, _, err := LoadSpotterReliability(path)
			if err == nil {
				t.Fatalf("nonfinite reliability %q accepted or silently skipped: %v", token, weights)
			}
			for receiver, weight := range weights {
				if math.IsNaN(weight) || math.IsInf(weight, 0) {
					t.Errorf("error result retained invalid weight for %s: %v", receiver, weight)
				}
			}
		})
	}
}

func TestB21ReliabilityLoaderPreservesFiniteAndTextSemantics(t *testing.T) {
	path := b21WriteReliability(t, "# ignored\n\nK1DUP 0.25\nK1ZERO -2\nK1ONE 2\nmissing_weight\nK1TEXT not_a_number\nK1SYNTAX 1e\n k1dup 0.75 trailing fields remain ignored\nK1HALF 0.5\n")
	weights, applied, err := LoadSpotterReliability(path)
	if err != nil {
		t.Fatal(err)
	}
	want := SpotterReliability{"K1DUP": 0.75, "K1ZERO": 0, "K1ONE": 1, "K1HALF": 0.5}
	if applied != 5 || !reflect.DeepEqual(weights, want) {
		t.Fatalf("finite clamps, duplicate count or unrelated text parsing changed: applied=%d weights=%v want applied=5 weights=%v", applied, weights, want)
	}
}

func b21RawConfusionModel() confusionModelFile {
	// Two modes and two bands ensure unused tables still receive integrity
	// validation. Every returned fixture owns fresh slices.
	return confusionModelFile{
		Modes: []string{"CW", "RTTY"}, SNREdges: []float64{-10, 0, 10},
		Alphabet: "A?", UnknownChar: "?",
		SubCounts: [][][][]int64{
			{{{1, 3}, {0, 0}}, {{1, 3}, {0, 0}}},
			{{{1, 3}, {0, 0}}, {{1, 3}, {0, 0}}},
		},
		DelCounts: [][][]int64{{{1, 3}, {1, 3}}, {{1, 3}, {1, 3}}},
		InsCounts: [][][]int64{{{1, 3}, {1, 3}}, {{1, 3}, {1, 3}}},
	}
}

func b21WriteModelBytes(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "confusion_model.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func b21WriteModel(t *testing.T, raw confusionModelFile) string {
	t.Helper()
	data, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	return b21WriteModelBytes(t, data)
}

func b21RequireModelRejected(t *testing.T, raw confusionModelFile) {
	t.Helper()
	if model, err := buildConfusionModel(raw); err == nil || model != nil {
		t.Errorf("invalid direct model accepted: model=%v err=%v", model, err)
	}
	if model, err := LoadConfusionModel(b21WriteModel(t, raw)); err == nil || model != nil {
		t.Errorf("invalid persisted model accepted: model=%v err=%v", model, err)
	}
}

func TestB21ConfusionModelRejectsNegativeCounts(t *testing.T) {
	for mode := 0; mode < 2; mode++ {
		for band := 0; band < 2; band++ {
			for row := 0; row < 2; row++ {
				for column := 0; column < 2; column++ {
					t.Run(fmt.Sprintf("sub/%d/%d/%d/%d", mode, band, row, column), func(t *testing.T) {
						raw := b21RawConfusionModel()
						// Other positive entries can conceal the negative value in
						// a nonnegative total; each element must be checked.
						raw.SubCounts[mode][band][row][column] = -1
						b21RequireModelRejected(t, raw)
					})
				}
				for _, table := range []string{"del", "ins"} {
					t.Run(fmt.Sprintf("%s/%d/%d/%d", table, mode, band, row), func(t *testing.T) {
						raw := b21RawConfusionModel()
						if table == "del" {
							raw.DelCounts[mode][band][row] = -1
						} else {
							raw.InsCounts[mode][band][row] = -1
						}
						b21RequireModelRejected(t, raw)
					})
				}
			}
		}
	}
}

func TestB21ConfusionModelRejectsNonfiniteEdges(t *testing.T) {
	for position := 0; position < 3; position++ {
		for _, value := range []struct {
			name  string
			value float64
		}{{"nan", math.NaN()}, {"positive_inf", math.Inf(1)}, {"negative_inf", math.Inf(-1)}} {
			t.Run(fmt.Sprintf("direct/%d/%s", position, value.name), func(t *testing.T) {
				raw := b21RawConfusionModel()
				raw.SNREdges[position] = value.value
				if model, err := buildConfusionModel(raw); err == nil || model != nil {
					t.Errorf("nonfinite edge accepted: model=%v err=%v", model, err)
				}
			})
		}
		for _, token := range []string{"NaN", "+Inf", "-Inf", "1e309", "-1e309"} {
			t.Run(fmt.Sprintf("file/%d/%s", position, token), func(t *testing.T) {
				data, err := json.Marshal(b21RawConfusionModel())
				if err != nil {
					t.Fatal(err)
				}
				edges := []string{"-10", "0", "10"}
				edges[position] = token
				// JSON cannot represent nonfinite values. Literal file bytes
				// exercise decoder refusal rather than marshaler refusal.
				if strings.Count(string(data), `"snr_band_edges":[-10,0,10]`) != 1 {
					t.Fatal("literal nonfinite fixture did not locate its edge array")
				}
				data = []byte(strings.Replace(string(data), `"snr_band_edges":[-10,0,10]`, `"snr_band_edges":[`+strings.Join(edges, ",")+`]`, 1))
				if model, err := LoadConfusionModel(b21WriteModelBytes(t, data)); err == nil || model != nil {
					t.Errorf("nonfinite persisted edge %q accepted: model=%v err=%v", token, model, err)
				}
			})
		}
	}
}

func TestB21ConfusionModelRejectsNonascendingEdges(t *testing.T) {
	for _, edges := range [][]float64{{0, 0, 10}, {-10, 10, 10}, {-10, 0, 0}, {1, 0, 10}, {-10, 11, 10}, {-10, 0, -1}} {
		t.Run(fmt.Sprint(edges), func(t *testing.T) {
			raw := b21RawConfusionModel()
			raw.SNREdges = edges
			b21RequireModelRejected(t, raw)
		})
	}
}

func TestB21ConfusionModelPreservesFiniteAscendingEdges(t *testing.T) {
	for _, edges := range [][]float64{{-math.MaxFloat64, 0, math.MaxFloat64}, {-10, -1, 0}, {0, math.SmallestNonzeroFloat64, math.Nextafter(math.SmallestNonzeroFloat64, math.Inf(1))}} {
		t.Run(fmt.Sprint(edges), func(t *testing.T) {
			raw := b21RawConfusionModel()
			raw.SNREdges = edges
			if model, err := buildConfusionModel(raw); err != nil || model == nil {
				t.Errorf("finite strictly ascending direct edges rejected: model=%v err=%v", model, err)
			}
			if model, err := LoadConfusionModel(b21WriteModel(t, raw)); err != nil || model == nil {
				t.Errorf("finite strictly ascending persisted edges rejected: model=%v err=%v", model, err)
			}
		})
	}
}

func TestB21ConfusionModelCountTotalBoundaries(t *testing.T) {
	for _, table := range []string{"sub", "del", "ins"} {
		for _, overflow := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/overflow=%t", table, overflow), func(t *testing.T) {
				raw := b21RawConfusionModel()
				counts := []int64{math.MaxInt64 - 3, 3}
				if overflow {
					counts[0] = math.MaxInt64
				}
				switch table {
				case "sub":
					raw.SubCounts[1][1][1] = counts
				case "del":
					raw.DelCounts[1][1] = counts
				case "ins":
					raw.InsCounts[1][1] = counts
				}
				if overflow {
					b21RequireModelRejected(t, raw)
					return
				}
				for _, source := range []string{"direct", "file"} {
					var model *ConfusionModel
					var err error
					if source == "direct" {
						model, err = buildConfusionModel(raw)
					} else {
						model, err = LoadConfusionModel(b21WriteModel(t, raw))
					}
					if err != nil || model == nil {
						t.Fatalf("%s exact MaxInt64 total rejected: model=%v err=%v", source, model, err)
					}
					var total int64
					switch table {
					case "sub":
						total = model.subRowSums[1][1][1]
					case "del":
						total = model.delSums[1][1]
					case "ins":
						total = model.insSums[1][1]
					}
					if total != math.MaxInt64 {
						t.Errorf("%s total=%d want literal MaxInt64", source, total)
					}
				}
			})
		}
	}
}

func TestB21ConfusionModelIndependentFiniteVectors(t *testing.T) {
	for _, zero := range []bool{false, true} {
		t.Run(fmt.Sprintf("zero_counts=%t", zero), func(t *testing.T) {
			raw := b21RawConfusionModel()
			if zero {
				for mode := range raw.Modes {
					for band := range raw.DelCounts[mode] {
						clear(raw.DelCounts[mode][band])
						clear(raw.InsCounts[mode][band])
						for row := range raw.SubCounts[mode][band] {
							clear(raw.SubCounts[mode][band][row])
						}
					}
				}
			}
			model, err := LoadConfusionModel(b21WriteModel(t, raw))
			if err != nil {
				t.Fatal(err)
			}
			// Hand-derived additive-one smoothing with alphabet size two:
			// counts [1,3] give 2/6 and 4/6; zero counts give 1/2.
			// Literals are natural logs, with tolerance for float64 evaluation;
			// they establish arithmetic compatibility, not calibration.
			weak, strong := -1.0986122886681098, -0.40546510810816444
			if zero {
				weak, strong = -0.6931471805599453, -0.6931471805599453
			}
			for _, query := range []struct {
				name, observed, candidate string
				want                      float64
			}{
				{"match", "AA", "AA", 0},
				{"substitution", "?", "A", strong},
				{"zero_substitution_row", "A", "?", -0.6931471805599453},
				{"delete_A", "?", "A?", weak},
				{"delete_unknown", "A", "?A", strong},
				{"insert_A", "A?", "?", weak},
				{"insert_unknown", "?A", "A", strong},
			} {
				for _, mode := range []string{"CW", "RTTY"} {
					if got := model.ScoreCandidate(query.observed, query.candidate, mode, 5); math.IsNaN(got) || math.IsInf(got, 0) || math.Abs(got-query.want) > 1e-14 {
						t.Errorf("%s/%s score=%.17g want literal %.17g", query.name, mode, got, query.want)
					}
				}
			}
			if got := model.ScoreCandidate("?", "A", "USB", 5); got != 0 {
				t.Errorf("unused model mode score=%v want 0", got)
			}
		})
	}
}
