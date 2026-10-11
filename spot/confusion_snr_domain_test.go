// File role: Checks the approved SNR domain and preserved in-domain confusion
// scores against literal count tables, independently of resolver ranking.
package spot

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
)

const b22SNRDomainModelJSON = `{
 "modes":["CW"], "snr_band_edges":[-10,0,10], "alphabet":"AB?", "unknown_char":"?",
 "sub_counts":[[
  [[0,3,4],[1,0,0],[0,0,0]],
  [[0,7,0],[6,0,0],[0,0,0]]
 ]],
 "del_counts":[[[1,4,0],[0,1,0]]],
 "ins_counts":[[[2,2,0],[0,3,0]]]
}`

func b22SNRDomainModels(t testing.TB) map[string]*ConfusionModel {
	t.Helper()
	var raw confusionModelFile
	if err := json.Unmarshal([]byte(b22SNRDomainModelJSON), &raw); err != nil {
		t.Fatal(err)
	}
	direct, err := buildConfusionModel(raw)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "literal-confusion-model.json")
	if err := os.WriteFile(path, []byte(b22SNRDomainModelJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfusionModel(path)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]*ConfusionModel{"direct": direct, "public_loader": loaded}
}

func TestB22ConfusionSNRExclusiveLowerInclusiveUpper(t *testing.T) {
	// For the A->B substitution, the first band has count 3 and row sum 7;
	// Laplace smoothing over three characters gives 4/10. The second band
	// has count 7 and row sum 7, giving 8/10. These literals do not use the
	// model's scoring or band-selection implementation to construct the oracle.
	for origin, model := range b22SNRDomainModels(t) {
		for _, tc := range []struct {
			name string
			snr  float64
			want float64
		}{
			{"nan", math.NaN(), 0},
			{"positive_inf", math.Inf(1), 0},
			{"negative_inf", math.Inf(-1), 0},
			{"below_lower", math.Nextafter(-10, math.Inf(-1)), 0},
			{"exact_lower_excluded", -10, 0},
			{"just_above_lower", math.Nextafter(-10, math.Inf(1)), math.Log(4.0 / 10)},
			{"just_below_middle", math.Nextafter(0, math.Inf(-1)), math.Log(4.0 / 10)},
			{"reported_zero_middle_upper", 0, math.Log(4.0 / 10)},
			{"just_above_middle", math.Nextafter(0, math.Inf(1)), math.Log(8.0 / 10)},
			{"exact_upper_included", 10, math.Log(8.0 / 10)},
			{"above_upper", math.Nextafter(10, math.Inf(1)), 0},
		} {
			t.Run(origin+"/"+tc.name, func(t *testing.T) {
				var workspace confusionAlignmentWorkspace
				_ = model.scorePreparedCandidate([]rune("B"), []rune("A"), "CW", 0, &workspace)
				for _, score := range []struct {
					name string
					got  float64
				}{
					{"public", model.ScoreCandidate("B", "A", "CW", tc.snr)},
					{"prepared", model.scorePreparedCandidate([]rune("B"), []rune("A"), "CW", tc.snr, nil)},
					{"prepared_reused_workspace", model.scorePreparedCandidate([]rune("B"), []rune("A"), "CW", tc.snr, &workspace)},
				} {
					if math.IsNaN(score.got) || math.IsInf(score.got, 0) || math.Abs(score.got-tc.want) > 1e-15 {
						t.Errorf("%s SNR=%v score=%v, want %v", score.name, tc.snr, score.got, tc.want)
					}
				}
			})
		}
	}
}

func TestB22ConfusionDomainPreservesOperationLiterals(t *testing.T) {
	for origin, model := range b22SNRDomainModels(t) {
		for _, tc := range []struct {
			name, observed, candidate string
			snr                       float64
			want                      float64
		}{
			{"first_band_substitution", "B", "A", 0, math.Log(4.0 / 10)},
			{"first_band_reverse_substitution", "A", "B", 0, math.Log(2.0 / 4)},
			{"first_band_deletion", "A", "AB", 0, math.Log(5.0 / 8)},
			{"first_band_insertion", "AB", "A", 0, math.Log(3.0 / 7)},
			{"second_band_substitution", "B", "A", 10, math.Log(8.0 / 10)},
			{"second_band_deletion", "A", "AB", 10, math.Log(2.0 / 4)},
			{"second_band_insertion", "AB", "A", 10, math.Log(4.0 / 6)},
			{"matching_identity", "AB", "AB", 0, 0},
			{"unknown_mode", "B", "A", 0, 0},
		} {
			t.Run(origin+"/"+tc.name, func(t *testing.T) {
				mode := "CW"
				if tc.name == "unknown_mode" {
					mode = "USB"
				}
				got := model.ScoreCandidate(tc.observed, tc.candidate, mode, tc.snr)
				if math.IsNaN(got) || math.IsInf(got, 0) || math.Abs(got-tc.want) > 1e-15 {
					t.Errorf("score=%v, want independent literal %v", got, tc.want)
				}
				for _, ineligible := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -10, 11} {
					if got := model.ScoreCandidate(tc.observed, tc.candidate, mode, ineligible); got != 0 {
						t.Errorf("SNR=%v contributes %v outside the declared domain", ineligible, got)
					}
				}
			})
		}
	}
}
