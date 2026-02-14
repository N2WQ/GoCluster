package spot

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testConfusionModelRaw() confusionModelFile {
	return confusionModelFile{
		Modes:       []string{"CW"},
		SNREdges:    []float64{-999, 999},
		Alphabet:    "ABKC18?",
		UnknownChar: "?",
		SubCounts: [][][][]int64{
			{
				{
					/* A */ {0, 1, 1, 1, 1, 1, 1},
					/* B */ {1, 0, 1, 1, 1, 40, 1}, // B->8 likely
					/* K */ {1, 1, 0, 1, 1, 1, 1},
					/* C */ {1, 1, 1, 0, 1, 1, 1},
					/* 1 */ {1, 1, 1, 1, 0, 1, 1},
					/* 8 */ {1, 1, 1, 1, 1, 0, 1},
					/* ? */ {1, 1, 1, 1, 1, 1, 0},
				},
			},
		},
		DelCounts: [][][]int64{
			{
				{1, 1, 1, 1, 1, 1, 1},
			},
		},
		InsCounts: [][][]int64{
			{
				{1, 1, 1, 1, 1, 1, 1},
			},
		},
	}
}

func TestBuildConfusionModelRejectsInvalidShapes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(confusionModelFile) confusionModelFile
		want   string
	}{
		{
			name: "no_modes",
			mutate: func(raw confusionModelFile) confusionModelFile {
				raw.Modes = nil
				return raw
			},
			want: "modes empty",
		},
		{
			name: "non_ascending_snr_edges",
			mutate: func(raw confusionModelFile) confusionModelFile {
				raw.SNREdges = []float64{0, 0}
				return raw
			},
			want: "strictly ascending",
		},
		{
			name: "unknown_not_in_alphabet",
			mutate: func(raw confusionModelFile) confusionModelFile {
				raw.UnknownChar = "Z"
				return raw
			},
			want: "not found in alphabet",
		},
		{
			name: "sub_counts_dimension_mismatch",
			mutate: func(raw confusionModelFile) confusionModelFile {
				raw.SubCounts[0][0] = raw.SubCounts[0][0][:len(raw.SubCounts[0][0])-1]
				return raw
			},
			want: "alphabet dimension mismatch",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := tc.mutate(testConfusionModelRaw())
			_, err := buildConfusionModel(raw)
			if err == nil {
				t.Fatalf("expected error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %q", tc.want, err.Error())
			}
		})
	}
}

func TestLoadConfusionModelFromFile(t *testing.T) {
	_, err := LoadConfusionModel("")
	if err == nil {
		t.Fatalf("expected empty path error")
	}

	raw := testConfusionModelRaw()
	data, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal confusion model: %v", err)
	}
	path := filepath.Join(t.TempDir(), "confusion_model.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write confusion model: %v", err)
	}

	model, err := LoadConfusionModel(path)
	if err != nil {
		t.Fatalf("load confusion model: %v", err)
	}
	if model == nil {
		t.Fatalf("expected model")
	}
}

func TestConfusionModelScoreCandidatePrefersLikelySubstitution(t *testing.T) {
	model, err := buildConfusionModel(testConfusionModelRaw())
	if err != nil {
		t.Fatalf("build confusion model: %v", err)
	}

	observed := "K1A8C"
	likely := model.ScoreCandidate(observed, "K1ABC", "CW", 20)   // B->8 favored
	unlikely := model.ScoreCandidate(observed, "K1ACC", "CW", 20) // C->8 baseline
	if likely <= unlikely {
		t.Fatalf("expected likely score > unlikely score; likely=%f unlikely=%f", likely, unlikely)
	}

	unknownMode := model.ScoreCandidate(observed, "K1ABC", "USB", 20)
	if unknownMode != 0 {
		t.Fatalf("expected zero score for unknown mode, got %f", unknownMode)
	}
}
