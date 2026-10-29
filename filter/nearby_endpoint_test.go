package filter

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"dxcluster/internal/testsupport"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func nearbyEndpointFilter(t *testing.T) *Filter {
	t.Helper()
	return nativeRuleFilter(t, RuleDefinition{0, RulePass, "NEARBY"})
}

func TestNearbyIndependentEndpoints(t *testing.T) {
	requireH3Mappings(t)
	for _, frequency := range []float64{14020, 1820, 3520, 5355} {
		for _, test := range []struct {
			dx, de string
			want   bool
		}{
			{"FN31", "", true}, {"", "FN31", true},
			{"FN31", "BAD", true}, {"BAD", "FN31", true},
			{"FN31", "EM10", true}, {"EM10", "FN31", true},
			{"", "", false}, {"BAD", "BAD", false},
			{"EM10", "", false}, {"", "EM10", false}, {"EM10", "EM10", false},
		} {
			t.Run(fmt.Sprintf("%.0f/%s/%s", frequency, test.dx, test.de), func(t *testing.T) {
				f := nearbyEndpointFilter(t)
				s := spot.NewSpotNormalized("W6ABC", "N0XYZ", frequency, "CW")
				s.DXMetadata = spot.CallMetadata{Grid: test.dx, Continent: "NA"}
				s.DEMetadata = spot.CallMetadata{Grid: test.de, Continent: "NA"}
				if got := f.MatchesWithContext(s, nearbyEndpointContext()); got != test.want {
					t.Fatalf("Matches = %v, want %v", got, test.want)
				}
				if got := f.MatchesWithContext(s, nearbyEndpointContext()); got != test.want {
					t.Fatalf("MatchesWithPath = %v, want %v", got, test.want)
				}
				f.preparedRules, _ = f.preparedRules.WithRule(RuleDefinition{9, RuleReject, "MODE=CW"})
				if f.MatchesWithContext(s, nearbyEndpointContext()) {
					t.Fatal("nearby endpoint bypassed the unrelated mode refusal")
				}
			})
		}
	}
}

func TestNearbyCachePrecedenceAndBandResolution(t *testing.T) {
	requireH3Mappings(t)
	near, far := pathreliability.EncodeCell("FN31"), pathreliability.EncodeCell("EM10")
	if far == pathreliability.InvalidCell || near == far ||
		pathreliability.EncodeCoarseCell("FN31") == pathreliability.EncodeCoarseCell("EM10") {
		t.Fatal("cache fixture lacks distinct mapped nearby and distant controls")
	}
	for _, dxSide := range []bool{true, false} {
		for _, test := range []struct {
			grid      string
			cell      pathreliability.CellID
			frequency float64
			want      bool
		}{
			{"", near, 14020, true}, {"EM10", near, 14020, true},
			{"FN31", far, 14020, false},
			{"", near, 3520, false}, {"FN31", far, 3520, true},
			{"FN31", far, 1820, true}, {"FN31", far, 5355, true},
		} {
			t.Run(fmt.Sprintf("dx=%v/grid=%s/cell=%d/freq=%.0f", dxSide, test.grid, test.cell, test.frequency), func(t *testing.T) {
				s := spot.NewSpotNormalized("W6ABC", "N0XYZ", test.frequency, "CW")
				if dxSide {
					s.DXMetadata.Grid, s.DXCellID = test.grid, uint16(test.cell)
				} else {
					s.DEMetadata.Grid, s.DECellID = test.grid, uint16(test.cell)
				}
				if got := nearbyEndpointFilter(t).MatchesWithContext(s, nearbyEndpointContext()); got != test.want {
					t.Fatalf("Matches = %v, want %v", got, test.want)
				}
			})
		}
	}
}

func TestNearbyUnavailableUserAndUnknownBand(t *testing.T) {
	requireH3Mappings(t)
	f := nearbyEndpointFilter(t)
	s := spot.NewSpotNormalized("W6ABC", "N0XYZ", 0, "CW")
	s.DXMetadata.Grid = "FN31"
	if f.MatchesWithContext(s, nearbyEndpointContext()) {
		t.Fatal("unknown band passed NEARBY")
	}
	for _, frequency := range []float64{14020, 1820, 3520, 5355} {
		s = spot.NewSpotNormalized("W6ABC", "N0XYZ", frequency, "CW")
		s.DXMetadata.Grid = "FN31"
		if f.MatchesWithContext(s, EvaluationContext{}) {
			t.Fatalf("unavailable user cell passed on %.0f", frequency)
		}
	}
}

// The table loader is process-owned through sync.Once. A joined fresh child
// loads private count-valid tables with the distant cells removed, exercising a
// genuine mapping miss without altering the parent's maps or checked-in files.
func TestNearbyUnmappableOppositeEndpoint(t *testing.T) {
	const tableEnv = "GOCLUSTER_NEARBY_UNMAPPED_TABLES"
	const marker = "NEARBY_UNMAPPABLE_CHILD_EXECUTED"
	if directory := os.Getenv(tableEnv); directory != "" {
		if err := pathreliability.InitH3MappingsFromDir(directory); err != nil {
			t.Fatal(err)
		}
		if pathreliability.EncodeCell("FN31") == pathreliability.InvalidCell ||
			pathreliability.EncodeCoarseCell("FN31") == pathreliability.InvalidCell ||
			pathreliability.EncodeCell("EM10") != pathreliability.InvalidCell ||
			pathreliability.EncodeCoarseCell("EM10") != pathreliability.InvalidCell {
			t.Fatal("private tables did not establish the available/unmapped controls")
		}
		for _, frequency := range []float64{14020, 1820, 3520, 5355} {
			for _, dxSide := range []bool{true, false} {
				s := spot.NewSpotNormalized("W6ABC", "N0XYZ", frequency, "CW")
				s.DXMetadata.Grid, s.DEMetadata.Grid = "FN31", "EM10"
				if !dxSide {
					s.DXMetadata.Grid, s.DEMetadata.Grid = s.DEMetadata.Grid, s.DXMetadata.Grid
				}
				if !nearbyEndpointFilter(t).MatchesWithContext(s, nearbyEndpointContext()) {
					t.Fatalf("unmapped opposite vetoed matching endpoint on %.0f/dx=%v", frequency, dxSide)
				}
			}
		}
		t.Log(marker)
		return
	}
	requireH3Mappings(t)
	directory := t.TempDir()
	for _, table := range []struct {
		name string
		cell pathreliability.CellID
	}{
		{"res1.bin", pathreliability.EncodeCoarseCell("EM10")},
		{"res2.bin", pathreliability.EncodeCell("EM10")},
	} {
		if table.cell == pathreliability.InvalidCell {
			t.Fatal("original table cannot resolve distant control")
		}
		data, err := os.ReadFile(filepath.Join(testsupport.H3TableDir(t), table.name))
		if err != nil {
			t.Fatal(err)
		}
		offset := (int(table.cell) - 1) * 8
		clear(data[offset : offset+8])
		if err := os.WriteFile(filepath.Join(directory, table.name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, executable, "-test.run=^TestNearbyUnmappableOppositeEndpoint$", "-test.v")
	child.Env = append(os.Environ(), tableEnv+"="+directory)
	output, err := child.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte(marker)) {
		t.Fatalf("private mapping child was not qualified: %v\n%s", err, output)
	}
}

func nearbyEndpointContext() EvaluationContext {
	return EvaluationContext{FineCell: pathreliability.EncodeCell("FN31"), CoarseCell: pathreliability.EncodeCoarseCell("FN31")}
}

func TestNearbyExactBandFallbackAndPartialContext(t *testing.T) {
	requireH3Mappings(t)
	full := nearbyEndpointContext()
	far := pathreliability.EncodeCell("EM10")
	if full.FineCell == 0 || full.CoarseCell == 0 || far == 0 || far == full.FineCell {
		t.Fatal("invalid mapped controls")
	}
	f := nearbyEndpointFilter(t)
	for _, tc := range []struct {
		norm, raw string
		frequency float64
		context   EvaluationContext
		want      bool
	}{
		{"20m", "80m", 3520, EvaluationContext{FineCell: full.FineCell}, true},
		{"", "20m", 3520, EvaluationContext{FineCell: full.FineCell}, true},
		{"???", "20m", 3520, EvaluationContext{FineCell: full.FineCell}, false},
		{"???", "20m", 14020, EvaluationContext{FineCell: full.FineCell}, true},
		{"BOGUS", "80m", 0, EvaluationContext{FineCell: full.FineCell}, true},
		{"", "", 0, full, false},
		{"80m", "20m", 14020, EvaluationContext{CoarseCell: full.CoarseCell}, true},
		{"20m", "80m", 3520, EvaluationContext{CoarseCell: full.CoarseCell}, false},
	} {
		s := spot.NewSpotNormalized("W6ABC", "N0XYZ", tc.frequency, "CW")
		s.BandNorm, s.Band = tc.norm, tc.raw
		s.DXMetadata.Grid = "FN31"
		s.DXCellID = uint16(full.FineCell)
		if got := f.MatchesWithContext(s, tc.context); got != tc.want {
			t.Fatalf("norm=%q raw=%q freq=%g got=%v want=%v", tc.norm, tc.raw, tc.frequency, got, tc.want)
		}
	}
}
