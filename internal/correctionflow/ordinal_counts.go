// File role: Validate the frozen CW ordinal event model's fixed count contract.
// These estimates describe proxy-assigned edit events, not RF correctness.
package correctionflow

import (
	"fmt"
	"math"
)

const OrdinalAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/"
const ordinalCountLimit = uint64(16_000_000 * 33)

type OrdinalTrainingSource struct {
	DateUTC       string `json:"date_utc"`
	SHA256        string `json:"sha256"`
	FocalRows     uint64 `json:"focal_rows"`
	AcceptedPairs uint64 `json:"accepted_pairs"`
	FittedCWPairs uint64 `json:"fitted_cw_pairs"`
}

type OrdinalEventCell struct {
	Pairs             uint64         `json:"pairs"`
	ExactPairs        uint64         `json:"exact_pairs"`
	Matches           uint64         `json:"matches"`
	Substitutions     uint64         `json:"substitutions"`
	Deletions         uint64         `json:"deletions"`
	Insertions        uint64         `json:"insertions"`
	SourceCharacters  uint64         `json:"source_characters"`
	SubjectCharacters uint64         `json:"subject_characters"`
	GapStops          uint64         `json:"gap_stops"`
	Opportunities     [37]uint64     `json:"opportunities"`
	Events            [37][38]uint64 `json:"events"`
	InsertionSymbols  [37]uint64     `json:"insertion_symbols"`
}

// OrdinalAsset is a fixed, bounded input DTO. The replay wire loader must check
// exact dimensions/required facts before decoding: encoding/json alone accepts
// missing or surplus fixed-array elements. NewOrdinalModel then checks meaning.
type OrdinalAsset struct {
	SchemaVersion       int                      `json:"schema_version"`
	Experimental        bool                     `json:"experimental"`
	Mode                string                   `json:"mode"`
	Alphabet            string                   `json:"alphabet"`
	Alignment           string                   `json:"alignment"`
	ReferenceVersion    string                   `json:"reference_version"`
	SNREdges            [7]int                   `json:"snr_edges"`
	EmissionPriorTotal  uint64                   `json:"emission_prior_total"`
	InsertionPriorTotal uint64                   `json:"insertion_prior_total"`
	GapPriorAlpha       uint64                   `json:"gap_prior_alpha"`
	GapPriorBeta        uint64                   `json:"gap_prior_beta"`
	Sources             [2]OrdinalTrainingSource `json:"sources"`
	Cells               [6]OrdinalEventCell      `json:"cells"`
}

// NewOrdinalModel owns fixed numeric tables. Mutating the DTO after return
// cannot change scoring; no vocabulary/cache, worker or retained source strings
// are introduced. Invalid input never silently requests a geometry-only run.
func NewOrdinalModel(asset *OrdinalAsset) (*OrdinalModel, error) {
	if asset == nil || asset.SchemaVersion != 1 || !asset.Experimental || asset.Mode != "CW" ||
		asset.Alphabet != OrdinalAlphabet || asset.Alignment != "unit-suffix-MSDI-v1" || asset.ReferenceVersion != "ccv2-reference-v1" ||
		asset.SNREdges != [7]int{-999, 0, 6, 12, 18, 24, 999} ||
		asset.EmissionPriorTotal != 1 || asset.InsertionPriorTotal != 1 || asset.GapPriorAlpha != 1 || asset.GapPriorBeta != 1 {
		return nil, fmt.Errorf("ordinal asset: unsupported contract")
	}
	var pairs, sourcePairs uint64
	for i, source := range asset.Sources {
		date, hash, rows := "20261107", "2a78ec300acc2cfe29048201af4e971cad933db01c6f36b567ac7443b57ccd34", uint64(458495)
		if i == 1 {
			date, hash, rows = "20261114", "ad0c8f5a942bab03da052cbe91c2087e59ff566cbd48aa2be8be43b1140bff49", 882021
		}
		if source.DateUTC != date || source.SHA256 != hash || source.FocalRows != rows || source.AcceptedPairs > rows || source.FittedCWPairs > source.AcceptedPairs {
			return nil, fmt.Errorf("ordinal asset: training source mismatch")
		}
		sourcePairs += source.FittedCWPairs
	}
	// Validate every count before allocating fitted backing. Fixed table sums
	// cannot overflow uint64 after the per-scalar bound has been established.
	for i := range asset.Cells {
		if err := validateOrdinalCell(asset.Cells[i]); err != nil {
			return nil, fmt.Errorf("ordinal cell %d: %w", i, err)
		}
		pairs += asset.Cells[i].Pairs
	}
	if pairs != sourcePairs {
		return nil, fmt.Errorf("ordinal asset: source/cell pair mismatch")
	}
	model := &OrdinalModel{}
	for b := range asset.Cells {
		counts := &asset.Cells[b]
		cell := &model.cells[b]
		cell.opportunities, cell.gapStops = counts.Opportunities, counts.GapStops
		for i := range counts.Events {
			for j, count := range counts.Events[i] {
				cell.logEvents[i][j] = math.Log((float64(count) + 1.0/38) / (float64(counts.Opportunities[i]) + 1))
			}
		}
		for i, count := range counts.InsertionSymbols {
			cell.logInsert[i] = math.Log((float64(count) + 1.0/37) / (float64(counts.Insertions) + 1))
		}
		q := (float64(counts.Insertions) + 1) / (float64(counts.Insertions+counts.GapStops) + 2)
		cell.logContinue, cell.logStop = math.Log(q), math.Log1p(-q)
	}
	return model, nil
}

func validateOrdinalCell(c OrdinalEventCell) error {
	for _, count := range []uint64{c.Pairs, c.ExactPairs, c.Matches, c.Substitutions, c.Deletions, c.Insertions, c.SourceCharacters, c.SubjectCharacters, c.GapStops} {
		if count > ordinalCountLimit {
			return fmt.Errorf("count exceeds bound")
		}
	}
	var source, match, substitutions, deletions, insertions uint64
	for i := range c.Events {
		row := &c.Events[i]
		if c.Opportunities[i] > ordinalCountLimit || c.InsertionSymbols[i] > ordinalCountLimit {
			return fmt.Errorf("symbol count exceeds bound")
		}
		var total uint64
		for j, count := range row {
			if count > ordinalCountLimit {
				return fmt.Errorf("event count exceeds bound")
			}
			total += count
			switch j {
			case 37:
				deletions += count
			case i:
				match += count
			default:
				substitutions += count
			}
		}
		if total != c.Opportunities[i] {
			return fmt.Errorf("event row/opportunity mismatch")
		}
		source += total
		insertions += c.InsertionSymbols[i]
	}
	if source != c.SourceCharacters || match != c.Matches || substitutions != c.Substitutions || deletions != c.Deletions || insertions != c.Insertions ||
		c.SubjectCharacters != match+substitutions+insertions || c.GapStops != source+c.Pairs || c.ExactPairs > c.Pairs ||
		c.SourceCharacters < c.Pairs || c.SourceCharacters > 32*c.Pairs || c.SubjectCharacters < c.Pairs || c.SubjectCharacters > 32*c.Pairs {
		return fmt.Errorf("event/character/gap/pair accounting mismatch")
	}
	// Nonempty strings of at most 32 symbols have unit distance in [1,32]
	// exactly when unequal. Exact-pair metadata must be feasible as well as
	// bounded; otherwise impossible training claims could pass the row sums.
	nonexact, edits := c.Pairs-c.ExactPairs, substitutions+deletions+insertions
	if nonexact > edits || edits > 32*nonexact || c.ExactPairs > match {
		return fmt.Errorf("exact-pair/edit accounting mismatch")
	}
	return nil
}
