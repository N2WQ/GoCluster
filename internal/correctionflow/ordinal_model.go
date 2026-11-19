// File role: Fixed-table Bayesian-smoothed, single-alignment ordinal ranking.
// The score is not a normalized string likelihood, Bayes factor or RF posterior.
package correctionflow

import (
	"math"
	"slices"
	"sort"
)

type ordinalScoringCell struct {
	opportunities        [37]uint64
	gapStops             uint64
	logEvents            [37][38]float64
	logInsert            [37]float64
	logContinue, logStop float64
}

type OrdinalModel struct{ cells [6]ordinalScoringCell }

func ordinalSymbol(c byte) int {
	switch {
	case c >= 'A' && c <= 'Z':
		return int(c - 'A')
	case c >= '0' && c <= '9':
		return int(c-'0') + 26
	case c == '/':
		return 36
	}
	return -1
}

func ordinalSNRBin(mode string, report int, measured bool) int {
	if mode != "CW" || !measured {
		return -1
	}
	edges := [7]int{-999, 0, 6, 12, 18, 24, 999}
	for b := 0; b < 6; b++ {
		if report > edges[b] && report <= edges[b+1] {
			return b
		}
	}
	return -1
}

// Score uses one focal observation/SNR for every compared canonical candidate.
// A single fixed suffix alignment supplies events. Source opportunities, not
// errors or inserted-symbol frequency, govern the declared availability floor.
func (m *OrdinalModel) Score(candidate, observed, mode string, report int, measured bool) (float64, bool) {
	b := ordinalSNRBin(mode, report, measured)
	if m == nil || b < 0 || !frequencyModelCall(candidate) || !frequencyModelCall(observed) {
		return 0, false
	}
	cell := &m.cells[b]
	if cell.gapStops < 50 {
		return 0, false
	}
	for i := range candidate {
		if cell.opportunities[ordinalSymbol(candidate[i])] < 50 {
			return 0, false
		}
	}
	score := float64(len(candidate)+1) * cell.logStop
	walkOrdinalAlignment(candidate, observed, func(op byte, source, target int) {
		if op == 'I' {
			score += cell.logContinue + cell.logInsert[target]
		} else {
			score += cell.logEvents[source][target]
		}
	})
	return score, !math.IsNaN(score) && !math.IsInf(score, 0)
}

// RankFrequencyProposals scores the entire eligible bounded population before
// the gate beam. If even candidate nine is unavailable, every candidate retains
// geometry ordering. Missing evidence never receives a neutral winning score.
// The returned slice is owned and contains only proposal scalars.
func (m *OrdinalModel) RankFrequencyProposals(proposals []FrequencyProposal, observed, mode string, report int, measured bool) ([]FrequencyProposal, bool) {
	if len(proposals) > 80 {
		return nil, false
	}
	out := slices.Clone(proposals)
	var scores [80]float64
	available := len(out) > 0
	for i, proposal := range out {
		score, ok := m.Score(proposal.Identity, observed, mode, report, measured)
		scores[i], available = score, available && ok
	}
	indices := make([]int, len(out))
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		a, b := indices[i], indices[j]
		if available && scores[a] != scores[b] {
			return scores[a] > scores[b]
		}
		return frequencyGeometryAhead(out[a], out[b])
	})
	ranked := make([]FrequencyProposal, len(out))
	for i, index := range indices {
		ranked[i] = out[index]
	}
	return ranked, available
}

// walkOrdinalAlignment uses suffix unit distance and forward M/S/D/I precedence.
// Its fixed workspace handles at most 32 ASCII symbols; callers validate input.
func walkOrdinalAlignment(source, observed string, event func(op byte, source, target int)) {
	var cost [33][33]uint8
	m, n := len(source), len(observed)
	for i := m; i >= 0; i-- {
		for j := n; j >= 0; j-- {
			switch {
			case i == m:
				cost[i][j] = uint8(n - j)
			case j == n:
				cost[i][j] = uint8(m - i)
			default:
				sub := cost[i+1][j+1]
				if source[i] != observed[j] {
					sub++
				}
				cost[i][j] = min(sub, 1+cost[i+1][j], 1+cost[i][j+1])
			}
		}
	}
	i, j := 0, 0
	for i < m || j < n {
		switch {
		case i < m && j < n && source[i] == observed[j] && cost[i][j] == cost[i+1][j+1]:
			event('M', ordinalSymbol(source[i]), ordinalSymbol(observed[j]))
			i++
			j++
		case i < m && j < n && cost[i][j] == 1+cost[i+1][j+1]:
			event('S', ordinalSymbol(source[i]), ordinalSymbol(observed[j]))
			i++
			j++
		case i < m && cost[i][j] == 1+cost[i+1][j]:
			event('D', ordinalSymbol(source[i]), 37)
			i++
		default:
			event('I', -1, ordinalSymbol(observed[j]))
			j++
		}
	}
}
