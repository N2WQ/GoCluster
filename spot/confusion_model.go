package spot

import (
	"dxcluster/strutil"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
)

// ConfusionModel holds mode/SNR-binned substitution, deletion, and insertion
// counts learned offline from RBN analytics. It is immutable after load and safe
// for concurrent read-only use.
type ConfusionModel struct {
	modes       []string
	modeIndex   map[string]int
	snrEdges    []float64
	alphabet    []rune
	charIndex   map[rune]int
	unknownRune rune
	unknownIdx  int
	subCounts   [][][][]int64
	delCounts   [][][]int64
	insCounts   [][][]int64
	subRowSums  [][][]int64
	delSums     [][]int64
	insSums     [][]int64
}

type confusionModelFile struct {
	Modes       []string      `json:"modes"`
	SNREdges    []float64     `json:"snr_band_edges"`
	Alphabet    string        `json:"alphabet"`
	UnknownChar string        `json:"unknown_char"`
	SubCounts   [][][][]int64 `json:"sub_counts"`
	DelCounts   [][][]int64   `json:"del_counts"`
	InsCounts   [][][]int64   `json:"ins_counts"`
}

type confusionOpKind int

const (
	confusionOpMatch confusionOpKind = iota
	confusionOpSub
	confusionOpDel
	confusionOpIns
)

type confusionOp struct {
	kind confusionOpKind
	t    rune
	o    rune
}

// LoadConfusionModel parses and validates confusion_model.json output from the
// RBN analytics pipeline.
func LoadConfusionModel(path string) (*ConfusionModel, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("confusion model path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw confusionModelFile
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse confusion model: %w", err)
	}
	return buildConfusionModel(raw)
}

func buildConfusionModel(raw confusionModelFile) (*ConfusionModel, error) {
	if len(raw.Modes) == 0 {
		return nil, fmt.Errorf("confusion model: modes empty")
	}
	if len(raw.SNREdges) < 2 {
		return nil, fmt.Errorf("confusion model: snr_band_edges must contain at least 2 values")
	}
	for i := 0; i < len(raw.SNREdges)-1; i++ {
		if raw.SNREdges[i] >= raw.SNREdges[i+1] {
			return nil, fmt.Errorf("confusion model: snr_band_edges must be strictly ascending")
		}
	}
	alphabetRunes := []rune(raw.Alphabet)
	if len(alphabetRunes) == 0 {
		return nil, fmt.Errorf("confusion model: alphabet empty")
	}
	unknown := []rune(raw.UnknownChar)
	if len(unknown) != 1 {
		return nil, fmt.Errorf("confusion model: unknown_char must contain exactly one rune")
	}
	modeCount := len(raw.Modes)
	bandCount := len(raw.SNREdges) - 1
	alphaCount := len(alphabetRunes)

	if len(raw.SubCounts) != modeCount || len(raw.DelCounts) != modeCount || len(raw.InsCounts) != modeCount {
		return nil, fmt.Errorf("confusion model: mode dimension mismatch")
	}

	for m := 0; m < modeCount; m++ {
		if len(raw.SubCounts[m]) != bandCount || len(raw.DelCounts[m]) != bandCount || len(raw.InsCounts[m]) != bandCount {
			return nil, fmt.Errorf("confusion model: SNR band dimension mismatch for mode index %d", m)
		}
		for b := 0; b < bandCount; b++ {
			if len(raw.SubCounts[m][b]) != alphaCount {
				return nil, fmt.Errorf("confusion model: sub_counts alphabet dimension mismatch (mode=%d band=%d)", m, b)
			}
			for t := 0; t < alphaCount; t++ {
				if len(raw.SubCounts[m][b][t]) != alphaCount {
					return nil, fmt.Errorf("confusion model: sub_counts row mismatch (mode=%d band=%d row=%d)", m, b, t)
				}
			}
			if len(raw.DelCounts[m][b]) != alphaCount || len(raw.InsCounts[m][b]) != alphaCount {
				return nil, fmt.Errorf("confusion model: del/ins alphabet dimension mismatch (mode=%d band=%d)", m, b)
			}
		}
	}

	modeIndex := make(map[string]int, modeCount)
	for i, mode := range raw.Modes {
		mode = strutil.NormalizeUpper(mode)
		if mode == "" {
			return nil, fmt.Errorf("confusion model: empty mode name")
		}
		if _, ok := modeIndex[mode]; ok {
			return nil, fmt.Errorf("confusion model: duplicate mode %q", mode)
		}
		modeIndex[mode] = i
		raw.Modes[i] = mode
	}

	charIndex := make(map[rune]int, alphaCount)
	unknownIdx := -1
	for i, r := range alphabetRunes {
		if _, ok := charIndex[r]; ok {
			return nil, fmt.Errorf("confusion model: duplicate alphabet rune %q", r)
		}
		charIndex[r] = i
		if r == unknown[0] {
			unknownIdx = i
		}
	}
	if unknownIdx < 0 {
		return nil, fmt.Errorf("confusion model: unknown_char %q not found in alphabet", string(unknown[0]))
	}

	subRowSums := make([][][]int64, modeCount)
	delSums := make([][]int64, modeCount)
	insSums := make([][]int64, modeCount)
	for m := 0; m < modeCount; m++ {
		subRowSums[m] = make([][]int64, bandCount)
		delSums[m] = make([]int64, bandCount)
		insSums[m] = make([]int64, bandCount)
		for b := 0; b < bandCount; b++ {
			subRowSums[m][b] = make([]int64, alphaCount)
			for t := 0; t < alphaCount; t++ {
				var rowSum int64
				for o := 0; o < alphaCount; o++ {
					rowSum += raw.SubCounts[m][b][t][o]
				}
				subRowSums[m][b][t] = rowSum
			}
			for t := 0; t < alphaCount; t++ {
				delSums[m][b] += raw.DelCounts[m][b][t]
				insSums[m][b] += raw.InsCounts[m][b][t]
			}
		}
	}

	return &ConfusionModel{
		modes:       raw.Modes,
		modeIndex:   modeIndex,
		snrEdges:    append([]float64(nil), raw.SNREdges...),
		alphabet:    alphabetRunes,
		charIndex:   charIndex,
		unknownRune: unknown[0],
		unknownIdx:  unknownIdx,
		subCounts:   raw.SubCounts,
		delCounts:   raw.DelCounts,
		insCounts:   raw.InsCounts,
		subRowSums:  subRowSums,
		delSums:     delSums,
		insSums:     insSums,
	}, nil
}

// ScoreCandidate returns a log-likelihood-style score for observing `observed`
// given candidate true call `candidate` for a mode and SNR. Higher is better.
func (m *ConfusionModel) ScoreCandidate(observed, candidate, mode string, snr float64) float64 {
	if m == nil {
		return 0
	}
	mode = strutil.NormalizeUpper(mode)
	modeIdx, ok := m.modeIndex[mode]
	if !ok {
		return 0
	}
	bandIdx := m.snrBandIndex(snr)
	trueRunes := []rune(strutil.NormalizeUpper(candidate))
	obsRunes := []rune(strutil.NormalizeUpper(observed))
	if len(trueRunes) == 0 || len(obsRunes) == 0 {
		return 0
	}
	ops := alignConfusionCalls(trueRunes, obsRunes)
	if len(ops) == 0 {
		return 0
	}

	alpha := float64(len(m.alphabet))
	score := 0.0
	for _, op := range ops {
		switch op.kind {
		case confusionOpMatch:
			continue
		case confusionOpSub:
			ti := m.charIdx(op.t)
			oi := m.charIdx(op.o)
			count := float64(m.subCounts[modeIdx][bandIdx][ti][oi])
			denom := float64(m.subRowSums[modeIdx][bandIdx][ti]) + alpha
			score += math.Log((count + 1.0) / denom)
		case confusionOpDel:
			ti := m.charIdx(op.t)
			count := float64(m.delCounts[modeIdx][bandIdx][ti])
			denom := float64(m.delSums[modeIdx][bandIdx]) + alpha
			score += math.Log((count + 1.0) / denom)
		case confusionOpIns:
			oi := m.charIdx(op.o)
			count := float64(m.insCounts[modeIdx][bandIdx][oi])
			denom := float64(m.insSums[modeIdx][bandIdx]) + alpha
			score += math.Log((count + 1.0) / denom)
		}
	}
	return score
}

func (m *ConfusionModel) charIdx(r rune) int {
	if idx, ok := m.charIndex[r]; ok {
		return idx
	}
	return m.unknownIdx
}

func (m *ConfusionModel) snrBandIndex(snr float64) int {
	last := len(m.snrEdges) - 2
	for i := 0; i < len(m.snrEdges)-1; i++ {
		if m.snrEdges[i] < snr && snr <= m.snrEdges[i+1] {
			return i
		}
	}
	return last
}

func alignConfusionCalls(trueRunes, obsRunes []rune) []confusionOp {
	m := len(trueRunes)
	n := len(obsRunes)
	dp := make([][]int, m+1)
	for i := 0; i <= m; i++ {
		dp[i] = make([]int, n+1)
		dp[i][0] = i
	}
	for j := 0; j <= n; j++ {
		dp[0][j] = j
	}
	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if trueRunes[i-1] == obsRunes[j-1] {
				dp[i][j] = dp[i-1][j-1]
				continue
			}
			delCost := dp[i-1][j]
			insCost := dp[i][j-1]
			subCost := dp[i-1][j-1]
			dp[i][j] = 1 + minInt(delCost, insCost, subCost)
		}
	}

	ops := make([]confusionOp, 0, m+n)
	i := m
	j := n
	for i > 0 || j > 0 {
		if i == 0 {
			ops = append(ops, confusionOp{kind: confusionOpIns, o: obsRunes[j-1]})
			j--
			continue
		}
		if j == 0 {
			ops = append(ops, confusionOp{kind: confusionOpDel, t: trueRunes[i-1]})
			i--
			continue
		}
		if trueRunes[i-1] == obsRunes[j-1] {
			ops = append(ops, confusionOp{kind: confusionOpMatch, t: trueRunes[i-1], o: obsRunes[j-1]})
			i--
			j--
			continue
		}
		delCost := dp[i-1][j]
		insCost := dp[i][j-1]
		subCost := dp[i-1][j-1]
		minCost := minInt(delCost, insCost, subCost)
		if subCost == minCost {
			ops = append(ops, confusionOp{kind: confusionOpSub, t: trueRunes[i-1], o: obsRunes[j-1]})
			i--
			j--
			continue
		}
		if delCost == minCost {
			ops = append(ops, confusionOp{kind: confusionOpDel, t: trueRunes[i-1]})
			i--
			continue
		}
		ops = append(ops, confusionOp{kind: confusionOpIns, o: obsRunes[j-1]})
		j--
	}
	for l, r := 0, len(ops)-1; l < r; l, r = l+1, r-1 {
		ops[l], ops[r] = ops[r], ops[l]
	}
	return ops
}

func minInt(a, b, c int) int {
	if a <= b && a <= c {
		return a
	}
	if b <= a && b <= c {
		return b
	}
	return c
}
