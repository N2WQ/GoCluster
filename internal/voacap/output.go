package voacap

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

const ft8ReferenceBandwidthHz = 2500.0

var method30NumberRE = regexp.MustCompile(`[-+]?\d+(?:\.\d+)?`)

// PredictionRecord is one parsed VOACAP method-30 prediction cell for an
// experiment hour and frequency.
type PredictionRecord struct {
	HourUTC        int
	FrequencyMHz   float64
	VOACAPSNRDBHz  int
	FT8SNRDB       int
	Reliability    float64
	HasReliability bool
}

// FT8EquivalentSNRDB converts VOACAP dB-Hz SNR to the WSJT-X FT8 dB value
// referenced to a 2500 Hz bandwidth.
func FT8EquivalentSNRDB(voacapSNRDBHz int) int {
	return int(math.Round(float64(voacapSNRDBHz) - 10*math.Log10(ft8ReferenceBandwidthHz)))
}

// ParseMethod30Predictions extracts FREQ, SNR, and optional REL rows from
// VOACAP method-30 text output. VOACAP prints a leading best-frequency column
// before the configured frequency slots, so this parser skips that column and
// returns only positive configured frequency cells.
func ParseMethod30Predictions(output []byte) ([]PredictionRecord, error) {
	if len(bytes.TrimSpace(output)) == 0 {
		return nil, errors.New("VOACAP output is empty")
	}

	var records []PredictionRecord
	var current *method30Block
	scanner := bufio.NewScanner(bytes.NewReader(output))
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		switch method30RowKind(line) {
		case "FREQ":
			if err := appendMethod30Block(&records, current); err != nil {
				return nil, err
			}
			block, err := parseMethod30FrequencyRow(line, lineNumber)
			if err != nil {
				return nil, err
			}
			current = &block
		case "SNR":
			if current == nil {
				return nil, fmt.Errorf("VOACAP SNR row at line %d appears before a FREQ row", lineNumber)
			}
			cells, err := method30NumericCells(line)
			if err != nil {
				return nil, fmt.Errorf("parse SNR row at line %d: %w", lineNumber, err)
			}
			current.snr = roundCells(cells)
			current.snrLine = lineNumber
		case "REL":
			if current == nil {
				return nil, fmt.Errorf("VOACAP REL row at line %d appears before a FREQ row", lineNumber)
			}
			cells, err := method30NumericCells(line)
			if err != nil {
				return nil, fmt.Errorf("parse REL row at line %d: %w", lineNumber, err)
			}
			current.rel = cells
			current.relLine = lineNumber
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan VOACAP output: %w", err)
	}
	if err := appendMethod30Block(&records, current); err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, errors.New("VOACAP output did not contain method-30 FREQ/SNR prediction rows")
	}
	return records, nil
}

type method30Block struct {
	hour        int
	frequencies []float64
	freqLine    int
	snr         []int
	snrLine     int
	rel         []float64
	relLine     int
}

func parseMethod30FrequencyRow(line string, lineNumber int) (method30Block, error) {
	cells, err := method30NumericCells(line)
	if err != nil {
		return method30Block{}, fmt.Errorf("parse FREQ row at line %d: %w", lineNumber, err)
	}
	if len(cells) < 3 {
		return method30Block{}, fmt.Errorf("FREQ row at line %d has %d numeric cells, want at least 3", lineNumber, len(cells))
	}
	hour := int(math.Round(cells[0]))
	if hour < 0 || hour > 24 || math.Abs(cells[0]-float64(hour)) > 0.05 {
		return method30Block{}, fmt.Errorf("FREQ row at line %d has invalid hour %.2f", lineNumber, cells[0])
	}
	frequencies := make([]float64, 0, len(cells)-2)
	for _, cell := range cells[2:] {
		if cell <= 0 {
			break
		}
		frequencies = append(frequencies, cell)
	}
	if len(frequencies) == 0 {
		return method30Block{}, fmt.Errorf("FREQ row at line %d has no positive configured frequency cells", lineNumber)
	}
	return method30Block{hour: hour, frequencies: frequencies, freqLine: lineNumber}, nil
}

func appendMethod30Block(records *[]PredictionRecord, block *method30Block) error {
	if block == nil {
		return nil
	}
	if len(block.snr) == 0 {
		return fmt.Errorf("FREQ row at line %d has no matching SNR row before next block", block.freqLine)
	}
	requiredCells := len(block.frequencies) + 1
	if len(block.snr) < requiredCells {
		return fmt.Errorf("SNR row at line %d has %d numeric cells, want at least %d for FREQ row at line %d", block.snrLine, len(block.snr), requiredCells, block.freqLine)
	}
	hasReliability := len(block.rel) > 0
	if hasReliability && len(block.rel) < requiredCells {
		return fmt.Errorf("REL row at line %d has %d numeric cells, want at least %d for FREQ row at line %d", block.relLine, len(block.rel), requiredCells, block.freqLine)
	}

	for i, frequency := range block.frequencies {
		snr := block.snr[i+1]
		record := PredictionRecord{
			HourUTC:       block.hour,
			FrequencyMHz:  frequency,
			VOACAPSNRDBHz: snr,
			FT8SNRDB:      FT8EquivalentSNRDB(snr),
		}
		if hasReliability {
			record.Reliability = block.rel[i+1]
			record.HasReliability = true
		}
		*records = append(*records, record)
	}
	return nil
}

func method30RowKind(line string) string {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return ""
	}
	switch fields[len(fields)-1] {
	case "FREQ", "SNR", "REL":
		return fields[len(fields)-1]
	default:
		return ""
	}
}

func method30NumericCells(line string) ([]float64, error) {
	matches := method30NumberRE.FindAllString(line, -1)
	cells := make([]float64, 0, len(matches))
	for _, match := range matches {
		cell, err := strconv.ParseFloat(match, 64)
		if err != nil {
			return nil, err
		}
		cells = append(cells, cell)
	}
	return cells, nil
}

func roundCells(cells []float64) []int {
	rounded := make([]int, 0, len(cells))
	for _, cell := range cells {
		rounded = append(rounded, int(math.Round(cell)))
	}
	return rounded
}
