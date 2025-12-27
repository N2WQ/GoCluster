//go:build ignore
// +build ignore

// Standalone analysis helper for offline comparison of call-correction logs.
// Marked with an ignore build tag so it does not clash with the main server
// binary during normal builds.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

type Correction struct {
	Ts                        string  `json:"ts"`
	Strategy                  string  `json:"strategy"`
	FreqKhz                   float64 `json:"freq_khz"`
	Subject                   string  `json:"subject"`
	Winner                    string  `json:"winner"`
	Mode                      string  `json:"mode"`
	Source                    string  `json:"source"`
	TotalReporters            int     `json:"total_reporters"`
	SubjectSupport            int     `json:"subject_support"`
	WinnerSupport             int     `json:"winner_support"`
	RunnerUpSupport           int     `json:"runner_up_support"`
	SubjectConfidence         int     `json:"subject_confidence"`
	WinnerConfidence          int     `json:"winner_confidence"`
	Distance                  int     `json:"distance"`
	DistanceModel             string  `json:"distance_model"`
	MaxEditDistance           int     `json:"max_edit_distance"`
	MinReports                int     `json:"min_reports"`
	MinAdvantage              int     `json:"min_advantage"`
	MinConfidence             int     `json:"min_confidence"`
	D3ExtraReports            int     `json:"d3_extra_reports"`
	D3ExtraAdvantage          int     `json:"d3_extra_advantage"`
	D3ExtraConfidence         int     `json:"d3_extra_confidence"`
	FreqGuardMinSeparationKhz float64 `json:"freq_guard_min_separation_khz"`
	FreqGuardRunnerRatio      float64 `json:"freq_guard_runner_ratio"`
	Decision                  string  `json:"decision"`
	Reason                    string  `json:"reason,omitempty"`
}

type BustedEntry struct {
	Date        string
	Time        string
	BustedCall  string
	CorrectCall string
	FreqKhz     float64
	Mode        string
	RawLine     string
}

// Purpose: Offline entrypoint to compare correction logs against reference busted calls.
// Key aspects: Loads two log formats and prints summary statistics to stdout.
// Upstream: Invoked by `go run` for this ignore-tagged tool.
// Downstream: parseModifiedLog, parseBustedLog, analyzeCorrections.
func main() {
	fmt.Println("Loading logs...")

	corrections := parseModifiedLog("data/logs/callcorr_debug_modified.log")
	bustedEntries := parseBustedLog("data/logs/Busted-04-Dec-2025.txt")

	fmt.Printf("\nFound %d applied corrections in modified cluster\n", len(corrections))
	fmt.Printf("Found %d busted calls in reference cluster\n", len(bustedEntries))

	analyzeCorrections(corrections, bustedEntries)
}

// Purpose: Parse the modified cluster's correction log into structured entries.
// Key aspects: Scans for JSON lines where decision=applied.
// Upstream: main.
// Downstream: os.Open, bufio.Scanner, json.Unmarshal.
func parseModifiedLog(filepath string) []Correction {
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var corrections []Correction
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, `"decision":"applied"`) {
			// Find JSON start
			idx := strings.Index(line, "{")
			if idx >= 0 {
				jsonStr := line[idx:]
				var corr Correction
				if err := json.Unmarshal([]byte(jsonStr), &corr); err == nil {
					corrections = append(corrections, corr)
				}
			}
		}
	}

	return corrections
}

// Purpose: Parse a "busted calls" reference file into structured entries.
// Key aspects: Extracts the "?" marker row and normalizes the fields.
// Upstream: main.
// Downstream: os.Open, bufio.Scanner, strconv.ParseFloat.
func parseBustedLog(filepath string) []BustedEntry {
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var entries []BustedEntry
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, "?") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 7 {
			continue
		}

		// Find the ? marker
		qIdx := -1
		for i, f := range fields {
			if f == "?" {
				qIdx = i
				break
			}
		}

		if qIdx < 0 || qIdx+1 >= len(fields) {
			continue
		}

		freq, err := strconv.ParseFloat(fields[3], 64)
		if err != nil {
			continue
		}

		entry := BustedEntry{
			Date:        fields[0],
			Time:        fields[1],
			BustedCall:  fields[2],
			FreqKhz:     freq,
			CorrectCall: fields[qIdx+1],
			Mode:        fields[len(fields)-1],
			RawLine:     line,
		}
		entries = append(entries, entry)
	}

	return entries
}

// Purpose: Normalize callsigns for comparison.
// Key aspects: Uppercases and strips '-' and '/' before trimming.
// Upstream: analyzeCorrections.
// Downstream: strings.ToUpper, strings.ReplaceAll, strings.TrimSpace.
func normalizeCall(call string) string {
	call = strings.ToUpper(call)
	call = strings.ReplaceAll(call, "-", "")
	call = strings.ReplaceAll(call, "/", "")
	return strings.TrimSpace(call)
}

// Purpose: Convert a "HHMMZ" time token to minutes since midnight.
// Key aspects: Safe fallback to 0 on malformed inputs.
// Upstream: analyzeCorrections.
// Downstream: strconv.Atoi.
func timeToMinutes(timeStr string) int {
	// Format: 0000Z
	if len(timeStr) < 4 {
		return 0
	}
	hours, _ := strconv.Atoi(timeStr[0:2])
	mins, _ := strconv.Atoi(timeStr[2:4])
	return hours*60 + mins
}

// Purpose: Compare applied corrections against known busted entries.
// Key aspects: Matches by time/frequency window and reports accuracy buckets.
// Upstream: main.
// Downstream: normalizeCall, timeToMinutes, time.Parse, math.Abs, fmt.Printf.
func analyzeCorrections(corrections []Correction, bustedEntries []BustedEntry) {
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Println("ANALYSIS: Comparing Corrections with Reference Busted Calls")
	fmt.Println(strings.Repeat("=", 100))

	correctFixes := 0
	wrongFixes := 0
	possibleFalsePos := 0

	type Match struct {
		corr   Correction
		bust   *BustedEntry
		result string
	}

	var correctMatches, wrongMatches, falsePosMatches []Match

	// For each correction, try to find matching busted entry
	for _, corr := range corrections {
		ts, _ := time.Parse(time.RFC3339, corr.Ts)
		corrTimeMins := ts.Hour()*60 + ts.Minute()
		corrSubject := normalizeCall(corr.Subject)
		corrWinner := normalizeCall(corr.Winner)

		foundMatch := false

		for _, bust := range bustedEntries {
			bustTimeMins := timeToMinutes(bust.Time)
			bustBusted := normalizeCall(bust.BustedCall)
			bustCorrect := normalizeCall(bust.CorrectCall)

			timeDiff := math.Abs(float64(corrTimeMins - bustTimeMins))
			freqDiff := math.Abs(corr.FreqKhz - bust.FreqKhz)

			// Match criteria: within 5 min and 1 kHz
			if timeDiff <= 5 && freqDiff <= 1.0 {
				if corrSubject == bustBusted && corrWinner == bustCorrect {
					correctFixes++
					correctMatches = append(correctMatches, Match{corr, &bust, "CORRECT_FIX"})
					foundMatch = true
					break
				} else if corrSubject == bustBusted {
					wrongFixes++
					wrongMatches = append(wrongMatches, Match{corr, &bust, "WRONG_FIX"})
					foundMatch = true
					break
				}
			}
		}

		if !foundMatch {
			possibleFalsePos++
			falsePosMatches = append(falsePosMatches, Match{corr, nil, "POSSIBLE_FALSE_POSITIVE"})
		}
	}

	// Summary
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Println("SUMMARY STATISTICS")
	fmt.Println(strings.Repeat("=", 100))
	fmt.Printf("Total corrections applied by system:     %d\n", len(corrections))
	fmt.Printf("Total busted calls in reference:         %d\n", len(bustedEntries))
	fmt.Printf("Correct fixes (matched busted->correct): %d\n", correctFixes)
	fmt.Printf("Wrong fixes (busted call, wrong answer): %d\n", wrongFixes)
	fmt.Printf("Possible false positives:                %d\n", possibleFalsePos)

	if len(corrections) > 0 {
		accuracy := float64(correctFixes) / float64(len(corrections)) * 100
		fmt.Printf("\nCorrection Accuracy: %.1f%%\n", accuracy)
	}

	// Show examples of correct fixes
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Println("EXAMPLES OF CORRECT FIXES (first 20)")
	fmt.Println(strings.Repeat("=", 100))

	for i, match := range correctMatches {
		if i >= 20 {
			break
		}
		fmt.Printf("\n%d. CORRECT FIX:\n", i+1)
		fmt.Printf("   Time: %s (modified) vs %s %s (reference)\n",
			match.corr.Ts[:19], match.bust.Date, match.bust.Time)
		fmt.Printf("   Freq: %.1f kHz vs %.1f kHz\n",
			match.corr.FreqKhz, match.bust.FreqKhz)
		fmt.Printf("   Fixed: %s -> %s\n", match.corr.Subject, match.corr.Winner)
		fmt.Printf("   Reference: %s -> %s\n", match.bust.BustedCall, match.bust.CorrectCall)
		fmt.Printf("   Confidence: %d%% (%d/%d spotters)\n",
			match.corr.WinnerConfidence, match.corr.WinnerSupport, match.corr.TotalReporters)
		fmt.Printf("   Distance: %d (model: %s)\n",
			match.corr.Distance, match.corr.DistanceModel)
	}

	// Wrong fixes
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Println("EXAMPLES OF WRONG FIXES")
	fmt.Println(strings.Repeat("=", 100))

	if len(wrongMatches) == 0 {
		fmt.Println("   None found - excellent!")
	} else {
		for i, match := range wrongMatches {
			if i >= 10 {
				break
			}
			fmt.Printf("\n%d. WRONG FIX:\n", i+1)
			fmt.Printf("   Time: %s vs %s %s\n",
				match.corr.Ts[:19], match.bust.Date, match.bust.Time)
			fmt.Printf("   Freq: %.1f kHz vs %.1f kHz\n",
				match.corr.FreqKhz, match.bust.FreqKhz)
			fmt.Printf("   Our fix: %s -> %s\n", match.corr.Subject, match.corr.Winner)
			fmt.Printf("   Correct: %s -> %s\n", match.bust.BustedCall, match.bust.CorrectCall)
			fmt.Printf("   Confidence: %d%%\n", match.corr.WinnerConfidence)
		}
	}

	// False positives
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Println("POSSIBLE FALSE POSITIVES (first 15) - corrections with no matching busted call")
	fmt.Println(strings.Repeat("=", 100))

	for i, match := range falsePosMatches {
		if i >= 15 {
			break
		}
		fmt.Printf("\n%d. Time: %s, Freq: %.1f kHz\n",
			i+1, match.corr.Ts[:19], match.corr.FreqKhz)
		fmt.Printf("   Corrected: %s -> %s\n", match.corr.Subject, match.corr.Winner)
		fmt.Printf("   Confidence: %d%% (%d/%d spotters)\n",
			match.corr.WinnerConfidence, match.corr.WinnerSupport, match.corr.TotalReporters)
		fmt.Printf("   Distance: %d\n", match.corr.Distance)
	}

	// Find missed corrections
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Println("MISSED CORRECTIONS (first 20) - busted calls we didn't fix")
	fmt.Println(strings.Repeat("=", 100))

	correctedSet := make(map[string]bool)
	for _, match := range correctMatches {
		if match.bust != nil {
			key := fmt.Sprintf("%s_%s_%s_%.1f",
				match.bust.Date, match.bust.Time,
				normalizeCall(match.bust.BustedCall), match.bust.FreqKhz)
			correctedSet[key] = true
		}
	}
	for _, match := range wrongMatches {
		if match.bust != nil {
			key := fmt.Sprintf("%s_%s_%s_%.1f",
				match.bust.Date, match.bust.Time,
				normalizeCall(match.bust.BustedCall), match.bust.FreqKhz)
			correctedSet[key] = true
		}
	}

	var missed []BustedEntry
	for _, bust := range bustedEntries {
		key := fmt.Sprintf("%s_%s_%s_%.1f",
			bust.Date, bust.Time,
			normalizeCall(bust.BustedCall), bust.FreqKhz)
		if !correctedSet[key] {
			missed = append(missed, bust)
		}
	}

	fmt.Printf("Total missed: %d\n", len(missed))
	for i, bust := range missed {
		if i >= 20 {
			break
		}
		fmt.Printf("\n%d. Time: %s %s, Freq: %.1f kHz\n",
			i+1, bust.Date, bust.Time, bust.FreqKhz)
		fmt.Printf("   Busted: %s -> Should be: %s\n",
			bust.BustedCall, bust.CorrectCall)
	}
}
