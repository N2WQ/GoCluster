#!/usr/bin/env python3
"""
Analyze call correction effectiveness by comparing modified cluster log
with reference (Busted) cluster log.
"""

import json
import re
from datetime import datetime
from collections import defaultdict

def parse_modified_log(filepath):
    """Parse the call correction debug log."""
    corrections = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if '"decision":"applied"' in line:
                # Extract JSON from log line
                json_start = line.index('{')
                json_data = json.loads(line[json_start:])
                corrections.append(json_data)
    return corrections

def parse_busted_log(filepath):
    """Parse the reference cluster log with busted calls."""
    busted_entries = []
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            # Format: 04-Dec 0000Z CALL freq BUSTEDCALL snr spotter mode
            # The "?" marker indicates a busted call
            if '?' in line:
                parts = line.strip().split()
                if len(parts) >= 7:
                    try:
                        date_str = parts[0]  # 04-Dec
                        time_str = parts[1]  # 0000Z
                        call_reported = parts[2]  # The busted call
                        freq = float(parts[3])
                        # After "?" is the correct call
                        idx = parts.index('?')
                        if idx + 1 < len(parts):
                            correct_call = parts[idx + 1]
                            mode = parts[-1] if len(parts) > 0 else ''

                            busted_entries.append({
                                'date': date_str,
                                'time': time_str,
                                'busted_call': call_reported,
                                'correct_call': correct_call,
                                'freq_khz': freq,
                                'mode': mode,
                                'raw_line': line.strip()
                            })
                    except (ValueError, IndexError):
                        continue
    return busted_entries

def time_to_minutes(time_str):
    """Convert time string like '0032Z' to minutes since midnight."""
    hours = int(time_str[:2])
    minutes = int(time_str[2:4])
    return hours * 60 + minutes

def normalize_call(call):
    """Normalize callsign for comparison."""
    return call.upper().strip().replace('-', '').replace('/', '')

def find_matches(corrections, busted_entries):
    """Find matches between corrections and busted calls."""
    matches = []
    corrections_found = 0
    corrections_missed = 0
    false_positives = 0

    print("\n" + "="*100)
    print("ANALYSIS: Comparing Corrections with Reference Busted Calls")
    print("="*100)

    # For each correction applied, see if it matches a busted call in reference
    for corr in corrections:
        ts = datetime.fromisoformat(corr['ts'].replace('Z', '+00:00'))
        corr_time_mins = ts.hour * 60 + ts.minute
        corr_subject = normalize_call(corr['subject'])
        corr_winner = normalize_call(corr['winner'])
        corr_freq = corr['freq_khz']

        # Look for matching busted entry (within time and frequency tolerance)
        found_match = False
        for bust in busted_entries:
            bust_time_mins = time_to_minutes(bust['time'])
            bust_freq = bust['freq_khz']
            bust_busted = normalize_call(bust['busted_call'])
            bust_correct = normalize_call(bust['correct_call'])

            # Check if they match: similar time (within 5 min), similar freq (within 1 kHz), and calls match
            time_diff = abs(corr_time_mins - bust_time_mins)
            freq_diff = abs(corr_freq - bust_freq)

            if time_diff <= 5 and freq_diff <= 1.0:
                # Check if our correction matches the busted->correct mapping
                if corr_subject == bust_busted and corr_winner == bust_correct:
                    matches.append({
                        'correction': corr,
                        'busted': bust,
                        'result': 'CORRECT_FIX',
                        'time_diff_min': time_diff,
                        'freq_diff_khz': freq_diff
                    })
                    corrections_found += 1
                    found_match = True
                    break
                elif corr_subject == bust_busted:
                    # We corrected it, but to the wrong call
                    matches.append({
                        'correction': corr,
                        'busted': bust,
                        'result': 'WRONG_FIX',
                        'time_diff_min': time_diff,
                        'freq_diff_khz': freq_diff
                    })
                    false_positives += 1
                    found_match = True
                    break

        if not found_match:
            # We applied a correction, but there's no busted call in the reference
            # This could be a false positive (we incorrectly "fixed" a good call)
            matches.append({
                'correction': corr,
                'busted': None,
                'result': 'POSSIBLE_FALSE_POSITIVE',
                'time_diff_min': 0,
                'freq_diff_khz': 0
            })

    return matches, corrections_found, corrections_missed, false_positives

def main():
    modified_log = r'c:\src\gocluster\data\logs\callcorr_debug_modified.log'
    busted_log = r'c:\src\gocluster\data\logs\Busted-04-Dec-2025.txt'

    print("Loading logs...")
    corrections = parse_modified_log(modified_log)
    busted_entries = parse_busted_log(busted_log)

    print(f"\nFound {len(corrections)} applied corrections in modified cluster")
    print(f"Found {len(busted_entries)} busted calls in reference cluster")

    matches, corrections_found, corrections_missed, false_positives = find_matches(corrections, busted_entries)

    # Print summary statistics
    print("\n" + "="*100)
    print("SUMMARY STATISTICS")
    print("="*100)
    print(f"Total corrections applied by system:     {len(corrections)}")
    print(f"Total busted calls in reference:         {len(busted_entries)}")
    print(f"Correct fixes (matched busted->correct): {corrections_found}")
    print(f"Wrong fixes (busted call, wrong answer): {false_positives}")
    print(f"Possible false positives:                {len(corrections) - corrections_found - false_positives}")

    if len(corrections) > 0:
        accuracy = (corrections_found / len(corrections)) * 100
        print(f"\nCorrection Accuracy: {accuracy:.1f}%")

    # Show examples of correct fixes
    print("\n" + "="*100)
    print("EXAMPLES OF CORRECT FIXES (first 20)")
    print("="*100)
    correct_fixes = [m for m in matches if m['result'] == 'CORRECT_FIX']
    for i, match in enumerate(correct_fixes[:20], 1):
        corr = match['correction']
        bust = match['busted']
        print(f"\n{i}. CORRECT FIX:")
        print(f"   Time: {corr['ts'][:19]} (modified) vs {bust['date']} {bust['time']} (reference)")
        print(f"   Freq: {corr['freq_khz']:.1f} kHz vs {bust['freq_khz']:.1f} kHz")
        print(f"   Fixed: {corr['subject']} -> {corr['winner']}")
        print(f"   Reference: {bust['busted_call']} -> {bust['correct_call']}")
        print(f"   Confidence: {corr['winner_confidence']}% ({corr['winner_support']}/{corr['total_reporters']} spotters)")
        print(f"   Distance: {corr['distance']} (model: {corr['distance_model']})")

    # Show examples of wrong fixes
    print("\n" + "="*100)
    print("EXAMPLES OF WRONG FIXES")
    print("="*100)
    wrong_fixes = [m for m in matches if m['result'] == 'WRONG_FIX']
    if wrong_fixes:
        for i, match in enumerate(wrong_fixes[:10], 1):
            corr = match['correction']
            bust = match['busted']
            print(f"\n{i}. WRONG FIX:")
            print(f"   Time: {corr['ts'][:19]} vs {bust['date']} {bust['time']}")
            print(f"   Freq: {corr['freq_khz']:.1f} kHz vs {bust['freq_khz']:.1f} kHz")
            print(f"   Our fix: {corr['subject']} -> {corr['winner']}")
            print(f"   Correct: {bust['busted_call']} -> {bust['correct_call']}")
            print(f"   Confidence: {corr['winner_confidence']}%")
    else:
        print("   None found - excellent!")

    # Show possible false positives
    print("\n" + "="*100)
    print("POSSIBLE FALSE POSITIVES (first 15) - corrections with no matching busted call in reference")
    print("="*100)
    false_pos = [m for m in matches if m['result'] == 'POSSIBLE_FALSE_POSITIVE']
    for i, match in enumerate(false_pos[:15], 1):
        corr = match['correction']
        print(f"\n{i}. Time: {corr['ts'][:19]}, Freq: {corr['freq_khz']:.1f} kHz")
        print(f"   Corrected: {corr['subject']} -> {corr['winner']}")
        print(f"   Confidence: {corr['winner_confidence']}% ({corr['winner_support']}/{corr['total_reporters']} spotters)")
        print(f"   Distance: {corr['distance']}")

    # Now find busted calls that we DIDN'T correct
    print("\n" + "="*100)
    print("MISSED CORRECTIONS (first 20) - busted calls in reference that we didn't fix")
    print("="*100)

    corrected_busted = set()
    for match in matches:
        if match['result'] in ['CORRECT_FIX', 'WRONG_FIX'] and match['busted']:
            key = (match['busted']['date'], match['busted']['time'],
                   normalize_call(match['busted']['busted_call']),
                   match['busted']['freq_khz'])
            corrected_busted.add(key)

    missed = []
    for bust in busted_entries:
        key = (bust['date'], bust['time'], normalize_call(bust['busted_call']), bust['freq_khz'])
        if key not in corrected_busted:
            missed.append(bust)

    print(f"Total missed: {len(missed)}")
    for i, bust in enumerate(missed[:20], 1):
        print(f"\n{i}. Time: {bust['date']} {bust['time']}, Freq: {bust['freq_khz']:.1f} kHz")
        print(f"   Busted: {bust['busted_call']} -> Should be: {bust['correct_call']}")
        print(f"   Raw: {bust['raw_line']}")

if __name__ == '__main__':
    main()
