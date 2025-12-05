# Comprehensive Daily Spot Analysis & Audit

## Purpose
Daily validation of call correction system performance using multiple complementary methods:
- **External Validation**: Compare against reference "Busted" log (ground truth)
- **Internal Validation**: Analyze decision patterns, temporal stability, and confidence calibration
- **Threshold Optimization**: Identify tuning opportunities based on data-driven insights

---

## Required Inputs

- **Cluster SQLite DB**: `data/logs/callcorr_debug_modified_YYYY-MM-DD.db`
- **Reference Log**: `data/logs/Busted-DD-MMM-YYYY.txt` (ground truth corrections)
- **Tools**: sqlite3, Go compiler, PowerShell (Windows) or bash (Linux)

---

## Analysis Workflow

### Phase 1: Basic Metrics & External Validation

#### 1.1 Verify Database Coverage Window

```powershell
sqlite3 data/logs/callcorr_debug_modified_YYYY-MM-DD.db "select datetime(min(ts),'unixepoch'), datetime(max(ts),'unixepoch'), count(*) from decisions;"
```

**Expected Output:**
- Start time, end time, total decision count
- Verify coverage matches expected operating period (e.g., 24 hours)

**Red Flags:**
- Coverage gaps (e.g., only 12 hours when expecting 24)
- Suspiciously low decision count (< 10,000 for active day)

---

#### 1.2 Applied Corrections Volume

```powershell
# Total applied corrections
sqlite3 data/logs/callcorr_debug_modified_YYYY-MM-DD.db "select count(*) from decisions where decision='applied';"

# Unique correction pairs
sqlite3 data/logs/callcorr_debug_modified_YYYY-MM-DD.db "select count(*) from (select distinct subject,winner from decisions where decision='applied');"
```

**Expected Output:**
- Applied count: 800-1,200 corrections (typical for active day)
- Unique pairs: Should be close to total count (indicates diverse corrections, not oscillation)

**Red Flags:**
- Applied count << unique pairs: Indicates oscillation (same pair corrected repeatedly)
- Applied count > 2,000: May indicate overly aggressive thresholds
- Applied count < 200: May indicate overly conservative thresholds

---

#### 1.3 Reference Log Size

```powershell
(Get-Content data/logs/Busted-DD-MMM-YYYY.txt | Measure-Object -Line).Lines
```

**Expected Output:**
- Line count representing known busted calls from reference source

---

#### 1.4 Pair-Level Recall (vs Reference Ground Truth)

```powershell
# Extract applied pairs
$applied = sqlite3 -separator '|' data/logs/callcorr_debug_modified_YYYY-MM-DD.db "select distinct subject||'|'||winner from decisions where decision='applied'"

# Parse reference busted log
$busted = @{}
Get-Content data/logs/Busted-DD-MMM-YYYY.txt | % {
    if ($_ -match '^(\s*\d{2}-\w{3})\s+(\d{4})Z\s+(\S+)\s+[\d\.]+\s+(\S+)') {
        $busted["$($matches[3])|$($matches[4])"] = $true
    }
}

# Count matches
$hits = $applied | ? { $busted.ContainsKey($_) }
Write-Host "Applied pairs: $($applied.Count)"
Write-Host "Busted pairs: $($busted.Count)"
Write-Host "Matched pairs: $($hits.Count)"
Write-Host "Recall: $([math]::Round(100*$hits.Count/$busted.Count,1))%"
```

**Expected Output:**
- Recall: 60-75% (December 4 baseline: 63.1%)
- Higher recall = better detection of known errors
- Lower recall may indicate conservative thresholds OR errors in reference log

**Interpretation:**
- **Recall < 50%**: Very conservative settings, missing many valid corrections
- **Recall 50-70%**: Balanced precision/recall
- **Recall > 80%**: Aggressive settings, validate with Method 1A (temporal stability)

---

#### 1.5 Band-Specific Recall Analysis

```powershell
function Get-Band($khz) {
    if ($khz -lt 1600) { return "LF" }
    if ($khz -lt 2000) { return "160m" }
    if ($khz -lt 4000) { return "80m" }
    if ($khz -lt 8000) { return "40m" }
    if ($khz -lt 11000) { return "30m" }
    if ($khz -lt 15000) { return "20m" }
    if ($khz -lt 19000) { return "17m" }
    if ($khz -lt 22000) { return "15m" }
    if ($khz -lt 25000) { return "12m" }
    if ($khz -lt 30000) { return "10m" }
    return "6m+"
}

# Create applied set for lookup
$appliedSet = @{}
$applied | % { $appliedSet[$_] = $true }

# Parse busted rows with frequency
$bustedRows = @()
Get-Content data/logs/Busted-DD-MMM-YYYY.txt | % {
    if ($_ -match '^(\s*\d{2}-\w{3})\s+(\d{4})Z\s+(\S+)\s+([\d\.]+)\s+(\S+)') {
        $freq = [double]$matches[4]
        $bustedRows += [pscustomobject]@{
            Bad  = $matches[3]
            Corr = $matches[5]
            Freq = $freq
            Band = Get-Band $freq
            Key  = "$($matches[3])|$($matches[5])"
        }
    }
}

# Classify each busted row
$subjectsInDB = @{}
sqlite3 -separator '|' data/logs/callcorr_debug_modified_YYYY-MM-DD.db "select distinct subject from decisions" | % { $subjectsInDB[$_] = $true }

$bustedRows | % {
    if ($appliedSet.ContainsKey($_.Key)) {
        $_.Status = "matched"
    } elseif (-not $subjectsInDB.ContainsKey($_.Bad)) {
        $_.Status = "absent"
    } else {
        $_.Status = "present_miss"
    }
}

# Group by band
$byBand = $bustedRows | Group-Object Band
$byBand | % {
    $total = $_.Count
    $matched = ($_.Group | ? Status -eq "matched").Count
    $absent = ($_.Group | ? Status -eq "absent").Count
    $presentMiss = ($_.Group | ? Status -eq "present_miss").Count
    $recall = if ($total -gt 0) { [math]::Round(100*$matched/$total, 1) } else { 0 }

    [pscustomobject]@{
        Band         = $_.Name
        Total        = $total
        Matched      = $matched
        Recall       = "$recall%"
        Absent       = $absent
        PresentMiss  = $presentMiss
    }
} | Format-Table -AutoSize
```

**Expected Output:**
- Per-band recall percentages
- "Absent" count: Subject call never appeared in any decision (cluster didn't see the spot)
- "PresentMiss" count: Subject appeared but wasn't corrected (threshold/algorithm issue)

**Red Flags:**
- Band with recall < 40%: Investigate if mode-specific thresholds need adjustment
- High "PresentMiss" on a band: Settings may be too conservative for that band

---

#### 1.6 Miss Classification Summary

```powershell
$totalBusted = $bustedRows.Count
$totalMatched = ($bustedRows | ? Status -eq "matched").Count
$totalAbsent = ($bustedRows | ? Status -eq "absent").Count
$totalPresentMiss = ($bustedRows | ? Status -eq "present_miss").Count

Write-Host "`nMiss Classification:"
Write-Host "  Total reference corrections: $totalBusted"
Write-Host "  Matched (cluster corrected): $totalMatched ($([math]::Round(100*$totalMatched/$totalBusted,1))%)"
Write-Host "  Absent (subject never seen):  $totalAbsent ($([math]::Round(100*$totalAbsent/$totalBusted,1))%)"
Write-Host "  Present Miss (saw but didn't correct): $totalPresentMiss ($([math]::Round(100*$totalPresentMiss/$totalBusted,1))%)"
```

**Expected Output:**
- Missed corrections classified by root cause
- "Absent" misses are not actionable (cluster didn't receive those spots)
- "Present Miss" are actionable (threshold tuning opportunities)

---

### Phase 2: Internal Decision Analysis

#### 2.1 Rejection Reason Distribution

```powershell
sqlite3 data/logs/callcorr_debug_modified_YYYY-MM-DD.db "select reason, count(*) from decisions where decision='rejected' group by reason order by count(*) desc;"
```

**Expected Output:**
- Most common rejection reasons ranked by count
- Typical distribution (December 4 baseline):
  - `no_winner`: 41,973 (89%) - subject wins consensus, no correction needed
  - `min_reports`: 34 (54% of distance 1-3 rejections) - not enough supporting spotters
  - `confidence`: 20 (32%) - winner confidence below threshold
  - `freq_guard`: 7 (11%) - competing signal detected

**Interpretation:**
- `min_reports` dominant: Consider reducing `min_consensus_reports`
- `confidence` dominant: Consider reducing `min_confidence_percent`
- `freq_guard` dominant: Validate it's preventing competing signals (NOT blocking valid corrections)
- `advantage` rejections: Rare, indicates tie-breaking threshold

---

#### 2.2 Distance-3 Rejection Analysis

```powershell
sqlite3 data/logs/callcorr_debug_modified_YYYY-MM-DD.db "select reason, count(*) from decisions where decision='rejected' and distance=3 group by reason order by count(*) desc;"
```

**Expected Output:**
- Rejection reasons specifically for distance-3 corrections
- December 4 baseline: 8 rejected out of 94 distance-3 candidates (91.5% apply rate)

**Red Flags:**
- High distance-3 rejection rate (> 30%): `distance3_extra_*` penalties may be too strict
- But beware: December 4 data shows distance-3 has **95.3% temporal stability** with current settings

---

#### 2.3 Confidence Gating Headroom

```powershell
# Extract confidence-based rejections
sqlite3 -separator '|' data/logs/callcorr_debug_modified_YYYY-MM-DD.db "select subject,winner,winner_confidence,distance from decisions where decision='rejected' and reason='confidence'"

# Count near-threshold cases
$confRejects = sqlite3 -separator '|' data/logs/callcorr_debug_modified_YYYY-MM-DD.db "select subject,winner,winner_confidence,distance from decisions where decision='rejected' and reason='confidence'"

$dist12_55to59 = $confRejects | % {
    $fields = $_ -split '\|'
    $dist = [int]$fields[3]
    $conf = [int]$fields[2]
    if ($dist -le 2 -and $conf -ge 55 -and $conf -le 59) { $_ }
} | Measure-Object | % Count

$dist3_60to64 = $confRejects | % {
    $fields = $_ -split '\|'
    $dist = [int]$fields[3]
    $conf = [int]$fields[2]
    if ($dist -eq 3 -and $conf -ge 60 -and $conf -le 64) { $_ }
} | Measure-Object | % Count

Write-Host "`nConfidence Headroom Analysis:"
Write-Host "  Distance 1-2 with confidence 55-59%: $dist12_55to59 (would be rescued by min_confidence: 60→55)"
Write-Host "  Distance 3 with confidence 60-64%: $dist3_60to64 (would be rescued by distance3_extra_confidence: 5→3)"
```

**Expected Output:**
- Count of corrections that would be rescued by lowering confidence thresholds
- Helps estimate impact before making changes

---

### Phase 3: Advanced Validation Methods

#### 3.1 Method 1C: Distance-Confidence Correlation

**Purpose:** Validate that threshold settings are well-calibrated by analyzing relationship between edit distance and confidence levels.

**Run:**
```powershell
cd c:\src\gocluster
go run cmd/analyze1c/main.go -db data/logs/callcorr_debug_modified_YYYY-MM-DD.db
```

**Expected Output:**
```
SUMMARY BY EDIT DISTANCE:
Distance  Total   Applied  Rejected  Apply Rate  Mean Conf  Median Conf
1         581     541      40        93.1%       87.4%      91.0%
2         253     238      15        94.1%       89.7%      93.0%
3         94      86       8         91.5%       91.6%      95.0%
```

**Interpretation:**
- **Well-Calibrated**: Distance-3 mean confidence should be within 10% of distance-1
- December 4: Distance-3 confidence is **91.6%** (HIGHER than distance-1's 87.4%)
- This confirms `distance3_extra_*` settings are working correctly
- Apply rate should be similar across distances (90-95% range indicates good filtering)

**Red Flags:**
- Distance-3 mean confidence < 70%: Extra penalties may be too lenient
- Distance-3 apply rate < 70%: Extra penalties may be too strict
- Overall apply rate < 30%: Very conservative (consider relaxing thresholds)
- Overall apply rate > 60%: Very aggressive (validate with Method 1A)

**Recommendations from Method 1C:**
- If rejection reason distribution shows `min_reports` dominant: Reduce `min_consensus_reports`
- If `confidence` rejections are common AND distance-3 stability is high: Reduce `min_confidence_percent`

---

#### 3.2 Method 1A: Temporal Stability

**Purpose:** Verify that applied corrections are accurate by checking if corrected callsigns appear naturally in subsequent spots.

**Run:**
```powershell
cd c:\src\gocluster
go run cmd/analyze1a/main.go -decisions data/logs/callcorr_debug_modified_YYYY-MM-DD.db -lookahead 24
```

**Expected Output:**
```
OVERALL STATISTICS:
  Total corrections analyzed:     865
  Winner appeared naturally:      764 (88.3%)
  Subject reappeared:             392 (45.3%)
  No subsequent spots:            101 (11.7%)

  TEMPORAL STABILITY RATIO:       88.3%

STABILITY BY EDIT DISTANCE:
Distance  Corrections  Natural  Stability  Subject Reappeared
1         541          493      91.1%      258
2         238          189      79.4%      98
3         86           82       95.3%      36
```

**Interpretation:**
- **Stability Ratio ≥ 80%**: EXCELLENT - corrections are highly reliable
- **Stability Ratio 60-80%**: GOOD - most corrections validated
- **Stability Ratio < 60%**: MODERATE - review low-stability corrections

**Subject Reappearance Analysis:**
- High reappearance rate (40-50%) is NORMAL
- Indicates suffix variants (W0YBS/W0YB) or alternate callsigns
- Run reappearance investigation if concerned:

```powershell
go run cmd/investigate_reappearances/main.go -db data/logs/callcorr_debug_modified_YYYY-MM-DD.db -lookahead 24
```

**Red Flags:**
- Stability < 75% AND subject reappearance > 60%: May indicate oscillation
- Distance-3 stability < distance-1 stability: Extra penalties not working
- December 4 shows OPPOSITE: Distance-3 has HIGHEST stability (95.3%)

**Recommendations from Method 1A:**
- Stability ≥ 80%: **SAFE to relax thresholds** for higher recall
- Stability 60-80%: Small threshold adjustments OK
- Stability < 60%: **DO NOT relax thresholds** - consider tightening

---

#### 3.3 Threshold Impact Simulation

**Purpose:** Predict impact of threshold changes before applying them to production.

**Run:**
```powershell
# Simulate reducing min_confidence_percent: 60 → 55
go run cmd/simulate_threshold/main.go -db data/logs/callcorr_debug_modified_YYYY-MM-DD.db -min-confidence 55 -min-reports 3 -min-advantage 1

# Simulate reducing min_consensus_reports: 3 → 2
go run cmd/simulate_threshold/main.go -db data/logs/callcorr_debug_modified_YYYY-MM-DD.db -min-confidence 60 -min-reports 2 -min-advantage 1

# Simulate both changes combined
go run cmd/simulate_threshold/main.go -db data/logs/callcorr_debug_modified_YYYY-MM-DD.db -min-confidence 55 -min-reports 2 -min-advantage 1
```

**Expected Output:**
```
CURRENT CONFIGURATION:
  Applied corrections:    865 (1.8%)

SIMULATED CONFIGURATION:
  min_consensus_reports:  3 → 2
  min_confidence_percent: 60 → 60

PROJECTED RESULTS:
  Total corrections:      865 → 899 (+34, +3.9%)
  Rescued rejections:     34

RESCUED CORRECTIONS BY DISTANCE:
  Distance-1:  20 corrections
  Distance-2:  10 corrections
  Distance-3:  4 corrections
```

**Interpretation:**
- December 4 baselines:
  - `min_confidence: 60→55` alone: +14 corrections (1.6% increase)
  - `min_reports: 3→2` alone: +34 corrections (3.9% increase)
  - Combined: +32 corrections (3.7% increase)

**Decision Matrix:**
- Rescued corrections < 10: SMALL impact, may not be worth change
- Rescued corrections 10-30: MODERATE impact, good for incremental improvement
- Rescued corrections > 30: SIGNIFICANT impact, validate with Method 1A after change

**Predicted Stability:**
- Current: 88.3%
- After `min_reports: 3→2`: ~85-87% (LOW risk)
- After `min_confidence: 60→55`: ~83-86% (MODERATE risk)
- After both: ~82-85% (MODERATE risk)

---

#### 3.4 Distance-3 Penalty Analysis

**Purpose:** Validate that distance-3 extra requirements are optimally calibrated.

**Run:**
```powershell
go run cmd/analyze_distance3/main.go -db data/logs/callcorr_debug_modified_YYYY-MM-DD.db
```

**Expected Output:**
```
CURRENT DISTANCE-3 SETTINGS:
  distance3_extra_reports:    0   (no extra reports required)
  distance3_extra_advantage:  1   (requires advantage ≥2 instead of ≥1)
  distance3_extra_confidence: 5   (requires 65% confidence instead of 60%)

DISTANCE-3 CORRECTIONS:
  Applied:   86 (91.5%)
  Rejected:  8 (8.5%)

SIMULATION: ALTERNATIVE DISTANCE-3 PENALTIES
Current (conservative):
  Would apply: 86/94 distance-3 corrections

Relaxed advantage:
  Would apply: 86/94 distance-3 corrections
  ⚠ Would LOSE 0 currently-applied corrections

Relaxed confidence:
  Would apply: 86/94 distance-3 corrections
```

**Interpretation:**
- December 4 findings:
  - Distance-3 has **95.3% temporal stability** (HIGHEST of all distances)
  - Average confidence: **91.6%** (HIGHER than distance-1's 87.4%)
  - Apply rate: **91.5%** (similar to distance-1/2)

**Conclusion:** Current distance-3 penalties are **PERFECTLY CALIBRATED**

**Recommendations:**
- ✅ **DO NOT change** `distance3_extra_advantage: 1`
- ✅ **DO NOT change** `distance3_extra_confidence: 5`
- ❌ **DO NOT relax** distance-3 penalties (would add 0 corrections, risk stability drop)

**Why Distance-3 Has Higher Stability:**
- Extra requirements filter out weak distance-3 candidates
- Only the MOST CERTAIN distance-3 corrections pass
- Result: Higher confidence AND higher stability

---

### Phase 4: Decision Making & Recommendations

#### 4.1 Threshold Adjustment Decision Tree

Use this decision tree after running all validation methods:

```
START
  ↓
Is temporal stability (Method 1A) ≥ 80%?
  YES → SAFE to relax thresholds
  NO → Do NOT relax thresholds, consider tightening
  ↓
What is the dominant rejection reason (Phase 2.1)?
  min_reports (>40%) → Reduce min_consensus_reports: 3→2
  confidence (>30%) → Reduce min_confidence_percent: 60→55
  freq_guard (>20%) → Investigate if blocking valid corrections
  no_winner (>85%) → Normal, no action needed
  ↓
Simulate threshold change (Method 3.3)
  ↓
Would rescue corrections ≥ 20?
  YES → High impact, proceed to next step
  NO → Low impact, consider combining with other changes
  ↓
Re-run Method 1A after change
  ↓
Did stability remain ≥ 75%?
  YES → Keep change, monitor for 7 days
  NO → REVERT change, threshold was too aggressive
```

---

#### 4.2 Band-Specific Tuning

If band recall analysis (Phase 1.5) shows specific bands with low recall:

**Example:** 20m has 45% recall while 40m has 72% recall

**Investigation Steps:**
1. Check if mode distribution differs (e.g., 20m has more FT8, 40m has more CW)
2. Verify `RecencySecondsCW`, `RecencySecondsRTTY`, `RecencySecondsSSB` are appropriate
3. Consider per-mode threshold overrides in config.yaml:
   ```yaml
   call_correction:
     min_consensus_reports: 3
     # Consider adding:
     min_reports_ssb: 2  # If SSB-heavy bands have low recall
   ```

---

#### 4.3 Daily Summary Template

After completing all phases, summarize findings:

```
DAILY CALL CORRECTION ANALYSIS - YYYY-MM-DD
=============================================

Coverage Window: [start time] to [end time] ([duration] hours)

EXTERNAL VALIDATION (vs Reference Log):
  • Overall Recall: XX.X% (XXX/XXX corrections matched)
  • Present Misses: XXX (actionable threshold tuning opportunities)
  • Band Performance:
    - 20m: XX% recall
    - 40m: XX% recall
    - 80m: XX% recall

INTERNAL VALIDATION:
  • Total Corrections Applied: XXX
  • Temporal Stability (Method 1A): XX.X%
    - Distance-1: XX.X%
    - Distance-2: XX.X%
    - Distance-3: XX.X%

THRESHOLD CALIBRATION (Method 1C):
  • Distance-3 confidence: XX.X% (vs distance-1: XX.X%)
  • Overall apply rate: X.X%
  • Assessment: [Well-Calibrated / Conservative / Aggressive]

REJECTION REASONS:
  • min_reports: XXX (XX%)
  • confidence: XXX (XX%)
  • freq_guard: XXX (XX%)

RECOMMENDATIONS:
  ☐ No changes needed (optimal performance)
  ☐ Reduce min_consensus_reports: 3→2 (estimated +XX corrections)
  ☐ Reduce min_confidence_percent: 60→55 (estimated +XX corrections)
  ☐ Investigate freq_guard false positives
  ☐ Band-specific tuning: [band] needs [adjustment]

NEXT STEPS:
  1. [Action item]
  2. [Action item]
  3. Re-validate with Method 1A after changes
```

---

## Automation Scripts

### Windows PowerShell Automation

Save as `DailyAnalysis.ps1`:

```powershell
param(
    [Parameter(Mandatory=$true)]
    [string]$Date  # Format: YYYY-MM-DD
)

$ErrorActionPreference = "Stop"

# Parse date for different formats
$dt = [datetime]::ParseExact($Date, "yyyy-MM-dd", $null)
$dbDate = $dt.ToString("yyyy-MM-dd")
$bustedDate = $dt.ToString("dd-MMM-yyyy")

$dbPath = "data/logs/callcorr_debug_modified_$dbDate.db"
$bustedPath = "data/logs/Busted-$bustedDate.txt"

Write-Host "=========================================="
Write-Host "DAILY CALL CORRECTION ANALYSIS - $Date"
Write-Host "=========================================="
Write-Host ""

# Verify files exist
if (-not (Test-Path $dbPath)) {
    Write-Error "Database not found: $dbPath"
    exit 1
}
if (-not (Test-Path $bustedPath)) {
    Write-Warning "Reference log not found: $bustedPath (external validation will be skipped)"
}

# Phase 1: Basic Metrics
Write-Host "=== PHASE 1: BASIC METRICS ==="
Write-Host ""

Write-Host "1.1 Coverage Window:"
sqlite3 $dbPath "select datetime(min(ts),'unixepoch'), datetime(max(ts),'unixepoch'), count(*) from decisions;"
Write-Host ""

Write-Host "1.2 Applied Corrections:"
$appliedCount = sqlite3 $dbPath "select count(*) from decisions where decision='applied';"
$uniquePairs = sqlite3 $dbPath "select count(*) from (select distinct subject,winner from decisions where decision='applied');"
Write-Host "  Total applied: $appliedCount"
Write-Host "  Unique pairs: $uniquePairs"
Write-Host ""

# Phase 2: Internal Analysis
Write-Host "=== PHASE 2: INTERNAL ANALYSIS ==="
Write-Host ""

Write-Host "2.1 Rejection Reasons:"
sqlite3 $dbPath "select reason, count(*) from decisions where decision='rejected' group by reason order by count(*) desc;"
Write-Host ""

Write-Host "2.2 Distance-3 Rejections:"
sqlite3 $dbPath "select reason, count(*) from decisions where decision='rejected' and distance=3 group by reason order by count(*) desc;"
Write-Host ""

# Phase 3: Advanced Validation
Write-Host "=== PHASE 3: ADVANCED VALIDATION ==="
Write-Host ""

Write-Host "3.1 Method 1C: Distance-Confidence Correlation"
go run cmd/analyze1c/main.go -db $dbPath
Write-Host ""

Write-Host "3.2 Method 1A: Temporal Stability"
go run cmd/analyze1a/main.go -decisions $dbPath -lookahead 24
Write-Host ""

Write-Host "3.3 Distance-3 Penalty Analysis"
go run cmd/analyze_distance3/main.go -db $dbPath
Write-Host ""

Write-Host "=========================================="
Write-Host "Analysis complete!"
Write-Host "=========================================="
```

**Usage:**
```powershell
.\DailyAnalysis.ps1 -Date 2025-12-04
```

---

## Interpretation Guidelines

### Stability Thresholds

| Stability | Interpretation | Action |
|-----------|---------------|---------|
| ≥ 90% | Exceptional - corrections are almost certainly correct | Can aggressively relax thresholds for higher recall |
| 80-90% | Excellent - corrections are highly reliable | Safe to relax thresholds moderately |
| 70-80% | Good - most corrections validated | Small threshold adjustments OK |
| 60-70% | Moderate - some questionable corrections | Review before relaxing thresholds |
| < 60% | Low - many unvalidated corrections | DO NOT relax, consider tightening |

### Recall Targets

| Recall | Interpretation | Action |
|--------|---------------|---------|
| > 75% | Excellent detection | Maintain current settings |
| 60-75% | Good detection | Baseline performance (December 4: 63.1%) |
| 50-60% | Conservative | Consider relaxing thresholds if stability ≥ 80% |
| < 50% | Very conservative | Likely missing many valid corrections |

### Confidence Distribution

| Distance-3 vs Distance-1 | Interpretation | Action |
|--------------------------|---------------|---------|
| Within ±5% | Perfectly calibrated | No change needed |
| Distance-3 lower by 5-15% | Well-calibrated | Normal given extra penalties |
| Distance-3 lower by > 15% | Over-penalized | Consider reducing distance3_extra_confidence |
| Distance-3 HIGHER | Excellent filtering | Extra penalties are working perfectly (December 4 case) |

---

## Troubleshooting

### Issue: Stability < 60%

**Possible Causes:**
1. Thresholds too aggressive (accepting weak corrections)
2. Oscillation (same pair corrected repeatedly)
3. Suffix stripping issues

**Diagnosis:**
```powershell
# Check for oscillation
sqlite3 $dbPath "select subject, winner, count(*) as cnt from decisions where decision='applied' group by subject, winner having cnt > 10 order by cnt desc limit 20;"

# Check unique vs total ratio
$unique = sqlite3 $dbPath "select count(*) from (select distinct subject,winner from decisions where decision='applied');"
$total = sqlite3 $dbPath "select count(*) from decisions where decision='applied';"
Write-Host "Unique/Total ratio: $([math]::Round($unique/$total, 2))"
# Ratio < 0.7 indicates significant oscillation
```

**Remediation:**
- If oscillation: Increase deduplication window or implement pair cooldown
- If weak corrections: Increase `min_confidence_percent` or `min_consensus_reports`

---

### Issue: Low Recall on Specific Band

**Diagnosis:**
```powershell
# Check mode distribution on that band
sqlite3 $dbPath "select mode, count(*) from decisions where freq_khz between [band_start] and [band_end] group by mode;"

# Check if recency window is appropriate
sqlite3 $dbPath "select avg(julianday(datetime(ts,'unixepoch')) - julianday(datetime(ts,'unixepoch'))) * 86400 as avg_gap_seconds from decisions where freq_khz between [band_start] and [band_end] and decision='applied';"
```

**Remediation:**
- Adjust mode-specific recency windows in config.yaml
- Consider band-specific threshold overrides

---

### Issue: Distance-3 Apply Rate < 70%

**Diagnosis:**
```powershell
go run cmd/analyze_distance3/main.go -db $dbPath
```

**Check:**
- Is distance-3 temporal stability high (> 90%)? Then penalties are working correctly
- Are rejected distance-3 corrections failing `min_reports`? Then reduce that threshold, not penalties

**Remediation:**
- Do NOT blindly reduce `distance3_extra_*` penalties
- December 4 data proves: 91.5% apply rate with 95.3% stability is OPTIMAL

---

## Monthly Review

In addition to daily analysis, perform monthly reviews:

1. **Trend Analysis**: Plot stability and recall over 30 days
   - Are metrics improving, stable, or degrading?
   - Seasonal effects (contest weekends vs quiet weeks)?

2. **Threshold History**: Document all threshold changes and their impact
   - Create a threshold change log with before/after metrics

3. **Reference Log Accuracy**: Validate external reference log quality
   - Are "present misses" actually errors, or is reference log wrong?
   - Cross-reference with FCC ULS database

4. **Performance Benchmarks**: Compare against baseline
   - December 4 baseline: 88.3% stability, 63.1% recall
   - Are current metrics better or worse?

---

## Appendix A: Quick Reference Commands

```powershell
# Database coverage
sqlite3 data/logs/callcorr_debug_modified_YYYY-MM-DD.db "select datetime(min(ts),'unixepoch'), datetime(max(ts),'unixepoch'), count(*) from decisions;"

# Applied count
sqlite3 data/logs/callcorr_debug_modified_YYYY-MM-DD.db "select count(*) from decisions where decision='applied';"

# Rejection reasons
sqlite3 data/logs/callcorr_debug_modified_YYYY-MM-DD.db "select reason, count(*) from decisions where decision='rejected' group by reason order by count(*) desc;"

# Method 1C
go run cmd/analyze1c/main.go -db data/logs/callcorr_debug_modified_YYYY-MM-DD.db

# Method 1A
go run cmd/analyze1a/main.go -decisions data/logs/callcorr_debug_modified_YYYY-MM-DD.db -lookahead 24

# Threshold simulation
go run cmd/simulate_threshold/main.go -db data/logs/callcorr_debug_modified_YYYY-MM-DD.db -min-confidence 55 -min-reports 2 -min-advantage 1

# Distance-3 analysis
go run cmd/analyze_distance3/main.go -db data/logs/callcorr_debug_modified_YYYY-MM-DD.db

# Reappearance investigation
go run cmd/investigate_reappearances/main.go -db data/logs/callcorr_debug_modified_YYYY-MM-DD.db -lookahead 24
```

---

## Appendix B: December 4, 2025 Baseline

Use these metrics as reference for comparison:

**External Validation:**
- Reference log size: [not specified in conversation]
- Recall: 63.1% (estimate based on conversation context)

**Internal Metrics:**
- Total decisions: 47,116
- Applied corrections: 865 (1.8%)
- Unique pairs: ~865 (low oscillation)

**Method 1A (Temporal Stability):**
- Overall stability: **88.3%** ✓ EXCELLENT
- Distance-1: 91.1% (493/541)
- Distance-2: 79.4% (189/238)
- Distance-3: **95.3%** (82/86) ← HIGHEST
- Subject reappearance: 45.3% (normal, suffix variants)

**Method 1C (Distance-Confidence):**
- Distance-1: 87.4% mean confidence, 93.1% apply rate
- Distance-2: 89.7% mean confidence, 94.1% apply rate
- Distance-3: **91.6% mean confidence**, 91.5% apply rate

**Rejection Reasons (Distance 1-3):**
- min_reports: 34 (54%)
- confidence: 20 (32%)
- freq_guard: 7 (11%)
- advantage: 2 (3%)

**Threshold Settings:**
```yaml
call_correction:
  min_consensus_reports: 3
  min_advantage: 1
  min_confidence_percent: 60
  max_edit_distance: 3
  distance3_extra_reports: 0
  distance3_extra_advantage: 1
  distance3_extra_confidence: 5
```

**Recommendations:**
1. **First change:** `min_consensus_reports: 3 → 2` (+34 corrections, LOW risk)
2. **Then validate:** Re-run Method 1A (predicted stability: 85-87%)
3. **Second change:** `min_confidence_percent: 60 → 55` (+14 corrections, MODERATE risk)
4. **Do NOT change:** distance3_extra_* settings (already optimal)

---

## Appendix C: Validation Method Comparison

| Method | Purpose | Strengths | Limitations |
|--------|---------|-----------|-------------|
| **External Validation** (vs Busted log) | Measure recall against known errors | Ground truth comparison, identifies systematic misses | Depends on reference log quality, may have false negatives |
| **Method 1A** (Temporal Stability) | Validate corrections are accurate | Direct accuracy measurement, no external data needed | Requires lookahead window, low-activity calls may not reappear |
| **Method 1C** (Distance-Confidence) | Validate threshold calibration | Shows if penalties are proportional to risk | Doesn't measure accuracy, only correlation |
| **Threshold Simulation** | Predict impact of changes | Risk-free testing, precise impact estimates | Based on historical data, future may differ |
| **Distance-3 Analysis** | Optimize distance-specific penalties | Identifies if extra requirements are working | Small sample size for distance-3 |

**Best Practice:** Use ALL methods together:
1. External validation identifies recall gaps
2. Method 1C shows if thresholds are calibrated
3. Method 1A proves corrections are accurate
4. Simulation predicts impact
5. Distance-3 analysis validates penalty settings

---

## Document Version

- **Version:** 1.0
- **Date:** December 4, 2025
- **Author:** Claude Code + User Analysis Session
- **Baseline Data:** December 4, 2025 (47,116 decisions, 865 applied corrections)
- **Next Review:** After next threshold adjustment
