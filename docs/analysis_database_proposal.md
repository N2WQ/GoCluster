# Call Correction Analysis Database - Design Proposal

## Executive Summary

This document proposes a comprehensive data structure for long-term analysis of call correction decisions in a DX cluster environment processing 7 million spots per day. The design enables objective validation of correction quality without requiring external ground truth data.

**Document Version:** 2.0 (revised with raw data handling, temporal specifications, and audit requirements)

---

## Problem Statement

We need to objectively analyze the effectiveness of our call correction method using:
- ✅ RBN feed data (raw spotter reports)
- ✅ Our cluster's decision-making logs
- ⚠️ Optional: Corrections from other clusters (quality unknown)
- ❌ No ground truth of actual transmitted callsigns

The analysis must support multiple validation methodologies:
1. **Temporal Consistency Analysis** - Do corrected calls appear naturally afterward?
2. **Frequency Clustering Coherence** - Are we merging competing signals incorrectly?
3. **Distance-Confidence Correlation** - Do our thresholds make sense?
4. **CTY/Known Calls Validation** - Do corrections point to valid callsigns?
5. **SNR-Weighted Precision** - Are high-SNR spotters more reliable?
6. **Threshold Sensitivity Analysis** - Can we tune settings for better performance?
7. **Rejection Reason Analysis** - Why are we missing valid corrections?

---

## Current System Context

### Existing Decision Logging
The system currently logs decisions as JSON lines containing:
- Timestamp, frequency, mode, source
- Subject call (original) and winner call (corrected)
- Consensus statistics (reporters, supporters, confidence percentages)
- Edit distance and distance model (plain/morse/baudot)
- Decision outcome (applied/rejected) and rejection reason
- Threshold configuration snapshot

### Current Limitations
- **JSON logs are not queryable** - Cannot efficiently filter by frequency, mode, or decision type
- **No temporal tracking** - Cannot measure what happens after corrections
- **No aggregation** - Cannot compute daily statistics without full log parsing
- **No relationship tracking** - Cannot link related decisions or track spotter quality
- **Storage inefficiency** - Repetitive string data in every log line

### Volume Characteristics
- **7 million spots/day total**
- **~350,000 spots/day** evaluated for correction (5% are CW/RTTY/SSB with sufficient spotters)
- **~40,000 corrections/day** applied (1% correction rate)
- **~310,000 rejections/day** (correction considered but rejected by thresholds)

### Critical Gap Identified
**The original proposal only captured correction candidates.** However, long-term analysis of missed corrections, drift detection, and false negative patterns requires access to ALL raw RBN data - including the 6.65M spots that were never evaluated for correction. This revision addresses raw data handling strategies.

---

## Proposed Solution: Daily SQLite Databases

### Database Partitioning Strategy

**Recommendation: One SQLite database per day**

Format: `data/analysis/callcorr_YYYY-MM-DD.db`

**Benefits:**
- Manageable file sizes (~260 MB/day)
- Clean temporal boundaries
- Easy archival (compress old databases to .gz)
- Parallel analysis across multiple days
- Simple backup and retention policies

**Alternative Considered:** Hourly databases would enable parallel writes but add significant management complexity. Only needed if write contention becomes an issue during production use.

---

## Raw Data Handling Strategy

### The Raw Spot Index Challenge

**Problem:** Analysis of missed corrections, temporal drift, and false negatives requires comparing our decisions against ALL spots received, not just correction candidates.

**Bad Solution:** Store all 7M spots in SQLite daily.
- Would inflate database to ~3.5 GB/day
- Slow queries across large tables
- Unnecessary duplication of RBN CSV data

**Recommended Hybrid Approach:**

#### 1. Keep Raw RBN CSV Files (Primary Source)
- Continue writing raw RBN feed to daily CSV: `data/raw/rbn_YYYY-MM-DD.csv.gz`
- Compressed CSV: ~500 MB/day
- Fields: timestamp, spotter, dx_call, frequency, mode, snr, comment, source
- **Advantage:** Complete audit trail, parseable with standard tools

#### 2. Lightweight All-Spots Index in SQLite
Add a minimal index table capturing just keys for every spot:

**Essential Fields:**
- Spot sequence ID (monotonic, for ordering)
- Timestamp (Unix seconds)
- DX call (normalized)
- Frequency (integer millihertz)
- Mode ID (integer lookup)
- Source ID (integer lookup)
- CSV line offset (for quick lookup back to raw file)

**Purpose:** Fast filtering to find "what spots did we receive for call X on frequency Y" without full CSV parsing.

**Storage:** ~100 bytes/row × 7M = 700 MB/day uncompressed, ~200 MB with compression

#### 3. Monthly Parquet Mirrors (Optional, for Analytics)
Export raw CSV to columnar Parquet monthly for fast cross-day analytics:
- `data/parquet/rbn_2025-12.parquet`
- Partitioned by day
- Columnar compression: ~150 MB/month
- Enables fast "find all spots for K1ABC in December" queries

**Decision Point:** Only implement if cross-month analysis is frequent. Start with CSV + lightweight index.

#### 4. Audit Metadata
In database metadata table, store:
- SHA-256 hash of each raw CSV file
- Row count verification
- Source file URI/path
- Ingestion timestamp

**Purpose:** Verify data integrity, detect tampering, enable reproducible analysis.

---

## Data Structure Requirements

### 1. Core Decision Records

**Purpose:** Store every correction evaluation, whether applied, rejected, or determined unnecessary.

**Essential Fields:**
- **Identity:** Decision ID, timestamp, original spot ID (for audit trail)
- **Callsigns:** Subject (original), winner (corrected), runner-up (second choice)
- **Location:** Frequency, mode, source feed
- **Decision:** Applied/rejected/no_correction_needed, rejection reason if rejected
- **Consensus Statistics:**
  - Total unique reporters
  - Supporters for subject, winner, and runner-up
  - Confidence percentages (0-100) for subject and winner
- **Distance Metrics:** Edit distance (1-3), distance model used (plain/morse/baudot)
- **Threshold Snapshot:** Configuration values at decision time (enables replay analysis)
- **Validation Results:**
  - CTY database lookup results (subject valid? winner valid?)
  - Known calls database hits (SCP/MASTER.SCP)
  - Final confidence label assigned (C/B/V/P/S/?)

**Why This Matters:**
- Enables threshold sensitivity analysis (replay with different settings)
- Tracks why corrections were rejected (rejection reason distribution)
- Links validation outcomes to decision quality

**Expected Volume:** 350,000 rows/day

---

### 2. Individual Spotter Vote Details

**Purpose:** Preserve evidence of what each spotter actually reported.

**Essential Fields:**
- Link to parent decision
- Spotter callsign
- What they decoded (reported call)
- Their frequency (in Hz for precision)
- Their SNR (signal-to-noise ratio in dB)
- Timestamp of their report
- Vote classification (voted for subject/winner/runner-up/other)

**Storage Strategy - Hybrid Approach:**

The original proposal suggested storing all votes as compressed JSON blobs. While this saves space (90% reduction), it impedes heavy slicing by SNR, specific reporters, or frequency ranges.

**Recommended Hybrid:**

**Option A: JSON Blobs + Per-Reporter Fact Table (Recommended)**
- Store full vote details as JSON blob per decision (for portability/audit)
- ALSO maintain a sparse `top_spotters_fact_table` with one row per (top spotter, decision)
- Top spotters = most active 500-1000 spotters (covers 80% of volume)
- Enables fast "show all decisions where W3LPL voted" without JSON parsing
- Additional storage: ~5% overhead

**Option B: Daily Parquet Vote Tables**
- Keep short JSON blobs in SQLite (for record completeness)
- Export votes to daily Parquet `votes_YYYY-MM-DD.parquet` partitioned by call type
- Use Parquet for heavy analytics (SNR distribution analysis, spotter quality scoring)
- SQLite for operational queries, Parquet for deep analytics

**Option C: Normalized Vote Table with Aggressive Indexing**
- Traditional normalized approach: one row per vote
- Use integer call IDs (lookup table) instead of TEXT
- Partition by decision_id ranges
- Only viable if database can handle 5M+ rows/day efficiently

**Decision Needed:** Choose based on primary query patterns. If spotter quality analysis is frequent, use Option A. If mostly decision-centric queries, use JSON blobs only.

**Why This Matters:**
- Enables SNR-weighted analysis (are high-SNR spotters more reliable?)
- Tracks spotter quality over time
- Supports debugging of specific correction decisions
- Allows vote distribution analysis

**Expected Volume:** 350,000 compressed records/day

---

### 3. Temporal Stability Tracking

**Purpose:** Measure what happens AFTER **ANY** decision (applied, rejected, or no-correction) to validate decision quality.

**Critical Insight:** We need to validate ALL decision types:
- **Applied corrections:** Did we correct to the right call?
- **Rejected corrections:** Should we have corrected but didn't?
- **No-correction decisions:** Was leaving it alone the right choice?

**Essential Fields:**
- Link to parent decision
- Decision type (applied/rejected/no_correction)
- Observation window (60 minutes after decision)

**For Applied Corrections:**
- **Winner appeared naturally count** (without correction) - indicates success
- **Subject reappeared count** (original busted call seen again) - indicates error
- **Winner via correction count** (winner corrected again) - indicates consistent issue

**For Rejected/No-Correction Decisions:**
- **Subject appeared naturally count** (original call appeared uncorrected) - indicates correct decision
- **Alternative calls appeared count** (different calls appeared at same freq/time) - indicates missed correction
- **Alternative call tracking** (what calls appeared and their frequency)

**Stability Metrics:**
- **Applied correction stability:** winner_natural / (winner_natural + subject_reappear) × 100
- **Rejection validity:** subject_natural / (subject_natural + alternatives) × 100
- Last updated timestamp

**Computation Strategy:**
This table is NOT populated in real-time. Instead, run a batch job every hour that:
1. Identifies all corrections from 60+ minutes ago
2. Queries subsequent spots to see what appeared
3. Computes and stores stability metrics

**Temporal Specification Required:**

**Observation Windows (Multiple Horizons):**
- **Short-term (15 minutes):** Detects immediate reappearance patterns
- **Medium-term (60 minutes):** Primary validation window (recommended baseline)
- **Long-term (6 hours):** Captures QSO duration and propagation changes

Store one row per (decision_id, window_type) to compare stability across horizons.

**Matching Rules:**
- **Time tolerance:** Spot must occur in [decision_time + 1 second, decision_time + window_end]
- **Frequency tolerance:** Within ±1 kHz of original decision frequency
- **Call normalization:** Strip SSID suffixes, portable prefixes before comparison
- **Mode filtering:** Only count spots in same mode family (CW/RTTY vs SSB vs digital)
- **Correction chain handling:** If call appears via another correction, count as "winner_corrected" not "winner_natural"

**Stability Calculations:**

**For Applied Corrections:**
```
applied_stability = winner_natural / (winner_natural + subject_reappear) × 100

Interpretation:
- stability = 100: Winner always appears naturally afterward (perfect correction)
- stability = 0: Subject always reappears (correction was wrong)
- stability = NULL: No subsequent appearances (cannot validate)
```

**For Rejected Corrections:**
```
rejection_validity = subject_natural / (subject_natural + alternatives) × 100

Interpretation:
- validity = 100: Subject always appears naturally (correct to reject)
- validity = 0: Alternative calls always appear (false negative - should have corrected)
- validity = 50-99: Mixed signal, competing stations, or ambiguous case
```

**For No-Correction Decisions:**
```
Same as rejection_validity - validates that leaving spot alone was correct
```

**Edge Cases:**
- Multiple corrections of same call in observation window: Count each separately
- Call appears with different SSID: Consider same unless analysis requires distinction
- Competing stations (different grids): Track all alternatives, flag as "ambiguous"
- No subsequent appearances: Mark as NULL stability (common on weak signals)

**Why This Matters:**
- **Validates ALL decisions** - not just corrections, but also rejections and no-action
- Identifies false negatives (should have corrected but didn't)
- Identifies false positives (corrected incorrectly)
- Identifies correct rejections (properly left alone)
- Enables comprehensive precision and recall calculations

**Expected Volume:**
- Applied corrections: 40,000 rows/day × 3 windows = 120,000 rows
- Rejected corrections: 310,000 rows/day × 3 windows = 930,000 rows
- No-correction decisions: Could add 0-40,000 if tracked
- **REVISED TOTAL: ~1,050,000 rows/day** (if tracking all decision types)

---

### 4. Frequency Cluster Context

**Purpose:** Capture competing signals that might be incorrectly merged.

**Essential Fields:**
- Link to parent decision
- Number of distinct callsigns within ±1 kHz
- Frequency range statistics:
  - Minimum frequency in cluster
  - Maximum frequency in cluster
  - Spread (range in kHz)
- Runner-up signal details:
  - Runner-up center frequency
  - Separation from winner frequency

**Why This Matters:**
- Detects competing stations incorrectly merged (frequency guard validation)
- Identifies cases where multiple stations are active nearby
- Validates frequency tolerance settings
- Helps explain high-confidence errors (e.g., W1MN → W1WC was likely two stations)

**Expected Volume:** 40,000 rows/day (only applied corrections)

---

### 5. Quality Score Updates

**Purpose:** Track how quality anchors evolve based on correction outcomes.

**Essential Fields:**
- Link to parent decision
- Callsign affected
- Frequency bin (e.g., 14025000 Hz for 500 Hz bin)
- Old score, new score, delta (+1 for winners, -1 for losers)
- Timestamp

**Why This Matters:**
- Validates quality anchor system effectiveness
- Tracks "reputation" of frequently-seen calls
- Enables analysis of whether quality anchors improve accuracy

**Expected Volume:** 200,000 rows/day (~5 calls per correction × 40K corrections)

---

### 6. Reference Cluster Comparisons

**Purpose:** Link our decisions to external reference data (when available).

**Essential Fields:**
- Link to our decision
- Reference source name (cluster identifier)
- Reference busted call and correct call
- Reference timestamp and frequency
- Match quality metrics:
  - Time delta (how close timestamps matched)
  - Frequency delta (how close frequencies matched)
- Agreement classification:
  - Agree: Both corrected same way
  - Disagree: Both corrected differently
  - Partial: One corrected, other didn't
  - Missed: Reference corrected, we didn't
- Reference cluster quality estimate (see below)
- Notes field for analysis

**Matching Logic Specification:**

**Time Window:** ±5 minutes from our decision timestamp
**Frequency Window:** ±1.0 kHz from our decision frequency
**Call Normalization:**
- Strip SSID suffixes (W3LPL-# → W3LPL)
- Remove portable indicators (W6/UT5UF → UT5UF for comparison)
- Uppercase, trim whitespace
- Handle beacon suffixes (/B, /BCN)

**Agreement Classification Rules:**

| Our Decision | Reference Decision | Classification | Notes |
|--------------|-------------------|----------------|-------|
| Applied X→Y | Marked X→Y | Agree | Both corrected identically |
| Applied X→Y | Marked X→Z | Disagree | Both corrected differently |
| Applied X→Y | No mark | Partial (false positive?) | We corrected, they didn't flag |
| Rejected | Marked X→Y | Missed | They caught it, we didn't |
| Not evaluated | Marked X→Y | Missed (not seen) | Signal didn't reach our feed |

**Reference Cluster Quality Estimation:**

Since reference cluster quality is unknown, use **Dawid-Skene disagreement modeling**:

**Approach:**
1. For corrections where BOTH clusters acted, measure agreement rate
2. Use temporal stability (Method 1A) to estimate "ground truth" probability for each decision
3. Build confusion matrix: P(reference=correct | our_stability_score)
4. Estimate reference cluster's reliability: P(reference_correct) across all comparisons
5. Assign each reference source a quality score (0.0-1.0)

**Output:** `reference_quality_scores` table with per-source reliability estimates.

**Why This Matters:** Avoids treating reference data as binary ground truth. Weighs comparisons by estimated reference reliability.

**Why This Matters:**
- Enables calibration against external data
- Computes precision (correct/total) and recall (correct/missed)
- Identifies systematic differences between systems
- Provides training data for machine learning approaches

**Expected Volume:** 500 rows/day (sparse - only where external data exists)

---

### 7. Daily Summary Statistics

**Purpose:** Pre-computed rollup for quick reporting.

**Essential Fields:**
- Date
- Volume counts:
  - Total spots evaluated
  - Corrections applied, rejected, not needed
- Rejection reason breakdown (distribution)
- Quality metrics:
  - CTY validation rate (% applied corrections that are CTY-valid)
  - SCP hit rate (% corrections toward known calls)
  - Average and median winner confidence
- Distance distribution (count by edit distance 1/2/3)
- Mode breakdown (CW/RTTY/SSB distribution)
- Temporal stability summary (average stability, high-stability count)
- Computation timestamp

**Why This Matters:**
- Fast daily reporting without full database scans
- Trend analysis over weeks/months
- Performance monitoring

**Expected Volume:** 1 row/day

---

## Database Optimization Strategies

### Storage Optimizations

**1. Store Only Correction Candidates**
Don't log all 7 million spots - only the ~350,000 where correction was evaluated.
**Savings:** 95% storage reduction

**2. Use Integer Foreign Keys for Lookups**
Replace repetitive string values with integer references:
- Modes: CW=1, RTTY=2, USB=3, LSB=4
- Sources: RBN=1, RBN-DIGITAL=2, PSKREPORTER=3
- Rejection reasons: max_edit_distance=1, min_reports=2, etc.
- Distance models: plain=0, morse=1, baudot=2
- Decision outcomes: no_correction=0, applied=1, rejected=2

**Savings:** 40% space reduction, 3× faster queries

**3. Bit-Pack Boolean Flags**
Store CTY/SCP validation results as single integer with 4 bits instead of 4 separate columns.
**Savings:** 75% reduction on these fields

**4. Integer Frequency Storage**
Store frequencies as integer millihertz (14.0255 MHz → 14025500) instead of floating point.
**Benefits:** Exact equality comparisons, faster indexing, 50% smaller

**5. Threshold and Configuration Versioning**

Don't duplicate threshold configuration in every row. Store unique configurations once, reference by ID.

**Threshold Sets Table:**
- `threshold_set_id` (primary key)
- All threshold parameters (strategy, min_confidence, max_distance, etc.)
- Unique constraint on parameter combination
- Created timestamp

**Configuration Versions Table:**
- `config_version_id` (primary key)
- Full config snapshot (JSON or key-value pairs)
- Algorithm version (e.g., "majority_v2", "morse_distance_v1.3")
- Effective timestamp range (start, end)
- Git commit hash (if available)

**Decision Table Links:**
- Each decision references `threshold_set_id` AND `config_version_id`
- Enables "what-if" replay: "Apply 2025-01 decisions using 2025-06 thresholds"

**Why This Matters:**
- Eliminates 100+ bytes per row (significant at 350K rows/day)
- Enables accurate historical replay
- Tracks algorithm evolution over time

**Savings:** Eliminates 100+ bytes per row

**6. Compressed Spotter Votes**
Store as JSON array with short keys instead of individual rows.
**Savings:** 90% vs normalized table

### Performance Optimizations

**1. Write-Ahead Logging (WAL) Mode**
Enables concurrent reads during writes, essential for high-volume logging.
**Performance:** 10-20× faster writes than default rollback journal

**2. Strategic Indexing**
Index on common query patterns:
- Timestamp (for temporal queries)
- Frequency (for frequency-based analysis)
- Subject and winner calls (for call tracking)
- Decision outcome (for filtering applied vs rejected)
- Mode and source (for subset analysis)
- Composite indexes (frequency + timestamp for range queries)

**3. Batch Inserts**
Group inserts into transactions of 1,000 rows.
**Performance:** 100× faster than individual inserts

**4. Larger Page Size**
Use 8KB pages instead of default 1KB for better SSD performance.
**Performance:** 20-30% faster on modern storage

**5. Composite Indexes for Common Query Patterns**

Beyond single-column indexes, add composites for frequent filters:
- `(mode_id, ts)` - "All CW decisions in time range"
- `(freq_mhz, ts)` - "All decisions on 14 MHz in time range"
- `(subject, ts)` - "Track specific call over time"
- `(winner, ts)` - "Track corrected call over time"
- `(decision_enum, reject_reason_id)` - "Why were corrections rejected?"

**6. Enforce Data Integrity Constraints**

**Primary Keys:**
- All tables use stable, auto-increment INTEGER primary keys
- `WITHOUT ROWID` optimization where PK is good physical key

**Foreign Keys:**
- Enable `PRAGMA foreign_keys = ON`
- All reference columns have foreign key constraints
- Prevent orphaned records

**Unique Constraints:**
- Prevent duplicate decisions: `UNIQUE(ts, subject, freq_mhz, mode_id)` with resolution policy
- Threshold sets: `UNIQUE` on parameter combination
- Lookup tables: `UNIQUE` on name columns

**Check Constraints:**
- Confidence values: `CHECK(subject_conf BETWEEN 0 AND 100)`
- Distance values: `CHECK(dist BETWEEN 0 AND 3)`
- Frequency sanity: `CHECK(freq_mhz BETWEEN 1800000 AND 30000000)`

---

## Revised Storage Estimates @ 7M Spots/Day

### SQLite Database (Analysis Data)

| Component | Rows/Day | Storage/Row | Daily Total |
|-----------|----------|-------------|-------------|
| Core decisions | 350,000 | 200 bytes | 70 MB |
| Spotter votes (compressed JSON) | 350,000 | 400 bytes | 140 MB |
| **Top spotters fact table** | **100,000** | **50 bytes** | **5 MB** |
| **Temporal stability (ALL decisions, 3 windows)** | **1,050,000** | **80 bytes** | **84 MB** |
| Frequency clusters | 40,000 | 80 bytes | 3 MB |
| Quality updates | 200,000 | 40 bytes | 8 MB |
| Reference comparisons | 500 | 150 bytes | 0.1 MB |
| **All-spots index** | **7,000,000** | **30 bytes** | **210 MB** |
| Daily statistics | 1 | 1 KB | <0.1 MB |
| Lookup tables | Static | 5 KB | <0.1 MB |
| Config versions / threshold sets | ~10 | 500 bytes | <0.1 MB |
| Indexes + SQLite overhead | - | ~25% | 130 MB |
| **SQLite TOTAL** | - | - | **~650 MB/day** |

### Raw Data (CSV Files)

| Component | Daily Size | Notes |
|-----------|------------|-------|
| Raw RBN CSV (compressed) | 500 MB | gzipped, complete audit trail |
| **COMBINED DAILY TOTAL** | **~1.15 GB/day** | SQLite + CSV |

### Long-Term Retention

**With Raw Data:**
- **Monthly:** ~39 GB (SQLite) + ~15 GB (CSV) = ~54 GB
- **Yearly:** ~475 GB (SQLite) + ~180 GB (CSV) = ~655 GB

**With Monthly Parquet (optional):**
- Replace daily CSV with monthly Parquet: ~4 GB/month
- **Yearly:** ~475 GB (SQLite) + ~48 GB (Parquet) = ~523 GB

**Retention Strategy Recommendations:**
- **Hot data (last 30 days):** Keep SQLite + CSV uncompressed
- **Warm data (31-365 days):** Keep SQLite, compress CSV to .gz
- **Cold data (>1 year):** Archive SQLite to .gz, keep Parquet monthly mirrors only

This is manageable for 3-5 year retention on modern storage (2-3 TB total).

---

## Analysis Capabilities Enabled

### Query Patterns Supported

**1. Comprehensive Temporal Consistency Analysis**

**Validate Applied Corrections:**
- Find corrections with high stability (>80%) to confirm accuracy
- Identify low-stability corrections (<50%) as potential errors
- Correlate stability with confidence, SNR, distance

**Validate Rejected Corrections (False Negatives):**
- Find rejections with low validity (<50%) where alternative calls appeared
- Identify which threshold caused rejection (reason analysis)
- Calculate false negative rate by rejection reason

**Validate No-Correction Decisions:**
- Confirm spots left alone were correct (subject appeared naturally)
- Identify cases where correction should have occurred but wasn't evaluated

**Comprehensive Metrics:**
- True Positive Rate: Applied corrections with high stability
- False Positive Rate: Applied corrections with low stability
- True Negative Rate: Rejections with high validity
- False Negative Rate: Rejections with low validity
- Precision: TP / (TP + FP)
- Recall: TP / (TP + FN)
- F1 Score: 2 × (Precision × Recall) / (Precision + Recall)

**2. Threshold Sensitivity Analysis**
Simulate different threshold settings without reprocessing:
- Query: "How many corrections would be rejected at 65% confidence vs 60%?"
- Compare stability rates at different threshold levels
- Optimize threshold settings based on historical data

**3. SNR-Weighted Precision**
Compare reliability of high-SNR vs low-SNR spotters:
- Group by SNR thresholds (high/medium/low)
- Compute average stability for each group
- Validate SNR gating effectiveness

**4. Known Calls Validation**
Cross-reference with CTY and SCP databases:
- % corrections from unknown → known calls (likely good)
- % corrections from known → unknown calls (likely bad)
- CTY validity rate by mode, distance, confidence

**5. Rejection Reason Analysis (Critical for False Negatives)**
Understand why we didn't correct calls that should have been corrected:
- For each rejection reason, compute validity score distribution
- Identify rejections with low validity (alternative calls appeared)
- Calculate: "What % of rejections were correct decisions?"
- **Example query:**
  - Rejections due to "min_confidence" threshold
  - Where rejection_validity < 50% (alternative calls appeared)
  - Result: "35% of confidence rejections were false negatives"
- Identify which thresholds are too strict (causing false negatives)
- Identify which thresholds are too lenient (causing false positives)

**6. Competing Signal Detection**
Find corrections with strong runner-up at different frequency:
- Flag cases where runner-up had >50% of winner support
- Check frequency separation
- Validate frequency guard settings

**7. Distance Model Validation**
Compare morse-aware vs plain distance:
- Cases where morse distance significantly differs from plain
- Correlation with temporal stability
- Effectiveness of mode-specific distance models

**8. Reference Cluster Calibration**
When external data available:
- Agreement rate (our applied = their applied)
- Disagreement analysis (we applied, they didn't or vice versa)
- Precision and recall calculations
- Trust score for reference cluster

---

## Implementation Roadmap

### Decision Points Required Before Implementation

**1. Raw Data Strategy:**
- [ ] Keep all-spots index in SQLite? (Adds 210 MB/day but enables missed correction analysis)
- [ ] Keep raw CSV files? (Recommended: yes, for audit trail)
- [ ] Generate monthly Parquet? (Optional, for heavy cross-day analytics)

**2. Spotter Vote Storage:**
- [ ] JSON blobs only? (Simplest, 90% space savings)
- [ ] JSON + top spotter fact table? (Recommended, enables fast spotter queries)
- [ ] Normalized table? (Only if spotter analysis is primary use case)
- [ ] Daily Parquet export? (For analytics-heavy workflows)

**3. Temporal Stability Windows:**
- [ ] Single 60-minute window? (Simplest)
- [ ] Multiple horizons (15m/60m/6h)? (Recommended, detects different patterns)

**4. Reference Cluster Quality:**
- [ ] Treat as ground truth? (Not recommended)
- [ ] Implement Dawid-Skene quality estimation? (Recommended, more defensible)

### Phase 1: Foundation (Week 1)
**Objective:** Establish core logging capability

**Deliverables:**
- Database schema with core tables (decisions, votes, lookups, all-spots index)
- Raw CSV ingestion pipeline with hash verification
- Integration point in main correction logic
- Dual logging (continue JSON, add SQLite)
- Basic query examples

**Decision Artifacts:**
- Document chosen approach for decisions 1-4 above
- Finalize schema SQL script
- Define matching rules and tolerances

**Success Criteria:**
- Real-time logging during production with <5ms overhead per decision
- Daily database files created successfully with integrity checks passing
- CSV hash verification working

### Phase 2: Validation Data (Week 2)
**Objective:** Add temporal and cluster context

**Deliverables:**
- Temporal stability tracking (batch job, multiple windows)
- Frequency cluster summaries
- Initial analysis queries for Methods 1A and 1B
- All-spots index population (if decision 1 = yes)

**Success Criteria:**
- Stability scores computed hourly for all windows
- Ability to query stability by confidence/distance/mode
- Missed correction detection working (requires all-spots index)

### Phase 3: Quality & Reference (Week 3)
**Objective:** Add advanced tracking and external calibration

**Deliverables:**
- Quality score update logging
- Reference comparison import tool with matching logic
- Reference cluster quality estimation (Dawid-Skene)
- Daily statistics rollup job

**Success Criteria:**
- Quality evolution trackable over time
- Reference cluster data importable and queryable
- Quality scores computed for reference sources
- Daily dashboard queries running in <1 second

### Phase 4: Analysis & Reporting (Week 4)
**Objective:** Operationalize analysis

**Deliverables:**
- Query library for all validation methods
- Daily analysis report generator
- Threshold optimization tool (replay with varied thresholds)
- Visualization dashboard or reports

**Success Criteria:**
- Automated daily reports with key metrics
- Threshold optimization recommendations based on historical stability
- Missed correction reports identifying patterns
- Spotter quality rankings (if fact table implemented)

---

## Operational Considerations

### Audit and Data Integrity

**File Hash Tracking:**
Store in metadata table for each daily database:
- SHA-256 hash of source RBN CSV file
- Row count from CSV vs row count in all-spots index (must match)
- Ingestion timestamp
- Source file path/URI
- Database creation timestamp
- Schema version

**Purpose:**
- Detect file corruption or tampering
- Enable reproducible analysis (verify input data unchanged)
- Track data lineage

**Implementation:**
Compute hash during CSV ingestion, store before closing database.

### Retention and Compaction

**Daily Operations:**
1. Create new daily database at 00:00 UTC
2. Ingest previous day's data (can lag by hours if needed)
3. Run integrity checks (hash verification, row counts)
4. Compress previous day's CSV to .gz
5. Compute daily statistics rollup

**Monthly Operations:**
1. Generate monthly summary reports
2. Optional: Export to Parquet for cross-month analytics
3. Compress SQLite files older than 30 days to .gz
4. Archive to cold storage if desired

**Yearly Operations:**
1. Consolidate extremely old data (>2 years) to annual Parquet files
2. Purge raw CSV if disk space critical (keep Parquet + SQLite)
3. Generate annual summary reports

### Headline Metrics Rollup

**Problem:** Avoid rescanning entire database for daily dashboards.

**Solution:** `daily_statistics` table (pre-computed)

**Computed Metrics:**
- Total spots evaluated, corrections applied/rejected
- Rejection reason distribution
- Average confidence, stability, CTY hit rate
- Mode and distance distributions
- Top 10 most-corrected calls
- Top 10 spotters by volume

**Computation:** Run nightly batch job after temporal stability updates complete.

**Access:** Dashboard queries only `daily_statistics` table, never scans raw decisions.

---

## Key Design Principles

1. **Optimize for analysis, not real-time queries** - Batch processing is acceptable for validation
2. **Store raw evidence, compute metrics later** - Don't pre-aggregate prematurely
3. **Enable threshold replay** - Must support "what-if" analysis with different settings
4. **Support multiple validation methods** - Each validation approach needs specific data
5. **Preserve audit trail** - Link back to original spot IDs and raw data
6. **Balance normalization vs performance** - Use compression where appropriate
7. **Plan for growth** - Structure scales to years of data
8. **Maintain compatibility** - Keep existing JSON logging during transition

---

## Success Metrics

The database design will be successful if it enables:

✅ **Objective quality assessment** - Compute correction accuracy without ground truth
✅ **Threshold optimization** - Data-driven tuning of configuration
✅ **Error pattern detection** - Identify systematic correction failures
✅ **Performance monitoring** - Track correction quality over time
✅ **Method comparison** - Evaluate different correction strategies
✅ **Reference calibration** - Assess quality of external data sources
✅ **Scalable storage** - Sustainable for multi-year retention

---

## Summary of Revisions (v2.0)

**Addressed Gaps from External Review:**

1. **Raw Data Handling:** Added lightweight all-spots index (210 MB/day) to enable missed correction analysis and drift detection. Keeps raw CSV files with SHA-256 hashes for audit trail.

2. **Spotter Vote Storage:** Expanded from JSON-only to hybrid approach recommendation (JSON + top spotter fact table) for fast per-spotter queries while maintaining space efficiency.

3. **Temporal Stability Specification:** Defined exact matching rules, multiple observation windows (15m/60m/6h), and edge case handling for stability computation.

4. **Reference Cluster Quality:** Added Dawid-Skene disagreement modeling approach to estimate reference cluster reliability rather than treating as binary ground truth.

5. **Configuration Versioning:** Added config_version table linking to algorithm versions and git commits for accurate historical replay.

6. **Audit and Integrity:** Added SHA-256 hashing, row count verification, foreign key constraints, and unique constraints to prevent duplication.

7. **Composite Indexes:** Specified key composite indexes for common query patterns (mode+time, freq+time, call+time).

8. **Storage Estimates:** Revised from 260 MB/day to ~650 MB/day (SQLite) + 500 MB/day (CSV) = ~1.15 GB/day total, ~655 GB/year with all raw data retained. Increased to track temporal stability for ALL decisions (applied, rejected, no-correction), not just applied corrections.

**Trade-offs Acknowledged:**
- Additional 210 MB/day for all-spots index enables critical missed correction analysis
- Hybrid vote storage adds 5% overhead but dramatically improves query performance for spotter analysis
- Tracking stability for ALL decisions (not just applied) increases temporal stability table from 7 MB to 84 MB/day but enables comprehensive false negative detection
- Multiple temporal windows triple stability table size but reveal different pattern types
- Keeping raw CSV doubles storage but provides essential audit trail and reproducibility

**Decision Points for Implementation Team:**
Four key architectural decisions must be made before implementation (see roadmap section). Each has clear trade-offs documented.

---

## Conclusion

This revised design provides a comprehensive foundation for objective analysis of call correction quality using available data sources (RBN feed and internal decision logs) while addressing critical gaps in raw data access, temporal specifications, and audit requirements.

The revised storage estimate of ~1.15 GB/day (~655 GB/year with CSV, ~523 GB/year with Parquet) is manageable for multi-year retention on modern storage. The hybrid approach balances detailed evidence preservation, query performance, and storage efficiency.

**Key Enhancement:** By tracking temporal stability for ALL decisions (applied, rejected, and no-correction), we can compute comprehensive precision and recall metrics, identify false negatives (missed corrections), and validate that our rejection thresholds are correctly calibrated.

The database structure supports:
- Multiple independent validation methodologies that triangulate on correction accuracy
- Retrospective threshold optimization through replay analysis
- Missed correction detection and pattern analysis (via all-spots index)
- Reference cluster quality calibration (via disagreement modeling)
- Long-term trend analysis and algorithm evolution tracking
- Reproducible analysis through cryptographic hashing and versioning

The phased implementation approach with explicit decision points allows incremental deployment with continuous validation, minimizing risk while building toward comprehensive analysis capability.
