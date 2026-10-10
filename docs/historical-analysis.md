# Historical decision analysis

These commands inspect an existing historical SQLite database. They do not
generate production decisions or replace [pipeline replay](rbn_replay.md).
The stored schema is a legacy input: a synthetic database qualifies the reader,
not the provenance of a production database.

## Distance-3 observations

```powershell
go run ./cmd/analyze_distance3 -db C:\data\decisions.db
```

The command selects `distance = 3` from `decisions`, reading `subject`, `winner`,
`winner_support`, `subject_support`, `total_reporters`, `winner_confidence`,
`decision` and `reason`. The existing default database path is retained;
supply `-db` for your input.
Existing SQLite open behavior is retained: a missing path can create an empty
database before the schema error is reported.

Only case variants of `applied` and `rejected` are recognized. NULL, empty,
padded or other labels are unknown, with a separate value breakdown.
Outcome percentages use all selected rows. Applied-only means and minima exclude
rejected and unknown rows. Missing populations produce `N/A`, not a measured
zero. Rejection reasons count only recognized rejections; samples show at most
ten applied and five rejected rows. Breakdown keys and sample ordering are
deterministic for selected values.

`winner_confidence` is a logged score, not a measured probability of correctness.
Advantage is `winner_support - subject_support`; stored counts/scores are assumed
valid. There is no accuracy, reliability or calibration evaluation in this report.

The six hypothetical scenarios preserve the original scalar comparison choices.
They assume bases of three winner reports, advantage one and score 60, then add
the displayed extras. They do not load current settings or replay other gates.
For each scenario, the report distinguishes applied rows retained or removed,
rejected rows added and their signed net change. Unknown rows passing the
predicates are separate and excluded from recognized totals and net changes.
Passing a hypothetical gate does not mean the correction was correct.

A valid empty database succeeds with zero counts and unavailable statistics.
Open, query, scan, iteration, row-close and output failures return unsuccessful
status. Failed database loads publish no partial counts. A later stdout failure
may leave a partial report; use status and stderr before trusting it.

The command loads its selected rows for this invocation. It is an offline tool,
not a server queue; memory depends on input cardinality.

See [ADR-0277](decisions/ADR-0277-descriptive-historical-decision-analysis.md),
[TSR-0075](troubleshooting/TSR-0075-historical-analysis-claims-and-failures.md)
and [qualification](command-fix-execution.md#checkpoint-26-descriptive-distance-3-analysis).

## Hypothetical threshold comparison

```powershell
go run ./cmd/simulate_threshold -db C:\data\decisions.db -min-reports 3 -min-advantage 1 -min-confidence 55
```

`simulate_threshold` selects distances 1 through 3 from the legacy `decisions`
table. In addition to the distance-3 reader's columns, it requires `id`,
`min_reports`, `min_advantage` and `min_confidence`. It preserves uppercase
callsign display and existing flags/defaults. The three numeric flags accept
integers; they describe hypothetical gates, not recommended production settings.

Outcome recognition and unknown handling follow the descriptive rules above.
The report shows every stored baseline tuple and its frequency across all
selected rows, instead of inferring common current settings from one record.
Outcome percentages use all selected rows. No selected rows means no inferred
baseline and unavailable ratios.

The comparison tests minimum winner reports, support advantage and logged score.
Distance 3 deliberately retains assumed extras of advantage one and score five,
with no extra reports. It does not load current configuration or replay other
correction gates. Threshold sums beyond the native integer range are impossible
for native integer inputs and cannot wrap into permissive gates.

For recognized outcomes, the report gives applied rows retained or removed and
rejected rows added. Net change is **added minus removed**. The passing-row
percentage uses applied plus rejected rows; the change percentage uses the
original applied count. A zero denominator is `N/A`. Unknown passes stay
separate and do not change those totals. At most ten added-row samples appear;
passing these scalar tests does not establish correctness.

The tool publishes no partial counts after a failed load and exits unsuccessfully
on query, scan, iteration, close or output failure. Existing SQLite creation and
late partial-stdout behavior are retained. Its loaded rows and tuple breakdown
use memory proportional to input cardinality. No stability, accuracy or
calibration prediction follows from this report.

See [checkpoint 27 qualification](command-fix-execution.md#checkpoint-27-hypothetical-threshold-set-comparison).

## Distance summaries

```powershell
go run ./cmd/analyze1c -db C:\data\decisions.db
```

`analyze1c` requires only `distance`, `decision`, `winner_confidence` and `reason`
in the legacy `decisions` table. It selects every row and groups by the stored
distance, including zero or values outside 1 through 3. Its database flag and
default remain unchanged.

Known outcomes follow the case-only recognition rules above; NULL, empty,
padded or other outcomes stay unknown. Overall percentages use all selected
rows; each distance's percentages use that distance's rows. Raw unknown values
are reported separately. Rejection reasons contain only recognized rejections,
with that distance's rejection count as denominator. NULL/empty reasons retain
the existing `UNKNOWN` bucket. A stored literal `UNKNOWN` shares that bucket.
Reasons sort by descending count, then by their literal text.

Mean and median use only applied logged integer scores. Empty applied groups
show `N/A`; an observed zero remains `0.0`. Arithmetic sums exactly before
division and final floating conversion, avoiding integer overflow and early
rounding of opposite extremes. The displayed one-decimal values are approximate
at large magnitudes. Scores are unvalidated logs rather than correctness
probabilities, and their presentation has no percent unit.

Empty valid input succeeds with unavailable ratios/statistics. Failed loads
publish no partial report; query, scan, iteration, close or output failures exit
unsuccessfully. SQLite file creation and late partial-stdout effects remain.
Invocation-local maps and applied-score slices use memory proportional to input.
No correlation, accuracy, calibration, reliability or optimal-setting evaluation
follows from these descriptive summaries.

See [checkpoint 28b qualification](command-fix-execution.md#checkpoint-28b-descriptive-distance-summaries).

## Decision inspection

```powershell
go run ./cmd/inspect_decisions -db C:\data\decisions.db
```

`inspect_decisions` preserves its database flag/default and legacy projections.
It counts every row, shows separate applied/rejected/unknown totals, and lists
raw outcome spellings. Case-only known outcomes agree across the breakdown and
all samples. NULL and the literal string `<NULL>` remain distinct. Binary
grouping preserves literal unknown spellings even on a `NOCASE` column.

Each sample list has at most ten rows. Applied samples use lexical selected-field
ordering. Distance-1 recognized rejections sort by winner support descending;
recognized rejections with logged scores strictly greater than 70 sort by score
descending. Selected-field binary ties make the capped lists deterministic.
Unknown rows are excluded before either rejection limit. These orderings describe
selected samples, without a chronological or correctness claim.

Each selection scans only its original columns; unused or unselected numeric
fields acquire no validation requirement. Distance-1 samples include stored
thresholds. NULL sample reasons retain `UNKNOWN`, while valid empty reasons
remain empty, preserving this command's existing distinction.

The four SQL selections run independently. A database changing during inspection
can differ between those selections; this command establishes no single database
snapshot. All four must load before reporting. Failed query, scan, iteration,
close or output returns unsuccessful status. Empty compatible input succeeds
with zero totals and no samples. Raw-label memory depends on input cardinality;
the three sample lists retain at most thirty rows in total. Logged scores remain
unvalidated, and the output establishes no correction accuracy or calibration.

See [checkpoint 29 qualification](command-fix-execution.md#checkpoint-29-consistent-decision-inspection).

## Reappearance observations

```powershell
go run ./cmd/investigate_reappearances -db C:\data\decisions.db -lookahead 24
```

This command selects case-only applied seeds using its original `id`, `ts`,
`subject`, `winner`, `freq_khz`, `distance` and `winner_confidence` columns.
NULL, empty, padded or other seed outcomes are excluded. The database and
lookahead flags/defaults remain; lookahead retains the existing integer-to-Go
duration conversion. Historical numbers are unvalidated, and frequency
interpretation assumes finite values.

For each seed, the command selects at most the first **100 combined follow-up
rows** whose uppercase subject equals either normalized seed identity within
`(seed time, seed time + lookahead]`. Follow-up outcome labels do not filter that
population, and follow-up winner fields are unused. The cap is shared between
identities. One identity at row 101 can therefore be missed even if it appears
inside the requested interval.

Each identity contributes its first observed frequency. Later values do not
replace it, including an initial zero; when normalized identities coincide, one
follow-up can satisfy both. Ordering uses timestamp, ID and every selected-value
tie field, with binary text comparisons. No unique-ID schema rule is added.
Samples contain at most twenty resulting dual cases. Separation below 0.5 kHz
and at least 0.5 kHz uses the dual-case denominator; no cases gives `N/A`.

The capped first-frequency method supplies logged observations. It does not
establish station legitimacy, correction accuracy, calibration or simultaneous
operation. Multiple SQL statements promise no single database snapshot. Every
query, scan, iteration and close is checked, including later selected rows after
both identities are already observed. Loading failure publishes no partial
report and exits unsuccessfully; failed output also exits unsuccessfully.
Invocation memory holds dual cases proportional to input and one active nested
cursor, without retaining follow-up frequency arrays.

See [checkpoint 30 qualification](command-fix-execution.md#checkpoint-30-descriptive-reappearance-observations).
