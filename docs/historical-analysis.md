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
