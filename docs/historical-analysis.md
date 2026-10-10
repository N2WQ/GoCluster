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
