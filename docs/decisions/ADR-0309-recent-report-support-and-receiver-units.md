# ADR-0309: Recent Report Support and Receiver Units

- Status: Accepted
- Date: 2026-10-15
- Decision Origin: Design, Approved v2; timing exceptions Approved v4/v5;
  measurement-boundary repair Approved v6

## Context

Lifetime input counts describe historical volume, including numerically unusable
reports. They do not measure current usable evidence. Tiny tracked receiver
remainders also cannot establish a useful diversity floor. The selected glyph
remains a regional SNR summary, not QSO probability or calibrated confidence.

## Decision

Retain one bucket-owned float64 recent raw usable report counter. Age it at the
existing monotonic reference and add incoming aged report count after the raw
numerical boundary. Signal weight is separate: a fresh usable report weighted
0.01 contributes one report unit. Raw numerical rejection adds zero; later cap
rejection preserves the raw unit. A quarter-admitted report adds 1 raw unit and
0.25 admitted units. Existing fractional cappedCount supplies admitted support.
Keep every numerical materialization boundary and cap-admission fraction.

Selected unique populations supply support before display coefficients or
single-direction discounts. Off/shadow gate recent raw usable units; enforce
gates recent admitted units. Use exact fractional comparisons with no epsilon
promotion, integer rounding or lifetime fallback. Zero is explicit. Configured
integer floors and saved settings retain their schemas and values, including
exact comparisons above the integer precision of float64.

Carry both fractional quantities and explicit admitted availability in Sample
and observed Result provenance. Admitted units are unavailable when cap tracking
is off. Preserve this observed value through existing forecast replacements.
All-dropped prediction-stale quantities remain diagnostics only; they cannot
fund a surviving fresh selection or revive store-expired evidence.

Receiver diversity sums each tracked hash's accepted aged quantities across
distinct selected populations before the inclusive one-unit threshold. 0.6+0.6
qualifies one receiver; a shared bucket containing 0.6 qualifies none. Do not
prefilter tiny quantities before summing. Signal weight cannot supply receiver
units. Keep the bounded slots, replacement policy, physical capacities and
eight-unit per-bucket cap. Slot rotation remains an approximation; aggregate
accepted volume is not reconstructed from current slots.

Preserve the configured ordinary/beacon receiver requirement:
ceil(configured floor / receiver cap), bounded by physical capacity. A stricter
user PATHSAMPLES setting raises recent support only. Shadow tests admitted
support at that effective user floor while retaining the configured receiver
baseline. It describes selected-population gates, not an alternative capped
median; absent/all-stale diagnostics do not create a shadow-block claim.

Keep legacy integer diagnostics and place a compact /u recent-unit token after
the lifetime count in observed/insufficient DIAG tags. Insufficient reasons
precede optional cap details. Display rounding occurs after qualification;
the fixed wire budget can still clip the reason when a report is present.
Forecast tag formats, SNR thresholds, CLOSED precedence and native160 remain.

## Alternatives considered

1. Keep lifetime counts: old or numerically unusable reports can fund a glyph.
2. Use signal mass as report count: changes the report-support control into a
   signal-strength control and duplicates the separate weight gate.
3. Round support or qualify receivers before union: loses fractional support
   and can count the same identity incorrectly.
4. Retain report/receiver history globally: broadens ownership and resource
   scope beyond the bounded bucket/slot design.

## Consequences

Stricter support can produce fewer observed glyphs and more existing forecast
fallbacks. 21 fresh reports meet 21; after one half-life 10.5 no longer do. CLOSED
can still replace positive insufficient evidence. These operating policies are
not calibrated confidence or independent-sample guarantees.

Logical bucket size may rise from 448 to 456 bytes. Actual allocator rounding
and complete Store retention must pass the 40-byte incremental per-bucket gate;
zero warmed allocations and nonwaived performance guards remain required.
No learned-store persistence migration, new retained registry, configuration
value change or firewall change is introduced.

Approved v5 excepts comparative elapsed-time acceptance for the preserved S7
production source. Nine controls passed, one exceeded 1.10 at 1.1731677803 and
seventeen were unrun. The failure remains a failure; practical CPU and delivery
effects await full-cluster profiling. Zero warmed allocations, controlled
profiles, logical size and the 40-byte retained-memory increase remain gates.
The v5 resource observer was permitted to recognize only the identified
16-byte runtime timer event with exact counters and its captured stack; charge
candidate residual bytes without crediting baseline noise. This changes no
production support, receiver, SNR or forecast semantics.

The v5 resource observer subsequently rejected a different baseline pattern:
one 32-byte allocation and a 16-byte free. A separate enforce update batch
recorded one unattributed allocation. Neither failure is waived or converted
into a pass. TSR-0099 preserves this evidence and its observer follow-up.

Approved v6 replaces the quiet-runtime assumption with a conservative bound:
candidate PostGC-minus-Start TotalAlloc, less verified NEW baseline bucket and
sidecar bytes, must remain at most 40 bytes per new bucket. Charge temporary
candidate allocations and give no baseline-noise, warm-owner or frees credit.
Exact live-owner profiles, allocator classes and all five counter identities
remain required. Exceeding the bound blocks acceptance while actual retained
growth remains inconclusive. Logical bucket size remains at most 456 bytes.

The six warmed update allocation gates use standard AllocsPerRun(1), separate
prebuilt warm/measured owners and 100,000 real public updates per callback.
Exact state and recent support must pass before the allocation verdict; a
deliberate measured allocation must fail. These single-processor controls do
not explain the earlier four-processor allocation or prove concurrent behavior.
All replacement gates passed at the S7 local checkpoint. Original v5 profiles
remain bound to their original binaries for the unchanged production source.
The Windows report transport repair reused its one saved baseline and executed
only 39 remaining processes; it changed no numerical or resource acceptance gate.

## Links

- Code: pathreliability/report_support.go, store.go, candidate_query.go,
  predictor.go; telnet/server.go.
- Tests: recent_support_accounting_test.go, recent_support_admission_test.go,
  recent_receiver_units_test.go, recent_support_resource_test.go;
  telnet/path_recent_support_test.go and path_recent_support_provenance_test.go.
- Docs: pathreliability/README.md, data/config/PATH_PREDICTIONS.md,
  docs/OPERATOR_GUIDE.md, docs/path-reliability-first-wave-execution.md.
- Troubleshooting: TSR-0099, global heap-counter observers.
- Supersedes ADR-0134's lifetime-floor policy; preserves its separate configured
  receiver requirement and refines ADR-0308's temporary receiver qualification.
