# ADR-0334: Frequency-Change-Aware Secondary Dedupe

- Status: Accepted
- Date: 2026-11-10
- Decision Origin: Design

## Context

Secondary FAST/MED/SLOW suppression groups reports by band, reporting country
and area, DX call, and human/skimmer source class. Its historical key omits
frequency and mode. A reported frequency or mode change can therefore remain
hidden until the suppression window expires.

The owner selected earlier visibility of reported changes, accepting more
repeats without requiring corroboration. Newly reported and stronger-report
exceptions must continue to work at any frequency, including after the change
limit is exhausted. These are operating policies, not verified QSY detection.
The owner approved implementation and local validation as scope v6. Sample
capture and persistence from earlier proposals were deferred.
Scope v7 subsequently amended only the shadow lab retained-memory target:
at most 4 times matched off memory at both 100,000 and 200,000 historical keys
with 20% additional mode keys, including both caches and fixed comparison
state. This neither selects a production memory multiplier nor relaxes the
CPU/allocation budgets or activation boundary.

## Decision

Keep the feature off by default. Required startup settings live under
`dedup.secondary_frequency_changes` in `dedupe.yaml`:

- `off`: construct only the historical baseline engine;
- `shadow`: baseline decisions drive all spot output while a separate candidate
  cache receives the same ordered inputs and supplies comparison counters;
- `on`: construct only the frequency-change-aware candidate engine.

The candidate adds a canonical mode byte at zero-based offset 21 of the
32-byte secondary key for skimmer sources. Human keys remain mode-free.
Configured canonical modes, including custom/hidden modes and USB/LSB, have
bounded immutable identities. Canonical variants share identity; a reserved
unknown identity provides defensive handling without an incoming-string
interner. The historical baseline encoding and value layout remain unchanged.

For a live candidate entry, evaluate report exceptions first. An exception
renews `when` and replaces report state. If outside tolerance, it also moves
anchor frequency and anchor time, even for an earlier minute or exhausted
budget; it never changes the change count. Otherwise, an in-tolerance report
is suppressed. An out-of-tolerance report passes only if its minute is not
earlier than the anchor minute and the configured change count is available.
That admission moves the anchor, replaces report state and increments the
count without renewing `when`. Expiry resets the count.

Use whole Hz rounded to the nearest integer. Starting tolerances are 500 Hz
for skimmer CW/RTTY, 3000 Hz for skimmer FT8/FT4/FT2, 1000 Hz for other skimmer
modes, and 2000 Hz for all human modes. The starting change limit is two.
All numeric settings must be positive integers; the CW/RTTY tolerance cannot
be below the configured frequency-averaging tolerance. The bound applies to
frequency-change admissions per entry lifetime, excluding report exceptions
and initial mode admissions.

Use 64 paired shards selected by the historical key hash. The same shard lock
encloses a baseline/candidate comparison or their paired cleanup at one clock
value. One owned cleanup timer is stopped and joined; there are no separately
running child cleanup loops or sequencing queues.

Capture immutable per-policy decision evidence before async spot ownership
handoff. In `on`, live DEDUPE diagnostics show the comparison anchor and mode
identity from that decision. Off/shadow diagnostic bytes remain historical.
Do not change archive encoding or archived-history rendering.

Publish fixed comparison counters through the existing periodic runtime stats
loop and standard logging path. Counters reset per run and identify their run
and reporting interval. Expose baseline/candidate cardinalities separately.
No shared logger implementation, streams, config, sample capture, or sample
persistence changes are authorized by this decision.

## Alternatives considered

1. Keep historical suppression everywhere. It preserves output but continues
   hiding reported frequency/mode changes for the policy window.
2. Use a single approximate shadow cache. Rejected because mode separation,
   report state and expiry diverge between the engines.
3. Serialize the entire policy and its full cleanup under one lock. Paired
   historical-key shards keep comparison order without stopping unrelated
   shards throughout a complete cache sweep.
4. Capture sampled events in new persistent storage. Deferred by the owner;
   v6 uses counters only and leaves existing logging ownership unchanged.

## Consequences

### Benefits

- Reported changes can appear sooner when the candidate engine is enabled.
- Shadow counters measure additions, losses, admission reasons and budget
  pressure while preserving baseline spot output.
- Fixed diagnostic evidence describes the actual decision, even after a
  queued spot's cache entry changes.

### Risks

- Incorrect reports can create false changes and more repeats.
- Report-state renewal can delay a forward that the historical engine would
  admit on expiry; this is measured as a lost forward in shadow.
- Shadow retains both caches and larger, mode-separated candidate entries;
  actual memory, contention and cleanup costs require measurement.
- Counter summaries use existing disposable runtime logging. Missing final
  summaries cannot be recovered after restart. Counters cannot establish
  usefulness or harm without the deferred example-review work.

### Operational impact

Add the complete required subtree to private config before starting the new
binary; keep `state: off` for the initial shipment. Settings apply at startup.
There is no automatic activation and no production-QSY accuracy claim.
When enabled, shared MED changes affect archive/peer admission; any allowed
lane can affect ring history and telnet remains gated by user policy and the
existing output stages. Primary dedupe, frequency averaging, stabilizer policy,
peer content, archive schema and user filters retain their contracts.

## Links

- [Approved scope, traceability and validation status](../qsy-dedupe-v6-validation.md)
- [Operator settings](../../data/config/README.md#secondary-frequency-change-policy)
- Refines the candidate secondary-key policy of
  [ADR-0234](ADR-0234-peer-owned-resources-and-exact-spot-keys.md); baseline and
  primary key behavior remain accepted.
- Extends the admitted contents under
  [ADR-0148](ADR-0148-post-stabilizer-shared-secondary-fanout.md); its final
  stabilized fanout and shared MED ownership remain accepted.
- [ADR-0333](ADR-0333-shared-bounded-runtime-logging.md) remains unchanged;
  its outstanding resource qualification is not completed by this work.
