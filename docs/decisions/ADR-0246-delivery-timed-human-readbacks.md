# ADR-0246: Delivery-timed Human Configuration Readbacks

- Status: Accepted
- Date: 2026-10-05
- Decision Origin: Design

## Context

Users need clear current filter/preference output and a full interval to read
it. Row-triggered pauses can be disabled or expire while a response is waiting
in the queue. Human readbacks must suppress scrolling through preparation and
delivery, then begin the reading interval at successful server delivery.
Machine YAML commands need uninterrupted pause-state semantics.
Counts alone do not explain ordinary selections well. The human presentation
must follow the readable operator examples while exact views preserve every
stored value and explain category-specific matching behavior.

[ADR-0195](ADR-0195-telnet-auto-read-pause.md) established automatic pauses;
[ADR-0243](ADR-0243-manual-telnet-spot-pause.md) added manual controls. Their
command-owned deadline mutation is insufficient when writer completion also
changes the deadline.

## Decision

### Human Views And The Output Budget

Provide `SHOW FILTER` as an aligned, readable overview, `SHOW FILTER FULL` as
exact complete rules, `SHOW FILTER <category>` as exact category rules, and
`SHOW SETTINGS`. Keep CC `SHOW/FILTER` and `SH/FILTER` aliases and category
aliases CONF and PC93. Refine the initial count-based overview to show actual
short selections, with clear counts when selections cannot fit. Group DX/DE
geography and inclusion switches. Bound preview entry count and rendered
length before collecting, sorting or joining; counts do not construct all
detail strings. Use stable ordering for map selections.

Summaries reflect actual category behavior rather than a generic allow-all
interpretation. Ordinary string/integer categories can remain restrictive
with a nonempty allow map even when allow_all is true. EVENT uses key presence,
including false entries, ignores allow restrictions when allow_all is true,
and always includes untagged spots. PATH preserves its UNLIKELY/CLOSED rules.
Enabled NEARBY suspends geography rules even when user cells are unavailable;
the human view explains rejection on affected bands and retained rules.

FULL/category show every exact flag, false entry, ordered pattern and explicit
toggle default. Human lines are at most 78 printable ASCII characters followed
by CRLF. Quote exact strings with Go-style ASCII escapes; wrap long individual
values as complete quoted pieces joined by + without adding characters or
trimming whitespace. Never split an escape. Map keys sort lexicographically
or numerically; pattern order and duplicates remain intact. Preflight escaped
length before unrestricted quoting/sorting, and bound quoted-piece scratch
space by the line width. The human exact format does not change client YAML.

SETTINGS separates configured preferences and effective behavior from session
controls. Read effective station/beacon path minimums from active runtime
state and loaded predictor configuration. For server floors 21/11, reconnect
can retain a saved personal minimum of 15 without activating it; show the
configured 15 and effective 21/11 rather than deriving a beacon minimum of 15
from saved preferences. Explain disabled/unavailable prediction explicitly.
Pending delivery is a reading hold, not a finite countdown already underway.
Both views report preset association and `(modified)` using the retained reference in
[ADR-0244](ADR-0244-exact-configuration-persistence.md).

Every new human or YAML readback is complete within a 65,536-byte final
response limit, or returns an explicit error. Count CRLF conversion, headers,
document markers, metadata, and human footers. Preflight before unrestricted
clone/sort/serialization and enforce the final converted destination budget.
Generate the complete response before enqueueing it as one control message.
A successful detail view contains every value; an oversized response never
silently omits rules. A human size error follows the same pause policy.

Keep LOAD admission separate: a valid preset up to 256 KiB can load while
FULL/YAML inspection fails its smaller readback budget. Machine write/VALIDATE
admission reserves a complete CONFIG readback under
[ADR-0245](ADR-0245-machine-yaml-configuration.md).

### Acceptance, Delivery, And Ordering

Every human filter/settings readback begins suppression at acceptance, before
the transaction-stripe wait, preparation, queueing, or delivery. Ignore the
automatic row threshold, including zero. Use the configured positive duration;
use 30 seconds if that duration is zero. Other generic automatic responses
retain their existing row threshold and either-zero-disables contract.

A pending delivery hold has separate fixed state from finite pause deadlines
and stale-envelope cutoffs. It does not install an infinite synthetic cutoff.
After the writer successfully writes and flushes the response's batch, start
the full reading interval from that completion time. Preserve a longer active
finite pause and carry all suppression counts from pending delivery into the
reading interval, including when an older finite deadline expired meanwhile.
Successful delivery measures the server write/flush; terminal rendering time
is unknown.

Fixed completion metadata carries a pause epoch and duration in the queued
control message. The writer applies the latest eligible completion only after
successful write/flush, including mixed batches. Failed batches have no
completion effect. Every new human configuration readback establishes a fresh
epoch. A later valid processed PAUSE or RESUME advances the epoch and cancels
earlier pending completion authority. Invalid controls have no pause effects. Close or session
replacement invalidates pending completion authority.

Manual controls, generic automatic extension, human acceptance/completion, and
closure share one per-client mutation mutex. Generic automatic extension
computes a maximum within that authority, cannot shorten a pause, and does not
cancel an existing pending hold. Manual PAUSE retains its fresh-duration
replacement behavior. RESUME retains immediate resume, counter reporting/reset,
and stale-envelope discard. Reset counters for a newly inactive/nonpending
pause only; preserve them across continuous suppression.

Spot admission/consumption checks remain atomic and allocation-free. No timer,
worker, replay queue, callback registry, or server-lifetime per-request state
is added for readbacks. Output already prepared for a socket write cannot be
recalled by a subsequent pause. Suppressed spots are discarded without replay
and remain separate from slow-client drops. Other control traffic continues.

### Human And Machine Replies

Human replies, including human size errors, include the delivery/reading
footer and RESUME guidance:

```text
Live spots paused during delivery and for at least 30s afterward.
Type RESUME when ready. Missed spots are not replayed.
```

SHOW HOLD reports pending delivery as active even when no finite interval
remains. A later manual control supersedes the pending reading interval.

Every machine command and its success/error responses use framed YAML, have
no human footer, and do not start, extend, cancel, reset, or otherwise alter
pause state or suppression counters. Ordinary live traffic under an existing
pause can still increase those counters independently of the YAML request.

## Alternatives considered

1. Keep row-threshold pauses for configuration readbacks. Rejected because
   short replies and zero settings would bypass the selected reading pause.
2. Start a finite pause only at enqueue. Rejected because preparation, queueing,
   and delivery can consume the user's reading interval.
3. Let every pending completion restart a pause after RESUME. Rejected because
   later processed manual controls must take precedence.
4. Store an infinite deadline, use timers/callbacks, or reset the count at
   completion. Separate pending state and fixed epochs preserve finite cutoff
   semantics, bounded resources, and continuous suppression counts.
5. Truncate or paginate oversized output. Initial implementation selects a
   complete-or-error limit; pagination is outside this feature.
6. Keep counts and raw flags as the compact human overview. Readable short
   selections and grouped rows better explain current behavior; exact views
   still retain raw flags and all stored values.
7. Word-wrap raw values or display literal Unicode. ASCII quoted pieces provide
   predictable terminal width and lossless values, including whitespace.

## Consequences

### Benefits

- Readback output stays readable through preparation and delivery.
- Users receive a full reading interval and retain explicit manual control.
- Readable selections and exact output expose current configuration honestly.
- YAML clients receive stable framed documents without pause side effects.

### Risks

- Live spots suppressed during pending delivery/reading are missed by design.
- A queued reply can hold suppression longer than its eventual reading interval.
- Broken connections can prevent complete delivery; terminal rendering is unknown.
- Large valid configurations can require individual category inspection.
- Benchmarks establish allocation behavior for measured paths, not production
  throughput, p99 latency, or performance improvement.

### Operational impact

These human readbacks pause even when generic automatic pauses are disabled.
Use SHOW HOLD to distinguish pending delivery from a finite countdown and
RESUME to resume immediately. Check size errors rather than interpreting an
incomplete document as success. No new configuration knob or persistent pause
preference is introduced.

## Links

- Related issues/PRs/commits: -
- Implementation: [pause authority](../../telnet/readback_pause.go),
  [human handler/status](../../telnet/configuration_readback.go),
  [bounded rendering](../../telnet/configuration_render.go),
  [ASCII exact formatting](../../telnet/configuration_human.go),
  [matcher-specific summaries](../../telnet/configuration_human_summary.go),
  [writer integration](../../telnet/server.go)
- Related tests: [pause ordering/counts](../../telnet/readback_pause_test.go),
  [literal output/limits](../../telnet/configuration_readback_test.go),
  [approved human examples and exact values](../../telnet/configuration_human_test.go),
  [restored rule semantics](../../telnet/configuration_human_summary_test.go),
  [mixed and failed delivery batches](../../telnet/readback_writer_test.go),
  [existing pause controls](../../telnet/read_pause_command_test.go),
  [existing writer pause checks](../../telnet/writer_v15_test.go)
- Related docs: [operator guide](../OPERATOR_GUIDE.md),
  [transport guide](../../telnet/README.md),
  [ADR-0244](ADR-0244-exact-configuration-persistence.md),
  [ADR-0245](ADR-0245-machine-yaml-configuration.md)
- Related TSRs: -
- Supersedes / superseded by: Refines
  [ADR-0195](ADR-0195-telnet-auto-read-pause.md) for these human readbacks and
  shared pause synchronization, and
  [ADR-0243](ADR-0243-manual-telnet-spot-pause.md) for writer-owned completion
  and command precedence. Other automatic/manual contracts remain accepted.
