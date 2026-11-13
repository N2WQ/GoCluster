# ADR-0349: Human Explicit Reports And Rejection Diagnostics

- Status: Accepted
- Date: 2026-11-12
- Decision Origin: Troubleshooting chat
- Supersedes: ADR-0348 explicit-dB compatibility clause and no-retained-state clause only as described below.

## Context

The v1 human-comment repair deliberately retained legacy explicit-dB handling.
That left `UP 2 -12 dB` reporting 2 and deleting the split instruction, while
`S9 +10 dB` and `59+20dB` could become report metadata. The substring fallback
also accepted embedded text and bypassed the ordinary integer bound.
Human telnet rejection without a usable final wire time had no reason-specific
counter or sample log. The owner selected both bounded follow-up slices and
authorized Scope Ledger v2 for unattended execution in the existing checkout.

## Decision

### Human explicit reports

Recognize a whole ASCII integer and case-insensitive `dB`, either compact or
as immediately adjacent source tokens, within inclusive -200 through +200.
Do not search backward across intervening tokens or extract numeric suffixes
from embedded text. Preserve the supported `HHMMZ`-prefixed report forms and
their original time text. The first valid explicit report in source order wins
before the unchanged signed FT8/FT4 shorthand rule (-30 through +30).

Readings compactly attached to S9/59 remain text. A standalone preceding `S9`
or `59` marker also excludes the immediately following explicit candidate.
This is a local syntax rule, not a general natural-language strength classifier.
Preserve rejected candidates and orphan units. Remove only the selected report
number and unit; mode/event recognition, actual keyword precedence and existing
text normalization remain unchanged. A token actually selected as a mode cannot
also supply the report. Configured aliases retain their existing ownership.

Examples:

| Input | Report | Remaining comment |
| --- | --- | --- |
| `UP 2 -12 dB` | -12 | `UP 2` |
| `20 1200Z -12 dB` | -12 | `20 1200Z` |
| `S9 +10 dB` | absent | `S9 +10 dB` |
| `59+20dB` | absent | `59+20dB` |
| `FT8 -14 label=+31dB` | -14 | `label=+31dB` |
| `1200Z-14dB` | -14 | `1200Z` |
| `+200dB` | 200 | empty |
| `+201dB` or `1000dB` | absent | original text |

All existing human callers use this one result for display, SNR filters,
archive storage and existing learning checks. Do not change legacy RBN parser
entry points or display traces, learning eligibility/algorithms, archive rows,
schemas, runtime configuration values, CWT policy or other alias rules.

### Human wire-time diagnostics

At the existing human terminal-time rejection gate, increment one client-owned
counter independently of queue-overflow drops and logging. Use the existing
rate-limited counter: attempt the first sample immediately, then at most one
per 30 seconds per client. Include source, `unusable_wire_time`, total and a
quoted bounded sample. Mark truncation and retain head/tail evidence, including
the end of the wire line. Bound the complete escaped message below the existing
2048-byte logger record limit. Format only permitted samples and retain no raw
line, new worker, queue or registry.

Expose the total in client health snapshots and existing health-transition log
formatting as `wire_time`. The count survives reconnect generations of the same
client and resets with a replacement client/process. It covers this gate only,
not every malformed upstream line. Logging is an admission attempt: bounded
logging may discard records, while the counter still advances. Existing idle
classification and health-transition cadence are unchanged.

The approved rejection grammar, terminal-prose ambiguity, UTC/date/skew
conversion and source timestamp ownership remain those of ADR-0348. The added
fixed-size counter is the only new client-lifetime state.

### Measurement limits

A separate non-steered review received the measurement question without the
preferred design, intended changes or parser-derived expected values. Primary
sources establish that WSJT reports use SNR relative to 2500 Hz noise bandwidth,
whereas S9 over-strength readings use an S9 signal-level reference. They are
not interchangeable without an appropriate noise measurement. For a known
WSJT measurement, 0 dB means power ratio 1 and +10 dB means ratio 10. Under
the calibrated HF S9 convention, S9+20 means -53 dBm signal level; SNR remains
unknown. These are reference meanings, not calibration claims about free text.

The numeric limits and accepted grammar are owner-selected product guardrails.
They do not establish decoder provenance, receive direction, calibration,
measurement attribution or improved prediction accuracy.

## Alternatives considered

1. Preserve all explicit-dB behavior: retains wrong number binding and invented
   reports. The owner selected the stricter human grammar.
2. Keep embedded numeric extraction and remove only its substring: retains
   ambiguity about what the number describes. The owner chose to keep it text.
3. Share the repair with RBN/skimmer callers: violates the approved compatibility
   boundary. Keep human report acceptance separate from legacy recognition.
4. Move rejection accounting into the runtime through a new callback: adds
   wiring and ownership when the client already owns the rejection and lifetime.
   Reuse its health snapshot and the existing bounded log route instead.

## Consequences

Operators can identify final-time incompatibilities without unbounded logs.
Split information and strength readings remain searchable text. The deliberate
compatibility cost is losing report metadata from embedded or out-of-range
forms. Existing stored rows are not rewritten, and log samples cannot replace
a complete upstream capture. No production incidence or model-accuracy claim
is made by local validation.

## Links

- [ADR-0348](ADR-0348-human-spot-comment-integrity.md) (partially superseded).
- [TSR-0132](../troubleshooting/TSR-0132-human-comment-report-and-time-parsing.md).
- [Human comments](../../spot/README.md#human-spot-comments).
- [Wire-time framing](../../rbn/README.md#human-telnet-wire-time).
- [WSJT-X reporting convention](https://wsjtx.github.io/wsjtx/guide-full.html#_standard_exchange).
- [IARU Region 1 S-meter standard, section 4.1.3](https://www.iaru-r1.org/wp-content/uploads/2019/08/hf_managers_handbook_v9.pdf).
