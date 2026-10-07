# Commands And HELP

This directory is the source of truth for telnet command help. The public `HELP` output is built in [`processor.go`](processor.go), not copied from a separate static document.

## What Lives Here

- command parsing for `HELP`, `DX`, `SHOW`, `BYE`, and related aliases
- per-dialect HELP catalogs
- build, dedupe, and path-glyph HELP notes injected from runtime snapshots
- history, DXCC, propagation-outlook, and build-info read paths used by
  `SHOW DX`, `SHOW MYDX`, `SHOW DXCC`, `SHOW PROP`, and `SHOW BUILD`
- own-call identity display used by `SHOW OWN`

## HELP Source Of Truth

The top-level `HELP` output is assembled by:

- `buildHelpCatalog(...)`
- `filterHelpLines(...)`
- `pathGlyphHelpLines(...)`
- `dedupeHelpNotes(...)`
- `installConfigurationHelp(...)` in [`configuration_help.go`](configuration_help.go)

The main landing page [`../README.md`](../README.md) now includes a generated HELP block for the default `go` dialect. A test in this package checks that the README block still matches the processor output built from the shipped config files.

## Dialects

The runtime supports two command dialects:

- `go`: the default dialect shown in the main README
- `cc`: a DXSpider-style alias set with `SHOW/DX`, `SET/FILTER`, `UNSET/FILTER`, and `SET/ANN`-style shortcuts

Dialect selection is user-visible and persisted per callsign. HELP is rendered for the active dialect, so the command list and examples change with the selected dialect.

## Command Surface

The operator-facing commands handled here are:

- `HELP [topic]`
- `DX`
- `SHOW DX`
- `SHOW MYDX`
- `SHOW DXCC`
- `SHOW PROP`
- `SHOW BUILD`
  shows the startup-resolved UTC `YYMMDD` version, release tag, and Go toolchain
  when available; unstamped release tags are omitted;
  commit and build timestamp remain separate metadata in
  `--version` and PC18, rather than appearing in the version string.
- `SHOW OWN`
- `SHOW FILTER [FULL|category]`
- `SHOW SETTINGS`
- `WHOSPOTSME [band]`
- `PAUSE [seconds]`
- `SHOW HOLD`
- `RESUME`
- `SHOW DEDUPE`
- `SET DEDUPE`
- `SET DIAG`
- `SET GRID`
- `SET NOISE`
- `SET PATHSAMPLES`
- `SET SOLAR`
- `RESET FILTER`
- `SAVE PRESET <name>`
- `LIST PRESET`
- `LOAD PRESET <name>`
- `DELETE PRESET <name>`
- `DIALECT`
- `BYE`
- `GET YAML FILTER|SETTINGS|CONFIG|CAPABILITIES [ID <id>]`
- `PUT YAML FILTER|SETTINGS|CONFIG`
- `PATCH YAML FILTER|SETTINGS|CONFIG`
- `VALIDATE YAML CONFIG`

Filter mutation, named presets, configuration readbacks/uploads, `SHOW PROP`
execution, and read-pause state are handled in the telnet layer.
This package documents those commands in HELP, but the parsers for `PASS`,
`REJECT`, `SHOW FILTER`, `SHOW SETTINGS`, GET/PUT/PATCH/VALIDATE YAML, `SHOW PROP`,
`PAUSE`, `SHOW HOLD`, `RESUME`, and the `cc` aliases live under [`../telnet`](../telnet).

`SHOW FILTER` uses aligned labels and wraps passing finite selections and
useful exclusions by name. BAND, MODE, SOURCE, EVENT, PATH, CONFIDENCE and DX/DE
continents keep their names; disabled choices are not enumerated. Long callsign,
DXCC, grid and zone lists retain counts. It groups geography rules and inclusion
switches. FULL/category show complete effective PASS/REJECT selections using
ALL/NONE, with ON/OFF switches. GET YAML FILTER preserves stored flags, false
entries and defaults. `SHOW/FILTER` and
`SH/FILTER` accept the same FULL/category arguments in `cc`. `SHOW SETTINGS`
separates configured preferences and effective behavior from session status.
Path minimums reflect active runtime state for both stations and beacons.
Both views report the associated preset and whether preferences differ from
its retained reference snapshot.

Human lines contain at most 78 printable ASCII characters followed by CRLF.
Exact strings use quoted ASCII escapes; complete quoted pieces joined by `+`
preserve long values without trimming spaces or splitting escapes. Map keys
use stable ordering and pattern lists retain supplied order. Preview entry
count and rendered length are bounded before sorting or joining selections.

These human readbacks always suppress live spots during preparation, queueing
and delivery, followed by a full reading interval after the server write and
flush. They use the positive configured duration or 30 seconds when it is
zero, ignoring the row threshold even when disabled. Longer existing pauses
are preserved; later valid PAUSE/RESUME controls supersede earlier pending
completion effects. Human size errors follow the same pause policy.

Client YAML commands work in both dialects and never change pause state,
including on success or error. Each reply is one complete framed control
message with no human footer. GET exposes exact writable configuration
separately from read-only status; CAPABILITIES lists schema version, choices,
availability and limits. Optional GET IDs preserve case and accept 1-32 ASCII
letters, digits or hyphens.

PUT requires the complete resource, including all four RuleSet members. PATCH
retains omitted fields and replaces supplied maps/lists as whole collections.
Uploads require schema_version, request_id and configuration; PUT/PATCH also
require the revision from GET as if_revision. GET again after reconnect or a
conflict. Validation or persistence failure leaves live and saved configuration
unchanged; unavailable choices are rejected. Even an unchanged PUT persists
before success without resetting unrelated runtime state. Writes preserve the
preset reference and can set or clear modified. VALIDATE CONFIG checks a
complete proposal without applying or saving it.

Every new readback has a 65,536-byte final response limit, including CRLF,
framing and human footers, with complete output or an explicit error. Upload
bodies use a separate 65,536-byte limit, excluding markers but counting actual
LF/CRLF bytes, and a 30-second deadline from valid-header acceptance.
Malformed upload headers, oversize, expiry and unreliable/incomplete framing
close the connection without dispatching payload remnants. A fully received
invalid document gets a YAML error and keeps the connection open. Ordinary
command headers keep their configured limit (128 bytes in the shipped config).

See the [human examples](../README.md#output-examples),
[client examples](../README.md#client-yaml-configuration), and
[complete framing/schema contract](../telnet/README.md#client-yaml-configuration).

`PAUSE` defaults to 30 seconds and accepts whole seconds from 1 to 300 in both
dialects. It remains available when automatic read pause is disabled. A new
`PAUSE` sets a fresh duration; automatic pauses may extend an active pause but
cannot shorten it. `SHOW HOLD` reports either pause, and `RESUME` ends it without
replaying missed spots. Command replies and other control traffic continue.

`SHOW PROP` HELP should stay aligned with the telnet formatter: the command
shows only rows whose `REL` prediction is `HIGH`, `MEDIUM`, or `LOW`.

## Notes For Documentation

- If HELP text changes here, the main README HELP block must change too.
- Numeric SSIDs on DX calls are stripped at spot construction boundaries; login
  calls remain distinct session identities while `SHOW OWN` and `WHOSPOTSME`
  use the baseline own call.
- Path-glyph notes should match the shipped glyph symbols from `data/config/path_reliability.yaml`.
- Dedupe HELP should match the effective secondary dedupe windows from `data/config`.

For session flow and filter behavior, see [`../telnet/README.md`](../telnet/README.md).
For preset ownership, contents, bounds and failure behavior, see
[`Named Presets`](../telnet/README.md#named-presets). The four commands
and their HELP topics are available in both dialects. Successful LOAD/SAVE
establish the applied/saved reference; failed association persistence after
SAVE retains the previous reference both live and on disk. Protected
temporary-defaults sessions cannot SAVE, LOAD or commit machine writes.
The 20-name, 256 KiB-per-preset and 8 MiB collection limits remain separate
from the readback budget, so a valid large preset may LOAD even when FULL/YAML
readback returns a size error.

### Archive history

`SHOW DX` and `SHOW MYDX` search archived spots using your current filters and
existing self-spot rules. Both accept a count, a selector, or one of each in
either order. The default count is 50; the range is 1-250. `SH DX` and `SH MYDX`
are aliases in both dialects; `SHOW/DX` and `SH/DX` remain `cc`-dialect aliases.
Bare numbers are counts, not ADIF selectors.

```text
SHOW DX K1ABC 10
SHOW MYDX 10 K1ABC
SHOW MYDX 3D2/R 10
```

An exact canonical CTY label takes precedence and selects its whole ADIF entity;
`3D2/R` selects Rotuma. Otherwise a full valid call selects only its normalized
stored DX identity, and other supported prefixes select their CTY entity.
Queries and decoded old records use the same DX normalization, including numeric
SSID removal. An exact call is never widened to its country when no rows match.
A supplied selector requires loaded CTY, but a valid full call with unresolved
country remains a valid exact query. Conflicting canonical labels fail explicitly.
`SHOW DXCC` detail lookup retains its existing behavior.

Every history form applies the page's captured current time minus
`archive.retention_seconds`; the exact cutoff is included. Select newest matching
rows, then show that page chronologically. A page visits at most 200,000 archive
candidates, including malformed/decode-failed records, filter rejects and one
non-consuming older lookahead. At most 199,999 records are consumed. The work
limit reports incomplete search; reaching the requested count also offers a
continuation when older search remains.

Use the exact returned `SHOW DX NEXT H1...` command on the same connection.
`H1...` denotes the supplied H1-plus-32-hex-digit token, not a literal command
argument. Supported DX/MYDX aliases also accept NEXT. Each connection owns one
search; successful page publication rotates its token, so an old token cannot
be reused. Continued pages are marked as older history. Each page uses a fresh
archive view and current cutoff, not a snapshot frozen across commands.

Relevant filter/path-setting changes invalidate continuation, even if restored
to their previous values. Each page captures coherent settings; propagation
observations remain live. New valid searches replace the old search, even if
the new read fails; invalid fresh requests preserve it. A failed continuation
keeps its position retryable unless close, settings changes or a new search
independently invalidated it. Unreadable-record warnings persist across pages;
archive errors and unsafe malformed continuation boundaries are explicit errors.

For queue acceptance, disconnect and publication ownership, see
[telnet history](../telnet/README.md#archive-history-and-continuation). The reader
uses existing timestamp keys; no callsign index, backfill or migration is needed.
See [ADR-0251](../docs/decisions/ADR-0251-exact-call-paged-history.md) and
[TSR-0041](../docs/troubleshooting/TSR-0041-exact-call-history-and-scan-cap.md).

### Canonical DXCC input and display

History selection is described above; canonical labels retain precedence over
exact calls and prefixes.

Telnet `PASS`/`REJECT DXDXCC|DEDXCC` accept canonical CTY prefixes or existing
positive ADIF numbers. All human `SHOW FILTER` views display every unambiguous
canonical prefix for each selected entity; FULL/category show effective
PASS/REJECT selections. Conflicting labels are omitted; valid alternatives
remain.
Long overview counts count entities. Entries with no usable labels show
`Unknown DXCC (12345)`; machine YAML remains numeric. See
[canonical DXCC prefixes](../telnet/README.md#canonical-dxcc-prefixes).
