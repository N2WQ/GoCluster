# ADR-0245: Machine YAML Configuration

- Status: Accepted
- Date: 2026-10-05
- Decision Origin: Design

## Context

Clients need exact machine-readable filter rules and preferences, plus safe
configuration writes. Human formatting can hide explicit false entries, empty
defaults, and configured/effective distinctions. GRID and NEARBY also need a
combined update to avoid an intermediate inconsistent configuration.

The ordinary command reader uppercases human input and limits headers to 128
bytes. Reusing it for YAML bodies would lose case and reject YAML punctuation.
Disk commits, reconnect ownership, and runtime publication require the exact
configuration persistence contract in
[ADR-0244](ADR-0244-exact-configuration-persistence.md).

## Decision

### Commands And Version 1 Documents

Use canonical `GET YAML FILTER`, `GET YAML SETTINGS`, `GET YAML CONFIG`, and
`GET YAML CAPABILITIES`. Use `PUT YAML FILTER|SETTINGS|CONFIG` for complete
replacement, `PATCH YAML FILTER|SETTINGS|CONFIG` for supplied-field updates, and
`VALIDATE YAML CONFIG` for complete validation without application or saving.
The commands are available in both dialects.

GET may supply `ID <request_id>`; otherwise the server assigns an identifier.
Identifiers are 1-32 ASCII letters, digits, or hyphens and preserve case.
Header verbs/resources are case-insensitive. Body bytes and scalar case are
preserved after Telnet negotiation decoding.

Success readbacks contain `schema_version: 1`, `request_id`, `resource`,
`revision`, `configuration`, and `status`. Resource layout is:

| Resource | `configuration` contents |
| --- | --- |
| FILTER | Exact filter fields directly. |
| SETTINGS | Six writable settings directly. |
| CONFIG | `filters` and `settings` together. |
| CAPABILITIES | Supported commands/schema, field names, available value choices, and limits. |

The exact filter schema has 14 `allow_all`/`block_all`/`allow`/`block` rule
families, four ordered callsign-pattern lists, six boolean-or-`DEFAULT`
toggles, and `nearby_enabled`. Stored false map entries remain present. Empty
maps/lists are ordinary collections. Explicit false, zero, empty default
strings, and inherited toggle selections survive GET/edit/PUT. The six
settings are dialect, grid, noise class, dedupe policy, path observation
minimum, and solar summary minutes.

Read-only status separates configured settings, effective behavior, session
controls, server defaults, and preset association/modification. Clients cannot
write these status fields or replace the preset reference through YAML. The
baseline snapshot is never emitted. Exact snake_case fields and per-server
choices are published in CAPABILITIES and the transport guide.

Uploads require `schema_version`, `request_id`, `configuration`, and, for
PUT/PATCH, `if_revision`. VALIDATE does not require a matching revision. PUT
requires every writable field of the selected resource; omissions are errors.
PATCH preserves omitted fields, including omitted rule-set members. Supplied
maps and lists replace their corresponding complete collections.

Require one ordinary YAML document. Reject anchors, aliases, merge keys,
custom tags, null values, unknown/read-only fields, and duplicates after key
interpretation. Validation reports unsupported values without normalization,
pruning, or substitution. Recognized unavailable choices are errors. Nonempty
machine GRID values have length four or six and require usable mapping; NEARBY
requires a usable candidate GRID and H3 tables. Callsign patterns use ASCII letters,
digits, slash, and hyphen, with at most one leading or trailing `*`.

### Transactions, Revisions, And Admission

GET captures one configuration under the full-callsign transaction stripe.
PUT/PATCH require the opaque revision from GET. Configuration changes advance
the revision; callsign pattern order is ignored while multiplicity is retained.
Pause, diagnostics, preset status, and login timestamps do not advance it. An
unchanged write keeps its revision. Reconnect/restart creates fresh session
revision authority, so clients GET again before writing.

Under the same stripe, prepare a detached resulting configuration, validate
it, prepare dependent runtime state, check readback admission, and generate
the bounded success acknowledgement before committing. Atomically persist the
exact configuration and unchanged preset reference, then publish live state.
Validation or persistence failure leaves both prior live and durable state
intact. No normal fallible preparation remains after commit. Queue/socket
failure can lose an acknowledgement after a successful commit; GET resolves
that outcome.

Unchanged PUT/PATCH still persist before success. This repairs durable state
when an earlier human command changed live preferences but its save failed,
without resetting solar scheduling, NEARBY restoration, diagnostics, or pause
state. Exactly identical writes repair disk without publishing runtime state.
Pattern order is included in that identity check despite being ignored for
revisions. Successful write acknowledgements report `valid: true`, `applied: true`,
and `persisted: true`; validation reports true/false/false. Protected
temporary-defaults sessions reject PUT/PATCH and keep validation available.

Admit machine writes and VALIDATE only when their resulting complete CONFIG
readback fits 65,536 final bytes, with reserved maximum request identifier,
128-byte opaque revision representation, preset name, counters, status, and
actual candidate/lookup GRID width. Preflight precedes unrestricted cloning,
sorting, and encoding. The final destination enforces converted CRLF bytes,
markers, and metadata. This admission also applies to small PATCH requests.
A reducing PATCH or complete PUT can replace an oversized human configuration.
LOAD retains its independent 256 KiB preset budget; it checks acknowledgement
fit and can succeed when detailed readback is too large.

### Transport, Reception, And Replies

Header parsing stays bounded at 128 bytes. After accepting a valid upload
header, receive standalone `---` and `...` lines, retaining at most 65,536
body bytes including received LF/CRLF endings. Markers are outside that body
budget. Use an absolute 30-second upload deadline starting at header
acceptance. Check it during reads and completed-frame acceptance, independently
of timer scheduling, including already buffered input.

Each active reception owns one watchdog. Cleanup stops it or joins an already
running callback before returning; a retired callback cannot close a later
reception. Hard done/socket interruption occurs independently of optional
reporting. Oversized bodies, deadline expiry, incomplete/unreliable framing,
and early ingress failures with recognized machine intent are terminal.
Rejected PUT/PATCH/VALIDATE headers close the connection without dispatching
any remaining payload. A complete invalid GET header is recoverable. A fully
received invalid YAML document is recoverable through a framed error. All
failure paths preserve configuration.
Terminal rejection prevents final preference autosave, including unsaved live
changes from an earlier failed human command. Ownership fencing and membership
cleanup still run; ordinary disconnect autosave is unchanged.

Strict YAML decoding holds one of four cancellable global preparation permits
while its `yaml.Node` tree remains reachable. Extract detached values and
presence masks, then release the permit before waiting for the stripe or disk.
Readback generation has independent preflight and destination bounds.

Complete generation/validation before queueing success. Enqueue each framed
reply as one control message, with standalone `---`/`...`, CRLF endings, and
at most 65,536 final bytes. Live spots and other messages stay outside that
document. Size errors are explicit and structured, never truncated successes.
Broken connections cannot guarantee complete delivery.

GET, PUT, PATCH, VALIDATE, CAPABILITIES, and all machine errors have no pause
effects or human pause footer. They do not reset counters. Live traffic
suppressed by a pre-existing pause may naturally increase its counter.

## Alternatives considered

1. Keep `SHOW ... YAML` as the canonical machine API. GET/PUT/PATCH provide
   clear read/replacement/update meanings and a shared resource vocabulary.
2. Support replacement only. PATCH is also selected for practical small edits;
   omission and collection replacement rules remain explicit.
3. Allow partial application, unavailable-choice fallback, or stale writes.
   Rejected because clients could report success for an unintended result or
   overwrite concurrent configuration.
4. Accept 256 KiB uploads or truncate large readbacks. The selected initial
   bound is 64 KiB, with complete-or-error output and no pagination.
5. Permit advanced YAML expansion or recover a rejected payload as commands.
   Rejected because resource use and command boundaries become less dependable.

## Consequences

### Benefits

- Clients can round-trip exact preferences and change related settings together.
- Revisions, atomic persistence, and session ownership prevent stale writes.
- Limits constrain preparation, retained input, and queued response payloads.
- Capability discovery identifies disabled choices before clients submit them.

### Risks

- Some valid human/preset configurations exceed complete YAML readback limits.
- A small PATCH can fail because the resulting configuration is too large.
- Framing failures close the connection and require a new GET after reconnect.
- A lost acknowledgement leaves commit outcome uncertain until readback.
- Payload bounds are not whole-process memory or throughput guarantees.

### Operational impact

Use GET before edits, submit only writable configuration, and GET again after
conflicts or reconnect. Repair protected saved records instead of overwriting
them with temporary defaults. Use human category readbacks to inspect a large
configuration. No new runtime knob, authentication policy, or named-preset
overwrite command is introduced. Test coverage establishes contract behavior;
it does not establish production latency or p99 improvements.

## Links

- Related issues/PRs/commits: -
- Implementation: [machine commands](../../telnet/machine_commands.go),
  [header reader](../../telnet/machine_input.go),
  [upload receiver](../../telnet/machine_upload.go),
  [strict schema](../../telnet/machine_schema.go),
  [validation](../../telnet/machine_validation.go),
  [readback admission](../../telnet/configuration_readback.go)
- Related tests: [machine writes](../../telnet/machine_commands_test.go),
  [headers](../../telnet/machine_input_test.go),
  [framing/deadlines](../../telnet/machine_upload_test.go),
  [blocked-reporting cleanup](../../telnet/machine_deadline_lifecycle_test.go),
  [native/ziutek sessions](../../telnet/machine_session_test.go),
  [echo/trickle/blocked negotiation](../../telnet/machine_transport_coverage_test.go),
  [terminal rejection disk/reconnect](../../telnet/machine_failure_persistence_test.go),
  [schema](../../telnet/machine_schema_test.go),
  [literal output/limits](../../telnet/configuration_readback_test.go)
- Related docs: [operator guide](../OPERATOR_GUIDE.md),
  [transport guide](../../telnet/README.md),
  [ADR-0244](ADR-0244-exact-configuration-persistence.md),
  [ADR-0246](ADR-0246-delivery-timed-human-readbacks.md)
- Related TSRs: -
- Supersedes / superseded by: No previous machine-write protocol decision.

- State metadata and related schema version rules are refined by
  [ADR-0253](ADR-0253-fcc-state-enrichment-and-filtering.md).
