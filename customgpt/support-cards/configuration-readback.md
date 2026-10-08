# Support Card: Configuration Readbacks And Client YAML

## Match

Use for human SHOW FILTER/FULL/category or SHOW SETTINGS output, canonical
GET/PUT/PATCH/VALIDATE YAML configuration commands, revision conflicts, upload
errors, protected temporary records, preset modification status or partial SAVE.
If `getSupportRoute` is not decisive, follow source-map or troubleshooting links
and retrieve this card with `getDoc`; it is not a new automatic Worker route.

## First Safe Check

Identify whether this is a human readback, a machine upload/readback, a preset
operation or a login warning. Retrieve the relevant section of `telnet/README.md`
before interpreting the exact command and response. Ask for one redacted
command/error artifact that distinguishes the next step.

## Must Include

- DXDXCC/DEDXCC PASS/REJECT accept exact canonical CTY prefixes and existing
  positive numeric ADIF codes, including mixed lists. Canonical input requires
  CTY; unknown or conflicting labels reject the whole list without mutation.
  IT9, I and IG9 select the same ADIF entity, not separate country subdivisions.
  All human SHOW FILTER views use unambiguous canonical prefixes; FULL/category
  show effective PASS/REJECT selections, including all labels per entity.
  Conflicting labels are omitted; valid alternatives remain. Long overview counts
  count entities. With no usable label, rules show `Unknown DXCC (12345)`;
  each entity retains its own number in this fallback. YAML stays numeric.
  SHOW DX/MYDX recognize canonical labels before portable callsign processing;
  numeric history arguments remain counts. SHOW DXCC details are unchanged.

- Human SHOW FILTER uses aligned labels and wraps all passing finite selections
  by name: BAND, MODE, SOURCE, EVENT, PATH, CONFIDENCE, DX/DE continents and states.
  Explain All, None and useful explicit exclusions, including All except.
  Do not enumerate disabled choices or replace finite names with counts.
  Long callsign, DXCC, grid and zone lists retain counts, with grouped geography
  and inclusion switches. Unknown-mode, untagged-event, confidence-exemption,
  PATH legacy and NEARBY qualifications still apply.
  FULL/category shows complete effective PASS/REJECT selections using ALL/NONE.
  ALL means unrestricted before listed REJECT entries. Ordinary false entries
  and rejected PASS entries are omitted; a false-only allow map gives PASS: NONE.
  Switches show ON/OFF. Callsign patterns retain order with REJECT precedence.
  PATH includes inherited CLOSED behavior; EVENT false keys remain active.
  NEARBY suspends geography even when unavailable. Use GET YAML FILTER for
  schema 1 flags, false entries and defaults; use SCHEMA 2 for state rules. SHOW SETTINGS separates
  configured
  selections and effective choices from session status; both show the preset.
- Every human line is at most 78 printable ASCII characters followed by CRLF.
  Exact strings use quoted ASCII escapes. Complete quoted pieces joined by +
  preserve long values without trimming whitespace or splitting escapes.
  Indentation, line endings and the + marker are not stored characters. Map
  keys use stable ordering; callsign patterns retain supplied order. Compact
  preview count and rendered length are bounded before sorting or joining.
  Finite selections preflight aggregate escaped size before key collection;
  every complete wrapped response must fit 65,536 bytes. An unusually large
  retained finite selection can return an explicit size error.
- Effective path minimums come from restored runtime state and loaded server
  configuration. With station/beacon floors 21/11, reconnect leaves a saved
  personal minimum of 15 inactive, so effective values remain 21/11. An active
  personal minimum of 30 gives 30/30. Explain disabled/unavailable prediction.
- NEARBY distinguishes enabled from usable. Enabled NEARBY suspends ordinary
  location rules even with unavailable cells; spots on affected bands fail
  NEARBY matching instead of falling back to ordinary location rules.
  Usable grids remain bare when short and simple, as in `On; grid FN31PR`;
  otherwise, quoted ASCII pieces preserve the exact retained grid bytes.
  Escaping an unusual saved grid is not itself a response-size failure;
  the complete rendered response still has to fit its byte budget.
- Ordinary string/integer categories can remain restrictive with a nonempty
  allow map even when allow_all is true. EVENT uses key presence, including
  entries stored as false, ignores allow-list restrictions when allow_all is
  true, and always includes untagged spots. PATH retains its UNLIKELY/CLOSED
  relationship. To remove an EVENT rule through YAML, omit its key from the
  replacement map rather than setting it to false.
- Human readbacks, including size errors, suppress spots before preparation,
  through queueing/delivery and for the full interval after successful write
  and flush. They ignore zero row thresholds and use 30 seconds for a zero
  duration. Longer pauses remain; later processed PAUSE/RESUME takes precedence.
  Pending delivery is not an already-running reading countdown; the SETTINGS
  snapshot describes the full interval that follows server delivery.
- Canonical machine reads are GET YAML FILTER/SETTINGS/CONFIG/CAPABILITIES.
  Machine success/errors have no pause-state effects or human footer. Existing
  suppressed counts may still increase from live traffic during a prior pause.
- GET separates exact writable configuration from read-only status. Preserve
  explicit false, zero, empty values/collections and DEFAULT selections. GET IDs
  are 1-32 ASCII letters/digits/hyphens and preserve case; CAPABILITIES is read-only.
- PUT requires a complete FILTER, SETTINGS or CONFIG resource. PATCH retains
  omissions; supplied maps/lists replace their whole collections. VALIDATE YAML
  CONFIG checks a complete proposal without applying or saving it.
- PUT/PATCH use GET's matching revision as if_revision. GET again after reconnect,
  restart or conflict. Validation/persistence failure leaves live and saved
  configuration unchanged; unavailable choices are rejected. Even an unchanged
  PUT persists before success, repairing failed human autosave without resetting
  solar scheduling, NEARBY restoration, diagnostics or pause.
- Every new readback's 65,536-byte final budget includes CRLF, markers and human
  footers. Complete output or an explicit error replaces silent truncation.
  Machine proposals must fit a complete CONFIG readback with reserved metadata;
  valid larger presets may still LOAD under the separate 256 KiB preset limit.
- Uploads use standalone ---/... lines, 65,536 actual body bytes excluding
  markers, and a 30-second absolute deadline from valid-header acceptance.
  Malformed upload headers and oversized/expired/incomplete/unreliable framing
  close the connection without dispatching remaining bytes. Fully received
  invalid documents return framed YAML errors and leave the connection open.
  Terminal rejection also skips final preference autosave, preserving the disk
  record even when an earlier human command changed live state but failed to save.
- Unreadable/unsupported user records are preserved. Protected temporary-defaults
  sessions allow readbacks and temporary human changes but reject SAVE before
  any library write, LOAD and PUT/PATCH. A login timestamp/IP save warning after
  successful restoration instead continues with the restored preferences.
- Preset modified compares current preferences with the retained applied/saved
  reference for the full callsign/SSID. Reversing changes clears it; PAUSE,
  diagnostics and temporary NEARBY dedupe do not affect it. Library overwrite or
  deletion does not change the reference. Partial SAVE retains the previous
  association/reference live and on disk, including subsequent ordinary saves.

## Must Avoid

- Do not interpret REJECT DXDXCC IT9 as Sicily-only or a callsign-prefix block.
  Do not suggest W6/K1ABC as canonical filter labels or a numeric history ADIF
  selector. Do not replace machine YAML's numeric keys with human label groups.

- Do not invent SHOW YAML aliases, pagination, a force-revision command or an
  operator setting for raising these limits.
- Do not upload read-only status or omit required PUT fields to mean defaults.
- Do not silently substitute an unavailable choice, promise memory-only success
  or skip persistence because a PUT matches the current live configuration.
- Do not claim a malformed upload tail can become ordinary commands, or confuse
  the ordinary 128-byte shipped command-header limit with the upload-body limit.
- Do not reset/delete a protected record or overwrite a named preset with
  temporary defaults as a first troubleshooting step.
- Do not treat a saved library snapshot as proof that its new SSID association
  persisted, or treat all SSIDs as one session configuration.
- Do not interpret false EVENT entries as disabled, infer unrestricted ordinary
  rules from allow_all alone, or calculate effective path minimums from a saved
  preference that reconnect did not activate.
- Do not strip quoted spaces, join wrapped pieces with a separator, or treat
  the human effective format as the client YAML schema.

## US And Canadian State And Version Compatibility

DXSTATE/DESTATE accept 60 US mailing codes and 13 Canadian province/territory codes. Unknown state passes
unrestricted and REJECT-only rules, and fails an explicit PASS list. NEARBY
suspends/locks/restores state rules. These values can differ from operating
location; do not infer them from a portable prefix, grid or callsign district.
Enrichment remains active when `fcc_uls.enabled` is false: that flag disables
license rejection only. Missing/unavailable state stays empty.
FCC extraction/rebuilding and ISED publication make their own cached calls unavailable: named state PASS
filters exclude those spots, named REJECT filters admit them, and archived empty
states remain empty after refresh. See the
[State filter documentation](../../telnet/README.md#us-state-and-canadian-province-filters)
for the refresh interruption and its measured local duration.

Default GET remains schema 1 and does not expose state fields. Request
`GET YAML CONFIG SCHEMA 2` or `GET YAML FILTER SCHEMA 2` for `dx_states` and
`de_states`; body schema_version selects upload vocabulary. Schema 1 writes
preserve hidden state and state-only edits still advance the shared revision.
Capabilities are versioned; schema 1 advertises supported versions without new
choice-object fields. A near-limit schema 1 configuration can produce an explicit
schema 2 size error; reduce ordinary rules using schema 1 rather than truncating
or discarding hidden fields.

New saved records use version 3; legacy/version 1 initializes new states
as unrestricted, version 2 states remain exact, and older versions acquire no
MINSNR thresholds, including nested preset baselines. Protected malformed/future
records must not be rewritten. New archive version 6 stores observed state;
versions 2–5 have unknown state, with no current-FCC history hydration. Explain
state PASS exclusions of older rows. Downgrades require matching binary/data
backups; failed imports instead retain the last good FCC database.

## Sources

- [telnet/README.md human readbacks](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#human-configuration-readbacks)
- [telnet/README.md client schema and framing](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#client-yaml-configuration)
- [telnet/README.md named presets and persistence](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#named-presets)
- [README.md output examples](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md#output-examples)
- [commands/README.md HELP ownership](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md)
- [data/config/README.md effective node YAML](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md)

Canadian licensing uses the required `ised` configuration block and independent
paired ISED snapshot. `ised.enabled: false` keeps downloads and enrichment on.
The same DXSTATE/DESTATE fields accept all 13 province/territory codes alongside
the 60 FCC codes. Base-call CTY selects source, including foreign portable calls.
Events use inclusive UTC dates; a prefix match means callsign plausibility,
without proving eligibility. History uses stored State. Layout versions stay
archive 6, saved preferences 3 and machine YAML 1/2/3; older binaries may reject
Canadian codes, so downgrade with matching backups. Route source/failure details
to [ADR-0254](../../docs/decisions/ADR-0254-canadian-ised-license-and-state-reuse.md)
and [configuration](../../data/config/README.md#canadian-ised-reference-data).

## MINSNR

Numeric `PASS MINSNR CW,RTTY 10` and `REJECT MINSNR FT8,FT4 -10` both set an
inclusive minimum per selected mode: automated reports must be at least that
value. Zero and negative integers enable thresholds. Human spots and spots
without SNR bypass MINSNR only; other filters still apply. Do not describe
numeric REJECT as rejecting stronger signals or invent a SET MINSNR command.

Mode lists accept comma or space separators and are validated before any edit.
ALL must stand alone in the mode selector. A numeric ALL update covers current
supported modes. PASS's value ALL and REJECT's value NONE clear selected rules;
ALL in the selector clears every saved rule, including inactive ones. RESET
FILTER and PASS NOFILTER also clear them. SHOW FILTER, FULL and MINSNR show
sorted thresholds, configured/inactive counts and exemptions. Removed modes
remain saved but inactive and reactivate only when the exact canonical name
returns. Clear an inactive rule by its displayed exact name. Maps are limited
to 128 total rules and 65,536 aggregate ASCII key bytes, including inactive keys.

Thresholds are saved in profiles/presets, contribute to modified status and
revision tokens, and invalidate history continuation when edited. Existing
live/history self-spot exceptions still apply. Valid human edits keep existing
live-first save/log behavior; machine persistence failures publish no change.

Use `GET YAML FILTER SCHEMA 3` or `GET YAML CONFIG SCHEMA 3` for `min_snr`, a
map of exact uppercase mode keys to signed integers. Schema 3 status includes
configured/inactive counts and sorted inactive keys. A supplied map replaces
all thresholds; `{}` clears it, while omitted PATCH preserves it. Full schema 3
PUT/VALIDATE requires the map. A supplied unavailable name must already exist
in the prior configuration; new unavailable keys fail validation. Schemas 1/2
retain their shapes and preserve hidden thresholds. Default GET remains 1.

Saved version 3 reads older records without inventing minima and preserves
version 2 states and nested preset baselines. Downgrade requires matching
profile/preset backups, rather than editing only the version marker. Archive
format and report parsing do not change for this feature. See the
[telnet MINSNR contract](../../telnet/README.md#minimum-snr) and
[ADR-0258](../../docs/decisions/ADR-0258-per-mode-minimum-snr-filter.md).
