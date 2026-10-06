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
  group all such labels per entity and retain every flag and false entry.
  Conflicting labels are omitted; valid alternatives remain. Long overview counts
  count entities. With no usable label, rules show `Unknown DXCC (12345)`;
  each entity retains its own number in this fallback. YAML stays numeric.
  SHOW DX/MYDX recognize canonical labels before portable callsign processing;
  numeric history arguments remain counts. SHOW DXCC details are unchanged.

- Human SHOW FILTER uses aligned labels and wraps all passing finite selections
  by name: BAND, MODE, SOURCE, EVENT, PATH, CONFIDENCE and DX/DE continents.
  Explain All, None and useful explicit exclusions, including All except.
  Do not enumerate disabled choices or replace finite names with counts.
  Long callsign, DXCC, grid and zone lists retain counts, with grouped geography
  and inclusion switches. Unknown-mode, untagged-event, confidence-exemption,
  PATH legacy and NEARBY qualifications still apply.
  FULL/category exposes every exact flag and value, including false entries,
  explicit defaults and empty collections. SHOW SETTINGS separates configured
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
  the human exact format as the client YAML schema.

## Sources

- [telnet/README.md human readbacks](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#human-configuration-readbacks)
- [telnet/README.md client schema and framing](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#client-yaml-configuration)
- [telnet/README.md named presets and persistence](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#named-presets)
- [README.md output examples](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md#output-examples)
- [commands/README.md HELP ownership](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md)
- [data/config/README.md effective node YAML](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md)
