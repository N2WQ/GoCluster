# Telnet Surface

This directory owns the telnet session layer: login flow, prompt handling, filter commands, client fan-out, and path display.

## Login And Session Behavior

- Clients log in with a callsign before commands are accepted.
- Login rejects command-like and mode-like tokens such as numeric-only port
  strings, all-letter words, `FT8`, `NOFT8`, or `SET/NOFT8` before any
  per-user state is loaded.
- Valid login tokens must include a concrete call-like identity segment such as
  `K1ABC`, `DL6LD`, `4U1UN`, `P5/N1K`, or `W6TEST-1`.
- When CTY data is loaded, login calls must resolve to a known CTY prefix.
- US calls (ADIF 291) must pass FCC ULS validation when ULS is available.
- Local TEST calls with CTY-valid US prefixes, such as `W6TEST` or `W6TEST-1`,
  bypass FCC ULS validation but still require CTY validity.
- CTY or FCC data outages fail open so operators are not locked out by a
  reference-data refresh or missing local database.
- The greeting can include dialect, grid, noise, and dedupe status from config.
- Dialect choice and filter state are persisted per callsign.
- If `NEARBY` is active, the login greeting warns the user.

The handshake transcript tests in this package cover the visible login sequence.

## Spot Line Format

Spot lines are formatted in [`../spot/spot.go`](../spot/spot.go) and sent by the telnet layer.

Key operator-visible facts for the shipped config:

- `data/config/runtime.yaml` sets `telnet.output_line_length: 76`, so spot
  lines are `76` characters before CRLF
- mode starts at 1-based column `40`
- the fixed tail holds:
  - grid at column `65`
  - confidence at column `70`
  - time at column `72`
- the telnet layer normalizes line endings to CRLF

The formatter keeps the right-side tail stable by truncating comment text before it can push the grid, confidence, or time columns around.

Beacon spots with a blank source comment display `BEACON` in the comment field.
The mode, SNR/report text, path glyph, grid, confidence, and time columns stay
in their normal positions. This is a telnet and archive display convention; it
does not rewrite the comment forwarded to peers.
Blank `NCDXF B` source-class beacon comments display `NCDXF BEACON`.

## Filters

The telnet layer owns the live filter parser and persistence rules.

- `PASS` adds to the allow list and removes from the block list
- `REJECT` adds to the block list and removes from the allow list
- if a value appears in both, block wins
- `RESET FILTER` restores configured defaults for new users
- `PASS/REJECT MODE <list>` are deltas; modes not listed are unchanged
- `UNKNOWN` is the MODE token for blank-mode spots; use `PASS MODE UNKNOWN` or `PASS MODE ALL` to show them again after they are hidden

Path and confidence filters are operator-visible here:

- `PASS/REJECT CONFIDENCE` works with `?`, `S`, `C`, `P`, `V`, `B`
- `PASS/REJECT PATH` works with `HIGH`, `MEDIUM`, `LOW`, `UNLIKELY`, `CLOSED`, `INSUFFICIENT`
- `PASS/REJECT MODE` and `PASS/REJECT EVENT` validate against the active `spot_taxonomy.yaml`
- the shipped `PASS/REJECT EVENT` families are `LLOTA`, `IOTA`, `POTA`, `SOTA`, `WWFF`, or `ALL`
- `PASS TOXIC` shows spots classified as toxic; `REJECT TOXIC` hides only spots with the classifier status `TOXIC`

`CLOSED` is the optional VOACAP closed fallback class. It is filter-visible as
`CLOSED`, but remains compatible with `UNLIKELY`: existing
`PASS/REJECT PATH UNLIKELY` filters still include closed fallback spots. Direct
`PASS/REJECT PATH CLOSED` rules target only closed fallback spots.

EVENT filters are family-level. Standalone tokens such as `POTA` and acronym-prefixed references such as `POTA-1234` both match `POTA`; the reference stays in the comment and is not separately filterable. Spots with no recognized EVENT tag are not affected by EVENT filters, including `REJECT EVENT ALL`.

Toxic comment filtering is optional and fail-open. When the classifier is
disabled, unavailable, timed out, or has not seen a human comment yet, the spot
passes `REJECT TOXIC`. Routine comments that match the local ham-radio safe
gate are marked `SAFE_LOCAL` and also pass. The classifier receives only the
cleaned free-text comment, not callsigns, band, mode, source, IP, session data,
or archive records.

`SHOW FILTER` reports the current `TOXIC` toggle. The toggle applies to live
spots and archive-backed history queries. Local self-spot bypasses still honor
`REJECT TOXIC` once a spot is classified as `TOXIC`.

## Dedupe Policies

The telnet server exposes per-user dedupe policy control through `SHOW DEDUPE` and `SET DEDUPE`.

- new users default to `dedup.default_policy` from the active config directory; the shipped default is `SLOW`
- the selected policy is persisted per callsign
- `SHOW DEDUPE` reports the active policy and whether `FAST`, `MED`, and `SLOW` are enabled server-side
- if a user requests a disabled policy, the server falls back to an enabled one and reports that in the response

Policy windows come from the active `dedup.secondary_*_window_seconds` values
in `data/config/dedupe.yaml`:

- `FAST`: shortest configured window, keyed by band + DE DXCC + DE grid2 + DX call
- `MED`: middle configured window, keyed by band + DE DXCC + DE grid2 + DX call
- `SLOW`: longest configured window, keyed by band + DE DXCC + DE CQ zone + DX call

This is why `SLOW` suppresses more repeats from one region than `FAST` or `MED`: CQ zone is broader than a 2-character grid square.

## Bulletin Dedupe

WWV, WCY, and `TO ALL` announcement lines are control traffic, not spots, so they do not pass through the spot dedupe pipeline.

The telnet server applies a separate all-source duplicate guard before those lines enter per-client control queues:

- `telnet.bulletin_dedupe_window_seconds` sets the duplicate window; the shipped default is `600`.
- `telnet.bulletin_dedupe_window_seconds: 0` disables bulletin dedupe.
- `telnet.bulletin_dedupe_max_entries` bounds retained bulletin keys; the shipped default is `4096`.
- The key is the normalized bulletin kind plus the exact line shown to users, after newline normalization.
- Direct talk messages are not included.

If a duplicate is suppressed, slow clients do not see another control-queue enqueue. Unique bulletins still use the normal control queue, where a full queue disconnects the client.

## Grid, Noise, And Nearby

- `SET GRID` stores the user's Maidenhead grid for path reliability
- `SET NOISE` stores the user's noise class; the checked-in path config applies one receive-side penalty per class
- `SET PATHSAMPLES` stores a stricter per-user path sample floor; it cannot lower the cluster default
- `SHOW PROP <call|prefix|grid> [band] [mode]` uses the saved grid and noise
  class to show a point-to-point VOACAP outlook; omitted mode defaults to CW
- `PASS NEARBY ON` requires a grid and keeps spots whose DX side or DE side falls in the user's nearby area

`NEARBY` uses H3 cells:

- coarse resolution on `160m`, `80m`, and `60m`
- finer resolution on the other supported bands

While `NEARBY` is active:

- the regular location filters are suspended
- attempts to change `DXGRID2`, `DEGRID2`, `DXCONT`, `DECONT`, `DXZONE`, `DEZONE`, `DXDXCC`, and `DEDXCC` are rejected with a warning
- `SHOW FILTER` reports `NEARBY: ON (location filters suspended)`

When `PASS NEARBY OFF` is used, the telnet layer restores the saved location-filter snapshot that existed before `NEARBY` was enabled.

`NEARBY` state is persisted. On login:

- the greeting warns when `NEARBY` is active
- if the user has no usable grid or H3 mapping is unavailable, the state remains stored but inactive until it can be reactivated cleanly

## Path Display

The telnet layer asks the path predictor for a class and glyph when path display is enabled and the user has a grid.

- normal classes come from [`../pathreliability`](../pathreliability)
- a per-user `SET PATHSAMPLES` override can only require more observations than the cluster default
- optional `R` and `G` solar-weather overrides are applied afterward
- the insufficient state is preserved and is not replaced by solar overrides

`SHOW PROP` is a command path rather than a spot-display glyph path. It resolves
an explicit grid, gridstore callsign, or CTY-derived prefix/callsign to a target
grid and asks the VOACAP fallback cache for the current UTC hour through the
configured forecast horizon. Empty or partial single-band lookups enqueue a
refresh through the existing VOACAP fallback worker and may wait briefly;
all-band lookups show cached rows while refreshing missing or partial bands in
the background. Cached rows show mode-specific glyphs for `EFF`, `RX`, and
`TX`, plus text `REL` for the merged path class. `RX` and `TX` are per-leg
projections on the same glyph scale, not independent live spot glyph decisions.
Rows whose `REL` prediction is `UNLIKELY` or `CLOSED` are hidden. Bucket p50 is
not shown.

For command HELP and dialect details, see [`../commands/README.md`](../commands/README.md).
