# GoCluster DX Cluster

GoCluster is a Go-based DX cluster for amateur radio operators. It collects spots from skimmer and operator feeds, adds CTY metadata, applies protection and cleanup stages, and serves fixed-width telnet output with filtering, confidence tags, and optional path hints.

## Start As A Telnet User

Connect to the cluster's configured telnet host and port. For a local default
setup, the shipped port is `8300`:

```text
telnet localhost 8300
```

Log in with your callsign, then start with:

- `HELP`: show the command list.
- `HELP <command>`: show command-specific help.
- `SHOW MYDX` or `SHOW DX`: show recent spots after your filters.
- `SHOW FILTER`: show your active filter state.
- `SHOW DEDUPE`: show your repeated-spot suppression policy.
- `SET GRID <grid>`: set your 4-6 character Maidenhead grid for distance,
  nearby filtering, and path hints.
- `SET NOISE QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL`: set local receive noise
  class for path hints.
- `BYE`: disconnect.

The sections below prioritize the telnet experience: command examples first,
then filters, dedupe, confidence, diagnostics, and path hints. Node setup,
build, release, and service details are later in this file and in
[`docs/OPERATOR_GUIDE.md`](docs/OPERATOR_GUIDE.md).

## Common Telnet Commands

- To allow or block spots, use `PASS <type> <list>` and
  `REJECT <type> <list>`, then confirm with `SHOW FILTER`.
- To change how repeated spots appear, use `SET DEDUPE FAST|MED|SLOW`.
- To focus on nearby spots, set your grid and use `PASS NEARBY ON`.
- To investigate surprising output, use
  `SET DIAG OFF|DEDUPE|SOURCE|CONF|PATH|MODE`.
- To understand path hints, use `SET GRID` and `SET NOISE`, then use
  `SET DIAG PATH` on spots whose path glyphs look surprising.
- To confirm the baseline call used for own-call features, use `SHOW OWN`.
- To see recent spotter countries for your baseline call, use `WHOSPOTSME [band]`.
- To receive periodic solar summaries, use `SET SOLAR 15|30|60|OFF`.

New DX spots are materialized and displayed without trailing numeric SSIDs on
the DX call. For example, `K1ABC-2` is treated as `K1ABC` when it is the station
being spotted. Login calls keep their session identity, but local own-call
features ignore numeric SSIDs.

### Output Examples

The examples below show representative telnet command output. Exact filter
lists and timing windows can differ when an operator changes the active config.

Confirm which baseline call is used for own-call features:

```text
> SHOW OWN
Own call: N2WQ
Login call: N2WQ-7
SSID handling: numeric SSIDs are ignored for own-call features.
```

Inspect and change your duplicate-suppression policy:

```text
> SHOW DEDUPE
Dedupe: SLOW (cqzone) (fast=on med=on slow=on)
> SET DEDUPE FAST
Dedupe policy set to FAST
```

Check whether nearby filtering or a blocklist is active. This example is
abridged; real `SHOW FILTER` output includes every filter domain:

```text
> SHOW FILTER
Current filters: BAND=ALL MODE=CW, LSB, USB, RTTY, FT8, FT4, FT2, PSK, JS8,
                 MSK144, SSTV SOURCE=ALL EVENT=LLOTA, IOTA, POTA, SOTA, WWFF
BAND: allow=ALL block=NONE
MODE: enabled=CW, LSB, USB, RTTY, FT8, FT4, FT2, PSK, JS8, MSK144, SSTV
EVENT: enabled=LLOTA, IOTA, POTA, SOTA, WWFF; no-event spots=pass
...
NEARBY: OFF
SELF: ON
```

See recent countries that have heard your baseline call:

```text
> WHOSPOTSME 20M
WHOSPOTSME 20M (last 10m):
  EU:  G(4) DL(2) F(1)
  NA:  K(3) VE(1)
```

### Filter Examples

Use comma-separated lists for multiple values. `PASS` adds to the allowlist for
that filter type; `REJECT` adds to the blocklist. Run `SHOW FILTER` after
changes to confirm the effective state.

```text
PASS BAND 20m,40m
REJECT BAND 160m,80m

PASS DXZONE 14,15
REJECT DEZONE 3,4,5

PASS DXCONT EU,AF
REJECT DECONT NA

PASS DXDXCC 291,110
REJECT DEDXCC 291

PASS DXGRID2 FN,EM
REJECT DEGRID2 DM

PASS DXCALL K1*,W1AW
REJECT DECALL N0CALL

PASS SOURCE HUMAN
REJECT SOURCE SKIMMER

PASS CONFIDENCE P,V,C
REJECT CONFIDENCE ?

PASS PATH HIGH,MEDIUM
REJECT PATH CLOSED,INSUFFICIENT

SHOW FILTER
```

## HELP

The section below mirrors the default `go` dialect `HELP` output from [`commands/processor.go`](commands/processor.go) using the shipped config in [`data/config`](data/config).

<!-- BEGIN DEFAULT_GO_HELP -->
```text
Available commands:
HELP - Show command list or command-specific help.
DX - Post a spot (human entry).
SHOW DX - Alias of SHOW MYDX.
SH DX - Alias of SHOW DX.
SHOW MYDX - Show filtered spot history.
SHOW DXCC - Look up DXCC/ADIF and zones.
SHOW BUILD - Show binary build metadata.
SHOW OWN - Show own-call identity.
WHOSPOTSME - Show recent spotter countries.
SHOW DEDUPE - Show dedupe policy.
SET DEDUPE - Select dedupe policy.
SET DIAG - Select diagnostic comments.
SET SOLAR - Solar summary cadence.
SET GRID - Set your grid (4-6 chars).
SET NOISE - Set noise class.
SET PATHSAMPLES - Set path sample floor.
PASS NEARBY - Toggle nearby filtering.
SHOW FILTER - Display filter state.
PASS - Allow filter matches.
REJECT - Block filter matches.
RESET FILTER - Reset filters to defaults.
DIALECT - Show or switch dialect.
BYE - Disconnect.
Type HELP <command> for details.

Filter core rules:
PASS <type> <list> adds to allowlist and removes from blocklist.
REJECT <type> <list> adds to blocklist and removes from allowlist.
PASS/REJECT MODE <list> are deltas; modes not listed are unchanged.
UNKNOWN is the MODE token for blank-mode spots.
If an item appears in both lists, block wins.

ALL keyword (type-scoped):
PASS <type> ALL - allow everything for that type
REJECT <type> ALL - block everything for most types
REJECT EVENT ALL - block only tagged EVENT spots
RESET FILTER resets all filters to configured defaults for new users.

Feature toggles (not list-based):
PASS BEACON | REJECT BEACON
PASS WWV | REJECT WWV
PASS WCY | REJECT WCY
PASS ANNOUNCE | REJECT ANNOUNCE
PASS SELF | REJECT SELF
PASS TOXIC | REJECT TOXIC
PASS NEARBY ON|OFF

Confidence glyphs:
  ? - One reporter only; no prior/static support promoted it to S.
  S - One reporter only, but the call has static or recent on-band support.
  P - Resolver modes: lower-confidence multi-spotter support. FT modes:
    corroboration burst support at or above the configured P threshold but
    below the configured V threshold.
  V - Resolver modes: higher-confidence multi-spotter support. FT modes:
    corroboration burst support at or above the configured V threshold.
  C - The call was corrected.
  B - A correction was attempted, but base-call or CTY validation failed, so
    the original call was kept.

Event filters:
  EVENT recognizes the taxonomy EVENT families as standalone comment tokens or
    acronym-prefixed references such as POTA-1234. Only the event family is
    filtered; the reference remains in the comment.
  Spots with no recognized EVENT tag are not affected by EVENT filters,
    including REJECT EVENT ALL.

Path reliability glyphs:
  ">" - HIGH: favorable path.
  "=" - MEDIUM: workable path.
  "<" - LOW: weak or marginal path.
  "-" - UNLIKELY: poor path.
  " " - INSUFFICIENT: not enough recent evidence.
  Bucket p50 data is authoritative; VOACAP may only replace insufficient data
    when closed or aligned with sparse p50.
  "#" - CLOSED: VOACAP fallback predicts the current UTC hour's blended,
    noise-adjusted SNR at or below the mode's closed threshold.
  PATH filters use HIGH, MEDIUM, LOW, UNLIKELY, CLOSED, INSUFFICIENT.

List types:
  BAND, MODE, SOURCE, EVENT, DXCALL, DECALL, DXGRID2, DEGRID2, DXCONT, DECONT
  DXZONE, DEZONE, DXDXCC, DEDXCC, CONFIDENCE, PATH

Supported modes:
  CW, FT2, FT4, FT8, JS8, LSB, USB, RTTY, MSK144, PSK, SSTV, UNKNOWN

Supported events:
  LLOTA, IOTA, POTA, SOTA, WWFF

Supported bands:
  2200m, 630m, 160m, 80m, 60m, 40m, 30m, 20m, 17m, 15m, 12m, 10m, 6m, 2m
  1.25m, 70cm, 33cm, 23cm, 13cm
```
<!-- END DEFAULT_GO_HELP -->

## Dedupe Policies

The cluster already removes upstream duplicates before spots reach users.
`SET DEDUPE` controls the second, operator-facing dedupe stage that decides how
aggressively repeated live spots are hidden in your telnet feed. `SHOW DEDUPE`
shows the active policy and whether `FAST`, `MED`, and `SLOW` are enabled
server-side.

Separately, shared-ingest flood control is configured in [`data/config/floodcontrol.yaml`](data/config/floodcontrol.yaml). That stage runs before primary dedupe, is not per-user, and can `observe`, `suppress`, or `drop` by actor rail. The shipped file starts in `observe` mode on every rail, but the file itself is required at startup.

Each user can choose a policy for their own session. The exact windows come
from the active `dedup.secondary_*_window_seconds` values in
`data/config/dedupe.yaml`:

- `FAST`: shortest configured window, keyed by band + DE DXCC + DE 2-character grid + DX call.
- `MED`: middle configured window, using the same key as `FAST`.
- `SLOW`: longest configured window, keyed by band + DE DXCC + DE CQ zone + DX call.

In plain terms:

- `FAST` shows more repeats from the same general area.
- `MED` is the middle ground.
- `SLOW` suppresses more repeats because CQ zone is broader than a 2-character grid square.
- New users use the operator-configured `dedup.default_policy` from `data/config/dedupe.yaml`; the shipped default is `SLOW`.
- If you request a disabled policy, the server automatically chooses an enabled policy and tells you what it picked.

WWV, WCY, and `TO ALL` announcement bulletins have a separate server-wide duplicate guard because they are delivered as telnet control traffic rather than spots. The shipped `runtime.yaml` suppresses identical bulletin lines for `600s` across peer and relay sources; set `telnet.bulletin_dedupe_window_seconds: 0` to disable that behavior.

## EVENT Filtering

`PASS EVENT` and `REJECT EVENT` filter spots by comment-derived activation/event family. Supported families come from `data/config/spot_taxonomy.yaml`; the shipped config defines `LLOTA`, `IOTA`, `POTA`, `SOTA`, and `WWFF`. Spots with no recognized EVENT tag are not affected by EVENT filters.

Event recognition is intentionally family-level. A comment token such as `POTA` or `POTA-1234` marks the spot as `POTA`; the reference text stays in the comment and is not a separate filter key. Slash forms such as `POTA/SOTA` and event-specific reference grammars without the acronym prefix are not interpreted by this filter.

Common commands:

```text
PASS EVENT POTA,SOTA
REJECT EVENT WWFF
PASS EVENT ALL
REJECT EVENT ALL
```

`REJECT EVENT ALL` hides all EVENT-tagged spots; spots with no event tag still pass this filter domain.

## Toxic Comment Filtering

`PASS TOXIC` and `REJECT TOXIC` control optional filtering for human-entered
spot comments that the toxicity classifier has already evaluated.

- `REJECT TOXIC` hides only spots marked `TOXIC`.
- `UNKNOWN`, `SAFE`, `SAFE_LOCAL`, and `UNAVAILABLE` spots still pass.
- Skimmer and other non-human spots bypass the classifier.
- The classifier receives only the cleaned free-text comment, not mode, band,
  callsigns, source, IP, session data, raw spot lines, or archive records.
- `SHOW FILTER` shows the current `TOXIC` state.

The feature is disabled by default. Operators enable it with
[`data/config/toxicity.yaml`](data/config/toxicity.yaml), a private bearer-token
environment variable, and a Cloudflare Worker that exposes `POST /classify`.
[`data/config/toxicity_safe_gate.yaml`](data/config/toxicity_safe_gate.yaml)
contains the conservative local safe gate for routine ham-radio shorthand.

The safe gate is deliberately narrow: the whole comment must look routine to
bypass AI. Mixed comments such as a normal ham token plus unrelated abusive or
ordinary language are sent to the Worker, including common Western-language
comments in English, Spanish, French, German, Italian, and Portuguese. There
are no language-specific classifier goroutines or language detectors.

## MODE And EVENT Taxonomy

`data/config/spot_taxonomy.yaml` is the single operator-editable table for supported MODE tokens, EVENT families, PSKReporter mode routing, and existing mode capability classes. `ingest.yaml` keeps only PSKReporter transport/runtime settings; mode admission moved to `pskreporter_route` in the taxonomy.

This is a binary+config contract. Deploy or roll back the binary and config directory together, and restart the cluster after changing taxonomy entries.

## NEARBY Filtering

`NEARBY` is a quick local-area filter for operators who want spots near their own location without building manual continent, zone, DXCC, or grid lists.

First set your grid with `SET GRID <4-6 char maidenhead>`, then turn nearby filtering on with `PASS NEARBY ON`. While it is on, the cluster keeps spots whose DX side or DE side falls in your nearby area.

Band handling is intentionally simple:

- `160m`, `80m`, and `60m` use a coarser local area.
- All other supported bands use a finer local area.

`NEARBY` also changes how location filters behave:

- While `NEARBY` is on, the regular location filters are suspended: `DXGRID2`, `DEGRID2`, `DXCONT`, `DECONT`, `DXZONE`, `DEZONE`, `DXDXCC`, and `DEDXCC`.
- Attempts to change those filters while `NEARBY` is on are rejected with a warning.
- `PASS NEARBY OFF` restores the saved location-filter state from before `NEARBY` was enabled.

`NEARBY` persists across logins. The login greeting warns you when it is active,
and `SHOW FILTER` includes the current `NEARBY` state. If your stored grid is
missing, `NEARBY` stays stored but inactive until a valid grid is configured.
When path reliability is enabled, missing or invalid H3 mapping tables fail
startup and are reported in the system log instead of silently weakening path
predictions.

## Confidence Tags

Confidence tags appear in the telnet confidence column and can be filtered with `PASS CONFIDENCE` and `REJECT CONFIDENCE`.

- `?`: little evidence.
- `S`: one current report, but the call has static or recent on-band support.
- `P`: corroborated, but not the strongest support level.
- `V`: strongly corroborated.
- `C`: the DX call was corrected and the corrected call passed validation.
- `B`: a correction was attempted, but the suggested call failed validation, so the original call was kept.

Different mode families calculate support differently, but the displayed glyphs
are intended to be read the same way by telnet users.

Local non-test `DX` self-spots are treated as operator-authoritative and are forced to `V`.

For the mode-specific support rules, timing knobs, and decision history, see
[`spot/README.md`](spot/README.md).

## Diagnostic Comments

`SET DIAG <mode>` replaces the free-form spot comment for your telnet session only. The spot mode/report and fixed tail columns remain in their normal positions.

- `SET DIAG OFF`: show normal comments.
- `SET DIAG DEDUPE`: `<DE-DXCC>|<DE-key>|<src>|<policy>`, where `<src>` is `H` for human-class or `S` for skimmer/automated-class.
- `SET DIAG SOURCE`: `<source>` with `MAN`, `RBN`, `RBNFT`, `PSK`, `DXS`, `UP`, or `P:<peer>` for peer-origin spots.
- `SET DIAG CONF`: `<score>%` when the pipeline calculated a confidence percent, otherwise `--%`.
- `SET DIAG PATH`: `n<count>|w<weight>|a<age>` for usable path evidence, `n<count>|<reason>` for insufficient evidence (`none`, `lown`, `lowr`, `loww`, or `stale`), `vcap|<snr>|h<hour>|s<ssn>` for a cached VOACAP closed fallback, or `valn|<p50>/<snr>h<hour>s<ssn>` for VOACAP-aligned sparse p50.
- `SET DIAG MODE`: `<mode>|<provenance>` to show the final normalized mode and why it was assigned.

Mode provenance tokens:

- `SRC`: source supplied the mode explicitly.
- `CMT`: mode parsed from the spot comment.
- `EVD`: inferred from recent same-DX/frequency evidence.
- `FQ`: inferred from digital frequency evidence.
- `RCW`: regional band-plan CW default.
- `RVO`: regional voice default.
- `RMIX`: regional mixed segment, left blank intentionally.
- `RUNK`: unknown-region blank default.
- `UNK`: no provenance recorded.

`SET DIAG PATH` explains the path-reliability hint behind the spot. The fields
are intentionally short because they must fit in the normal DX-cluster comment
area:

- `n<count>` is the raw selected observation count behind the selected path
  evidence in every receiver-cap mode. It is a sample-size indicator, not a
  confidence percent. `n0` means no usable
  selected observations; `n1` means one selected observation; higher values
  such as `n18` or `n32` mean a larger evidence base.
- `w<weight>` is the rounded effective weight after decay, fine/coarse sample
  selection, receive/transmit merge, and reverse-direction discounting. Fine
  and coarse are overlapping evidence layers, so blended scalar weight uses the
  larger layer instead of adding both. It is not dB, SNR, or a percent. A count
  can be much larger than the weight when the observations are old, discounted,
  or weakly applicable to the exact path. Weight is an evidence-strength gate;
  it is not the path class itself. A path can show `>` in the normal path column
  with `w1` in the diagnostic comment when the effective weight is just above
  the minimum and the normalized signal estimate maps to `HIGH`.
- `a<age>` is the effective age of the selected evidence. Ages under one minute
  are seconds, then rounded up to `m` or `h`. Blended fine/coarse age uses local
  fine mass plus the coarse regional complement, so stale local mass can make a
  direction old enough to be dropped by the freshness gate.
- `vcap|<snr>|h<hour>|s<ssn>` means the optional VOACAP closed fallback
  supplied the result. `<snr>` is the selected hour's rounded bidirectional
  FT8-equivalent SNR after receive-side noise penalty, `h<hour>` is the
  selected UTC forecast hour, and `<ssn>` is the rounded EWMA SSN generation
  used for the run.
- `valn|<p50>/<snr>h<hour>s<ssn>` means sparse bucket p50 evidence was
  insufficient by sample gates but aligned with the current-hour VOACAP class.
  `<p50>` is rounded for display so the diagnostic fits the fixed-width line.
- `n<count>|none` means there was no usable selected path sample.
- `n<count>|lown` means selected evidence existed but the selected observation count
  stayed below the configured minimum.
- `n<count>|lowr` means raw selected observations met the count floor, but
  attributed receiver diversity was too low under receiver-cap enforcement or
  receiver-cap shadow evaluation.
- `n<count>|loww` means selected evidence existed but the effective weight
  stayed below the configured minimum.
- `n<count>|stale` means selected evidence existed but was too old for the
  band's display/filter freshness gate.

The cluster line keeps the spot mode/report and fixed tail columns in their
normal positions. If the diagnostic comment is too long for the remaining
comment space, the right edge is clipped. Read clipped path diagnostics from
left to right; the omitted rightmost characters are display loss only, not
different path logic. For VOACAP fallback diagnostics, the selected SNR and UTC
hour are intentionally kept near the left edge.

Example readings:

- `n18|w7`: 18 selected observations, rounded effective weight 7. The age
  token may be clipped if it does not fit before the fixed tail.
- `n0|none`: no usable selected path sample.
- `n3|lown`: three selected observations existed, but the configured minimum
  sample size was not met.
- `n19/c5/rx1|lowr`: nineteen raw observations existed, but capped receiver
  evidence represented only one attributed receiver.
- `n19/c5/rx1|w3`: raw selected observations are shown first, capped effective
  count after `/c`, and attributed receivers after `/rx` when receiver caps
  reduced diagnostic evidence.
- `vcap|-34|h20|s112`: VOACAP fallback selected the 20:00 UTC forecast record,
  predicted FT8-equivalent SNR -34, and used SSN generation 112.
- `valn|-15/-15h20s112`: sparse bucket p50 rounded to -15 dB and the 20:00
  UTC VOACAP forecast also mapped to that same path class.
- `n1|loww`: one selected observation existed, but the effective weight was
  below the minimum.
- `n32|w1`: large selected count but low rounded effective weight. Treat this
  as useful but thinner evidence than `w7`.

## Path Reliability Tags

Path reliability is an optional telnet hint based on your grid, the DX grid,
recent reports, and the active settings in
[`data/config/path_reliability.yaml`](data/config/path_reliability.yaml). Some
path settings are operator policy, but decay, merge weights, mode thresholds,
offsets, and noise tables are algorithm calibration.

At a high level, the cluster:

1. accepts recent reports from supported path modes such as `FT8`, `FT4`, `CW`, `RTTY`, `PSK`, and `WSPR`
2. converts those reports onto a common FT8-like signal scale
3. groups them by coarse and fine geographic cells derived from Maidenhead grids
4. combines recent DX-to-you and you-to-DX evidence with decay over time
5. rejects selected evidence that is too old for the band's freshness gate
6. resolves your selected noise class on the receive side; the checked-in table
   applies one scalar dB penalty per class
7. maps the result to `HIGH`, `MEDIUM`, `LOW`, `UNLIKELY`, or `INSUFFICIENT`;
   VOACAP closed fallback results use the separate `CLOSED` filter value

What the classes mean to an operator:

| Display | PATH filter value | Operator meaning | If it looks wrong |
| --- | --- | --- | --- |
| `>` | `HIGH` | Recent evidence suggests a favorable path, or sparse p50 aligned with VOACAP when bucket evidence was insufficient. | Use `SET DIAG PATH` to see sample count, weight, age, or `valn` alignment. |
| `=` | `MEDIUM` | Recent evidence suggests a workable path, or sparse p50 aligned with VOACAP when bucket evidence was insufficient. | Use `SET DIAG PATH`; low effective weight can still map to a usable class. |
| `<` | `LOW` | Recent evidence suggests a weak or marginal path, or sparse p50 aligned with VOACAP when bucket evidence was insufficient. | Use `SET DIAG PATH` to confirm grids, sample count, freshness, or `valn` alignment. |
| `-` | `UNLIKELY` | Recent evidence suggests a poor path, or sparse p50 aligned with VOACAP when bucket evidence was insufficient. | Check whether your grid and the DX grid are correct before treating this as a hard no. |
| `#` | `CLOSED` | Bucket evidence was insufficient, but the optional VOACAP fallback predicts closed conditions for the current mode and path. | Check `SET DIAG PATH`; VOACAP fallback never overrides sufficient bucket evidence. |
| blank | `INSUFFICIENT` | The cluster did not have enough usable recent evidence to rate the path. | Run `SET DIAG PATH`; common reasons are `none`, `lown`, `lowr`, `loww`, and `stale`. |

Important operational notes:

- You need `SET GRID` for path hints to be useful.
- `SET NOISE` stores a receive-noise class; the checked-in path config applies
  one scalar dB penalty per class.
- `SET PATHSAMPLES <count>` lets you require more selected observations than
  the cluster default before your session shows a path tag. `SET PATHSAMPLES
  DEFAULT` clears that personal override.
- Stale evidence becomes `INSUFFICIENT`; age alone does not demote a strong
  path through weaker glyph tiers.
- Active p50 uses midpoint representatives for fixed SNR bins. Balanced
  weak/strong evidence uses the middle between both selected bin representatives
  instead of always choosing the weaker bin.
- Fine/coarse scalar weight uses union semantics because the fine layer also
  updates the coarse layer. The p50 histogram keeps the existing local-emphasis
  shape, so this fixes evidence mass without changing the selected p50
  distribution for eligible samples.
- Receiver contribution caps are configured in `enforce` mode. Normal glyphs
  and PATH filters use capped receiver evidence, with the checked-in cap set to
  eight decayed effective observations per receiver per bucket.
- Five-minute `Path predictions (5m)` logs split insufficient evidence into
  `no_sample`, `low_count`, `low_receiver`, `low_weight`, and `stale`;
  `low_count` means the selected raw sample count missed the observation floor,
  `low_receiver` means receiver diversity missed the derived receiver gate,
  `low_weight` means decayed effective weight missed the weight floor, and
  `stale` can increase when honest fine/coarse age drops an old local direction.
  VOACAP fallback outcomes are counted separately as `voacap_closed` and
  `voacap_aligned`.
  A separate `VOACAP fallback (5m)` line appears when fallback work occurs and
  reports stage counters such as `queued`, `success`, `cache_hit`,
  `no_current_hour`, `closed`, `closed_no_p50`,
  `closed_with_sparse_p50`, `closed_with_sparse_p50_class_*`, `aligned`,
  `open_no_p50`, and `class_mismatch`.
  A separate `VOACAP p50 compare (5m)` line may appear when sufficient p50
  predictions can be compared against an existing current-hour VOACAP cache
  record. It is cache-only: cache misses do not run VOACAP, start delay
  windows, or change glyphs.
  These lines are written to `logging.propagation.dir`, not the system log.
- If grids are missing, evidence is stale, too sparse, or too weak, the result
  stays `INSUFFICIENT`. When path reliability is enabled, H3 table failures are
  startup failures because those cells are critical to path predictions.
- If `voacap_fallback.enabled` is true, an insufficient bucket result may start
  a delayed nonblocking VOACAP lookup. A cached fallback can replace the blank
  glyph with the configured closed glyph when the current UTC hour's blended
  bidirectional VOACAP SNR, after the user's receive-side noise penalty, is at
  or below `mode_thresholds.<mode>.closed`, or with a normal glyph when sparse
  bucket p50 and VOACAP map to the same path class. Sufficient bucket p50
  results stay authoritative. Runtime fallback decks cover the rolling UTC
  forecast window; parsed VOACAP hour `24` is treated as UTC hour `0`.
- `PATH` filters work on the class names, not on the glyph characters.
  `CLOSED` is a VOACAP-closed subtype of `UNLIKELY`: existing
  `PASS/REJECT PATH UNLIKELY` filters still include closed fallback spots,
  while direct `PASS/REJECT PATH CLOSED` rules can pass or reject only closed
  fallback spots.
- `R` and `G` are solar-weather display overrides, not normal path classes.

If solar-weather support is enabled, a normal path glyph can be replaced by:

- `R` for a radio-blackout override
- `G` for a geomagnetic-storm override

Those overrides never replace `INSUFFICIENT`.

For the exact thresholds, per-mode offsets, weight rules, and shipped tables, see [`pathreliability/README.md`](pathreliability/README.md).

## What The Cluster Does

- Ingests spots from RBN CW/RTTY, RBN digital, PSKReporter, optional DXSummit HTTP polling, local `DX` commands, and optional peer feeds.
- Shows enabled ingest sources in the console dashboard; DXSummit appears as `DXSUMMIT` when enabled and recently polling.
- Normalizes callsigns, frequencies, modes, and reports before shared validation and enrichment.
- Adds CTY metadata and optional FCC license checks where that policy applies.
- Applies shared-ingest flood policy before primary dedupe using the shipped `floodcontrol.yaml` rails.
- Deduplicates and fans out spots to telnet clients with per-user filters.
- Optionally derives path-reliability glyphs from recent reports between your grid and the DX grid.

## Running A Node

Compiled ready-to-run packages are published on GitHub Releases. The current
published binary package is Windows amd64:

1. Open the latest release:
   [`https://github.com/N2WQ/GoCluster/releases/latest`](https://github.com/N2WQ/GoCluster/releases/latest)
2. Download the release asset named `gocluster-windows-amd64.zip`.
3. Extract the zip and open the `ready_to_run/` directory.
4. Start with the packaged `ready_to_run/README.md`.

Do not use GitHub's automatic `Source code (zip)` or `Source code (tar.gz)`
downloads unless you want the developer source tree. Those archives are not the
ready-to-run package. More detail is in [`download/README.md`](download/README.md).

From a ready-to-run Windows package:

```pwsh
cd ready_to_run
$env:DXC_CONFIG_PATH = "data/config.local"
.\gocluster.exe
```

From a source checkout:

```pwsh
$env:DXC_CONFIG_PATH = "data/config.local"
go run .
```

Then connect with telnet using the configured port from
`data/config/runtime.yaml`.

## Configuring A Real Node

The checked-in [`data/config`](data/config) directory is public example config.
For a real node, copy the whole directory to a private complete config
directory such as ignored `data/config.local`, edit that private copy, and run
with `DXC_CONFIG_PATH` pointing at the directory.

Review normal deployment/runtime files before first run:

- `app.yaml`: set `server.node_id`, choose local UI mode, and confirm log paths.
- `runtime.yaml`: confirm telnet port, filter defaults, buffers, and Go runtime controls.
- `ingest.yaml`: configure RBN, PSKReporter, DXSummit, and local/human ingest settings.
- `peering.yaml`: edit only if this node peers with other clusters.
- `reputation.yaml`: edit only if IPinfo/Cymru reputation enrichment is enabled.
- `solarweather.yaml`: edit only if solar/geomagnetic path overrides are enabled.
- `data.yaml`: adjust CTY, FCC, H3, skew, and data paths if your deployment layout differs.
- `spot_taxonomy.yaml`: edit only when changing supported modes, event families, or PSKReporter mode routing.

Do not retune `pipeline.yaml`, path thresholds, solar override gates, or
mode-inference calibration as normal setup. Use
[`data/config/README.md`](data/config/README.md) for the ownership class before
editing a YAML file.

The loader expects a complete config directory and rejects unknown YAML files.
It walks the required startup config set before aborting, so missing required
files and missing/null YAML-owned settings are reported together in the startup
diagnostics. Extra YAML keys are logged as config warnings and ignored; known
removed migration keys still fail startup with a migration hint. Keep private
callsigns, peer hostnames/IPs, passwords, and tokens out of committed example
config.

At minimum, replace the public placeholder identity before connecting a real
node: change `server.node_id` in `app.yaml` from `N0CALL-1`, change the RBN
login callsigns in `ingest.yaml` from `N0CALL-1`, and update any private
upstream telnet `host` and login fields you enable. If peering is enabled,
also replace peer hosts, login callsigns, and passwords in `peering.yaml`.

## Build And Service Notes

GoCluster builds from the repo root with Go `1.26+`.

Windows amd64 binary:

```pwsh
go test ./...
go build -trimpath -o gocluster.exe .
```

Windows release-style package for local testing:

```pwsh
.\scripts\create-release.ps1 -PackageOnly -AllowDirty
```

Clean publishable Windows release package:

```pwsh
.\scripts\create-release.ps1
```

Linux amd64 binary from source:

```sh
go test ./...
GOOS=linux GOARCH=amd64 go build -trimpath -o gocluster .
```

Deploy the Linux binary together with a complete config directory and required
runtime data such as `data/cty`, `data/h3`, `data/peers/topology.db`, and
`data/skm_correction/rbnskew.json` when those inputs are used by your config.
There is not currently a published Linux ready-to-run release asset.

For unattended Linux operation, use a private config directory and set
`ui.mode: headless` in that config's `app.yaml`. The interactive local console
requires a real terminal and is not shown by a normal `systemd` service. See
[`docs/OPERATOR_GUIDE.md`](docs/OPERATOR_GUIDE.md) for the complete service
account, unit-file, and operational command sequence.

## Operator Logs

`logging.dropped_calls` can write optional UTC-rotated files for dropped calls without changing any drop policy. The shipped config enables it; set `logging.dropped_calls.enabled: false` to disable those files. When enabled, the cluster writes separate files for bad DE/DX calls, FCC no-license drops, and harmonic suppressions under `logging.dropped_calls.dir`.

Each entry uses the same timestamped file logger as the system log and records only the ingestion source, dropped role, reason, call, DE, DX, mode, and a short detail field. Frequency, category, and dashboard text are intentionally omitted.

`logging.login_attempts`, `logging.reputation_drops`, `logging.telnet_connections`, `logging.ingest_connections`, and `logging.peer_connections` write separate file-only event logs for failed or blocked login attempts, reputation-gated spot drops, telnet lifecycle, ingest lifecycle, and peer lifecycle. These event logs do not add local console or UI output; check `data/config/README.md` for the per-log `enabled`, `dir`, `retention_days`, and `dedupe_window_seconds` settings.

Runtime file logs keep a stable active filename derived from the configured
directory name, such as `data/logs/system/system.log` and
`data/logs/propagation/propagation.log`. Completed UTC days archive with the
existing date-only format, such as `07-Jun-2026.log`.

`logging.propagation` writes a separate file-only propagation log under
`data/logs/propagation` by default. This is where the five-minute path
prediction, source mix, bucket, weight distribution, ge10 variance, unique
spotter/grid-pair, and report inputs are written.
Daily propagation reports read the completed propagation archive by default;
pass `prop_report -log` to an old system log path when generating reports from
historical files.

```yaml
logging:
  dropped_calls:
    enabled: true
    dir: "data/logs/dropped_calls"
    retention_days: 7
    dedupe_window_seconds: 120
    bad_de_dx: true
    no_license: true
    harmonics: true
  propagation:
    enabled: true
    dir: "data/logs/propagation"
    retention_days: 7
```

## Repo Layout

The repo root now follows a simple ownership rule:

- `main.go` is the live binary entrypoint only.
- `internal/cluster` contains the live runtime implementation and cluster-local helpers.
- `cmd/` contains standalone tools and offline runners.
- `scripts/` contains build, release, profiling, validation, and developer helper scripts; use [`scripts/README.md`](scripts/README.md) before running or changing them.
- `data/` contains more than config: public example YAML in `data/config/`,
  private ignored config in `data/config.local/`, reference inputs such as CTY,
  FCC, H3, grids, beacons, and reputation/IPinfo data, plus runtime/local state
  such as users, logs, reports, diagnostics, peer topology, RBN data, SCP data,
  and skew/correction data. Treat committed example/reference data differently
  from ignored operator-local state.
- Domain packages such as `spot`, `peer`, `telnet`, `config`, and `pathreliability` remain reusable subsystems with their own tests and package-local docs.

Historical analysis notes and protocol reference material live under [`docs/archive/analysis`](docs/archive/analysis) and [`docs/reference`](docs/reference) rather than competing with the live binary at the repo root.

## Deeper Docs

Implementation-heavy material now lives next to the relevant code:

- [`commands/README.md`](commands/README.md) - HELP source of truth, dialects, and command/filter behavior
- [`telnet/README.md`](telnet/README.md) - login flow, output lines, dedupe, `NEARBY`, path display, and filter persistence
- [`spot/README.md`](spot/README.md) - confidence calculation, correction flow, and FT policy knobs
- [`pathreliability/README.md`](pathreliability/README.md) - path bucket math and shipped YAML tuning
- [`rbn/README.md`](rbn/README.md) - structural RBN parsing and comment handoff
- [`pskreporter/README.md`](pskreporter/README.md) - MQTT normalization, path-only modes, and FT frequency handling
- [`dxsummit/README.md`](dxsummit/README.md) - HTTP polling, DXSummit source markers, and HF/VHF/UHF scope
- [`peer/README.md`](peer/README.md) - peer forwarding, receive-only behavior, and control-plane details
- [`scripts/README.md`](scripts/README.md) - build, release, profiling, and workflow helper scripts
- [`data/config/README.md`](data/config/README.md) - YAML ownership, loader rules, and safe config editing boundaries
- [`data/h3/README.md`](data/h3/README.md) - H3 dataset notes

Additional operator references:

- [`docs/OPERATOR_GUIDE.md`](docs/OPERATOR_GUIDE.md)
