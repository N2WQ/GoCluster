# GoCluster Operator Guide

This guide is for running a GoCluster node and connecting to it as a telnet
DX-cluster user. For implementation details, use the package READMEs linked
from the repository root.

## Get A Binary

The current ready-to-run release asset is Windows amd64:

```text
https://github.com/N2WQ/GoCluster/releases/latest
gocluster-windows-amd64.zip
```

Download that asset, not GitHub's automatic source-code archives. Extract it
and open `ready_to_run/`.

Linux operators currently build from source:

```sh
GOOS=linux GOARCH=amd64 go build -trimpath -o gocluster .
```

## Configure A Real Node

The packaged and checked-in `data/config` directory is public example config.
For a real node:

1. Copy the whole directory to a private complete directory, for example
   `data/config.local`.
2. Edit the private copy.
3. Start the server with `DXC_CONFIG_PATH` pointing at that directory.

Review normal deployment/runtime files before first run:

- `app.yaml`: server node ID, `headless` or `tview-v2` local UI mode, and logging paths.
- `runtime.yaml`: telnet port, default filters, buffers, and Go runtime controls.
- `ingest.yaml`: RBN, PSKReporter, DXSummit, and human/manual ingest settings.
- `peering.yaml`: only if this node connects to peer clusters.
- `reputation.yaml`: only if IPinfo/Cymru reputation enrichment is enabled.
- `solarweather.yaml`: only if solar/geomagnetic path overrides are enabled.
- `data.yaml`: CTY, FCC, H3, skew, and runtime data paths.
- `spot_taxonomy.yaml`: only when changing supported modes, events, or
  PSKReporter mode routing.

Do not retune `pipeline.yaml`, path thresholds, solar override gates, or
mode-inference calibration as normal setup. Use `data/config/README.md` for the
ownership class before editing a YAML file.

Keep real callsigns, peer hosts/IPs, passwords, and service tokens out of the
public example config and out of shared archives.

At minimum, replace the public placeholder identity before connecting a real
node: change `server.node_id` in `app.yaml` from `N0CALL-1`, change the RBN
login callsigns in `ingest.yaml` from `N0CALL-1`, and update any private
upstream telnet `host` and login fields you enable. If peering is enabled,
also replace peer hosts, login callsigns, and passwords in `peering.yaml`.

## Run On Windows

From the extracted `ready_to_run` directory:

```pwsh
$env:DXC_CONFIG_PATH = "data/config.local"
.\gocluster.exe
```

From a source checkout:

```pwsh
$env:DXC_CONFIG_PATH = "data/config.local"
go run .
```

To compile from source on Windows:

```pwsh
go test ./...
go build -trimpath -o gocluster.exe .
```

## Run On Linux

Build from the repository root with Go `1.26+`:

```sh
go test ./...
GOOS=linux GOARCH=amd64 go build -trimpath -o gocluster .
```

Install the binary and the required runtime data together, for example under
`/opt/gocluster`. Keep a complete private config directory at a stable path
such as `/opt/gocluster/data/config.local`.

Runtime data commonly needed beside the binary includes `data/cty`, `data/h3`,
`data/peers/topology.db`, and `data/skm_correction/rbnskew.json` when those
inputs are used by your config.

For unattended service operation, set `ui.mode: headless` in the private
`app.yaml`.

Create the service account, install directory, binary, config, and runtime
data, then assign ownership to the service user:

```sh
sudo useradd -r -s /bin/false gocluster
sudo mkdir -p /opt/gocluster
sudo cp gocluster /opt/gocluster/
sudo cp -R data /opt/gocluster/
sudo chown -R gocluster:gocluster /opt/gocluster
```

Save this unit file as `/etc/systemd/system/gocluster.service`:

```ini
[Unit]
Description=GoCluster DX Cluster
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=gocluster
Group=gocluster
WorkingDirectory=/opt/gocluster
Environment=DXC_CONFIG_PATH=/opt/gocluster/data/config.local
ExecStart=/opt/gocluster/gocluster
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

Enable and inspect the service:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now gocluster
sudo systemctl status gocluster
journalctl -u gocluster -f
```

The interactive local console requires the process to run in a real terminal.
For console inspection, stop the service, edit `app.yaml` in the private config
directory, change `ui.mode` to `tview-v2`, then run the binary manually:

```sh
sudo systemctl stop gocluster
cd /opt/gocluster
DXC_CONFIG_PATH=/opt/gocluster/data/config.local ./gocluster
```

On the Overview page, the Caches & Data Freshness footer includes CTY, FCC, and
skew dates plus `VOACAP SSN: <integer|n/a>`. The integer is the rounded current
SSN generation used for VOACAP forecast cache keys and deck generation; `n/a`
means the runtime has no initialized VOACAP SSN generation.
The Path Predictions panel also shows the same fallback snapshot as
`VOACAP cache: <cache> (C) / <delay> (D) / <inflight> (I) / <queue> (Q)`
before the H3 path-pair counts. These are the existing in-memory fallback cache
entries, delayed lookups, inflight jobs, and queued jobs from the current
process.

After inspection, set `ui.mode` back to `headless` before returning to
unattended service mode.

## Connect And Use Commands

Connect to the configured telnet port from `runtime.yaml`:

```text
telnet localhost 8300
```

Log in with your callsign. Useful first commands:

- `HELP`: show the command list.
- `HELP <command>`: show command-specific help.
- `SHOW MYDX` or `SHOW DX`: show filtered spot history.
- `SHOW DXCC <call>`: look up DXCC/ADIF and zones.
- `SHOW PROP <call|prefix|grid> [band] [mode]`: show hourly
  propagation outlook from your grid to a target.
- `SHOW OWN`: show your login call and baseline own call.
- `WHOSPOTSME [band]`: show recent spotter countries for your baseline call.
- `SET GRID <grid>`: set your 4-6 character Maidenhead grid.
- `SET NOISE QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL`: set receive noise class.
- `SET PATHSAMPLES <count|DEFAULT>`: require more path samples than the cluster default, or clear your personal override.
- `SET DIAG OFF|DEDUPE|SOURCE|CONF|PATH|MODE`: replace spot comments with compact per-session diagnostics.
- `SET SOLAR 15|30|60|OFF`: opt into or stop periodic solar summaries.
- `DIALECT`, `DIALECT LIST`, `DIALECT <go|cc>`: show or switch command dialect.
- `SHOW FILTER`: display active filters.
- `PASS <type> <list>`: allow matching spots.
- `REJECT <type> <list>`: block matching spots.
- `RESET FILTER`: restore default filters.
- `PASS NEARBY ON|OFF`: toggle nearby local-area filtering.
- `SHOW DEDUPE`: show your dedupe policy.
- `SET DEDUPE FAST|MED|SLOW`: change your dedupe policy.
- `DX <freq> <call> <comment>`: post a local human spot.
- `BYE`: disconnect.

The top-level repository README contains the generated default `HELP` output.
That block is checked against the command processor in tests.

Numeric SSIDs on the spotted DX call are removed regardless of ingest source.
For example, a new DX call of `K1ABC-2` is materialized and displayed as
`K1ABC`. Telnet login identity remains the full login call, while manual spots,
`SHOW OWN`, self-spot matching, and `WHOSPOTSME` use the baseline call when a
login has a numeric SSID. Existing archive files are not rewritten; old stored
rows can still contain a numeric SSID.

`SET DIAG MODE` is useful when the displayed mode is surprising. It shows
`<mode>|<provenance>`, where blank modes are shown as `--`. Provenance tokens
are `SRC` source explicit, `CMT` comment explicit, `EVD` recent evidence, `FQ`
digital frequency, `RCW` regional CW default, `RVO` regional voice default,
`RMIX` regional mixed blank, `RUNK` regional unknown blank, and `UNK` unknown.

### Reading `SET DIAG PATH`

`SET DIAG PATH` replaces the normal spot comment with the path-reliability data
used for that spot. The mode/report and fixed tail columns are preserved.

The compact format omits the path class glyph because the normal path
column already shows it when path display is enabled:

```text
n<count>|w<weight>|a<age>
```

When receiver contribution caps reduce the diagnostic evidence, raw selected
count is shown first, capped effective count is shown after `/c`, and live
attributed receivers are shown after `/rx`:

```text
n<raw>/c<capped>/rx<receivers>|w<weight>|a<age>
```

Insufficient evidence is shown as:

```text
n<count>|<reason>
n<count>|<reason>|v<voacap-reason>
```

VOACAP fallback results are shown as:

```text
vcap|<snr>|h<hour>|s<ssn>
valn|<p50>/<snr>h<hour>s<ssn>
vup|<p50>/<snr>r<rel>s<ssn>
vop|<snr>r<rel>h<hour>s<ssn>
```

Beacon RX-only decisions add `brx|` to bucket diagnostics. Beacon VOACAP
fallback uses the same compact shapes with `bvcap`, `bvaln`, `bvup`, or `bvop`
prefixes.

- `n<count>` is the raw selected observation count behind the displayed path
  decision in every receiver-cap mode. It is a sample-size clue, not a
  confidence percent.
- `n<raw>/c<capped>/rx<receivers>` means receiver contribution caps reduced
  the diagnostic evidence. The raw count is the sample floor; the capped count,
  receiver count, and capped weight explain receiver-cap trust evidence.
- `w<weight>` is the rounded effective weight after decay and path selection.
  Fine and coarse are overlapping evidence layers, so blended scalar weight uses
  the larger layer instead of adding both. It is not SNR or dB. Weight is an
  evidence-strength gate, not the displayed path class itself.
- `a<age>` is the effective age of the selected evidence. Bare numbers are
  seconds; `m` and `h` mean minutes and hours. Blended fine/coarse age uses
  local fine mass plus the coarse regional complement; old local evidence can
  make a direction stale even when coarse regional evidence was recently
  refreshed.
- `vcap|<snr>|h<hour>|s<ssn>` means the optional VOACAP closed fallback
  supplied the result. `<snr>` is the selected hour's rounded bidirectional
  FT8-equivalent SNR after receive-side noise penalty, `h<hour>` is the
  selected UTC forecast hour, and `<ssn>` is the rounded EWMA SSN generation
  used for the run. `PASS/REJECT PATH CLOSED` targets these closed fallback
  spots; `UNLIKELY` PATH filters still include them for compatibility.
  The runtime SSN monitor persists NOAA validators, the last observation, EWMA,
  and the current rounded SSN generation at
  `voacap_fallback.ssn_state_path`; a restart can reuse that SSN baseline when
  the state file is present. Completed VOACAP hourly forecast windows persist
  in the per-node Pebble cache at `voacap_fallback.forecast_cache_db_path`.
  On restart, records that still match the current cache schema, model
  generation, rounded SSN generation, forecast month, TTL, and current UTC hour
  hydrate the memory cache before workers start, so they bypass
  `voacap_fallback.delay_seconds`. Stale or malformed cache records are pruned;
  a missing/unavailable cache cold-starts normal delay/queue behavior.
- `valn|<p50>/<snr>h<hour>s<ssn>` means sparse bucket p50 evidence was
  insufficient by sample gates but aligned with the current-hour VOACAP class.
  `<p50>` is rounded for display so the diagnostic fits the fixed-width line.
- `vup|<p50>/<snr>r<rel>s<ssn>` means sparse p50 was insufficient, VOACAP
  mapped one class stronger, and the VOACAP request-SNR REL gate passed. REL is
  shown as percent and is not a direct HIGH/MEDIUM/LOW probability.
- `vop|<snr>r<rel>h<hour>s<ssn>` means there was no sparse p50, but cached
  current-hour VOACAP mapped to an open class and passed the REL gate.
- `brx|...` means the spot was marked as a beacon and the path decision used
  only the DX-to-user receive leg. `bvcap`, `bvaln`, `bvup`, and `bvop` are the
  equivalent beacon VOACAP fallback diagnostics; their SNR and REL fields are
  receive-leg values, not bidirectional effective values.
- `none` means no usable selected sample existed.
- `lown` means selected samples existed, but their observation count was below
  the configured minimum.
- `lowr` means raw selected observations met the count floor, but receiver
  diversity was below the derived receiver gate.
- `loww` means selected samples existed, but their effective weight was below
  the configured minimum.
- `stale` means selected samples existed, but the selected evidence was too old
  for the band's freshness gate.
- `v*` suffixes on insufficient diagnostics explain VOACAP state for sparse or
  no-p50 candidates: `vq` queued, `vdly` delayed, `vinf` inflight, `vband`
  unsupported band, `vnbnd` empty/unknown band, `vugrd` invalid user grid,
  `vdgrd` invalid DX grid, `vucel` invalid user cell, `vdcel` invalid DX cell,
  `vbad` other invalid request, `vssn` SSN unavailable, `vcur` no current-hour
  cache record, `vqf` queue full, `vnr` worker not running, `vdis` disabled,
  `vun` unavailable, `vrel` open forecast blocked by REL or tier guards, `vnc`
  usable forecast that did not classify closed, and `vhit` ready cache hit with
  no emitted fallback.

The five-minute `Path predictions (5m)` propagation log uses the same reason
split: `no_sample`, `low_count`, `low_receiver`, `low_weight`, and `stale`.
`low_count` is the raw observation-count gate; `low_receiver` is the
receiver-diversity gate; `low_weight` is the decayed effective-weight gate; and
`stale` can increase when fine/coarse age drops an old local direction before
receive/transmit merge.
VOACAP fallback outcomes are counted separately as `voacap_closed`,
`voacap_aligned`, `voacap_sparse_upgrade`, and `voacap_open`.
Beacon spots add `beacon_rx`, `beacon_rx_insufficient`,
`beacon_rx_<reason>`, and `beacon_rx_voacap_*` counters to the same final
emission line.
Native 160m darkness fallback emissions add `native160_low` and
`native160_unlikely` to the same line. These are conservative LOW/UNLIKELY
fills for insufficient 160m p50 when no usable current-hour VOACAP result has
precedence.

When the optional VOACAP fallback has activity, a separate
`VOACAP fallback (5m)` propagation log line explains the stage path:
`queued`, `success`, `failure`, `cache_hit`, `no_current_hour`, `delay_wait`,
`inflight`, `queue_full`, `not_running`, `ssn_unavailable`,
`invalid_request`, split invalid-request reasons (`invalid_unsupported_band`,
`invalid_empty_unknown_band`, `invalid_user_grid`, `invalid_dx_grid`,
`invalid_user_cell`, `invalid_dx_cell`), `closed`, `closed_no_p50`,
`closed_with_sparse_p50`, `closed_with_sparse_p50_class_*`, `aligned`,
`open_no_p50`, and `class_mismatch`, plus the REL-gated counters
`sparse_upgrade`, `open_no_p50_rel`, `rel_missing`, `rel_below_floor`, and
`rel_multi_tier`.
Use `Path predictions (5m)` to count final emitted glyphs.
Use `VOACAP fallback (5m)` to explain why a fallback lookup did or did not
emit.
Runtime VOACAP fallback decks select Method 20 below 7000 km and Method 30 at
and above 7000 km using the same Maidenhead grid-center endpoints written to
the VOACAP circuit. Cached records still reuse the existing fine path-cell
granularity, so near-threshold method reuse follows the same res-2 cache
boundary as other VOACAP fallback data.
When sparse or no-p50 candidates are present, a separate `Sparse p50 VOACAP
(5m)` line splits those candidates by p50 evidence (`no_p50`,
`very_low_count`), path kind (`beacon_rx`, `non_beacon`), cache/work state
(`cache_miss_total`, `cache_hit`, `queued`, `delayed`, `inflight`,
`invalid_request`, split invalid-request reasons, `ssn_unavailable`,
`no_current_hour`, `queue_full`, `not_running`, `disabled`, `unavailable`), and
outcome (`closed`, `aligned`, `sparse_upgrade`, `open_rel_pass`,
`open_rel_fail`, `not_closed`, `rel_missing`, `rel_below_floor`,
`rel_multi_tier`). It is diagnostic only; it does not change glyph decisions.
When native 160m fallback evaluates candidates, `Native 160m fallback (5m)`
reports `candidates`, `emitted`, class splits, `not_dark`, `unknown`,
`display_disabled`, and civil-darkness buckets `dark_ge_50`, `dark_ge_75`, and
`dark_ge_90`.
When sufficient p50 predictions can be compared against an existing current-hour
VOACAP cache record, a separate `VOACAP p50 compare (5m)` line reports cache
hits, cache misses, class agreement, stronger/weaker effective SNR, closed
VOACAP versus p50 class, and absolute SNR-delta buckets. The comparison is
cache-only: cache misses do not run VOACAP, start delay windows, or change
glyphs.
The shipped config writes these aggregate lines to `data/logs/propagation`,
not the system log.

The fixed-width cluster format may clip the right edge of a long diagnostic
comment to keep the grid, confidence, and time columns aligned. The leftmost
fields remain the important ones: count and effective weight or reason for
bucket results, and SNR plus selected UTC hour for VOACAP fallback results.

Example readings:

- `n18|w7`: 18 selected observations, rounded effective weight 7.
- `n0|none`: no usable selected sample.
- `n3|lown`: three selected observations existed, but not enough to emit a
  path class.
- `n0|none|vdly`: no usable selected path sample, and VOACAP is still in its
  configured delay window.
- `n2|lown|vrel`: very sparse p50 existed, VOACAP had a usable open forecast,
  but the REL or one-tier guard blocked an open fallback glyph.
- `n19/c5/rx1|lowr`: nineteen raw observations existed, but only one
  attributed receiver contributed capped evidence.
- `n19/c5/rx1|w3`: receiver caps reduced diagnostic evidence; raw count is
  shown first, capped effective count after `/c`, and attributed receiver count
  after `/rx`.
- `vcap|-34|h20|s112`: VOACAP fallback selected the 20:00 UTC forecast record,
  blended both directions, applied the user's receive noise penalty, rounded the
  effective FT8-equivalent SNR to -34, and used SSN generation 112.
- `valn|-15/-15h20s112`: sparse bucket p50 rounded to -15 dB and the 20:00
  UTC VOACAP forecast also mapped to that same path class.
- `vup|-19/-15r84s112`: sparse p50 rounded to -19 dB, VOACAP rounded to
  -15 dB, and VOACAP REL 84% passed the one-tier upgrade gate.
- `vop|-19r75h20s112`: no sparse p50 existed, but the 20:00 UTC VOACAP record
  rounded to -19 dB and REL 75% passed the open fallback gate.
- `n160|d82`: native 160m fallback filled an insufficient 160m result using an
  82% civil-dark path fraction. Beacon receive-only paths use `bn160|d82`.
- `n1|loww`: one selected observation existed, but the effective weight was
  below the minimum.
- `n32|w1`: large selected count but low rounded effective weight.

### Reading `SHOW PROP`

`SHOW PROP <call|prefix|grid> [band] [mode]` exposes the same rolling VOACAP
forecast window used by the fallback. Omitted mode defaults to CW. If an
explicit single-band request has no rows, or fewer rows than
`voacap_fallback.forecast_hours`, the command starts a refresh through the
existing fallback worker and waits briefly. All-band requests show cached rows
immediately while refreshing missing or partial bands in the background.

The target can be an explicit Maidenhead grid, a callsign found in the grid
store, or a CTY-derived prefix/callsign center. With no band, the command
queries all configured VOACAP fallback bands. With no mode, it uses `CW`.
Rows run from the current UTC hour through the configured
`voacap_fallback.forecast_hours` cache horizon, but only rows whose `REL`
prediction is `HIGH`, `MEDIUM`, or `LOW` are displayed.

```text
PROP FN31 -> JM77 target=IT9 source=cty-derived mode=FT8 band=20m noise=SUBURBAN ssn=112 hours=8
UTC  EFF  RX  TX  REL
18Z  <    -   <   LOW
```

- `EFF` is the merged bidirectional effective-path glyph.
- `RX` is the target-to-user receive-leg glyph after the user's `SET NOISE` penalty.
- `TX` is the user-to-target transmit-leg glyph.
- `REL` is the configured path class for the requested mode and merged path.
- Rows whose `REL` prediction is `UNLIKELY` or `CLOSED` are hidden. If every
  cached row is hidden, the command reports that there are no
  `HIGH`/`MEDIUM`/`LOW` rows in the current forecast window.
- Bucket p50 is intentionally not shown; sufficient bucket p50 remains
  authoritative for live spot glyphs.

## Logs And Health

System logs, propagation logs, optional dropped-call logs, and file-only event
logs are configured in `app.yaml`. Runtime file logs keep a stable active
filename derived from the configured directory name, such as `system.log` or
`propagation.log`, and completed UTC days archive as `DD-Mon-YYYY.log`.
Propagation logs live under `logging.propagation.dir`; they contain the path
prediction aggregates used by the daily propagation report. The file-only event
logs cover login attempt failures, reputation-gated spot drops, telnet client
lifecycle, ingest source lifecycle, and peer lifecycle. They do not add local UI
or console panes.
Under `systemd`, stdout/stderr also go to journald and can be tailed with:

```sh
journalctl -u gocluster -f
```

Common startup failures are usually config-path or config-content issues:

- `DXC_CONFIG_PATH` must point at a complete config directory, not one YAML file.
- Unknown YAML files fail startup. Extra YAML keys are logged as config
  warnings and ignored, except known removed migration keys, which still fail
  startup with a migration hint.
- Required startup YAML files, YAML-owned settings, and reference tables must
  be present. The loader reports all missing required files and settings it can
  find before aborting startup.
- When path reliability is enabled, `data.h3_table_path` must contain valid
  `res1.bin` and `res2.bin` H3 tables. Missing or malformed H3 tables fail
  startup because path predictions depend on those cells.
- Gridstore startup open failures are written to the system log. Corruption
  starts checkpoint recovery and runs temporarily without persistence; other
  open failures abort startup.
- The default config directory is `data/config` when `DXC_CONFIG_PATH` is not set.

For config loader details, see `data/config/README.md`.
