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

- `app.yaml`: server node ID, console/headless mode, and logging paths.
- `runtime.yaml`: telnet port, default filters, buffers, and memory controls.
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
directory, change `ui.mode` to `ansi` or `tview-v2`, then run the binary
manually:

```sh
sudo systemctl stop gocluster
cd /opt/gocluster
DXC_CONFIG_PATH=/opt/gocluster/data/config.local ./gocluster
```

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
- `SHOW OWN`: show your login call and baseline own call.
- `WHOSPOTSME [band]`: show recent spotter countries for your baseline call.
- `SET GRID <grid>`: set your 4-6 character Maidenhead grid.
- `SET NOISE QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL`: set receive noise class.
- `SET PATHSAMPLES <count|DEFAULT>`: require more path samples than the cluster default, or clear your personal override.
- `SET DIAG OFF|DEDUPE|SOURCE|CONF|PATH|PATHP50|MODE`: replace spot comments with compact per-session diagnostics.
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

When receiver contribution caps reduce the diagnostic evidence, capped count is
shown first and raw selected count is shown after `/r`:

```text
n<capped>/r<raw>|w<weight>|a<age>
```

Insufficient evidence is shown as:

```text
n<count>|<reason>
```

- `n<count>` is the raw number of selected observations behind the displayed
  path decision. It is a sample-size clue, not a confidence percent.
- `n<capped>/r<raw>` means receiver contribution caps reduced the diagnostic
  evidence. In the shipped `shadow` mode this is informational; in `enforce`
  mode the capped count and capped weight gate the path class.
- `w<weight>` is the rounded effective weight after decay and path selection.
  It is not SNR or dB. Weight is an evidence-strength gate, not the displayed
  path class itself.
- `a<age>` is the effective age of the selected evidence. Bare numbers are
  seconds; `m` and `h` mean minutes and hours.
- `none` means no usable selected sample existed.
- `lown` means selected samples existed, but their raw observation count was
  below the configured minimum.
- `loww` means selected samples existed, but their effective weight was below
  the configured minimum.
- `stale` means selected samples existed, but the selected evidence was too old
  for the band's freshness gate.

The five-minute `Path predictions (5m)` propagation log uses the same reason
split: `no_sample`, `low_count`, `low_weight`, and `stale`. `low_count` is the
observation-count gate; `low_weight` is the decayed effective-weight gate. The
shipped config writes these aggregate lines to `data/logs/propagation`, not the
system log.

The fixed-width cluster format may clip the right edge of a long diagnostic
comment to keep the grid, confidence, and time columns aligned. The leftmost
fields remain the important ones: count and effective weight or reason.

Example readings:

- `n18|w7`: 18 selected raw observations, rounded effective weight 7.
- `n0|none`: no usable selected sample.
- `n3|lown`: three selected observations existed, but not enough to emit a
  path class.
- `n5/r19|lown`: nineteen raw observations existed, but capped receiver
  evidence would not meet the minimum sample floor.
- `n5/r19|w3`: capped receiver evidence is shown because one or more receivers
  hit a contribution cap.
- `n1|loww`: one selected observation existed, but the effective weight was
  below the minimum.
- `n32|w1`: large raw sample count but low rounded effective weight.

### Reading `SET DIAG PATHP50`

`SET DIAG PATHP50` is a second path diagnostic view. It does not change glyphs
or PATH filters. The normal path glyph is still shown in the fixed tail column.
The diagnostic comment shows p50 SNR and the mean-minus-p50 delta so operators
can see how the distribution method compares with the current method.

```text
p<db>d<delta>n<count>
```

- `p<db>` is the shadow p50 SNR bin lower edge in FT8-equivalent dB. It uses
  fixed 1 dB bins, so it is a compact bin value rather than an exact raw report.
- `d<delta>` is active mean SNR minus p50 SNR. Positive `d` means the active
  mean is stronger than p50; negative `d` means p50 is stronger than the active
  mean.
- `n<count>` is the compact selected observation count for this prediction.
  PATHP50 omits the longer `n<capped>/r<raw>` form; use `SET DIAG PATH` when
  raw/capped detail matters.
- Positive values omit a plus sign to preserve comment space. Unavailable
  values are shown as `?`.

When operators are actively using `SET DIAG PATHP50`, the propagation log can
also include `Path p50 diag (5m)` aggregates. Those counters are
diagnostic-observed only: they summarize spots for which PATHP50 already
computed p50, and they do not mean the cluster computed p50 for every normal
spot.

The same diagnostic run can also write `Path p50 shadow` aggregate lines. These
compare the current mean-based glyph class with the p50 shadow glyph class and
bucket the differences by sample count, band, mode family, source, and glyph
pair. Use those lines to evaluate whether p50 is usually more conservative,
usually more optimistic, or only different in low-sample contexts. They are not
an enforcement switch.

Example readings:

- `p-15d4n19`: p50 lower edge is -15 dB, active mean is 4 dB stronger than
  p50, 19 selected observations.
- `p3d-2n42`: p50 lower edge is 3 dB, active mean is 2 dB weaker than p50,
  42 selected observations.
- `p?d?n0`: no p50 or delta is available.

## Logs And Health

System logs, propagation logs, optional dropped-call logs, and file-only event
logs are configured in `app.yaml`. Propagation logs are separate daily files
under `logging.propagation.dir`; they contain the path prediction aggregates
used by the daily propagation report. The file-only event logs are separate
daily files for login attempt failures, reputation-gated spot drops, telnet
client lifecycle, ingest source lifecycle, and peer lifecycle. They do not add
local UI or console panes.
Under `systemd`, stdout/stderr also go to journald and can be tailed with:

```sh
journalctl -u gocluster -f
```

Common startup failures are usually config-path or config-content issues:

- `DXC_CONFIG_PATH` must point at a complete config directory, not one YAML file.
- Unknown YAML files or unknown keys fail startup.
- Required YAML-owned settings and reference tables must be present.
- The default config directory is `data/config` when `DXC_CONFIG_PATH` is not set.

For config loader details, see `data/config/README.md`.
