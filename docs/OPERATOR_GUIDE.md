# Operator Guide (One Page)

## Purpose
This server aggregates real-time spotting data and publishes a single per-band propagation glyph for each path. Think of the glyph as a quick operational hint, not a guarantee.

## Quick Start
- Run the server from the repo root: `go run .`
- Default config directory: `data/config`
- Main config files:
  - `data/config/ingest.yaml` (spot sources)
  - `data/config/path_reliability.yaml` (glyph thresholds, decay, noise)
  - `data/config/solarweather.yaml` (optional R/G overrides)
  - `data/config/peering.yaml` (optional cluster peering)

## Operator Commands (telnet)
- `SET GRID <grid>`: set your location (4–6 chars).
- `SET NOISE <QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL>`: adjusts local noise penalty.
- `SET SOLAR 15|30|60|OFF`: opt‑in cadence for solar summaries (wall‑clock aligned).

## What the Glyphs Mean
- **High / Medium / Low / Unlikely**: chance of a usable path *right now*, based on recent spots and decay.
- **Insufficient**: we just don’t have enough recent evidence to rate the path.
- **R**: radio‑blackout override — strong flare/X‑ray activity on a **sunlit** path.
- **G**: geomagnetic‑storm override — high Kp on a **high‑latitude** path.
- Overrides are rare and band‑specific; they only show up when the path geometry makes them relevant.

## How Location Is Used
- We start with your 4‑digit Maidenhead grid and convert it to the center lat/lon (a 2° × 1° square).
- That point maps to **H3 res‑2** (local) and **res‑1** (regional) cells so local + regional evidence can be blended.
- H3 cells are a global hex‑grid index—a consistent way to bucket nearby locations.
- If H3 tables are missing or grids are invalid, the path will show **Insufficient**.

## Health Signals (logs)
Every 5 minutes, the server logs:
- `Path predictions (5m)` — combined vs insufficient, and no‑sample vs low‑weight.
- `Path buckets (5m)` — per‑band bucket counts.
- `Path weight dist (5m)` — per‑band weight histogram.
Stats ticker adds:
- `Data` — last updated timestamps for CTY and FCC ULS.
- `Calls` — correction/unlicensed/frequency/harmonic counts plus reputation drops `(R)`.
- `Calls` decision reasons (resolver-primary) now include conservative recent corroboration labels:
  - applied: `resolver_applied_recent_plus1`, `resolver_applied_neighbor_recent_plus1`
  - rejected: `resolver_recent_plus1_reject_*` (distance/family, winner recent insufficient, subject not weaker, contested neighbor)
- `Stabilizer` — telnet delay counters `(H/I/D/S/O)` when call-correction stabilizer is enabled: held, immediate, delayed, suppressed, overflow-release. Reason tuples use `u/a/p/e` for `unknown_or_nonrecent`, `ambiguous_resolver`, `p_low_confidence`, and `edit_neighbor_contested`. Suppressed delayed spots do not update recent-on-band support.
- `Path only` — per‑interval path‑only updates with drop reasons for WSPR (U=updated, S=stale, N=no SNR, G=no grid, H=bad H3, B=bad band, M=mode).

## Common Troubleshooting
- **All paths show Insufficient**: verify `SET GRID`, confirm spot ingestion, and check H3 tables in `data/h3`.
- **No R/G overrides**: check `solarweather.yaml` enabled flag, feed freshness, and that the band is eligible.
- **Over‑aggressive overrides**: tighten thresholds or band lists in `solarweather.yaml`.

## FCC ULS Allowlist
- Optional regex allowlist file is configured in `data/config/data.yaml` under `fcc_uls.allowlist_path`.
- Patterns are matched against the normalized base callsign (SSID/slash stripped).
- Prefix a line with `US:` or `ADIF291:` to target a jurisdiction; lines without a prefix default to `US`.
- Calls with **3+ leading letters before the first digit** are dropped by CTY gating unless allowlisted.

## Data Sources (when overrides enabled)
- GOES X‑ray (corrected 0.1–0.8 nm) for R levels.
- Observed 3‑hour Kp for G levels and auroral boundary gating.

## Operator Principles
- Treat glyphs as **probabilistic hints**.
- Trust **Insufficient** rather than forcing a decision.
- R/G overrides are **rare by design** — they signal strong, path‑relevant space weather.
