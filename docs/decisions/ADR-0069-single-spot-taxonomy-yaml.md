# ADR-0069: Single Spot Taxonomy YAML

Status: Accepted

Date: 2026-04-22

Decision Origin: Design

## Context

Mode and EVENT behavior had drifted across compiled parser tables, filter/help lists, PSKReporter mode lists in `ingest.yaml`, mode inference seeds, path reliability mode classes, FT handling, archive retention, and operator docs. That made adding a mode or EVENT family require code edits in several unrelated packages and created opportunities for partial support.

## Decision

`data/config/spot_taxonomy.yaml` is the required reference table for cluster-supported modes and EVENT families. Startup loads one immutable taxonomy snapshot before filter defaults, user filter normalization, mode inference, PSKReporter construction, and feed startup.

The taxonomy owns:

- mode parser tokens and PSK variants
- filter-visible MODE and EVENT names
- default MODE selection and CC mode shortcut eligibility
- EVENT standalone tokens and acronym-prefixed reference prefixes
- PSKReporter route (`normal`, `path_only`, `ignore`)
- existing mode capability flags such as FT dial canonicalization, FT confidence timing keys, report formatting, path-reliability ingest, confidence-filter exemption, call-correction profile, source-skew correction, frequency averaging, and custom SCP bucket

Algorithm families remain code-owned. YAML can opt a mode into an existing capability class, but it cannot define new decoding algorithms, transports, distance models, or EVENT grammars beyond the supported token and prefix matchers.

## Alternatives Considered

- Keep PSKReporter mode lists in `ingest.yaml` and add EVENTs only in code. This keeps the change smaller but preserves split ownership.
- Add separate `modes.yaml` and `events.yaml`. This is clearer at first but still leaves operators with two taxonomy surfaces.
- Fully dynamic algorithms in YAML. This was rejected because it would make hot-path behavior and validation less deterministic.

## Consequences

- New binaries require `spot_taxonomy.yaml`; old binaries must not be restarted against a config directory containing the new required file.
- `ingest.yaml` no longer accepts `pskreporter.modes` or `pskreporter.path_only_modes`.
- Adding a supported mode or EVENT family requires editing `spot_taxonomy.yaml` and restarting with the matching binary/config directory.
- Live fan-out remains allocation-free for EVENT checks and does not parse comments.
- Parser lookup structures and mode/event capability maps are built once at startup and are immutable for the process lifetime.

## Links

- `data/config/spot_taxonomy.yaml`
- `spot/taxonomy.go`
- `docs/decisions/ADR-0068-event-comment-tags-and-filter-contract.md`

## Supersession Notes

- ADR-0149 removes archive retention class ownership from spot taxonomy. Archive
  retention is now one archive-level window, not a per-mode taxonomy capability.
