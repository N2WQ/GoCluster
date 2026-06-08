# Support Card: DXSummit Connected With No Startup Spots

## Match

Use when a node operator sees DXSummit connected but no startup spots, no rows
after source start, or dashboard/source visibility that looks quiet.

## First Safe Check

Check `dxsummit.startup_backfill_seconds` and distinguish seed-only startup from
live spot emission.

## Must Include

- `startup_backfill_seconds` controls whether startup history is emitted.
- Seed-only startup can establish the high-water cursor without emitting
  historical rows.
- A connected source with no startup spots is not automatically an ingest
  failure.

## Must Avoid

- Do not call seed-only startup an ingest failure.
- Do not assume all sources emit user-visible spots immediately after startup.

## Sources

- `customgpt/troubleshooting-index.md`
- `dxsummit/README.md`
- `data/config/README.md`
- `docs/decisions/ADR-0066-dxsummit-http-ingest.md`
