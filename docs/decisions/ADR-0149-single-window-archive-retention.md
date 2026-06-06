# ADR-0149: Single-Window Archive Retention

- Status: Accepted
- Date: 2026-06-06
- Decision Origin: Design

## Context
Archive retention previously had separate FT and non-FT windows. That required
archive cleanup to inspect each expired candidate record, decode its mode, ask
the spot taxonomy for an archive retention class, and then choose a cutoff.

The operator decision is now to keep all archived spot modes for the same
duration. The default retention window is 86400 seconds.

## Decision
Use one archive-level retention setting:

- `archive.retention_seconds` applies to every archived spot mode;
- omitted or non-positive programmatic values normalize to 86400 seconds;
- removed YAML keys `archive.retention_ft_seconds` and
  `archive.retention_default_seconds` are rejected with a migration hint;
- `spot_taxonomy.yaml` no longer owns `archive_retention_class`;
- archive cleanup deletes by timestamp key only and no longer decodes mode for
  retention decisions.

## Alternatives Considered
1. Keep two YAML keys with equal shipped values.
   - Rejected because it preserves an obsolete distinction and keeps cleanup
     mode-decoding work alive.
2. Keep `archive_retention_class` in taxonomy for future flexibility.
   - Rejected because there is no current operational rationale for per-mode
     archive lifetime and stale knobs create support ambiguity.
3. Silently map old split keys to the new value.
   - Rejected because old configs with conflicting values would be ambiguous.
     Startup should fail clearly until the operator chooses one value.

## Consequences
### Benefits
- Archive retention behavior is easier to explain: one window for all modes.
- Cleanup avoids per-record mode decoding and taxonomy lookup.
- The taxonomy schema is narrower and no longer exposes an unused archive
  lifetime concept.

### Risks
- Existing private config directories using the removed split keys must be
  updated before startup.
- Old taxonomy files containing `archive_retention_class` fail strict decoding
  with the new binary.
- Archive storage can increase for deployments that previously used a shorter
  FT retention window.

### Operational Impact
- No archive record schema, telnet protocol, peer protocol, or spot parsing
  change.
- The shipped and local retained window is 86400 seconds.
- Existing archive rows are not rewritten; cleanup converges naturally by key
  timestamp under the new single cutoff.

## Links
- Related tests: `archive/archive_cleanup_test.go`,
  `config/archive_config_test.go`, `spot/taxonomy_test.go`
- Related config: `data/config/archive.yaml`,
  `data/config/spot_taxonomy.yaml`
- Related docs: `data/config/README.md`
- Supersedes / superseded by: partially supersedes the archive-retention-class
  portion of ADR-0069
