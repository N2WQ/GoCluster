# ADR-0254: Canadian ISED License and State Reuse

- Status: Accepted
- Date: 2026-10-07
- Decision Origin: Design

## Context

ADR-0253 added FCC license facts, registered-address State metadata, DESTATE and
DXSTATE filters, stored history, and compatible saved/machine configuration.
The user requested the full equivalent for Canada and explicitly selected reuse
of the existing State fields and commands. ISED publishes assigned individual
and sponsored-club callsigns separately from dated special-event callsigns and
temporary prefix substitutions. Its semicolon records contain literal quotes;
CSV quote interpretation can reject or merge valid records.

The user selected active special events, prefix substitutions as callsign
plausibility checks, inclusive UTC event dates, and registered-address province
rules. Scope Ledger v1 was authorized with `Approved v1`. This decision extends
the accepted FCC design rather than introducing a separate province interface.

## Decision

1. Download ISED's assigned and special-event ZIPs through the existing HTTP
   helper. Treat both inputs as one Canadian snapshot. Parse literal
   semicolon-delimited records with named headers and preserved empty fields;
   do not apply CSV quote semantics. Bound downloaded/extracted bytes, record
   length and row counts. Keep only callsign/province, dated event evidence and
   source provenance in the SQLite projection; omit names, full addresses,
   qualifications and event descriptions.
2. Publish an independently owned Canadian database only after both inputs
   parse and the complete projection commits. Record both archive SHA-256
   values in the completed database as the authority for the published source
   pair. HTTP sidecar success does not establish database publication. Retry a
   partially downloaded or unpublished pair even when subsequent requests
   return unchanged validators. Preserve the last successful database after a
   download, parse, build or replacement failure. Canadian refresh ownership,
   generation and handles remain separate from FCC ownership, so a Canadian
   refresh does not suppress FCC facts.
   Use unique extraction directories under `ised.temp_dir`; keep database
   scratch beside `ised.db_path` for atomic publication. The streaming Canadian
   builder requires no SQLite TEMP tables or sorts and does not alter SQLite's
   process-wide temporary-directory setting.
3. Evaluate event membership on inclusive UTC calendar dates. An exact active
   special call is assigned even when trustee province evidence is unavailable.
   For temporary substitutions, use the supported ISED prefix mapping and
   declared replacement evidence to require an assigned ordinary base call.
   This establishes callsign plausibility only; the export does not prove
   residency, club membership or other event eligibility. Unsupported or
   ambiguous membership evidence remains unknown and fails open. A healthy,
   complete snapshot can reject an absent callsign when enforcement is enabled.
4. Use club province when any club information is present; otherwise use the
   individual's province. Exact special calls use the listed trustee's address
   evidence. Substitutions use the ordinary base call's address evidence.
   Blank, invalid or conflicting province evidence remains unknown without
   discarding otherwise established assignment. Province is a registered
   address, not an inferred operating location.
5. Reuse factual lookup, scheduling, database replacement and bounded-cache
   machinery. Keep one aggregate 200,000-entry lookup cap with
   `fcc_uls.cache_ttl_seconds` as the shared TTL. Source and database generation
   distinguish FCC/ISED cache entries. Canadian entries additionally expire at
   UTC midnight. Unavailable answers are not retained as negative facts.
   Cancel and join each background refresh owner during shutdown. Offline
   tools register configured local snapshots and start no downloader. Replay
   spots are not enriched or license-gated, matching existing FCC replay behavior.
6. Add the required `ised` block to the existing canonical startup loader and
   `data/config/data.yaml`: enforcement, two HTTP(S) source URLs, two archive
   paths, database/temp paths, and `HH:MM` daily UTC refresh time. The public
   example enables enforcement and refreshes at `22:15`. Missing/null/invalid
   settings fail load; no omitted ISED runtime defaults are injected.
   `ised.enabled` controls rejection only, so explicit false still downloads
   and enriches province. Reuse the existing ADIF-qualified allowlist and
   shared lookup TTL instead of adding Canadian variants. Protect FCC/ISED
   archives, databases, metadata and the existing allowlist from path
   collisions; shared temp directories are permitted.
7. Route licensing by base identity, including cross-border portable calls.
   Canadian applicability covers Canada, Sable Island and St. Paul Island
   (ADIF 1, 211 and 252). Apply Canadian checks at central DE admission, final
   corrected DX admission and login; offline tools register both local snapshots
   with existing setup behavior. Preserve the existing US login jurisdiction
   coverage and admission exceptions.
8. Reuse each role's `CallMetadata.State`, DESTATE/DXSTATE commands,
   capabilities, readbacks and history filtering. Add the 13 Canadian province
   and territory codes to the existing 60-code shared vocabulary, for 73
   canonical codes. FCC imports retain their narrower FCC validator; ISED
   imports accept only Canadian provinces. Mixed selections such as
   `PASS DXSTATE NY,ON` retain category AND, selection OR, rejection precedence,
   unknown-value behavior and NEARBY restoration.
9. Retain archive record version 6, saved configuration version 2, and machine
   YAML schemas 1 and 2. Schema 1 keeps its existing projection and preserves
   hidden State rules; schema 2 exposes the enlarged finite State vocabulary.
   History uses stored State without hydration or backfill. A prior binary can
   reject Canadian codes despite recognizing the same version marker, so
   downgrade requires a matching profile, preset and archive backup.

## Alternatives considered

1. Add separate DE/DX province fields, filter commands and caches. Rejected:
   the selected contract explicitly reuses the existing State machinery.
2. Ingest ordinary assignments alone. Rejected: the user selected legitimate
   active special events and prefix substitutions in first-version scope.
3. Interpret prefix substitutions as proven event eligibility. Rejected:
   source data omits material eligibility restrictions; plausibility is the
   selected supportable contract.
4. Publish after either input refreshes or rely on HTTP sidecars as publication
   authority. Rejected: partially refreshed input could omit event membership,
   and unchanged validators must remain retryable after a failed build.
5. Combine FCC and Canadian database ownership or create a second lookup cache.
   Rejected: independent snapshot publication preserves FCC availability and
   the shared cache retains the accepted aggregate resource bound.
6. Bump otherwise unchanged disk/archive/YAML formats. Rejected: the existing
   State representation carries Canadian codes. Document the stricter
   vocabulary of older readers and require matching rollback backups.

## Consequences

### Benefits

- Canadian license plausibility, province filters and stored history use the
  established user interface and metadata path.
- One bounded lookup owner supplies license and registered-address facts.
- Failed Canadian refreshes preserve the published source pair and FCC lookup
  independence.

### Risks

- Registered province can differ from operating location, and prefix matching
  does not establish event eligibility.
- Snapshot freshness depends on successful acquisition. A last successful
  snapshot can be stale; downloaded timestamps do not establish an ISED
  publication guarantee.
- Old binaries and strict clients can reject Canadian codes even when their
  format version is unchanged. Restore matching backups for downgrade.

### Operational impact

- Existing private config directories must add every required ISED key before
  startup. Disabling rejection does not disable acquisition or enrichment.
- Operators retain distinct archive/database/sidecar paths for the two sources,
  and use the existing shared cache TTL and ADIF-qualified allowlist.
- Refresh/import diagnostics report source readiness, generation, known/unknown
  province, input failures and aggregate cache cardinality.
- Stop writers before backing up profiles, presets or archive data. Preserve
  the last successful reference database when troubleshooting refresh failures.

## Links

- Authorization: user `Approved v1` in the implementation conversation.
- Source: [ISED downloads](https://ised-isde.canada.ca/site/amateur-radio-operator-certificate-services/en/downloads),
  [ISED RIC-9 call sign policy](https://ised-isde.canada.ca/site/spectrum-management-telecommunications/en/licences-and-certificates/radiocom-information-circulars-ric/ric-9-call-sign-policy-and-special-event-prefixes).
- Related tests: [required ISED configuration](../../config/ised_config_test.go),
  [bounded HTTP download](../../download/download_test.go),
  [State filtering](../../filter/state_test.go),
  [archive State](../../archive/state_test.go).
- Related docs: [configuration](../../data/config/README.md),
  [domain contract](../domain-contract.md),
  [telnet interface](../../telnet/README.md).
- Related TSRs: none; this is a feature design decision.
- Refines [ADR-0253](ADR-0253-fcc-state-enrichment-and-filtering.md) for Canadian
  source ownership, shared State vocabulary and older-reader compatibility.
  ADR-0253 and its ADR-0244/0245/0251 dependencies otherwise remain accepted.
