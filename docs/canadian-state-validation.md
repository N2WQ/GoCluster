# Canadian ISED License and State Validation

The change implements the user's `Approved v1` scope, reusing the existing
State fields, gates, downloader, SQLite publication, bounded cache, filters and
history. [ADR-0254](decisions/ADR-0254-canadian-ised-license-and-state-reuse.md)
extends the [FCC decision](decisions/ADR-0253-fcc-state-enrichment-and-filtering.md).

## Scope and Falsifiable Evidence

| Approved contract | Implementation | Evidence |
| --- | --- | --- |
| Download both official ISED archives and parse their actual format | `download/download.go`, `uls/extract.go`, `uls/ised_import.go` | Literal 18/7-field fixtures with empty fields, bare/leading quotes, UTF-8/CRLF/BOM; header reordering, missing/duplicate headers, invalid framing/encoding, truncation and byte/line/row bounds; downloaded source histogram below |
| Minimal call/province projection and selected address rules | `uls/ised_import.go` | All 13 literal provinces, club/personal precedence, blank/invalid province without membership loss, order-independent sticky duplicate conflicts; malformed event prose is discarded with uncertainty retained |
| Active exact calls, assigned-base prefix plausibility, inclusive UTC dates | `uls/ised_lookup.go` | Exact trustee/no-trustee and conflicts; UTC first/last/midnight boundaries; all 20 literal RIC-9 prefix pairs across digits 0–9, 400 indexed national/area lookups; unsupported mappings/use-by and reversed dates remain unknown |
| One completed pair, last-good retention and reliable retries | `uls/ised_refresh.go`, shared replacement/extraction | Partial second-download failure followed by both 304s, startup/restart reconciliation, malformed unchanged retry, failed swap and later retry, metadata-write failure, stale readable sidecar plus upstream rollback through 304 and 200 paths; direct path-collision refusal before HTTP |
| Independent sources, bounded cache and lifecycle | `uls/license_check.go`, `uls/cache.go`, runtime setup/close | Namespace collision, aggregate small-cap churn and slot coupling; source reset isolation, factual generation barrier, positive/negative midnight requery and actual cold query crossing midnight; finite pool-wait cancellation, detached-query diagnostics; blocked HTTP cancellation, owner-wait cancellation and deterministic join of both workers; configured shared extraction children and failure cleanup; actual SQLite temporary-directory setting remains unchanged by Canadian refresh |
| Central DE and final corrected DX, including delayed delivery | `internal/cluster/ingest_validation.go`, `bootstrap.go` | Canada/Sable/St. Paul consumers, cross-border slash orders, enforcement-off enrichment, on rejection, TEST/beacon and qualified allowlist exceptions; actual resolver output through immediate/delayed broadcast and decoded archive |
| Base-identity Canadian login with unchanged US entity coverage | `telnet/login_validation.go`, `server.go` | Both slash orders across the border, Canadian entities 1/211/252, mainland US versus FCC territories, TEST/allowlist/disabled/outage cases |
| Reuse State73 and existing persistence/history formats | `spot/state.go`, filter/archive/telnet consumers | Independent 60+13 vocabulary, mixed selections and unknown truth cases; 72/73/74 stored-map bounds and false entries; literal archive v6 province bytes and all 13 codes; YAML1 hidden preservation, YAML2 capabilities/readback/framing, NEARBY restoration; recorded history remains unchanged after replacing current provinces |
| Required explicit ISED config and safe source paths | `config/config.go`, `data/config/data.yaml` | Missing/null/value/URL/time validation, disabled setting preservation, startup readback, archive/DB/sidecar/allowlist overlaps including symlink/hardlink aliases and refusal of dangling managed links before any metadata write |
| Offline setup parity | replay/evaluator dependency setup | Sequential replay runs read each configured local snapshot, clear missing optional data, require an enabled source's DB and make zero HTTP requests. Replay spots receive no new admission or enrichment, matching existing FCC replay behavior |
| Operator/support guidance and durable decision | READMEs, operator/domain contracts, support routes/card, ADR-0254/index | Reviewed examples, required private-config migration, recorded-history semantics, unchanged archive6/disk2/YAML1/2 layouts and matching-backup downgrade guidance |

The parser, event and retry oracles were challenged before implementation and
reviewed again against the written code. Bounded workers handled disjoint
implementation/test scopes; final integration and claims remain lead-owned.
The code-quality reviews used inherited context and were **not independent
reviews**. No scientific/model behavior changed. Support-agent documentation
impact is required: config, source routing, unknown State, events, filters and
downgrade answers changed. The source/operator/troubleshooting indexes and
configuration-readback support card now route to the authoritative contracts.

## Downloaded Source

The unaltered ZIPs downloaded on 2026-10-07 were served by a local HTTP server
and processed through exported `uls.RefreshCanadian`, outside the repository.
The production parser matches a separate raw-field-position analysis:

- Assigned calls: **92,142**; known province: **90,500**; unknown: **1,642**.
- Events: **956**; **2** unambiguous active records on 2026-10-07 UTC.
- SQLite schema version: **1**; `PRAGMA quick_check`: **ok**.
- Permanent tables: `CA`, `Events`, `SourceMeta`; no names, full addresses,
  qualifications or event descriptions are stored.
- `VA1AA` resolves NS; active `VX9M` resolves NB and `VE3FIRE` ON; reversed
  historical `CG7GMT` remains unavailable rather than definitively missing.
- An unchanged second refresh reports `updated=false`.

| Province | Count | Province | Count |
| --- | ---: | --- | ---: |
| AB | 9,173 | BC | 22,670 |
| MB | 2,525 | NB | 1,995 |
| NL | 1,559 | NS | 2,975 |
| NT | 109 | NU | 41 |
| ON | 26,323 | PE | 417 |
| QC | 20,591 | SK | 1,874 |
| YT | 248 | Unknown | 1,642 |

Input SHA-256 values are
`7e74746339b930c93d8bbcd7e1857da97ab6fd5011d30093a438c08658ef1662`
(main) and
`7665cb30995480adac773db705f76d61af92d2e8994ca2bc369d30e4fdf35224`
(events). Local artifacts are `/tmp/gocluster-ised-source-evidence` and
`/tmp/gocluster-ised-full-refresh-final-ownership.log`. The sample test is explicitly
opt-in through `GOCLUSTER_ISED_SAMPLE_DIR`; CI fixture tests do not require live
ISED or this local artifact.

ISED publishes [assigned and event downloads](https://ised-isde.canada.ca/site/amateur-radio-operator-certificate-services/en/downloads)
and [RIC-9's prefix policy](https://ised-isde.canada.ca/site/spectrum-management-telecommunications/en/licences-and-certificates/radiocom-information-circulars-ric/ric-9-call-sign-policy-and-special-event-prefixes).
Snapshot presence proves assignment/plausibility, without proving all event
eligibility. No daily publication guarantee or source timezone is inferred.

## Resource and Performance Limits

There are exactly two source owners and one aggregate 200,000-entry cache.
Generation/day are entry properties, not growing key dimensions. Each reader
pool has at most four connections and a five-second cold-query budget. Refresh
builders are serialized per source and retain no secondary in-memory registry.
Limits are 64 MiB main and 8 MiB event ZIP/plaintext, 64 KiB lines, 1,000,000 main
and 100,000 event rows; extraction and hash reads honor cancellation. ZIP index
memory is bounded by compressed input size, without a measured peak-heap claim.
Source-removal sweeps hold the shared cache mutex for at most its aggregate
cardinality. Event queries use indexed keys and primary-key joins, but can read
many matching historical event rows. These bounds do not establish a deployed
latency SLA or long-running leak freedom.

Final source review found that SQLite's temporary-directory setting is
process-wide. The pinned driver synchronizes its relevant accesses, so this did
not establish a race; it did establish possible cross-source directory coupling.
The Canadian builder now leaves that setting alone, uses `ised.temp_dir` for
unique extraction children, and keeps publication scratch beside `ised.db_path`.
Its streaming SQL requires no TEMP tables or sorts. The regression checks the
actual setting before/after refresh and shared-directory content/cleanup.

Local Linux warm lookup benchmarks (three runs) retain **0 B/0 allocations**:
FCC 176–235 ns/op; Canada 344–368 ns/op. FCC cold lookup was 41.8–42.1 us/op,
1,569–1,571 B and 32 allocations, versus the pre-change 31.8–34.6 us/op, 544 B
and 18 allocations. The added cancellable pool/query budget increases cold
cost. Measurements are local microbenchmarks, without a speedup or p99 claim.

## Final Checks

The final production state passed `go test ./...`, `go test -race -p 1 ./...`,
`go vet ./...`, `staticcheck ./...` and
`golangci-lint run ./... --config=.golangci.yaml` (0 issues), with Go 1.27.1,
Staticcheck 2026.2.1 and golangci-lint 2.14.0. Selected native Windows Go 1.27.1
checks passed ULS, download, config, spot, filter, archive and telnet; the actual
Windows sharing-denial test also passed, retaining old bytes and succeeding
after handle release. Native Windows ULS checks and the opt-in full-source test
were rerun successfully after the temporary-directory fix. Parser fuzzing passed
30 seconds with **266,941 executions**. Targeted archive/State-list fuzz runs
also passed.

YAML rigor checked all 20 runtime files. The new boolean comment was reviewed:
its enforcement-only meaning is a non-obvious side effect and belongs beside
the setting. The PowerShell crawler checker returned success while scanning
zero files in this environment; this was not treated as coverage. A manual
check of the actual changed support-critical inputs inspected 21 entry headers,
followed by source-aware comment review. Markdown link and diff checks found no
missing local targets or whitespace errors.

A first parallel full race run failed the existing peer age-timing assertion
`TestPeerSpotSecondAgeCheckUsesOriginalTimestamp` for PC61 (relays=1); it reported
no data race. The isolated test passed in 6.611 seconds, and a subsequent full
race run with serialized package execution passed. Peer code/tests were not
changed. The later alias and temporary-directory fixes received targeted checks
and the complete final lane reported above.

The final normal and serialized race logs are
`/tmp/gocluster-canadian-final-test.log` and
`/tmp/gocluster-canadian-final-race-complete.log`. The final source pair was also
reprocessed through exported refresh; the counts, provinces, membership and
unchanged retry above remained correct. Generated maps were regenerated and
`go run ./cmd/codemap check -all` reported **code maps are fresh**. A final check
resolved all 260 local Markdown targets, including the generated maps' repo-root
targets. `git diff --check` passed.
