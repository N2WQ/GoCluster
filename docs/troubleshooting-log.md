# Troubleshooting Log

This index tracks troubleshooting records (`TSR-XXXX`) that can lead to ADRs.

## How To Use
- Create new troubleshooting records in `docs/troubleshooting/` using `TSR-TEMPLATE.md`.
- Add one row per TSR to the table below.
- If a TSR results in a durable decision, add/create the ADR and cross-link both records.
- Do not delete old TSR rows; mark status updates in place.

## TSR Index
| TSR | Title | Status | Date | Area | Led To ADR | Links |
|---|---|---|---|---|---|---|
| TSR-0080 | Archive Shared Snapshot Normalization | Monitoring | 2026-10-10 | archive, spot, final fanout, peer | ADR-0281 | `docs/troubleshooting/TSR-0080-archive-shared-snapshot-normalization.md` |
| TSR-0078 | Custom SCP Stale Deletion and Close | Resolved | 2026-10-10 | custom_scp, cleanup, persistence, shutdown | ADR-0279 | `docs/troubleshooting/TSR-0078-custom-scp-stale-deletion-and-close.md` |
| TSR-0077 | Peer Probe Retirement | Monitoring | 2026-10-10 | peerpoc, phase deadlines, timestamps, writer and keeper ownership | ADR-0278 | `docs/troubleshooting/TSR-0077-peer-probe-retirement.md` |
| TSR-0076 | Diagnostic Wrapper Settings and Status | Monitoring | 2026-10-10 | diagnostic startup wrapper, GC precedence and native child status | none | `docs/troubleshooting/TSR-0076-diagnostic-wrapper-settings-and-status.md` |
| TSR-0075 | Historical Analysis Claims and Failures | Monitoring | 2026-10-10 | historical decision readers, classification and report claims | ADR-0277 | `docs/troubleshooting/TSR-0075-historical-analysis-claims-and-failures.md` |
| TSR-0074 | Producer Clock for Diagnostic Tests | Monitoring | 2026-10-10 | peer diagnostic timing tests | none | `docs/troubleshooting/TSR-0074-producer-clock-for-diagnostic-tests.md` |
| TSR-0073 | Operator Utility Exit Outcomes | Monitoring | 2026-10-10 | operator tools, failure feedback and partial output | none | `docs/troubleshooting/TSR-0073-operator-utility-exit-outcomes.md` |
| TSR-0072 | Sunspot Watcher Outcomes | Monitoring | 2026-10-10 | sunspot watcher, one-shot outcomes and retries | none | `docs/troubleshooting/TSR-0072-sunspot-watcher-outcomes.md` |
| TSR-0071 | Root Executable Argument Admission | Monitoring | 2026-10-10 | root CLI, help, version precedence and startup | none | `docs/troubleshooting/TSR-0071-root-executable-argument-admission.md` |
| TSR-0070 | Toxicity Positive Duration Admission | Monitoring | 2026-10-10 | toxicity, optional YAML, defaults and numeric units | ADR-0276 | `docs/troubleshooting/TSR-0070-toxicity-positive-duration-admission.md` |
| TSR-0069 | Output Pipeline Retirement | Monitoring | 2026-10-10 | runtime output, grace, held work and released fanout | ADR-0275 | `docs/troubleshooting/TSR-0069-output-pipeline-retirement.md` |
| TSR-0068 | Classifier Terminal Retirement | Monitoring | 2026-10-10 | toxicity lifecycle, queued references and HTTP cancellation | ADR-0274 | `docs/troubleshooting/TSR-0068-classifier-terminal-retirement.md` |
| TSR-0067 | SHOW HOLD Token Recognition | Monitoring | 2026-10-10 | telnet read-pause control, token parsing and status | none | `docs/troubleshooting/TSR-0067-show-hold-token-recognition.md` |
| TSR-0066 | Command Discovery Wording | Monitoring | 2026-10-10 | filter usage, HELP, DX admission and command recipes | none | `docs/troubleshooting/TSR-0066-command-discovery-wording.md` |
| TSR-0065 | HELP Routing And Alias Coverage | Monitoring | 2026-10-10 | commands HELP routing, telnet dispatch aliases and taxonomy | none | `docs/troubleshooting/TSR-0065-help-routing-alias-coverage.md` |
| TSR-0064 | CC HELP Category Grammar | Monitoring | 2026-10-10 | commands HELP, telnet filter categories and advertised forms | none | `docs/troubleshooting/TSR-0064-cc-help-category-grammar.md` |
| TSR-0063 | History COMMENT Alias Boundary | Monitoring | 2026-10-10 | commands, telnet reader, history selections and continuation | ADR-0273 | `docs/troubleshooting/TSR-0063-history-comment-alias-boundary.md` |
| TSR-0062 | Taxonomy Human Input Alphabet | Monitoring | 2026-10-09 | telnet reader, configured MODE/EVENT names and aliases, input restrictions | none | `docs/troubleshooting/TSR-0062-taxonomy-human-input-alphabet.md` |
| TSR-0061 | SOLAR Availability And Outcomes | Monitoring | 2026-10-09 | solar summaries, command feedback, saved/effective cadence | none | `docs/troubleshooting/TSR-0061-solar-availability-and-outcomes.md` |
| TSR-0060 | DEDUPE Configured And Effective Readback | Monitoring | 2026-10-09 | telnet, DEDUPE, stored settings, effective lanes, bounded readback | none | `docs/troubleshooting/TSR-0060-dedupe-configured-effective-readback.md` |
| TSR-0059 | Fixed Setting Argument Arity | Monitoring | 2026-10-09 | telnet settings, diagnostic mode, scheduling, argument admission | none | `docs/troubleshooting/TSR-0059-fixed-setting-argument-arity.md` |
| TSR-0058 | DXCC Lookup And Logout Argument Arity | Monitoring | 2026-10-09 | commands, DXCC lookup, logout aliases, connection usability | none | `docs/troubleshooting/TSR-0058-dxcc-logout-arity.md` |
| TSR-0057 | Filter Argument Tails Lost During Normalization | Monitoring | 2026-10-09 | telnet, CC aliases, flags, toggles, raw arguments | none | `docs/troubleshooting/TSR-0057-filter-argument-tail-normalization.md` |
| TSR-0056 | Human Callsign Pattern Admission | Monitoring | 2026-10-09 | telnet, callsign filters, syntax, machine validation | none | `docs/troubleshooting/TSR-0056-human-callsign-pattern-admission.md` |
| TSR-0055 | GRID Prefix Acceptance And Mapping Feedback | Monitoring | 2026-10-09 | telnet, GRID, syntax, mapping availability, NEARBY | none | `docs/troubleshooting/TSR-0055-grid-prefix-and-mapping-feedback.md` |
| TSR-0054 | NEARBY Opposite Endpoint Veto | Monitoring | 2026-10-09 | filter, telnet, NEARBY, endpoint cells, history | none | `docs/troubleshooting/TSR-0054-nearby-opposite-endpoint-veto.md` |
| TSR-0053 | MODE Delta Became A Whitelist Transition | Monitoring | 2026-10-09 | filter, telnet, MODE, feedback, marked reload | ADR-0272 | `docs/troubleshooting/TSR-0053-mode-delta-whitelist-transition.md` |
| TSR-0052 | Stored YAML Schema Allocation | Monitoring | 2026-10-09 | filter, YAML, schema validation, allocation, resource admission | ADR-0270 | `docs/troubleshooting/TSR-0052-stored-yaml-schema-allocation.md` |
| TSR-0051 | Stored YAML Alias Compatibility | Monitoring | 2026-10-09 | filter, YAML, aliases, merges, persistence | ADR-0267 | `docs/troubleshooting/TSR-0051-stored-yaml-alias-compatibility.md` |
| TSR-0050 | Human Persistence Feedback | Monitoring | 2026-10-09 | telnet, filter, dialect, persistence | none | `docs/troubleshooting/TSR-0050-human-persistence-feedback.md` |
| TSR-0049 | NOFILTER Restored NEARBY Restrictions | Monitoring | 2026-10-09 | filter, telnet, reset, snapshot | none | `docs/troubleshooting/TSR-0049-nofilter-nearby-restoration.md` |
| TSR-0048 | Rejected Human Line Tail Dispatch | Monitoring | 2026-10-09 | telnet, input, protocol, lifecycle | ADR-0266 | `docs/troubleshooting/TSR-0048-rejected-human-line-tail-dispatch.md` |
| TSR-0047 | History Mode Alias Clause Collision | Monitoring | 2026-10-08 | commands, telnet, history parser | ADR-0263 | `docs/troubleshooting/TSR-0047-history-mode-alias-clause-collision.md` |
| TSR-0046 | Comment Matching CPU And History Input | Monitoring | 2026-10-08 | filter, commands, telnet, peer | none | `docs/troubleshooting/TSR-0046-comment-matching-cpu-and-history-input.md` |
| TSR-0045 | CCCluster Outbound Handshake | Monitoring | 2026-10-08 | peer, ccluster, handshake, PC20, PC51 | ADR-0260 | `docs/troubleshooting/TSR-0045-cccluster-outbound-handshake.md` |
| TSR-0044 | Support Agent Token Mismatch | Monitoring | 2026-10-07 | customgpt, deployment, admission | ADR-0257 | `docs/troubleshooting/TSR-0044-support-agent-token-mismatch.md` |
| TSR-0043 | Support Search Action Limit | Monitoring | 2026-10-07 | customgpt, search, actions, response-budget | ADR-0256 | `docs/troubleshooting/TSR-0043-support-search-action-limit.md` |
| TSR-0042 | Short Portable License Identity | Resolved | 2026-10-07 | uls, telnet, runtime, license, portable, allowlist | none | `docs/troubleshooting/TSR-0042-short-portable-license-identity.md` |
| TSR-0041 | Exact-Call History And Archive Scan Cap | Monitoring | 2026-10-06 | commands, telnet, archive, pagination, lifecycle | ADR-0251 | `docs/troubleshooting/TSR-0041-exact-call-history-and-scan-cap.md` |
| TSR-0040 | Release Preflight and Output Safety | Monitoring | 2026-10-03 | release, PowerShell, Git, CRLF, ownership, CGO, Application Control | ADR-0242 | `docs/troubleshooting/TSR-0040-release-preflight-and-output-safety.md` |
| TSR-0039 | Peer Normalized Relay, Date Admission and Telnet Framing | Monitoring | 2026-10-04 | peer, protocol, relay, date, admission, Telnet, framing | ADR-0239, ADR-0240, ADR-0241 | `docs/troubleshooting/TSR-0039-peer-normalized-relay-and-telnet-iac.md` |
| TSR-0038 | Lint Analyzer Toolchain Compatibility | Resolved | 2026-10-03 | development, lint, Go toolchain | ADR-0250 | `docs/troubleshooting/TSR-0038-lint-analyzer-toolchain-compatibility.md` |
| TSR-0037 | Peer Session Cancellation Publication | Resolved | 2026-10-03 | peer, cancellation, lifecycle, race, ownership | none | `docs/troubleshooting/TSR-0037-peer-session-cancellation-publication.md` |
| TSR-0036 | Spot Collisions and Peer Ownership | Monitoring | 2026-10-02 | dedup, peer, diagnostics, persistence | ADR-0234, ADR-0235 | `docs/troubleshooting/TSR-0036-spot-collisions-and-peer-ownership.md` |
| TSR-0035 | PC92 Qualification Accounting | Monitoring | 2026-10-01 | peer, telnet, allocation, qualification | ADR-0230, ADR-0231, ADR-0232 | `docs/troubleshooting/TSR-0035-pc92-qualification-accounting.md` |
| TSR-0034 | Host-Dependent Code Map Freshness | Monitoring | 2026-07-12 | workflow, codemap, CI, portability | ADR-0229 | `docs/troubleshooting/TSR-0034-host-dependent-code-map-freshness.md` |
| TSR-0033 | Fable Workflow Checker CRLF Self-Inconsistency | Resolved | 2026-07-11 | workflow, checker, Fable, CRLF | none | `docs/troubleshooting/TSR-0033-fable-workflow-checker-crlf-self-inconsistency.md` |
| TSR-0032 | Single Human Upstream Telnet Config | Resolved | 2026-07-10 | config, rbn, ingest, dashboard, lifecycle, operations | ADR-0218 | `docs/troubleshooting/TSR-0032-single-human-upstream-telnet-config.md` |
| TSR-0031 | RBN Startup Retry And Console Liveness | Resolved | 2026-06-16 | rbn, ingest, dashboard, lifecycle, operations | ADR-0181 | `docs/troubleshooting/TSR-0031-rbn-startup-retry-and-console-liveness.md` |
| TSR-0030 | VOACAP Runtime Output File Retention | Resolved | 2026-06-16 | voacap, pathreliability, file lifecycle, operations | ADR-0180 | `docs/troubleshooting/TSR-0030-voacap-runtime-output-file-retention.md` |
| TSR-0029 | H3-Backed Tests Skipped From Package CWD | Resolved | 2026-06-10 | pathreliability, telnet, filter, tests | - | `docs/troubleshooting/TSR-0029-h3-backed-tests-skipped-from-package-cwd.md` |
| TSR-0028 | VOACAP Fallback Current-Hour Miss | Resolved | 2026-06-09 | voacap, pathreliability, propagation logs | ADR-0164 | `docs/troubleshooting/TSR-0028-voacap-fallback-current-hour-miss.md` |
| TSR-0027 | Support-Agent Shallow Answers | Resolved | 2026-06-07 | customgpt, supportability, actions, operations | ADR-0154 | `docs/troubleshooting/TSR-0027-support-agent-shallow-answers.md` |
| TSR-0026 | Archive Cleanup Scan CPU Spikes | Resolved | 2026-06-06 | archive, Pebble, hot path, profiling | ADR-0151 | `docs/troubleshooting/TSR-0026-archive-cleanup-scan-cpu-spikes.md` |
| TSR-0025 | P50 Merge CPU And Heap Pressure | Resolved | 2026-06-04 | pathreliability, retained state, hot path, profiling | ADR-0139 | `docs/troubleshooting/TSR-0025-p50-merge-cpu-and-heap-pressure.md` |
| TSR-0024 | Custom SCP Test Time Horizon | Resolved | 2026-05-23 | spot, custom_scp, tests | ADR-0136 | `docs/troubleshooting/TSR-0024-custom-scp-test-time-horizon.md` |
| TSR-0023 | Path Capped Count Gate Overload | Resolved | 2026-05-11 | pathreliability, config, telnet diagnostics, propagation logs | ADR-0134 | `docs/troubleshooting/TSR-0023-path-capped-count-gate-overload.md` |
| TSR-0022 | Path Receiver Cap Lifetime Count | Resolved | 2026-05-10 | pathreliability, retained state | ADR-0130 | `docs/troubleshooting/TSR-0022-path-receiver-cap-lifetime-count.md` |
| TSR-0021 | Command-Like Login CTY Admission | Resolved | 2026-05-04 | spot, CTY, telnet, ingest, peer, replay | ADR-0110 | `docs/troubleshooting/TSR-0021-command-like-login-cty-admission.md` |
| TSR-0002 | Per-Spot Call Correction Split-Evidence Ambiguity and Quality Penalty Reinforcement | Resolved | 2026-02-23 | spot/correction, main/telnet fan-out | ADR-0021 | `docs/troubleshooting/TSR-0002-call-correction-ambiguity-and-quality-penalty.md` |
| TSR-0003 | Phase 2 Signal Resolver Shadow-Mode Design and Validation Plan | Open | 2026-02-23 | spot/correction, main output pipeline | ADR-0022 | `docs/troubleshooting/TSR-0003-phase2-signal-resolver-shadow-design.md` |
| TSR-0004 | Resolver Primary Switchover Mode and Rollback Contract | Resolved | 2026-02-25 | main output pipeline, spot/correction, config | ADR-0026 | `docs/troubleshooting/TSR-0004-resolver-primary-switchover-mode.md` |
| TSR-0005 | Resolver-Primary Recall Gap on Truncation Families and Overstated V Glyph in Contested States | Resolved | 2026-02-26 | main output pipeline, spot/correction | ADR-0032 | `docs/troubleshooting/TSR-0005-resolver-primary-family-recall-and-contested-glyph.md` |
| TSR-0006 | Resolver Neighborhood Competition and Contested Edit-Neighbor Rails | Resolved | 2026-02-26 | main output pipeline, correctionflow, telnet suppression, replay, config | ADR-0033 | `docs/troubleshooting/TSR-0006-resolver-neighborhood-and-edit-neighbor-contested-rails.md` |
| TSR-0007 | Resolver-Primary One-Short Misses and Conservative Recent +1 Corroborator Rail | Resolved | 2026-02-26 | main output pipeline, spot/correction, correctionflow, replay, config | ADR-0034 | `docs/troubleshooting/TSR-0007-resolver-recent-plus1-corroborator-rails.md` |
| TSR-0008 | Resolver Neighborhood Forced-Split Regression From Unrelated Adjacent Buckets | Resolved | 2026-02-26 | internal/correctionflow, main output pipeline, replay, config | ADR-0035 | `docs/troubleshooting/TSR-0008-resolver-neighborhood-unrelated-adjacent-split-regression.md` |
| TSR-0009 | Resolver Reliability/Confusion Parity Gap and Winner Tie-Break Integration | Resolved | 2026-02-26 | main output pipeline, cmd/rbn_replay, cmd/callcorr_reveng_rebuilt, internal/correctionflow, spot/signal_resolver | ADR-0036 | `docs/troubleshooting/TSR-0009-resolver-reliability-confusion-parity-gap.md` |
| TSR-0010 | Stabilizer Delaying V Glyph Spots Against Intended Delay Eligibility | Resolved | 2026-03-01 | internal/correctionflow stabilizer policy, main/replay parity, docs | ADR-0046 | `docs/troubleshooting/TSR-0010-stabilizer-v-glyph-delay-eligibility.md` |
| TSR-0011 | Peering Enabled Flag Overridden by Host/Port Auto-Enable | Resolved | 2026-03-03 | config, peer | ADR-0047 | `docs/troubleshooting/TSR-0011-peering-enabled-flag-override.md` |
| TSR-0012 | PC92 Hop Suffix Amplification and Overlong Diagnostics Gap | Resolved | 2026-03-05 | peer/protocol, peer/manager, peer/reader | ADR-0050 | `docs/troubleshooting/TSR-0012-pc92-hop-suffix-amplification-and-overlong-gap.md` |
| TSR-0013 | FT2 Mode Token and Operator-Surface Gap | Resolved | 2026-03-27 | spot, pskreporter, filter, telnet, commands, docs | ADR-0055 | `docs/troubleshooting/TSR-0013-ft2-mode-token-and-operator-surface-gap.md` |
| TSR-0014 | Local Self-Spot Delay and Confidence Semantics | Resolved | 2026-03-27 | commands, main output pipeline, telnet, custom_scp, docs | ADR-0056 | `docs/troubleshooting/TSR-0014-local-self-spot-delay-and-confidence.md` |
| TSR-0015 | PSKReporter FT Frequency Semantic Mismatch | Resolved | 2026-04-08 | pskreporter ingest, spot, archive, mode inference, FT confidence | ADR-0058 | `docs/troubleshooting/TSR-0015-pskreporter-ft-frequency-semantic-mismatch.md` |
| TSR-0016 | FT Confidence Arrival Window Misses Live Corroboration | Superseded | 2026-04-08 | main output pipeline, FT confidence, docs | ADR-0059 | `docs/troubleshooting/TSR-0016-ft-confidence-arrival-window-misses-live-corroboration.md` |
| TSR-0017 | FT Confidence Slot Anchors Still Split Live Corroboration | Resolved | 2026-04-08 | main output pipeline, FT confidence, stats, docs | ADR-0060 | `docs/troubleshooting/TSR-0017-ft-confidence-slot-anchor-fragmentation.md` |
| TSR-0018 | Peer Bulletin Duplicate Fanout | Resolved | 2026-04-19 | peer, telnet fan-out, config | ADR-0063 | `docs/troubleshooting/TSR-0018-peer-bulletin-duplicate-fanout.md` |
| TSR-0019 | MODE UNKNOWN Filter Feedback | Resolved | 2026-04-22 | filter, telnet, docs | - | `docs/troubleshooting/TSR-0019-mode-unknown-filter-feedback.md` |
| TSR-0020 | RBN Mode Column Confusion | Resolved | 2026-04-26 | rbn, parser, replay, call correction | ADR-0087 | `docs/troubleshooting/TSR-0020-rbn-mode-column-confusion.md` |

TSR-0035 v14 follow-up: controlled retry service, Flush-event ordering, startup/global-gate serialization and lifetime-owned retry timer backing; see [TSR-0035](troubleshooting/TSR-0035-pc92-qualification-accounting.md).
