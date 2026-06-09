# Decision Log

This index tracks all architecture and workflow decisions recorded as ADRs.

## How To Use

- Create new ADRs in `docs/decisions/` using `ADR-TEMPLATE.md`.
- Add one row per ADR to the table below.
- Do not delete old ADR rows; mark status as `Superseded`/`Deprecated` when needed.

## ADR Index

| ADR | Title | Status | Date | Area | Supersedes | Superseded By | Links |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ADR-0001 | Placeholder Title | Proposed | YYYY-MM-DD | placeholder area | - | - | `docs/decisions/ADR-0001-placeholder-slug.md` |
| ADR-0158 | VOACAP Process Wrapper Experiment | Accepted | 2026-06-08 | voacap, experiments, process lifecycle | - | - | `docs/decisions/ADR-0158-voacap-process-wrapper-experiment.md` |
| ADR-0157 | VOACAP SSN Moving Average Experiment | Accepted | 2026-06-08 | voacap, solarweather, experiments | - | - | `docs/decisions/ADR-0157-voacap-ssn-moving-average-experiment.md` |
| ADR-0156 | Expanded Repo-Managed Audit Skills | Accepted | 2026-06-08 | workflow, Codex, skills, lifecycle, decision-memory | - | - | `docs/decisions/ADR-0156-expanded-repo-managed-audit-skills.md` |
| ADR-0155 | Repo-Authoritative Codex Skills | Accepted | 2026-06-08 | workflow, Codex, skills, portability | - | - | `docs/decisions/ADR-0155-repo-authoritative-codex-skills.md` |
| ADR-0154 | Support-Agent Quality Contract | Accepted | 2026-06-07 | customgpt, supportability, actions, operations | ADR-0107; ADR-0108; ADR-0109; ADR-0112; ADR-0113 | - | `docs/decisions/ADR-0154-support-agent-quality-contract.md` |
| ADR-0153 | Startup Config Diagnostics And Gridstore Logging | Accepted | 2026-06-07 | config, startup, pathreliability, gridstore, operations | ADR-0067 unknown-key fatal behavior | - | `docs/decisions/ADR-0153-startup-config-diagnostics-and-gridstore-logging.md` |
| ADR-0152 | Stable Active Log Filenames With Date-Only Archives | Accepted | 2026-06-07 | logging, propagation reports, operations | ADR-0093 active filename shape; ADR-0123 active filename shape | - | `docs/decisions/ADR-0152-stable-active-log-filenames-with-date-only-archives.md` |
| ADR-0151 | Archive Range Deletion Cleanup | Accepted | 2026-06-06 | archive, config, hot path, profiling | ADR-0149 cleanup implementation | - | `docs/decisions/ADR-0151-archive-range-deletion-cleanup.md` |
| ADR-0149 | Single-Window Archive Retention | Accepted | 2026-06-06 | archive, config, spot taxonomy, hot path | ADR-0069 archive retention class | - | `docs/decisions/ADR-0149-single-window-archive-retention.md` |
| ADR-0148 | Post-Stabilizer Shared Secondary Fanout | Accepted | 2026-06-06 | internal/cluster, stabilizer, dedupe, archive, peer, hot path | ADR-0013 archive/peer side rail; ADR-0020 archive/peer unchanged clause; ADR-0029 archive/peer unchanged clause; ADR-0138 archive/peer MED side rail | - | `docs/decisions/ADR-0148-post-stabilizer-shared-secondary-fanout.md` |
| ADR-0147 | Generated Code Map Freshness Gate | Accepted | 2026-06-05 | workflow, docs/code-maps, scripts, CI, release | - | - | `docs/decisions/ADR-0147-generated-code-map-freshness-gate.md` |
| ADR-0146 | H3 Path Cell Duplicate Work Removal | Accepted | 2026-06-05 | spot, internal/cluster, pathreliability, hot path | - | - | `docs/decisions/ADR-0146-h3-path-cell-duplicate-work-removal.md` |
| ADR-0145 | PSKReporter Fast Payload Parser | Accepted | 2026-06-05 | pskreporter, mqtt, parser, hot path | - | - | `docs/decisions/ADR-0145-pskreporter-fast-payload-parser.md` |
| ADR-0144 | Slice-Shaped Scope Ledgers | Accepted | 2026-06-05 | workflow, Codex, validation | - | - | `docs/decisions/ADR-0144-slice-shaped-scope-ledgers.md` |
| ADR-0143 | Custom SCP Support Lookup Duplicate Work Removal | Accepted | 2026-06-05 | spot, internal/correctionflow, custom_scp, hot path | - | - | `docs/decisions/ADR-0143-custom-scp-support-lookup-duplicate-work-removal.md` |
| ADR-0142 | UI v2 Buffer Bounds And Render Churn | Accepted | 2026-06-04 | ui/tview-v2, config, retained state, hot path | - | - | `docs/decisions/ADR-0142-ui-v2-buffer-bounds-and-render-churn.md` |
| ADR-0141 | Go Runtime Max Procs YAML Control | Accepted | 2026-06-04 | config, startup, runtime scheduler, operations | - | - | `docs/decisions/ADR-0141-go-runtime-max-procs-yaml-control.md` |
| ADR-0140 | Agentic Code-Walk, Blast-Radius, and Leak Workflow | Accepted | 2026-06-04 | workflow, Codex, tooling, skills | - | - | `docs/decisions/ADR-0140-agentic-code-walk-blast-radius-leak-workflow.md` |
| ADR-0139 | Active Path P50 Histogram Lane Retention | Accepted | 2026-06-04 | pathreliability, retained state, hot path, reports | - | - | `docs/decisions/ADR-0139-active-path-p50-histogram-lane-retention.md` |
| ADR-0138 | YAML-Owned Default Dedupe Policy | Accepted | 2026-05-25 | config, dedupe, telnet, user records | - | - | `docs/decisions/ADR-0138-yaml-owned-default-dedupe-policy.md` |
| ADR-0137 | Prod User Dedupe Slow Bulk Update | Accepted | 2026-05-25 | user records, dedupe, operations | - | - | `docs/decisions/ADR-0137-prod-user-dedupe-slow-bulk-update.md` |
| ADR-0136 | Custom SCP Test Time Fixtures | Accepted | 2026-05-23 | spot, custom_scp, tests | - | - | `docs/decisions/ADR-0136-custom-scp-test-time-fixtures.md` |
| ADR-0135 | Path Cap8 Enforcement And Shadow Candidate Retirement | Accepted | 2026-05-11 | pathreliability, config, retained state, propagation logs, reports | ADR-0132; ADR-0133; ADR-0129 (shipped cap value portion) | - | `docs/decisions/ADR-0135-path-cap8-enforcement-and-shadow-candidate-retirement.md` |
| ADR-0134 | Path Observation Floor And Receiver Diversity Gate | Accepted | 2026-05-11 | pathreliability, config, telnet diagnostics, propagation logs | ADR-0129 (enforce-mode capped-count floor portion); ADR-0132 (cap-shadow low-count-only fields) | - | `docs/decisions/ADR-0134-path-observation-floor-and-receiver-diversity-gate.md` |
| ADR-0133 | Path Receiver Cap P50 Shadow Histograms | Superseded | 2026-05-11 | pathreliability, config, propagation logs, reports | - | ADR-0135 | `docs/decisions/ADR-0133-path-receiver-cap-p50-shadow-histograms.md` |
| ADR-0132 | Path Receiver Cap Shadow Candidates | Superseded | 2026-05-10 | pathreliability, config, propagation logs, reports | - | ADR-0135 | `docs/decisions/ADR-0132-path-receiver-cap-shadow-candidates.md` |
| ADR-0131 | Path P50 Midpoint And Even-Split Semantics | Accepted | 2026-05-10 | pathreliability, telnet display, PATH filters | ADR-0126 (lower-edge p50 representative and exact-boundary behavior) | - | `docs/decisions/ADR-0131-path-p50-midpoint-and-even-split-semantics.md` |
| ADR-0130 | Path Receiver Cap Count Decay | Accepted | 2026-05-10 | pathreliability, retained state, config semantics | ADR-0129 (lifetime count semantics) | - | `docs/decisions/ADR-0130-path-receiver-cap-count-decay.md` |
| ADR-0129 | Path Receiver Capacity And Enforcement | Accepted | 2026-05-10 | pathreliability, config, retained state | ADR-0095 (slot ceiling and enforcement mode) | ADR-0130 (count-decay semantics); ADR-0135 (shipped cap value portion) | `docs/decisions/ADR-0129-path-receiver-capacity-and-enforcement.md` |
| ADR-0128 | Remove PATHP50 Diagnostics And Clamp Config | Accepted | 2026-05-10 | pathreliability, telnet diagnostics, config, reports | ADR-0126 PATHP50 diagnostics; ADR-0123 Path p50 diag aggregate | - | `docs/decisions/ADR-0128-remove-pathp50-diagnostics-and-clamp-config.md` |
| ADR-0127 | Location-Specific Path Noise Penalties | Accepted | 2026-05-09 | pathreliability, config, telnet, reports | ADR-0124 | - | `docs/decisions/ADR-0127-location-specific-path-noise-penalties.md` |
| ADR-0126 | Active P50 Path Scoring | Accepted | 2026-05-09 | pathreliability, telnet diagnostics, propagation logs | ADR-0122; ADR-0125; ADR-0123 PATHP50 shadow-comparison portion | ADR-0128 (PATHP50 diagnostics only); ADR-0131 (p50 representative and exact-boundary behavior) | `docs/decisions/ADR-0126-active-p50-path-scoring.md` |
| ADR-0125 | Gated PATHP50 Shadow Glyph Comparison | Superseded | 2026-05-08 | pathreliability, telnet diagnostics, propagation logs | - | ADR-0126 | `docs/decisions/ADR-0125-gated-pathp50-shadow-comparison.md` |
| ADR-0124 | No-Noise-Penalty Path Evaluation | Superseded | 2026-05-08 | pathreliability, config, telnet | ADR-0064 | ADR-0127 | `docs/decisions/ADR-0124-no-noise-penalty-path-evaluation.md` |
| ADR-0123 | Dedicated Propagation Log | Accepted | 2026-05-08 | logging, pathreliability, propagation reports | - | ADR-0128 (`Path p50 diag` aggregate only) | `docs/decisions/ADR-0123-dedicated-propagation-log.md` |
| ADR-0122 | Path P50 Shadow Diagnostics | Superseded | 2026-05-08 | pathreliability, telnet diagnostics, config | - | ADR-0126 | `docs/decisions/ADR-0122-path-p50-shadow-diagnostics.md` |
| ADR-0121 | PSKReporter Trusted Spot Materialization | Accepted | 2026-05-07 | pskreporter, spot, hot path | - | - | `docs/decisions/ADR-0121-pskreporter-trusted-spot-materialization.md` |
| ADR-0120 | Resolver Correction State Hardening | Accepted | 2026-05-06 | spot, correction, config, concurrency | - | - | `docs/decisions/ADR-0120-resolver-correction-state-hardening.md` |
| ADR-0119 | Non-trivial Scope Adversarial Review | Accepted | 2026-05-06 | workflow, scope ledger, validation | - | - | `docs/decisions/ADR-0119-non-trivial-scope-adversarial-review.md` |
| ADR-0118 | Telnet Broadcast Job Value Queues | Accepted | 2026-05-06 | telnet, fan-out, queues, hot path | - | - | `docs/decisions/ADR-0118-telnet-broadcast-job-value-queues.md` |
| ADR-0117 | Hot Path Duplicate Work Removal | Accepted | 2026-05-06 | spot, output pipeline, custom_scp, telnet, mqtt | - | - | `docs/decisions/ADR-0117-hot-path-duplicate-work-removal.md` |
| ADR-0116 | Blank Beacon Comment Fallback | Accepted | 2026-05-06 | spot, telnet, rbn, archive, peer | - | - | `docs/decisions/ADR-0116-blank-beacon-comment-fallback.md` |
| ADR-0115 | DX Numeric SSID Canonicalization | Accepted | 2026-05-05 | spot, ingest, telnet, commands, replay | - | - | `docs/decisions/ADR-0115-dx-numeric-ssid-canonicalization.md` |
| ADR-0114 | Human Comment Toxicity Classifier | Accepted | 2026-05-05 | toxicity, telnet, output pipeline, config, archive | - | - | `docs/decisions/ADR-0114-human-comment-toxicity-classifier.md` |
| ADR-0113 | Support-Agent PowerShell Script Retrieval | Accepted | 2026-05-04 | customgpt, supportability, scripts, actions | - | - | `docs/decisions/ADR-0113-support-agent-powershell-script-retrieval.md` |
| ADR-0112 | Support-Agent Deployment Bundle Isolation | Accepted | 2026-05-04 | customgpt, supportability, actions | - | - | `docs/decisions/ADR-0112-support-agent-deployment-bundle-isolation.md` |
| ADR-0111 | Support-Agent Large-File Recovery | Accepted | 2026-05-04 | customgpt, supportability, actions | - | - | `docs/decisions/ADR-0111-support-agent-large-file-recovery.md` |
| ADR-0110 | Call-Like CTY Admission | Accepted | 2026-05-04 | spot, CTY, telnet, ingest, peer, replay | ADR-0090 | - | `docs/decisions/ADR-0110-call-like-cty-admission.md` |
| ADR-0109 | Support-Agent Bearer Authentication | Accepted | 2026-05-04 | customgpt, supportability, actions, security | - | - | `docs/decisions/ADR-0109-support-agent-bearer-auth.md` |
| ADR-0108 | Repo-Derived Support-Agent Routes | Accepted | 2026-05-04 | customgpt, supportability, actions | - | - | `docs/decisions/ADR-0108-repo-derived-support-agent-routes.md` |
| ADR-0107 | Support-Agent Action Contract Alignment | Accepted | 2026-05-04 | customgpt, supportability, actions | - | - | `docs/decisions/ADR-0107-support-agent-action-contract-alignment.md` |
| ADR-0106 | Go Crawler Entry Comments | Accepted | 2026-05-04 | workflow, code quality, supportability | - | - | `docs/decisions/ADR-0106-go-crawler-entry-comments.md` |
| ADR-0105 | Go Comment Intent Workflow Gate | Accepted | 2026-05-04 | workflow, code quality, supportability | - | - | `docs/decisions/ADR-0105-go-comment-intent-workflow-gate.md` |
| ADR-0104 | Go Comment Intent Pass | Accepted | 2026-05-04 | runtime, replay, supportability | - | - | `docs/decisions/ADR-0104-go-comment-intent-pass.md` |
| ADR-0103 | YAML Documentation Rigor Workflow Gate | Accepted | 2026-05-04 | workflow, config, docs | - | - | `docs/decisions/ADR-0103-yaml-documentation-rigor-workflow-gate.md` |
| ADR-0102 | YAML Key Comment Standard | Accepted | 2026-05-04 | config, docs, customgpt | - | - | `docs/decisions/ADR-0102-yaml-key-comment-standard.md` |
| ADR-0101 | PowerShell Script Header Standard | Accepted | 2026-05-04 | scripts, docs, customgpt | - | - | `docs/decisions/ADR-0101-powershell-script-header-standard.md` |
| ADR-0100 | YAML File Header Standard | Accepted | 2026-05-04 | config, docs, customgpt | - | - | `docs/decisions/ADR-0100-yaml-file-header-standard.md` |
| ADR-0099 | README Telnet-User First Ordering | Accepted | 2026-05-02 | docs, README, telnet | - | - | `docs/decisions/ADR-0099-readme-telnet-user-first-ordering.md` |
| ADR-0098 | Support-Agent Raw GitHub Source URLs | Accepted | 2026-05-02 | docs, customgpt | - | - | `docs/decisions/ADR-0098-support-agent-raw-github-source-urls.md` |
| ADR-0097 | YAML Ownership Boundary Documentation | Accepted | 2026-05-01 | config, docs, customgpt | - | - | `docs/decisions/ADR-0097-yaml-ownership-boundary-docs.md` |
| ADR-0096 | Path Prediction Low Count Logging | Accepted | 2026-04-30 | pathreliability, telnet diagnostics, reports | - | - | `docs/decisions/ADR-0096-path-prediction-low-count-logging.md` |
| ADR-0095 | Path Reliability Receiver Contribution Caps | Accepted | 2026-04-30 | pathreliability, config, telnet diagnostics, reports | - | ADR-0129 (slot ceiling and enforcement mode) | `docs/decisions/ADR-0095-path-reliability-receiver-contribution-caps.md` |
| ADR-0094 | Support-Agent Documentation Impact Gate | Accepted | 2026-04-30 | workflow, docs, customgpt | ADR-0092 (extends compact strict workflow) | - | `docs/decisions/ADR-0094-support-agent-doc-impact-gate.md` |
| ADR-0093 | File-Only Connection and Gate Event Logs | Accepted | 2026-04-30 | logging, telnet, ingest, peer, reputation | - | - | `docs/decisions/ADR-0093-file-only-connection-and-gate-event-logs.md` |
| ADR-0092 | Codex Token-Efficient Strict Workflow | Accepted | 2026-04-30 | workflow, Codex | - | - | `docs/decisions/ADR-0092-codex-token-efficient-strict-workflow.md` |
| ADR-0091 | Peer and DXSummit Skimmer Marker Stripping | Accepted | 2026-04-30 | peer, dxsummit, parser | - | - | `docs/decisions/ADR-0091-peer-dxsummit-skimmer-marker-stripping.md` |
| ADR-0090 | Local Telnet Login Call Validation | Superseded | 2026-04-30 | telnet, CTY, FCC ULS | - | ADR-0110 | `docs/decisions/ADR-0090-local-telnet-login-call-validation.md` |
| ADR-0089 | SET SOLAR Help Routing | Accepted | 2026-04-30 | commands, docs, customgpt | - | - | `docs/decisions/ADR-0089-set-solar-help-routing.md` |
| ADR-0088 | FT2 Ingest-Rate Dashboard Counter | Accepted | 2026-04-26 | stats, ui, dashboard | - | - | `docs/decisions/ADR-0088-ft2-ingest-rate-dashboard-counter.md` |
| ADR-0087 | RBN Spot-Class Admission | Accepted | 2026-04-26 | rbn, parser, replay, call correction | - | - | `docs/decisions/ADR-0087-rbn-spot-class-admission.md` |
| ADR-0086 | User Path Sample Floor | Accepted | 2026-04-26 | telnet, pathreliability, user records | - | - | `docs/decisions/ADR-0086-user-path-sample-floor.md` |
| ADR-0085 | Path Reliability Minimum Observation Count | Accepted | 2026-04-26 | pathreliability, config, telnet diagnostics | - | - | `docs/decisions/ADR-0085-path-reliability-min-observation-count.md` |
| ADR-0084 | SET DIAG Modes and Path Observation Count | Accepted | 2026-04-26 | telnet diagnostics, pathreliability | - | - | `docs/decisions/ADR-0084-set-diag-modes-and-path-count.md` |
| ADR-0083 | Operator Docs Follow-Up | Accepted | 2026-04-25 | docs, operator guide, Linux service | - | - | `docs/decisions/ADR-0083-operator-docs-follow-up.md` |
| ADR-0082 | YAML Documentation Completeness Pass | Accepted | 2026-04-25 | config, docs, yaml | - | - | `docs/decisions/ADR-0082-yaml-documentation-completeness-pass.md` |
| ADR-0081 | Operator Docs Completeness Pass | Accepted | 2026-04-25 | docs, operator guide, release package | - | - | `docs/decisions/ADR-0081-operator-docs-completeness-pass.md` |
| ADR-0080 | Custom SCP Retained Heap Layout | Accepted | 2026-04-25 | custom_scp, retained state, profiling | - | - | `docs/decisions/ADR-0080-custom-scp-retained-heap-layout.md` |
| ADR-0079 | WHOSPOTSME Optional Band and Compact Output | Accepted | 2026-04-24 | commands, docs | ADR-0071 (operator command/output contract) | - | `docs/decisions/ADR-0079-whospotsme-optional-band-compact-output.md` |
| ADR-0078 | Release Package Clean Source Gate | Accepted | 2026-04-24 | release packaging, build reproducibility | - | - | `docs/decisions/ADR-0078-release-package-clean-source-gate.md` |
| ADR-0077 | Compile-Date Binary Version | Accepted | 2026-04-24 | build metadata, release packaging, operator console | - | - | `docs/decisions/ADR-0077-compile-date-binary-version.md` |
| ADR-0076 | GitHub Release Package | Accepted | 2026-04-24 | release packaging, deployment, GitHub Releases | - | - | `docs/decisions/ADR-0076-github-release-package.md` |
| ADR-0075 | Output Pipeline Context Allocation | Accepted | 2026-04-24 | main output pipeline, FT confidence, allocation | - | - | `docs/decisions/ADR-0075-output-pipeline-context-allocation.md` |
| ADR-0074 | Go Runtime Memory Tuning | Accepted | 2026-04-24 | config, startup, runtime memory | - | - | `docs/decisions/ADR-0074-go-runtime-memory-tuning.md` |
| ADR-0073 | Root Live-Runtime Boundary and Root Artifact Hygiene | Accepted | 2026-04-23 | repo structure, internal/cluster, cmd, docs | - | - | `docs/decisions/ADR-0073-root-live-runtime-boundary-and-root-artifact-hygiene.md` |
| ADR-0072 | Mandatory ADR Handling for Non-trivial Work | Accepted | 2026-04-23 | workflow, decision-memory | - | - | `docs/decisions/ADR-0072-mandatory-adr-handling-for-non-trivial-work.md` |
| ADR-0002 | UI v2 Render Pipeline Optimization | Accepted | 2026-02-07 | ui/tview-v2 | - | - | `docs/decisions/ADR-0002-ui-v2-render-pipeline.md` |
| ADR-0003 | Call Correction Anchor Gating and Config-Driven Determinism | Accepted | 2026-02-07 | spot/correction | - | - | `docs/decisions/ADR-0003-call-correction-anchor-gating.md` |
| ADR-0004 | Slash-Variant Precedence and Canonical Grouping in Call Correction | Accepted | 2026-02-08 | spot/correction | - | - | `docs/decisions/ADR-0004-call-correction-slash-precedence.md` |
| ADR-0005 | Mode-Specific Spotter Reliability Selection for Call Correction | Accepted | 2026-02-13 | spot/correction | - | - | `docs/decisions/ADR-0005-mode-specific-spotter-reliability.md` |
| ADR-0006 | Confusion-Model Tie-Break Ranking for Call Correction | Accepted | 2026-02-13 | spot/correction | - | - | `docs/decisions/ADR-0006-confusion-model-tie-break-ranking.md` |
| ADR-0007 | Call-Correction Top-K Evaluation, Strict Prior Bonus, and Decision Counters | Accepted | 2026-02-13 | spot/correction | - | - | `docs/decisions/ADR-0007-call-correction-topk-prior-bonus-observability.md` |
| ADR-0008 | Call Correction Recent-On-Band Bonus (Min-Reports Only) | Accepted | 2026-02-14 | spot/correction | - | - | `docs/decisions/ADR-0008-call-correction-recent-band-bonus.md` |
| ADR-0009 | Call Correction Stacked Prior + Recent Bonus for Min-Reports | Accepted | 2026-02-14 | spot/correction | ADR-0007 (prior-bonus scope) | - | `docs/decisions/ADR-0009-call-correction-stacked-prior-recent-bonus.md` |
| ADR-0010 | S Glyph Confidence Floor Includes Recent-On-Band Admission | Accepted | 2026-02-16 | main/spot confidence | - | - | `docs/decisions/ADR-0010-s-glyph-recent-on-band-floor.md` |
| ADR-0011 | SHOW DX / SHOW MYDX Optional DXCC Selector | Accepted | 2026-02-17 | commands/processor | - | - | `docs/decisions/ADR-0011-show-history-dxcc-selector.md` |
| ADR-0012 | CC Dialect Accepts SHOW DX / SH DX History Aliases | Accepted | 2026-02-17 | commands/processor | - | - | `docs/decisions/ADR-0012-cc-show-dx-alias.md` |
| ADR-0013 | Telnet Stabilizer for Risky Call-Correction Output | Accepted | 2026-02-17 | main/telnet fan-out | - | - | `docs/decisions/ADR-0013-call-correction-telnet-stabilizer.md` |
| ADR-0014 | Call-Correction Family Policy: Slash Threshold, Truncation Advantage Rail, and Telnet Family Suppression | Accepted | 2026-02-22 | spot/correction, main/telnet fan-out | - | - | `docs/decisions/ADR-0014-call-correction-family-policy.md` |
| ADR-0015 | YAML-Driven Call-Correction Family Policy Knobs | Accepted | 2026-02-23 | config, spot/correction, main/telnet fan-out | ADR-0014 (config surface) | - | `docs/decisions/ADR-0015-call-correction-family-policy-yamlization.md` |
| ADR-0016 | Family-Aware Recent/Stabilizer Keys, Suppressor Edge Guard, and Truncation Bonus Rails | Accepted | 2026-02-23 | main/telnet fan-out, spot/correction, config | - | - | `docs/decisions/ADR-0016-call-correction-family-aware-recent-and-delta2-rails.md` |
| ADR-0017 | Shared Daily Scheduler Parsing and Rate-Counter Semantics | Accepted | 2026-02-23 | internal/schedule, internal/ratelimit, main, uls, reputation, pskreporter, ingest | - | - | `docs/decisions/ADR-0017-scheduler-ratelimit-unification.md` |
| ADR-0018 | Filter Allow/Block Mutation Helper and Telnet Domain Handler Cleanup | Accepted | 2026-02-23 | filter, telnet/filter command engine | - | - | `docs/decisions/ADR-0018-filter-mutation-and-telnet-handler-consolidation.md` |
| ADR-0019 | Shared Spot Distance Core and Periodic Cleanup Runner | Accepted | 2026-02-23 | spot/correction, spot cleanup lifecycle | - | - | `docs/decisions/ADR-0019-spot-distance-core-and-cleanup-runner.md` |
| ADR-0020 | Multi-Check Telnet Stabilizer Retries With Bounded Re-Delay | Accepted | 2026-02-23 | main/telnet fan-out, config, stabilizer scheduler | ADR-0013 (single delayed check behavior) | - | `docs/decisions/ADR-0020-stabilizer-max-checks-retries.md` |
| ADR-0021 | Call-Correction Split-Evidence Ambiguity Guard and Validated Non-Winner Quality Penalty Rail | Accepted | 2026-02-23 | spot/correction | - | - | `docs/decisions/ADR-0021-call-correction-ambiguity-guard-and-quality-rail.md` |
| ADR-0022 | Phase 2 Signal Resolver Shadow Mode | Proposed | 2026-02-23 | spot/correction, main output pipeline, stats | - | - | `docs/decisions/ADR-0022-phase2-signal-resolver-shadow-mode.md` |
| ADR-0023 | Consolidate Historical Analysis into Replay Tool | Accepted | 2026-02-24 | cmd/rbn_replay, analysis workflow, docs | - | - | `docs/decisions/ADR-0023-replay-analysis-tool-consolidation.md` |
| ADR-0024 | Replay Dual-Method Recall/Stability Metrics | Superseded | 2026-02-24 | cmd/rbn_replay, replay evidence artifacts | - | ADR-0037 | `docs/decisions/ADR-0024-replay-dual-method-stability-metrics.md` |
| ADR-0025 | Replay Run History and Local Comparison Ledger | Accepted | 2026-02-24 | cmd/rbn_replay, replay operations | - | - | `docs/decisions/ADR-0025-replay-run-history-and-local-comparison.md` |
| ADR-0026 | Resolver Primary Switchover Mode | Accepted | 2026-02-25 | main output pipeline, spot/correction, config | - | - | `docs/decisions/ADR-0026-resolver-primary-switchover-mode.md` |
| ADR-0027 | Resolver Reliability-Weighted Voting With Fixed-Point Determinism | Accepted | 2026-02-25 | spot/signal_resolver, main output pipeline | - | - | `docs/decisions/ADR-0027-resolver-reliability-weighted-voting.md` |
| ADR-0028 | Shared Call-Correction Flow for Main and Replay | Accepted | 2026-02-26 | internal/correctionflow, main output pipeline, cmd/rbn_replay | - | - | `docs/decisions/ADR-0028-shared-correctionflow-main-replay.md` |
| ADR-0029 | Stabilizer Targeted Delay for Resolver Ambiguity and Low-Confidence P | Accepted | 2026-02-26 | main/telnet fan-out, config, stats | - | - | `docs/decisions/ADR-0029-stabilizer-targeted-ambiguity-and-lowp-delay.md` |
| ADR-0030 | Shared Stabilizer Policy Parity Across Main and Replay | Accepted | 2026-02-26 | internal/correctionflow, main output pipeline, cmd/rbn_replay | - | - | `docs/decisions/ADR-0030-shared-stabilizer-policy-parity-main-replay.md` |
| ADR-0031 | Skew Selection Uses Absolute Skew Threshold | Accepted | 2026-02-26 | config, skew, main, cmd/rbnskewfetch | - | - | `docs/decisions/ADR-0031-skew-selection-min-abs-skew.md` |
| ADR-0032 | Resolver-Primary Family-Gate Parity and Conservative Contested Confidence Glyphs | Accepted | 2026-02-26 | main output pipeline, spot/correction | - | - | `docs/decisions/ADR-0032-resolver-primary-family-gate-parity-and-conservative-contested-glyphs.md` |
| ADR-0033 | Resolver Neighborhood Selection and Contested Edit-Neighbor Rails | Accepted | 2026-02-26 | main output pipeline, correctionflow, telnet suppression, replay, config | - | - | `docs/decisions/ADR-0033-resolver-neighborhood-and-contested-edit-neighbor-rails.md` |
| ADR-0034 | Resolver-Primary Conservative Recent-On-Band +1 Corroborator Rail | Accepted | 2026-02-26 | main output pipeline, spot/correction, correctionflow, replay, config | - | - | `docs/decisions/ADR-0034-resolver-recent-plus1-corroborator-rail.md` |
| ADR-0035 | Resolver Neighborhood Anchor-Scoped Comparability Rails | Accepted | 2026-02-26 | internal/correctionflow, main output pipeline, replay, config | - | - | `docs/decisions/ADR-0035-resolver-neighborhood-anchor-comparability-rails.md` |
| ADR-0036 | Resolver Confusion Tie-Break and Runtime/Replay Reliability-Parity Wiring | Accepted | 2026-02-26 | main output pipeline, cmd/rbn_replay, cmd/callcorr_reveng_rebuilt, internal/correctionflow, spot/signal_resolver | - | - | `docs/decisions/ADR-0036-resolver-confusion-tiebreak-runtime-replay-parity.md` |
| ADR-0037 | Replay Resolver-Only Artifact Schema | Accepted | 2026-02-26 | cmd/rbn_replay, replay artifacts, replay history | ADR-0024 | - | `docs/decisions/ADR-0037-replay-resolver-only-artifacts.md` |
| ADR-0038 | Fixed-Lag Temporal Decoder for Resolver-Primary Runtime/Replay Parity | Accepted | 2026-02-27 | internal/correctionflow, main output pipeline, cmd/rbn_replay, config, stats | - | - | `docs/decisions/ADR-0038-fixed-lag-temporal-decoder-main-replay-parity.md` |
| ADR-0039 | Custom SCP Runtime Evidence and Shared Pebble Resilience Helper | Accepted | 2026-02-27 | main output pipeline, spot/correction, config, persistence | - | - | `docs/decisions/ADR-0039-custom-scp-runtime-evidence-and-shared-pebble-resilience.md` |
| ADR-0040 | Overview v2 Observability Contract Refresh | Accepted | 2026-02-27 | ui/tview-v2, main stats assembly | - | - | `docs/decisions/ADR-0040-overview-v2-observability-contract-refresh.md` |
| ADR-0041 | Tier-A Prelogin Admission Gate for Telnet DoS Resilience | Accepted | 2026-02-27 | telnet/session admission, config, main network observability | - | - | `docs/decisions/ADR-0041-telnet-tier-a-prelogin-admission-gate.md` |
| ADR-0042 | Telnet Reject Workers, Immutable Shard Cache, and Writer Micro-Batching | Accepted | 2026-02-27 | telnet/accept path, fan-out cache, writer loop, config | - | - | `docs/decisions/ADR-0042-telnet-reject-workers-and-writer-microbatching.md` |
| ADR-0043 | Custom SCP Admission Restricted to V Confidence | Accepted | 2026-02-28 | spot/custom_scp, main output pipeline, confidence semantics | ADR-0039 (admission policy refinement) | - | `docs/decisions/ADR-0043-custom-scp-v-only-admission.md` |
| ADR-0044 | Resolver Bayesian Capped Gate Bonus for Distance-1/2 Near-Threshold Winners | Accepted | 2026-02-28 | spot/correction, main output pipeline, replay, config | - | - | `docs/decisions/ADR-0044-resolver-bayes-capped-gate-bonus.md` |
| ADR-0045 | 10 Hz Frequency Resolution and Fixed Telnet Mode Column | Accepted | 2026-02-28 | spot, skew, main output pipeline, telnet formatting | - | - | `docs/decisions/ADR-0045-10hz-frequency-resolution-fixed-telnet-mode-column.md` |
| ADR-0046 | Stabilizer Delay Eligibility Scoped to ?, S, and P Glyphs | Accepted | 2026-03-01 | internal/correctionflow, main output pipeline, replay parity, docs | - | - | `docs/decisions/ADR-0046-stabilizer-delay-eligibility-unknown-s-p-only.md` |
| ADR-0047 | Peering Outbound Sessions Require Explicit Enablement | Accepted | 2026-03-03 | config, peer | - | - | `docs/decisions/ADR-0047-peering-explicit-enable-semantics.md` |
| ADR-0048 | Multi-Dimensional Prelogin Admission and Guardrailed Reject Logging | Accepted | 2026-03-03 | telnet/session admission, reputation integration, config, observability | ADR-0041 (extended admission dimensions and logging controls) | - | `docs/decisions/ADR-0048-multi-dimensional-prelogin-admission-and-logging-guardrails.md` |
| ADR-0049 | Peer Publish Limited to Local Non-Test Human/Manual Sources | Accepted | 2026-03-03 | main output pipeline, peer | - | - | `docs/decisions/ADR-0049-peer-publish-local-human-only.md` |
| ADR-0050 | Peering Hop-Suffix Canonicalization, Semantic PC92 Dedupe, and Overlong Diagnostics | Accepted | 2026-03-05 | peer/protocol, peer/manager, peer/reader | - | - | `docs/decisions/ADR-0050-peering-hop-canonicalization-pc92-dedupe-and-overlong-diagnostics.md` |
| ADR-0051 | Mode Inference Provenance and Band Default Policy | Superseded | 2026-03-06 | spot, rbn, pskreporter, commands | - | ADR-0052 | `docs/decisions/ADR-0051-mode-inference-provenance-and-band-default-policy.md` |
| ADR-0052 | Region-Aware Final Mode Inference and UNKNOWN Filter Token | Accepted | 2026-03-07 | spot, main output pipeline, filter, telnet, docs | ADR-0051 | - | `docs/decisions/ADR-0052-region-aware-mode-inference-and-unknown-filter-token.md` |
| ADR-0053 | Peering Receive-Only Forwarding Knob With Local DX Exception | Accepted | 2026-03-07 | config, peer, main output pipeline | - | - | `docs/decisions/ADR-0053-peering-receive-only-forwarding-knob.md` |
| ADR-0054 | Peering Control-Plane Priority and Local-Acceptance Relay Gate | Accepted | 2026-03-07 | peer | - | - | `docs/decisions/ADR-0054-peering-control-priority-and-local-acceptance-relay.md` |
| ADR-0055 | FT2 Explicit Mode Support and Filter Contract | Accepted | 2026-03-27 | spot, pskreporter, filter, telnet, commands, docs | - | - | `docs/decisions/ADR-0055-ft2-explicit-mode-support-and-filter-contract.md` |
| ADR-0056 | Local Self-Spots Use an Operator-Authoritative V Path | Accepted | 2026-03-27 | commands, main output pipeline, telnet, custom_scp, docs | - | - | `docs/decisions/ADR-0056-local-self-spots-operator-authoritative-v-path.md` |
| ADR-0057 | FT Confidence Uses Bounded Main-Loop Corroboration | Superseded | 2026-04-07 | main output pipeline, filter, custom_scp, docs | - | ADR-0059 | `docs/decisions/ADR-0057-ft-confidence-corroboration.md` |
| ADR-0058 | PSKReporter FT Frequency Canonicalization With Preserved Observed RF | Accepted | 2026-04-08 | pskreporter ingest, spot, archive, mode inference, FT confidence | - | - | `docs/decisions/ADR-0058-pskreporter-ft-frequency-canonicalization.md` |
| ADR-0059 | Slot-Aware FT Confidence Timing and RBN Arrival Fallback | Superseded | 2026-04-08 | main output pipeline, FT confidence, docs | ADR-0057 (timing/key model) | ADR-0060 | `docs/decisions/ADR-0059-slot-aware-ft-confidence-timing.md` |
| ADR-0060 | Source-Aware FT Burst Clustering for Live Corroboration | Accepted | 2026-04-08 | main output pipeline, FT confidence, stats, docs | ADR-0059 (slot-anchor model) | - | `docs/decisions/ADR-0060-source-aware-ft-burst-clustering.md` |
| ADR-0061 | YAML-Driven FT Corroboration Timing and Threshold Knobs | Accepted | 2026-04-08 | config, main output pipeline, FT confidence, docs | ADR-0060 (code-bound FT timing/threshold clause) | - | `docs/decisions/ADR-0061-ft-corroboration-yaml-knobs.md` |
| ADR-0062 | Shared Ingest Flood Control With Required YAML Policy | Accepted | 2026-04-08 | config, ingest, dedupe, stats, docs | - | - | `docs/decisions/ADR-0062-shared-ingest-flood-control.md` |
| ADR-0063 | Configurable Telnet Bulletin Dedupe | Accepted | 2026-04-19 | config, peer, telnet fan-out | - | - | `docs/decisions/ADR-0063-configurable-bulletin-dedupe.md` |
| ADR-0064 | Band-Specific Path Noise Penalties | Superseded | 2026-04-20 | pathreliability, telnet, config | - | ADR-0124 | `docs/decisions/ADR-0064-band-specific-path-noise-penalties.md` |
| ADR-0065 | Path Reliability Freshness Gate | Accepted | 2026-04-20 | pathreliability, telnet, reports, config | - | - | `docs/decisions/ADR-0065-path-reliability-freshness-gate.md` |
| ADR-0066 | DXSummit HTTP Ingest | Accepted | 2026-04-21 | dxsummit, config, spot, runtime, docs | - | - | `docs/decisions/ADR-0066-dxsummit-http-ingest.md` |
| ADR-0067 | Centralized YAML Settings Enforcement | Accepted | 2026-04-22 | config, pathreliability, solarweather, spot, runtime, reports | - | - | `docs/decisions/ADR-0067-centralized-yaml-settings-enforcement.md` |
| ADR-0068 | EVENT Comment Tags and Filter Contract | Superseded | 2026-04-22 | spot, filter, telnet, archive, commands, docs | - | ADR-0070 | `docs/decisions/ADR-0068-event-comment-tags-and-filter-contract.md` |
| ADR-0069 | Single Spot Taxonomy YAML | Accepted | 2026-04-22 | spot, config, filter, telnet, pskreporter, pathreliability | - | - | `docs/decisions/ADR-0069-single-spot-taxonomy-yaml.md` |
| ADR-0070 | EVENT Filters Preserve Untagged Spots | Accepted | 2026-04-23 | filter, telnet, commands, docs | ADR-0068 | - | `docs/decisions/ADR-0070-event-filters-preserve-untagged-spots.md` |
| ADR-0071 | WHOSPOTSME Rolling Country Summary | Accepted | 2026-04-23 | commands, config, main output pipeline, spot | - | ADR-0079 (operator command/output contract) | `docs/decisions/ADR-0071-whospotsme-rolling-country-summary.md` |
