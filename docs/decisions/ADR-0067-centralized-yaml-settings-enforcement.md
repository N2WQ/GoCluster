# ADR-0067: Centralized YAML Settings Enforcement

- Status: Accepted
- Date: 2026-04-22
- Decision Origin: Design

## Context

Runtime settings had two loading paths: the main `config.Load` directory merge
and package-local loaders for path reliability, solar weather, and IARU
reference tables. The package-local loaders started from Go defaults and overlaid
YAML, which meant omitted YAML keys could silently change behavior. One concrete
defect was `path_reliability.mode_offsets.ft4: 0` being overwritten by the
package fallback value.

The config directory also contains files whose top-level shape is not the main
`config.Config` shape: feature-root files, reference tables, and optional
secret/tool config. Blindly merging every YAML file made those categories
implicit and made accidental files hard to detect.

## Decision

`data/config` is the single source of truth for runtime settings and required
reference tables.

The config loader uses a filename registry:

- merged runtime files populate `config.Config`;
- `path_reliability.yaml` and `solarweather.yaml` are loaded as typed
  feature-root config while preserving their file-local YAML shape and requiring
  explicit non-null YAML-owned leaves;
- `iaru_regions.yaml` and `iaru_mode_inference.yaml` are required reference
  tables loaded from the active config directory at startup;
- `openai.yaml` remains optional for server startup and is validated only by
  propagation-report LLM generation.

Unknown YAML filenames, unknown runtime keys, missing or null required runtime
leaves, malformed feature-root config, and missing/malformed required reference
tables fail load/startup. Documented zero sentinels remain operator-owned values
and must not be normalized into code defaults. Go code may still contain
validation constants, enum names, algorithm constants, and test-helper configs,
but not hidden runtime fallback settings for YAML-owned behavior.

## Alternatives considered

1. Keep blind directory merging and patch only path reliability.
   - Rejected because solar weather, main config normalizers, and IARU tables
     had the same class of hidden fallback risk.
2. Wrap every YAML file under one `config.Config` schema.
   - Rejected because it would churn file shapes for feature-root and reference
     table files without improving operator clarity.
3. Keep optional built-in IARU reference tables.
   - Rejected because those tables are YAML-owned operational reference data.

## Consequences

### Benefits

- Production YAML that is incomplete fails early with a file/key error.
- Valid explicit zero values remain meaningful, including
  `path_reliability.mode_offsets.ft4: 0`,
  `telnet.broadcast_batch_interval_ms: 0`, and keepalive/config timer disables.
- `HELP`, propagation reports, and runtime path/solar behavior use the same
  centrally loaded effective config.
- Operators can audit settings by reviewing the active `data/config` directory,
  not hidden package defaults.
- Propagation-report LLM config is validated before report files are written
  when LLM generation is enabled, while `openai.yaml` stays outside server
  startup.

### Risks

- Existing production config directories that omitted YAML-owned settings will
  fail startup until the missing keys are added.
- Existing production config directories that used `null` for YAML-owned
  settings will fail instead of falling through to a Go fallback.
- The stricter loader can reject previously ignored typo keys.
- Tests must use complete config fixtures instead of minimal partial YAML when
  exercising production load behavior.

### Operational impact

No telnet protocol, slow-client, overload, reconnect, or shutdown behavior
changes when YAML is complete. Operators who set documented zero sentinels now
get the documented disabled/immediate behavior. The user-visible change is
earlier startup/report generation failure for missing or malformed config
instead of silent fallback.

## Links

- Narrow optional-classifier exception: [ADR-0276](ADR-0276-toxicity-config-zero-and-duration-admission.md)
  documents explicit numeric zero as a request for existing toxicity defaults,
  while retaining required presence and refusing unsupported positive durations.

- Related tests: `config`, `pathreliability`, `solarweather`, `spot`,
  `commands`, `internal/propreport`
- Related docs: `data/config/README.md`, `pathreliability/README.md`,
  `README.md`
- Related TSRs: none
