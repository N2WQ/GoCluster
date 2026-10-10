# ADR-0276: Toxicity Configuration Zero And Duration Admission

- Status: Accepted
- Date: 2026-10-10
- Decision Origin: Troubleshooting chat

## Context

The optional classifier loader already treats numeric zero as a request for six
built-in defaults. Approved v3 preserves that explicit user selection. Missing
and null keys are different: the strict loader requires all ten keys in a
present file, including when disabled. ADR-0067's general no-hidden-default rule
needs this narrow documented exception, rather than silently changing zero to a
disabled setting. The file remains optional deployment/runtime tool-boundary
config and is not merged into main configuration.

A positive timeout above the raw consumer ceiling silently falls back to 1200
ms. Cache TTL conversion from seconds can overflow a Go duration. Both can make
effective settings contradict the operator's positive YAML choice.

## Decision

- Keep `internal/toxicity.LoadConfig` as the production admission boundary.
  Missing or blank paths disable the feature. Present files retain strict known
  fields, required non-null keys and existing yaml.v3 scalar conversion.
- Explicit numeric zero means timeout 1200 ms, workers 2, queue 128, cache 4096
  entries, TTL 86400 seconds or maximum payload 512 bytes, respectively.
  Negative numeric values fail admission. Explicit false remains disabled.
- Refuse `timeout_ms` over 2147483647 before construction, matching the existing
  consumer ceiling. Refuse `cache_ttl_seconds` whose conversion to nanoseconds
  exceeds the maximum Go duration (9223372036 whole seconds). Native integer
  decoding bounds still apply on each architecture.
- Apply checks to disabled and enabled present files, before credential checks
  and before queue/cache allocation or workers start. Never print credentials.
- Preserve direct constructor semantics: nonpositive workers/queue disable the
  classifier, nonpositive cache count/TTL disable caching, nonpositive comment
  limits retain full payloads, and raw timeout fallback stays unchanged.
  Arbitrary malformed raw TTL overflow is outside this loader contract.
- Keep the existing drain formula and 500 ms to five-second clamp. Accepted
  timeout values are safe for its conversion and doubling.

## Alternatives considered

1. Turn YAML zero into disablement. Rejected: reverses the explicit user choice
   and changes the existing loader contract.
2. Default omitted keys. Rejected: hides incomplete configuration and violates
   required presence.
3. Accept unsupported positives and rely on raw fallback/overflow. Rejected:
   configuration readback would not describe the consumer's settings.
4. Add arbitrary capacity ceilings or change fractional scalar decoding.
   Outside this approved patch; representability is not a capacity recommendation.

## Consequences

### Benefits
- Zero/default semantics are discoverable and observable at consumers.
- Unsupported positive durations fail startup with a key-specific error.
- Raw constructors, optional startup and shipped positive settings retain behavior.

### Risks
- A previously accepted unsupported positive duration now fails load, even in a
  disabled present file. Operators must correct the named key.
- Very large but representable capacities are not practical operating advice;
  this patch does not introduce new arbitrary sizing policy.

### Operational impact
- No new key, schema, protocol, secret value, scientific claim or queue lifecycle.
- All required keys remain explicit. Missing optional file still disables the
  feature; enabled configuration still requires the existing safe-gate table.

## Links
- Related issues/PRs/commits: approved v3 checkpoint 21.
- Related tests: `internal/toxicity/config_contract_test.go`,
  `config_consumer_contract_test.go`, `internal/cluster/toxicity_config_contract_test.go`.
- Related docs: `data/config/README.md`, `data/config/toxicity.yaml`,
  `customgpt/support-cards/toxicity-filter.md`, `docs/command-fix-execution.md`.
- Related TSRs: [TSR-0070](../troubleshooting/TSR-0070-toxicity-positive-duration-admission.md).
- Supersedes / superseded by: none; narrows the documented exception in
  [ADR-0067](ADR-0067-centralized-yaml-settings-enforcement.md) and clarifies
  [ADR-0114](ADR-0114-human-comment-toxicity-classifier.md).
