---
name: "go-config-contract-audit"
description: "Use before designing, reviewing, or modifying Go YAML config loading, schema validation, defaults, operator settings, reference tables, optional secret/tool config, or config-owned runtime behavior."
---

# Go Config Contract Audit

Use this skill before implementing or approving changes that touch YAML config,
loader behavior, config structs, runtime defaults, reference-table loading, or
operator-visible settings.

## Required workflow

1. Classify every touched config file.
   - merged runtime config
   - feature-root config
   - required reference table
   - optional tool/secret config
   - test fixture

2. Identify the single source of truth.
   - Name the allowed loader path for each file.
   - Runtime code must not fall back to `DefaultConfig()` for YAML-owned settings.
   - Runtime code must not blindly merge unrelated YAML files.
   - Any package-local loader bypass must be explicitly documented and approved.

3. Audit defaults and presence.
   - Search for `DefaultConfig`, `default`, `if x == 0`, `yaml.Unmarshal`,
     `omitempty`, and loader-specific fallback paths.
   - Classify each hit as validation constant, algorithm constant, test helper,
     compatibility boundary, or illegal runtime default.
   - Require presence checks for YAML-owned settings.
   - Reject required `null` values.

4. Prove sentinel behavior.
   - Explicit `0` survives where documented.
   - Explicit `false` survives where documented.
   - Empty string, list, or map values are accepted only where documented.
   - Negative values fail unless explicitly supported.

5. Audit consumers.
   - List downstream runtime consumers.
   - Identify code that can re-default or reinterpret loaded values.
   - Add consumer-level tests for every documented sentinel.

6. Validate optional secret/tool config.
   - Keep optional config optional at server startup unless required by that
     feature boundary.
   - Use strict known-field validation at the use boundary.
   - Validate required non-secret keys.
   - Never log, print, commit, or document secret values.

## Output expectations

Report the material config ownership, defaulting, validation, and consumer
findings without requiring a fixed section or no-change statement.

Every YAML-owned setting changed or newly enforced must map to:
- YAML file/key
- loader validation
- runtime consumer
- regression test
- docs or ADR update when operator-visible
