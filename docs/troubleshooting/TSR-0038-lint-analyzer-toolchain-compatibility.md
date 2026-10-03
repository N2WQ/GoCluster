# TSR-0038 - Lint Analyzer Toolchain Compatibility

Status: Resolved
Date Opened: 2026-10-03
Date Resolved: 2026-10-03
Owner: GoCluster maintainers
Technical Area: local Go static analysis
Trigger Source: Chat request
Led To ADR(s): none
Tags: lint, staticcheck, golangci-lint, toolchain

## RCA Summary

- What happened: Staticcheck failed importing packages, and golangci-lint
  panicked before producing source diagnostics during the lint audit.
- Why: active Go 1.27.1 produced packages newer than the installed analyzers
  supported. golangci-lint 2.11.4 was built with Go 1.26.2; Staticcheck was
  2026.1 (v0.7.0).
- What fixed it: selecting Go 1.26.2 with process-local `GOTOOLCHAIN` allowed
  both analyzers to finish. No tool upgrade or module change was necessary.
- How we know: the same checkout changed from loader failures to a passing
  standalone Staticcheck scan and a completed golangci-lint scan reporting
  actionable source findings.
- Operator/support answer: match the development analyzers to their package
  loading toolchain. This failure does not diagnose a deployed cluster issue.

## Triggering Request

- Request date: 2026-10-03.
- Request summary: find, review, and propose fixes for lint errors and warnings.
- Request reference: chat Scope Ledger v1, authorized by `Approved v1`.

## Symptoms and Impact

Staticcheck reported `export data version 4 is greater than maximum supported
version 2`. golangci-lint panicked with `file requires newer Go version go1.27
(application built with go1.26)`. Neither failure establishes source cleanliness.
`go vet ./...` and Actionlint succeeded in the original environment.

## Timeline

1. 2026-10-03: reproduced analyzer failures with active Go 1.27.1.
2. 2026-10-03: selected Go 1.26.2 without changing persistent settings.
3. 2026-10-03: completed standalone and configured lint scans; began the
   separately approved source cleanup on `fix_lint`.

## Hypotheses and Tests

1. The import errors diagnose repository compilation defects: rejected as an
   explanation of these failures. Vet succeeded, and selecting Go 1.26.2
   removed the loader failure without changing source.
2. The analyzers cannot consume active Go 1.27 packages: supported by their
   error messages, tool versions, and successful matching-toolchain rerun.
3. A completed analyzer scan implies every build variant is clean: rejected.
   Default, qualification, SQLite qualification, and race-tag variants exposed
   different findings. Linux scans used `CGO_ENABLED=0` and do not cover
   Linux CGO code such as `cmd/h3gen`.

## Findings

- Root cause: local analyzer/toolchain incompatibility prevented analysis.
- Contributing factors: the repository targets Go 1.26, while the active
  machine installation was Go 1.27.1.
- No durable decision changes: this is a reproducible development workaround,
  not a new toolchain, dependency, or validation policy.

## Decision Linkage

- ADR created/updated: none.
- Decision delta summary: none; existing analyzer pins and lint rules remain.
- Contract/behavior changes: No contract changes.

## Verification and Monitoring

- Before source edits, `GOTOOLCHAIN=go1.26.2` with `staticcheck ./...` passed.
- The same environment with `golangci-lint run ./... --config=.golangci.yaml`
  completed and reported 42 default Windows findings rather than panicking.
- Tagged/platform discovery identified 90 distinct diagnostics across the
  scanned configurations. This record resolves the loader failure only;
  source-cleanup validation is reported separately at closeout.
- Monitor analyzer versions and the actual package-loading Go version when
  either tool begins failing before reporting source diagnostics.
- Rollback trigger: do not use a workaround that changes module semantics or
  hides required diagnostics; reassess compatibility instead.

## References

- Issue(s): chat lint audit, 2026-10-03.
- PR(s): none.
- Commit(s): discovery baseline `da2cc5ff5fa7ebae44194a9eaab4cdaa7f9e5975`.
- Related ADR(s): ADR-0227 (existing validation backstops).
- Related docs: [Development runbook](../dev-runbook.md),
  [CI workflow](../../.github/workflows/ci.yml),
  [lint configuration](../../.golangci.yaml).
