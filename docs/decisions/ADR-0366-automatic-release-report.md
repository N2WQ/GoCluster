# ADR-0366: Automatic Release Reports

- Status: Accepted
- Date: 2026-11-20
- Decision Origin: Owner-selected fully automatic releases; Scope Ledger v1

## Context

Requiring an explicit previous tag and committed reviewed notes made a normal
release require manual inputs. Operators still need a trustworthy baseline,
source comparison and public configuration inventory.

## Decision

Publishing with no arguments selects the destination repository's latest
published stable release. Verify its remote tag and strict ancestry of the
captured candidate, obtaining missing Git objects and shallow history without
moving local branches or tags or writing `FETCH_HEAD`. Refuse publication if
the selection or comparison cannot be verified, and recheck the selected latest
release and tag before publication. An explicit baseline remains available.

Generate the same automatic report for the ZIP and GitHub release. It links the
exact source comparison and summarizes the public YAML diff. It explicitly says
that it is not reviewed for all operator-facing changes and may omit required
actions. No per-release notes file is required. Preserve the YAML manifest,
clean-source gate, duplicate checks, output ownership and publication safety.

## Alternatives considered

Manual reviewed prose can better explain code-only upgrades, but it requires
per-release preparation. The owner selected automatic reports.

## Consequences

### Benefits

Normal publication needs no baseline argument or manually prepared notes.

### Risks

Operators must review changes themselves; neither unchanged YAML nor a source
comparison proves that no action is needed. The latest-release recheck narrows
but cannot eliminate a race with another publisher after the check.

### Operational impact

Missing baseline history, access, tools, or a stable remote state stop
publication rather than selecting an older release. Package-only previews
retain their existing options.

## Links

- Partially supersedes [ADR-0362](ADR-0362-versioned-config-release-report.md)
  for baseline selection and reviewed prose only.
- Preserves [release preparation safety](ADR-0242-release-preparation-safety.md)
  and [automatic tag identity](ADR-0252-automatic-commit-suffix-release-tags.md).
- [Maintainer instructions](../../scripts/README.md#release-preparation) and
  [operator upgrade guidance](../OPERATOR_GUIDE.md#upgrade-a-node).
- Tests: `scripts/test-release-identity.ps1` and `scripts/test-release-safety.ps1`.
