## Summary

- What changed:
- Why:

## Validation

- [ ] `go test ./...`
- [ ] `golangci-lint run ./...`

## Comment Policy (Required)

Reference: `docs/comment-policy.md`

### CI-enforced (must pass)

- [ ] Exported declarations include doc comments (`revive: exported`)
- [ ] Package comments are present (`revive: package-comments`)
- [ ] Comment spacing is clean (`revive: comment-spacings`)
- [ ] `nolint` directives include rationale and are used (`nolintlint`)
- [ ] Comment spelling checks pass (`misspell`)

### Reviewer-enforced (confirm for touched code paths)

- [ ] Comments explain `why` and invariants, not just `what`
- [ ] Concurrency ownership/lifecycle is documented where non-obvious
- [ ] Backpressure/drop/disconnect semantics are documented where behavior changed
- [ ] Deadline/timeout rationale is documented where behavior changed
- [ ] Operator-facing metrics/log meaning is documented where behavior changed

## Risks and Rollout

- User-visible behavior changes:
- Operational risks:
- Mitigations:
