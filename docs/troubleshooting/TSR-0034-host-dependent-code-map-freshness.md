# TSR-0034 - Host-Dependent Code Map Freshness

Status: Monitoring
Date Opened: 2026-07-12
Date Resolved: n/a
Owner: Codex
Technical Area: cmd/codemap, GitHub Actions
Trigger Source: Operator report
Led To ADR(s): ADR-0229
Tags: workflow, codemap, CI, Windows, Linux, build constraints

## RCA Summary

- What happened: the post-push CI code-map freshness check rejected both
  checked-in maps even though the same commit reported fresh maps locally.
- Why: `cmd/codemap` had two host-dependent inputs. It consumed host-selected
  `go list -json` files and imports, then hashed raw ADR bytes and compared raw
  generated Markdown bytes without canonicalizing CRLF and LF.
- What fixed it: code-map package inventories and imports are derived from the
  union of all checked-in Go source variants in each selected package directory;
  ADR hashing and generated-map comparison canonicalize CRLF to LF.
- How we know: an isolated Linux/amd64, CGO-enabled reproduction produced the
  same file and dependency differences shown by the hosted stale-map result;
  regression fixtures compare divergent host metadata and line endings against
  one expected inventory, dependency set, rendering, and fingerprint. A second
  clean-archive reproduction was identical across Windows and Linux and differed
  from the committed maps only in fingerprints, isolating raw ADR hashes as the
  remaining cause. Hosted confirmation remains pending until the change is
  pushed.
- Operator/support answer: regenerating on the other host is not a durable fix;
  use the host-independent generator defined by ADR-0229.

## Triggering Request

- Request date: 2026-07-12
- Request summary: diagnose GitHub Actions failures after pushing to `main` and
  propose and implement the approved correction.
- Request reference (chat/issue/link): GitHub Actions runs `29190757076` and
  `29191946105`.

## Symptoms and Impact

- What failed or looked wrong? Ubuntu CI reported
  `runtime-ingest-fanout.md` and `path-reliability-voacap.md` as stale, while
  Windows reported `code maps are fresh` at the same commits. After build-tag
  coverage was corrected, the second hosted run still rejected fingerprints
  generated from the clean LF checkout.
- User/operator impact: every direct push stopped at the code-map check and
  skipped the remaining baseline validation.
- Scope and affected components: repository code-map tooling and generated
  documentation; no production runtime behavior.

## Timeline

1. 2026-07-12 11:24 UTC - CI run `29190757076` began for commit
   `4c0b6d17e97eb2239ab53a0595fd068817d5b650`.
2. 2026-07-12 - hosted code-map validation reported both maps stale; later CI
   steps were skipped.
3. 2026-07-12 - local Windows validation reported both maps fresh.
4. 2026-07-12 - an isolated Linux/amd64, CGO-enabled generation reproduced the
   platform-specific file, import, and fingerprint differences.
5. 2026-07-12 - the repository owner selected full build-tagged source coverage
   and approved Scope Ledger v5.
6. 2026-07-12 12:03 UTC - CI run `29191946105` tested the v5 correction and
   again rejected both map fingerprints.
7. 2026-07-12 - clean-archive Windows and Linux generation produced identical
   maps whose only difference from the committed maps was the fingerprint;
   inspection found 66 CRLF ADRs in the local checkout and raw-byte ADR hashing.

## Hypotheses and Tests

1. Hypothesis A - the maps were merely not regenerated with the latest source.
   - Evidence/commands: `go run ./cmd/codemap check -all` on the clean Windows
     checkout at the failing commit.
   - Outcome: Rejected; the checked-in maps were fresh for Windows.
2. Hypothesis B - line endings or path separators changed rendered Markdown.
   - Evidence/commands: isolated no-index diffs between checked-in output and
     Linux/amd64, CGO-enabled generation.
   - Outcome: Rejected; the material diffs were selected filenames, one
     repository dependency, and derived fingerprints.
3. Hypothesis C - `go list` selected different build-constrained package
   metadata by host configuration.
   - Evidence/commands: compared Windows source selection with isolated
     `GOOS=linux`, `GOARCH=amd64`, `CGO_ENABLED=1` generation.
   - Outcome: Supported; differences included `reuseaddr_*`,
     `atomic_replace*`, `h3map*`, and the Linux/CGO-only `internal/fsutil`
     dependency.
4. Hypothesis D - raw ADR and generated-map byte comparison retained a second
   CRLF/LF dependency after build-tag coverage was corrected.
   - Evidence/commands: compared clean-archive Windows/Linux output with the
     committed maps, counted worktree ADR line endings, and inspected
     `loadADRs` hashing and the freshness `bytes.Equal` call.
   - Outcome: Supported; archive output was cross-platform identical, rendered
     content matched, and only fingerprints differed from the locally generated
     committed maps.

## Findings

- Root cause (or best current explanation): host-selected package metadata and
  raw line-ending-sensitive ADR/generated-map bytes were used as inputs to an
  artifact required to be identical across Windows development and Linux CI.
- Contributing factors: tests covered subprocess stream separation but did not
  present divergent platform metadata or equivalent LF/CRLF repositories, nor
  assert platform-independent files, dependency edges, rendering, fingerprints,
  and freshness comparison.
- Why this did or did not require a durable decision: it required ADR-0229
  because choosing full possible-build coverage changes the authoritative
  meaning of generated code-map inventories and edges.

## Decision Linkage

- ADR created/updated: ADR-0229; ADR-0147 receives reciprocal selective-
  supersession linkage.
- Decision delta summary: replace host-selected package files and imports with
  full build-tagged source coverage and canonicalize text line endings for
  deterministic hashing and freshness comparison.
- Contract/behavior changes (or `No contract changes`): generated code maps now
  describe possible-build source and dependency coverage rather than the
  invoking host's active build.

## Verification and Monitoring

- Validation steps run: `go test ./cmd/codemap -count=1`; targeted and package
  race tests for `cmd/rbn_replay`; isolated empty `GOMODCACHE` and `GOCACHE`
  code-map freshness; `go test ./...`; `go vet ./...`; `staticcheck ./...`;
  configured `golangci-lint`; `go test -race -count=1 ./...`; Codex workflow
  checker and mutation fixtures; troubleshooting-record checker; and
  `git diff --check`. The v7 follow-up additionally passed LF/CRLF hash,
  rendering, and substantive-change fixtures; byte-identical clean-archive
  Windows/Linux/CGO generation; isolated-cache freshness; Actionlint with
  `actions/checkout@v7`; the full workflow fixture suite; and the complete
  non-race Go baseline. All completed checks passed locally.
- Signals to monitor (metrics/logs): the next Ubuntu CI code-map check and full
  baseline after push.
- Rollback triggers: host-dependent rendered output, an omitted inactive-source
  dependency, or partial output after a package scan or parse error.

## References

- Issue(s): none
- PR(s): none
- Commit(s): `cb4da4403dc85e830d1b30376880e233f992db79`; corrective follow-up pending
- Related ADR(s): ADR-0147, ADR-0228, ADR-0229
- Related docs: `docs/code-maps/README.md`,
  `docs/code-maps/manifest.json`, `.github/workflows/ci.yml`
