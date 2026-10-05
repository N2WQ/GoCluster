# ADR-0076: GitHub Release Package

The dirty-version suffix, tag/version coupling, and release-script parameter
requirements below are superseded by [ADR-0236](ADR-0236-date-only-build-version.md).
Current invocations require an explicit positive `-ReleaseNumber`; other package
contracts remain.

[ADR-0242](ADR-0242-release-preparation-safety.md) refines preparation,
output replacement, source rechecks, and publication destination/commit binding.

- Status: Accepted
- Date: 2026-04-24
- Decision Origin: Design

## Context

Operators need a simple deployment artifact that does not require manually
copying individual files from a development checkout. The live server expects a
binary plus a `data/config` directory, while the local `data` tree also contains
runtime state, Pebble stores, logs, generated downloads, user files, and optional
secret-bearing tool config. Zipping the live working directory would risk
publishing state that should remain local.

The checked-in `data/config` tree is itself published by normal GitHub pushes.
Therefore it cannot contain private operational settings such as real peer
callsigns, peer hostnames or IP addresses, passwords, or service tokens.

The repo also needs to keep the existing development shape: `go build .` from
the repo root remains the live binary build path, and advanced users can keep
cloning, editing, and building the source tree directly.

## Decision

Publish a Windows amd64 GitHub Release asset named
`gocluster-windows-amd64.zip`. The zip contains a top-level `ready_to_run/`
directory with:

1. `gocluster.exe` built from the repo root.
2. A curated `data/` tree copied from source-controlled runtime inputs,
   including public example config.
3. A deliberately small operator documentation bundle: a zip-root `README.md`,
   `docs/OPERATOR_GUIDE.md`, and the config-local docs already included under
   `data/config`.
4. No live Pebble stores, logs, user state, generated local caches, or optional
   secret-bearing files such as `data/config/openai.yaml`.

The package is built by an explicit allowlist script instead of copying the
developer's live `data` directory. The first supported release target is Windows
amd64 only.

The repository's tracked `data/config` is public example config. Private
operational config lives in a complete ignored directory such as
`data/config.local/` and is selected with `DXC_CONFIG_PATH`. The release script
also validates the staged public config for example callsigns, `.example.invalid`
peer hosts, blank peering passwords, and placeholder/disabled IPinfo settings
before creating the zip.

The zip-root `README.md` is rendered during packaging from the staged
`data/config` directory for runtime-configured values such as `telnet.port`.
This prevents the release instructions from drifting away from the packaged
YAML.

Release packages are created by the single release script
`scripts/create-release.ps1`. The script refuses a dirty worktree, runs
`go mod tidy -diff` without modifying files, compiles a fresh Windows amd64
binary, creates `ready_to_run/`, creates `gocluster-windows-amd64.zip`, creates
and pushes the Git tag, and publishes the GitHub Release asset. The generated
identity is used consistently as the binary version, Git tag, and GitHub
Release name.

Local package-only builds use `scripts/create-release.ps1 -PackageOnly`.
`-AllowDirty` is reserved for package-only testing and visibly marks the
resulting binary version with `+dirty`.

Local script runs create the runnable directory at repo-root `ready_to_run/`
and write `gocluster-windows-amd64.zip` at the repo root by default.

The source tree includes a tracked `download/README.md` so visitors can see the
download path directly from Git. That file points to GitHub Releases and the
`gocluster-windows-amd64.zip` asset. The generated zip and `ready_to_run/`
directory remain untracked build artifacts.

Generated release notes and download docs explicitly warn users to download
`gocluster-windows-amd64.zip` and not GitHub's automatic source-code archives
unless they want the developer source tree.

## Alternatives considered

1. Zip the whole repo or whole `data` directory.
2. Check a deployable `dist/` folder into the repository.
3. Publish only a GitHub Actions artifact instead of a GitHub Release asset.
4. Commit `gocluster-windows-amd64.zip` directly to the source tree.
5. Keep a tag-triggered GitHub Actions publisher in addition to the release
   script.

## Consequences

### Benefits

- Operators can deploy by extracting one zip and running `gocluster.exe`.
- The download filename remains platform-specific, while the extracted
  runnable directory has an operator-oriented name.
- Local maintainers can inspect or copy the repo-root `ready_to_run/` directory
  without digging through temporary validation output.
- Git visitors have a tracked `download/README.md` pointer to the binary
  release asset without storing generated binaries in source history.
- Release notes put the binary-asset warning before build metadata so users see
  it before GitHub's automatic source-code downloads.
- Release contents are reproducible from committed inputs.
- The default script path fails before staging if local edits or stale module
  metadata would make the artifact differ from committed source.
- One command creates the local package, tag, pushed tag, GitHub Release, and
  uploaded asset.
- Runtime state and secrets are excluded by construction.
- Source developers keep the current repo layout and build workflow.
- Normal GitHub commits and release packages use the same public config policy,
  so the release path is not the only privacy boundary.
- Operators get enough local documentation to configure and launch the package
  without bundling the full repository docs tree.

### Risks

- The allowlist can omit a future required runtime input if new data
  dependencies are added without updating release packaging.
- Windows amd64 only is intentionally narrow; other platforms need a later
  decision and validation pass.
- The release script depends on the GitHub CLI being installed and
  authenticated.
- If publishing fails after tag creation or tag push, the operator must inspect
  local and remote tag state before retrying.
- The bundled docs are intentionally incomplete; operators need GitHub for
  detailed architecture, package-local internals, ADRs, and troubleshooting
  history.
- The source tree depends on GitHub Releases for the actual binary download;
  repository browsers must follow the tracked download pointer.
- GitHub's automatic source-code archive links cannot be removed from normal
  GitHub Releases, so release notes and download docs must disambiguate them.

### Operational impact

- Binary and packaged config should be deployed and rolled back together.
- Operators should run the executable from the extracted `ready_to_run`
  directory.
- Maintainers create releases with `scripts/create-release.ps1`; package-only
  validation uses `scripts/create-release.ps1 -PackageOnly`.
- Git visitors should use `download/README.md` to find the latest compiled
  release package.
- The release package does not include learned runtime Pebble data; stores and
  downloads are created or refreshed by runtime behavior according to YAML.
- Operators should copy the packaged public config to a private config
  directory, edit it, and run with `DXC_CONFIG_PATH` for real deployments.
- The zip-root `README.md` is the package start point. It is rendered from
  packaged YAML where it names runtime-configured values, and it points to the
  small bundled operator guide and to GitHub for the full documentation set.
- Already-published private values must be treated as exposed outside this ADR;
  rotate passwords and service tokens as needed, and handle any Git history
  rewrite as a separate repository operation.

## Links

- Related issues/PRs/commits:
- Related tests: `go test ./config`, `scripts/create-release.ps1 -PackageOnly -AllowDirty`, zip payload inspection for repo-root `ready_to_run/`, `go test ./...`, `go vet ./...`, `staticcheck ./...`, `golangci-lint run ./... --config=.golangci.yaml`
- Related docs: `README.md`, `download/README.md`, `scripts/create-release.ps1`
- Related TSRs:
- Supersedes / superseded by:
