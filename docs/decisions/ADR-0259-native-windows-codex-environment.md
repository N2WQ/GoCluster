# ADR-0259: Native Windows Codex Environment

- Status: Accepted
- Date: 2026-10-07
- Decision Origin: Design

## Context

The user resumed the native Windows environment migration and approved Scope
Ledger v1. Both pending workspace settings already disabled WSL for Codex,
while ADR-0250 and the environment guide still selected WSL2. Read-only Windows
checks passed for required development tools and repository skill metadata.
The installed VS Code extension includes a native Codex executable.

## Decision

- Select native Windows for Codex in the existing `C:\src\gocluster` checkout.
  Preserve `chatgpt.runCodexInWindowsSubsystemForLinux: false` in both
  `.vscode/settings.json` and `gocluster.code-workspace`.
- Open the workspace in a local Windows VS Code window and start a new Codex
  session to verify the actual execution platform, tools and loaded skills.
- Retain WSL for explicitly selected Linux validation. Keep the authoritative
  checkout in place and the operational launcher Windows-native.

## Alternatives considered

1. Continue selecting WSL2 for Codex: the prior accepted approach, replaced by
   the user's native Windows selection.
2. Move or duplicate the checkout: excluded because platform selection does not
   require another checkout and migration of operator paths is separate work.

## Consequences

### Benefits

Codex workspace settings and environment documentation agree on native Windows,
where required development tools and the bundled executable are available.

### Risks

Readiness probes from WSL through Windows PowerShell do not establish that a new
Codex session uses Windows, authenticates successfully, loads the expected skill
inventory, or operates its native sandbox. The documented Windows Application
Control limitation for some CGO test executables remains unresolved. Linux
validation cannot establish Windows executable permission.

### Operational impact

Reopen `gocluster.code-workspace` in local Windows VS Code and verify the new
session's execution environment. A standalone Windows Codex CLI was absent from
the probed PATH; the extension has its own bundled executable. Keep credentials,
plugin bindings and user configuration machine-local. Tool installation,
security-policy changes and launcher changes are outside this decision.

## Links

- Related validation: `scripts/verify-agentic-tools.ps1`,
  `scripts/verify-codex-skills.ps1`, workspace JSON consistency checks
- Related docs: [environment setup](../ENVIRONMENT.md#development-tools-and-wsl),
  `.vscode/settings.json`, `gocluster.code-workspace`
- Official setting reference:
  https://learn.chatgpt.com/docs/developer-settings?surface=ide
- Supersedes / superseded by:
  [ADR-0250](ADR-0250-go127-development-and-launcher.md), Codex environment
  selection only; its Go, skill ownership and launcher decisions remain accepted.
