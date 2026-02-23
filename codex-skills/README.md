# Codex Skills Bundle

This directory vendors the repo-approved Codex troubleshooting skills so onboarding does not depend on network installs.

Bundled skills:
- `gh-fix-ci`
- `sentry`

Install into local Codex home:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\install-codex-skills.ps1
```

Verify local install matches repo copies:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify-codex-skills.ps1
```

By default scripts install/verify the two bundled troubleshooting skills. To target a subset:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\install-codex-skills.ps1 -Skills gh-fix-ci
```
