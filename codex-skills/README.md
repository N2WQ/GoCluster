# Codex Skills Bundle

This directory vendors repo-approved Codex skills so onboarding does not depend on network installs.

Repo-managed skills:
- `gh-fix-ci`
- `go-blast-radius-audit`
- `go-code-walk`
- `go-config-contract-audit`
- `go-hotpath-design`
- `go-leak-detection`
- `go-retained-state-audit`
- `pprof-impact-review`
- `sentry`

Install into local Codex home:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\install-codex-skills.ps1
```

Verify local install matches repo copies:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify-codex-skills.ps1
```

By default scripts install/verify `gh-fix-ci` and `sentry`. To target a subset:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\install-codex-skills.ps1 -Skills gh-fix-ci
```
