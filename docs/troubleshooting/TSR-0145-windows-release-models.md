# TSR-0145 — Missing correction assets in Windows releases

Status: Monitoring. Opened November 26, 2026.

A clean Windows installation of `261122r949b` exited on
`Invalid call_correction.spotter_reliability_file`. The locally loaded CTY
database and the earlier FCC allowlist warning showed how far startup had
progressed. The final FCC and ISED cancellation warnings were consistent with
the process closing its background services.

All 48 published Windows ZIPs through that tag named four correction assets
in the shipped pipeline configuration and omitted them. The builder excluded
the entire RBN and FCC directories. The allowlist omission also removed the
configured licensing exceptions. Only seven tagged trees contained the
four models, and only three also contained the allowlist.

For manual recovery, identify the binary's release tag and follow the
[tag-specific download instructions](../../download/README.md#recover-an-older-windows-zip).
The text `version https://git-lfs.github.com/spec/v1` identifies a pointer.
Some older parsers can interpret its `size` line as a receiver weight.

[ADR-0378](../decisions/ADR-0378-windows-release-startup-assets.md) records
the release checks. For a recurrence, retain the original ZIP, binary tag,
effective config paths and first fatal line. Compare the archived file's hash
against the selected commit before investigating download or hardware issues.
