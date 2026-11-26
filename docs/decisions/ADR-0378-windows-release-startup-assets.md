# ADR-0378 — Windows release startup assets

- Status: Accepted
- Date: 2026-11-26
- Decision origin: Windows release troubleshooting, approved scope v2

## Context

Fresh installs lost call-correction data because the release excluded the
entire RBN tree. A check for filenames alone also accepts Git LFS pointers.
This decision refines [ADR-0076](ADR-0076-github-release-package.md)
and [ADR-0242](ADR-0242-release-preparation-safety.md).

## Decision

Release preparation resolves public startup inputs using the packaged
configuration and its runtime consumers. Each public filename setting needs
a classification as a shipped input, local state, credential, external
installation or downloaded data. The release includes individually selected
RBN models and the public FCC allowlist. Other RBN and FCC files stay private.

The builder compares each shipped input in the compressed ZIP to the captured
commit. Git LFS inputs use the SHA-256 object ID in the committed pointer.
Ordinary inputs use the committed blob bytes, including line endings. A dirty
local preview can use edited Go code, but its startup assets still have to
match the release commit.

Before output replacement or publication, run the extracted executable in a
fresh directory with outbound network isolation. Probe its own Telnet
listener and require three nonempty reliability tables and a loaded
confusion model. Failure preserves the previous stage and ZIP.

## Consequences

A release build needs hydrated Git LFS data and a Windows host capable of
running the isolated executable. On a networked host, the smoke helper
requests elevation for two temporary Windows Firewall blocks scoped to the
unique extracted executable. It removes them after the process retires.
An isolated Windows guest with only a loopback interface can run the test
directly. Declining elevation on a networked machine rejects the candidate.
Adding a public path requires deciding how
its contents are distributed. The runtime also diagnoses manually installed
LFS pointers. Older binaries use their existing diagnostics.

See the [incident record](../troubleshooting/TSR-0145-windows-release-models.md)
and the [operator recovery guide](../../download/README.md#recover-an-older-windows-zip).
