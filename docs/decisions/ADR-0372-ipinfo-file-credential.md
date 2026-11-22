# ADR-0372: IPinfo File Credential

- Status: Accepted
- Date: 2026-11-22
- Decision Origin: Troubleshooting chat, Approved v4 and Approved v5

## Context

The shipping configuration needs to support local IPinfo downloads at startup.
The credential belongs to the deployment. Operators using the default config
directory need the same startup behavior as other deployments.

## Decision

The shipping `reputation.yaml` selects `auto` and points to
`file:../secrets/.env` relative to the selected config directory. A usable local
credential enables the existing refresh worker. A missing file leaves it idle.
A malformed or unreadable file produces a startup warning and also leaves the
worker idle. The loader reads a regular file of at most 4 KiB once at startup.

Preserve explicit boolean settings and existing inline credentials. Remove the
compiled fallback. The release carries the reference and excludes local secrets.
The source file and archived YAML therefore use the same startup contract.

IPinfo opts in to protected request URLs in the shared downloader. Only a
digest of the original URL is stored for request identity. Redirects omit
`Referer` and credentials are removed from diagnostics and validators.
Changing the URL or credential requires a full fetch before conditional
validation can resume. A successful refresh replaces a legacy plaintext
sidecar with the protected format.

## Alternatives considered

- Require a complete private config copy on every host. This adds a config
  selection step to the default launch and creates a second set of settings to
  update.
- Put a credential in the shipping YAML or binary. This exposes a deployment
  credential in source or release artifacts.

## Consequences

The default launch enables refresh by provisioning one private file. Existing
configurations using boolean values continue to load. Deploy the new shipping
YAML with the matching executable, because older executables expect a boolean.
The snapshot verification and publication rules remain in
[ADR-0357](ADR-0357-ipinfo-verification-and-download-integrity.md).

## Links

- [Operator setup](../OPERATOR_GUIDE.md#ipinfo-refresh-and-recovery)
- [Troubleshooting evidence](../troubleshooting/TSR-0142-ipinfo-startup-credential.md)
- Regression owners are `config`, `download`, `reputation`, and the release safety harness.
