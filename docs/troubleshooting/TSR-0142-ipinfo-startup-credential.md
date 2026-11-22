# TSR-0142 - IPinfo Startup Credential

Status: Monitoring
Date Opened: 2026-11-22
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: reputation, startup configuration, release packaging
Trigger Source: Operator report and troubleshooting chat
Led To ADR(s): ADR-0372

## RCA Summary

- Pebble printed the absent IPinfo database path with quoted backslashes.
  The decoded Windows path resolved normally and the database directory was missing.
- Root cause: The shipping configuration disabled downloads and contained a placeholder
  token. A previously configured value was still compiled as a fallback, but
  the nonblank placeholder bypassed it.
- A separate HTTPS probe using the recovered value returned a complete gzip
  archive with the expected network and enrichment columns.
- The downloader saved the requested URL verbatim in its status file and Go
  could send that URL as the redirect referrer. The authenticated URL contained
  the token.

## Fix Or Mitigation

Use the local credential file and shipping YAML documented in the
[operator guide](../OPERATOR_GUIDE.md#ipinfo-refresh-and-recovery).
The credential must stay outside tracked source and released archives.
IPinfo requests store a URL digest and suppress credential-bearing request
diagnostics, redirect referrers, and response validators.

## Verification And Monitoring

An isolated startup fixture authenticated to a local HTTP server, imported a
snapshot, and returned the expected ASN from the resulting Pebble database.
Check the system log for the IPinfo store update after enabling the live import.
A successful archive fetch alone does not establish that Pebble publication
succeeded.

## References

- [ADR-0372](../decisions/ADR-0372-ipinfo-file-credential.md)
- [IPinfo download integrity](TSR-0136-ipinfo-and-download-integrity.md)
