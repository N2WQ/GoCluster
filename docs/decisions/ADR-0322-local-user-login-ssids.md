# ADR-0322: Local User Login SSIDs

- Status: Accepted
- Date: 2026-10-28
- Decision Origin: Design

## Context

The shared callsign syntax accepted alphanumeric and repeated hyphen segments.
Local user login needs a bounded numeric SSID without changing spot, peer, or
saved-record validation. Full normalized login spelling owns a user profile.

## Decision

After the existing callsign normalization, a local user login may have no SSID
or one final hyphen followed by one or two ASCII digits. Existing callsign,
CTY, and license checks still apply. Reject invalid SSIDs before lookup and
profile restoration, using the existing invalid-login response.

Do not remove leading zeroes: `K1ABC-01` and `K1ABC-1` remain separate full
login identities. Existing normalization still removes portable endings such
as `/P`; `K1ABC-1/P` therefore logs in as `K1ABC-1`.

## Alternatives considered

1. Change shared callsign validation. Rejected because it also controls spot,
   peer, replay, and stored-record paths outside the user-login requirement.
2. Canonicalize leading zeroes or migrate records. Rejected because this changes
   profile identity and can select or merge a different user's saved settings.
3. Accept longer zero-padded numbers. Rejected to keep the login suffix bounded
   to one or two digits.

## Consequences

### Benefits

- User logins cannot use letters, values above 99, or multiple SSID segments.
- Existing valid full-identity profiles retain their spelling and ownership.

### Risks

- Users of newly rejected spellings cannot reconnect with them. The number of
  affected deployed profiles is unknown.

### Operational impact

- Existing records are preserved without automatic migration or deletion.
- The configured invalid-login message and syntax diagnostic remain in use.
- Deploy the updated executable to activate the rule; no YAML change is needed.

## Links

- Related tests: `telnet/login_ssid_test.go`.
- Related docs: [telnet login](../../telnet/README.md#login-and-session-behavior).
- Related TSRs: none.
- Supersedes / superseded by: refines local-login SSID syntax under
  [ADR-0110](ADR-0110-call-like-cty-admission.md); no other admission changes.
