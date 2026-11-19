# TSR-0141 - PSKReporter Startup And Subscription Recovery

Status: Monitoring
Date Opened: 2026-11-19
Date Resolved: 2026-11-19
Owner: GoCluster maintainers
Technical Area: PSKReporter MQTT startup, retry and subscription health
Trigger Source: Local resilience review
Led To ADR(s): ADR-0364

## RCA Summary

- Root cause: Cluster startup ran a single blocking MQTT connect and returned
  before wiring either consumer on failure. Paho's automatic reconnect only
  covered an established session. The subscription callback checked token
  errors but ignored the broker's result for the requested topic.

## Fix Or Mitigation

[ADR-0364](../decisions/ADR-0364-pskreporter-session-recovery.md) gives the feed
one background session owner and gates readiness on confirmed subscriptions.
The [PSKReporter support guide](../../pskreporter/README.md#connection-and-health)
describes the failure phases.

## Verification And Monitoring

A loopback broker reproduced an initial outage and a negative SUBACK with a
false success log. The regression tests exercise startup recovery across both
production consumers, subscription refusal and timeout, established disconnect,
and shutdown while a session is pending. The review used local fixtures.
Production rollout status and broker behavior remain unverified.
