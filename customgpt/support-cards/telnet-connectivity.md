# Support Card: Telnet Connectivity

## Match

Use when a telnet user or node operator reports connection refused, timeout,
missing login prompt, local-only success, remote-only failure, or asks which
port controls telnet.

## First Safe Check

Confirm the configured telnet port in the effective `runtime.yaml`, then test
from the host before diagnosing firewall or external network behavior.

## Must Include

- Do not assume a default port when config may differ.
- Verify the GoCluster process is running.
- Test host-local connectivity first, then remote connectivity.
- If local works and remote fails, then firewall, bind address, or network path
  becomes more likely.

## Must Avoid

- Do not start with firewall changes before host-local evidence.
- Do not confuse telnet listener config with peer or ingest source ports.

## Sources

- `customgpt/troubleshooting-index.md`
- `docs/OPERATOR_GUIDE.md`
- `data/config/README.md`
- `telnet/README.md`
