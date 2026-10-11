# Support Card: Telnet Connectivity

## Match

Use when a telnet user or node operator reports connection refused, timeout,
missing login prompt, local-only success, remote-only failure, or asks which
port controls telnet. Also use for an invalid-input response or a disconnect
after malformed, overlong or incomplete human input.

## First Safe Check

Confirm the configured telnet port in the effective `runtime.yaml`, then test
from the host before diagnosing firewall or external network behavior.
For an input-triggered disconnect, inspect the exact entered line and server
rejection/disconnect log first; use the recovery contract in `telnet/README.md`.

## Must Include

- Do not assume a default port when config may differ.
- Verify the GoCluster process is running.
- Test host-local connectivity first, then remote connectivity.
- If local works and remote fails, then firewall, bind address, or network path
  becomes more likely.
- A rejected human line has no command effects. Recovery keeps the connection
  only when a safe line boundary is found within 64 KiB and five seconds, or an
  earlier existing deadline. Negotiation/buffered bytes count toward that bound.
- A partial read can disconnect even after Ctrl-U/backspace erases the text.
  Empty logged-in idle timeouts remain retryable. Machine uploads use separate
  framing limits; do not apply human recovery advice to a rejected upload.
- For manual DX comments, ASCII letters, digits, spaces and `/ # @ - . , * ? _`
  are accepted. Other punctuation and non-ASCII input is refused. The whole
  edited command uses the node's byte limit (default 128), excluding its ending.
  Route details to `commands/README.md` and `HELP DX`.

## Must Avoid

- Do not start with firewall changes before host-local evidence.
- Do not confuse telnet listener config with peer or ingest source ports.
- Do not advise resending a rejected line's tail, or claim it executed.

## Sources

- `customgpt/troubleshooting-index.md`
- `docs/OPERATOR_GUIDE.md`
- `data/config/README.md`
- `telnet/README.md`
