# Operator Guide Index

Use this index for operators and telnet users. It routes support questions to
existing operator-facing docs.

## Install And Run

| Need | Start here |
| --- | --- |
| Download the ready-to-run package | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [download/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/download/README.md) |
| Interpret build versions, interpret automatic commit-suffix release tags, or troubleshoot a duplicate release tag | [README.md build notes](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [ADR-0236](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decisions/ADR-0236-date-only-build-version.md), [ADR-0252](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decisions/ADR-0252-automatic-commit-suffix-release-tags.md), [peer/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/peer/README.md) |
| Configure a real node | [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| Run on Windows | [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) |
| Build or run on Linux | [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) |
| Run under `systemd` | [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) |
| View logs and health | [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) |
| Find file-only event logs | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [docs/decisions/ADR-0093-file-only-connection-and-gate-event-logs.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decisions/ADR-0093-file-only-connection-and-gate-event-logs.md) |

## Config

| Need | Start here |
| --- | --- |
| Understand `DXC_CONFIG_PATH` | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| Configure ISED Canadian licensing and reuse DXSTATE/DESTATE province filters | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md), [ADR-0254](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decisions/ADR-0254-canadian-ised-license-and-state-reuse.md) |
| Know which YAML file owns a setting | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| Know whether a YAML setting is operator policy or algorithm calibration | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| Fix unknown file/key startup errors | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| Keep secrets out of public config | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md) |
| Change supported modes or events | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) |
| Configure multiple human/upstream telnet servers | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [rbn/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/rbn/README.md) |

## Telnet Use

| Need | Start here |
| --- | --- |
| Connect and log in | [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md) |
| Find available commands | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |
| Get command-specific help, `HELP RULE`, rule fields/values and symbol legends | [commands/README.md HELP source](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md#help-source-of-truth) |
| Inspect saved/effective numbered rules and controls with `SHOW FILTER [RULE\|FULL]`, plus configured/effective settings | [telnet/README.md human readbacks](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#human-configuration-readbacks), [configuration support card](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/support-cards/configuration-readback.md) |
| Read/write explicit machine schema 5: every GET requires SCHEMA 5; complete PUT/VALIDATE CONFIG and whole-list rules PATCH | [telnet/README.md client protocol](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#client-yaml-configuration), [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md#command-surface), [configuration support card](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/support-cards/configuration-readback.md) |
| Understand dialects | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |
| Post spots | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |
| Show recent filtered history | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |
| Look up DXCC/ADIF/zones | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |
| Confirm baseline own-call identity | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md) |
| Show recent spotter countries | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |
| Show WWV/WCY at login or inspect five-report and FULL history, truncation and gaps | [WWV/WCY commands](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md#wwv-and-wcy-bulletins), [telnet bulletin history](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#wwv-and-wcy-bulletin-history) |
| Opt into solar summaries | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |

## Filtering And Output

| Need | Start here |
| --- | --- |
| Use `HELP RULE`, `PASS RULE`/`REJECT RULE` slots 0–9; distinguish `RESET FILTER RULE` from `RESET FILTER` and new-user defaults | [telnet/README.md Filters](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#filters), [Boolean contract](../docs/domain-contract.md#boolean-personal-rules), [configuration support card](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/support-cards/configuration-readback.md) |
| Keep BEACON/SELF/TOXIC/WWV/WCY/ANNOUNCE/PC93 controls separate from expressions | [telnet/README.md Filters](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#filters), [commands HELP](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md#help-source-of-truth) |
| `SAVE PRESET`, `LIST PRESET`, `LOAD PRESET`, `DELETE PRESET` and shared SSID snapshots | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#named-presets), [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |
| Understand `(modified)`, SAVE partial success and the retained per-SSID preset reference | [telnet/README.md named presets](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#named-presets), [configuration support card](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/support-cards/configuration-readback.md) |
| Boolean field examples: band, zone, ADIF DXCC, literal callsign patterns, grid, SOURCE, CONF and exact PATH | [telnet/README.md Filters](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#filters), [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md#command-surface) |
| MODE expressions and known `UNKNOWN` for blank modes | [telnet/README.md Filters](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#filters) |
| EVENT expressions, `EVENT=NONE` and unknown/NOT comparisons | [field contract](../docs/domain-contract.md#expressions-and-field-semantics), [telnet/README.md Filters](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#filters) |
| Toxic comment filtering | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md), [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), [cloudflare/toxicity-worker/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/cloudflare/toxicity-worker/README.md) |
| NEARBY Boolean geography projection, editable saved rules, latest-full-rule OFF and already-ON recovery | [telnet NEARBY](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#grid-noise-and-nearby), [recovery contract](../docs/domain-contract.md#nearby-projection-and-recovery), [configuration support card](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/support-cards/configuration-readback.md) |
| Native-v5 user records, one-time old-profile reset preserving settings, original backups and current-preset protection | [operator storage](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md#user-configuration-storage), [telnet persistence](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#persistence-format-and-rollback), [ADR-0318](../docs/decisions/ADR-0318-old-profile-reset-upgrade.md) |
| Dedupe policies | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md) |
| Confidence tags | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [spot/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/spot/README.md) |
| Path reliability tags, diagnostics, `SHOW PROP` outlook, insufficient reasons, and receiver contribution caps | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [pathreliability/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/pathreliability/README.md), [data/config/path_reliability.yaml](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/path_reliability.yaml) |
| Spot line format | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md), [spot/spot.go](https://raw.githubusercontent.com/N2WQ/GoCluster/main/spot/spot.go) |

## Sources

| Need | Start here |
| --- | --- |
| What the cluster ingests | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) |
| RBN behavior | [rbn/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/rbn/README.md) |
| Human/upstream telnet behavior | [rbn/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/rbn/README.md), [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| PSKReporter behavior | [pskreporter/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/pskreporter/README.md) |
| DXSummit behavior | [dxsummit/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/dxsummit/README.md), [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| Peer behavior | [peer/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/peer/README.md) |

## Troubleshooting

Start with [customgpt/troubleshooting-index.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/troubleshooting-index.md) for symptom-based routing.

| Symptom | Start here |
| --- | --- |
| Startup or config load failure | [customgpt/troubleshooting-index.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/troubleshooting-index.md), [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| Linux service failure | [customgpt/troubleshooting-index.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/troubleshooting-index.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md) |
| Telnet cannot connect | [customgpt/troubleshooting-index.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/troubleshooting-index.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md) |
| Missing spots after login | [customgpt/troubleshooting-index.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/troubleshooting-index.md), [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md), [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| Human readback paused spots despite zero settings; machine YAML pause expectations | [configuration support card](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/support-cards/configuration-readback.md), [telnet/README.md read pause](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#read-pause) |
| YAML revision conflict, rejected upload, explicit size error or temporary-defaults warning | [customgpt/troubleshooting-index.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/troubleshooting-index.md), [configuration support card](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/support-cards/configuration-readback.md), [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md) |
| Preserve unreadable records, back up user data or roll back configuration format versions | [telnet/README.md persistence format and rollback](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#persistence-format-and-rollback), [configuration support card](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/support-cards/configuration-readback.md) |
| Surprising mode, confidence, or path glyph | [customgpt/troubleshooting-index.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/troubleshooting-index.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), package README |
| Source-specific missing spots | [customgpt/troubleshooting-index.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/troubleshooting-index.md), [rbn/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/rbn/README.md), [pskreporter/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/pskreporter/README.md), [dxsummit/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/dxsummit/README.md), [peer/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/peer/README.md) |
| Login, reputation, telnet, ingest, or peer events are missing from console/UI | [customgpt/troubleshooting-index.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/troubleshooting-index.md), [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md) |
