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
| Inspect GO numbered rules, native CC selections/controls and shared configured/effective settings | [human readbacks](../telnet/README.md#human-configuration-readbacks), [CC manual](../docs/cc-dialect.md), [configuration card](support-cards/configuration-readback.md) |
| Back up/restore both banks with schema 7; distinguish active FILTER, shared SETTINGS and limited schema 6 | [client protocol](../telnet/README.md#client-yaml-configuration), [CC YAML ownership](../docs/cc-dialect.md#yaml-and-backups), [configuration card](support-cards/configuration-readback.md) |
| Switch independent GO/CC banks and six controls; identify shared preferences | [CC bank contract](../docs/cc-dialect.md), [commands HELP](../commands/README.md#help-source-of-truth) |
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
| Use GO `HELP RULE`, `PASS RULE`/`REJECT RULE` slots 0-9 and ordinary GO reset | [GO filters](../telnet/README.md#filters), [Boolean contract](../docs/domain-contract.md#boolean-personal-rules), [configuration card](support-cards/configuration-readback.md) |
| Edit native CC modes/sources, DOC/DOS/DXCTY/DXSTATE, DXBM and SET/NOFILTER | [CC commands](../docs/cc-dialect.md#terminal-commands), compiled `HELP SET/FILTER`; per-band geography initially uses GoCluster YAML. |
| Keep six BEACON/SELF/TOXIC/WWV/WCY/ANNOUNCE selections separate in each bank; PC93 follows ANNOUNCE | [CC controls](../docs/cc-dialect.md), [compiled HELP](../commands/README.md#help-source-of-truth) |
| SAVE/LIST/LOAD/DELETE tagged presets shared across SSIDs; LOAD preserves the other bank | [named presets](../telnet/README.md#named-presets), [CC presets](../docs/cc-dialect.md#presets-limits-and-recovery) |
| Understand two applied baselines, `(modified)` and SAVE library/association partial success | [named presets](../telnet/README.md#named-presets), [configuration card](support-cards/configuration-readback.md) |
| GO Boolean field examples: band, zone, ADIF DXCC, literal callsign patterns, grid, SOURCE, CONF and exact PATH | [telnet/README.md Filters](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#filters), [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md#command-surface) |
| GO MODE expressions and known `UNKNOWN` for blank modes | [telnet/README.md Filters](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#filters) |
| GO EVENT expressions, `EVENT=NONE` and unknown/NOT comparisons | [field contract](../docs/domain-contract.md#expressions-and-field-semantics), [telnet/README.md Filters](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#filters) |
| Toxic comment filtering | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md), [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), [cloudflare/toxicity-worker/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/cloudflare/toxicity-worker/README.md) |
| GO NEARBY Boolean conditions, missing-location semantics, ordinary dedupe and rule-only recovery | [telnet NEARBY](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#grid-noise-and-nearby), [recovery contract](../docs/domain-contract.md#nearby-predicate-and-recovery), [configuration support card](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/support-cards/configuration-readback.md) |
| Generation-seven profiles, one-time pre-seven filter/switch/preset reset, backups and independent notice retries | [operator storage](../data/config/README.md#user-configuration-storage), [CC recovery](../docs/cc-dialect.md#presets-limits-and-recovery), [ADR-0330](../docs/decisions/ADR-0330-native-cc-banks-and-generation-seven.md) |
| Interpret 128 KiB CC, 192 KiB CONFIG/preset, 256 KiB native/frame and 64 KiB GO/schema-six limits or busy refusals | [current limits](../docs/cc-dialect.md#presets-limits-and-recovery), [configuration card](support-cards/configuration-readback.md) |
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
