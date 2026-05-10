# Operator Guide Index

Use this index for operators and telnet users. It routes support questions to
existing operator-facing docs.

## Install And Run

| Need | Start here |
| --- | --- |
| Download the ready-to-run package | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [download/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/download/README.md) |
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
| Know which YAML file owns a setting | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| Know whether a YAML setting is operator policy or algorithm calibration | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| Fix unknown file/key startup errors | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| Keep secrets out of public config | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md) |
| Change supported modes or events | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) |

## Telnet Use

| Need | Start here |
| --- | --- |
| Connect and log in | [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md) |
| Find available commands | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |
| Get command-specific help | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |
| Understand dialects | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |
| Post spots | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |
| Show recent filtered history | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |
| Look up DXCC/ADIF/zones | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |
| Confirm baseline own-call identity | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md) |
| Show recent spotter countries | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |
| Opt into solar summaries | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |

## Filtering And Output

| Need | Start here |
| --- | --- |
| `PASS`, `REJECT`, `SHOW FILTER`, `RESET FILTER` | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md), [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) |
| MODE filtering and `UNKNOWN` | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md), [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) |
| EVENT filtering | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md) |
| Toxic comment filtering | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md), [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), [cloudflare/toxicity-worker/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/cloudflare/toxicity-worker/README.md) |
| `NEARBY` filtering | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md) |
| Dedupe policies | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md) |
| Confidence tags | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [spot/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/spot/README.md) |
| Path reliability tags, diagnostics, insufficient reasons, and receiver contribution caps | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [pathreliability/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/pathreliability/README.md), [data/config/path_reliability.yaml](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/path_reliability.yaml) |
| Spot line format | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md), [spot/spot.go](https://raw.githubusercontent.com/N2WQ/GoCluster/main/spot/spot.go) |

## Sources

| Need | Start here |
| --- | --- |
| What the cluster ingests | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) |
| RBN behavior | [rbn/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/rbn/README.md) |
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
| Surprising mode, confidence, or path glyph | [customgpt/troubleshooting-index.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/troubleshooting-index.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), package README |
| Source-specific missing spots | [customgpt/troubleshooting-index.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/troubleshooting-index.md), [rbn/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/rbn/README.md), [pskreporter/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/pskreporter/README.md), [dxsummit/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/dxsummit/README.md), [peer/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/peer/README.md) |
| Login, reputation, telnet, ingest, or peer events are missing from console/UI | [customgpt/troubleshooting-index.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/troubleshooting-index.md), [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md) |
