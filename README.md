# DX Cluster Server

A modern Go-based DX cluster that aggregates amateur radio spots, enriches them with CTY metadata, and broadcasts them to telnet clients.

## Architecture and Spot Sources

1. **Telnet Server** (`telnet/server.go`) handles client connections, commands, and spot broadcasting using worker goroutines.
2. **RBN Clients** (`rbn/client.go`) maintain connections to the CW/RTTY (port 7000) and Digital (port 7001) feeds. Each line is parsed, normalized, validated against the CTY database, and enriched before queuing.
3. **PSKReporter MQTT** (`pskreporter/client.go`) subscribes to `pskr/filter/v2/+/+/#`, converts JSON payloads into canonical spots, and applies locator-based metadata.
4. **CTY Database** (`cty/parser.go` + `data/cty/cty.plist`) performs longest-prefix lookups so both spotters and spotted stations carry continent/country/CQ/ITU/grid metadata.
5. **Dedup Engine** (`dedup/deduplicator.go`) optionally filters duplicate spots before they reach the ring buffer and telnet clients.

## Data Flow and Spot Record Format

```
[Source: RBN/PSKReporter] → Parser → CTY Lookup → Dedup (if enabled) → Ring Buffer → Telnet Broadcast
```

Each `spot.Spot` stores:
- **ID** – monotonic identifier
- **DXCall / DECall** – uppercased callsigns
- **Frequency** (kHz), **Band**, **Mode**, **Report** (dB/SNR)
- **Time** – UTC timestamp from the source
- **Comment** – parsed message or `Locator>Locator`
- **SourceType / SourceNode** – origin tags (`RBN`, `RBN-DIGITAL`, `PSKREPORTER`, etc.)
- **TTL** – hop count preventing loops
- **DXMetadata / DEMetadata** – structured `CallMetadata` each containing:
	- `Continent`
	- `Country`
	- `CQZone`
	- `ITUZone`
	- `Grid`

## Commands

Telnet clients can issue:
- `HELP` – list available commands
- `SHOW/DX [N]` – display the most recent `N` spots (default 10)
- `SHOW/STATION <CALL>` – show spots for a specific DX station
- `BYE` – disconnect cleanly

## Project Structure

```
C:\src\gocluster\
├── buffer\              # In-memory ring buffer storing processed spots
│   └── ringbuffer.go
├── commands\            # Command parser/processor for telnet sessions
│   └── processor.go
├── config\              # YAML configuration loader (`config.yaml`)
│   └── config.go
├── cty\                 # CTY prefix parsing and lookup (data enrichment)
│   └── parser.go
├── cmd\                 # Utilities (interactive CTY lookup CLI)
│   └── ctylookup\main.go
├── dedup\               # Deduplication engine and window management
│   └── deduplicator.go
├── filter\              # Per-user filter defaults and helpers
│   └── filter.go
├── pskreporter\         # MQTT client for PSKReporter FT8/FT4 spots
│   └── client.go
├── rbn\                 # Reverse Beacon Network TCP client/parser
│   └── client.go
├── spot\                # Canonical spot definition and helpers
│   └── spot.go
├── stats\               # Runtime statistics tracking
│   └── stats.go
├── telnet\              # Telnet server and broadcast helpers
│   └── server.go
├── main.go               # Entry point wiring config, clients, dedup, and telnet server
├── config.yaml          # Runtime configuration
├── data/cty/cty.plist   # CTY prefix database for metadata lookups
├── go.mod               # Go module definition
├── go.sum               # Dependency checksums
└── README.md            # This documentation
```

## Getting Started

1. Update `config.yaml` with your preferred callsigns for the `rbn`, `rbn_digital`, and optional `pskreporter` sections.
2. Install dependencies and run:
	 ```pwsh
	 go mod tidy
	 go run main.go
	 ```
3. Connect via `telnet localhost 7300`, enter your callsign, and the server will immediately stream real-time spots.

## Testing & Tooling

- `go test ./...` validates packages (not all directories contain tests yet).
- `gofmt -w ./...` keeps formatting consistent.
- `go run cmd/ctylookup -data data/cty/cty.plist` lets you interactively inspect CTY entries used for validation.

Let me know if you want diagrams, sample logs, or scripted deployment steps added next.
