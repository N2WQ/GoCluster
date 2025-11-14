# DX Cluster Server

A modern, high-performance DX cluster implementation in Go for amateur radio operators.

## Overview

This project implements a telnet-based DX cluster server that aggregates and distributes amateur radio "spots" (station sightings) from multiple sources including RBN (Reverse Beacon Network), FT8/FT4 via MQTT, and other clusters.

**Key Features:**
- Cross-platform (Windows and Ubuntu from single codebase)
- High performance (handles 500+ concurrent telnet sessions, 1000+ spots/minute)
- Multiple ingestion sources (RBN, FT8/FT4, peer clusters)
- Real-time spot broadcasting to connected clients
- HTTP admin API and web GUI
- MQTT broker and interfaces for modern integrations
- Configurable filtering and rate limiting

## Current Status

**Development Phase:** Milestone 5 of 10 complete

### ✅ Completed Milestones

- **Milestone 0: Hello Cluster** - Basic Go project structure, build system
- **Milestone 1: Configuration System** - YAML config file loading and validation
- **Milestone 2: Basic Telnet Server** - Client connections, login, proper telnet protocol handling
- **Milestone 3: Spot Data Structure** - Canonical spot model with validation and formatting
- **Milestone 4: Spot Buffer** - Thread-safe ring buffer for in-memory spot storage
- **Milestone 5: Broadcasting** - Real-time spot distribution to all connected telnet clients

### 🚧 Upcoming Milestones

- **Milestone 6: Command Processor** - Implement telnet commands (SHOW/DX, SET/FILTER, ANNOUNCE)
- **Milestone 7: Spot Filtering** - Per-user filters (band, mode, continent, source type)
- **Milestone 8: RBN Ingestion** - Connect to Reverse Beacon Network and ingest CW/RTTY spots
- **Milestone 9: MQTT Integration** - Embedded MQTT broker for FT8/FT4 and external publishers
- **Milestone 10: Admin API & GUI** - HTTP REST API and browser-based admin interface

## Project Structure
```
C:\src\gocluster\
├── buffer\              # Ring buffer for in-memory spot storage
│   └── ringbuffer.go
├── config\              # Configuration management
│   └── config.go
├── spot\                # Spot data model and validation
│   └── spot.go
├── telnet\              # Telnet server and client handling
│   └── server.go
├── main.go              # Main application entry point
├── config.yaml          # Server configuration file
├── go.mod               # Go module dependencies
├── go.sum               # Dependency checksums
├── test_buffer.go       # Ring buffer test program
└── README.md            # This file
```

## Requirements Specification

### 1. Architecture and Implementation

1.1 The cluster shall be implemented primarily in Go and run as a long-lived network service.

1.2 The cluster shall use a modular internal architecture separating at least these concerns: network gateways, spot ingestion, spot processing/dispatch, and administration/API.

1.3 The cluster shall represent all spots internally using a single canonical spot model (e.g., dx_call, de_call, de_location, freq, band, mode, time, source_type, tags).

**Status:** ✅ Complete (Milestones 0-5)

### 2. Network Interfaces

2.1 The cluster shall expose a telnet interface for end users (logging/contest software) using standard DX cluster line formats.

2.2 The cluster shall expose a telnet interface for **bidirectional** peering with other clusters (upstream/downstream), including loop prevention via hop-count or path tracking.

2.3 The cluster shall expose an MQTT interface for receiving spots and control messages from other systems or clusters.

2.4 The cluster shall expose an MQTT interface suitable for end-user or downstream consumers that wish to subscribe to structured spot streams.

2.5 The cluster shall expose an HTTP/S endpoint for an admin API.

2.6 The cluster shall serve a browser-based admin GUI over HTTP/S using the admin API.

2.7 The cluster shall support configurable TLS/SSL for telnet connections to support secure client access.

2.8 The cluster shall run an embedded MQTT broker to support end-user MQTT subscriptions without requiring external broker infrastructure.

**Status:** 🚧 2.1 partially complete (basic telnet), 2.2-2.8 pending

### 3. Spot Ingestion and Processing

3.1 The cluster shall ingest spots from at least the following source types: RBN (telnet or TCP), FT8/FT4 via MQTT, and one or more legacy telnet clusters.

3.2 The cluster shall normalize all ingested spots from any source into the canonical internal spot model.

3.3 The cluster shall implement deduplication of spots based on configurable criteria (e.g., same dx_call, de_call, band, and close-in-time frequency) **with a configurable time window (default: 1 minute)**.

3.4 The cluster shall support tagging of spots with metadata (e.g., source_type, contest tags, quality indicators) for downstream filtering and analysis.

3.5 The cluster shall support configurable spot validation rules (callsign format, frequency ranges per band, mode/frequency consistency) with options to reject, flag, or pass-through invalid spots.

**Status:** 🚧 3.2 complete (canonical model), 3.1/3.3/3.4/3.5 pending

### 4. Sessions, Users, and Client Behavior

4.1 The cluster shall accept multiple concurrent telnet client connections and maintain per-session state (e.g., callsign, login time, IP, basic metadata).

4.1.1 The cluster shall support optional authentication via callsign verification (password-based or simple callsign challenge) configurable per deployment.

4.2 The cluster shall provide a command language over telnet to support at minimum: **LOGIN, SET/FILTER, SHOW/DX, SHOW/STATION, ANNOUNCE, BYE/QUIT**, basic filter configuration, viewing recent spots, and sending announcements or messages.

4.3 The cluster shall support per-user filtering by at least band, mode, continent (de and/or dx), and source type (RBN, FT8/FT4, manual, upstream).

4.4 The cluster shall enforce per-session output rate limiting (lines per second and burst size) to prevent overwhelming clients and network links. **Default: 10 spots/sec sustained, 50 spot burst. Both shall be configurable.**

4.5 The cluster shall provide a mechanism for cluster-wide announcements or messages visible to all connected sessions (e.g., admin announcements, user ANN).

4.6 The cluster shall support configurable per-user or per-IP connection limits to prevent resource exhaustion.

**Status:** 🚧 4.1 complete (basic sessions), 4.1.1/4.2/4.3/4.4/4.5/4.6 pending

### 5. Administration and Security

5.1 The admin API shall provide read access to cluster health, active sessions, ingest sources, and recent spots.

5.2 The admin API shall provide write operations for safe administrative actions (e.g., disconnect session, enable/disable source, trigger config reload).

5.3 The admin GUI shall present at minimum: overall health status, counts of active sessions, per-source ingest status and rates, and a tail of recent spots.

5.4 The admin GUI shall provide a sessions view listing active sessions (callsign, IP, connection time, last activity) and allow an admin to disconnect a session.

5.5 The admin GUI shall provide a sources view listing ingest sources (type, status, last message time, error state) and allow enabling or disabling individual sources.

5.6 Access to the admin API and GUI shall be authenticated (at minimum username/password) and support configuration of allowed bind address/port (e.g., localhost-only, specific interface).

5.7 The admin API shall provide endpoints for viewing and modifying spot ingestion rules and filters without requiring a full restart.

**Status:** ⏸️ Not started (Milestone 10)

### 6. Metrics, Logging, and Persistence

6.1 The cluster shall expose basic metrics suitable for monitoring (e.g., spots per minute, active sessions, source-specific rates, error counts) via the admin API or a dedicated **Prometheus-format metrics endpoint**.

6.2 The cluster shall write structured logs for key events (e.g., startup/shutdown, connection open/close, ingest errors, configuration reloads, admin actions) suitable for external log aggregation.

6.3 The cluster shall optionally persist spots to a backing store (e.g., Postgres/Timescale or similar) for historical analysis, with **configurable retention policy (e.g., 30 days) and automatic purging of expired records**.

6.4 The cluster shall maintain in-memory ring buffers of recent spots (configurable size, e.g., last 1000-5000) for fast retrieval without database queries.

**Status:** 🚧 6.4 complete (ring buffer), 6.1/6.2/6.3 pending

### 7. Deployment and Configuration

7.1 The cluster shall be deployable on Ubuntu Linux as the primary reference environment, with documented installation steps and a systemd unit file.

7.2 The cluster shall support building and running on Windows (including Windows Server and EC2) from the same codebase via Go cross-compilation.

7.3 The cluster shall use a human-readable configuration file (e.g., YAML or JSON) for core settings including ports, sources, rate limits, and default filters.

7.4 The cluster shall support a runtime configuration reload mechanism (e.g., admin API call or signal) that applies changes without full process restart where feasible.

**Status:** 🚧 7.2 complete (cross-compilation works), 7.3 partially complete (basic YAML config), 7.1/7.4 pending

### 8. Performance and Extensibility

8.1 Under typical ham radio cluster traffic volumes, the cluster shall target end-to-end latency from ingest to telnet client of under 1 second under normal load, **and shall handle at minimum 1000 spots/minute and 500 concurrent telnet sessions on modest hardware (4 core, 8GB RAM)**.

8.2 The cluster design shall allow the internal services (gateways, ingest, processing, admin/API) to be deployed either as a single process/binary or separated into multiple processes if needed for scaling.

8.3 The cluster design shall allow future extension for contest-specific features (e.g., per-contest tagging, special filters, scoring-related metadata) without breaking existing telnet client compatibility.

8.4 The cluster shall gracefully handle source outages (RBN down, upstream cluster disconnect) without affecting other sources or connected clients.

**Status:** 🚧 Architecture supports 8.1-8.4, not yet tested at scale

### 9. Data Models and Compatibility

9.1 The canonical spot model shall include fields compatible with DXSpider/CC-Cluster spot formats for interoperability.

9.2 The cluster shall support legacy DX cluster protocol commands commonly used by logging software.

9.3 The cluster shall parse and emit spot lines in standard DX cluster format: `DX de CALL: FREQ DX-CALL comment TIMETAG`

9.4 The cluster shall implement loop prevention for peered clusters using a hop-count (TTL) mechanism, rejecting spots that exceed a configurable maximum hop count (default: 5).

**Status:** 🚧 9.1/9.3 complete (spot model and formatting), 9.2/9.4 pending

### 10. Testing and Quality

10.1 The cluster shall include integration tests for each network gateway (telnet, MQTT, HTTP).

10.2 The cluster shall include unit tests for spot deduplication, filtering, and validation logic.

10.3 The cluster shall provide a test harness or mock mode for development without live RBN/cluster connections.

**Status:** ⏸️ Not started

## Quick Start

### Prerequisites

- Go 1.23 or later
- Windows (development) or Ubuntu (production deployment)

### Installation
```bash
# Clone or download the project
cd C:\src\gocluster

# Install dependencies
go mod tidy

# Run the server
go run main.go
```

### Build Executables
```bash
# Build for Windows
go build -o dxcluster.exe

# Build for Linux (from Windows)
set GOOS=linux
set GOARCH=amd64
go build -o dxcluster-linux
```

### Configuration

Edit `config.yaml` to customize:
```yaml
server:
  name: "My DX Cluster"
  node_id: "W1ABC"
  
telnet:
  port: 7300
  tls_enabled: false
  max_connections: 500
  welcome_message: "Welcome to My DX Cluster\nPlease login with your callsign\n"

admin:
  http_port: 8080
  bind_address: "127.0.0.1"
  
logging:
  level: "info"
  file: "cluster.log"
```

### Connect to the Cluster
```bash
telnet localhost 7300
```

Enter your callsign when prompted. You'll start receiving DX spots in real-time.

## Development

### Running Tests
```bash
# Test the ring buffer
go run test_buffer.go

# Run unit tests (when available)
go test ./...
```

### Project Commands
```bash
# Run the server with live reload during development
go run main.go

# Build for current platform
go build -o dxcluster.exe

# Format code
go fmt ./...

# Check for issues
go vet ./...
```

## Architecture Notes

### Spot Flow
```
Ingest Sources → Spot Buffer → Broadcast Channel → Connected Clients
     ↓              ↓               ↓                    ↓
   (RBN)      (Ring Buffer)   (Go channels)      (Telnet sessions)
   (MQTT)     (Thread-safe)   (Buffered)         (Individual goroutines)
   (Peers)    (1000+ spots)   (Non-blocking)     (Rate limited)
```

### Concurrency Model

- Each telnet client runs in its own goroutine
- Spot broadcasting uses Go channels for thread-safe distribution
- Ring buffer uses read-write mutex for concurrent access
- Non-blocking sends prevent slow clients from blocking the system

### Key Design Decisions

1. **Go Language**: Cross-platform, excellent concurrency, static binaries
2. **Ring Buffer**: Fast in-memory storage, automatic old-spot eviction
3. **Channels**: Native Go pattern for broadcasting spots to multiple clients
4. **Modular Packages**: Separation of concerns (telnet, spot, buffer, config)
5. **YAML Config**: Human-readable, industry standard

## Contributing

This is currently a solo development project for learning purposes. Future contributions welcome once core functionality is complete.

## License

TBD

## Contact

Rudy - Amateur Radio Operator
Project: DX Cluster Server in Go
Development Start: November 2024

---

**Current Development Session:** Milestone 5 complete - Real-time spot broadcasting working
**Next Session:** Start Milestone 6 - Command processor implementation
