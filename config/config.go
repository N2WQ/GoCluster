// Package config provides configuration management for the DX Cluster Server.
//
// Configuration is loaded from a YAML file (typically config.yaml) and includes:
//   - Server identification (name, node ID)
//   - Telnet server settings (port, max connections, welcome message)
//   - RBN connection parameters (host, port, callsign)
//   - PSKReporter MQTT settings (broker, topic filters)
//   - Deduplication parameters (time windows)
//   - Admin interface settings (HTTP port, bind address)
//   - Logging configuration (level, file path)
//
// The configuration is loaded once at startup and shared across all components.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the complete cluster configuration loaded from config.yaml.
// All configuration sections are mandatory in the YAML file, though individual
// features (RBN, PSKReporter, Dedup) can be disabled via their Enabled flags.
type Config struct {
	Server      ServerConfig      `yaml:"server"`      // General server identification
	Telnet      TelnetConfig      `yaml:"telnet"`      // Telnet server configuration
	RBN         RBNConfig         `yaml:"rbn"`         // Reverse Beacon Network settings
	PSKReporter PSKReporterConfig `yaml:"pskreporter"` // PSKReporter MQTT settings
	Dedup       DedupConfig       `yaml:"dedup"`       // Deduplication engine settings
	Admin       AdminConfig       `yaml:"admin"`       // Admin HTTP interface (future use)
	Logging     LoggingConfig     `yaml:"logging"`     // Logging configuration (future use)
}

// ServerConfig contains general server identification settings.
// These values are used for identifying this cluster node in multi-server networks.
type ServerConfig struct {
	Name   string `yaml:"name"`    // Human-readable server name (e.g., "N2WQ DX Cluster")
	NodeID string `yaml:"node_id"` // Unique node identifier for DX cluster networks
}

// TelnetConfig contains telnet server settings.
// The telnet server is the primary interface for client connections.
type TelnetConfig struct {
	Port           int    `yaml:"port"`            // TCP port to listen on (typically 7300)
	TLSEnabled     bool   `yaml:"tls_enabled"`     // Enable TLS/SSL encryption (future use)
	MaxConnections int    `yaml:"max_connections"` // Maximum concurrent client connections (default: 500)
	WelcomeMessage string `yaml:"welcome_message"` // Message displayed to clients on connection
}

// RBNConfig contains Reverse Beacon Network (RBN) client settings.
// RBN provides automated CW and RTTY spot reports from skimmer stations worldwide.
type RBNConfig struct {
	Enabled  bool   `yaml:"enabled"`  // Enable/disable RBN client
	Host     string `yaml:"host"`     // RBN telnet server hostname (e.g., "telnet.reversebeacon.net")
	Port     int    `yaml:"port"`     // RBN telnet server port (typically 7000)
	Callsign string `yaml:"callsign"` // Your amateur radio callsign for RBN authentication
}

// PSKReporterConfig contains PSKReporter MQTT client settings.
// PSKReporter provides digital mode spots (FT8, FT4, etc.) via an MQTT broker.
type PSKReporterConfig struct {
	Enabled bool   `yaml:"enabled"` // Enable/disable PSKReporter client
	Broker  string `yaml:"broker"`  // MQTT broker hostname (e.g., "mqtt.pskreporter.info")
	Port    int    `yaml:"port"`    // MQTT broker port (typically 1883 for non-TLS)
	Topic   string `yaml:"topic"`   // MQTT topic filter (e.g., "pskr/filter/v2/+/FT8/#" for all FT8 spots)
}

// DedupConfig contains deduplication engine settings.
// The deduplicator prevents duplicate spots from being broadcast to clients.
type DedupConfig struct {
	Enabled              bool `yaml:"enabled"`                 // Enable/disable deduplication engine
	ClusterWindowSeconds int  `yaml:"cluster_window_seconds"`  // Time window for cluster-level dedup (typically 120s)
	UserWindowSeconds    int  `yaml:"user_window_seconds"`     // Time window for user-level dedup (typically 300s, future use)
}

// AdminConfig contains admin HTTP interface settings.
// This is reserved for future web-based administration features.
type AdminConfig struct {
	HTTPPort    int    `yaml:"http_port"`    // HTTP port for admin interface
	BindAddress string `yaml:"bind_address"` // IP address to bind to (e.g., "127.0.0.1" for local only)
}

// LoggingConfig contains logging settings.
// This is reserved for future structured logging features.
type LoggingConfig struct {
	Level string `yaml:"level"` // Log level (e.g., "info", "debug", "warn", "error")
	File  string `yaml:"file"`  // Log file path (empty string logs to stdout)
}

// Load reads and parses a YAML configuration file into a Config struct.
//
// Parameters:
//   - filename: Path to the YAML configuration file (typically "config.yaml")
//
// Returns:
//   - *Config: Parsed configuration structure
//   - error: Any error encountered during file reading or YAML parsing
//
// The function performs two operations:
//  1. Reads the entire file into memory using os.ReadFile
//  2. Unmarshals the YAML data into the Config structure
//
// If either operation fails, a descriptive error is returned with context.
// This function is typically called once during application startup.
func Load(filename string) (*Config, error) {
	// Read the entire configuration file
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse YAML data into Config structure
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &cfg, nil
}

// Print displays the loaded configuration to stdout in a human-readable format.
// This is called during startup to provide visibility into the active configuration.
//
// The output includes:
//   - Server name and node ID
//   - Telnet port
//   - RBN connection details (if enabled)
//   - PSKReporter connection details (if enabled)
//   - Deduplication window settings (if enabled)
//
// Only enabled features are displayed to keep the output concise and relevant.
func (c *Config) Print() {
	fmt.Printf("Server: %s (%s)\n", c.Server.Name, c.Server.NodeID)
	fmt.Printf("Telnet: port %d\n", c.Telnet.Port)

	// Only show RBN config if enabled
	if c.RBN.Enabled {
		fmt.Printf("RBN: %s:%d (as %s)\n", c.RBN.Host, c.RBN.Port, c.RBN.Callsign)
	}

	// Only show PSKReporter config if enabled
	if c.PSKReporter.Enabled {
		fmt.Printf("PSKReporter: %s:%d (topic: %s)\n", c.PSKReporter.Broker, c.PSKReporter.Port, c.PSKReporter.Topic)
	}

	// Only show deduplication config if enabled
	if c.Dedup.Enabled {
		fmt.Printf("Dedup: cluster=%ds, user=%ds\n", c.Dedup.ClusterWindowSeconds, c.Dedup.UserWindowSeconds)
	}
}
