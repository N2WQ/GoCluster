package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config represents the complete cluster configuration
type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Telnet      TelnetConfig      `yaml:"telnet"`
	RBN         RBNConfig         `yaml:"rbn"`
	RBNDigital  RBNConfig         `yaml:"rbn_digital"`
	PSKReporter PSKReporterConfig `yaml:"pskreporter"`
	Dedup       DedupConfig       `yaml:"dedup"`
	Filter      FilterConfig      `yaml:"filter"`
	Admin       AdminConfig       `yaml:"admin"`
	Logging     LoggingConfig     `yaml:"logging"`
}

// ServerConfig contains general server settings
type ServerConfig struct {
	Name   string `yaml:"name"`
	NodeID string `yaml:"node_id"`
}

// TelnetConfig contains telnet server settings
type TelnetConfig struct {
	Port             int    `yaml:"port"`
	TLSEnabled       bool   `yaml:"tls_enabled"`
	MaxConnections   int    `yaml:"max_connections"`
	WelcomeMessage   string `yaml:"welcome_message"`
	BroadcastWorkers int    `yaml:"broadcast_workers"`
}

// RBNConfig contains Reverse Beacon Network settings
type RBNConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Callsign string `yaml:"callsign"`
	Name     string `yaml:"name"`
}

// PSKReporterConfig contains PSKReporter MQTT settings
type PSKReporterConfig struct {
	Enabled bool   `yaml:"enabled"`
	Broker  string `yaml:"broker"`
	Port    int    `yaml:"port"`
	Topic   string `yaml:"topic"`
	Name    string `yaml:"name"`
	Workers int    `yaml:"workers"`
}

// DedupConfig contains deduplication settings
type DedupConfig struct {
	Enabled              bool `yaml:"enabled"`
	ClusterWindowSeconds int  `yaml:"cluster_window_seconds"`
	UserWindowSeconds    int  `yaml:"user_window_seconds"`
}

// AdminConfig contains admin interface settings
type AdminConfig struct {
	HTTPPort    int    `yaml:"http_port"`
	BindAddress string `yaml:"bind_address"`
}

// LoggingConfig contains logging settings
type LoggingConfig struct {
	Level string `yaml:"level"`
	File  string `yaml:"file"`
}

// FilterConfig holds default filter behavior for new users.
type FilterConfig struct {
	DefaultModes []string `yaml:"default_modes"`
}

// Load loads configuration from a YAML file
func Load(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &cfg, nil
}

// Print displays the configuration
func (c *Config) Print() {
	fmt.Printf("Server: %s (%s)\n", c.Server.Name, c.Server.NodeID)
	workerDesc := "auto"
	if c.Telnet.BroadcastWorkers > 0 {
		workerDesc = fmt.Sprintf("%d", c.Telnet.BroadcastWorkers)
	}
	fmt.Printf("Telnet: port %d (broadcast workers=%s)\n", c.Telnet.Port, workerDesc)
	if c.RBN.Enabled {
		fmt.Printf("RBN CW/RTTY: %s:%d (as %s)\n", c.RBN.Host, c.RBN.Port, c.RBN.Callsign)
	}
	if c.RBNDigital.Enabled {
		fmt.Printf("RBN Digital (FT4/FT8): %s:%d (as %s)\n", c.RBNDigital.Host, c.RBNDigital.Port, c.RBNDigital.Callsign)
	}
	if c.PSKReporter.Enabled {
		fmt.Printf("PSKReporter: %s:%d (topic: %s)\n", c.PSKReporter.Broker, c.PSKReporter.Port, c.PSKReporter.Topic)
	}
	if c.Dedup.Enabled {
		fmt.Printf("Dedup: cluster=%ds, user=%ds\n", c.Dedup.ClusterWindowSeconds, c.Dedup.UserWindowSeconds)
	}
	if len(c.Filter.DefaultModes) > 0 {
		fmt.Printf("Default modes: %s\n", strings.Join(c.Filter.DefaultModes, ", "))
	}
}
