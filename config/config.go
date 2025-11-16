package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the cluster
type Config struct {
	Server struct {
		Name   string `yaml:"name"`
		NodeID string `yaml:"node_id"`
	} `yaml:"server"`

	Telnet struct {
		Port           int    `yaml:"port"`
		TLSEnabled     bool   `yaml:"tls_enabled"`
		MaxConnections int    `yaml:"max_connections"`
		WelcomeMessage string `yaml:"welcome_message"`
	} `yaml:"telnet"`

	RBN struct {
		Enabled  bool   `yaml:"enabled"`
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		Callsign string `yaml:"callsign"`
	} `yaml:"rbn"`

	Admin struct {
		HTTPPort    int    `yaml:"http_port"`
		BindAddress string `yaml:"bind_address"`
	} `yaml:"admin"`

	Logging struct {
		Level string `yaml:"level"`
		File  string `yaml:"file"`
	} `yaml:"logging"`
}

// Load reads the configuration from a YAML file
func Load(filename string) (*Config, error) {
	// Read the file
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse the YAML
	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &cfg, nil
}

// Print displays the configuration (for debugging)
func (c *Config) Print() {
	fmt.Println("=== DX Cluster Configuration ===")
	fmt.Printf("Server Name: %s\n", c.Server.Name)
	fmt.Printf("Node ID: %s\n", c.Server.NodeID)
	fmt.Printf("Telnet Port: %d\n", c.Telnet.Port)
	fmt.Printf("TLS Enabled: %v\n", c.Telnet.TLSEnabled)
	fmt.Printf("Max Connections: %d\n", c.Telnet.MaxConnections)
	fmt.Printf("RBN Enabled: %v\n", c.RBN.Enabled)
	if c.RBN.Enabled {
		fmt.Printf("RBN Host: %s:%d\n", c.RBN.Host, c.RBN.Port)
		fmt.Printf("RBN Callsign: %s\n", c.RBN.Callsign)
	}
	fmt.Printf("Admin HTTP Port: %d\n", c.Admin.HTTPPort)
	fmt.Printf("Admin Bind Address: %s\n", c.Admin.BindAddress)
	fmt.Printf("Log Level: %s\n", c.Logging.Level)
	fmt.Printf("Log File: %s\n", c.Logging.File)
	fmt.Println("================================")
}
