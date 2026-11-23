package config

import (
	"strings"
	"testing"
)

func TestWebConfigCompatibility(t *testing.T) {
	dir := testConfigDir(t)
	cfg, err := Load(dir)
	if err != nil || cfg.Web == nil || cfg.Web.Enabled {
		t.Fatalf("shipped config should load with web disabled: %v", err)
	}
	removeTestConfigKey(t, dir, "runtime.yaml", "web", "enabled")
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "web.enabled") {
		t.Fatalf("incomplete web section accepted: %v", err)
	}
	removeTestConfigKey(t, dir, "runtime.yaml", "web")
	cfg, err = Load(dir)
	if err != nil || cfg.Web != nil {
		t.Fatalf("older config without web: err=%v", err)
	}
	writeTestConfigOverlay(t, dir, "runtime.yaml", "web: null")
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "web") {
		t.Fatalf("null web section accepted: %v", err)
	}
}

func TestWebConfigBounds(t *testing.T) {
	valid := WebConfig{true, "127.0.0.1:8080", "pilot.example.com", 30, 32}
	for _, addr := range []string{"localhost:8080", "0.0.0.0:8080", "192.0.2.1:8080", "[::]:8080", "127.0.0.1:0", "[::1%eth0]:8080"} {
		c := valid
		c.ListenAddress = addr
		if c.Validate() == nil {
			t.Errorf("accepted %q", addr)
		}
	}
	for _, host := range []string{"https://a.test", "A.test", "a.test:44", "-bad.test", "a..test", "a.test.", "127.0.0.1"} {
		c := valid
		c.Hostname = host
		if c.Validate() == nil {
			t.Errorf("accepted %q", host)
		}
	}
	for _, bounds := range [][2]int{{0, 1}, {301, 1}, {1, 0}, {1, 1025}} {
		c := valid
		c.HeartbeatSeconds, c.MaxConnections = bounds[0], bounds[1]
		if c.Validate() == nil {
			t.Errorf("accepted %v", bounds)
		}
	}
	valid.ListenAddress = "[::1]:8080"
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
}
