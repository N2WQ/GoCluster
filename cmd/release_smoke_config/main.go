// Command release_smoke_config configures a disposable, extracted Windows package
// for its startup probe. All packaged model and reference-data paths stay intact.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"gopkg.in/yaml.v3"
)

type settings = map[string]any

func namedSection(doc settings, name string) (settings, error) {
	section, ok := doc[name].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected configuration section %q", name)
	}
	return section, nil
}

func set(section settings, name string, value any) error {
	if _, ok := section[name]; !ok {
		return fmt.Errorf("missing required smoke configuration key %q", name)
	}
	section[name] = value
	return nil
}

func configure(dir string, port, sinkPort int, prompt string) error {
	if port < 1 || port > 65535 || sinkPort < 1 || sinkPort > 65535 || port == sinkPort || prompt == "" {
		return errors.New("invalid local smoke endpoints or prompt")
	}
	localURL := "http://127.0.0.1:" + strconv.Itoa(sinkPort) + "/release-smoke"
	type field struct {
		file, section, key string
		value              any
	}
	fields := []field{
		{"app", "ui", "mode", "headless"},
		{"app", "logging", "dir", "data/release-smoke/system"},
		{"app", "logging", "enabled", true},
		{"runtime", "web", "enabled", false},
		{"runtime", "telnet", "port", port},
		{"runtime", "telnet", "login_prompt", prompt},
		{"peering", "peering", "enabled", false},
		{"prop_report", "prop_report", "enabled", false},
		{"reputation", "reputation", "enabled", false},
		{"reputation", "reputation", "ipinfo_download_enabled", false},
		{"reputation", "reputation", "ipinfo_download_token", "file:smoke-unused-token"},
		{"reputation", "reputation", "ipinfo_api_token", ""},
		{"solarweather", "", "enabled", false},
		{"path_reliability", "", "enabled", false},
		{"path_reliability", "voacap_fallback", "enabled", false},
		{"path_reliability", "voacap_fallback", "voacap_home", "data/release-smoke/voacap"},
		{"voacap_experiment", "", "voacap_home", "data/release-smoke/voacap"},
		{"toxicity", "", "enabled", false},
		{"path_reliability", "native_160m_fallback", "enabled", false},
		{"data", "skew", "enabled", false},
		{"data", "cty", "url", localURL},
		{"data", "fcc_uls", "url", localURL},
		{"data", "ised", "url", localURL},
		{"data", "ised", "special_url", localURL},
		{"ingest", "rbn", "enabled", false},
		{"ingest", "rbn_digital", "enabled", false},
		{"ingest", "dxsummit", "enabled", false},
		{"ingest", "pskreporter", "enabled", false},
	}
	docs := make(map[string]settings)
	for _, f := range fields {
		path := filepath.Join(dir, f.file+".yaml")
		doc := docs[path]
		if doc == nil {
			data, err := os.ReadFile(path)
			if os.IsNotExist(err) && (f.file == "toxicity" || f.file == "voacap_experiment") {
				continue
			}
			if err != nil {
				return err
			}
			if err := yaml.Unmarshal(data, &doc); err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			docs[path] = doc
		}
		section := doc
		if f.section != "" {
			var err error
			section, err = namedSection(doc, f.section)
			if err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
		}
		if err := set(section, f.key, f.value); err != nil {
			return fmt.Errorf("%s %s: %w", path, f.section, err)
		}
	}
	ingest := docs[filepath.Join(dir, "ingest.yaml")]
	// The runtime supports both the current registry and the older singleton.
	switch feeds := ingest["human_telnet"].(type) {
	case []any:
		for _, feed := range feeds {
			entry, ok := feed.(map[string]any)
			if !ok {
				return errors.New("invalid human_telnet registry entry")
			}
			if err := set(entry, "enabled", false); err != nil {
				return err
			}
		}
	case map[string]any:
		if err := set(feeds, "enabled", false); err != nil {
			return err
		}
	default:
		return errors.New("invalid human_telnet registry")
	}
	for path, doc := range docs {
		data, err := yaml.Marshal(doc)
		if err != nil {
			return err
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	dir := flag.String("config", "", "temporary copy of the packaged public configuration")
	port := flag.Int("port", 0, "ephemeral Telnet port")
	sink := flag.Int("sink", 0, "owned loopback reference endpoint")
	prompt := flag.String("prompt", "", "per-process Telnet probe challenge")
	flag.Parse()
	if *dir == "" || flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	if err := configure(*dir, *port, *sink, *prompt); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
