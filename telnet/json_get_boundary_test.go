package telnet

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
	"testing"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

// Exercise normal storage, login, command dispatch and wire framing using one
// session. Both GETs therefore have the same real revision and status.
func TestJSONSchema7AdmittedProfileGET(t *testing.T) {
	const callsign = "N0CALL-67"
	s := newHandshakeTranscriptServerWithOptions(t, func(o *ServerOptions) {
		o.LoginGreeting, o.EchoMode = "READY\n", "off"
	})
	cfg, _ := v7PreparedFixture(t)
	cfg.Filters.CC = v7CCCapacityCandidate(cfg.Filters.CC, 6118)
	// A stored dialect string is preserved even when the runtime falls back
	// to GO. Its length can fill the last byte of the stored configuration.
	cfg.Settings.Dialect = "NN"
	base, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Settings.Dialect += strings.Repeat("N", filter.MaxV7ConfigurationBytes-len(base))
	stored, err := yaml.Marshal(cfg)
	if err != nil || len(stored) != filter.MaxV7ConfigurationBytes {
		t.Fatalf("stored configuration bytes=%d, err=%v", len(stored), err)
	}
	record := &filter.V7UserRecord{ConfigurationVersion: filter.V7ConfigurationVersion, Configuration: cfg}
	record.Configuration.Settings.Dialect += "N"
	if err := s.admitV7LoginRecord(configurationTestClient(s, callsign), record, s.now()); !errors.Is(err, filter.ErrStorageAdmission) {
		t.Fatalf("configuration cap plus one: %v", err)
	}
	record.Configuration.Settings.Dialect = cfg.Settings.Dialect
	prepared, err := filter.PrepareV7UserRecord(callsign, record)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}

	w, _ := ve7ccConnectWitness(t, s, callsign)
	id := strings.Repeat("1", 32)
	get := func() {
		t.Helper()
		if _, err := io.WriteString(w.conn, "GET YAML CONFIG SCHEMA 7 ID "+id+"\r\n"); err != nil {
			t.Fatal(err)
		}
	}
	get()
	var raw bytes.Buffer
	for raw.Len() <= maxV7YAMLBytes {
		line, err := w.reader.ReadBytes('\n')
		if err != nil {
			t.Fatal(err)
		}
		raw.Write(line)
		if bytes.Equal(line, []byte("...\r\n")) {
			break
		}
	}
	if raw.Len() < maxV7YAMLBytes-1024 || raw.Len() > maxV7YAMLBytes || !bytes.HasPrefix(raw.Bytes(), []byte("---\r\n")) || !bytes.HasSuffix(raw.Bytes(), []byte("...\r\n")) {
		t.Fatalf("expected one near-limit CRLF document, got %d bytes", raw.Len())
	}
	var doc struct {
		Version       int                         `yaml:"schema_version"`
		RequestID     string                      `yaml:"request_id"`
		Revision      string                      `yaml:"revision"`
		Configuration filter.V7Configuration      `yaml:"configuration"`
		Status        configurationReadbackStatus `yaml:"status"`
	}
	if err := yaml.Unmarshal(raw.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Version != 7 || doc.RequestID != id || !regexp.MustCompile(`^[0-9a-f]{32}-[0-9]+$`).MatchString(doc.Revision) ||
		doc.Configuration.Settings.Dialect != cfg.Settings.Dialect || doc.Status.Session.Callsign != callsign || doc.Status.Session.TemporaryDefaults {
		t.Fatal("GET did not return the restored profile with this login's revision and status")
	}

	ve7ccTCPSelector(t, w, "SET JSON ON", "JSON ON 1\r\n")
	jsonWireRecord(t, w.reader, "hello")
	jsonWireRecord(t, w.reader, "schema")
	jsonWireRecord(t, w.reader, "schema")
	get()
	wire, err := w.reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var reply []string
	if err := json.Unmarshal(wire, &reply); err != nil || len(reply) != 2 || reply[0] != "yaml" || reply[1] != raw.String() {
		t.Fatalf("JSON and text GET differ: raw=%d JSON=%d err=%v", raw.Len(), len(wire), err)
	}
	t.Logf("stored configuration=%d bytes, GET raw=%d JSON=%d bytes", len(stored), raw.Len(), len(wire))
}
