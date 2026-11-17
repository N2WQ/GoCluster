package telnet

// File role: One-shot, bounded resource measurements for the checkout command
// driver. This test executable never opens a listener or writes user storage.
// Python owns the expected limits/reasons; these receipts describe observations.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

const commandResourceMaximum = filter.MaxUserRecordBytes + 1

type commandResourceSpec struct {
	Nonce        string `json:"nonce"`
	Root         string `json:"root"`
	SourceDigest string `json:"source_digest"`
	InputHash    string `json:"input_hash"`
	Operation    string `json:"operation"`
	TargetBytes  int    `json:"target_bytes"`
}

type commandResourceMeasure struct {
	RawBytes       int    `json:"raw_bytes"`
	CanonicalBytes int    `json:"canonical_bytes"`
	FrameBytes     int    `json:"frame_bytes"`
	ReferenceBytes int    `json:"reference_bytes"`
	Admission      bool   `json:"admission"`
	Error          string `json:"error"`
}

type commandResourceArtifact struct {
	Bytes  int    `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type commandResourceReceipt struct {
	Nonce        string                             `json:"nonce"`
	PID          int                                `json:"pid"`
	BinaryHash   string                             `json:"binary_hash"`
	SourceDigest string                             `json:"source_digest"`
	InputHash    string                             `json:"input_hash"`
	Operation    string                             `json:"operation"`
	TargetBytes  int                                `json:"target_bytes"`
	Measurements map[string]commandResourceMeasure  `json:"measurements"`
	Artifacts    map[string]commandResourceArtifact `json:"artifacts"`
}

func commandResourceHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func commandResourceRead(t *testing.T, path string, maximum int) []byte {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	if err != nil || len(data) > maximum {
		t.Fatalf("resource fixture input exceeds bound or is unreadable: %v", err)
	}
	return data
}

func commandResourceMeasureProfile(data []byte) commandResourceMeasure {
	m := commandResourceMeasure{RawBytes: len(data)}
	record, err := filter.DecodeV7UserRecord(data)
	if err == nil {
		var canonical []byte
		canonical, err = filter.EncodeV7UserRecord(record)
		m.CanonicalBytes = len(canonical)
	}
	m.Admission = errors.Is(err, filter.ErrStorageAdmission)
	if err != nil {
		m.Error = err.Error()
		if len(m.Error) > 2048 {
			m.Error = m.Error[:2048]
		}
	}
	return m
}

// Existing addresses remain before the synthetic final scalar. Newline-heavy
// metadata separates canonical indentation bytes from physical/logical work.
// The size slope is measured from Go YAML, not assumed from Python formatting.
func commandResourceProfileAt(t *testing.T, input []byte, target int) map[string][]byte {
	t.Helper()
	record, err := filter.DecodeV7UserRecord(input)
	if err != nil || len(record.RecentIPs) >= 5 {
		t.Fatalf("profile size operation requires admitted owner with metadata room: %v", err)
	}
	prior := append([]string(nil), record.RecentIPs...)
	encode := func(lines, tail int) []byte {
		record.RecentIPs = append(append([]string(nil), prior...), strings.Repeat("A\n", lines)+strings.Repeat("Z", tail+1))
		data, marshalErr := yaml.Marshal(record)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		return data
	}
	first, slope := len(encode(1, 0)), len(encode(2, 0))-len(encode(1, 0))
	if slope <= 0 || target < first || target > commandResourceMaximum {
		t.Fatal("profile size target is outside the fixed fixture range")
	}
	lines := 1 + (target-first)/slope
	canonical := encode(lines, target-len(encode(lines, 0)))
	if len(canonical) != target {
		t.Fatal("Go canonical profile target was not exact")
	}
	// Encode only the final synthetic scalar differently. Duplicate fields and
	// aliases are unnecessary for this canonical-byte boundary control.
	value := record.RecentIPs[len(record.RecentIPs)-1]
	record.RecentIPs = prior
	prefix, err := yaml.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	var root yaml.Node
	if err := yaml.Unmarshal(prefix, &root); err != nil {
		t.Fatal(err)
	}
	fields := root.Content[0]
	var ips *yaml.Node
	for i := 0; i < len(fields.Content); i += 2 {
		if fields.Content[i].Value == "recent_ips" {
			ips = fields.Content[i+1]
		}
	}
	if ips == nil {
		ips = &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
		fields.Content = append(fields.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "recent_ips"}, ips)
	}
	ips.Content = append(ips.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Style: yaml.DoubleQuotedStyle, Value: value})
	compact, err := yaml.Marshal(&root)
	if err != nil {
		t.Fatal(err)
	}
	return map[string][]byte{"canonical.yaml": canonical, "compact.yaml": compact}
}

func commandResourceFrame(t *testing.T, cfg filter.V7Configuration, refs filter.V7PresetReferences) commandResourceMeasure {
	t.Helper()
	canonical, program, _, err := filter.PrepareV7Configuration(cfg)
	if err != nil {
		t.Fatalf("frame candidate failed earlier configuration admission: %v", err)
	}
	data, err := yaml.Marshal(canonical)
	if err != nil {
		t.Fatal(err)
	}
	status := v7WidestCapacityStatus()
	status.Effective.Dialect = "go"
	presets := v7ReadbackPresetStatuses(canonical, refs)
	for dialect := range presets {
		presets[dialect] = readbackPresetStatus{Name: strings.Repeat("1", 32)}
	}
	status.Configured, status.Preset = canonical.Settings, presets["go"]
	requestID, revision := strings.Repeat("1", 32), strings.Repeat("\"", 128)
	if !readbackMetadataFits(status, requestID, revision) {
		t.Fatal("frame fixture reached metadata admission before the final native frame boundary")
	}
	// Independent ordinary-envelope encoding records all fields and CRLF bytes.
	// The production renderer may use its compatible compact representation;
	// report both lengths rather than equating ordinary bytes with refusal.
	reference := v7ReferenceYAMLFrame(t, struct {
		Version       int    `yaml:"schema_version"`
		RequestID     string `yaml:"request_id"`
		Resource      string `yaml:"resource"`
		Revision      string `yaml:"revision"`
		Configuration struct {
			Filters  filter.V7FilterBanks `yaml:"filters"`
			Settings v7SettingsReadback   `yaml:"settings"`
		} `yaml:"configuration"`
		Status v7ReadbackStatus `yaml:"status"`
	}{Version: 7, RequestID: requestID, Resource: "CONFIG", Revision: revision,
		Configuration: struct {
			Filters  filter.V7FilterBanks `yaml:"filters"`
			Settings v7SettingsReadback   `yaml:"settings"`
		}{canonical.Filters, v7SettingsView(canonical.Settings)},
		Status: v7ReadbackStatus{Configured: v7SettingsView(canonical.Settings), Effective: status.Effective, Session: status.Session, Server: status.Server, Preset: status.Preset, ActiveDialect: "go", Presets: presets}})
	frame, frameErr := renderV7YAMLReadback(canonical, program.PreparedRules(), status, presets, "CONFIG", requestID, revision)
	m := commandResourceMeasure{CanonicalBytes: len(data), FrameBytes: len(frame), ReferenceBytes: len(reference), Admission: errors.Is(frameErr, errReadbackTooLarge)}
	if frameErr != nil {
		m.Error = frameErr.Error()
	}
	return m
}

func commandResourceFrames(t *testing.T, input []byte) (map[string][]byte, map[string]commandResourceMeasure) {
	t.Helper()
	record, err := filter.DecodeV7UserRecord(input)
	if err != nil {
		t.Fatal(err)
	}
	record.Configuration.Settings.Dialect = "go"
	record.Configuration.Settings.NoiseClass = "N"
	// A small bank plus one expanding setting reaches the independent 64-KiB
	// metadata preflight before the native 256-KiB frame cap. Fill the CC bank
	// with admitted country operands first, keeping those two limits distinct.
	cc, _ := v7NearestCCCapacity(t)
	count := 0
	for _, entry := range cc.Geography {
		count += len(entry.Global.Countries)
		for _, row := range entry.Bands {
			count += len(row.Countries)
		}
	}
	// Move a small part of that bank's bytes into the twice-rendered setting.
	// A completely full bank otherwise exhausts CONFIG before the final frame.
	record.Configuration.Filters.CC = v7CCCapacityCandidate(cc, count-32)
	base, err := yaml.Marshal(record.Configuration)
	if err != nil {
		t.Fatal(err)
	}
	low, high := 0, filter.MaxV7ConfigurationBytes-len(base)-64
	candidate := func(padding int) commandResourceMeasure {
		record.Configuration.Settings.NoiseClass = strings.Repeat("N", padding+1)
		return commandResourceFrame(t, record.Configuration, record.Presets)
	}
	if high <= 0 {
		t.Fatal("frame fixture has no configuration space for the setting")
	}
	first, last := candidate(low), candidate(high)
	if first.Error != "" || !last.Admission {
		t.Fatalf("fixed frame search does not bracket an output refusal: low=%d %+v high=%d %+v", low, first, high, last)
	}
	for step := 0; low+1 < high && step < 20; step++ {
		middle := (low + high) / 2
		if m := candidate(middle); m.Error == "" {
			low = middle
		} else if m.Admission {
			high = middle
		} else {
			t.Fatalf("unexpected frame search failure: %s", m.Error)
		}
	}
	if low+1 != high {
		t.Fatal("bounded frame search did not converge")
	}
	artifacts := make(map[string][]byte, 2)
	measurements := make(map[string]commandResourceMeasure, 2)
	for name, padding := range map[string]int{"fit.yaml": low, "over.yaml": high} {
		measurements[name] = candidate(padding)
		data, encodeErr := filter.EncodeV7UserRecord(record)
		if encodeErr != nil {
			t.Fatalf("frame fixture failed earlier profile admission: %v", encodeErr)
		}
		artifacts[name] = data
	}
	return artifacts, measurements
}

// These literal configured values distinguish body admission from admission of
// the actual final command response, including the automatic-pause footer. Each
// detached owner has its own bounded queue and no connection or writer worker.
func commandResourceDedupeFrames(t *testing.T) (map[string][]byte, map[string]commandResourceMeasure) {
	t.Helper()
	artifacts := make(map[string][]byte, 2)
	measurements := make(map[string]commandResourceMeasure, 2)
	for name, count := range map[string]int{"fit.txt": 49200, "over.txt": 49366} {
		s, c := readbackTestClient()
		s.dedupeFastEnabled, s.dedupeMedEnabled, s.dedupeSlowEnabled = true, true, true
		cfg, err := filter.DefaultV7Configuration(filter.SettingsConfiguration{Dialect: "go", DedupePolicy: strings.Repeat("X", count)})
		if err != nil {
			t.Fatal(err)
		}
		canonical, runtime, _, err := filter.PrepareV7Configuration(cfg)
		if err != nil {
			t.Fatal(err)
		}
		c.filterBanks, c.configuredSettings = &canonical.Filters, canonical.Settings
		*c.filter = runtime
		c.setDedupePolicy(dedupePolicyMed)
		baseline, err := filter.V7PresetFromConfiguration(canonical, "go")
		if err != nil {
			t.Fatal(err)
		}
		c.v7PresetReferences.GO = &filter.V7PresetReference{Name: "CONTROL", Baseline: baseline}
		c.refreshV7ConfigurationRevision()
		before, err := c.captureV7Configuration()
		if err != nil {
			t.Fatal(err)
		}
		refs, err := c.v7PresetReferences.Clone()
		if err != nil {
			t.Fatal(err)
		}
		revision, history, notice, unsaved := c.configurationRevision, c.historyGeneration, c.v7Notice, c.configurationUnsaved
		pause := machineEchoCoveragePause(c)
		commits := 0
		s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { commits++; return nil }
		body := s.formatDedupeStatus(c) // No automatic-pause footer is enabled yet.
		s.autoReadPauseMinRows, s.autoReadPauseDuration = 3, 30*time.Second
		response := s.formatDedupeStatus(c)
		if machineEchoCoveragePause(c) != pause {
			t.Fatal("dedupe formatter changed automatic pause before the final send")
		}
		if !s.sendCommandResponse(c, response, "resource dedupe final send") || len(c.controlChan) != 1 {
			t.Fatal("dedupe final-send fixture did not enqueue exactly one response")
		}
		message := <-c.controlChan
		wire := string(appendWriterNormalized(nil, message.line))
		reference := strings.ReplaceAll(strings.ReplaceAll(message.line, "\r\n", "\n"), "\n", "\r\n")
		if wire != reference {
			t.Fatal("dedupe final-send writer normalization changed CRLF bytes")
		}
		after, captureErr := c.captureV7Configuration()
		if captureErr != nil || !before.Equal(after) || !reflect.DeepEqual(refs, c.v7PresetReferences) || c.configurationRevision != revision || c.historyGeneration != history || c.v7Notice != notice || c.configurationUnsaved != unsaved || c.getDedupePolicy() != dedupePolicyMed || commits != 0 {
			t.Fatal("dedupe final send changed a preference, reference, revision, runtime or persistence owner")
		}
		active, remaining, suppressed := c.readPauseStatus(s.now())
		refused := response == "Readback failed: response exceeds 65,536 bytes.\r\n"
		if refused {
			if machineEchoCoveragePause(c) != pause || active || remaining != 0 || suppressed != 0 {
				t.Fatal("dedupe refusal created automatic-pause effects")
			}
		} else if !active || remaining != 30*time.Second || suppressed != 0 || !strings.Contains(wire, "Type RESUME to resume now.") {
			t.Fatal("fitting dedupe final send did not establish only the intended automatic pause")
		}
		m := commandResourceMeasure{RawBytes: count, CanonicalBytes: len(body), FrameBytes: len(wire), ReferenceBytes: len(reference), Admission: refused}
		if refused {
			m.Error = response
		}
		artifacts[name], measurements[name] = []byte(wire), m
	}
	return artifacts, measurements
}

func TestCommandResourceProfileBoundaryOracles(t *testing.T) {
	cfg, _ := v7PreparedFixture(t)
	input, err := filter.EncodeV7UserRecord(&filter.V7UserRecord{Configuration: cfg, RecentIPs: []string{"127.0.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []int{1_048_568, 1_048_577} {
		artifacts := commandResourceProfileAt(t, input, target)
		if len(artifacts["canonical.yaml"]) != target || len(artifacts["compact.yaml"]) >= 1_048_576 {
			t.Fatal("profile fixture did not separate physical and canonical sizes")
		}
		for name, data := range artifacts {
			m := commandResourceMeasureProfile(data)
			if target < 1_048_576 {
				if m.Error != "" || m.CanonicalBytes != target {
					t.Fatalf("fitting %s failed: %+v", name, m)
				}
			} else if !m.Admission || m.Error != "v7 document exceeds 1048576 bytes" {
				t.Fatalf("canonical %s did not reach the intended byte refusal: %+v", name, m)
			}
		}
	}
	// All 70 logical scalar occurrences must pay before five-entry retention.
	// Defining the anchor inside the known sequence avoids legacy recognition.
	record, err := filter.DecodeV7UserRecord(input)
	if err != nil {
		t.Fatal(err)
	}
	record.RecentIPs = nil
	prefix, err := yaml.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	work := append(prefix, []byte("recent_ips: [&ip "+strings.Repeat("A", 32768)+strings.Repeat(", *ip", 69)+"]\n")...)
	if m := commandResourceMeasureProfile(work); len(work) >= 1_048_576 || !m.Admission || m.Error != "stored configuration decoding work exceeds 2097152 units" {
		t.Fatalf("work fixture did not reach the intended shared budget: %+v", m)
	}
}

func TestCommandResourceFrameBoundaryOracles(t *testing.T) {
	cfg, _ := v7PreparedFixture(t)
	defaults, err := filter.DefaultV7Configuration(filter.SettingsConfiguration{Dialect: "go"})
	if err != nil {
		t.Fatal(err)
	}
	for name, configuration := range map[string]filter.V7Configuration{"literal": cfg, "operator-defaults": defaults} {
		t.Run(name, func(t *testing.T) {
			input, err := filter.EncodeV7UserRecord(&filter.V7UserRecord{Configuration: configuration})
			if err != nil {
				t.Fatal(err)
			}
			artifacts, measurements := commandResourceFrames(t, input)
			fit, over := measurements["fit.yaml"], measurements["over.yaml"]
			if fit.Error != "" || fit.Admission || fit.FrameBytes > 262_144 || !over.Admission || over.Error != errReadbackTooLarge.Error() || over.FrameBytes != 0 || over.ReferenceBytes <= 262_144 {
				t.Fatalf("frame owner boundary was not separated: fit=%+v over=%+v", fit, over)
			}
			for name, data := range artifacts {
				if m := commandResourceMeasureProfile(data); m.Error != "" {
					t.Fatalf("frame %s failed earlier profile admission: %+v", name, m)
				}
			}
			t.Logf("native frame fit=%+v over=%+v", fit, over)
		})
	}
}

func TestCommandResourceDedupeBoundaryOracles(t *testing.T) {
	artifacts, measurements := commandResourceDedupeFrames(t)
	fit, over := measurements["fit.txt"], measurements["over.txt"]
	if fit.RawBytes != 49200 || fit.Admission || fit.Error != "" || fit.FrameBytes > 65536 || fit.FrameBytes <= fit.CanonicalBytes || unquoteHumanPieces(t, string(artifacts["fit.txt"])) != strings.Repeat("X", 49200) {
		t.Fatalf("fitting final-send control lost configured bytes or footer: %+v", fit)
	}
	want := "Readback failed: response exceeds 65,536 bytes.\r\n"
	if over.RawBytes != 49366 || over.CanonicalBytes != 65536 || !over.Admission || over.Error != want || string(artifacts["over.txt"]) != want || over.FrameBytes != len(want) {
		t.Fatalf("final-send refusal was not isolated from body admission: %+v", over)
	}
	for _, data := range artifacts {
		assertHumanWire(t, string(data))
	}
	t.Logf("dedupe final-send fit=%+v over=%+v", fit, over)
}

// TestCheckoutCommandResourceFixture runs only as an explicitly requested
// one-shot helper. Ordinary package runs do not qualify resource measurements.
func TestCheckoutCommandResourceFixture(t *testing.T) {
	path := os.Getenv("GOCLUSTER_COMMAND_RESOURCE_FIXTURE")
	if path == "" {
		t.Log("resource fixture requires scripts/test-command-fixes.py")
		return
	}
	data := commandResourceRead(t, path, 4096)
	var spec commandResourceSpec
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&spec); err != nil {
		t.Fatal(err)
	}
	if decoder.Decode(new(any)) != io.EOF || len(spec.Nonce) != 32 || len(spec.SourceDigest) != 64 || len(spec.InputHash) != 64 || !filepath.IsAbs(spec.Root) {
		t.Fatal("invalid resource fixture identity")
	}
	checkout, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(spec.Root)
	relative, relErr := filepath.Rel(filepath.Join(checkout, ".tmp"), resolved)
	if err != nil || relErr != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || !strings.EqualFold(filepath.Clean(spec.Root), resolved) || filepath.Clean(path) != filepath.Join(spec.Root, "spec.json") {
		t.Fatal("resource fixture root must be an ordinary private checkout temporary directory")
	}
	input := commandResourceRead(t, filepath.Join(spec.Root, "input.yaml"), commandResourceMaximum)
	if commandResourceHash(input) != spec.InputHash {
		t.Fatal("resource fixture input digest mismatch")
	}
	receipt := commandResourceReceipt{Nonce: spec.Nonce, PID: os.Getpid(), SourceDigest: spec.SourceDigest, InputHash: spec.InputHash, Operation: spec.Operation, TargetBytes: spec.TargetBytes, Measurements: make(map[string]commandResourceMeasure), Artifacts: make(map[string]commandResourceArtifact)}
	artifacts := make(map[string][]byte)
	switch spec.Operation {
	case "profile_admission":
		receipt.Measurements["input.yaml"] = commandResourceMeasureProfile(input)
	case "profile_at_size":
		artifacts = commandResourceProfileAt(t, input, spec.TargetBytes)
		for name, value := range artifacts {
			m := commandResourceMeasureProfile(value)
			// The constructed canonical candidate is measured even on refusal.
			m.CanonicalBytes = len(artifacts["canonical.yaml"])
			receipt.Measurements[name] = m
		}
	case "config_frames":
		artifacts, receipt.Measurements = commandResourceFrames(t, input)
	case "dedupe_frames":
		if len(input) != 0 || spec.TargetBytes != 0 {
			t.Fatal("dedupe frames accept only their fixed owner vectors")
		}
		artifacts, receipt.Measurements = commandResourceDedupeFrames(t)
	default:
		t.Fatal("unknown resource fixture operation " + strconv.Quote(spec.Operation))
	}
	for name, value := range artifacts {
		if len(value) > commandResourceMaximum {
			t.Fatal("resource fixture artifact exceeds bound")
		}
		if err := os.WriteFile(filepath.Join(spec.Root, name), value, 0600); err != nil {
			t.Fatal(err)
		}
		receipt.Artifacts[name] = commandResourceArtifact{Bytes: len(value), SHA256: commandResourceHash(value)}
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	receipt.BinaryHash, err = fixtureHash(binary)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(receipt)
	if err != nil || len(encoded) > 16384 {
		t.Fatalf("resource fixture receipt exceeds bound: %v", err)
	}
	if err := os.WriteFile(filepath.Join(spec.Root, "receipt.json"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	fmt.Println("CHECKOUT_RESOURCE_FIXTURE_EXECUTED " + spec.Nonce)
}
