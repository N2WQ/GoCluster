package telnet

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"dxcluster/filter"
	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

func v5ReadbackDocument(t *testing.T, response string) map[string]any {
	t.Helper()
	if !strings.HasPrefix(response, "---\r\n") || !strings.HasSuffix(response, "...\r\n") || len(response) > maxYAMLBytes ||
		strings.Contains(strings.ReplaceAll(response, "\r\n", ""), "\n") {
		t.Fatalf("invalid bounded CRLF frame, length=%d", len(response))
	}
	var document map[string]any
	if err := yaml.Unmarshal([]byte(response), &document); err != nil {
		t.Fatal(err)
	}
	if document["schema_version"] != 5 {
		t.Fatalf("wrong schema: %v", document["schema_version"])
	}
	return document
}

func v5ReadbackFixture(t *testing.T) (filter.V5Configuration, *filter.PreparedRuleSet) {
	t.Helper()
	cfg, _ := v5PreparedFixture(t)
	cfg.Filters.Rules = []filter.RuleDefinition{
		{Slot: 0, Action: filter.RulePass, Expr: `DXCONT=EU AND (MODE=CW OR COMMENT=" MiXeD ""Q"" \ ")`},
		{Slot: 5, Action: filter.RuleReject, Expr: "DEZONE=5"},
		{Slot: 9, Action: filter.RuleReject, Expr: "SNR<-10"},
	}
	canonical, value, err := filter.PrepareV5Configuration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return canonical, value.PreparedRules()
}

func TestV5MachineReadbackResourceShapesAndEffectiveRules(t *testing.T) {
	cfg, set := v5ReadbackFixture(t)
	before := cfg.Clone()
	status := configurationReadbackStatus{
		Preset: readbackPresetStatus{Associated: true, Name: "saved", Modified: true},
	}
	beforeStatus := status
	for _, resource := range []string{"FILTER", "SETTINGS", "CONFIG"} {
		response, err := renderV5YAMLReadback(cfg, set, status, resource, "Ab-1", "opaque-2")
		if err != nil {
			t.Fatal(err)
		}
		document := v5ReadbackDocument(t, response)
		if document["resource"] != resource || document["request_id"] != "Ab-1" || document["revision"] != "opaque-2" {
			t.Fatalf("metadata changed: %+v", document)
		}
		gotStatus := document["status"].(map[string]any)
		for _, name := range []string{"min_snr", "comments"} {
			if _, present := gotStatus[name]; present {
				t.Fatalf("obsolete status %s serialized", name)
			}
		}
		preset := gotStatus["preset"].(map[string]any)
		if preset["associated"] != true || preset["modified"] != true || preset["name"] != "saved" {
			t.Fatal("renderer recomputed parent-owned preset metadata")
		}
		rules := gotStatus["rules"].(map[string]any)
		if rules["configured_count"] != 3 || rules["effective_count"] != 2 || rules["suspended_count"] != 1 {
			t.Fatalf("wrong rule activity: %+v", rules)
		}
		entries := rules["entries"].([]any)
		if len(entries) != 3 {
			t.Fatalf("wrong bounded row count: %d", len(entries))
		}
		for i, want := range []v5ReadbackRule{
			{Slot: 0, Action: filter.RulePass, EffectiveExpr: `MODE=CW OR COMMENT=" MiXeD ""Q"" \ "`},
			{Slot: 5, Action: filter.RuleReject, Suspended: true},
			{Slot: 9, Action: filter.RuleReject, EffectiveExpr: "SNR<-10"},
		} {
			row := entries[i].(map[string]any)
			if len(row) != 4 || row["slot"] != want.Slot || row["action"] != string(want.Action) ||
				row["effective_expr"] != want.EffectiveExpr || row["suspended"] != want.Suspended {
				t.Fatalf("wrong explicitly named effective row: %+v", row)
			}
		}
		if full := rules["full"].(map[string]any); full["atoms"] != 5 || full["values"] != 5 || full["nodes"] != 7 {
			t.Fatalf("wrong full counts: %+v", full)
		}
		configuration := document["configuration"].(map[string]any)
		var roundtrip any
		switch resource {
		case "FILTER":
			if len(configuration) != 8 || configuration["filters"] != nil || configuration["settings"] != nil {
				t.Fatalf("FILTER is not the plain filter DTO: %+v", configuration)
			}
			roundtrip = cfg.Filters
		case "SETTINGS":
			if len(configuration) != 6 || configuration["rules"] != nil || configuration["filters"] != nil {
				t.Fatalf("SETTINGS is not the plain settings DTO: %+v", configuration)
			}
			roundtrip = cfg.Settings
		case "CONFIG":
			if len(configuration) != 2 || configuration["filters"] == nil || configuration["settings"] == nil {
				t.Fatalf("CONFIG is not the plain configuration DTO: %+v", configuration)
			}
			roundtrip = cfg
		}
		// Writable readback data must survive a complete machine proposal without
		// converting false/DEFAULT/zero or losing full saved geography/quoting.
		body, err := yaml.Marshal(struct {
			SchemaVersion int    `yaml:"schema_version"`
			RequestID     string `yaml:"request_id"`
			IfRevision    string `yaml:"if_revision"`
			Configuration any    `yaml:"configuration"`
		}{5, "roundtrip", "opaque-2", roundtrip})
		if err != nil {
			t.Fatal(err)
		}
		request, err := decodeV5MachineRequest(body, machineCommand{Verb: "PUT", Resource: resource}, nil)
		if err != nil {
			t.Fatal(err)
		}
		after, err := request.apply(cfg)
		if err != nil || !after.Equal(cfg) {
			t.Fatalf("readback roundtrip changed saved configuration: %v", err)
		}
		for _, forbidden := range []string{"allow_all:", "block_all:", "dx_continents:", "block_comments:", "min_snr:", "program:", "syntax:", "nodes:\n", "effectiveexpr:"} {
			if strings.Contains(response, forbidden) {
				t.Fatalf("legacy/private shape serialized: %q", forbidden)
			}
		}
	}
	if !reflect.DeepEqual(cfg, before) || status != beforeStatus {
		t.Fatal("rendering mutated caller-owned config/status")
	}
	cfg.Filters.NearbyEnabled = false
	response, err := renderV5YAMLReadback(cfg, set, status, "CONFIG", "Ab-1", "opaque-2")
	if err != nil {
		t.Fatal(err)
	}
	rules := v5ReadbackDocument(t, response)["status"].(map[string]any)["rules"].(map[string]any)
	if rules["effective_count"] != 3 || rules["suspended_count"] != 0 {
		t.Fatal("NEARBY OFF retained suspended rules")
	}
	entries := rules["entries"].([]any)
	if entries[0].(map[string]any)["effective_expr"] != cfg.Filters.Rules[0].Expr {
		t.Fatal("NEARBY OFF did not show full effective expression")
	}
}

func TestV5MachineReadbackCapabilities(t *testing.T) {
	response, err := renderV5YAMLReadback(filter.V5Configuration{}, nil, configurationReadbackStatus{}, "CAPABILITIES", "cap-1", "rev-1")
	if err != nil {
		t.Fatal(err)
	}
	capabilities := v5ReadbackDocument(t, response)["configuration"].(map[string]any)
	if !reflect.DeepEqual(capabilities["supported_schema_versions"], []any{5}) {
		t.Fatal("capabilities advertised unsupported versions")
	}
	fields := capabilities["rule_fields"].([]any)
	names := filter.SupportedRuleFields()
	if len(fields) != len(names) {
		t.Fatal("capabilities copied an incomplete field vocabulary")
	}
	for i, field := range fields {
		row := field.(map[string]any)
		if row["name"] != names[i] || row["geographic"] != filter.IsGeographicRuleField(names[i]) {
			t.Fatalf("field ownership/classification drift: %+v", row)
		}
		want := filter.RuleFieldOperators(names[i])
		operators := row["operators"].([]any)
		if len(operators) != len(want) {
			t.Fatal("operator count drift")
		}
		for j := range want {
			if operators[j] != want[j] {
				t.Fatalf("operator metadata drift: %+v", row)
			}
		}
	}
	var encoded struct {
		Configuration struct {
			Fields []struct {
				Operators yaml.Node `yaml:"operators"`
			} `yaml:"rule_fields"`
		} `yaml:"configuration"`
	}
	if err := yaml.Unmarshal([]byte(response), &encoded); err != nil {
		t.Fatal(err)
	}
	if len(encoded.Configuration.Fields) != len(names) {
		t.Fatal("serialized operator inventory is incomplete")
	}
	for _, field := range encoded.Configuration.Fields {
		if field.Operators.Kind != yaml.SequenceNode || len(field.Operators.Content) == 0 {
			t.Fatal("operator list did not serialize as a nonempty sequence")
		}
		for _, operator := range field.Operators.Content {
			if operator.Kind != yaml.ScalarNode || operator.Tag != "!!str" || operator.Style != yaml.DoubleQuotedStyle {
				t.Fatalf("operator is not a portable quoted YAML string: %+v", operator)
			}
		}
	}
	limits := capabilities["rule_limits"].(map[string]any)
	for name, want := range map[string]int{
		"min_slot": 0, "max_slot": 9, "max_rules": 10, "max_expr_bytes": 1024,
		"max_atoms_per_rule": 32, "max_nodes_per_rule": 128, "max_depth": 8,
		"max_values_per_atom": 32, "max_total_atoms": 128, "max_total_values": 512,
	} {
		if limits[name] != want {
			t.Fatalf("limit %s: got %v want %d", name, limits[name], want)
		}
	}
	vocabulary := capabilities["vocabulary"].(map[string]any)
	for name, want := range map[string][]string{
		"bands": spot.SupportedBandNames(), "modes": filter.SupportedModes(),
		"sources": filter.SupportedSources, "events": append([]string{"NONE"}, filter.SupportedEvents()...),
		"continents": filter.SupportedContinents, "states": spot.StateCodes(),
		"confidence": filter.SupportedConfidenceSymbols, "path_classes": filter.SupportedPathClasses,
	} {
		got := vocabulary[name].([]any)
		if len(got) != len(want) {
			t.Fatalf("%s vocabulary count changed", name)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s vocabulary differs at %d", name, i)
			}
		}
	}
	for _, forbidden := range []string{
		"filter_categories:", "rule_set_fields:", "allow_all:", "min_snr:", "block_comments:",
		"human_line", "line_limit", "CONFIDENCE\n", "PC93\n", "MINSNR\n",
	} {
		if strings.Contains(response, forbidden) {
			t.Fatalf("capabilities advertised legacy alias/fabricated runtime value: %q", forbidden)
		}
	}
}

func TestV5MachineReadbackEmptyAndNoCompilation(t *testing.T) {
	response, err := renderV5YAMLReadback(filter.V5Configuration{}, nil, configurationReadbackStatus{}, "FILTER", "nil", "rev")
	if err != nil {
		t.Fatal(err)
	}
	document := v5ReadbackDocument(t, response)
	if got := document["configuration"].(map[string]any)["rules"].([]any); len(got) != 0 {
		t.Fatal("empty rules did not serialize as an empty list")
	}
	rows := document["status"].(map[string]any)["rules"].(map[string]any)
	if rows["configured_count"] != 0 || len(rows["entries"].([]any)) != 0 {
		t.Fatal("nil rule set was not empty")
	}
	cfg, set := v5ReadbackFixture(t)
	// Deliberately violate the caller's coherence precondition: a compiler on
	// readback would reject this witness. Rendering must remain DTO-only and
	// use the supplied immutable set for effective status, never a hidden cache.
	cfg.Filters.Rules[0].Expr = "not valid expression"
	response, err = renderV5YAMLReadback(cfg, set, configurationReadbackStatus{}, "FILTER", "witness", "rev")
	if err != nil || !strings.Contains(response, "not valid expression") {
		t.Fatalf("readback attempted to prepare public definitions: %v", err)
	}
	if _, err := renderV5YAMLReadback(cfg, set, configurationReadbackStatus{}, "UNKNOWN", "id", "rev"); err == nil {
		t.Fatal("unknown resource was silently rendered")
	}
	request, err := decodeV5MachineRequest(v5MachineRequestFixture(v5RuleListFixture([]string{
		"MODE=CW", "MODE=CW", "MODE=CW", "MODE=CW", "MODE=CW",
		"MODE=CW", "MODE=CW", "MODE=CW", "MODE=CW", "MODE=CW",
	}), true), machineCommand{Verb: "PATCH", Resource: "FILTER"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err = renderV5YAMLReadback(request.Configuration, request.preparedRules, configurationReadbackStatus{}, "FILTER", "ten", "rev")
	if err != nil {
		t.Fatal(err)
	}
	rules := v5ReadbackDocument(t, response)["status"].(map[string]any)["rules"].(map[string]any)
	if len(rules["entries"].([]any)) != 10 {
		t.Fatal("ten-slot readback omitted rows")
	}
}

func TestV5MachineReadbackFullFrameBudget(t *testing.T) {
	cfg, set := v5ReadbackFixture(t)
	status := configurationReadbackStatus{}
	status.Session.DiagnosticComments = strings.Repeat("X", 64)
	response, err := renderV5YAMLReadback(cfg, set, status, "CONFIG", "id", "rev")
	if err != nil {
		t.Fatal(err)
	}
	status.Session.DiagnosticComments = strings.Repeat("X", 64+maxYAMLBytes-len(response))
	response, err = renderV5YAMLReadback(cfg, set, status, "CONFIG", "id", "rev")
	if err != nil || len(response) != maxYAMLBytes {
		t.Fatalf("exact final frame bound: len=%d err=%v", len(response), err)
	}
	status.Session.DiagnosticComments += "X"
	response, err = renderV5YAMLReadback(cfg, set, status, "CONFIG", "id", "rev")
	if !errors.Is(err, errReadbackTooLarge) || response != "" {
		t.Fatalf("overlong frame escaped partially: len=%d err=%v", len(response), err)
	}
	// Source length is below the limit; quoted YAML escaping exceeds it.
	cfg.Settings.Grid = strings.Repeat("\x01", maxYAMLBytes/3)
	response, err = renderV5YAMLReadback(cfg, set, configurationReadbackStatus{}, "SETTINGS", "id", "rev")
	if !errors.Is(err, errReadbackTooLarge) || response != "" {
		t.Fatalf("escaped frame escaped budget: len=%d err=%v", len(response), err)
	}
}

func TestV5MachineCommandEnvelopesAndBoundedFallback(t *testing.T) {
	for _, verb := range []string{"PUT", "PATCH", "VALIDATE"} {
		response, err := renderV5YAMLCommandSuccess("CONFIG", "Ab-1", "opaque", verb, verb != "VALIDATE", verb != "VALIDATE")
		if err != nil {
			t.Fatal(err)
		}
		document := v5ReadbackDocument(t, response)
		result := document["result"].(map[string]any)
		if result["operation"] != verb || result["valid"] != true ||
			result["applied"] != (verb != "VALIDATE") || result["persisted"] != (verb != "VALIDATE") {
			t.Fatalf("wrong command result: %+v", result)
		}
		if document["schema_version"] != 5 || document["resource"] != "CONFIG" || document["request_id"] != "Ab-1" {
			t.Fatal("schema5 envelope metadata changed")
		}
	}
	for _, input := range []struct{ verb, resource string }{{"PUT", "CAPABILITIES"}, {"VALIDATE", "FILTER"}, {"GET", "CONFIG"}, {"PUT", "UNKNOWN"}} {
		if response, err := renderV5YAMLCommandSuccess(input.resource, "id", "rev", input.verb, false, false); err == nil || response != "" {
			t.Fatal("unsupported success response escaped")
		}
	}
	if response, err := renderV5YAMLCommandSuccess("CONFIG", "id", strings.Repeat("\x01", maxYAMLBytes/3), "PUT", true, true); !errors.Is(err, errReadbackTooLarge) || response != "" {
		t.Fatalf("success escaping exceeded full-frame limit: %d %v", len(response), err)
	}
	normal := renderV5YAMLCommandError("CONFIG", "Ab-1", "opaque", "invalid_document", "configuration.filters.rules[0].expr: col 6: invalid operand")
	document := v5ReadbackDocument(t, normal)
	if document["request_id"] != "Ab-1" || document["revision"] != "opaque" ||
		document["error"].(map[string]any)["code"] != "invalid_document" {
		t.Fatal("error metadata changed")
	}
	for _, message := range []string{strings.Repeat("X", maxYAMLBytes+1), strings.Repeat("\x01", maxYAMLBytes/3)} {
		response := renderV5YAMLCommandError("CONFIG", "id", "revision", "invalid_document", message)
		document := v5ReadbackDocument(t, response)
		if document["error"].(map[string]any)["code"] != "response_too_large" || len(response) > 512 {
			t.Fatal("fallback failed to bound escaped error details")
		}
	}
}
