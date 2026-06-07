package yamlconfig

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Path identifies a YAML-owned setting that must be present and non-null.
// Use "*" to require the remaining path under every sequence item.
type Path []string

// DiagnosticSeverity classifies a startup YAML diagnostic as warning-only or
// fatal. Startup logs both classes, but only errors block launch.
type DiagnosticSeverity string

const (
	DiagnosticWarning DiagnosticSeverity = "warning"
	DiagnosticError   DiagnosticSeverity = "error"
)

// Diagnostic is a single operator-facing YAML diagnostic.
type Diagnostic struct {
	Severity DiagnosticSeverity
	Source   string
	Path     string
	Message  string
}

func (d Diagnostic) Error() string {
	if d.Source == "" {
		return d.Message
	}
	return fmt.Sprintf("%s: %s", d.Source, d.Message)
}

// DecodeFile reads a YAML file, validates required YAML-owned paths, rejects
// unknown struct fields, and decodes into out.
func DecodeFile(path string, out any, required []Path) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("YAML config path is required")
	}
	bs, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return DecodeBytes(path, bs, out, required)
}

// DecodeBytes validates and decodes a YAML document from memory.
func DecodeBytes(name string, bs []byte, out any, required []Path) error {
	if err := ValidateRequiredPaths(name, bs, required); err != nil {
		return err
	}
	dec := yaml.NewDecoder(bytes.NewReader(bs))
	dec.KnownFields(true)
	if err := dec.Decode(out); err != nil {
		return err
	}
	return nil
}

// DecodeBytesPermissive validates required paths and decodes a YAML document
// without failing on unknown struct fields. Callers should pair this with
// UnknownFieldDiagnostics when extra keys should be warning-only.
func DecodeBytesPermissive(name string, bs []byte, out any, required []Path) error {
	if err := ValidateRequiredPaths(name, bs, required); err != nil {
		return err
	}
	dec := yaml.NewDecoder(bytes.NewReader(bs))
	if err := dec.Decode(out); err != nil {
		return err
	}
	return nil
}

// ValidateRequiredPaths verifies that each required setting path exists and is
// not YAML null. Values such as false, 0, "", [], and {} are still explicit.
func ValidateRequiredPaths(name string, bs []byte, required []Path) error {
	if len(required) == 0 {
		return nil
	}
	var doc yaml.Node
	dec := yaml.NewDecoder(bytes.NewReader(bs))
	if err := dec.Decode(&doc); err != nil {
		return err
	}
	if len(doc.Content) == 0 {
		return fmt.Errorf("%s: YAML document is empty", name)
	}
	root := doc.Content[0]
	for _, path := range required {
		if len(path) == 0 {
			continue
		}
		if err := requirePath(root, path, nil); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

// RequiredPathDiagnostics returns every missing or null required path instead
// of stopping at the first failure.
func RequiredPathDiagnostics(name string, bs []byte, required []Path) ([]Diagnostic, error) {
	if len(required) == 0 {
		return nil, nil
	}
	var doc yaml.Node
	dec := yaml.NewDecoder(bytes.NewReader(bs))
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	if len(doc.Content) == 0 {
		return []Diagnostic{{
			Severity: DiagnosticError,
			Source:   name,
			Path:     "<root>",
			Message:  "YAML document is empty",
		}}, nil
	}
	root := doc.Content[0]
	diagnostics := make([]Diagnostic, 0)
	for _, path := range required {
		if len(path) == 0 {
			continue
		}
		if err := requirePath(root, path, nil); err != nil {
			diagnostics = append(diagnostics, Diagnostic{
				Severity: DiagnosticError,
				Source:   name,
				Path:     diagnosticPathFromRequiredError(err),
				Message:  err.Error(),
			})
		}
	}
	return diagnostics, nil
}

// UnknownFieldDiagnostics reports mapping keys that are not represented by the
// supplied Go YAML shape. It deliberately returns warnings, not errors.
func UnknownFieldDiagnostics(name string, bs []byte, out any) ([]Diagnostic, error) {
	var doc yaml.Node
	dec := yaml.NewDecoder(bytes.NewReader(bs))
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	if len(doc.Content) == 0 {
		return nil, nil
	}
	t := reflect.TypeOf(out)
	if t == nil {
		return nil, nil
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	diagnostics := collectUnknownFields(name, doc.Content[0], t, nil)
	sort.Slice(diagnostics, func(i, j int) bool {
		if diagnostics[i].Source != diagnostics[j].Source {
			return diagnostics[i].Source < diagnostics[j].Source
		}
		return diagnostics[i].Path < diagnostics[j].Path
	})
	return diagnostics, nil
}

func requirePath(node *yaml.Node, path Path, rendered []string) error {
	if len(path) == 0 {
		if isNull(node) {
			return fmt.Errorf("required YAML setting %q must not be null", renderPath(rendered))
		}
		return nil
	}
	if isNull(node) {
		return fmt.Errorf("required YAML setting %q must not be null", renderPath(rendered))
	}
	part := path[0]
	if part == "*" {
		switch node.Kind {
		case yaml.SequenceNode:
			for i, child := range node.Content {
				itemPath := appendIndex(rendered, i)
				if err := requirePath(child, path[1:], itemPath); err != nil {
					return err
				}
			}
			return nil
		case yaml.MappingNode:
			for i := 0; i+1 < len(node.Content); i += 2 {
				key := node.Content[i].Value
				childPath := appendPathPart(rendered, key)
				if err := requirePath(node.Content[i+1], path[1:], childPath); err != nil {
					return err
				}
			}
			return nil
		default:
			return fmt.Errorf("required YAML setting %q must be a sequence or mapping", renderPath(rendered))
		}
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("required YAML setting %q is missing", renderPath(append(rendered, part)))
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == part {
			return requirePath(node.Content[i+1], path[1:], append(rendered, part))
		}
	}
	return fmt.Errorf("required YAML setting %q is missing", renderPath(append(rendered, part)))
}

func collectUnknownFields(name string, node *yaml.Node, t reflect.Type, path []string) []Diagnostic {
	if node == nil {
		return nil
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		return collectUnknownStructFields(name, node, t, path)
	case reflect.Slice, reflect.Array:
		if node.Kind != yaml.SequenceNode {
			return nil
		}
		elem := t.Elem()
		out := make([]Diagnostic, 0)
		for i, child := range node.Content {
			out = append(out, collectUnknownFields(name, child, elem, appendIndex(path, i))...)
		}
		return out
	case reflect.Map:
		if node.Kind != yaml.MappingNode {
			return nil
		}
		out := make([]Diagnostic, 0)
		elem := t.Elem()
		if !typeNeedsUnknownFieldWalk(elem) {
			return out
		}
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := node.Content[i].Value
			out = append(out, collectUnknownFields(name, node.Content[i+1], elem, appendPathPart(path, key))...)
		}
		return out
	default:
		return nil
	}
}

func collectUnknownStructFields(name string, node *yaml.Node, t reflect.Type, path []string) []Diagnostic {
	if node.Kind != yaml.MappingNode {
		return nil
	}
	fields := yamlStructFields(t)
	out := make([]Diagnostic, 0)
	for i := 0; i+1 < len(node.Content); i += 2 {
		keyNode := node.Content[i]
		valueNode := node.Content[i+1]
		fieldType, ok := fields[keyNode.Value]
		keyPath := appendPathPart(path, keyNode.Value)
		if !ok {
			rendered := renderPath(keyPath)
			out = append(out, Diagnostic{
				Severity: DiagnosticWarning,
				Source:   name,
				Path:     rendered,
				Message:  fmt.Sprintf("extra YAML key %q is ignored", rendered),
			})
			continue
		}
		out = append(out, collectUnknownFields(name, valueNode, fieldType, keyPath)...)
	}
	return out
}

func yamlStructFields(t reflect.Type) map[string]reflect.Type {
	fields := make(map[string]reflect.Type)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" && !field.Anonymous {
			continue
		}
		tag := field.Tag.Get("yaml")
		name := strings.Split(tag, ",")[0]
		if name == "-" {
			continue
		}
		if name == "" {
			name = field.Name
		}
		fields[name] = field.Type
	}
	return fields
}

func typeNeedsUnknownFieldWalk(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct, reflect.Slice, reflect.Array:
		return true
	case reflect.Map:
		return typeNeedsUnknownFieldWalk(t.Elem())
	default:
		return false
	}
}

func isNull(node *yaml.Node) bool {
	return node == nil || (node.Kind == yaml.ScalarNode && node.Tag == "!!null")
}

func appendPathPart(path []string, part string) []string {
	out := append([]string(nil), path...)
	return append(out, part)
}

func appendIndex(path []string, index int) []string {
	out := append([]string(nil), path...)
	if len(out) == 0 {
		out = append(out, fmt.Sprintf("[%d]", index))
		return out
	}
	out[len(out)-1] = fmt.Sprintf("%s[%d]", out[len(out)-1], index)
	return out
}

func renderPath(path []string) string {
	if len(path) == 0 {
		return "<root>"
	}
	return strings.Join(path, ".")
}

func diagnosticPathFromRequiredError(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	start := strings.IndexByte(text, '"')
	end := strings.LastIndexByte(text, '"')
	if start >= 0 && end > start {
		return text[start+1 : end]
	}
	return ""
}
