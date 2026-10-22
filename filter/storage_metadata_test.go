package filter

import (
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type storageMetadataInlineFields struct {
	Renamed     string `yaml:"renamed,omitempty"`
	DefaultName int
	Ignored     bool `yaml:"-"`
	private     bool
}

type storageMetadataInlineFixture struct {
	Inline   storageMetadataInlineFields `yaml:",inline"`
	Override bool                        `yaml:"renamed"`
	Tail     string                      `yaml:"tail,omitempty"`
}

func storageMetadataDocument(t *testing.T, raw string) *yaml.Node {
	t.Helper()
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(raw), &document); err != nil {
		t.Fatal(err)
	}
	return &document
}

func uncachedStoredFields(fields map[string]reflect.Type, typ reflect.Type) {
	for i := range typ.NumField() {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		tag := strings.Split(field.Tag.Get("yaml"), ",")
		if tag[0] == "-" {
			continue
		}
		inline := false
		for _, flag := range tag[1:] {
			inline = inline || flag == "inline"
		}
		if inline {
			uncachedStoredFields(fields, field.Type)
			continue
		}
		name := tag[0]
		if name == "" {
			name = strings.ToLower(field.Name)
		}
		fields[name] = field.Type
	}
}

func TestStorageMetadataMatchesUncachedSchema(t *testing.T) {
	fixture := storageMetadataInlineFields{private: true}
	if err := yaml.Unmarshal([]byte("private: false\n"), &fixture); err != nil || !fixture.private {
		t.Fatalf("unexported fixture field must remain excluded from the YAML schema: %v", err)
	}
	types := []reflect.Type{
		reflect.TypeFor[V5UserRecord](),
		reflect.TypeFor[V5SavedPreset](),
		reflect.TypeFor[V5PresetReference](),
		reflect.TypeFor[V5Configuration](),
		reflect.TypeFor[V5FilterConfiguration](),
		reflect.TypeFor[SettingsConfiguration](),
		reflect.TypeFor[RuleDefinition](),
		reflect.TypeFor[storageMetadataInlineFixture](),
	}
	state, err := newStorageDecodeState(storageMetadataDocument(t, "{}\n"))
	if err != nil {
		t.Fatal(err)
	}
	work := state.work
	for _, typ := range types {
		t.Run(typ.String(), func(t *testing.T) {
			uncached := make(map[string]reflect.Type)
			uncachedStoredFields(uncached, typ)
			cached := state.storedFields(typ)
			if len(cached) != len(uncached) {
				t.Fatalf("schema size differs: cached=%d uncached=%d", len(cached), len(uncached))
			}
			out := reflect.New(typ).Elem()
			for name, want := range uncached {
				got, exists := cached[name]
				if !exists || got.typ != want {
					t.Fatalf("field %q: cached=%v uncached=%v", name, got.typ, want)
				}
				if out.FieldByIndex(got.index).Type() != want {
					t.Fatalf("field %q has the wrong cached index", name)
				}
			}
			if len(state.fields) > len(types) {
				t.Fatal("schema storage grew beyond requested types")
			}
		})
	}
	if state.work != work {
		t.Fatalf("metadata construction charged decoding work: before=%#v after=%#v", work, state.work)
	}
}

func TestStorageMetadataOperationAndTypeOwnership(t *testing.T) {
	document := storageMetadataDocument(t, "{}\n")
	first, err := newStorageDecodeState(document)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newStorageDecodeState(document)
	if err != nil {
		t.Fatal(err)
	}
	setType := reflect.TypeFor[V5SavedPreset]()
	firstFields := first.storedFields(setType)
	if len(second.fields) != 0 {
		t.Fatal("a fresh operation inherited another operation's metadata")
	}
	// Deliberately damage only this test's private map to expose shared backing
	// storage. Production treats cached schemas as read-only.
	firstFields["configuration"] = storageDecodeField{typ: reflect.TypeFor[bool]()}
	if got := second.storedFields(setType)["configuration"].typ; got != reflect.TypeFor[V5Configuration]() {
		t.Fatalf("fresh operation shared mutable schema backing: %v", got)
	}
	recordFields := second.storedFields(reflect.TypeFor[V5UserRecord]())
	if recordFields["recent_ips"].typ != reflect.TypeFor[[]string]() {
		t.Fatal("record metadata lost its owner-specific field")
	}
	if _, exists := second.storedFields(setType)["recent_ips"]; exists {
		t.Fatal("record fields leaked into preset metadata")
	}
	if len(second.fields) != 2 {
		t.Fatalf("unexpected metadata type count: %d", len(second.fields))
	}
}

func TestStorageMetadataRevalidatesEachOwner(t *testing.T) {
	raw := "- {configuration_version: 5, configuration: {filters: {rules: []}, settings: {grid: FN31}}}\n" +
		"- {configuration_version: 5, configuration: {filters: {rules: []}, settings: {grid: null}}}\n" +
		"- {configuration_version: 5, configuration: {filters: {rules: []}, settings: {grid: FN32}}}\n"
	document := storageMetadataDocument(t, raw)
	state, err := newStorageDecodeState(document)
	if err != nil {
		t.Fatal(err)
	}
	for i, node := range document.Content[0].Content {
		err := state.validateV5StoredFields(node, reflect.TypeFor[V5SavedPreset]())
		if i == 1 {
			if err == nil || !strings.Contains(err.Error(), "invalid stored field grid") {
				t.Fatal("cached schema bypassed later null validation", err)
			}
			continue
		}
		if err != nil {
			t.Fatal("valid owner refused after cached-schema reuse", err)
		}
		state.materialize = true
		var value V5SavedPreset
		guard := newStorageDecodeGuard(false)
		if _, _, err := state.decode(node, reflect.TypeFor[V5SavedPreset](), reflect.ValueOf(&value).Elem(), guard); err != nil {
			t.Fatal(err)
		}
		if len(guard.typeErrors) != 0 {
			t.Fatal(guard.typeErrors)
		}
		if i == 2 && value.Configuration.Settings.Grid != "FN32" {
			t.Fatal("cached metadata reused an earlier owner's value")
		}
		state.materialize = false
	}
}

func TestStorageMetadataBoundIndependentOfOwnerCount(t *testing.T) {
	for _, count := range []int{1, 1000} {
		document := storageMetadataDocument(t, strings.Repeat("- {configuration_version: 5, configuration: {filters: {rules: []}, settings: {}}}\n", count))
		state, err := newStorageDecodeState(document)
		if err != nil {
			t.Fatal(err)
		}
		for _, node := range document.Content[0].Content {
			if err := state.validateV5StoredFields(node, reflect.TypeFor[V5SavedPreset]()); err != nil {
				t.Fatal(err)
			}
		}
		if len(state.fields) != 4 {
			t.Fatalf("%d owners retained %d schemas, want four fixed v5 DTO types", count, len(state.fields))
		}
	}
}
