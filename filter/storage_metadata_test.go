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

func TestStorageMetadataMatchesLegacySchema(t *testing.T) {
	fixture := storageMetadataInlineFields{private: true}
	if err := yaml.Unmarshal([]byte("private: false\n"), &fixture); err != nil || !fixture.private {
		t.Fatalf("unexported fixture field must remain excluded from the YAML schema: %v", err)
	}
	types := []reflect.Type{
		reflect.TypeFor[UserRecord](),
		reflect.TypeFor[SavedPreset](),
		reflect.TypeFor[PresetReference](),
		reflect.TypeFor[storagePlainUserRecord](),
		reflect.TypeFor[storagePlainSavedPreset](),
		reflect.TypeFor[storagePlainPresetReference](),
		reflect.TypeFor[Filter](),
		reflect.TypeFor[storageMetadataInlineFixture](),
	}
	state, err := newStorageDecodeState(storageMetadataDocument(t, "{}\n"))
	if err != nil {
		t.Fatal(err)
	}
	work := state.work
	for _, typ := range types {
		t.Run(typ.String(), func(t *testing.T) {
			legacy := make(map[string]reflect.Type)
			addStoredFields(legacy, typ)
			cached := state.storedFields(typ)
			if len(cached) != len(legacy) {
				t.Fatalf("schema size differs: cached=%d legacy=%d", len(cached), len(legacy))
			}
			out := reflect.New(typ).Elem()
			for name, want := range legacy {
				got, exists := cached[name]
				if !exists || got.typ != want {
					t.Fatalf("field %q: cached=%v legacy=%v", name, got.typ, want)
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
	setType := reflect.TypeFor[SavedPreset]()
	firstFields := first.storedFields(setType)
	if len(second.fields) != 0 {
		t.Fatal("a fresh operation inherited another operation's metadata")
	}
	// Deliberately damage only this test's private map to expose shared backing
	// storage. Production treats cached schemas as read-only.
	firstFields["grid"] = storageDecodeField{typ: reflect.TypeFor[bool]()}
	if got := second.storedFields(setType)["grid"].typ; got != reflect.TypeFor[string]() {
		t.Fatalf("fresh operation shared mutable schema backing: %v", got)
	}
	recordFields := second.storedFields(reflect.TypeFor[UserRecord]())
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
	for _, test := range []struct {
		name      string
		typ       reflect.Type
		raw       string
		wantError string
		lastField string
		lastValue string
	}{
		{"record", reflect.TypeFor[UserRecord](), "- {configuration_version: 4, dialect: go}\n- {configuration_version: 4, dialect: null}\n- {configuration_version: 4, dialect: cc}\n", "invalid stored field dialect", "Dialect", "cc"},
		{"preset", reflect.TypeFor[SavedPreset](), "- {configuration_version: 4, grid: FN31}\n- {configuration_version: 4, grid: null}\n- {configuration_version: 4, grid: FN32}\n", "invalid stored field grid", "Grid", "FN32"},
		{"preset versions", reflect.TypeFor[SavedPreset](), "- {configuration_version: 4, grid: FN31}\n- {configuration_version: 5, grid: FN31}\n- {grid: FN32}\n", "unsupported configuration version", "Grid", "FN32"},
		{"reference nested preset", reflect.TypeFor[PresetReference](), "- {name: FIRST, baseline: {configuration_version: 4, grid: FN31}}\n- {name: FIRST, baseline: {configuration_version: 4, grid: null}}\n- {name: SECOND, baseline: {configuration_version: 4, grid: FN32}}\n", "invalid stored field grid", "Name", "SECOND"},
		{"reference own value", reflect.TypeFor[PresetReference](), "- {name: FIRST, baseline: {configuration_version: 4}}\n- {name: null, baseline: {configuration_version: 4}}\n- {name: SECOND, baseline: {configuration_version: 4}}\n", "invalid stored field name", "Name", "SECOND"},
	} {
		t.Run(test.name, func(t *testing.T) {
			document := storageMetadataDocument(t, test.raw)
			for _, materialize := range []bool{false, true} {
				state, err := newStorageDecodeState(document)
				if err != nil {
					t.Fatal(err)
				}
				state.materialize = materialize
				for i, node := range document.Content[0].Content {
					out := reflect.New(test.typ).Elem()
					_, _, err := state.decodeOwner(node, test.typ, out, newStorageDecodeGuard(false))
					if i == 1 {
						if err == nil || !strings.Contains(err.Error(), test.wantError) {
							t.Fatalf("materialize=%t invalid owner: %v", materialize, err)
						}
					} else if err != nil {
						t.Fatalf("materialize=%t valid owner %d: %v", materialize, i, err)
					} else if materialize && i == 2 && out.FieldByName(test.lastField).String() != test.lastValue {
						t.Fatalf("materialization reused an earlier value: %#v", out.Interface())
					}
				}
			}
		})
	}
}

func TestStorageMetadataBoundIndependentOfOwnerCount(t *testing.T) {
	for _, count := range []int{1, 1000} {
		document := storageMetadataDocument(t, strings.Repeat("- {}\n", count))
		state, err := newStorageDecodeState(document)
		if err != nil {
			t.Fatal(err)
		}
		for _, node := range document.Content[0].Content {
			if err := state.preflightOwner(node, reflect.TypeFor[SavedPreset]()); err != nil {
				t.Fatal(err)
			}
		}
		if len(state.fields) != 2 {
			t.Fatalf("%d owners retained %d schemas, want owner and plain owner only", count, len(state.fields))
		}
	}
}
