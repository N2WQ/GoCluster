package telnet

import (
	"testing"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

func TestV7MachineSchemaEachCCSwitchFalseAndDefault(t *testing.T) {
	// The expected destinations are explicit, independent of the decoder's
	// indexed field table. Equality also catches accidental sibling writes.
	for _, field := range []struct {
		name string
		set  func(*filter.CCConfiguration, filter.DefaultBool)
	}{
		{"include_beacons", func(c *filter.CCConfiguration, value filter.DefaultBool) { c.IncludeBeacons = value }},
		{"allow_wwv", func(c *filter.CCConfiguration, value filter.DefaultBool) { c.AllowWWV = value }},
		{"allow_wcy", func(c *filter.CCConfiguration, value filter.DefaultBool) { c.AllowWCY = value }},
		{"allow_announce", func(c *filter.CCConfiguration, value filter.DefaultBool) { c.AllowAnnounce = value }},
		{"allow_self", func(c *filter.CCConfiguration, value filter.DefaultBool) { c.AllowSelf = value }},
		{"allow_toxic", func(c *filter.CCConfiguration, value filter.DefaultBool) { c.AllowToxic = value }},
	} {
		for _, value := range []struct {
			text string
			want filter.DefaultBool
		}{{"false", filter.DefaultBoolFalse}, {"DEFAULT", filter.DefaultBoolDefault}} {
			t.Run(field.name+"/"+value.text, func(t *testing.T) {
				before, _ := v7PreparedFixture(t)
				before.Filters.CC.IncludeBeacons, before.Filters.CC.AllowWWV = filter.DefaultBoolTrue, filter.DefaultBoolTrue
				before.Filters.CC.AllowWCY, before.Filters.CC.AllowAnnounce = filter.DefaultBoolTrue, filter.DefaultBoolTrue
				before.Filters.CC.AllowSelf, before.Filters.CC.AllowToxic = filter.DefaultBoolTrue, filter.DefaultBoolTrue
				saved, want := before.Clone(), before.Clone()
				field.set(&want.Filters.CC, value.want)
				request := decodeV7PatchFixture(t, "FILTER", "dialect: cc\nfilters:\n  "+field.name+": "+value.text+"\n")
				got, err := request.apply(before, "cc")
				if err != nil || !got.Equal(want) || !before.Equal(saved) {
					t.Fatalf("single native switch changed the wrong owner or input: %v", err)
				}
				canonical, runtime, _, err := filter.PrepareV7Configuration(got)
				if err != nil {
					t.Fatal(err)
				}
				response, err := renderV7YAMLReadback(canonical, runtime.PreparedRules(), configurationReadbackStatus{Effective: readbackEffectiveStatus{Dialect: "cc"}}, nil, "FILTER", "switch-1", "revision-1")
				if err != nil {
					t.Fatal(err)
				}
				var exported struct {
					Configuration struct {
						Filters map[string]yaml.Node `yaml:"filters"`
					} `yaml:"configuration"`
				}
				if err := yaml.Unmarshal([]byte(response), &exported); err != nil {
					t.Fatal(err)
				}
				node := exported.Configuration.Filters[field.name]
				if node.Value != value.text || value.text == "false" && node.Tag != "!!bool" || value.text == "DEFAULT" && node.Tag != "!!str" {
					t.Fatalf("native readback lost explicit value/tag: %+v", node)
				}
			})
		}
	}
}
