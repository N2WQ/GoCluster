// File role: Fixed command conveniences over the common physical-band catalog.
// Meta names are never stored as a Spot's band or added to physical discovery.
package spot

import (
	"slices"
	"strings"
)

// MetaBandDefinition describes one fixed, ordered collection of physical bands.
// Discovery functions return owned member slices, not the immutable catalog.
type MetaBandDefinition struct {
	Name    string
	Members []string
}

var metaBandDefinitions = [...]MetaBandDefinition{
	{Name: "WARC", Members: []string{"30m", "17m", "12m"}},
	{Name: "HFCONTEST", Members: []string{"160m", "80m", "40m", "20m", "15m", "10m"}},
	{Name: "HF", Members: []string{"160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m"}},
	{Name: "VHF", Members: []string{"6m", "4m", "2m", "1.25m"}},
	{Name: "UHF", Members: []string{"70cm", "33cm", "23cm", "13cm"}},
	{Name: "VHFUP", Members: []string{"6m", "4m", "2m", "1.25m", "70cm", "33cm", "23cm", "13cm"}},
}

// SupportedMetaBands returns the fixed catalog in HELP/discovery order.
func SupportedMetaBands() []MetaBandDefinition {
	result := make([]MetaBandDefinition, len(metaBandDefinitions))
	for i, definition := range metaBandDefinitions {
		result[i] = MetaBandDefinition{Name: definition.Name, Members: slices.Clone(definition.Members)}
	}
	return result
}

// MetaBandMembers accepts case-insensitive fixed names and returns an owned
// ordered list. It does not interpret physical aliases or invent meta bands.
func MetaBandMembers(name string) ([]string, bool) {
	for _, definition := range metaBandDefinitions {
		if strings.EqualFold(name, definition.Name) {
			return slices.Clone(definition.Members), true
		}
	}
	return nil, false
}
