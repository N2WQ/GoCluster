// File role: Native CC definitions and immutable prepared ownership. Public DTOs
// are detached from prepared generations; matching never retains caller maps.
package filter

import (
	"crypto/sha256"
	"hash"
	"slices"

	"gopkg.in/yaml.v3"
)

const (
	MaxCCModes          = 128
	MaxCCCountries      = 512
	MaxCCStates         = 73
	MaxCCCanonicalBytes = 128 << 10
)

type CCAction string

const (
	CCOff    CCAction = "off"
	CCPass   CCAction = "pass"
	CCReject CCAction = "reject"
)

// CCCategory uses exactly one typed value family. OFF owns neither list; an
// empty PASS denies every value, while an empty REJECT rejects no value.
type CCCategory struct {
	Action    CCAction `yaml:"action"`
	Countries []int    `yaml:"countries,omitempty,flow"`
	States    []string `yaml:"states,omitempty,flow"`
}

// A missing band override inherits Global. An explicit OFF row suspends that
// category for that band; editing Global removes all prior band overrides.
type CCGeography struct {
	Global CCCategory            `yaml:"global"`
	Bands  map[string]CCCategory `yaml:"bands"`
}

// CCDXBM stores a base selection plus exceptions for the fixed native slots.
// Active remains true even when the current selection admits every frequency.
type CCDXBM struct {
	Active    bool            `yaml:"active"`
	BaseAllow bool            `yaml:"base_allow"`
	Slots     map[string]bool `yaml:"slots"`
}

// Modes and Sources are explicit selections: empty means deny all. Only the
// separate default factory reinstalls operator defaults. Six delivery controls
// are captured here but remain outside PreparedCCFilter's personal matching.
type CCConfiguration struct {
	Modes          []string               `yaml:"modes,flow"`
	Sources        []string               `yaml:"sources,flow"`
	Geography      map[string]CCGeography `yaml:"geography"`
	DXBM           CCDXBM                 `yaml:"dxbm"`
	IncludeBeacons DefaultBool            `yaml:"include_beacons"`
	AllowWWV       DefaultBool            `yaml:"allow_wwv"`
	AllowWCY       DefaultBool            `yaml:"allow_wcy"`
	AllowAnnounce  DefaultBool            `yaml:"allow_announce"`
	AllowSelf      DefaultBool            `yaml:"allow_self"`
	AllowToxic     DefaultBool            `yaml:"allow_toxic"`
}

var ccGeographyNames = [...]string{"DOC", "DOS", "DXCTY", "DXSTATE"}

// Clone is for already admitted definitions. Untrusted DTOs must pass
// PrepareCCConfiguration before callers request an owned copy.
func (c CCConfiguration) Clone() CCConfiguration {
	c.Modes, c.Sources = slices.Clone(c.Modes), slices.Clone(c.Sources)
	geography := make(map[string]CCGeography, len(c.Geography))
	for name, entry := range c.Geography {
		entry.Global = entry.Global.clone()
		bands := make(map[string]CCCategory, len(entry.Bands))
		for band, category := range entry.Bands {
			bands[band] = category.clone()
		}
		entry.Bands = bands
		geography[name] = entry
	}
	c.Geography = geography
	c.DXBM.Slots = cloneCCSlots(c.DXBM.Slots)
	return c
}

func (c CCCategory) clone() CCCategory {
	c.Countries, c.States = slices.Clone(c.Countries), slices.Clone(c.States)
	return c
}

func cloneCCSlots(input map[string]bool) map[string]bool {
	result := make(map[string]bool, len(input))
	for name, enabled := range input {
		result[name] = enabled
	}
	return result
}

func (c CCConfiguration) toggles() [6]DefaultBool {
	return [6]DefaultBool{c.IncludeBeacons, c.AllowWWV, c.AllowWCY, c.AllowAnnounce, c.AllowSelf, c.AllowToxic}
}

// Equal compares prepared canonical definitions, never prepared pointer identity.
func (c CCConfiguration) Equal(other CCConfiguration) bool {
	if c.toggles() != other.toggles() || !slices.Equal(c.Modes, other.Modes) ||
		!slices.Equal(c.Sources, other.Sources) || len(c.Geography) != len(other.Geography) ||
		c.DXBM.Active != other.DXBM.Active || c.DXBM.BaseAllow != other.DXBM.BaseAllow ||
		len(c.DXBM.Slots) != len(other.DXBM.Slots) {
		return false
	}
	for name, geography := range c.Geography {
		peer, exists := other.Geography[name]
		if !exists || !geography.Global.equal(peer.Global) || len(geography.Bands) != len(peer.Bands) {
			return false
		}
		for band, category := range geography.Bands {
			peerCategory, present := peer.Bands[band]
			if !present || !category.equal(peerCategory) {
				return false
			}
		}
	}
	for slot, enabled := range c.DXBM.Slots {
		peer, exists := other.DXBM.Slots[slot]
		if !exists || enabled != peer {
			return false
		}
	}
	return true
}

func (c CCCategory) equal(other CCCategory) bool {
	return c.Action == other.Action && slices.Equal(c.Countries, other.Countries) && slices.Equal(c.States, other.States)
}

// Fingerprint has the same canonical-input contract as Equal. Fixed native
// iteration order avoids map-order dependence and historical fingerprint caches.
func (c CCConfiguration) Fingerprint() [32]byte {
	h := sha256.New()
	writeFingerprintString(h, "native-cc-v7")
	for _, values := range [][]string{c.Modes, c.Sources} {
		writeFingerprintInt(h, len(values))
		for _, value := range values {
			writeFingerprintString(h, value)
		}
	}
	for _, name := range ccGeographyNames {
		geography := c.Geography[name]
		writeFingerprintString(h, name)
		writeCCCategoryFingerprint(h, geography.Global)
		for _, band := range ccNativeBands {
			category, exists := geography.Bands[band]
			writeFingerprintInt(h, boolCCInt(exists))
			if exists {
				writeCCCategoryFingerprint(h, category)
			}
		}
	}
	writeFingerprintInt(h, boolCCInt(c.DXBM.Active))
	writeFingerprintInt(h, boolCCInt(c.DXBM.BaseAllow))
	for _, slot := range ccNativeSlots {
		enabled, exists := c.DXBM.Slots[slot.Name]
		writeFingerprintInt(h, boolCCInt(exists))
		writeFingerprintInt(h, boolCCInt(enabled))
	}
	for _, toggle := range c.toggles() {
		writeFingerprintInt(h, int(toggle))
	}
	var result [32]byte
	h.Sum(result[:0])
	return result
}

func boolCCInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func writeCCCategoryFingerprint(h hash.Hash, category CCCategory) {
	writeFingerprintString(h, string(category.Action))
	writeFingerprintInt(h, len(category.Countries))
	for _, country := range category.Countries {
		writeFingerprintInt(h, country)
	}
	writeFingerprintInt(h, len(category.States))
	for _, state := range category.States {
		writeFingerprintString(h, state)
	}
}

// CCCounts reports cardinality owned by this immutable generation, not traffic
// history. CanonicalBytes includes definitions and all six exact controls.
type CCCounts struct {
	Modes, Sources, GeographyRows, Countries, States, DXBMSlots, CanonicalBytes int
}

func canonicalCCBytes(c CCConfiguration) (int, error) {
	data, err := yaml.Marshal(c)
	return len(data), err
}
